/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.connector.system.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.slice.Slices;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.connector.system.SystemColumnHandle;
import io.trino.connector.system.SystemSplit;
import io.trino.connector.system.SystemTablesConfig;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.node.InternalNode;
import io.trino.security.AccessControl;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.connector.system.jdbc.FilterUtil.isImpossibleObjectName;
import static io.trino.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.getRelationTypes;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.metadata.MetadataListing.listSchemas;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.connector.FixedSplitSource.emptySplitSource;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TableJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "tables");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column("table_cat", VARCHAR)
            .column("table_schem", VARCHAR)
            .column("table_name", VARCHAR)
            .column("table_type", VARCHAR)
            .column("remarks", VARCHAR)
            .column("type_cat", VARCHAR)
            .column("type_schem", VARCHAR)
            .column("type_name", VARCHAR)
            .column("self_referencing_col_name", VARCHAR)
            .column("ref_generation", VARCHAR)
            .build();

    private static final ColumnHandle CATALOG_COLUMN = new SystemColumnHandle("table_cat");

    private final Metadata metadata;
    private final AccessControl accessControl;
    private final InternalNode currentNode;
    private final int schemaBatchSize;

    @Inject
    public TableJdbcTable(Metadata metadata, AccessControl accessControl, InternalNode currentNode, SystemTablesConfig config)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.schemaBatchSize = config.getJdbcSchemaBatchSize();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public RecordCursor cursor(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession connectorSession,
            TupleDomain<Integer> constraint,
            Set<Integer> requiredColumns,
            ConnectorSplit split)
    {
        Builder table = InMemoryRecordSet.builder(METADATA);
        Session session = ((FullConnectorSession) connectorSession).getSession();
        SystemSplit systemSplit = (SystemSplit) split;

        Domain schemaDomain = constraint.getDomain(1, VARCHAR);
        Domain tableDomain = constraint.getDomain(2, VARCHAR);
        Domain typeDomain = constraint.getDomain(3, VARCHAR);

        if (isImpossibleObjectName(schemaDomain) || isImpossibleObjectName(tableDomain)) {
            return table.build().cursor();
        }

        Optional<String> schemaFilter = tryGetSingleVarcharValue(schemaDomain);
        Optional<String> tableFilter = tryGetSingleVarcharValue(tableDomain);

        boolean includeTables = typeDomain.includesNullableValue(Slices.utf8Slice("TABLE"));
        boolean includeViews = typeDomain.includesNullableValue(Slices.utf8Slice("VIEW"));
        if (!includeTables && !includeViews) {
            return table.build().cursor();
        }

        String catalog = systemSplit.getCatalogName().orElseThrow();

        // NEW: Schema-level pagination implementation
        if (schemaFilter.isPresent()) {
            // If specific schema requested, process directly (no pagination needed)
            QualifiedTablePrefix prefix = tablePrefix(catalog, schemaFilter, tableFilter);
            processSchemaBatch(session, metadata, accessControl, prefix, table, includeTables, includeViews);
        }
        else {
            // Process all schemas in batches to reduce OPA load
            List<String> allSchemas = listAllAccessibleSchemas(session, metadata, accessControl, catalog);
            List<List<String>> schemaBatches = partitionSchemas(allSchemas, schemaBatchSize);
            
            for (List<String> schemaBatch : schemaBatches) {
                processSchemaBatch(session, metadata, accessControl, catalog, schemaBatch, tableFilter, 
                                 table, includeTables, includeViews);
            }
        }

        return table.build().cursor();
    }

    /**
     * Process a single schema batch, applying access control to all tables in the batch
     */
    private void processSchemaBatch(
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            String catalog,
            List<String> schemas,
            Optional<String> tableFilter,
            InMemoryRecordSet.Builder table,
            boolean includeTables,
            boolean includeViews)
    {
        // Collect all tables from this batch of schemas
        Map<SchemaTableName, RelationType> batchTables = new HashMap<>();
        
        for (String schema : schemas) {
            QualifiedTablePrefix prefix = tableFilter.isPresent() 
                ? tablePrefix(catalog, Optional.of(schema), tableFilter)
                : tablePrefix(catalog, Optional.of(schema), Optional.empty());
            
            Map<SchemaTableName, RelationType> schemaTables = getRelationTypes(session, metadata, accessControl, prefix);
            batchTables.putAll(schemaTables);
        }

        // Add all allowed tables from this batch to the result
        batchTables.forEach((name, type) -> {
            boolean isView = type == RelationType.VIEW;
            if ((includeTables && !isView) || (includeViews && isView)) {
                table.addRow(tableRow(catalog, name, isView ? "VIEW" : "TABLE"));
            }
        });
    }

    /**
     * Process a single schema (for specific schema filter case)
     */
    private void processSchemaBatch(
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            QualifiedTablePrefix prefix,
            InMemoryRecordSet.Builder table,
            boolean includeTables,
            boolean includeViews)
    {
        getRelationTypes(session, metadata, accessControl, prefix).forEach((name, type) -> {
            boolean isView = type == RelationType.VIEW;
            if ((includeTables && !isView) || (includeViews && isView)) {
                table.addRow(tableRow(prefix.getCatalogName(), name, isView ? "VIEW" : "TABLE"));
            }
        });
    }

    /**
     * Get list of all accessible schemas for pagination
     */
    private List<String> listAllAccessibleSchemas(
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            String catalog)
    {
        return listSchemas(session, metadata, accessControl, catalog);
    }

    /**
     * Partition schemas into batches for processing
     */
    private List<List<String>> partitionSchemas(List<String> schemas, int batchSize)
    {
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < schemas.size(); i += batchSize) {
            int end = Math.min(i + batchSize, schemas.size());
            batches.add(ImmutableList.copyOf(schemas.subList(i, end)));
        }
        return batches;
    }

    @Override
    public Optional<ConnectorSplitSource> splitSource(ConnectorSession connectorSession, TupleDomain<ColumnHandle> constraint)
    {
        Domain catalogDomain = constraint.getDomain(CATALOG_COLUMN, VARCHAR);
        if (isImpossibleObjectName(catalogDomain)) {
            return Optional.of(emptySplitSource());
        }

        Session session = ((FullConnectorSession) connectorSession).getSession();
        // This is an implementation of SINGLE_COORDINATOR distribution for this table.
        HostAddress address = currentNode.getHostAndPort();
        List<SystemSplit> splits = listCatalogNames(session, metadata, accessControl, catalogDomain).stream()
                .map(catalog -> new SystemSplit(address, constraint, Optional.of(catalog)))
                .collect(toImmutableList());
        return Optional.of(new FixedSplitSource(splits));
    }

    private static Object[] tableRow(String catalog, SchemaTableName name, String type)
    {
        return new Object[] {catalog, name.getSchemaName(), name.getTableName(), type,
                null, null, null, null, null, null};
    }
}
