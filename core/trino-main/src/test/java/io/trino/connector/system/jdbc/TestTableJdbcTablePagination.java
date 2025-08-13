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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.SecurityContext;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestTableJdbcTablePagination
{
    @Test
    public void testSchemaPaginationReducesOpaRequests()
    {
        // Create test data: 10 schemas with 5 tables each = 50 total tables
        List<String> schemas = ImmutableList.of(
                "schema1", "schema2", "schema3", "schema4", "schema5",
                "schema6", "schema7", "schema8", "schema9", "schema10"
        );
        
        // Mock dependencies
        Metadata metadata = mock(Metadata.class);
        AccessControl accessControl = mock(AccessControl.class);
        InternalNode currentNode = mock(InternalNode.class);
        SystemTablesConfig config = new SystemTablesConfig();
        config.setJdbcSchemaBatchSize(3); // Process 3 schemas per batch
        
        // Mock schema listing
        when(metadata.listSchemaNames(any(), anyString())).thenReturn(schemas);
        
        // Mock table listing for each schema
        for (String schema : schemas) {
            Map<SchemaTableName, RelationType> schemaTables = ImmutableMap.of(
                    new SchemaTableName(schema, "table1"), RelationType.TABLE,
                    new SchemaTableName(schema, "table2"), RelationType.TABLE,
                    new SchemaTableName(schema, "table3"), RelationType.TABLE,
                    new SchemaTableName(schema, "table4"), RelationType.VIEW,
                    new SchemaTableName(schema, "table5"), RelationType.VIEW
            );
            when(metadata.getRelationTypes(any(), eq(new QualifiedTablePrefix("test_catalog", schema))))
                    .thenReturn(schemaTables);
        }
        
        // Track access control calls to verify batching
        AtomicInteger accessControlCallCount = new AtomicInteger(0);
        when(accessControl.filterTables(any(SecurityContext.class), anyString(), any()))
                .thenAnswer(invocation -> {
                    accessControlCallCount.incrementAndGet();
                    Set<SchemaTableName> tables = invocation.getArgument(2);
                    return tables; // Return all tables (no filtering for test)
                });
        
        // Create TableJdbcTable with pagination
        TableJdbcTable tableJdbcTable = new TableJdbcTable(metadata, accessControl, currentNode, config);
        
        // Create test session and split
        Session session = mock(Session.class);
        ConnectorSession connectorSession = mock(FullConnectorSession.class);
        when(((FullConnectorSession) connectorSession).getSession()).thenReturn(session);
        
        SystemSplit split = new SystemSplit(
                HostAddress.fromString("localhost:8080"),
                TupleDomain.all(),
                java.util.Optional.of("test_catalog")
        );
        
        // Execute the cursor method
        RecordCursor cursor = tableJdbcTable.cursor(
                TestingTransactionHandle.create(),
                connectorSession,
                TupleDomain.all(),
                ImmutableSet.of(),
                split
        );
        
        // Verify results
        int rowCount = 0;
        while (cursor.advanceNextPosition()) {
            rowCount++;
            // Verify the structure of returned data
            assertThat(cursor.getSlice(0).toStringUtf8()).isEqualTo("test_catalog"); // catalog
            assertThat(cursor.getSlice(1)).isNotNull(); // schema
            assertThat(cursor.getSlice(2)).isNotNull(); // table
            assertThat(cursor.getSlice(3)).isNotNull(); // type
        }
        
        // Should have 50 total tables (10 schemas * 5 tables each)
        assertThat(rowCount).isEqualTo(50);
        
        // Verify pagination: 10 schemas with batch size 3 should result in 4 batches
        // Batch 1: schemas 1,2,3 (15 tables)
        // Batch 2: schemas 4,5,6 (15 tables)  
        // Batch 3: schemas 7,8,9 (15 tables)
        // Batch 4: schema 10 (5 tables)
        // Total: 4 access control calls instead of 10 (one per schema)
        assertThat(accessControlCallCount.get()).isEqualTo(4);
        
        cursor.close();
    }
    
    @Test
    public void testSingleSchemaFilterBypassesPagination()
    {
        // Mock dependencies
        Metadata metadata = mock(Metadata.class);
        AccessControl accessControl = mock(AccessControl.class);
        InternalNode currentNode = mock(InternalNode.class);
        SystemTablesConfig config = new SystemTablesConfig();
        config.setJdbcSchemaBatchSize(3);
        
        // Mock single schema result
        Map<SchemaTableName, RelationType> schemaTables = ImmutableMap.of(
                new SchemaTableName("target_schema", "table1"), RelationType.TABLE,
                new SchemaTableName("target_schema", "table2"), RelationType.VIEW
        );
        when(metadata.getRelationTypes(any(), any(QualifiedTablePrefix.class)))
                .thenReturn(schemaTables);
        
        AtomicInteger accessControlCallCount = new AtomicInteger(0);
        when(accessControl.filterTables(any(SecurityContext.class), anyString(), any()))
                .thenAnswer(invocation -> {
                    accessControlCallCount.incrementAndGet();
                    Set<SchemaTableName> tables = invocation.getArgument(2);
                    return tables;
                });
        
        TableJdbcTable tableJdbcTable = new TableJdbcTable(metadata, accessControl, currentNode, config);
        
        // Create constraint that filters to specific schema
        TupleDomain<Integer> constraint = TupleDomain.withColumnDomains(ImmutableMap.of(
                1, io.trino.spi.predicate.Domain.singleValue(VARCHAR, io.airlift.slice.Slices.utf8Slice("target_schema"))
        ));
        
        Session session = mock(Session.class);
        ConnectorSession connectorSession = mock(FullConnectorSession.class);
        when(((FullConnectorSession) connectorSession).getSession()).thenReturn(session);
        
        SystemSplit split = new SystemSplit(
                HostAddress.fromString("localhost:8080"),
                TupleDomain.all(),
                java.util.Optional.of("test_catalog")
        );
        
        RecordCursor cursor = tableJdbcTable.cursor(
                TestingTransactionHandle.create(),
                connectorSession,
                constraint,
                ImmutableSet.of(),
                split
        );
        
        int rowCount = 0;
        while (cursor.advanceNextPosition()) {
            rowCount++;
        }
        
        // Should have 2 tables from the target schema
        assertThat(rowCount).isEqualTo(2);
        
        // Should have exactly 1 access control call (no pagination needed for single schema)
        assertThat(accessControlCallCount.get()).isEqualTo(1);
        
        cursor.close();
    }
    
    @Test
    public void testEmptySchemaListHandling()
    {
        // Mock dependencies
        Metadata metadata = mock(Metadata.class);
        AccessControl accessControl = mock(AccessControl.class);
        InternalNode currentNode = mock(InternalNode.class);
        SystemTablesConfig config = new SystemTablesConfig();
        
        // Mock empty schema list
        when(metadata.listSchemaNames(any(), anyString())).thenReturn(ImmutableList.of());
        
        TableJdbcTable tableJdbcTable = new TableJdbcTable(metadata, accessControl, currentNode, config);
        
        Session session = mock(Session.class);
        ConnectorSession connectorSession = mock(FullConnectorSession.class);
        when(((FullConnectorSession) connectorSession).getSession()).thenReturn(session);
        
        SystemSplit split = new SystemSplit(
                HostAddress.fromString("localhost:8080"),
                TupleDomain.all(),
                java.util.Optional.of("test_catalog")
        );
        
        RecordCursor cursor = tableJdbcTable.cursor(
                TestingTransactionHandle.create(),
                connectorSession,
                TupleDomain.all(),
                ImmutableSet.of(),
                split
        );
        
        // Should have no rows
        assertThat(cursor.advanceNextPosition()).isFalse();
        
        // Should have no access control calls
        verify(accessControl, times(0)).filterTables(any(), anyString(), any());
        
        cursor.close();
    }
    
    @Test
    public void testConfigurableBatchSize()
    {
        // Test with different batch sizes
        for (int batchSize : ImmutableList.of(1, 2, 5, 10)) {
            SystemTablesConfig config = new SystemTablesConfig();
            config.setJdbcSchemaBatchSize(batchSize);
            
            assertThat(config.getJdbcSchemaBatchSize()).isEqualTo(batchSize);
        }
    }
}