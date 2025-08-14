# Schema-Level Pagination for `system.jdbc.tables`

## Overview

This implementation introduces **schema-level pagination** to the `system.jdbc.tables` query processing in Trino. The goal is to significantly reduce the number of requests made to external access control systems like OPA (Open Policy Agent) when users query metadata through JDBC.

## Problem Statement

### Before: High OPA Load
- When querying `system.jdbc.tables` without schema filters, Trino would:
  1. List all schemas in the catalog
  2. For each schema individually, list all tables  
  3. Send each schema's tables to OPA for access control filtering
  4. Result: **N OPA requests** for N schemas

### Example Impact
- **100 schemas** → **100 OPA requests**
- **500 schemas** → **500 OPA requests**
- Each request adds latency and load to the OPA service

## Solution: Schema-Level Pagination

### After: Batched Processing
- Group schemas into configurable batches
- Process multiple schemas together in each batch
- Make a single OPA request per batch instead of per schema
- Result: **B OPA requests** for N schemas (where B = ⌈N/batch_size⌉)

### Performance Improvement
| Schemas | Batch Size | OPA Requests | Reduction |
|---------|------------|--------------|-----------|
| 100     | 10         | 10           | 90%       |
| 500     | 25         | 20           | 96%       |
| 1000    | 50         | 20           | 98%       |

## Implementation Details

### 1. Configuration
- **Property**: `system.jdbc.schema-batch-size`  
- **Default**: `5`
- **Range**: `1` to `∞`
- **Location**: `etc/config.properties`

```properties
# Process 10 schemas per batch (reduces OPA calls by ~90% for 100 schemas)
system.jdbc.schema-batch-size=10
```

### 2. Core Logic Changes

#### Modified Files
1. **`core/trino-main/src/main/java/io/trino/connector/system/jdbc/TableJdbcTable.java`**
   - Added schema batching logic
   - Configurable batch size injection
   - Smart pagination that bypasses batching for single schema queries

2. **`core/trino-main/src/main/java/io/trino/connector/system/SystemTablesConfig.java`** *(NEW)*
   - Configuration class for system tables properties
   - Validates batch size constraints

3. **`core/trino-main/src/main/java/io/trino/connector/system/SystemConnectorModule.java`**
   - Registers the configuration class with Guice dependency injection

### 3. Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SCHEMA-LEVEL PAGINATION FLOW                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. Query: SELECT * FROM system.jdbc.tables                        │
│                         ↓                                           │
│  2. List all schemas in catalog                                     │
│                         ↓                                           │
│  3. Partition schemas into batches                                  │
│     [schema1, schema2, schema3] [schema4, schema5] [schema6]        │
│                         ↓                                           │
│  4. For each batch:                                                 │
│     - Collect all tables from batch schemas                        │
│     - Send combined table set to OPA                               │
│     - Apply filtering results                                       │
│                         ↓                                           │
│  5. Return filtered results                                         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 4. Key Implementation Features

#### Smart Query Optimization
- **Single Schema Filter**: Bypasses pagination entirely for optimal performance
  ```sql
  SELECT * FROM system.jdbc.tables WHERE table_schem = 'specific_schema'
  ```

- **Multiple Schema Query**: Uses pagination to reduce OPA load
  ```sql  
  SELECT * FROM system.jdbc.tables  -- All schemas
  ```

#### Batch Processing Algorithm
```java
private List<List<String>> partitionSchemas(List<String> schemas, int batchSize) {
    List<List<String>> batches = new ArrayList<>();
    for (int i = 0; i < schemas.size(); i += batchSize) {
        int end = Math.min(i + batchSize, schemas.size());
        batches.add(ImmutableList.copyOf(schemas.subList(i, end)));
    }
    return batches;
}
```

#### Access Control Integration
- Collects tables from multiple schemas in each batch
- Makes a single `AccessControl.filterTables()` call per batch
- Preserves all existing security semantics
- Compatible with both regular and batch OPA implementations

## Benefits

### 1. **Dramatic OPA Load Reduction**
- **90%+ reduction** in OPA requests for typical deployments
- **96%+ reduction** for large deployments (500+ schemas)

### 2. **Improved Query Performance**
- Fewer network round-trips to OPA
- Reduced OPA service load
- Better overall system responsiveness

### 3. **Configurable and Safe**
- Tunable batch size for different deployment sizes
- Smart bypassing for single-schema queries
- No impact on security model or access control semantics

### 4. **Backward Compatible**
- No changes to existing APIs
- No impact on non-paginated queries
- Transparent to end users

## Usage Examples

### Configuration Tuning

```properties
# Small deployment (< 50 schemas)
system.jdbc.schema-batch-size=5

# Medium deployment (50-200 schemas)  
system.jdbc.schema-batch-size=10

# Large deployment (200+ schemas)
system.jdbc.schema-batch-size=25
```

### Monitoring Impact

You can monitor the effectiveness by observing:
1. **OPA request logs** - Should see fewer but larger filter requests
2. **Query performance** - `system.jdbc.tables` queries should be faster
3. **OPA service metrics** - Reduced load and improved response times

## Testing

The implementation includes comprehensive tests:

- **Unit Tests**: Validate batch partitioning logic
- **Integration Tests**: Verify OPA integration and access control
- **Performance Tests**: Measure reduction in access control calls
- **Edge Case Tests**: Handle empty schemas, single schema filters, etc.

## Future Enhancements

1. **Dynamic Batch Sizing**: Adjust batch size based on OPA response times
2. **Caching**: Cache filtered results for short periods
3. **Metrics**: Add detailed metrics for monitoring pagination effectiveness
4. **Other System Tables**: Apply similar pagination to other metadata tables

## Migration Notes

- **Zero downtime deployment** - Feature is additive only
- **Default behavior preserved** - Existing queries work unchanged  
- **Gradual rollout recommended** - Start with conservative batch sizes
- **Monitor OPA service** - Watch for any unexpected load patterns

---

**Implementation Status**: ✅ Complete  
**Testing Status**: ✅ Validated  
**Documentation Status**: ✅ Complete  
**Ready for Production**: ✅ Yes