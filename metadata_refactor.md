# ADBC C# Metadata Refactoring Plan

## Overview

Refactor ADBC C# metadata implementation to eliminate ~65% code duplication between HiveServer2 (Thrift) and StatementExecution API (SEA) by creating shared abstractions for type mapping, schema construction, and field synthesis across **ALL metadata calls** (GetColumns, GetPrimaryKeys, GetCrossReference, etc.).

## Background

### Current State
- **HiveServer2 (Thrift)**: Uses `ColumnTypeId` enum, `SqlTypeNameParser`, and `GetColumnSchema()` to populate metadata from Thrift protocol calls
- **SEA (REST)**: Uses `DatabricksTypeMapper` static methods and inline array builders to populate metadata from SQL commands
- **Duplication**: ~1450 LOC duplicated across type mapping, schema construction, pattern handling, and PK/FK validation

### How Current Implementations Share Values

**Thrift Pattern** (GetObjects vs Statement-based):
```
GetObjects (hierarchical):
  Connection.GetColumnsAsync() → TRowSet
  → SetPrecisionScaleAndTypeName() → TableInfo
  → GetColumnSchema(TableInfo) → Hierarchical StructArray

Statement-based (flat):
  Connection.GetColumnsAsync() → TRowSet  ← SAME Thrift call!
  → Create flat arrays with 24 columns
  → EnhanceGetColumnsResult():
      For each row: SetPrecisionScaleAndTypeName()  ← SAME logic!
      Replaces values with parsed precision/scale
  → Flat statement-based structure
```

**Key Insight**: Both use the **same** underlying Thrift calls and **same** `SetPrecisionScaleAndTypeName()` method. The only difference is output format (hierarchical vs flat).

**SEA Pattern** (GetObjects vs Statement-based):
```
GetObjects (hierarchical):
  SQL: SHOW COLUMNS, DESC TABLE EXTENDED
  → Parse JSON → DatabricksTypeMapper.GetXdbcDataType()
  → Build hierarchical structure

Statement-based (flat):
  GetColumnsFlat():
    Same SQL execution
    → DatabricksTypeMapper.GetXdbcDataType()  ← SAME logic!
    → Transform to uppercase column names
    → Flat statement-based structure
```

**The duplication**: `DatabricksTypeMapper` reimplements the same type mapping that `ColumnTypeId` + `SetPrecisionScaleAndTypeName()` already do in Thrift!

### Problem
- Type mapping changes require updates in 4+ locations
- Schema construction logic duplicated between protocols
- Difficult to maintain consistency (xdbc_* field values must match ADBC spec)
- No shared source of truth for common constants like `ColumnTypeId`
- **PK/FK metadata also duplicated** across Thrift and SEA

## Goals

1. **Single source of truth** for type mappings and XDBC field synthesis
2. **Zero changes** to Thrift output (used by Apache Spark/Impala drivers)
3. **Enable code reuse** between Thrift and SEA for common functionality
4. **Cover ALL metadata calls** - GetColumns, GetPrimaryKeys, GetCrossReference, GetImportedKeys, GetTableTypes, etc.
5. **Extension points** for Databricks-specific enhancements in databricks repo
6. **Maintainability** - type changes in one place, not four

## Architecture Design

### Core Abstractions

```
arrow-adbc/csharp/src/Drivers/Apache/Hive2/Metadata/ (NEW)
├── ColumnTypeMapper.cs              - Unified type-to-XDBC mapping
├── ColumnMetadataRecord.cs          - Common 24-field column data model
├── TableMetadataRecord.cs           - Common table data model (10 fields)
├── SchemaMetadataRecord.cs          - Common schema data model (2 fields)
├── CatalogMetadataRecord.cs         - Common catalog data model (1 field)
├── PrimaryKeyMetadataRecord.cs      - Common PK data model (6 fields)
├── ForeignKeyMetadataRecord.cs      - Common FK data model (14 fields)
├── MetadataFieldPopulator.cs        - Abstract field synthesis
├── MetadataSchemaBuilder.cs         - Arrow array construction for ALL metadata types
└── MetadataPatternConverter.cs      - Pattern utilities
```

### Key Classes

#### 1. ColumnTypeMapper
**Purpose**: Single source of truth replacing `ColumnTypeId` enum usage and `DatabricksTypeMapper`

**Core Methods**:
- `GetBaseTypeName(string typeName)` - Extract base type: "DECIMAL(10,2)" → "DECIMAL"
- `GetXdbcDataType(string typeName)` - Map to JDBC type code: "INTEGER" → 4
- `GetColumnSize(string typeName)` - Calculate COLUMN_SIZE using `SqlTypeNameParser`
- `GetDecimalDigits(string typeName)` - Calculate scale/precision
- `GetNumPrecRadix(string typeName)` - Always 10 for numeric types
- Additional: `GetBufferLength`, `GetCharOctetLength`, `GetSqlDataType`, `GetSqlDatetimeSub`

**Reuses**: Existing `SqlTypeNameParser` from HiveServer2Connection for type parsing

#### 2. ColumnMetadataRecord
**Purpose**: Protocol-agnostic data model for 24 column metadata fields (23 standard + BASE_TYPE_NAME extension)

**Fields**:
- Identity: `CatalogName`, `SchemaName`, `TableName`, `ColumnName`
- Type info: `TypeName`, `XdbcTypeName`, `XdbcDataType`, `BaseTypeName` (extension)
- Size/precision: `XdbcColumnSize`, `XdbcDecimalDigits`, `XdbcNumPrecRadix`
- Nullability: `Nullable`, `IsNullable`
- Additional: `ColumnDefault`, `OrdinalPosition`, `IsAutoIncrement`
- Scope fields (always null): `ScopeCatalog`, `ScopeSchema`, `ScopeTable`
- Custom: `CustomProperties` dictionary for Databricks extensions

**Note**: `BaseTypeName` is the 24th field - a Databricks/Spark extension that extracts the base type without parameters (e.g., "DECIMAL" from "DECIMAL(10,2)").

#### 3. Simple Metadata Records
**Purpose**: Protocol-agnostic data models for catalog/schema/table metadata

**CatalogMetadataRecord** (1 field):
- `CatalogName` (TABLE_CAT)

**SchemaMetadataRecord** (2 fields):
- `CatalogName` (TABLE_CATALOG)
- `SchemaName` (TABLE_SCHEM)

**TableMetadataRecord** (10 fields):
- Identity: `CatalogName`, `SchemaName`, `TableName`, `TableType`
- Additional: `Remarks`, `TypeCatalog`, `TypeSchema`, `TypeName`, `SelfReferencingColName`, `RefGeneration`

#### 4. PrimaryKeyMetadataRecord & ForeignKeyMetadataRecord
**Purpose**: Protocol-agnostic data models for PK/FK metadata

**PrimaryKeyMetadataRecord** (6 fields):
- `CatalogName` (TABLE_CAT)
- `SchemaName` (TABLE_SCHEM)
- `TableName` (TABLE_NAME)
- `ColumnName` (COLUMN_NAME)
- `KeySequence` (KEQ_SEQ) - Position within PK
- `PrimaryKeyName` (PK_NAME)

**ForeignKeyMetadataRecord** (14 fields):
- PK info: `PkCatalogName`, `PkSchemaName`, `PkTableName`, `PkColumnName`
- FK info: `FkCatalogName`, `FkSchemaName`, `FkTableName`, `FkColumnName`
- Metadata: `KeySequence`, `UpdateRule`, `DeleteRule`, `FkName`, `PkName`, `Deferrability`

#### 5. MetadataFieldPopulator
**Purpose**: Abstract base class that orchestrates field population for ALL metadata types

**Main Methods**:
- `PopulateCatalogMetadata(...)` - For GetCatalogs
- `PopulateSchemaMetadata(...)` - For GetSchemas
- `PopulateTableMetadata(...)` - For GetTables
- `PopulateColumnMetadata(...)` - For GetColumns
- `PopulatePrimaryKeyMetadata(...)` - For GetPrimaryKeys
- `PopulateForeignKeyMetadata(...)` - For GetCrossReference/GetImportedKeys

**Virtual extension points**:
- `PopulateCustomFields(record, customData)` - Override in databricks repo for Delta-specific fields
- `GetColumnSize(typeName)` - Override for custom type handling

#### 6. MetadataSchemaBuilder
**Purpose**: Builds Arrow arrays from metadata record collections for ALL metadata types

**Methods**:

**For Statement-Based Flat Metadata** (statement-based):
- `BuildFlatCatalogsSchema(IEnumerable<CatalogMetadataRecord>)` - 1-column (TABLE_CAT)
- `BuildFlatSchemasSchema(IEnumerable<SchemaMetadataRecord>)` - 2-column (TABLE_CATALOG, TABLE_SCHEM)
- `BuildFlatTablesSchema(IEnumerable<TableMetadataRecord>)` - 10-column (TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, ...)
- `BuildFlatColumnsSchema(IEnumerable<ColumnMetadataRecord>)` - 24-column (23 standard + BASE_TYPE_NAME extension)
- `BuildFlatPrimaryKeysSchema(IEnumerable<PrimaryKeyMetadataRecord>)` - 6-column
- `BuildFlatForeignKeysSchema(IEnumerable<ForeignKeyMetadataRecord>)` - 14-column

**For GetObjects Hierarchical Metadata** (nested structures):
- `BuildColumnsStructArray(IEnumerable<ColumnMetadataRecord>)` - Builds ONLY the `table_columns` StructArray portion (19 fields per ADBC spec)
  - **Important**: This builds the leaf-level column metadata, NOT the entire catalog→schema→table→column hierarchy
  - Used within GetObjects to populate the `table_columns` field of each table
  - The full hierarchy assembly happens in GetObjects itself via nested dictionaries and ListArrays

**Replaces**:
- Thrift: `GetColumnSchema()` method (lines 1262-1344 in HiveServer2Connection.cs) → `BuildColumnsStructArray()`
- Thrift: Statement-based array builders in HiveServer2Statement (GetCatalogs, GetSchemas, GetTables, GetColumns, GetPrimaryKeys, GetCrossReference) → `BuildFlat*Schema()` methods
- SEA: ALL `Get*Flat` methods in StatementExecutionConnection (lines 1696-1730, 1836-1880, 2040-2120, 2180-2330, 3009-3100, 3360-3500) → `BuildFlat*Schema()` methods

**Architectural Note**:
- **GetObjects** builds hierarchy through nesting (catalog → List<schema> → List<table> → List<column>), NOT by calling flat builders
- **Statement-based queries** use flat builders directly to return flat statement-based arrays
- Only the column metadata portion (`BuildColumnsStructArray()`) is shared between both approaches

## Data Flow

### HiveServer2 (Thrift) Flow - ALL Metadata

**GetCatalogs/GetSchemas/GetTables (Statement-Based Flat)**:
```
1. Execute Thrift calls → TGetCatalogsResp / TGetSchemasResp / TGetTablesResp
2. Extract raw data: catalogName, schemaName, tableName, tableType, remarks
3. For each row: MetadataFieldPopulator.PopulateCatalogMetadata() or PopulateSchemaMetadata() or PopulateTableMetadata()
4. MetadataSchemaBuilder.BuildFlatCatalogsSchema() or BuildFlatSchemasSchema() or BuildFlatTablesSchema()
5. Return flat statement-based structure (1/2/10 columns) ✓
```

**GetObjects (Hierarchical)** - Custom nesting logic:
```
1. Execute Thrift calls → TGetCatalogsResp, TGetSchemasResp, TGetTablesResp, TGetColumnsResp
2. Build intermediate structure:
   catalogMap = Dictionary<catalog, Dictionary<schema, Dictionary<table, List<ColumnMetadataRecord>>>>
3. Populate catalogMap:
   - For catalogs: Extract catalog names
   - For schemas: Extract catalog+schema pairs
   - For tables: Extract catalog+schema+table+tableType tuples
   - For columns: MetadataFieldPopulator.PopulateColumnMetadata()
     → ColumnTypeMapper synthesizes all xdbc_* fields
     → Store in catalogMap[catalog][schema][table]
4. Build nested Arrow structure:
   a. Create catalog-level StringArray
   b. For each catalog → Build ListArray<StructArray> of schemas
      - Each schema has: db_schema_name + ListArray<StructArray> of tables
   c. For each table → Build StructArray containing:
      - table_name, table_type
      - table_columns: MetadataSchemaBuilder.BuildColumnsStructArray(columnRecords) ← 19 fields
      - table_constraints: Build constraint arrays
5. Return nested hierarchy: catalog → schemas → tables → columns ✓
```

**Key Insight**: GetObjects does NOT use flat builders. It builds hierarchy via nested dictionaries, then creates nested ListArray<StructArray> structures. Only the column portion uses a shared builder (`BuildColumnsStructArray()`).

**GetColumns (Statement-Based Flat)**:
```
1. Execute Thrift calls → TGetColumnsResp with TRowSet  ← SAME call as GetObjects!
2. Extract raw data: columnName, typeName, isNullable
3. For each row: MetadataFieldPopulator.PopulateColumnMetadata()
   - ColumnTypeMapper synthesizes all xdbc_* fields  ← SAME mapper as GetObjects!
4. MetadataSchemaBuilder.BuildFlatColumnsSchema(records)
   - Flat format with uppercase column names (TABLE_CAT, TABLE_SCHEM, etc.)
5. Return 24-column flat statement-based structure (23 standard + BASE_TYPE_NAME) ✓
```

**GetPrimaryKeys/GetCrossReference**:
```
1. Execute Thrift calls → TGetPrimaryKeysResp or TGetCrossReferenceResp
2. Extract raw data: catalog, schema, table, column, keySequence
3. For each row: MetadataFieldPopulator.PopulatePrimaryKeyMetadata() or PopulateForeignKeyMetadata()
4. MetadataSchemaBuilder.BuildFlatPrimaryKeysSchema() or BuildFlatForeignKeysSchema()
5. Return flat statement-based structure ✓
```

### SEA Flow - ALL Metadata

**GetCatalogs/GetSchemas/GetTables (Statement-Based)**:
```
1. Execute SQL commands → SHOW CATALOGS, SHOW SCHEMAS, SHOW TABLES
2. Parse results: catalogName, schemaName, tableName, tableType
3. For each row: MetadataFieldPopulator.PopulateCatalogMetadata() or PopulateSchemaMetadata() or PopulateTableMetadata()
   - SAME populators as Thrift
4. MetadataSchemaBuilder.BuildFlatCatalogsSchema() or BuildFlatSchemasSchema() or BuildFlatTablesSchema()
   - SAME builders as Thrift
5. Return flat statement-based structure ✓
```

**GetObjects (Hierarchical)** - Custom nesting logic:
```
1. Execute SQL commands → SHOW CATALOGS, SHOW SCHEMAS, SHOW TABLES, SHOW COLUMNS, DESC TABLE EXTENDED
2. Build intermediate structure:
   catalogMap = Dictionary<catalog, Dictionary<schema, Dictionary<table, List<ColumnMetadataRecord>>>>
3. Populate catalogMap:
   - For catalogs: Parse SHOW CATALOGS results
   - For schemas: Parse SHOW SCHEMAS results
   - For tables: Parse SHOW TABLES results
   - For columns: MetadataFieldPopulator.PopulateColumnMetadata()
     → SAME ColumnTypeMapper as Thrift
     → Store in catalogMap[catalog][schema][table]
4. Build nested Arrow structure (IDENTICAL LOGIC TO THRIFT):
   a. Create catalog-level StringArray
   b. For each catalog → Build ListArray<StructArray> of schemas
   c. For each table → Build StructArray with:
      - table_columns: MetadataSchemaBuilder.BuildColumnsStructArray(columnRecords) ← SAME as Thrift
5. Return nested hierarchy matching Thrift output exactly ✓
```

**Key Insight**: Both Thrift and SEA use the SAME hierarchy assembly pattern (catalogMap + nested ListArrays) and the SAME `BuildColumnsStructArray()` for column metadata.

**GetColumns (Statement-Based Flat)**:
```
1. Execute SQL commands → SHOW COLUMNS, DESC TABLE EXTENDED  ← SAME SQL as GetObjects!
2. Parse results: columnName, typeName (from JSON), isNullable
3. For each row: MetadataFieldPopulator.PopulateColumnMetadata()
   - SAME ColumnTypeMapper as GetObjects  ← Reuses hierarchical path!
4. MetadataSchemaBuilder.BuildFlatColumnsSchema(records)
   - SAME builder as Thrift
5. Return 24-column flat statement-based structure (23 standard + BASE_TYPE_NAME) ✓
```

**GetPrimaryKeys/GetCrossReference**:
```
1. Execute SQL: SHOW KEYS, parse DESC TABLE EXTENDED JSON
2. Extract: catalog, schema, table, column, keySequence
3. For each row: MetadataFieldPopulator.PopulatePrimaryKeyMetadata()
   - SAME populator as Thrift
4. MetadataSchemaBuilder.BuildFlatPrimaryKeysSchema()
   - SAME builder as Thrift
5. Return flat statement-based structure ✓
```

**Benefits**:
- 100% type mapping reuse via ColumnTypeMapper
- 100% flat schema construction reuse for statement-based queries
- GetObjects column metadata (BuildColumnsStructArray) shared between protocols
- Only protocol-specific: SQL/Thrift execution and GetObjects hierarchy assembly pattern (catalogMap + nested ListArrays)

## Implementation Steps

### Phase 1: Foundation (Create Shared Abstractions)

**Step 1.1**: Create `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/Metadata/ColumnTypeMapper.cs`
- Implement all type mapping methods
- Use `ColumnTypeId` enum values internally
- Leverage existing `SqlTypeNameParser` for precision/scale extraction
- Add unit tests for all JDBC type codes

**Step 1.2**: Create metadata record models
- `CatalogMetadataRecord.cs` - 1-field struct for catalog metadata
- `SchemaMetadataRecord.cs` - 2-field struct for schema metadata
- `TableMetadataRecord.cs` - 10-field struct for table metadata
- `ColumnMetadataRecord.cs` - 24-field struct (23 standard + BASE_TYPE_NAME extension)
- `PrimaryKeyMetadataRecord.cs` - 6-field struct for PK metadata
- `ForeignKeyMetadataRecord.cs` - 14-field struct for FK metadata
- Add validation methods

**Step 1.3**: Create `MetadataFieldPopulator.cs`
- Implement `PopulateCatalogMetadata()` - Simple, just wraps catalog name
- Implement `PopulateSchemaMetadata()` - Simple, wraps catalog + schema
- Implement `PopulateTableMetadata()` - Wraps table info with 10 fields
- Implement `PopulateColumnMetadata()` orchestrator with ColumnTypeMapper
- Implement `PopulatePrimaryKeyMetadata()` for PK
- Implement `PopulateForeignKeyMetadata()` for FK/cross-reference
- Wire in `ColumnTypeMapper` dependency
- Add virtual extension points

**Step 1.4**: Create `MetadataSchemaBuilder.cs`

**For Statement-Based Flat Builders**:
- Add `BuildFlatCatalogsSchema()` - 1 column (TABLE_CAT)
- Add `BuildFlatSchemasSchema()` - 2 columns (TABLE_CATALOG, TABLE_SCHEM)
- Add `BuildFlatTablesSchema()` - 10 columns (TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, ...)
- Add `BuildFlatColumnsSchema()` - 24 columns (23 standard + BASE_TYPE_NAME)
- Add `BuildFlatPrimaryKeysSchema()` - 6 columns
- Add `BuildFlatForeignKeysSchema()` - 14 columns

**For GetObjects Hierarchical Builder**:
- Extract `GetColumnSchema()` logic from HiveServer2Connection (lines 1262-1344)
- Rename to `BuildColumnsStructArray()` - Builds ONLY the table_columns StructArray (19 fields per ADBC spec)
- Add clear documentation that this builds the leaf-level column metadata, NOT the full hierarchy
- The full catalog→schema→table→column nesting happens in GetObjects itself

**Important Architectural Note**:
- GetObjects does NOT use flat builders - it builds hierarchy via nested dictionaries and ListArrays
- Only `BuildColumnsStructArray()` is shared between GetObjects and statement-based queries
- Flat builders are ONLY for statement-based metadata calls

**Validation**: All unit tests pass, no production code changes yet

### Phase 2: Thrift Integration (Preserve Existing Behavior)

**Step 2.1**: Refactor `SetPrecisionScaleAndTypeName()` in `/arrow-adbc/csharp/src/Drivers/Apache/Spark/SparkConnection.cs` (lines 70-114)
```csharp
// BEFORE: Duplicated switch statement
case (short)ColumnTypeId.DECIMAL:
    SqlDecimalParserResult result = SqlTypeNameParser<SqlDecimalParserResult>.Parse(...);
    tableInfo?.Precision.Add(result.Precision);

// AFTER: Use ColumnTypeMapper
var mapper = new ColumnTypeMapper();
tableInfo?.BaseTypeName.Add(mapper.GetBaseTypeName(typeName));
tableInfo?.Precision.Add(mapper.GetColumnSize(typeName));
```

**Step 2.2**: Update `GetObjects()` in `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/HiveServer2Connection.cs` (lines 500-691)
- Use `MetadataFieldPopulator` to create `ColumnMetadataRecord` instances
- Replace `GetColumnSchema()` call with `MetadataSchemaBuilder.BuildColumnsStructArray()`
- **Important**: Keep the existing catalogMap dictionary and nested ListArray logic unchanged
- Only replace the column metadata array building portion

**Step 2.3**: Update `EnhanceGetColumnsResult()` in `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/HiveServer2Statement.cs` (lines 573-648)
```csharp
// BEFORE: Calls Connection.SetPrecisionScaleAndTypeName() for each row
Connection.SetPrecisionScaleAndTypeName(colType, typeName, tableInfo, columnSize, decimalDigits);

// AFTER: Use MetadataFieldPopulator
var record = populator.PopulateColumnMetadata(catalog, schema, table, column, typeName, isNullable, ...);
baseTypeNames.Add(record.XdbcTypeName);
columnSizeValues.Add(record.XdbcColumnSize);
```

**Step 2.4**: Update simple metadata statement methods in `HiveServer2Statement.cs` (lines 492-520)
- `GetCatalogsAsync()` - Use `MetadataSchemaBuilder.BuildFlatCatalogsSchema()`
- `GetSchemasAsync()` - Use `MetadataSchemaBuilder.BuildFlatSchemasSchema()`
- `GetTablesAsync()` - Use `MetadataSchemaBuilder.BuildFlatTablesSchema()`

**Step 2.5**: Update PK/FK statement methods in `HiveServer2Statement.cs`
- `GetPrimaryKeysAsync()` - Use `MetadataFieldPopulator.PopulatePrimaryKeyMetadata()`
- `GetCrossReferenceAsync()` - Use `MetadataFieldPopulator.PopulateForeignKeyMetadata()`
- Use `MetadataSchemaBuilder` for array construction

**Validation**:
- Byte-level comparison: old output == new output for ALL metadata calls
- Full Spark/Hive2 test suite passes
- No performance regression (±10%)

### Phase 3: SEA Integration (Eliminate Duplication)

**Step 3.1**: Update column metadata in `/databricks/csharp/src/StatementExecution/StatementExecutionConnection.cs`
- Replace direct `DatabricksTypeMapper` calls with `ColumnTypeMapper`
- Use `MetadataFieldPopulator.PopulateColumnMetadata()` in GetObjects implementation
- Use `MetadataSchemaBuilder` for schema construction

**Step 3.2**: Update ALL `Get*Flat` methods in `StatementExecutionConnection.cs`
- `GetCatalogsFlat()` (lines 1696-1730) - Use `MetadataSchemaBuilder.BuildFlatCatalogsSchema()`
- `GetSchemasFlat()` (lines 1836-1880) - Use `MetadataSchemaBuilder.BuildFlatSchemasSchema()`
- `GetTablesFlat()` (lines 2040-2120) - Use `MetadataSchemaBuilder.BuildFlatTablesSchema()`
- `GetColumnsFlat()` (lines 2180-2330) - Use `MetadataSchemaBuilder.BuildFlatColumnsSchema()`
- `GetPrimaryKeysFlat()` (lines 3009-3100) - Use `MetadataSchemaBuilder.BuildFlatPrimaryKeysSchema()`
- `GetCrossReferenceFlat()` (lines 3360-3500) - Use `MetadataSchemaBuilder.BuildFlatForeignKeysSchema()`
- Remove ALL inline array builder code (~600 LOC reduction!)

**Step 3.3**: Deprecate `/databricks/csharp/src/DatabricksTypeMapper.cs`
```csharp
[Obsolete("Use ColumnTypeMapper from Apache.Arrow.Adbc.Drivers.Apache.Hive2.Metadata")]
internal static class DatabricksTypeMapper { ... }
```
- Keep for 1 release cycle
- Add migration guide in XML comments

**Step 3.4**: Extract pattern utilities from `/databricks/csharp/src/MetadataUtilities.cs`
- Move to `MetadataPatternConverter` for shared use
- Keep catalog normalization methods as-is (Databricks-specific)

**Validation**:
- Field-by-field comparison: old SEA output == new SEA output for ALL metadata calls
- All Databricks E2E tests pass
- Type mapping consistency: Thrift xdbc_data_type == SEA xdbc_data_type for all types
- PK/FK metadata matches exactly between Thrift and SEA

### Phase 4: Extension Points (Enable Databricks Customization)

**Step 4.1**: Create example override in databricks repo
```csharp
// databricks/csharp/src/DatabricksMetadataFieldPopulator.cs
internal class DatabricksMetadataFieldPopulator : MetadataFieldPopulator
{
    protected override void PopulateCustomFields(ColumnMetadataRecord record, object? customData)
    {
        if (customData is DescTableExtendedResult.ColumnInfo columnInfo)
        {
            record.CustomProperties["DELTA_GENERATION_EXPRESSION"] = columnInfo.GenerationExpression;
            // Add other Delta-specific fields
        }
    }
}
```

**Step 4.2**: Document extension mechanism in XML comments

### Phase 5: Testing & Validation

**Step 5.1**: Comparison tests for ALL metadata types
- Create `MetadataOutputComparisonTests.cs` in test suite
- Byte-level equivalence for Thrift GetObjects output
- Field-by-field validation for SEA output
- Type mapping consistency tests (Thrift == SEA for all 30+ types)
- **PK/FK comparison**: Validate GetPrimaryKeys and GetCrossReference output matches exactly

**Step 5.2**: Integration tests
- End-to-end against live Spark/Databricks clusters
- All GetObjects depths (Catalogs, DbSchemas, Tables, All)
- Pattern matching (%, _, exact identifiers)
- PK/FK extraction from DESC TABLE EXTENDED
- **ALL statement-based metadata calls**:
  - GetCatalogs (1 column)
  - GetSchemas (2 columns)
  - GetTables (10 columns)
  - GetColumns (24 columns: 23 standard + BASE_TYPE_NAME)
  - GetPrimaryKeys (6 columns)
  - GetCrossReference (14 columns)
  - GetTableTypes (1 column)

**Step 5.3**: Performance benchmarks
- GetObjects(100 tables): baseline vs refactored (target: ±10%)
- GetCatalogs/GetSchemas/GetTables statement queries: baseline vs refactored
- GetColumns statement query: baseline vs refactored
- GetPrimaryKeys/GetCrossReference: baseline vs refactored
- Memory usage comparison
- Type mapping overhead measurement
- Array builder overhead for simple metadata

## Critical Files

### Files to Create (in arrow-adbc repo)
1. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/Metadata/ColumnTypeMapper.cs` - Type mapping logic
2. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/Metadata/CatalogMetadataRecord.cs` - Catalog data model (1 field)
3. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/Metadata/SchemaMetadataRecord.cs` - Schema data model (2 fields)
4. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/Metadata/TableMetadataRecord.cs` - Table data model (10 fields)
5. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/Metadata/ColumnMetadataRecord.cs` - Column data model (24 fields: 23 standard + BASE_TYPE_NAME)
6. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/Metadata/PrimaryKeyMetadataRecord.cs` - PK data model (6 fields)
7. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/Metadata/ForeignKeyMetadataRecord.cs` - FK data model (14 fields)
8. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/Metadata/MetadataFieldPopulator.cs` - Field synthesis for ALL metadata types
9. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/Metadata/MetadataSchemaBuilder.cs` - Array construction (flat builders + BuildColumnsStructArray)
10. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/Metadata/MetadataPatternConverter.cs` - Pattern utilities

### Files to Modify (Thrift - arrow-adbc repo)
1. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/HiveServer2Connection.cs` (lines 500-691, 1262-1344)
   - Integrate `MetadataFieldPopulator` in GetObjects for column metadata
   - Replace `GetColumnSchema()` (lines 1262-1344) with `MetadataSchemaBuilder.BuildColumnsStructArray()`
   - **Critical**: Preserve existing catalogMap dictionary and nested ListArray hierarchy assembly logic

2. `/arrow-adbc/csharp/src/Drivers/Apache/Spark/SparkConnection.cs` (lines 70-114)
   - Replace `SetPrecisionScaleAndTypeName()` switch with `ColumnTypeMapper`

3. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/HiveServer2Statement.cs`
   - Replace `EnhanceGetColumnsResult()` (lines 573-648) with `MetadataFieldPopulator` + `BuildFlatColumnsSchema()`
   - Update `GetCatalogsAsync()` (line 492-497) to use `BuildFlatCatalogsSchema()`
   - Update `GetSchemasAsync()` (lines 499-507) to use `BuildFlatSchemasSchema()`
   - Update `GetTablesAsync()` (lines 509-520) to use `BuildFlatTablesSchema()`
   - Update `GetPrimaryKeysAsync()` (lines 481-490) to use `BuildFlatPrimaryKeysSchema()`
   - Update `GetCrossReferenceAsync()` (lines 462-476) to use `BuildFlatForeignKeysSchema()`

### Files to Modify (SEA - databricks repo)
1. `/databricks/csharp/src/StatementExecution/StatementExecutionConnection.cs`
   - Replace `DatabricksTypeMapper` with `ColumnTypeMapper`
   - Update `GetObjects()` implementation:
     - Use `MetadataFieldPopulator` for column metadata
     - Replace inline `BuildColumnsStructArray` logic with `MetadataSchemaBuilder.BuildColumnsStructArray()`
     - **Important**: Keep existing catalogMap and nesting logic unchanged
   - Update ALL `Get*Flat()` methods - replace inline array builders with shared builders:
     - `GetCatalogsFlat()` (lines 1696-1730) → Use `BuildFlatCatalogsSchema()`
     - `GetSchemasFlat()` (lines 1836-1880) → Use `BuildFlatSchemasSchema()`
     - `GetTablesFlat()` (lines 2040-2120) → Use `BuildFlatTablesSchema()`
     - `GetColumnsFlat()` (lines 2180-2330) → Use `BuildFlatColumnsSchema()`
     - `GetPrimaryKeysFlat()` (lines 3009-3100) → Use `BuildFlatPrimaryKeysSchema()`
     - `GetCrossReferenceFlat()` (lines 3360-3500) → Use `BuildFlatForeignKeysSchema()`

2. `/databricks/csharp/src/StatementExecution/StatementExecutionStatement.cs`
   - Update metadata command routing to use shared builders
   - No changes to SQL execution logic

3. `/databricks/csharp/src/DatabricksTypeMapper.cs`
   - Mark as `[Obsolete]` with migration guide

4. `/databricks/csharp/src/MetadataUtilities.cs`
   - Extract pattern conversion to shared `MetadataPatternConverter`
   - Keep Databricks-specific methods (catalog normalization, PK/FK validation)

### Files to Reference (Reuse Existing)
1. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/HiveServer2Connection.cs` (lines 119-283)
   - `ColumnTypeId` enum - reused internally by `ColumnTypeMapper`

2. `/arrow-adbc/csharp/src/Drivers/Apache/Hive2/SqlTypeNameParser.cs`
   - Reused for DECIMAL(p,s), VARCHAR(n) parsing

3. `/databricks/csharp/src/StatementExecution/SqlCommandBuilder.cs`
   - No changes - continues building SQL commands

4. `/databricks/csharp/src/ColumnMetadataSchemas.cs`
   - Continue using for 24-column schema definitions

## Benefits

### Code Reduction
- **Type Mapping**: 700 LOC → 300 LOC (57% reduction)
- **Schema Construction (Columns)**: 400 LOC → 150 LOC (62% reduction)
- **Simple Metadata Builders (Catalogs/Schemas/Tables)**: 600 LOC → 150 LOC (75% reduction)
- **Pattern Handling**: 200 LOC → 80 LOC (60% reduction)
- **PK/FK Array Building**: 300 LOC → 100 LOC (67% reduction)
- **Total**: ~2200 LOC → ~780 LOC (65% reduction)

### Maintainability
- **Before**: Type change requires 4 locations (ColumnTypeId, DatabricksTypeMapper, SetPrecisionScaleAndTypeName, inline builders)
- **After**: Type change requires 1 location (ColumnTypeMapper)
- **Time savings**: Bug fix 4 hours → 1 hour; New type 2 days → 4 hours
- **ALL metadata calls** (GetColumns, GetPrimaryKeys, GetCrossReference) now share the same builders

### Consistency
- **Single source of truth** for xdbc_* field values ensures Thrift and SEA always match
- **Shared tests** validate type mapping across protocols
- **Extension points** enable Databricks customization without forking
- **PK/FK consistency**: Both protocols use identical array builders

### Future-Proofing
- Easy to add new types (e.g., GEOGRAPHY, VECTOR) in one place
- Easy to support new JDBC fields in one schema builder
- Clear separation between protocol-specific (SQL/Thrift) and shared logic
- Adding new metadata calls (e.g., GetIndexInfo) requires only one builder implementation

## Risk Mitigation

### Backward Compatibility
**Risk**: Breaking Thrift output breaks Spark/Impala drivers

**Mitigation**:
- Byte-level equivalence testing for ALL metadata calls
- Staged rollout with feature flag
- 3-release deprecation timeline

### Performance
**Risk**: Abstraction introduces overhead

**Mitigation**:
- Benchmark ±10% variance acceptable
- SqlTypeNameParser already has caching
- Profile hotspots and optimize

### Testing
**Risk**: Insufficient validation

**Mitigation**:
- Comparison tests (old == new output) for ALL metadata types
- Full integration test suite against live clusters
- Type mapping consistency validation
- PK/FK metadata consistency validation

## Verification

### Unit Tests
```bash
cd /Users/madhavendra.rathore/Desktop/adbc-databricks/databricks/csharp
dotnet test --filter "FullyQualifiedName~ColumnTypeMapperTests"
dotnet test --filter "FullyQualifiedName~MetadataSchemaBuilderTests"
dotnet test --filter "FullyQualifiedName~CatalogMetadataTests"
dotnet test --filter "FullyQualifiedName~SchemaMetadataTests"
dotnet test --filter "FullyQualifiedName~TableMetadataTests"
dotnet test --filter "FullyQualifiedName~PrimaryKeyMetadataTests"
dotnet test --filter "FullyQualifiedName~ForeignKeyMetadataTests"
```

### Integration Tests
```bash
# Thrift (HiveServer2) - ALL metadata calls
dotnet test --filter "FullyQualifiedName~HiveServer2ConnectionTests.GetObjects"
dotnet test --filter "FullyQualifiedName~HiveServer2StatementTests.GetCatalogs"
dotnet test --filter "FullyQualifiedName~HiveServer2StatementTests.GetSchemas"
dotnet test --filter "FullyQualifiedName~HiveServer2StatementTests.GetTables"
dotnet test --filter "FullyQualifiedName~HiveServer2StatementTests.GetColumns"
dotnet test --filter "FullyQualifiedName~HiveServer2StatementTests.GetPrimaryKeys"
dotnet test --filter "FullyQualifiedName~HiveServer2StatementTests.GetCrossReference"

# SEA (Databricks) - ALL metadata calls
dotnet test --filter "FullyQualifiedName~StatementExecutionConnectionTests.GetObjects"
dotnet test --filter "FullyQualifiedName~StatementExecutionStatementTests.GetCatalogs"
dotnet test --filter "FullyQualifiedName~StatementExecutionStatementTests.GetSchemas"
dotnet test --filter "FullyQualifiedName~StatementExecutionStatementTests.GetTables"
dotnet test --filter "FullyQualifiedName~StatementExecutionStatementTests.GetColumns"
dotnet test --filter "FullyQualifiedName~StatementExecutionStatementTests.GetPrimaryKeys"
dotnet test --filter "FullyQualifiedName~StatementExecutionStatementTests.GetCrossReference"
dotnet test --filter "FullyQualifiedName~DatabricksConnectionTest.MetadataTests"
```

### Comparison Tests
```bash
# Create baseline before refactor
dotnet run --project ComparisonTests -- capture-baseline --all-metadata

# After refactor - compare byte-level for ALL metadata types
dotnet run --project ComparisonTests -- compare-output --all-metadata
```

### Performance Benchmarks
```bash
dotnet run --project BenchmarkTests --configuration Release
# Metrics: GetObjects latency, GetColumns latency, GetPrimaryKeys latency,
#          memory usage, type mapping overhead
```

## Success Criteria

1. ✅ All existing Thrift tests pass (100% compatibility)
2. ✅ All existing SEA tests pass (100% compatibility)
3. ✅ Byte-level equivalence for Thrift GetObjects output
4. ✅ Field-by-field equivalence for SEA GetObjects output
5. ✅ Type mapping consistency: Thrift xdbc_data_type == SEA xdbc_data_type
6. ✅ **PK/FK consistency**: GetPrimaryKeys and GetCrossReference output identical across protocols
7. ✅ **Simple metadata consistency**: GetCatalogs, GetSchemas, GetTables output identical across protocols
8. ✅ **ALL statement metadata calls** produce identical output before/after refactor (7 types total)
9. ✅ Performance within ±10% of baseline for ALL metadata operations
10. ✅ Code reduction ~65% in duplicated areas
11. ✅ Single location for type mapping changes
12. ✅ Single location for schema construction across ALL metadata types

## Appendix: GetObjects Hierarchy Assembly

### How GetObjects Actually Builds the Hierarchy

The GetObjects method builds a nested hierarchy, NOT flat arrays. Here's the actual pattern used in both Thrift and SEA:

#### Step 1: Build Intermediate Dictionary Structure
```csharp
// Accumulate metadata in nested dictionaries
var catalogMap = new Dictionary<string, Dictionary<string, Dictionary<string, List<ColumnMetadataRecord>>>>();

// Populate from Thrift/SQL results:
catalogMap[catalog][schema][table] = new List<ColumnMetadataRecord>();
```

#### Step 2: Build Nested Arrow Schema
```csharp
// ADBC GetObjects Schema (from StandardSchemas.cs):
catalog_name: string
catalog_db_schemas: list<struct<
  db_schema_name: string
  db_schema_tables: list<struct<
    table_name: string
    table_type: string
    table_columns: list<struct<  ← 19 fields here (BuildColumnsStructArray)
      column_name: string
      ordinal_position: int32
      remarks: string
      xdbc_data_type: int16
      ... (15 more fields)
    >>
    table_constraints: list<struct<...>>
  >>
>>
```

#### Step 3: Assemble Nested Structure
```csharp
// For each catalog
var catalogBuilder = new StringArray.Builder();
var catalogDbSchemasBuilder = new ListArray.Builder(dbSchemaStructType);

foreach (var catalog in catalogMap.Keys)
{
    catalogBuilder.Append(catalog);
    
    // For each schema in this catalog
    var schemaStructArrays = new List<StructArray>();
    foreach (var schema in catalogMap[catalog].Keys)
    {
        // For each table in this schema
        var tableStructArrays = new List<StructArray>();
        foreach (var table in catalogMap[catalog][schema].Keys)
        {
            var columnRecords = catalogMap[catalog][schema][table];
            
            // Build column metadata using shared builder
            var columnsArray = MetadataSchemaBuilder.BuildColumnsStructArray(columnRecords);
            
            // Build table struct containing columns
            var tableStruct = new StructArray(
                tableSchema,
                new IArrowArray[] { tableNameArray, tableTypeArray, columnsArray, constraintsArray }
            );
            tableStructArrays.Add(tableStruct);
        }
        
        // Build schema struct containing tables
        var schemaStruct = new StructArray(
            schemaSchema,
            new IArrowArray[] { schemaNameArray, new ListArray(..., tableStructArrays) }
        );
        schemaStructArrays.Add(schemaStruct);
    }
    
    catalogDbSchemasBuilder.Append(new ListArray(..., schemaStructArrays));
}

// Final result: catalog → List<schema> → List<table> → List<column>
return new RecordBatch(schema, new IArrowArray[] { catalogBuilder.Build(), catalogDbSchemasBuilder.Build() });
```

### Key Takeaways

1. **GetObjects uses nested dictionaries** (catalogMap) to accumulate metadata hierarchically
2. **GetObjects builds nested ListArray<StructArray>** structures, NOT flat arrays
3. **Only the column portion** uses a shared builder (`BuildColumnsStructArray()`)
4. **Flat builders are ONLY for statement-based queries** - they return flat statement-based arrays
5. **The hierarchy assembly logic stays in GetObjects** - it's NOT in MetadataSchemaBuilder
