<!--
Copyright (c) 2025 ADBC Drivers Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# SEA Metadata Architecture

## Overview

The Databricks ADBC driver supports two protocols for metadata retrieval:
- **Thrift** (HiveServer2): Uses Thrift RPC calls to fetch metadata from the server
- **SEA** (Statement Execution API): Uses SQL commands (`SHOW CATALOGS`, `SHOW SCHEMAS`, etc.) via the REST API

Both protocols share the same Arrow result structure for `GetObjects`, `GetColumns`, `GetPrimaryKeys`, and other metadata operations. This document describes how shared code is structured to avoid duplication while allowing each protocol to fetch data from its respective backend.

## Shared Interface: IGetObjectsDataProvider

```
AdbcConnection.GetObjects()  [sync ADBC API]
         │
         ▼
GetObjectsResultBuilder.BuildGetObjectsResultAsync()  [async orchestrator]
         │
         ├─ provider.GetCatalogsAsync()
         ├─ provider.GetSchemasAsync()
         ├─ provider.GetTablesAsync()
         └─ provider.PopulateColumnInfoAsync()
         │
         ▼
    BuildResult() → HiveInfoArrowStream  [Arrow structure construction]
```

`IGetObjectsDataProvider` is the abstraction between "how to fetch metadata" and "how to build Arrow results":

- **GetObjectsResultBuilder** knows how to construct the nested Arrow structures (catalog → schema → table → column) required by the ADBC GetObjects spec.
- **IGetObjectsDataProvider** implementations know how to retrieve the raw data from their protocol.

### Thrift Implementation (HiveServer2Connection)
- Calls Thrift RPCs: `GetCatalogsAsync()`, `GetSchemasAsync()`, `GetTablesAsync()`, `GetColumnsAsync()`
- Server returns typed result sets with precision, scale, column size
- `SetPrecisionScaleAndTypeName` override handles per-connection type mapping

### SEA Implementation (StatementExecutionConnection)
- Executes SQL: `SHOW CATALOGS`, `SHOW SCHEMAS IN ...`, `SHOW TABLES IN ...`, `SHOW COLUMNS IN ...`
- Server returns type name strings only — metadata is computed locally via `ColumnMetadataHelper`
- `ColumnMetadataHelper.PopulateTableInfoFromTypeName` derives data type codes, column sizes, decimal digits from type names

## Async Design

The ADBC base class defines `GetObjects()` as synchronous:
```csharp
public abstract IArrowArrayStream GetObjects(GetObjectsDepth depth, ...);
```

Internally, the interface and builder are async:
```csharp
interface IGetObjectsDataProvider {
    Task<IReadOnlyList<string>> GetCatalogsAsync(...);
    // ...
}

static async Task<HiveInfoArrowStream> BuildGetObjectsResultAsync(
    IGetObjectsDataProvider provider, ...) { ... }
```

The sync ADBC boundary blocks once at the top level:
```csharp
public override IArrowArrayStream GetObjects(...) {
    return BuildGetObjectsResultAsync(this, ...).GetAwaiter().GetResult();
}
```

This avoids nested `.Result` blocking calls on every Thrift RPC while maintaining the sync ADBC API contract.

## Shared Schema Factories

`MetadataSchemaFactory` (in hiveserver2) provides schema definitions used by both protocols:

| Factory Method | Used By |
|---|---|
| `CreateCatalogsSchema()` | DatabricksStatement, StatementExecutionStatement |
| `CreateSchemasSchema()` | DatabricksStatement, StatementExecutionStatement |
| `CreateTablesSchema()` | DatabricksStatement, StatementExecutionStatement |
| `CreateColumnMetadataSchema()` | DatabricksStatement, FlatColumnsResultBuilder |
| `CreatePrimaryKeysSchema()` | MetadataSchemaFactory builders |
| `CreateCrossReferenceSchema()` | MetadataSchemaFactory builders |
| `BuildGetInfoResult()` | HiveServer2Connection, StatementExecutionConnection |

## Type Mapping

### Thrift Path
```
Server result → SetPrecisionScaleAndTypeName (per-connection override)
    ├─ SparkConnection: parses DECIMAL/CHAR precision from type name
    └─ HiveServer2ExtendedConnection: uses server-provided values

For flat GetColumns, EnhanceGetColumnsResult (on HiveServer2Statement) adds
a BASE_TYPE_NAME column and optionally overrides precision/scale by calling
SetPrecisionScaleAndTypeName per row. This is Thrift-only — SEA builds the
complete result from scratch via FlatColumnsResultBuilder.
```

### SEA Path
```
SHOW COLUMNS response → ColumnMetadataHelper.PopulateTableInfoFromTypeName
    └─ Computes: data type code, column size, decimal digits, base type name
```

### Shared GetArrowType
`HiveServer2Connection.GetArrowType()` (internal static) converts a column type ID to an Apache Arrow type. Both Thrift and SEA use this — SEA derives the type ID via `ColumnMetadataHelper.GetDataTypeCode()` first.

## SQL Command Builders

SEA metadata uses `MetadataCommandBase` with command subclasses:

| Command | SQL Generated |
|---|---|
| `ShowCatalogsCommand` | `SHOW CATALOGS [LIKE 'pattern']` |
| `ShowSchemasCommand` | `SHOW SCHEMAS IN \`catalog\` [LIKE 'pattern']` |
| `ShowTablesCommand` | `SHOW TABLES IN CATALOG \`catalog\` [SCHEMA LIKE ...] [LIKE ...]` |
| `ShowColumnsCommand` | `SHOW COLUMNS IN CATALOG \`catalog\` [SCHEMA LIKE ...] [TABLE LIKE ...] [LIKE ...]` |
| `ShowKeysCommand` | `SHOW KEYS IN CATALOG ... IN SCHEMA ... IN TABLE ...` |
| `ShowForeignKeysCommand` | `SHOW FOREIGN KEYS IN CATALOG ... IN SCHEMA ... IN TABLE ...` |

Pattern conversion: ADBC `%` → Databricks `*`, ADBC `_` → Databricks `.`

## GetObjects RPC Count

Each IGetObjectsDataProvider method makes one server call. Total RPCs by depth:

| Depth | Methods Called | RPCs |
|---|---|---|
| Catalogs | GetCatalogsAsync | 1 |
| DbSchemas | + GetSchemasAsync | 2 |
| Tables | + GetTablesAsync | 3 |
| All | + PopulateColumnInfoAsync | 4 |
