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

## Fast Metadata Query (`DESC TABLE EXTENDED ... STATIC ONLY`)

`GetColumnsExtended` runs `DESC TABLE EXTENDED <table> AS JSON` to fetch column +
key metadata in a single round-trip. Runtime PR #198486 added a `STATIC ONLY`
modifier to that command which makes the server return catalog metadata only
(no Delta log access, no Mesa RPCs, no other expensive I/O). When opted in via
`adbc.databricks.enable_fast_metadata_query`, the driver emits the new modifier
**and** pairs it with the protocol-specific off-WLM routing signal so the
fast-metadata path takes effect end-to-end:

| Protocol | SQL emitted | Off-WLM signal | Where |
|---|---|---|---|
| SEA | `DESC TABLE EXTENDED <t> STATIC ONLY AS JSON` | HTTP header `x-databricks-sea-can-run-fully-sync: true` | Header is unconditionally set on metadata calls via `ExecuteMetadataSqlAsync` → `IsMetadata=true` → `StatementExecutionClient.cs:225`. SEA always targets a warehouse, so the flag alone gates the SQL change. |
| Thrift | `DESC TABLE EXTENDED <t> STATIC ONLY AS JSON` | `TExecuteStatementReq.RunAsync = false` on the descStmt | `DatabricksStatement.GetColumnsExtendedAsync` flips both when `adbc.databricks.enable_fast_metadata_query=true` AND the connection path matches `/sql/1.0/(warehouses\|endpoints)/{id}` (general clusters: flag is ignored). |

Both signals together are required:

- `STATIC ONLY` without off-WLM routing → server uses the lightweight metadata
  path but the request is still queued through WLM.
- Off-WLM routing without `STATIC ONLY` → request bypasses WLM but the server
  still does the full metadata scan.

### Fallback safety

`STATIC ONLY` requires `AS JSON` per the runtime grammar; older servers without
PR #198486 reject the new keyword with parse error `INVALID_STATIC_ONLY_USAGE`
(SQL state `42601`). The existing `catch (HiveServer2Exception ex) when
(ex.SqlState == "42601" || ex.SqlState == "20000")` in
`DatabricksStatement.GetColumnsExtendedAsync` already handles this and falls back
to the base `GetColumns + GetPrimaryKeys + GetCrossReference` implementation.
