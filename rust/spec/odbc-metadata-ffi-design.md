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

# Design: Metadata FFI Layer for Databricks Rust Driver

## Context

The Databricks ADBC Rust driver (`rust/`) is used as the core engine for an ODBC wrapper.
The driver has:
- An internal metadata interface (`DatabricksClient` trait + `metadata/` module) that handles
  SHOW SQL commands, result parsing, type mapping, and Arrow building
- An existing ADBC FFI (`ffi` feature) for standard ADBC C API exposure via `adbc_ffi::export_driver!`

The ODBC wrapper needs flat, tabular metadata results (not ADBC's nested `get_objects` hierarchy).
The two FFI layers serve different purposes but **coexist in the same shared library**:
- **ADBC FFI** (`ffi` feature) -- Standard ADBC C API for connection lifecycle, statement
  execution, and basic metadata. Used by both ADBC language bindings AND the ODBC wrapper.
- **Metadata FFI** (`metadata-ffi` feature) -- Additional catalog functions
  (GetTables, GetColumns, GetPrimaryKeys, etc.) that return raw Arrow data from Databricks.
  Used only by the ODBC wrapper, which handles column renaming, type mapping, and reshaping.

The ODBC wrapper calls **both** FFI layers:
- ADBC FFI for: Driver init, Database config, Connection open/close, Statement execute
- Metadata FFI for: SQLTables, SQLColumns, SQLPrimaryKeys, SQLForeignKeys, etc.

The `metadata-ffi` feature implies `ffi` (both are always present for the ODBC build).

## Architecture Overview

```
+--------------------------------------------------------------+
|                     ODBC Wrapper (C/C++)                      |
|                                                               |
|  Connection lifecycle:              Metadata operations:      |
|  AdbcDriverInit()                   metadata_get_tables()     |
|  AdbcDatabaseNew/SetOption()        metadata_get_columns()    |
|  AdbcConnectionNew()                metadata_get_primary_keys()|
|  AdbcStatementExecuteQuery()        metadata_get_foreign_keys()|
|  ...                                metadata_get_catalogs()   |
|                                     metadata_get_schemas()    |
+----------+------------------------------+---------------------+
           | ADBC C API                   | Metadata C FFI
           | (feature = "ffi")            | (feature = "metadata-ffi")
           |                              | Arrow C Data Interface
+----------v------------------+  +--------v---------------------+
| lib.rs                      |  | src/ffi/catalog.rs            |
| adbc_ffi::                  |  | - extern "C" metadata_*       |
|   export_driver!            |  |   functions (6 catalog fns)   |
|                             |  |                               |
| Standard ADBC               |  | src/ffi/handle.rs             |
| Driver/Database/            |  | - metadata_connection_from_ref|
| Connection/                 |  | - metadata_connection_free    |
| Statement                   |  |                               |
|                             |  | src/ffi/error.rs              |
|                             |  | - metadata_get_last_error     |
+----------+------------------+  +--------+---------------------+
           |                              |
           |     calls Rust API           |
           +-----------+------------------+
                       |
+----------------------v------------------------------------+
|  src/metadata/service.rs  (ConnectionMetadataService)     |
|  - get_catalogs()      -> raw Arrow RecordBatch           |
|  - get_schemas()       -> raw Arrow RecordBatch           |
|  - get_tables()        -> raw Arrow RecordBatch           |
|  - get_columns()       -> raw Arrow RecordBatch           |
|  - get_primary_keys()  -> raw Arrow RecordBatch           |
|  - get_foreign_keys()  -> raw Arrow RecordBatch           |
|                                                           |
|  collect_batches(): drains reader, concat_batches         |
+----------------------+------------------------------------+
                       | reuses
+----------------------v------------------------------------+
|  Existing infrastructure                                  |
|  - metadata/sql.rs (SqlCommandBuilder)                    |
|  - metadata/parse.rs (CatalogInfo, SchemaInfo, etc.)      |
|  - metadata/type_mapping.rs (type codes, used by builder) |
|  - metadata/builder.rs (ADBC get_objects Arrow building)  |
|  - client/mod.rs (DatabricksClient trait)                 |
|  - connection.rs (Connection - session, runtime)          |
+-----------------------------------------------------------+
```

## Key Design: Raw Arrow Pass-Through

The metadata FFI layer passes raw Arrow data from Databricks directly to the caller.
There is **no** intermediate Arrow -> Rust structs -> Arrow conversion in this path:

1. `ConnectionMetadataService` calls `DatabricksClient` methods (e.g. `list_tables()`)
2. The client executes SQL (`SHOW TABLES`, etc.) and returns an `ExecuteResult` with a reader
3. `collect_batches()` drains the reader and concatenates into a single `RecordBatch`
4. The raw `RecordBatch` is exported via Arrow C Data Interface to the C caller
5. The caller (ODBC wrapper) handles column renaming, type mapping, and reshaping

The ADBC `get_objects` path (`connection.rs` -> `builder.rs`) still uses parsed structs
for hierarchical grouping -- that path is unchanged.

## Files

### Metadata FFI Files

| File | Purpose |
|------|---------|
| `src/metadata/service.rs` | `ConnectionMetadataService` with 6 methods returning raw Arrow |
| `src/ffi/catalog.rs` | `extern "C"` functions (6 catalog + handle mgmt + error) |
| `src/ffi/handle.rs` | Opaque handle: `metadata_connection_from_ref`, `metadata_connection_free` |
| `src/ffi/error.rs` | Thread-local error buffer, `metadata_get_last_error` |
| `src/ffi/mod.rs` | FFI module root (conditionally compiled) |

### Shared Infrastructure (unchanged)

| File | Used by |
|------|---------|
| `src/metadata/sql.rs` | Service (primary/foreign keys SQL), Client (SHOW commands) |
| `src/metadata/parse.rs` | ADBC `get_objects` path (catalogs, schemas, tables, columns) |
| `src/metadata/type_mapping.rs` | ADBC `get_objects` path (Arrow builder) |
| `src/metadata/builder.rs` | ADBC `get_objects` path (nested Arrow construction) |
| `src/client/mod.rs` | Service + Connection (DatabricksClient trait) |

## Exported FFI Functions (9 total)

### Handle Management
| Function | Signature |
|----------|-----------|
| `metadata_connection_from_ref` | `(conn: *const c_void) -> FfiConnectionHandle` |
| `metadata_connection_free` | `(handle: FfiConnectionHandle)` |

### Error Retrieval
| Function | Signature |
|----------|-----------|
| `metadata_get_last_error` | `(message: *mut *const c_char, sqlstate: *mut *const c_char, code: *mut c_int) -> FfiStatus` |

### Catalog Functions
| Function | Signature |
|----------|-----------|
| `metadata_get_catalogs` | `(conn, out) -> FfiStatus` |
| `metadata_get_schemas` | `(conn, catalog, schema_pattern, out) -> FfiStatus` |
| `metadata_get_tables` | `(conn, catalog, schema_pattern, table_pattern, table_types, out) -> FfiStatus` |
| `metadata_get_columns` | `(conn, catalog, schema_pattern, table_pattern, column_pattern, out) -> FfiStatus` |
| `metadata_get_primary_keys` | `(conn, catalog, schema, table, out) -> FfiStatus` |
| `metadata_get_foreign_keys` | `(conn, catalog, schema, table, out) -> FfiStatus` |

All catalog functions export results via `FFI_ArrowArrayStream` (Arrow C Data Interface).

## ConnectionMetadataService

```rust
pub struct ConnectionMetadataService {
    client: Arc<dyn DatabricksClient>,
    session_id: String,
    runtime: tokio::runtime::Handle,
}
```

| Method | Implementation |
|--------|---------------|
| `get_catalogs` | `client.list_catalogs()` -> `collect_batches()` |
| `get_schemas` | `client.list_schemas()` -> `collect_batches()` |
| `get_tables` | `client.list_tables()` -> `collect_batches()` |
| `get_columns` | Per-catalog `client.list_columns()` -> `concat_batches()` |
| `get_primary_keys` | `execute_statement(SHOW KEYS)` -> `collect_batches()` |
| `get_foreign_keys` | `execute_statement(SHOW FOREIGN KEYS)` -> `collect_batches()` |

`get_columns` has special handling: when catalog is `None`, empty, or wildcard (`%`/`*`),
it discovers all catalogs first, queries each individually, and concatenates the results.

## Cargo.toml

```toml
[features]
ffi = ["dep:adbc_ffi"]
metadata-ffi = ["ffi", "dep:arrow"]

[dependencies]
arrow = { version = "57", optional = true, default-features = false, features = ["ffi"] }
arrow-select = "57"  # for concat_batches
```

## Key Design Decisions

1. **Raw Arrow pass-through**: No intermediate parsing in the FFI path. The driver returns
   exactly what Databricks sends. Column renaming, type mapping, and schema reshaping are
   the caller's responsibility.
2. **Separate `metadata-ffi` feature flag**: Keeps ADBC FFI and metadata FFI independent.
   The metadata FFI adds `arrow` (for Arrow C Data Interface) as an optional dependency.
3. **No `MetadataService` trait**: Single concrete implementation (`ConnectionMetadataService`).
   The trait was unnecessary indirection since there's only one implementation and it's not
   mocked in tests.
4. **Arrow C Data Interface for results**: Zero-copy transport of Arrow data to C callers.
5. **Thread-local error buffer**: Simple, matches ODBC error retrieval pattern.
6. **`collect_batches()` helper**: Drains the reader and uses `arrow_select::concat::concat_batches`
   to merge multiple batches into one, handling empty results gracefully.
7. **Minimal FFI surface**: Only 6 catalog functions that correspond to real SQL queries.
   Operations like `get_table_types`, `get_statistics`, `get_type_info`, etc. are handled
   by the ODBC wrapper directly (they're either static data or empty stubs).
