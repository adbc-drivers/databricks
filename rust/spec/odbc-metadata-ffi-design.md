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
| lib.rs                      |  | src/ffi/metadata.rs           |
| adbc_ffi::                  |  | - extern "C" metadata_*       |
|   export_driver!            |  |   functions (6 catalog fns)   |
|                             |  |   Takes *const Connection     |
| Standard ADBC               |  |                               |
| Driver/Database/            |  | src/ffi/error.rs              |
| Connection/                 |  | - metadata_get_last_error     |
| Statement                   |  |                               |
+----------+------------------+  +--------+---------------------+
           |                              |
           |     calls Rust API           | calls Connection directly
           +-----------+------------------+
                       |
+----------------------v------------------------------------+
|  Existing infrastructure                                  |
|  - connection.rs (client(), session_id(), runtime_handle)|
|  - client/mod.rs (DatabricksClient trait)                 |
|  - metadata/sql.rs (SqlCommandBuilder)                    |
|  - metadata/parse.rs (CatalogInfo, SchemaInfo, etc.)      |
|  - metadata/type_mapping.rs (type codes, used by builder) |
|  - metadata/builder.rs (ADBC get_objects Arrow building)  |
+-----------------------------------------------------------+
```

## Key Design: Streaming Arrow Pass-Through

The metadata FFI layer streams raw Arrow data from Databricks directly to the caller.
There is **no** intermediate buffering, collecting, or Arrow -> Rust structs -> Arrow
conversion in this path:

1. FFI functions receive a `*const Connection` pointer (from `adbc_connection_.private_data`)
2. They call `DatabricksClient` methods directly via `Connection` accessors
3. The client executes SQL (`SHOW TABLES`, etc.) and returns an `ExecuteResult` with a `ResultReader`
4. The FFI layer wraps the reader in a `ResultReaderAdapter` (bridges `ResultReader` → `RecordBatchReader`)
5. The `RecordBatchReader` is exported via Arrow C Data Interface (`FFI_ArrowArrayStream`) to the C caller
6. The caller (ODBC wrapper) consumes batches incrementally, handling column renaming, type mapping, and reshaping

This streaming approach avoids materializing the entire result set in memory. The
`ResultReaderAdapter` (from `reader/mod.rs`) lazily pulls batches from the underlying
network reader on each `next()` call.

The ADBC `get_objects` path (`connection.rs` -> `builder.rs`) still uses parsed structs
for hierarchical grouping -- that path is unchanged.

## Files

### Metadata FFI Files

| File | Purpose |
|------|---------|
| `src/ffi/metadata.rs` | `extern "C"` functions (6 catalog fns + helpers) |
| `src/ffi/error.rs` | Thread-local error buffer, `metadata_get_last_error` |
| `src/ffi/mod.rs` | FFI module root (conditionally compiled) |
| `include/databricks_metadata_ffi.h` | C header declarations for all FFI functions |

### Shared Infrastructure (unchanged)

| File | Used by |
|------|---------|
| `src/metadata/sql.rs` | FFI (primary/foreign keys SQL), Client (SHOW commands) |
| `src/metadata/parse.rs` | ADBC `get_objects` path (catalogs, schemas, tables, columns) |
| `src/metadata/type_mapping.rs` | ADBC `get_objects` path (Arrow builder) |
| `src/metadata/builder.rs` | ADBC `get_objects` path (nested Arrow construction) |
| `src/client/mod.rs` | FFI + Connection (DatabricksClient trait) |

## Exported FFI Functions (7 total)

### Error Retrieval
| Function | Signature |
|----------|-----------|
| `metadata_get_last_error` | `(error_out: *mut FfiError) -> FfiStatus` |

`FfiError` is a `#[repr(C)]` struct with fixed-size buffers:
```c
struct FfiError {
    char message[1024];   // null-terminated error message
    char sql_state[6];    // null-terminated SQLSTATE code
    int32_t native_error; // native error code
};
```

### Catalog Functions

All functions take `*const void` (a `Connection*`) directly — no handle create/free lifecycle.
The ODBC side already has `adbc_connection_.private_data` which is the Rust `Connection*`.

| Function | Signature |
|----------|-----------|
| `metadata_get_catalogs` | `(conn, out) -> FfiStatus` |
| `metadata_get_schemas` | `(conn, catalog, schema_pattern, out) -> FfiStatus` |
| `metadata_get_tables` | `(conn, catalog, schema_pattern, table_pattern, table_types, out) -> FfiStatus` |
| `metadata_get_columns` | `(conn, catalog, schema_pattern, table_pattern, column_pattern, out) -> FfiStatus` |
| `metadata_get_primary_keys` | `(conn, catalog, schema, table, out) -> FfiStatus` |
| `metadata_get_foreign_keys` | `(conn, catalog, schema, table, out) -> FfiStatus` |

All catalog functions export results via `FFI_ArrowArrayStream` (Arrow C Data Interface).
`metadata_get_columns` returns an empty stream when catalog is null/empty (since
`SHOW COLUMNS IN ALL CATALOGS` is not supported by Databricks).

## Cargo.toml

```toml
[features]
ffi = ["dep:adbc_ffi"]
metadata-ffi = ["ffi", "dep:arrow"]

[dependencies]
arrow = { version = "57", optional = true, default-features = false, features = ["ffi"] }
```

## Key Design Decisions

1. **Direct `Connection` access**: FFI functions take `*const Connection` directly instead of
   an intermediate service/handle layer. This eliminates ~300 lines of indirection. The ODBC
   side already has the `Connection*` from `adbc_connection_.private_data`.
2. **Raw Arrow pass-through**: No intermediate parsing in the FFI path. The driver returns
   exactly what Databricks sends. Column renaming, type mapping, and schema reshaping are
   the caller's responsibility.
3. **Separate `metadata-ffi` feature flag**: Keeps ADBC FFI and metadata FFI independent.
   The metadata FFI adds `arrow` (for Arrow C Data Interface) as an optional dependency.
4. **Arrow C Data Interface for results**: Zero-copy transport of Arrow data to C callers.
5. **Thread-local error buffer**: Simple, matches ODBC error retrieval pattern. The error
   buffer is cleared at the start of each FFI function (except `metadata_get_last_error`
   itself) so callers never see stale errors from a previous call.
6. **`catch_unwind` on all FFI entry points**: Panics must not unwind across the FFI
   boundary (undefined behavior in Rust). All `extern "C"` functions wrap their body in
   `std::panic::catch_unwind` and convert panics to error status codes.
7. **Streaming via `ResultReaderAdapter`**: Instead of collecting all batches into a single
   `RecordBatch`, the reader is streamed through the FFI layer using `ResultReaderAdapter`
   which bridges `ResultReader` → `RecordBatchReader` for zero-copy FFI export.
8. **Minimal FFI surface**: Only 6 catalog functions that correspond to real SQL queries.
   Operations like `get_table_types`, `get_statistics`, `get_type_info`, etc. are handled
   by the ODBC wrapper directly (they're either static data or empty stubs).
9. **C header file**: `include/databricks_metadata_ffi.h` provides declarations for all
   FFI types and functions, ensuring the ODBC C++ side doesn't need to manually mirror
   Rust declarations.
10. **`list_columns` takes `catalog: &str`**: The `DatabricksClient` trait enforces catalog
    as required (not `Option`). The empty-catalog guard lives in the FFI layer, which returns
    an empty stream instead of propagating to the client.
