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

# Design: ODBC Metadata FFI Layer for Databricks Rust Driver

## Context

The Databricks ADBC Rust driver (`rust/`) is being used as the core engine for an ODBC wrapper.
The driver already has:
- An internal metadata interface (`DatabricksClient` trait + `metadata/` module) that handles
  SHOW SQL commands, result parsing, type mapping, and Arrow building
- An existing ADBC FFI (`ffi` feature) for standard ADBC C API exposure via `adbc_ffi::export_driver!`

The ODBC wrapper needs flat, tabular metadata results (not ADBC's nested `get_objects` hierarchy).
The two FFI layers serve different purposes but **coexist in the same shared library**:
- **ADBC FFI** (`ffi` feature) → Standard ADBC C API for connection lifecycle, statement
  execution, and basic metadata. Used by both ADBC language bindings AND the ODBC wrapper.
- **Metadata FFI** (`odbc-ffi` feature) → Additional ODBC-specific catalog functions
  (GetTables, GetColumns, GetTypeInfo, GetPrimaryKeys, etc.) that return flat tabular
  results. Used only by the ODBC wrapper.

The ODBC wrapper calls **both** FFI layers:
- ADBC FFI for: Driver init, Database config, Connection open/close, Statement execute
- Metadata FFI for: SQLTables, SQLColumns, SQLGetTypeInfo, SQLPrimaryKeys, etc.

The `odbc-ffi` feature implies `ffi` (both are always present for the ODBC build).

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                     ODBC Wrapper (C/C++)                      │
│                                                              │
│  Connection lifecycle:              Metadata operations:     │
│  AdbcDriverInit()                   odbc_get_tables()        │
│  AdbcDatabaseNew/SetOption()        odbc_get_columns()       │
│  AdbcConnectionNew()                odbc_get_type_info()     │
│  AdbcStatementExecuteQuery()        odbc_get_primary_keys()  │
│  ...                                odbc_get_foreign_keys()  │
│                                     ...                      │
└─────────┬──────────────────────────────┬─────────────────────┘
          │ ADBC C API                   │ Metadata C FFI
          │ (feature = "ffi")            │ (feature = "odbc-ffi")
          │                              │ Arrow C Data Interface
┌─────────▼──────────┐    ┌─────────────▼─────────────────────┐
│ lib.rs             │    │ src/ffi/odbc.rs                    │
│ adbc_ffi::         │    │ - extern "C" odbc_* functions      │
│   export_driver!   │    │ - Handle management                │
│                    │    │ - Error propagation                │
│ Standard ADBC      │    │ - Arrow C Data Interface export    │
│ Driver/Database/   │    │                                    │
│ Connection/        │    │ src/ffi/handle.rs                  │
│ Statement          │    │ - Connection ↔ opaque pointer      │
└─────────┬──────────┘    └─────────────┬─────────────────────┘
          │                             │
          │     calls Rust API          │
          └──────────┬──────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│  src/metadata/service.rs  (MetadataService trait)           │
│  - get_tables() → flat Arrow RecordBatch                    │
│  - get_columns() → flat JDBC-compatible schema              │
│  - get_type_info() → supported type descriptors             │
│  - get_primary_keys() → PK info for a table                 │
│  - get_foreign_keys() → FK info for a table                 │
│  - get_catalogs() → catalog list                            │
│  - get_schemas() → schema list                              │
│  - get_table_types() → table type list                      │
│  - get_statistics/special_columns/procedures/privileges     │
│    → empty results with correct schemas                     │
└────────────────────┬────────────────────────────────────────┘
                     │ reuses
┌────────────────────▼────────────────────────────────────────┐
│  Existing infrastructure                                    │
│  - metadata/sql.rs (SqlCommandBuilder)                      │
│  - metadata/parse.rs (CatalogInfo, SchemaInfo, etc.)        │
│  - metadata/type_mapping.rs (type codes)                    │
│  - metadata/builder.rs (Arrow array construction)           │
│  - client/mod.rs (DatabricksClient trait)                   │
│  - connection.rs (Connection - session, runtime)            │
└─────────────────────────────────────────────────────────────┘
```

## New Files

| File | Purpose |
|------|---------|
| `src/metadata/service.rs` | `MetadataService` trait + `ConnectionMetadataService` impl |
| `src/metadata/schemas.rs` | Arrow schema definitions for each flat metadata result set |
| `src/metadata/type_info.rs` | Type info catalog (SQLGetTypeInfo result data) |
| `src/ffi/mod.rs` | FFI module root (conditionally compiled) |
| `src/ffi/odbc.rs` | `extern "C"` functions for ODBC wrapper |
| `src/ffi/handle.rs` | Opaque handle management (Connection → handle map) |
| `src/ffi/error.rs` | C-compatible error types and propagation |

## Modified Files

| File | Change |
|------|--------|
| `Cargo.toml` | Add `odbc-ffi` feature flag, add `arrow` dependency (for FFI) |
| `src/lib.rs` | Add `pub mod ffi;` (conditional on `odbc-ffi`) |
| `src/metadata/mod.rs` | Add `pub mod service; pub mod schemas; pub mod type_info;` |

---

## Detailed Design

### 1. MetadataService Trait (`src/metadata/service.rs`)

The core Rust-level abstraction that Connection implements. Each method returns a flat
`RecordBatch` (not the nested ADBC `get_objects` format).

```rust
/// ODBC-style metadata operations returning flat Arrow result sets.
///
/// Each method corresponds to an ODBC catalog function and returns
/// a RecordBatch with a well-defined schema matching the JDBC/ODBC specification.
pub trait MetadataService {
    fn get_catalogs(&self) -> Result<RecordBatch>;
    fn get_schemas(&self, catalog: Option<&str>, schema_pattern: Option<&str>) -> Result<RecordBatch>;
    fn get_tables(&self, catalog: Option<&str>, schema_pattern: Option<&str>,
                  table_pattern: Option<&str>, table_types: Option<&[&str]>) -> Result<RecordBatch>;
    fn get_columns(&self, catalog: Option<&str>, schema_pattern: Option<&str>,
                   table_pattern: Option<&str>, column_pattern: Option<&str>) -> Result<RecordBatch>;
    fn get_type_info(&self, data_type: Option<i16>) -> Result<RecordBatch>;
    fn get_primary_keys(&self, catalog: &str, schema: &str, table: &str) -> Result<RecordBatch>;
    fn get_foreign_keys(&self, catalog: &str, schema: &str, table: &str) -> Result<RecordBatch>;
    fn get_table_types(&self) -> Result<RecordBatch>;
    fn get_statistics(&self, catalog: &str, schema: &str, table: &str, unique: bool) -> Result<RecordBatch>;
    fn get_special_columns(&self, identifier_type: i16, catalog: &str, schema: &str,
                           table: &str, scope: i16, nullable: i16) -> Result<RecordBatch>;
    fn get_procedures(&self, catalog: Option<&str>, schema_pattern: Option<&str>,
                      proc_pattern: Option<&str>) -> Result<RecordBatch>;
    fn get_procedure_columns(&self, catalog: Option<&str>, schema_pattern: Option<&str>,
                             proc_pattern: Option<&str>, column_pattern: Option<&str>) -> Result<RecordBatch>;
    fn get_table_privileges(&self, catalog: Option<&str>, schema_pattern: Option<&str>,
                            table_pattern: Option<&str>) -> Result<RecordBatch>;
    fn get_column_privileges(&self, catalog: Option<&str>, schema: Option<&str>,
                             table: &str, column_pattern: Option<&str>) -> Result<RecordBatch>;
}
```

**Implementation strategy for `ConnectionMetadataService`:**

| Method | Implementation | Reuses |
|--------|---------------|--------|
| `get_catalogs` | `client.list_catalogs()` → `parse_catalogs()` → flat Arrow | `parse.rs`, `sql.rs` |
| `get_schemas` | `client.list_schemas()` → `parse_schemas()` → flat Arrow | `parse.rs`, `sql.rs` |
| `get_tables` | `client.list_tables()` → `parse_tables()` → flat Arrow | `parse.rs`, `sql.rs` |
| `get_columns` | `client.list_columns()` → `parse_columns()` → JDBC schema | `parse.rs`, `type_mapping.rs` |
| `get_type_info` | Static catalog of Databricks types → Arrow | `type_mapping.rs` (new `type_info.rs`) |
| `get_primary_keys` | `execute_statement(SHOW KEYS)` → parse | `sql.rs::build_show_primary_keys` |
| `get_foreign_keys` | `execute_statement(SHOW FOREIGN KEYS)` → parse | `sql.rs::build_show_foreign_keys` |
| `get_table_types` | `client.list_table_types()` → Arrow | existing |
| `get_statistics` | Returns empty result with correct schema | new schema only |
| `get_special_columns` | Returns empty result with correct schema | new schema only |
| `get_procedures` | Returns empty result with correct schema | new schema only |
| `get_procedure_columns` | Returns empty result with correct schema | new schema only |
| `get_table_privileges` | Returns empty result with correct schema | new schema only |
| `get_column_privileges` | Returns empty result with correct schema | new schema only |

### 2. Result Schemas (`src/metadata/schemas.rs`)

Static `Arc<Schema>` constants for each metadata result set, matching JDBC/ODBC specification.
Uses `lazy_static!` pattern. These are separate from `adbc_core::schemas::*` which define the
nested ADBC format.

### 3. Type Info Catalog (`src/metadata/type_info.rs`)

Static data for SQLGetTypeInfo — describes all SQL types supported by Databricks.
Reuses `type_mapping.rs::databricks_type_to_xdbc()` for type code consistency.

### 4. FFI Layer (`src/ffi/`)

#### 4a. Handle Management (`src/ffi/handle.rs`)

Provides `odbc_connection_from_adbc()` to extract a metadata handle from an ADBC connection,
plus `OdbcConnectionHandle` type alias. All `odbc_*` functions accept this handle.

#### 4b. Error Types (`src/ffi/error.rs`)

`OdbcFfiStatus` enum (Success, Error, InvalidHandle, NoData) and `OdbcFfiError` struct
with thread-local error buffer for C callers.

#### 4c. Exported Functions (`src/ffi/odbc.rs`)

Each function: validate handle → recover Connection → call MetadataService → export via
Arrow C Data Interface (`FFI_ArrowArrayStream`) → return status code.

### 5. Cargo.toml Changes

```toml
[features]
ffi = ["dep:adbc_ffi"]
odbc-ffi = ["ffi", "dep:arrow"]

[dependencies]
arrow = { version = "57", optional = true, default-features = false, features = ["ffi"] }
```

### 6. Primary Key / Foreign Key Parsing

New parsers in `parse.rs`: `PrimaryKeyInfo`, `ForeignKeyInfo` structs and
`parse_primary_keys()`, `parse_foreign_keys()` functions.

---

## Implementation Order

### Phase 1: Internal Rust Layer (no FFI yet)
1. `metadata/schemas.rs` — Define all JDBC/ODBC result set schemas
2. `metadata/type_info.rs` — Define static type info catalog entries
3. Add PK/FK parsers to `metadata/parse.rs`
4. `metadata/service.rs` — `MetadataService` trait + `ConnectionMetadataService` implementation
5. Unit tests for all new metadata service methods

### Phase 2: FFI Layer
6. `ffi/error.rs` — C-compatible error types and thread-local error buffer
7. `ffi/handle.rs` — Opaque connection handle management
8. `ffi/odbc.rs` — All `extern "C"` functions with Arrow C Data Interface export
9. `ffi/mod.rs` — Module wiring with `#[cfg(feature = "odbc-ffi")]`
10. `Cargo.toml` + `lib.rs` — Feature flag and module declarations

### Phase 3: Verification
11. `cargo build --features odbc-ffi` compiles cleanly
12. `cargo test` passes
13. `cargo clippy --features odbc-ffi -- -D warnings` clean

---

## Key Design Decisions

1. **Separate `odbc-ffi` feature flag**: Keeps ADBC FFI and ODBC metadata FFI independent.
2. **`MetadataService` trait**: Clean internal boundary between FFI and business logic.
3. **Flat Arrow results**: ODBC expects tabular results, not nested hierarchies.
4. **Arrow C Data Interface for results**: Zero-copy transport of Arrow data to C callers.
5. **Thread-local error buffer**: Simple, matches ODBC error retrieval pattern.
6. **Empty results for unsupported operations**: Correct schemas, empty data.
7. **Reuse existing infrastructure**: `SqlCommandBuilder`, parsers, type mappings reused.
