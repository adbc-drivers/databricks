# Sprint Plan: Connection Metadata Implementation

**Sprint Duration:** 2 weeks
**Created:** 2026-02-04
**Design Document:** [connection-metadata-design.md](./connection-metadata-design.md)

---

## Sprint Goal

Implement the high and medium priority Connection metadata methods (`get_objects()`, `get_table_schema()`, `get_table_types()`) by creating the metadata module infrastructure, enabling users to query catalog/schema/table/column metadata from Databricks SQL endpoints.

---

## Scope

### In Scope
- Create new `metadata/` module with service, builder, schemas, and SQL components
- Implement `get_objects()` with full depth support (catalogs → schemas → tables → columns → constraints)
- Implement `get_table_schema()` for retrieving Arrow schema of specific tables
- Implement `get_table_types()` returning supported table types
- Type mapping from Databricks SQL types to Arrow and XDBC types
- Unit tests for all new components
- Integration tests for metadata methods

### Out of Scope (Deferred to Future Sprint)
- `read_partition()` - Low priority per design doc
- `get_statistics()` - Low priority per design doc
- `get_statistic_names()` - Low priority per design doc

---

## Tasks

### Task 1: Create Metadata Module Structure and Data Types

**Description:**
Set up the new `metadata/` module directory structure and implement all data structures for metadata results. This establishes the foundation for all subsequent tasks.

**Acceptance Criteria:**
- [ ] Create `rust/src/metadata/mod.rs` with module exports
- [ ] Create `rust/src/metadata/types.rs` with data structures:
  - `CatalogInfo` struct
  - `SchemaInfo` struct
  - `TableInfo` struct
  - `ColumnInfo` struct (with all XDBC fields)
  - `PrimaryKeyInfo` struct
  - `ForeignKeyInfo` struct
- [ ] Add `pub mod metadata;` to `lib.rs`
- [ ] All structs derive `Debug`, `Clone`, and implement `Default` where appropriate
- [ ] Add Apache 2.0 license headers to all new files

**Files to Create/Modify:**
- `rust/src/metadata/mod.rs` (new)
- `rust/src/metadata/types.rs` (new)
- `rust/src/lib.rs` (modify - add module export)

**Reference:** Design doc sections "Data Structures" (lines 168-241)

---

### Task 2: Implement SqlCommandBuilder for SQL Generation

**Description:**
Implement the `SqlCommandBuilder` that generates SQL commands for metadata queries. This component handles pattern escaping, identifier quoting, and building SHOW commands with proper filters.

**Acceptance Criteria:**
- [ ] Create `rust/src/metadata/sql.rs`
- [ ] Implement `SqlCommandBuilder` struct with builder pattern methods:
  - `new()` constructor
  - `with_catalog()` - set catalog filter
  - `with_schema_pattern()` - set schema LIKE pattern
  - `with_table_pattern()` - set table LIKE pattern
  - `with_column_pattern()` - set column LIKE pattern
- [ ] Implement SQL generation methods:
  - `build_show_catalogs()` → `SHOW CATALOGS`
  - `build_show_schemas()` → `SHOW SCHEMAS IN ...`
  - `build_show_tables()` → `SHOW TABLES IN CATALOG ...`
  - `build_show_columns()` → `SHOW COLUMNS IN CATALOG ...`
  - `build_show_primary_keys()` → `SHOW KEYS IN CATALOG ...`
  - `build_show_foreign_keys()` → `SHOW FOREIGN KEYS IN CATALOG ...`
- [ ] Implement helper methods:
  - `escape_identifier()` - backtick-quote identifiers
  - `jdbc_to_sql_pattern()` - handle pattern conversion
- [ ] Add unit tests for all SQL generation scenarios

**Files to Create/Modify:**
- `rust/src/metadata/sql.rs` (new)
- `rust/src/metadata/mod.rs` (modify - add export)

**Reference:** Design doc section "SQL Command Builder" (lines 499-640)

---

### Task 3: Implement MetadataService Core (Catalogs and Schemas)

**Description:**
Implement the `MetadataService` struct that executes metadata SQL queries via the existing SEA client. Start with `list_catalogs()` and `list_schemas()` methods as the foundation.

**Acceptance Criteria:**
- [ ] Create `rust/src/metadata/service.rs`
- [ ] Implement `MetadataService` struct with:
  - `client: Arc<dyn DatabricksClient>` field
  - `session_id: String` field
  - `runtime: tokio::runtime::Handle` field
- [ ] Implement `new()` constructor
- [ ] Implement `list_catalogs() -> Result<Vec<CatalogInfo>>`:
  - Execute `SHOW CATALOGS` via SEA client
  - Parse Arrow result into `CatalogInfo` structs
- [ ] Implement `list_schemas(catalog, pattern) -> Result<Vec<SchemaInfo>>`:
  - Build SQL using `SqlCommandBuilder`
  - Execute via SEA client
  - Parse Arrow result into `SchemaInfo` structs
- [ ] Implement helper method for executing SQL and converting results
- [ ] Add unit tests with mocked client responses

**Files to Create/Modify:**
- `rust/src/metadata/service.rs` (new)
- `rust/src/metadata/mod.rs` (modify - add export)

**Reference:** Design doc section "MetadataService Interface" (lines 109-165)

---

### Task 4: Implement MetadataService Table and Column Methods ✅ COMPLETED

**Description:**
Extend `MetadataService` with methods for listing tables, columns, primary keys, and foreign keys. These are needed for the full `get_objects()` implementation.

**Acceptance Criteria:**
- [x] Implement `list_tables(catalog, schema_pattern, table_pattern, table_types) -> Result<Vec<TableInfo>>`:
  - Build SQL with all filters using `SqlCommandBuilder`
  - Filter by table_types if provided
  - Parse Arrow result into `TableInfo` structs
- [x] Implement `list_columns(catalog, schema_pattern, table_pattern, column_pattern) -> Result<Vec<ColumnInfo>>`:
  - Build SQL using `SqlCommandBuilder`
  - Parse Arrow result with all column metadata fields
- [x] Implement `list_table_types() -> Vec<String>`:
  - Return static list: `["SYSTEM TABLE", "TABLE", "VIEW", "METRIC_VIEW"]`
- [x] Implement `list_primary_keys(catalog, schema, table) -> Result<Vec<PrimaryKeyInfo>>`:
  - Execute `SHOW KEYS` query
  - Parse result into `PrimaryKeyInfo` structs
- [x] Implement `list_foreign_keys(catalog, schema, table) -> Result<Vec<ForeignKeyInfo>>`:
  - Execute `SHOW FOREIGN KEYS` query
  - Parse result into `ForeignKeyInfo` structs
- [x] Add unit tests for each method

**Files Modified:**
- `rust/src/metadata/service.rs` (modified - added 5 new methods with 33+ unit tests)

**Reference:** Design doc sections "MetadataService Interface" (lines 109-165) and "SQL Commands for Metadata" (lines 80-94)

**Implementation Notes:**
- `list_tables()` uses `SqlCommandBuilder` to generate SHOW TABLES query with catalog/schema/table filters
- Table type normalization maps Databricks-specific types (MANAGED, EXTERNAL) to standard ADBC types (TABLE)
- `list_columns()` parses type info to extract column_size, decimal_digits, and num_prec_radix from type strings
- Ordinal positions are recomputed per table to ensure correct 1-based ordering
- `list_primary_keys()` and `list_foreign_keys()` use static SQL builders for fully qualified queries
- All methods handle empty results gracefully and return empty vectors

---

### Task 5: Implement Type Mapping Functions

**Description:**
Implement functions to map Databricks SQL type names to Arrow DataTypes and XDBC/JDBC type codes. These are essential for `get_table_schema()` and populating XDBC fields in `get_objects()`.

**Acceptance Criteria:**
- [ ] Create `rust/src/metadata/type_mapping.rs`
- [ ] Implement `databricks_type_to_arrow(type_name: &str) -> DataType`:
  - Handle all Databricks types: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, STRING, VARCHAR, CHAR, BINARY, DATE, TIMESTAMP, TIMESTAMP_NTZ, TIMESTAMP_LTZ, ARRAY, MAP, STRUCT, VOID/NULL
  - Parse DECIMAL precision/scale from type string
  - Default to Utf8 for unknown types
- [ ] Implement `databricks_type_to_xdbc(type_name: &str) -> i16`:
  - Map to JDBC type codes (e.g., INTEGER=4, VARCHAR=12, etc.)
- [ ] Implement helper `parse_decimal_params(type_name: &str) -> (u8, i8)`:
  - Extract precision and scale from `DECIMAL(p,s)` format
- [ ] Add comprehensive unit tests covering all type mappings

**Files to Create/Modify:**
- `rust/src/metadata/type_mapping.rs` (new)
- `rust/src/metadata/mod.rs` (modify - add export)

**Reference:** Design doc section "Type Mapping" (lines 643-706)

---

### Task 6: Implement GetObjectsBuilder for Nested Arrow Structure ✅ COMPLETED

**Description:**
Implement the `GetObjectsBuilder` that constructs the nested Arrow structure required by the ADBC `get_objects()` specification. This involves building complex nested Lists and Structs.

**Acceptance Criteria:**
- [x] Create `rust/src/metadata/builder.rs`
- [x] Create `rust/src/metadata/schemas.rs` with Arrow schema definitions:
  - `get_objects_schema()` - top-level schema
  - `db_schema_schema()` - nested schema for db_schemas
  - `table_schema()` - nested schema for tables
  - `column_schema()` - nested schema for columns (with all XDBC fields)
  - `constraint_schema()` - nested schema for constraints
  - `usage_schema()` - nested schema for foreign key usage
- [x] Implement `GetObjectsBuilder` struct with methods:
  - `new()` constructor
  - `add_catalog(name)` - add a catalog entry
  - `add_schema(catalog, schema)` - add a schema to a catalog
  - `add_table(catalog, schema, table_info)` - add a table to a schema
  - `add_column(catalog, schema, table, column_info)` - add column to table
  - `add_constraints(catalog, schema, table, pks, fks)` - add constraints
  - `build() -> Result<impl RecordBatchReader>` - finalize and return reader
- [x] Handle all depth levels correctly (Catalogs, Schemas, Tables, Columns)
- [x] Add unit tests for builder at each depth level

**Files Created/Modified:**
- `rust/src/metadata/builder.rs` (new) - 1200+ lines
- `rust/src/metadata/schemas.rs` (new) - 300+ lines
- `rust/src/metadata/mod.rs` (modified - added exports)
- `rust/Cargo.toml` (modified - added arrow-select, arrow-buffer dependencies)

**Reference:** Design doc sections "get_objects Implementation" (lines 243-417) and Arrow schema diagram (lines 248-306)

**Implementation Notes:**
- Used direct StructArray and ListArray construction instead of builders due to complexity of nested structures
- Empty ListArrays require at least one offset [0], handled via slice(0,0) pattern
- Added helper methods `create_empty_*_struct()` for each level to properly initialize empty arrays
- XDBC fields populated via `databricks_type_to_xdbc()` type mapping function
- All 18 unit tests pass covering catalogs-only, with-schemas, with-tables, full-depth, and constraints scenarios

---

### Task 7: Implement Connection Metadata Methods

**Description:**
Wire up the metadata infrastructure to implement `get_objects()`, `get_table_schema()`, and `get_table_types()` in `connection.rs`. Replace the stub implementations with working code.

**Acceptance Criteria:**
- [ ] Implement `get_objects(depth, catalog, db_schema, table_name, table_type, column_name)`:
  - Create `MetadataService` instance
  - Fetch catalogs, filter by pattern
  - Based on depth, fetch schemas/tables/columns/constraints
  - Use `GetObjectsBuilder` to construct result
  - Return `RecordBatchReader`
- [ ] Implement `get_table_schema(catalog, db_schema, table_name)`:
  - Use `MetadataService.list_columns()` for the specific table
  - Use `databricks_type_to_arrow()` to map types
  - Construct and return Arrow `Schema`
  - Return `NOT_FOUND` error if table doesn't exist
- [ ] Implement `get_table_types()`:
  - Return static list as single-column Arrow result
  - Schema: `table_type: Utf8 NOT NULL`
- [ ] Add `use` statements for metadata module in `connection.rs`
- [ ] Remove `NOT_IMPLEMENTED` error returns for these three methods

**Files to Create/Modify:**
- `rust/src/connection.rs` (modify)

**Reference:** Design doc sections "get_objects Implementation" (lines 309-417), "get_table_schema Implementation" (lines 420-463), "get_table_types Implementation" (lines 465-497)

---

### Task 8: Add Unit and Integration Tests

**Description:**
Add comprehensive unit tests for all metadata components and integration tests that run against a real Databricks endpoint.

**Acceptance Criteria:**
- [ ] Unit tests in each module file (`#[cfg(test)] mod tests`):
  - `sql.rs`: Test all SQL generation methods with various filter combinations
  - `type_mapping.rs`: Test all type mappings (already in Task 5)
  - `builder.rs`: Test builder at each depth level (already in Task 6)
  - `service.rs`: Test with mocked client responses
- [ ] Create `rust/tests/integration/metadata_tests.rs` with:
  - `test_get_objects_catalogs_depth`
  - `test_get_objects_schemas_depth`
  - `test_get_objects_tables_depth`
  - `test_get_objects_columns_depth`
  - `test_get_objects_with_catalog_filter`
  - `test_get_objects_with_schema_pattern`
  - `test_get_objects_with_table_type_filter`
  - `test_get_table_schema_existing_table`
  - `test_get_table_schema_nonexistent_table`
  - `test_get_table_types`
- [ ] Integration tests use environment variables for connection (DATABRICKS_HOST, DATABRICKS_TOKEN, etc.)
- [ ] All tests pass with `cargo test`
- [ ] Document test setup requirements in test file comments

**Files to Create/Modify:**
- `rust/tests/integration/metadata_tests.rs` (new)
- `rust/tests/integration/mod.rs` (modify or create)

**Reference:** Design doc section "Test Strategy" (lines 801-828)

---

## Task Dependencies

```
Task 1 (Module Structure)
    ↓
Task 2 (SqlCommandBuilder) ──────────────────┐
    ↓                                        │
Task 3 (MetadataService Core)                │
    ↓                                        │
Task 4 (MetadataService Tables/Columns)      │
    ↓                                        │
Task 5 (Type Mapping) ←──────────────────────┘
    ↓
Task 6 (GetObjectsBuilder)
    ↓
Task 7 (Connection Methods)
    ↓
Task 8 (Tests)
```

**Critical Path:** Tasks 1 → 2 → 3 → 4 → 6 → 7

**Parallelizable:**
- Task 5 (Type Mapping) can be done in parallel with Tasks 3-4
- Unit tests within each task can be written alongside implementation

---

## Definition of Done

- [ ] All acceptance criteria met for each task
- [ ] Code follows project conventions (Apache 2.0 headers, doc comments)
- [ ] `cargo build` succeeds with no warnings
- [ ] `cargo test` passes all unit tests
- [ ] `cargo clippy -- -D warnings` passes
- [ ] `cargo fmt --check` passes
- [ ] Integration tests pass against Databricks endpoint
- [ ] Code reviewed and approved

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Complex nested Arrow structure in GetObjectsBuilder | Medium | High | Reference Go/C# implementations; add incremental unit tests |
| SHOW commands return unexpected columns | Low | Medium | Test against real Databricks early; add defensive parsing |
| Type mapping edge cases | Medium | Low | Default to Utf8 for unknown types; log warnings |
| Integration test environment unavailable | Low | High | Ensure CI has access to test workspace |

---

## References

- [Connection Metadata Design Document](./connection-metadata-design.md)
- [ADBC Specification - Connection Interface](https://arrow.apache.org/adbc/current/format/specification.html)
- [Databricks SQL Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution)
- Go driver implementation: `../go/`
- C# driver implementation: `../csharp/`
