# Sprint Plan: Connection Metadata Implementation

**Sprint Goal:** Implement `get_objects`, `get_table_schema`, and `get_table_types` ADBC Connection methods for the Databricks Rust driver.

**Design Doc:** [connection-metadata-design.md](connection-metadata-design.md)

**Duration:** 2 weeks

---

## Story

**Title:** Implement ADBC Connection metadata methods (`get_objects`, `get_table_schema`, `get_table_types`)

**Description:**
The Databricks Rust ADBC driver currently stubs `get_objects`, `get_table_schema`, and `get_table_types` with `not_implemented()` errors. This story implements all three methods using SHOW SQL commands through the existing `DatabricksClient.execute_statement()` infrastructure.

The implementation follows a single-query-per-depth strategy for `get_objects` (minimizing server round trips vs the driverbase fan-out approach), adds 7 metadata methods to the `DatabricksClient` trait, creates a new `metadata/` module with SQL building, result parsing, type mapping, and nested Arrow construction, and wires everything into `Connection`.

**Acceptance Criteria:**
- `get_table_types()` returns a `RecordBatchReader` with `["SYSTEM TABLE", "TABLE", "VIEW", "METRIC_VIEW"]`
- `get_table_schema(catalog, schema, table)` returns an Arrow `Schema` with correct field names, types, and nullability
- `get_table_schema(None, schema, table)` discovers the catalog via `list_tables` before querying columns
- `get_objects()` works at all 4 depth levels (Catalogs, Schemas, Tables, All) returning correctly nested Arrow structs matching `adbc_core::schemas::GET_OBJECTS_SCHEMA`
- `get_objects()` at `All` depth fans out `SHOW COLUMNS IN CATALOG <cat>` per catalog in parallel
- All SHOW SQL commands support optional pattern filters (LIKE) and identifier escaping
- Unit tests pass for SQL building, type mapping, result parsing, and Arrow builder
- Integration tests pass against a live Databricks endpoint

---

## Sub-Tasks

### Task 1: Metadata module foundation — SQL builder, type mapping, trait methods, SeaClient impl

**Scope:** Create the `metadata/` module and implement everything needed to issue metadata queries and map types, plus wire up the `DatabricksClient` trait and `SeaClient`.

**Files to create:**
- `rust/src/metadata/mod.rs` — Module exports
- `rust/src/metadata/sql.rs` — `SqlCommandBuilder` (build SHOW CATALOGS/SCHEMAS/TABLES/COLUMNS/KEYS SQL with filters, wildcard detection, identifier escaping)
- `rust/src/metadata/type_mapping.rs` — `databricks_type_to_arrow()` and `databricks_type_to_xdbc()` functions

**Files to modify:**
- `rust/src/client/mod.rs` — Add 7 metadata method signatures to `DatabricksClient` trait (`list_catalogs`, `list_schemas`, `list_tables`, `list_columns`, `list_primary_keys`, `list_foreign_keys`, `list_table_types`)
- `rust/src/client/sea.rs` — Implement all 7 methods in `SeaClient` (each builds SQL via `SqlCommandBuilder` and calls `self.execute_statement()`)
- `rust/src/lib.rs` — Add `pub mod metadata;`

**Unit tests:**
- `test_sql_command_builder_show_catalogs`
- `test_sql_command_builder_show_schemas_with_catalog`
- `test_sql_command_builder_show_schemas_all_catalogs`
- `test_sql_command_builder_show_schemas_null_or_wildcard`
- `test_sql_command_builder_show_tables_with_patterns`
- `test_sql_command_builder_show_columns_requires_catalog`
- `test_sql_command_builder_show_columns_with_patterns`
- `test_sql_command_builder_escape_identifiers`
- `test_type_mapping_databricks_to_arrow` (all Databricks types → Arrow DataType)
- `test_type_mapping_databricks_to_xdbc` (all Databricks types → JDBC type codes)

**Estimated effort:** ~3 days

---

### Task 2: Result parsing, Arrow builder, and `get_table_types` / `get_table_schema`

**Scope:** Implement result parsing from `ExecuteResult` readers into intermediate structs, the nested Arrow builder for `get_objects`, type mapping integration, and the simpler `get_table_types` and `get_table_schema` Connection methods.

**Files to create:**
- `rust/src/metadata/parse.rs` — Parse functions: `parse_catalogs`, `parse_schemas`, `parse_tables`, `parse_columns`, `parse_columns_as_fields`; intermediate structs (`CatalogInfo`, `SchemaInfo`, `TableInfo`, `ColumnInfo`)
- `rust/src/metadata/builder.rs` — Nested Arrow struct construction: `build_get_objects_catalogs`, `build_get_objects_schemas`, `build_get_objects_tables`, `build_get_objects_all`; grouping helpers (`group_schemas_by_catalog`, `group_tables_by_catalog_schema`, `group_tables_and_columns`); hierarchy types (`CatalogNode`, `SchemaNode`, `TableNode`)

**Files to modify:**
- `rust/src/connection.rs` — Implement `get_table_types()` (static list + `GET_TABLE_TYPES_SCHEMA`) and `get_table_schema()` (call `list_columns`, parse via `parse_columns_as_fields`, handle missing catalog via `list_tables` discovery)

**Unit tests:**
- `test_parse_catalogs_from_reader` (mock reader)
- `test_parse_schemas_from_reader` (mock reader)
- `test_parse_tables_from_reader` (mock reader)
- `test_parse_columns_from_reader` (mock reader)
- `test_parse_columns_as_fields` (mock reader)
- `test_build_get_objects_catalogs_depth` (builder with synthetic data)
- `test_build_get_objects_schemas_depth`
- `test_build_get_objects_tables_depth`
- `test_build_get_objects_all_depth`
- `test_group_schemas_by_catalog`
- `test_group_tables_by_catalog_schema`
- `test_get_table_types_returns_correct_types`
- `test_get_table_schema_builds_correct_schema` (mock client)

**Estimated effort:** ~4 days

---

### Task 3: `get_objects` Connection integration and integration tests ✅

**Scope:** Implement `get_objects()` in `Connection` with the single-query-per-depth strategy, including parallel `SHOW COLUMNS` fan-out at `All` depth. Add unit tests with mock clients.

**Files modified:**
- `rust/src/connection.rs` — Implemented `get_objects()` at all 4 depth levels + `filter_by_pattern()` / `like_match()` helpers

**Unit tests added:**
- `test_get_objects_catalogs_depth`
- `test_get_objects_catalogs_depth_with_pattern_filter`
- `test_get_objects_catalogs_depth_with_wildcard_pattern`
- `test_get_objects_schemas_depth`
- `test_get_objects_tables_depth`
- `test_get_objects_tables_depth_with_table_type_filter`
- `test_get_objects_all_depth`
- `test_get_objects_all_depth_multi_catalog_parallel`
- `test_get_objects_empty_result`
- `test_like_match_exact` / `test_like_match_percent_wildcard` / `test_like_match_underscore_wildcard` / `test_like_match_combined_wildcards`
- `test_filter_by_pattern_none` / `test_filter_by_pattern_empty_or_wildcard` / `test_filter_by_pattern_exact_match` / `test_filter_by_pattern_wildcard_match`

**Implementation decisions:**
- `ObjectDepth::Columns` treated identically to `ObjectDepth::All` per ADBC spec
- Client-side `filter_by_pattern` uses recursive `like_match` without regex dependency
- `collect_reader()` helper unifies different `impl RecordBatchReader` opaque types from builders into concrete `RecordBatchIterator`

---

### Task 4: Integration tests for metadata methods ✅

**Scope:** Add end-to-end integration tests in `rust/tests/integration.rs` for all three metadata methods (`get_objects`, `get_table_schema`, `get_table_types`) against a live Databricks endpoint.

**Files modified:**
- `rust/tests/integration.rs` — Added 11 integration tests + `create_live_connection()` helper

**Integration tests added (all `#[ignore]`, require env vars):**
- `test_get_objects_catalogs_depth` — Verifies schema matches `GET_OBJECTS_SCHEMA`, finds "main" catalog, `catalog_db_schemas` null
- `test_get_objects_schemas_depth` — Verifies schemas are populated, finds "default" schema, `db_schema_tables` null
- `test_get_objects_tables_depth` — Verifies tables in main.default, `table_columns`/`table_constraints` null
- `test_get_objects_columns_depth` — Verifies columns in main.information_schema.tables, constraints empty list (not null)
- `test_get_objects_with_catalog_filter` — Verifies catalog pattern "main" returns only "main"
- `test_get_objects_with_schema_pattern` — Verifies schema pattern "default" filters correctly
- `test_get_objects_with_table_type_filter` — Verifies table_type filter "VIEW" in main.information_schema
- `test_get_table_schema_existing_table` — Verifies main.information_schema.tables returns known columns
- `test_get_table_schema_nonexistent_table` — Verifies NOT_FOUND error for nonexistent table
- `test_get_table_schema_without_catalog` — Verifies catalog discovery via list_tables when catalog is None
- `test_get_table_types` — Verifies schema matches `GET_TABLE_TYPES_SCHEMA`, contains TABLE and VIEW

**Implementation decisions:**
- Extracted `create_live_connection()` helper to avoid code duplication across tests
- Tests use `main.information_schema.tables` as a well-known table that exists in every Unity Catalog workspace
- Tests validate schema conformance, depth-appropriate null/non-null fields, and data presence
- Nonexistent table test uses a unique name (`nonexistent_table_xyz_12345`) to avoid false positives

---

## Dependencies

```
Task 1 ──► Task 2 ──► Task 3 ──► Task 4
```

- Task 2 depends on Task 1 (needs trait methods, SQL builder, type mapping)
- Task 3 depends on Task 2 (needs parsing, builder, and the simpler Connection methods)
- Task 4 depends on Task 3 (needs all Connection metadata methods implemented)

## Risk / Notes

- `SHOW COLUMNS IN ALL CATALOGS` is not yet available server-side — fan-out per catalog is the workaround
- The nested Arrow builder for `get_objects` (`builder.rs`) is the most complex piece — building deeply nested `StructBuilder`/`ListBuilder` hierarchies matching `GET_OBJECTS_SCHEMA`
- Table type constraints (primary/foreign keys) are left as empty lists in the initial implementation per design doc
- Integration tests require a live Databricks workspace with `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` env vars
