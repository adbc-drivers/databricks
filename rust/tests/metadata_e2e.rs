// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! E2E tests for all metadata operations: catalogs, schemas, tables, columns,
//! primary keys, foreign keys, cross-references, procedures, and procedure columns.
//!
//! These tests require a real Databricks connection and are marked with `#[ignore]`.
//!
//! # Setup
//!
//! ```bash
//! export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
//! export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
//! export DATABRICKS_TOKEN="your-pat-token"
//! ```
//!
//! # Running
//!
//! ```bash
//! cargo test --test metadata_e2e -- --ignored --nocapture
//! ```

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Database as _;
use adbc_core::Driver as _;
use adbc_core::Optionable;
use adbc_core::Statement as _;
use arrow_array::{Array, RecordBatchReader, StringArray};
use databricks_adbc::metadata::sql::SqlCommandBuilder;
use databricks_adbc::Driver;

fn get_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!(
            "Environment variable {} is required. See test file docs.",
            name
        )
    })
}

fn create_connection() -> impl adbc_core::Connection {
    let host = get_env("DATABRICKS_HOST");
    let http_path = get_env("DATABRICKS_HTTP_PATH");
    let token = get_env("DATABRICKS_TOKEN");

    let mut driver = Driver::new();
    let mut db = driver.new_database().expect("Failed to create database");

    db.set_option(OptionDatabase::Uri, OptionValue::String(host))
        .expect("Failed to set uri");
    db.set_option(
        OptionDatabase::Other("databricks.http_path".into()),
        OptionValue::String(http_path),
    )
    .expect("Failed to set http_path");
    db.set_option(
        OptionDatabase::Other("databricks.auth.type".into()),
        OptionValue::String("access_token".into()),
    )
    .expect("Failed to set auth type");
    db.set_option(
        OptionDatabase::Other("databricks.access_token".into()),
        OptionValue::String(token),
    )
    .expect("Failed to set access_token");

    db.new_connection().expect("Failed to create connection")
}

/// Execute a SQL query and return (schema, row_count, first row's first column as String).
fn execute_query(
    conn: &mut impl adbc_core::Connection,
    sql: &str,
) -> (arrow_schema::SchemaRef, usize, Option<String>) {
    let mut stmt = conn.new_statement().expect("Failed to create statement");
    stmt.set_sql_query(sql).expect("Failed to set query");
    let reader = stmt.execute().expect("Failed to execute");
    let schema = reader.schema();

    let mut total_rows = 0;
    let mut first_value = None;
    for batch_result in reader {
        let batch = batch_result.expect("Batch error");
        if first_value.is_none() && batch.num_rows() > 0 {
            if let Some(arr) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                if !arr.is_null(0) {
                    first_value = Some(arr.value(0).to_string());
                }
            }
        }
        total_rows += batch.num_rows();
    }

    (schema, total_rows, first_value)
}

// ─── SHOW CATALOGS ──────────────────────────────────────────────────────────

#[test]
#[ignore]
fn test_metadata_get_catalogs() {
    let mut conn = create_connection();

    let (schema, row_count, first_val) = execute_query(&mut conn, "SHOW CATALOGS");

    println!("getCatalogs: {} rows", row_count);
    assert!(row_count > 0, "Expected at least one catalog");
    assert!(
        schema.column_with_name("catalog").is_some(),
        "Expected 'catalog' column, got: {:?}",
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );
    println!("  first catalog: {:?}", first_val);
    println!("  PASS");
}

// ─── SHOW SCHEMAS ───────────────────────────────────────────────────────────

#[test]
#[ignore]
fn test_metadata_get_schemas() {
    let mut conn = create_connection();

    let (schema, row_count, _) = execute_query(&mut conn, "SHOW SCHEMAS IN CATALOG `main`");

    println!("getSchemas (catalog=main): {} rows", row_count);
    assert!(row_count > 0, "Expected at least one schema in 'main'");
    assert!(
        schema.column_with_name("databaseName").is_some()
            || schema.column_with_name("namespace").is_some()
            || schema.column_with_name("schema_name").is_some(),
        "Expected a schema name column, got: {:?}",
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );
    println!("  PASS");
}

#[test]
#[ignore]
fn test_metadata_get_schemas_all_catalogs() {
    let mut conn = create_connection();

    let (_, row_count, _) = execute_query(&mut conn, "SHOW SCHEMAS IN ALL CATALOGS");

    println!("getSchemas (all catalogs): {} rows", row_count);
    assert!(
        row_count > 0,
        "Expected at least one schema across all catalogs"
    );
    println!("  PASS");
}

// ─── SHOW TABLES ────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn test_metadata_get_tables() {
    let mut conn = create_connection();

    let (schema, row_count, _) = execute_query(&mut conn, "SHOW TABLES IN CATALOG `main`");

    println!(
        "getTables (catalog=main): {} rows, columns: {:?}",
        row_count,
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );
    // main catalog should have at least some tables (information_schema views)
    assert!(row_count > 0, "Expected at least one table in 'main'");
    println!("  PASS");
}

// ─── SHOW COLUMNS ───────────────────────────────────────────────────────────

#[test]
#[ignore]
fn test_metadata_get_columns() {
    let mut conn = create_connection();

    let (schema, row_count, _) = execute_query(
        &mut conn,
        "SHOW COLUMNS IN CATALOG `main` SCHEMA LIKE 'information_schema' TABLE LIKE 'tables'",
    );

    println!(
        "getColumns (main.information_schema.tables): {} rows, columns: {:?}",
        row_count,
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );
    assert!(
        row_count > 0,
        "Expected columns for information_schema.tables"
    );
    println!("  PASS");
}

// ─── SHOW KEYS (Primary Keys) ──────────────────────────────────────────────

#[test]
#[ignore]
fn test_metadata_get_primary_keys() {
    let mut conn = create_connection();

    // Primary keys may not exist on most tables; just verify the query executes
    let (schema, row_count, _) = execute_query(
        &mut conn,
        "SHOW KEYS IN CATALOG `main` IN SCHEMA `information_schema` IN TABLE `tables`",
    );

    println!(
        "getPrimaryKeys (main.information_schema.tables): {} rows, columns: {:?}",
        row_count,
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );
    // row_count may be 0 — that's fine, query executed successfully
    println!("  PASS");
}

// ─── SHOW FOREIGN KEYS ─────────────────────────────────────────────────────

#[test]
#[ignore]
fn test_metadata_get_foreign_keys() {
    let mut conn = create_connection();

    let (schema, row_count, _) = execute_query(
        &mut conn,
        "SHOW FOREIGN KEYS IN CATALOG `main` IN SCHEMA `information_schema` IN TABLE `tables`",
    );

    println!(
        "getForeignKeys (main.information_schema.tables): {} rows, columns: {:?}",
        row_count,
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );
    println!("  PASS");
}

// ─── getProcedures (uses SqlCommandBuilder to exercise actual code path) ─────

#[test]
#[ignore]
fn test_metadata_get_procedures_cross_catalog() {
    let mut conn = create_connection();

    // Use the actual SQL builder — tests build_get_procedures with None catalog
    let sql = SqlCommandBuilder::build_get_procedures(None, None, None);
    println!("getProcedures SQL (cross-catalog): {}", sql);
    let (schema, row_count, _) = execute_query(&mut conn, &sql);

    println!("getProcedures (cross-catalog): {} rows", row_count);
    assert!(schema.column_with_name("routine_catalog").is_some());
    assert!(schema.column_with_name("routine_name").is_some());
    assert!(schema.column_with_name("specific_name").is_some());
    assert!(schema.column_with_name("comment").is_some());
    println!("  PASS");
}

#[test]
#[ignore]
fn test_metadata_get_procedures_specific_catalog() {
    let mut conn = create_connection();

    // Tests build_get_procedures with specific catalog
    let sql = SqlCommandBuilder::build_get_procedures(Some("main"), None, None);
    println!("getProcedures SQL (catalog=main): {}", sql);
    let (schema, row_count, _) = execute_query(&mut conn, &sql);

    println!(
        "getProcedures (catalog=main): {} rows, {} cols",
        row_count,
        schema.fields().len()
    );
    println!("  PASS");
}

#[test]
#[ignore]
fn test_metadata_get_procedures_with_schema_filter() {
    let mut conn = create_connection();

    // Tests build_get_procedures with schema pattern
    let sql = SqlCommandBuilder::build_get_procedures(None, Some("default"), None);
    println!("getProcedures SQL (schema=default): {}", sql);
    let (_, row_count, _) = execute_query(&mut conn, &sql);

    println!("getProcedures (schema=default): {} rows", row_count);
    println!("  PASS");
}

/// Verify the static procedures_schema matches expected columns.
/// This is a local-only test (no server round-trip needed).
#[test]
fn test_metadata_procedures_empty_catalog_schema() {
    let schema = databricks_adbc::metadata::sql::procedures_schema();
    assert_eq!(schema.fields().len(), 5);
    assert!(schema.column_with_name("routine_catalog").is_some());
    assert!(schema.column_with_name("routine_schema").is_some());
    assert!(schema.column_with_name("routine_name").is_some());
    assert!(schema.column_with_name("comment").is_some());
    assert!(schema.column_with_name("specific_name").is_some());
}

// ─── getProcedureColumns (uses SqlCommandBuilder) ───────────────────────────

#[test]
#[ignore]
fn test_metadata_get_procedure_columns_cross_catalog() {
    let mut conn = create_connection();

    // Use the actual SQL builder — tests build_get_procedure_columns with None catalog
    let sql = SqlCommandBuilder::build_get_procedure_columns(None, None, None, None);
    println!("getProcedureColumns SQL (cross-catalog): {}", sql);
    let (schema, row_count, _) = execute_query(&mut conn, &sql);

    println!("getProcedureColumns (cross-catalog): {} rows", row_count);
    assert!(schema.column_with_name("specific_catalog").is_some());
    assert!(schema.column_with_name("parameter_name").is_some());
    assert!(schema.column_with_name("data_type").is_some());
    assert!(schema.column_with_name("ordinal_position").is_some());
    assert!(schema.column_with_name("comment").is_some());
    println!("  PASS");
}

/// Verify the static procedure_columns_schema matches expected columns.
/// This is a local-only test (no server round-trip needed).
#[test]
fn test_metadata_procedure_columns_empty_catalog_schema() {
    let schema = databricks_adbc::metadata::sql::procedure_columns_schema();
    assert_eq!(schema.fields().len(), 16);
    assert!(schema.column_with_name("specific_catalog").is_some());
    assert!(schema.column_with_name("parameter_name").is_some());
    assert!(schema.column_with_name("data_type").is_some());
    assert!(schema.column_with_name("ordinal_position").is_some());
    assert!(schema.column_with_name("comment").is_some());
}
