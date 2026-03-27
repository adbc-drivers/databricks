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
use databricks_adbc::Driver;

/// getProcedures SQL for cross-catalog (NULL catalog → system).
const PROCEDURES_SQL_CROSS_CATALOG: &str = "\
    SELECT routine_catalog, routine_schema, routine_name, comment, specific_name \
    FROM system.information_schema.routines \
    WHERE routine_type = 'PROCEDURE' \
    ORDER BY routine_catalog, routine_schema, routine_name";

/// getProcedures SQL for specific catalog.
fn procedures_sql_for_catalog(catalog: &str) -> String {
    format!(
        "SELECT routine_catalog, routine_schema, routine_name, comment, specific_name \
         FROM `{}`.information_schema.routines \
         WHERE routine_type = 'PROCEDURE' \
         ORDER BY routine_catalog, routine_schema, routine_name",
        catalog.replace('`', "``")
    )
}

/// getProcedures SQL with schema filter.
fn procedures_sql_with_schema(schema_pattern: &str) -> String {
    format!(
        "SELECT routine_catalog, routine_schema, routine_name, comment, specific_name \
         FROM system.information_schema.routines \
         WHERE routine_type = 'PROCEDURE' \
         AND routine_schema LIKE '{}' \
         ORDER BY routine_catalog, routine_schema, routine_name",
        schema_pattern
    )
}

/// getProcedureColumns SQL for cross-catalog (NULL catalog → system).
const PROCEDURE_COLUMNS_SQL_CROSS_CATALOG: &str = "\
    SELECT \
    p.specific_catalog, p.specific_schema, p.specific_name, \
    p.parameter_name, p.parameter_mode, p.is_result, \
    p.data_type, p.full_data_type, \
    p.numeric_precision, p.numeric_precision_radix, p.numeric_scale, \
    p.character_maximum_length, p.character_octet_length, \
    p.ordinal_position, p.parameter_default, p.comment \
    FROM system.information_schema.parameters p \
    JOIN system.information_schema.routines r \
    ON p.specific_catalog = r.specific_catalog \
    AND p.specific_schema = r.specific_schema \
    AND p.specific_name = r.specific_name \
    WHERE r.routine_type = 'PROCEDURE' \
    ORDER BY p.specific_catalog, p.specific_schema, p.specific_name, p.ordinal_position";

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

// ─── getProcedures ──────────────────────────────────────────────────────────

#[test]
#[ignore]
fn test_metadata_get_procedures_cross_catalog() {
    let mut conn = create_connection();

    let (schema, row_count, _) = execute_query(&mut conn, PROCEDURES_SQL_CROSS_CATALOG);

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

    let sql = procedures_sql_for_catalog("main");
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

    let sql = procedures_sql_with_schema("default");
    let (_, row_count, _) = execute_query(&mut conn, &sql);

    println!("getProcedures (schema=default): {} rows", row_count);
    println!("  PASS");
}

// ─── getProcedureColumns ────────────────────────────────────────────────────

#[test]
#[ignore]
fn test_metadata_get_procedure_columns_cross_catalog() {
    let mut conn = create_connection();

    let (schema, row_count, _) = execute_query(&mut conn, PROCEDURE_COLUMNS_SQL_CROSS_CATALOG);

    println!("getProcedureColumns (cross-catalog): {} rows", row_count);
    assert!(schema.column_with_name("specific_catalog").is_some());
    assert!(schema.column_with_name("parameter_name").is_some());
    assert!(schema.column_with_name("data_type").is_some());
    assert!(schema.column_with_name("ordinal_position").is_some());
    assert!(schema.column_with_name("comment").is_some());
    println!("  PASS");
}
