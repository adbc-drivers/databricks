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

//! Connection metadata methods test.
//!
//! This example exercises the ADBC Connection metadata methods:
//! - `get_table_types()` — returns supported table types
//! - `get_objects()` — returns catalog/schema/table/column hierarchy
//! - `get_table_schema()` — returns Arrow schema for a specific table
//!
//! Watch the debug logs for the generated SHOW SQL commands and parsed
//! result counts at each depth level.
//!
//! Run with:
//! ```bash
//! RUST_LOG=debug cargo run --example metadata_test
//! ```
//!
//! Or filter to driver logs only:
//! ```bash
//! RUST_LOG=databricks_adbc=debug cargo run --example metadata_test
//! ```

use adbc_core::options::{ObjectDepth, OptionDatabase, OptionValue};
use adbc_core::Connection as ConnectionTrait;
use adbc_core::Database as DatabaseTrait;
use adbc_core::Driver as DriverTrait;
use adbc_core::Optionable;
use adbc_core::Statement as StatementTrait;
use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, RecordBatchReader, StructArray};
use databricks_adbc::Driver;
use std::time::Instant;

fn main() {
    // Initialize tracing with DEBUG level to see SQL commands and metadata flow
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .init();

    // Connection parameters from environment variables
    let host =
        std::env::var("DATABRICKS_HOST").expect("DATABRICKS_HOST environment variable required");
    let http_path = std::env::var("DATABRICKS_HTTP_PATH")
        .expect("DATABRICKS_HTTP_PATH environment variable required");
    let token =
        std::env::var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN environment variable required");

    println!("=== Connection Metadata Methods Test ===\n");
    println!("Host: {}", &host);
    println!("HTTP Path: {}", &http_path);
    println!();
    println!("This test exercises get_table_types, get_objects, and get_table_schema.");
    println!("Watch the debug logs for:");
    println!("  - SQL commands sent to the server (SHOW CATALOGS, SHOW SCHEMAS, etc.)");
    println!("  - Parsed result counts at each depth level");
    println!("  - Catalog resolution and column fetching in get_table_schema");
    println!();

    // Create driver and database
    let mut driver = Driver::new();
    let mut db = driver.new_database().expect("Failed to create database");

    // Set options
    db.set_option(OptionDatabase::Uri, OptionValue::String(host))
        .expect("Failed to set uri");
    db.set_option(
        OptionDatabase::Other("databricks.http_path".into()),
        OptionValue::String(http_path),
    )
    .expect("Failed to set http_path");
    db.set_option(
        OptionDatabase::Other("databricks.access_token".into()),
        OptionValue::String(token),
    )
    .expect("Failed to set access_token");

    let mut conn = db.new_connection().expect("Failed to create connection");

    // --- Test 1: get_info ---
    println!("--- Test 1: get_info ---");
    test_get_info(&conn);

    // --- Test 2: get_table_types ---
    println!("\n--- Test 2: get_table_types ---");
    test_get_table_types(&conn);

    // --- Test 3: get_objects at Catalogs depth ---
    println!("\n--- Test 3: get_objects(Catalogs) ---");
    test_get_objects_catalogs(&conn);

    // --- Test 4: get_objects at Schemas depth ---
    println!("\n--- Test 4: get_objects(Schemas) - filtered to 'main' catalog ---");
    test_get_objects_schemas(&conn);

    // --- Test 5: get_objects at Tables depth ---
    println!("\n--- Test 5: get_objects(Tables) - filtered to 'main.default' ---");
    test_get_objects_tables(&conn);

    // --- Test 6: get_objects at All depth (includes columns) ---
    println!("\n--- Test 6: get_objects(All) - single table with columns ---");
    test_get_objects_all(&mut conn);

    // --- Test 7: get_table_schema ---
    println!("\n--- Test 7: get_table_schema ---");
    test_get_table_schema(&conn);

    // --- Test 8: get_table_schema with catalog resolution ---
    println!("\n--- Test 8: get_table_schema (auto catalog resolution) ---");
    test_get_table_schema_auto_catalog(&conn);

    println!("\n=== All Metadata Tests Complete ===");
}

fn test_get_info(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn.get_info(None).expect("get_info failed");

    let mut total_rows = 0u64;
    for batch_result in reader.by_ref() {
        match batch_result {
            Ok(batch) => {
                total_rows += batch.num_rows() as u64;
                print_info_batch(&batch);
            }
            Err(e) => {
                eprintln!("  Error reading batch: {:?}", e);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    println!("  Total info entries: {}", total_rows);
    println!("  Time: {:.3}s", elapsed.as_secs_f64());
}

fn print_info_batch(batch: &RecordBatch) {
    let codes = batch
        .column(0)
        .as_primitive::<arrow_array::types::UInt32Type>();

    for i in 0..batch.num_rows() {
        let code = codes.value(i);
        let info_name = match code {
            0 => "VendorName",
            1 => "VendorVersion",
            2 => "VendorArrowVersion",
            100 => "DriverName",
            101 => "DriverVersion",
            102 => "DriverArrowVersion",
            103 => "DriverADBCVersion",
            _ => "Unknown",
        };
        println!("  {} (code={})", info_name, code);
    }
}

fn test_get_table_types(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn.get_table_types().expect("get_table_types failed");

    let schema = reader.schema();
    println!("  Schema: {:?}", schema);

    for batch_result in reader.by_ref() {
        match batch_result {
            Ok(batch) => {
                let col = batch.column(0).as_string::<i32>();
                for i in 0..batch.num_rows() {
                    println!("  Table type: {}", col.value(i));
                }
            }
            Err(e) => {
                eprintln!("  Error reading batch: {:?}", e);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    println!("  Time: {:.3}s", elapsed.as_secs_f64());
}

fn test_get_objects_catalogs(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn
        .get_objects(ObjectDepth::Catalogs, None, None, None, None, None)
        .expect("get_objects(Catalogs) failed");

    let schema = reader.schema();
    println!("  Schema fields: {}", schema.fields().len());

    let mut total_catalogs = 0u64;
    for batch_result in reader.by_ref() {
        match batch_result {
            Ok(batch) => {
                let names = batch.column(0).as_string::<i32>();
                for i in 0..batch.num_rows() {
                    println!("  Catalog: {}", names.value(i));
                    total_catalogs += 1;
                }
            }
            Err(e) => {
                eprintln!("  Error reading batch: {:?}", e);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    println!("  Total catalogs: {}", total_catalogs);
    println!("  Time: {:.3}s", elapsed.as_secs_f64());
}

/// Helper to downcast a ListArray value (Arc<dyn Array>) to StructArray reference.
fn as_struct(array: &dyn Array) -> &StructArray {
    array.as_any().downcast_ref::<StructArray>().unwrap()
}

fn test_get_objects_schemas(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn
        .get_objects(ObjectDepth::Schemas, Some("main"), None, None, None, None)
        .expect("get_objects(Schemas) failed");

    let mut total_schemas = 0u64;
    for batch_result in reader.by_ref() {
        match batch_result {
            Ok(batch) => {
                let catalog_names = batch.column(0).as_string::<i32>();
                let db_schemas_list = batch.column(1).as_list::<i32>();

                for i in 0..batch.num_rows() {
                    let catalog = catalog_names.value(i);
                    if db_schemas_list.is_null(i) {
                        println!("  {}: (no schemas)", catalog);
                        continue;
                    }
                    let schemas_arr = db_schemas_list.value(i);
                    let schemas_struct = as_struct(schemas_arr.as_ref());
                    let schema_names = schemas_struct.column(0).as_string::<i32>();

                    for j in 0..schemas_struct.len() {
                        println!("  {}.{}", catalog, schema_names.value(j));
                        total_schemas += 1;
                    }
                }
            }
            Err(e) => {
                eprintln!("  Error reading batch: {:?}", e);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    println!("  Total schemas: {}", total_schemas);
    println!("  Time: {:.3}s", elapsed.as_secs_f64());
}

fn test_get_objects_tables(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn
        .get_objects(
            ObjectDepth::Tables,
            Some("main"),
            Some("default"),
            None,
            None,
            None,
        )
        .expect("get_objects(Tables) failed");

    let mut total_tables = 0u64;
    for batch_result in reader.by_ref() {
        match batch_result {
            Ok(batch) => {
                let catalog_names = batch.column(0).as_string::<i32>();
                let db_schemas_list = batch.column(1).as_list::<i32>();

                for i in 0..batch.num_rows() {
                    let catalog = catalog_names.value(i);
                    if db_schemas_list.is_null(i) {
                        continue;
                    }
                    let schemas_arr = db_schemas_list.value(i);
                    let schemas_struct = as_struct(schemas_arr.as_ref());
                    let schema_names = schemas_struct.column(0).as_string::<i32>();
                    let tables_lists = schemas_struct.column(1).as_list::<i32>();

                    for j in 0..schemas_struct.len() {
                        let schema_name = schema_names.value(j);
                        if tables_lists.is_null(j) {
                            continue;
                        }
                        let tables_arr = tables_lists.value(j);
                        let tables_struct = as_struct(tables_arr.as_ref());
                        let table_names = tables_struct.column(0).as_string::<i32>();
                        let table_types = tables_struct.column(1).as_string::<i32>();

                        for k in 0..tables_struct.len() {
                            println!(
                                "  {}.{}.{} ({})",
                                catalog,
                                schema_name,
                                table_names.value(k),
                                table_types.value(k)
                            );
                            total_tables += 1;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("  Error reading batch: {:?}", e);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    println!("  Total tables: {}", total_tables);
    println!("  Time: {:.3}s", elapsed.as_secs_f64());
}

fn test_get_objects_all(conn: &mut impl ConnectionTrait) {
    // First, create a temp table so we have a known target
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(
            "CREATE TABLE IF NOT EXISTS main.default.__adbc_metadata_test (id INT, name STRING, value DOUBLE)",
        )
        .expect("Failed to set query");
        let _ = stmt.execute();
    }

    let start = Instant::now();
    let mut reader = conn
        .get_objects(
            ObjectDepth::All,
            Some("main"),
            Some("default"),
            Some("__adbc_metadata_test"),
            None,
            None,
        )
        .expect("get_objects(All) failed");

    for batch_result in reader.by_ref() {
        match batch_result {
            Ok(batch) => {
                let catalog_names = batch.column(0).as_string::<i32>();
                let db_schemas_list = batch.column(1).as_list::<i32>();

                for i in 0..batch.num_rows() {
                    let catalog = catalog_names.value(i);
                    if db_schemas_list.is_null(i) {
                        continue;
                    }
                    let schemas_arr = db_schemas_list.value(i);
                    let schemas_struct = as_struct(schemas_arr.as_ref());
                    let schema_names = schemas_struct.column(0).as_string::<i32>();
                    let tables_lists = schemas_struct.column(1).as_list::<i32>();

                    for j in 0..schemas_struct.len() {
                        let schema_name = schema_names.value(j);
                        if tables_lists.is_null(j) {
                            continue;
                        }
                        let tables_arr = tables_lists.value(j);
                        let tables_struct = as_struct(tables_arr.as_ref());
                        let table_names = tables_struct.column(0).as_string::<i32>();
                        let table_types = tables_struct.column(1).as_string::<i32>();
                        let columns_lists = tables_struct.column(2).as_list::<i32>();

                        for k in 0..tables_struct.len() {
                            println!(
                                "  {}.{}.{} ({})",
                                catalog,
                                schema_name,
                                table_names.value(k),
                                table_types.value(k)
                            );

                            if columns_lists.is_null(k) {
                                println!("    (no columns)");
                                continue;
                            }
                            let cols_arr = columns_lists.value(k);
                            let cols_struct = as_struct(cols_arr.as_ref());
                            let col_names = cols_struct.column(0).as_string::<i32>();
                            let col_ordinals = cols_struct
                                .column(1)
                                .as_primitive::<arrow_array::types::Int32Type>();

                            for c in 0..cols_struct.len() {
                                let ordinal = if col_ordinals.is_null(c) {
                                    "?".to_string()
                                } else {
                                    col_ordinals.value(c).to_string()
                                };
                                println!(
                                    "    column[{}]: {} (ordinal={})",
                                    c,
                                    col_names.value(c),
                                    ordinal
                                );
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("  Error reading batch: {:?}", e);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    println!("  Time: {:.3}s", elapsed.as_secs_f64());

    // Drop reader before borrowing conn mutably for cleanup
    drop(reader);

    // Cleanup
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query("DROP TABLE IF EXISTS main.default.__adbc_metadata_test")
            .expect("Failed to set query");
        let _ = stmt.execute();
    }
}

fn test_get_table_schema(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let schema = conn
        .get_table_schema(Some("main"), Some("information_schema"), "tables")
        .expect("get_table_schema failed");

    println!("  Table: main.information_schema.tables");
    println!("  Fields ({}):", schema.fields().len());
    for (i, field) in schema.fields().iter().enumerate() {
        println!(
            "    [{}] {} : {} (nullable={})",
            i,
            field.name(),
            field.data_type(),
            field.is_nullable()
        );
    }

    let elapsed = start.elapsed();
    println!("  Time: {:.3}s", elapsed.as_secs_f64());
}

fn test_get_table_schema_auto_catalog(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    // Pass None for catalog to test auto-resolution via list_tables
    let schema = conn
        .get_table_schema(None, Some("information_schema"), "columns")
        .expect("get_table_schema (auto catalog) failed");

    println!("  Table: ?.information_schema.columns (catalog auto-resolved)");
    println!("  Fields ({}):", schema.fields().len());
    for (i, field) in schema.fields().iter().enumerate() {
        println!(
            "    [{}] {} : {} (nullable={})",
            i,
            field.name(),
            field.data_type(),
            field.is_nullable()
        );
    }

    let elapsed = start.elapsed();
    println!("  Time: {:.3}s", elapsed.as_secs_f64());
}
