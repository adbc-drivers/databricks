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

//! Example: Execute a query via the ADBC FFI boundary.
//!
//! This loads libdatabricks_adbc.so via dlopen (just like C++ would),
//! then uses the ManagedDriver API to execute a query and print results.
//!
//! Run with:
//! ```bash
//! cargo run --example ffi_query --features ffi
//! ```
//!
//! Required environment variables:
//! - DATABRICKS_HOST: Workspace URL (e.g. "https://my-workspace.databricks.com")
//! - DATABRICKS_HTTP_PATH: Warehouse HTTP path (e.g. "/sql/1.0/warehouses/abc123")
//! - DATABRICKS_TOKEN: Personal access token

use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Connection as _, Database as _, Driver as _, Optionable, Statement as _};
use adbc_driver_manager::ManagedDriver;
use arrow_array::{RecordBatch, RecordBatchReader};

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .init();

    let host = std::env::var("DATABRICKS_HOST").expect("DATABRICKS_HOST required");
    let http_path = std::env::var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH required");
    let token = std::env::var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN required");

    // 1. Load the shared library (same as C++ dlopen + dlsym)
    let mut driver = ManagedDriver::load_dynamic_from_name(
        "databricks_adbc",
        Some(b"AdbcDatabricksInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    // 2. Create and configure database
    let mut database = driver.new_database().expect("Failed to create database");
    database
        .set_option(OptionDatabase::Uri, OptionValue::String(host))
        .unwrap();
    database
        .set_option(
            OptionDatabase::Other("databricks.http_path".into()),
            OptionValue::String(http_path),
        )
        .unwrap();
    database
        .set_option(
            OptionDatabase::Other("databricks.access_token".into()),
            OptionValue::String(token),
        )
        .unwrap();

    // 3. Create connection (opens Databricks session)
    let mut connection = database
        .new_connection()
        .expect("Failed to create connection");

    // 4. Execute query
    let mut statement = connection
        .new_statement()
        .expect("Failed to create statement");
    statement
        .set_sql_query("SELECT 1 AS id, 'hello from FFI' AS message")
        .unwrap();
    let mut reader = statement.execute().expect("Failed to execute");

    // 5. Print results
    let schema = reader.schema();
    println!("Schema: {schema}");
    for batch in &mut reader {
        let batch: RecordBatch = batch.expect("Failed to read batch");
        println!(
            "Batch: {} rows x {} cols",
            batch.num_rows(),
            batch.num_columns()
        );
        for col in 0..batch.num_columns() {
            let col_data = batch.column(col);
            println!("  {}: {:?}", schema.field(col).name(), col_data);
        }
    }
    println!("Done.");
}
