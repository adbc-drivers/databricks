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

//! Logging configuration example.
//!
//! Demonstrates two ways to enable debug logging:
//!
//! 1. **ADBC options** (recommended for ODBC consumers):
//!    ```bash
//!    cargo run --example logging_test
//!    ```
//!    The driver writes logs to a file configured via `databricks.log_file`.
//!
//! 2. **RUST_LOG environment variable** (standalone / interactive use):
//!    ```bash
//!    RUST_LOG=databricks_adbc=debug cargo run --example logging_test
//!    ```
//!    Logs go to stderr. This takes effect only if no `databricks.log_level`
//!    option is set.

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Connection as _;
use adbc_core::Database as _;
use adbc_core::Driver as _;
use adbc_core::Optionable;
use adbc_core::Statement as _;
use databricks_adbc::Driver;

fn main() {
    let host =
        std::env::var("DATABRICKS_HOST").expect("DATABRICKS_HOST environment variable required");
    let http_path = std::env::var("DATABRICKS_HTTP_PATH")
        .expect("DATABRICKS_HTTP_PATH environment variable required");
    let token =
        std::env::var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN environment variable required");

    let log_file = std::env::var("LOG_FILE").unwrap_or_else(|_| "/tmp/databricks-adbc.log".into());

    println!("=== Logging Configuration Example ===\n");
    println!("Log level : DEBUG");
    println!("Log file  : {}\n", log_file);

    // Create driver and database
    let mut driver = Driver::new();
    let mut database = driver.new_database().expect("Failed to create database");

    database
        .set_option(OptionDatabase::Uri, OptionValue::String(host))
        .expect("Failed to set uri");
    database
        .set_option(
            OptionDatabase::Other("databricks.http_path".into()),
            OptionValue::String(http_path),
        )
        .expect("Failed to set http_path");
    database
        .set_option(
            OptionDatabase::Other("databricks.access_token".into()),
            OptionValue::String(token),
        )
        .expect("Failed to set access_token");

    // Configure logging via ADBC options — the driver initializes tracing-subscriber
    // on the first new_connection() call.
    database
        .set_option(
            OptionDatabase::Other("databricks.log_level".into()),
            OptionValue::String("debug".into()),
        )
        .expect("Failed to set log_level");
    database
        .set_option(
            OptionDatabase::Other("databricks.log_file".into()),
            OptionValue::String(log_file.clone()),
        )
        .expect("Failed to set log_file");

    // Connect — this triggers logging initialization and emits the first log lines
    let mut connection = database.new_connection().expect("Failed to connect");
    println!("Connected (session_id: {})\n", connection.session_id());

    // Execute a query so we can see statement and result-fetch logs
    let mut statement = connection
        .new_statement()
        .expect("Failed to create statement");
    statement
        .set_sql_query("SELECT 1 AS value")
        .expect("Failed to set query");

    let mut reader = statement.execute().expect("Failed to execute");
    let batch = reader.next().expect("No batch").expect("Batch error");
    println!(
        "Query returned {} row(s), {} column(s)\n",
        batch.num_rows(),
        batch.num_columns()
    );

    // Show the log file contents
    println!("=== Log file contents ({}) ===\n", log_file);
    match std::fs::read_to_string(&log_file) {
        Ok(contents) => print!("{}", contents),
        Err(e) => eprintln!("Failed to read log file: {}", e),
    }
}
