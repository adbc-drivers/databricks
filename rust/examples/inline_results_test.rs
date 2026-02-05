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

//! Inline Arrow results test to verify behavior with small result sets.
//!
//! This example tests the inline Arrow results path, which is used when the server
//! returns small result sets directly in the API response (via the `attachment` field)
//! rather than using CloudFetch presigned URLs.
//!
//! The server decides whether to use inline or CloudFetch based on:
//! - Result size (smaller results favor inline)
//! - Latency characteristics (inline avoids extra HTTP roundtrips)
//!
//! Run with:
//! ```bash
//! RUST_LOG=debug cargo run --example inline_results_test
//! ```
//!
//! Or with tracing output:
//! ```bash
//! RUST_LOG=databricks_adbc=debug cargo run --example inline_results_test
//! ```

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Connection as ConnectionTrait;
use adbc_core::Database as DatabaseTrait;
use adbc_core::Driver as DriverTrait;
use adbc_core::Optionable;
use adbc_core::Statement as StatementTrait;
use arrow_array::{RecordBatch, RecordBatchReader};
use databricks_adbc::Driver;
use std::time::Instant;

fn main() {
    // Initialize tracing with DEBUG level to see inline vs CloudFetch decisions
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

    println!("=== Inline Arrow Results Test ===\n");
    println!("Host: {}", &host);
    println!("HTTP Path: {}", &http_path);
    println!();
    println!("This test executes small queries that should return inline Arrow results");
    println!("rather than using CloudFetch. Watch the debug logs for:");
    println!("  - 'Using inline Arrow reader' - confirms inline path");
    println!("  - 'Using CloudFetch reader' - indicates CloudFetch path");
    println!();

    // Create driver and database
    let mut driver = Driver::new();
    let mut db = driver.new_database().expect("Failed to create database");

    // Set options
    db.set_option(OptionDatabase::Uri, OptionValue::String(host.into()))
        .expect("Failed to set uri");
    db.set_option(
        OptionDatabase::Other("databricks.http_path".into()),
        OptionValue::String(http_path.into()),
    )
    .expect("Failed to set http_path");
    db.set_option(
        OptionDatabase::Other("databricks.access_token".into()),
        OptionValue::String(token.into()),
    )
    .expect("Failed to set access_token");

    let mut conn = db.new_connection().expect("Failed to create connection");

    // Test 1: Very small result set (likely inline)
    println!("--- Test 1: Very Small Result (10 rows) ---");
    run_query(
        &mut conn,
        "SELECT id, id * 2 as doubled FROM RANGE(10)",
        "very_small",
    );

    // Test 2: Small result set (likely inline)
    println!("\n--- Test 2: Small Result (100 rows) ---");
    run_query(
        &mut conn,
        "SELECT id, id * 2 as doubled, CONCAT('row_', CAST(id AS STRING)) as label FROM RANGE(100)",
        "small",
    );

    // Test 3: Medium result set (may be inline or CloudFetch depending on server)
    println!("\n--- Test 3: Medium Result (1000 rows) ---");
    run_query(
        &mut conn,
        "SELECT id, id * 2 as doubled, CONCAT('row_', CAST(id AS STRING)) as label, RAND() as random_val FROM RANGE(1000)",
        "medium",
    );

    // Test 4: Empty result set
    println!("\n--- Test 4: Empty Result ---");
    run_query(&mut conn, "SELECT * FROM RANGE(10) WHERE id < 0", "empty");

    // Test 5: Single row result
    println!("\n--- Test 5: Single Row Result ---");
    run_query(
        &mut conn,
        "SELECT 1 as one, 'hello' as greeting, CURRENT_TIMESTAMP() as ts",
        "single_row",
    );

    // Test 6: Result with various data types
    println!("\n--- Test 6: Various Data Types ---");
    run_query(
        &mut conn,
        r#"
        SELECT
            CAST(1 AS TINYINT) as tiny,
            CAST(100 AS SMALLINT) as small,
            CAST(1000 AS INT) as medium_int,
            CAST(1000000 AS BIGINT) as big,
            CAST(3.14 AS FLOAT) as pi_float,
            CAST(3.14159265359 AS DOUBLE) as pi_double,
            'hello' as str,
            CAST('world' AS BINARY) as bin,
            TRUE as flag,
            CURRENT_DATE() as today,
            CURRENT_TIMESTAMP() as now
        "#,
        "data_types",
    );

    println!("\n=== All Tests Complete ===");
    println!("\nNote: Whether inline or CloudFetch was used depends on:");
    println!("  - Server-side result size thresholds");
    println!("  - Network latency characteristics");
    println!("  - Current server configuration");
}

fn run_query(conn: &mut impl ConnectionTrait, sql: &str, test_name: &str) {
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    println!("Executing query for test '{}'...", test_name);

    let start = Instant::now();
    stmt.set_sql_query(sql).expect("Failed to set query");

    let mut reader = match stmt.execute() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("  Error executing query: {:?}", e);
            return;
        }
    };

    let schema = reader.schema();
    let num_fields = schema.fields().len();

    let mut total_rows = 0u64;
    let mut total_batches = 0u64;

    while let Some(batch_result) = reader.next() {
        let batch_result: Result<RecordBatch, _> = batch_result;
        match batch_result {
            Ok(batch) => {
                let rows = batch.num_rows();
                total_rows += rows as u64;
                total_batches += 1;
            }
            Err(e) => {
                eprintln!("  Error reading batch: {:?}", e);
                break;
            }
        }
    }

    let elapsed = start.elapsed();

    println!("  Schema: {} fields", num_fields);
    println!("  Total batches: {}", total_batches);
    println!("  Total rows: {}", total_rows);
    println!("  Time: {:.3}s", elapsed.as_secs_f64());
}
