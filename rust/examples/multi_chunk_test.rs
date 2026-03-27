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

//! Multi-chunk CloudFetch test to verify streaming behavior.

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
    // Initialize tracing with simple format
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // Connection parameters from environment variables
    let host =
        std::env::var("DATABRICKS_HOST").expect("DATABRICKS_HOST environment variable required");
    let http_path = std::env::var("DATABRICKS_HTTP_PATH")
        .expect("DATABRICKS_HTTP_PATH environment variable required");
    let token =
        std::env::var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN environment variable required");

    println!("=== Multi-Chunk CloudFetch Test ===\n");
    println!("Host: {}", &host);
    println!("HTTP Path: {}", &http_path);
    println!();

    // Create driver and database
    let mut driver = Driver::new();
    let mut db = driver.new_database().expect("Failed to create database");

    // Set options using the correct option names
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
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    // Generate a large enough dataset that requires multiple CloudFetch chunks
    // Try a simple sequence with LIMIT but no RANGE limit
    let sql = r#"
        WITH base AS (
            SELECT explode(sequence(1, 10)) as x
        ),
        expanded AS (
            SELECT
                a.x as a, b.x as b, c.x as c, d.x as d, e.x as e, f.x as f
            FROM base a
            CROSS JOIN base b
            CROSS JOIN base c
            CROSS JOIN base d
            CROSS JOIN base e
            CROSS JOIN base f
        )
        SELECT
            row_number() OVER (ORDER BY a, b, c, d, e, f) as id,
            a * 100000 + b * 10000 + c * 1000 + d * 100 + e * 10 + f as computed_value,
            CONCAT('row_', CAST(a AS STRING), '_', CAST(b AS STRING), '_', CAST(c AS STRING)) as label,
            RAND() as random_value,
            REPEAT('x', 100) as padding
        FROM expanded
    "#;

    println!("Executing query (10^6 = 1M rows expected)...\n");

    let start = Instant::now();
    stmt.set_sql_query(sql).expect("Failed to set query");

    let mut reader = stmt.execute().expect("Failed to execute query");

    println!("Query executed, streaming results...\n");

    let schema = reader.schema();
    println!("Schema: {:?}\n", schema);

    let mut total_rows = 0u64;
    let mut total_batches = 0u64;
    let mut batch_sizes: Vec<usize> = Vec::new();

    // Stream through all batches
    for batch_result in &mut reader {
        let batch_result: Result<RecordBatch, _> = batch_result;
        match batch_result {
            Ok(batch) => {
                let rows = batch.num_rows();
                total_rows += rows as u64;
                total_batches += 1;
                batch_sizes.push(rows);

                if total_batches <= 10 || total_batches.is_multiple_of(100) {
                    println!(
                        "Batch {}: {} rows (cumulative: {} rows)",
                        total_batches, rows, total_rows
                    );
                }
            }
            Err(e) => {
                eprintln!("Error reading batch: {:?}", e);
                break;
            }
        }
    }

    let elapsed = start.elapsed();

    println!("\n=== Results ===");
    println!("Total batches: {}", total_batches);
    println!("Total rows: {}", total_rows);
    println!("Total time: {:.2}s", elapsed.as_secs_f64());
    println!(
        "Throughput: {:.2} rows/sec",
        total_rows as f64 / elapsed.as_secs_f64()
    );

    if !batch_sizes.is_empty() {
        let min_batch = batch_sizes.iter().min().unwrap();
        let max_batch = batch_sizes.iter().max().unwrap();
        let avg_batch = total_rows as f64 / total_batches as f64;
        println!("\nBatch size stats:");
        println!("  Min: {} rows", min_batch);
        println!("  Max: {} rows", max_batch);
        println!("  Avg: {:.1} rows", avg_batch);
    }

    println!("\n=== Test Complete ===");
}
