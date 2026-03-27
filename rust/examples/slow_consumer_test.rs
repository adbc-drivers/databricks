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

//! Slow consumer test to verify memory backpressure in CloudFetch streaming.
//!
//! This test simulates a slow consumer to observe:
//! - Chunks building up to the memory limit
//! - Downloads pausing when limit is reached
//! - New downloads only starting after chunks are consumed

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Connection as ConnectionTrait;
use adbc_core::Database as DatabaseTrait;
use adbc_core::Driver as DriverTrait;
use adbc_core::Optionable;
use adbc_core::Statement as StatementTrait;
use arrow_array::{RecordBatch, RecordBatchReader};
use databricks_adbc::Driver;
use std::time::{Duration, Instant};

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

    println!("=== Slow Consumer CloudFetch Test ===\n");
    println!("This test uses a low memory limit (max_chunks_in_memory=2) and");
    println!("slow consumption to demonstrate backpressure behavior.\n");
    println!("Host: {}", &host);
    println!("HTTP Path: {}", &http_path);
    println!();

    // Create driver and database
    let mut driver = Driver::new();
    let mut db = driver.new_database().expect("Failed to create database");

    // Set connection options
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

    // Set a LOW memory limit to observe backpressure clearly
    // With max_chunks_in_memory=2, we should see:
    // - First 2 chunks download
    // - Memory limit hit, downloads pause
    // - After consuming chunk 0, chunk 2 starts downloading
    // - After consuming chunk 1, chunk 3 starts downloading
    // - etc.
    db.set_option(
        OptionDatabase::Other("databricks.cloudfetch.max_chunks_in_memory".into()),
        OptionValue::String("2".into()),
    )
    .expect("Failed to set max_chunks_in_memory");

    let mut conn = db.new_connection().expect("Failed to create connection");
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    // Generate a dataset with multiple chunks
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

    println!("Executing query (1M rows, expecting ~7 chunks)...\n");

    let start = Instant::now();
    stmt.set_sql_query(sql).expect("Failed to set query");

    let mut reader = stmt.execute().expect("Failed to execute query");

    println!("Query executed, starting SLOW consumption...\n");
    println!("Watch the logs for:");
    println!("  - 'Memory limit reached: 2/2 chunks in memory' - backpressure active");
    println!("  - 'Released chunk X' followed by 'Scheduling download for chunk Y'");
    println!();

    let schema = reader.schema();
    println!("Schema: {:?}\n", schema);

    let mut total_rows = 0u64;
    let mut total_batches = 0u64;
    let mut chunks_consumed = 0u64;
    let mut last_chunk_rows = 0u64;

    // Simulate slow consumption - sleep between chunks
    // Each chunk has ~160K rows across ~39 batches, so we count batches
    // and sleep after processing approximately one chunk worth
    const ROWS_PER_CHUNK: u64 = 160_000; // approximate
    const SLEEP_BETWEEN_CHUNKS: Duration = Duration::from_secs(3);

    for batch_result in &mut reader {
        let batch_result: Result<RecordBatch, _> = batch_result;
        match batch_result {
            Ok(batch) => {
                let rows = batch.num_rows();
                total_rows += rows as u64;
                total_batches += 1;
                last_chunk_rows += rows as u64;

                // Print every 10th batch
                if total_batches.is_multiple_of(10) {
                    println!(
                        "Batch {}: {} rows (cumulative: {} rows)",
                        total_batches, rows, total_rows
                    );
                }

                // After consuming approximately one chunk worth of rows, sleep
                if last_chunk_rows >= ROWS_PER_CHUNK {
                    chunks_consumed += 1;
                    println!(
                        "\n>>> SLOW CONSUMER: Finished chunk ~{}, sleeping {}s to simulate slow processing...\n",
                        chunks_consumed,
                        SLEEP_BETWEEN_CHUNKS.as_secs()
                    );
                    std::thread::sleep(SLEEP_BETWEEN_CHUNKS);
                    last_chunk_rows = 0;
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
        "Effective throughput: {:.2} rows/sec (includes sleep time)",
        total_rows as f64 / elapsed.as_secs_f64()
    );

    println!("\n=== Expected Behavior ===");
    println!("With max_chunks_in_memory=2:");
    println!("  1. Chunks 0 and 1 download immediately");
    println!("  2. Memory limit hit (2/2), chunk 2 waits");
    println!("  3. After consuming chunk 0, chunk 2 starts downloading");
    println!("  4. Pattern continues: release one, download one");
    println!("\nCheck the DEBUG logs above for 'Memory limit reached' and");
    println!("'Scheduling download' messages to verify backpressure worked.");

    println!("\n=== Test Complete ===");
}
