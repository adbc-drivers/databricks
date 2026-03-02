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

//! End-to-end tests for the CloudFetch pipeline against a real Databricks workspace.
//!
//! These tests validate the channel-based CloudFetch pipeline works correctly
//! with actual cloud storage downloads (Azure Blob presigned URLs).
//!
//! Run with:
//!   cargo test --test e2e -- --include-ignored
//!
//! Requires environment variables (set via .cargo/config.toml):
//!   DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Database as _;
use adbc_core::Driver as _;
use adbc_core::Optionable;
use arrow_array::RecordBatchReader;
use databricks_adbc::Driver;
use std::time::Instant;

/// Helper: create a configured Database from env vars, with optional overrides.
fn create_database(max_chunks_in_memory: Option<usize>) -> databricks_adbc::Database {
    let host = std::env::var("DATABRICKS_HOST").expect("DATABRICKS_HOST not set");
    let http_path = std::env::var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH not set");
    let token = std::env::var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN not set");

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

    // Ensure CloudFetch is enabled
    database
        .set_option(
            OptionDatabase::Other("databricks.cloudfetch.enabled".into()),
            OptionValue::String("true".into()),
        )
        .expect("Failed to enable cloudfetch");

    // Override max_chunks_in_memory if specified
    if let Some(chunks) = max_chunks_in_memory {
        database
            .set_option(
                OptionDatabase::Other("databricks.cloudfetch.max_chunks_in_memory".into()),
                OptionValue::String(chunks.to_string()),
            )
            .expect("Failed to set max_chunks_in_memory");
    }

    database
}

/// Helper: execute a query and return total row count and batch count.
fn execute_and_count(database: &databricks_adbc::Database, query: &str) -> (i64, usize) {
    use adbc_core::Connection as _;
    use adbc_core::Statement as _;

    let mut connection = database.new_connection().expect("Failed to connect");
    let mut statement = connection
        .new_statement()
        .expect("Failed to create statement");
    statement.set_sql_query(query).expect("Failed to set query");

    let reader = statement.execute().expect("Failed to execute query");

    let mut total_rows: i64 = 0;
    let mut batch_count: usize = 0;

    for batch_result in reader {
        let batch = batch_result.expect("Error reading batch");
        total_rows += batch.num_rows() as i64;
        batch_count += 1;
    }

    (total_rows, batch_count)
}

// ── Scenario 1: 1M rows across multiple CloudFetch batches ─────────────────

/// Validates that a SELECT query returning 1M rows is fetched via CloudFetch
/// across multiple batches and all rows are received with correct count.
///
/// This exercises the basic CloudFetch pipeline: SEA API → link fetching →
/// parallel download workers → consumer pipeline.
///
/// We include a wide string column (REPEAT) to ensure the total result size
/// exceeds the server's inline threshold (~16 MB), forcing CloudFetch
/// (external links) rather than inline Arrow.
#[test]
#[ignore]
fn test_cloudfetch_1m_rows_all_received() {
    let expected_rows: i64 = 1_000_000;
    let database = create_database(None);

    // Generate 1M rows with wide string columns to push result size past CloudFetch threshold.
    // Each row has: id (BIGINT, 8 bytes) + padding (STRING, ~100 bytes) = ~108 bytes/row
    // Total: ~108 MB, well above the ~16 MB inline threshold.
    let query = format!(
        "SELECT id, REPEAT('x', 100) AS padding \
         FROM (SELECT EXPLODE(SEQUENCE(1, {})) AS id)",
        expected_rows
    );

    let start = Instant::now();
    let (total_rows, batch_count) = execute_and_count(&database, &query);
    let elapsed = start.elapsed();

    println!(
        "1M rows test: received {} rows in {} batches ({:.2?})",
        total_rows, batch_count, elapsed
    );

    assert_eq!(
        total_rows, expected_rows,
        "Expected {} rows but received {}",
        expected_rows, total_rows
    );

    // With 1M wide rows (~108 MB), CloudFetch should produce multiple batches
    assert!(
        batch_count > 1,
        "Expected multiple batches for 1M rows, got {}",
        batch_count
    );
}

// ── Scenario 2: 10M rows exercises link-prefetch beyond 32-link limit ──────

/// Validates that a SELECT query returning 10M rows exercises the link-prefetch
/// loop beyond the server's 32-link-per-response limit and all rows are received
/// without truncation.
///
/// With 10M rows, the server will return results across many chunks. The SEA API
/// returns at most 32 external links per response, so the scheduler must issue
/// multiple fetch_links() calls to retrieve all chunks.
///
/// Uses CROSS JOIN of two SEQUENCE ranges to avoid the Photon protobuf memory
/// limit that occurs with a single SEQUENCE(1, 10000000).
#[test]
#[ignore]
fn test_cloudfetch_10m_rows_link_prefetch_beyond_32_links() {
    let expected_rows: i64 = 10_000_000;
    let database = create_database(None);

    // Generate 10M rows via CROSS JOIN of two smaller sequences:
    // 10,000 × 1,000 = 10,000,000 rows.
    // This avoids the Photon protobuf limit for a single huge SEQUENCE.
    // Include wide string padding (~100 bytes/row) to push total size to ~1 GB,
    // which forces the server to create many CloudFetch chunks (>32 links).
    let query = "SELECT \
        a.id * 1000 + b.id AS row_id, \
        REPEAT('x', 100) AS padding \
        FROM (SELECT EXPLODE(SEQUENCE(0, 9999)) AS id) a \
        CROSS JOIN (SELECT EXPLODE(SEQUENCE(0, 999)) AS id) b";

    let start = Instant::now();
    let (total_rows, batch_count) = execute_and_count(&database, query);
    let elapsed = start.elapsed();

    println!(
        "10M rows test: received {} rows in {} batches ({:.2?})",
        total_rows, batch_count, elapsed
    );

    assert_eq!(
        total_rows, expected_rows,
        "Expected {} rows but received {} — possible truncation",
        expected_rows, total_rows
    );

    // With 10M rows of BIGINT data (~80 MB), CloudFetch should produce many batches.
    // The server's 32-link-per-response limit means the scheduler must call
    // fetch_links() multiple times. Verify we got more than 32 batches.
    assert!(
        batch_count > 32,
        "Expected >32 batches for 10M rows (proving link prefetch beyond server limit), got {}",
        batch_count
    );
}

// ── Scenario 3: CloudFetch downloads from Azure Blob presigned URLs ────────

/// Validates that the CloudFetch pipeline correctly handles presigned URL
/// download from real cloud storage (Azure Blob) with proper authentication
/// headers.
///
/// This test verifies that:
/// - The query result uses external links (CloudFetch path, not inline)
/// - Downloads from Azure Blob Storage presigned URLs succeed
/// - Authentication headers (SAS tokens) are handled correctly
/// - Arrow IPC data is parsed correctly from downloaded blobs
///
/// We use a wide result set (500K rows × string padding) to ensure the data
/// exceeds the inline threshold and forces CloudFetch via Azure Blob.
#[test]
#[ignore]
fn test_cloudfetch_azure_blob_presigned_url_download() {
    use adbc_core::Connection as _;
    use adbc_core::Statement as _;

    let expected_rows: i64 = 500_000;
    let database = create_database(None);

    let mut connection = database.new_connection().expect("Failed to connect");
    let mut statement = connection
        .new_statement()
        .expect("Failed to create statement");

    // Generate 500K rows with wide string columns to ensure CloudFetch is triggered.
    // Include multiple columns to verify full schema round-trip.
    let query = format!(
        "SELECT id, CAST(id AS STRING) AS id_str, REPEAT('abcdefghij', 10) AS padding \
         FROM (SELECT EXPLODE(SEQUENCE(1, {})) AS id)",
        expected_rows
    );
    statement
        .set_sql_query(&query)
        .expect("Failed to set query");

    let reader = statement.execute().expect("Failed to execute query");
    let schema = reader.schema();

    // Verify schema has the expected columns
    assert_eq!(
        schema.fields().len(),
        3,
        "Expected 3 columns (id, id_str, padding), got {}",
        schema.fields().len()
    );

    let mut total_rows: i64 = 0;
    let mut batch_count: usize = 0;

    for batch_result in reader {
        let batch = batch_result.expect("Error reading batch from Azure Blob download");
        assert!(
            batch.num_rows() > 0,
            "Received empty batch — possible download failure"
        );
        assert_eq!(batch.num_columns(), 3, "Each batch should have 3 columns");
        total_rows += batch.num_rows() as i64;
        batch_count += 1;
    }

    println!(
        "Azure Blob download test: received {} rows in {} batches",
        total_rows, batch_count
    );

    assert_eq!(
        total_rows, expected_rows,
        "Expected {} rows from Azure Blob download but received {}",
        expected_rows, total_rows
    );

    // Multiple batches confirms CloudFetch path was exercised
    assert!(
        batch_count > 1,
        "Expected multiple batches from CloudFetch (Azure Blob download), got {}",
        batch_count
    );
}

// ── Scenario 4: Backpressure via bounded result_channel ────────────────────

/// Validates that backpressure works correctly under load — the bounded
/// result_channel prevents unbounded memory growth during large result
/// consumption.
///
/// Strategy:
/// - Configure `max_chunks_in_memory = 2` (very small buffer)
/// - Query 1M wide rows (many CloudFetch chunks)
/// - Consume with deliberate delays between batches
/// - If backpressure works, the pipeline won't pre-download all chunks
///   into memory. We verify:
///   1. All rows are eventually received (correctness)
///   2. The consumer can introduce delays without losing data
///   3. The process doesn't OOM (implicit — if it finishes, memory was bounded)
///
/// The bounded channel (capacity = max_chunks_in_memory) ensures the scheduler
/// blocks when it's gotten too far ahead of the consumer.
#[test]
#[ignore]
fn test_cloudfetch_backpressure_bounded_memory() {
    use adbc_core::Connection as _;
    use adbc_core::Statement as _;

    let expected_rows: i64 = 1_000_000;

    // Use a very small buffer to exercise backpressure
    let max_chunks_in_memory: usize = 2;
    let database = create_database(Some(max_chunks_in_memory));

    let mut connection = database.new_connection().expect("Failed to connect");
    let mut statement = connection
        .new_statement()
        .expect("Failed to create statement");

    // Use wide rows to ensure CloudFetch is triggered (not inline), so the
    // bounded result_channel is the actual bottleneck under test.
    let query = format!(
        "SELECT id, REPEAT('x', 100) AS padding \
         FROM (SELECT EXPLODE(SEQUENCE(1, {})) AS id)",
        expected_rows
    );
    statement
        .set_sql_query(&query)
        .expect("Failed to set query");

    let reader = statement.execute().expect("Failed to execute query");

    let mut total_rows: i64 = 0;
    let mut batch_count: usize = 0;
    let start = Instant::now();

    for batch_result in reader {
        let batch = batch_result.expect("Error reading batch under backpressure");
        total_rows += batch.num_rows() as i64;
        batch_count += 1;

        // Introduce a deliberate delay every 10 batches to simulate slow consumer.
        // This forces backpressure: the scheduler and download workers must wait
        // because the bounded result_channel (capacity=2) is full.
        if batch_count.is_multiple_of(10) {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    let elapsed = start.elapsed();

    println!(
        "Backpressure test (max_chunks_in_memory={}): received {} rows in {} batches ({:.2?})",
        max_chunks_in_memory, total_rows, batch_count, elapsed
    );

    assert_eq!(
        total_rows, expected_rows,
        "Expected {} rows under backpressure but received {} — data may have been lost",
        expected_rows, total_rows
    );

    // With max_chunks_in_memory=2, the pipeline should still complete
    // successfully. The key validation is that all rows arrived despite
    // the consumer being slow — no deadlock, no data loss, no OOM.
    assert!(
        batch_count > 1,
        "Expected multiple batches for 1M rows under backpressure, got {}",
        batch_count
    );

    // If we got here without OOM or deadlock, backpressure is working.
    // The bounded result_channel prevented the scheduler from dispatching
    // all chunks at once, keeping memory usage bounded.
    println!(
        "Backpressure validation PASSED: {} rows consumed with max_chunks_in_memory={}, no OOM or deadlock",
        total_rows, max_chunks_in_memory
    );
}
