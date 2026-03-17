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

//! End-to-end CloudFetch tests against a real Databricks workspace.
//!
//! These tests validate the channel-based pipeline redesign works correctly:
//! 1. Multi-chunk downloads return all rows in correct sequential order
//! 2. Presigned URL expiry triggers automatic refresh via refetch_link
//! 3. Cancellation during active downloads terminates cleanly
//!
//! Run with: cargo test --test e2e_cloudfetch -- --ignored

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Connection as ConnectionTrait;
use adbc_core::Database as DatabaseTrait;
use adbc_core::Driver as DriverTrait;
use adbc_core::Optionable;
use adbc_core::Statement as StatementTrait;
use arrow_array::{Int32Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use databricks_adbc::client::{ChunkLinkFetchResult, DatabricksHttpClient, HttpClientConfig};
use databricks_adbc::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use databricks_adbc::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use databricks_adbc::reader::cloudfetch::streaming_provider::StreamingCloudFetchProvider;
use databricks_adbc::types::cloudfetch::{CloudFetchConfig, CloudFetchLink};
use databricks_adbc::types::sea::CompressionCodec;
use databricks_adbc::Driver;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helper: create a Databricks connection from environment variables
// ---------------------------------------------------------------------------

fn create_databricks_connection() -> (databricks_adbc::Database, databricks_adbc::Connection) {
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

    let connection = database
        .new_connection()
        .expect("Failed to create connection");

    (database, connection)
}

// ---------------------------------------------------------------------------
// Helper: Arrow IPC creation for mock tests
// ---------------------------------------------------------------------------

fn create_arrow_ipc(batches: &[RecordBatch]) -> Vec<u8> {
    let schema = batches[0].schema();
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
    }
    buffer
}

fn create_test_batch(chunk_index: i64, num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("chunk_id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let ids: Vec<i32> = (0..num_rows as i32)
        .map(|i| chunk_index as i32 * 1000 + i)
        .collect();
    let values: Vec<String> = (0..num_rows)
        .map(|i| format!("chunk{}_row{}", chunk_index, i))
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// Mock link fetcher for URL-expiry test (returns 403 on first attempt)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Expiry403LinkFetcher {
    base_url: String,
    total_chunks: i64,
    refetch_count: AtomicU32,
}

impl Expiry403LinkFetcher {
    fn new(base_url: &str, total_chunks: i64) -> Self {
        Self {
            base_url: base_url.to_string(),
            total_chunks,
            refetch_count: AtomicU32::new(0),
        }
    }

    fn make_link(&self, chunk_index: i64, path_suffix: &str) -> CloudFetchLink {
        CloudFetchLink {
            url: format!("{}/chunk/{}{}", self.base_url, chunk_index, path_suffix),
            chunk_index,
            row_offset: chunk_index * 100,
            row_count: 100,
            byte_count: 5000,
            expiration: chrono::Utc::now() + chrono::Duration::hours(1),
            http_headers: HashMap::new(),
            next_chunk_index: if chunk_index + 1 < self.total_chunks {
                Some(chunk_index + 1)
            } else {
                None
            },
        }
    }

    fn get_refetch_count(&self) -> u32 {
        self.refetch_count.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ChunkLinkFetcher for Expiry403LinkFetcher {
    async fn fetch_links(
        &self,
        start_chunk_index: i64,
        _start_row_offset: i64,
    ) -> databricks_adbc::error::Result<ChunkLinkFetchResult> {
        if start_chunk_index >= self.total_chunks {
            return Ok(ChunkLinkFetchResult::end_of_stream());
        }

        let end = (start_chunk_index + 3).min(self.total_chunks);
        let links: Vec<CloudFetchLink> = (start_chunk_index..end)
            .map(|i| {
                // Chunks 1 and 3: initially point to /expired path → 403
                if i == 1 || i == 3 {
                    self.make_link(i, "/expired")
                } else {
                    self.make_link(i, "")
                }
            })
            .collect();

        let has_more = end < self.total_chunks;
        Ok(ChunkLinkFetchResult {
            links,
            has_more,
            next_chunk_index: if has_more { Some(end) } else { None },
            next_row_offset: None,
        })
    }

    async fn refetch_link(
        &self,
        chunk_index: i64,
        _row_offset: i64,
    ) -> databricks_adbc::error::Result<CloudFetchLink> {
        self.refetch_count.fetch_add(1, Ordering::SeqCst);
        // After refetch, return the valid (non-expired) link
        Ok(self.make_link(chunk_index, ""))
    }
}

// ===========================================================================
// INTEGRATION TEST 1: End-to-end sequential consumption (wiremock)
//
// Test that all chunks are downloaded and read in order through the full
// pipeline with realistic HTTP mocking.
// ===========================================================================

#[tokio::test]
async fn test_end_to_end_sequential_consumption() {
    // This test verifies the full pipeline with multiple chunks served via wiremock
    let total_chunks = 10;
    let rows_per_chunk = 50;
    let server = MockServer::start().await;

    // Set up mock responses for all chunks
    for i in 0..total_chunks {
        let batch = create_test_batch(i, rows_per_chunk);
        let ipc_data = create_arrow_ipc(&[batch]);

        Mock::given(method("GET"))
            .and(path(format!("/chunk/{}", i)))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ipc_data))
            .mount(&server)
            .await;
    }

    // Create mock link fetcher
    #[derive(Debug)]
    struct SimpleChunkLinkFetcher {
        base_url: String,
        total_chunks: i64,
        rows_per_chunk: usize,
    }

    impl SimpleChunkLinkFetcher {
        fn new(base_url: &str, total_chunks: i64, rows_per_chunk: usize) -> Self {
            Self {
                base_url: base_url.to_string(),
                total_chunks,
                rows_per_chunk,
            }
        }
    }

    #[async_trait::async_trait]
    impl ChunkLinkFetcher for SimpleChunkLinkFetcher {
        async fn fetch_links(
            &self,
            start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> databricks_adbc::error::Result<ChunkLinkFetchResult> {
            if start_chunk_index >= self.total_chunks {
                return Ok(ChunkLinkFetchResult::end_of_stream());
            }

            // Return batch of 3 links at a time
            let end = (start_chunk_index + 3).min(self.total_chunks);
            let links: Vec<CloudFetchLink> = (start_chunk_index..end)
                .map(|i| CloudFetchLink {
                    url: format!("{}/chunk/{}", self.base_url, i),
                    chunk_index: i,
                    row_offset: i * self.rows_per_chunk as i64,
                    row_count: self.rows_per_chunk as i64,
                    byte_count: 5000,
                    expiration: chrono::Utc::now() + chrono::Duration::hours(1),
                    http_headers: HashMap::new(),
                    next_chunk_index: if i + 1 < self.total_chunks {
                        Some(i + 1)
                    } else {
                        None
                    },
                })
                .collect();

            let has_more = end < self.total_chunks;
            Ok(ChunkLinkFetchResult {
                links,
                has_more,
                next_chunk_index: if has_more { Some(end) } else { None },
                next_row_offset: None,
            })
        }

        async fn refetch_link(
            &self,
            chunk_index: i64,
            _row_offset: i64,
        ) -> databricks_adbc::error::Result<CloudFetchLink> {
            Ok(CloudFetchLink {
                url: format!("{}/chunk/{}", self.base_url, chunk_index),
                chunk_index,
                row_offset: chunk_index * self.rows_per_chunk as i64,
                row_count: self.rows_per_chunk as i64,
                byte_count: 5000,
                expiration: chrono::Utc::now() + chrono::Duration::hours(1),
                http_headers: HashMap::new(),
                next_chunk_index: if chunk_index + 1 < self.total_chunks {
                    Some(chunk_index + 1)
                } else {
                    None
                },
            })
        }
    }

    let fetcher: Arc<dyn ChunkLinkFetcher> =
        Arc::new(SimpleChunkLinkFetcher::new(&server.uri(), total_chunks, rows_per_chunk));
    let http_client = Arc::new(DatabricksHttpClient::new(HttpClientConfig::default()).unwrap());
    let downloader = Arc::new(ChunkDownloader::new(
        http_client,
        CompressionCodec::None,
        0.1,
    ));

    let config = CloudFetchConfig {
        max_chunks_in_memory: 4,
        num_download_workers: 3,
        max_retries: 3,
        max_refresh_retries: 3,
        retry_delay: Duration::from_millis(10),
        ..CloudFetchConfig::default()
    };

    let provider = StreamingCloudFetchProvider::new(config, fetcher, downloader);

    // Consume all batches and verify ordering
    let mut chunk_ids_seen = Vec::new();
    let mut total_rows: usize = 0;

    while let Some(batch) = provider.next_batch().await.expect("Should not error") {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("First column should be Int32");
        let first_id = id_col.value(0);
        let chunk_index = first_id / 1000;
        chunk_ids_seen.push(chunk_index);
        total_rows += batch.num_rows();

        assert_eq!(
            batch.num_rows(),
            rows_per_chunk,
            "Each chunk should have {} rows",
            rows_per_chunk
        );
    }

    // Assertions
    assert_eq!(
        chunk_ids_seen.len(),
        total_chunks as usize,
        "Should receive all {} chunks",
        total_chunks
    );

    // Verify sequential order
    for i in 0..total_chunks {
        assert_eq!(
            chunk_ids_seen[i as usize], i as i32,
            "Chunk {} should be at position {}",
            i, i
        );
    }

    assert_eq!(
        total_rows,
        (total_chunks as usize) * rows_per_chunk,
        "Total rows should be {} × {} = {}",
        total_chunks,
        rows_per_chunk,
        (total_chunks as usize) * rows_per_chunk
    );

    println!(
        "✓ end_to_end_sequential_consumption: {} chunks, {} rows, all in order",
        total_chunks, total_rows
    );
}

// ===========================================================================
// REAL DATABRICKS TEST: CloudFetch multi-chunk sequential order and completeness
//
// A CloudFetch query returning multiple chunks downloads all chunks in correct
// sequential order and all rows are returned without data loss or reordering.
// ===========================================================================

#[test]
#[ignore]
fn test_cloudfetch_multichunk_sequential_order_and_completeness_real() {
    // Initialize tracing for debug visibility
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    let (_db, mut conn) = create_databricks_connection();
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    // Generate 1M rows with padding — large enough to produce multiple CloudFetch chunks.
    // Each row has a deterministic `id` (1..1_000_000) so we can verify
    // completeness and ordering after streaming.
    // The REPEAT('x', 200) padding ensures rows are wide enough to trigger
    // multiple chunks (each chunk ~160K rows with this width).
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
            CONCAT('row_', CAST(a AS STRING), '_', CAST(b AS STRING)) as label,
            RAND() as random_value,
            REPEAT('x', 200) as padding
        FROM expanded
    "#;

    println!("Executing 1M-row CloudFetch query...");
    let start = Instant::now();

    stmt.set_sql_query(sql).expect("Failed to set query");
    let mut reader = stmt.execute().expect("Failed to execute query");

    let schema = reader.schema();
    println!("Schema: {:?}", schema);
    assert!(
        schema.fields().len() >= 3,
        "Expected at least 3 columns, got {}",
        schema.fields().len()
    );

    // Detect the Arrow type for the id column — Databricks may return Int32 or Int64
    let id_field = schema.field(0);
    println!("ID column type: {:?}", id_field.data_type());

    let mut total_rows: u64 = 0;
    let mut total_batches: u64 = 0;
    let mut prev_max_id: i64 = 0;
    let mut all_ids_monotonic = true;

    // Helper to extract id value from a column regardless of Int32/Int64 type
    fn get_id_value(batch: &RecordBatch, col: usize, row: usize) -> i64 {
        let col_ref = batch.column(col);
        if let Some(arr) = col_ref.as_any().downcast_ref::<arrow_array::Int64Array>() {
            arr.value(row)
        } else if let Some(arr) = col_ref.as_any().downcast_ref::<arrow_array::Int32Array>() {
            arr.value(row) as i64
        } else {
            panic!(
                "Expected Int32 or Int64 for id column, got {:?}",
                col_ref.data_type()
            );
        }
    }

    // Stream all batches — verify monotonic ordering and count rows
    for batch_result in &mut reader {
        let batch = batch_result.expect("Error reading batch");
        let rows = batch.num_rows();
        total_rows += rows as u64;
        total_batches += 1;

        // Verify that IDs within this batch are monotonically increasing
        // and that the first ID in this batch > the last ID of the previous batch
        let first_id = get_id_value(&batch, 0, 0);
        let last_id = get_id_value(&batch, 0, rows - 1);

        if first_id <= prev_max_id {
            all_ids_monotonic = false;
            eprintln!(
                "Ordering violation: batch {} first_id={} <= prev_max_id={}",
                total_batches, first_id, prev_max_id
            );
        }

        // Check intra-batch monotonicity
        for i in 1..rows {
            let curr = get_id_value(&batch, 0, i);
            let prev = get_id_value(&batch, 0, i - 1);
            if curr <= prev {
                all_ids_monotonic = false;
                eprintln!(
                    "Intra-batch ordering violation at batch {}, row {}: {} <= {}",
                    total_batches, i, curr, prev
                );
                break;
            }
        }

        prev_max_id = last_id;

        if total_batches.is_multiple_of(50) {
            println!(
                "Batch {}: {} rows (cumulative: {})",
                total_batches, rows, total_rows
            );
        }
    }

    let elapsed = start.elapsed();

    println!("\n=== Results ===");
    println!("Total batches: {}", total_batches);
    println!("Total rows: {}", total_rows);
    println!("Time: {:.2}s", elapsed.as_secs_f64());
    println!(
        "Throughput: {:.0} rows/sec",
        total_rows as f64 / elapsed.as_secs_f64()
    );

    // --- Assertions ---

    // 1. Exactly 1,000,000 rows (10^6 from CROSS JOIN of 6 base-10 sequences)
    assert_eq!(
        total_rows, 1_000_000,
        "Expected 1,000,000 rows but got {}. Data loss detected!",
        total_rows
    );

    // 2. Multiple batches (confirms CloudFetch multi-chunk was used)
    assert!(
        total_batches > 1,
        "Expected multiple batches from CloudFetch, got {}",
        total_batches
    );

    // 3. IDs are monotonically increasing (correct sequential order)
    assert!(
        all_ids_monotonic,
        "Row IDs were not monotonically increasing — chunks may be reordered"
    );

    // 4. Last ID should be 1,000,000
    assert_eq!(
        prev_max_id, 1_000_000,
        "Last row ID should be 1,000,000 but got {}",
        prev_max_id
    );

    println!("\n✓ All 1,000,000 rows received in correct sequential order");
}

// ===========================================================================
// INTEGRATION TEST 2: End-to-end 401 recovery (wiremock)
//
// Test that presigned URL expiry (401/403) mid-stream triggers refetch_link
// and the pipeline continues successfully.
// ===========================================================================

#[tokio::test]
async fn test_end_to_end_401_recovery() {
    // This test uses wiremock to simulate a real CloudFetch pipeline where
    // some chunk URLs return 403 (Forbidden), triggering the driver's
    // refetch_link mechanism to get fresh URLs and retry.

    let total_chunks = 5;
    let server = MockServer::start().await;

    // --- Set up mock responses ---

    // Chunks 0, 2, 4: always return data on the normal path
    for i in [0_i64, 2, 4] {
        let batch = create_test_batch(i, 100);
        let ipc_data = create_arrow_ipc(&[batch]);

        Mock::given(method("GET"))
            .and(path(format!("/chunk/{}", i)))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ipc_data))
            .mount(&server)
            .await;
    }

    // Chunks 1 and 3: /expired path returns 403 (simulating expired presigned URL)
    for i in [1_i64, 3] {
        Mock::given(method("GET"))
            .and(path(format!("/chunk/{}/expired", i)))
            .respond_with(ResponseTemplate::new(403).set_body_string("Forbidden - URL expired"))
            .mount(&server)
            .await;

        // After refetch_link is called, the normal path returns data
        let batch = create_test_batch(i, 100);
        let ipc_data = create_arrow_ipc(&[batch]);
        Mock::given(method("GET"))
            .and(path(format!("/chunk/{}", i)))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ipc_data))
            .mount(&server)
            .await;
    }

    // --- Create the pipeline ---

    let fetcher = Arc::new(Expiry403LinkFetcher::new(&server.uri(), total_chunks));
    let fetcher_ref = Arc::clone(&fetcher);
    let link_fetcher: Arc<dyn ChunkLinkFetcher> = fetcher;

    let http_client = Arc::new(DatabricksHttpClient::new(HttpClientConfig::default()).unwrap());
    let downloader = Arc::new(ChunkDownloader::new(
        http_client,
        CompressionCodec::None,
        0.1,
    ));

    let config = CloudFetchConfig {
        max_chunks_in_memory: 4,
        num_download_workers: 2,
        max_retries: 5,
        max_refresh_retries: 5,
        retry_delay: Duration::from_millis(10),
        ..CloudFetchConfig::default()
    };

    let provider = StreamingCloudFetchProvider::new(
        config,
        link_fetcher,
        downloader,
    );

    // --- Consume all batches ---

    let mut chunk_ids_seen = Vec::new();
    let mut total_rows: usize = 0;

    while let Some(batch) = provider
        .next_batch()
        .await
        .expect("Consumer should not see errors")
    {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("First column should be Int32");
        let first_id = id_col.value(0);
        let chunk_index = first_id / 1000;
        chunk_ids_seen.push(chunk_index);
        total_rows += batch.num_rows();

        // Verify each batch has the expected number of rows
        assert_eq!(batch.num_rows(), 100, "Each chunk should have 100 rows");
    }

    // --- Assertions ---

    // 1. All 5 chunks consumed in sequential order
    assert_eq!(
        chunk_ids_seen,
        vec![0, 1, 2, 3, 4],
        "All chunks should be consumed in order. Got: {:?}",
        chunk_ids_seen
    );

    // 2. Total rows: 5 chunks × 100 rows = 500
    assert_eq!(
        total_rows, 500,
        "Expected 500 rows, got {}. Data was lost during URL refresh.",
        total_rows
    );

    // 3. refetch_link was called at least twice (once for chunk 1, once for chunk 3)
    let refetch_count = fetcher_ref.get_refetch_count();
    assert!(
        refetch_count >= 2,
        "Expected at least 2 refetch_link calls (for expired chunks 1 and 3), got {}",
        refetch_count
    );

    println!(
        "✓ end_to_end_401_recovery: {} chunks, refetch_link called {} times",
        total_chunks, refetch_count
    );
}

// ===========================================================================
// INTEGRATION TEST 3: End-to-end cancellation mid-stream (wiremock)
//
// Test that cancellation during active downloads terminates cleanly without
// deadlock, panic, or resource leak.
// ===========================================================================

#[tokio::test]
async fn test_end_to_end_cancellation_mid_stream() {
    // This test verifies that cancellation works correctly by:
    // 1. Starting downloads with slow mock responses
    // 2. Canceling mid-stream
    // 3. Verifying clean shutdown without deadlock

    let total_chunks = 20;
    let rows_per_chunk = 50;
    let server = MockServer::start().await;

    // Set up mock responses with delays to simulate slow downloads
    for i in 0..total_chunks {
        let batch = create_test_batch(i, rows_per_chunk);
        let ipc_data = create_arrow_ipc(&[batch]);

        Mock::given(method("GET"))
            .and(path(format!("/chunk/{}", i)))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(ipc_data)
                    .set_delay(Duration::from_millis(100)), // Slow response to allow cancellation
            )
            .mount(&server)
            .await;
    }

    // Create mock link fetcher
    #[derive(Debug)]
    struct SimpleCancelLinkFetcher {
        base_url: String,
        total_chunks: i64,
        rows_per_chunk: usize,
    }

    impl SimpleCancelLinkFetcher {
        fn new(base_url: &str, total_chunks: i64, rows_per_chunk: usize) -> Self {
            Self {
                base_url: base_url.to_string(),
                total_chunks,
                rows_per_chunk,
            }
        }
    }

    #[async_trait::async_trait]
    impl ChunkLinkFetcher for SimpleCancelLinkFetcher {
        async fn fetch_links(
            &self,
            start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> databricks_adbc::error::Result<ChunkLinkFetchResult> {
            if start_chunk_index >= self.total_chunks {
                return Ok(ChunkLinkFetchResult::end_of_stream());
            }

            // Return batch of 5 links at a time
            let end = (start_chunk_index + 5).min(self.total_chunks);
            let links: Vec<CloudFetchLink> = (start_chunk_index..end)
                .map(|i| CloudFetchLink {
                    url: format!("{}/chunk/{}", self.base_url, i),
                    chunk_index: i,
                    row_offset: i * self.rows_per_chunk as i64,
                    row_count: self.rows_per_chunk as i64,
                    byte_count: 5000,
                    expiration: chrono::Utc::now() + chrono::Duration::hours(1),
                    http_headers: HashMap::new(),
                    next_chunk_index: if i + 1 < self.total_chunks {
                        Some(i + 1)
                    } else {
                        None
                    },
                })
                .collect();

            let has_more = end < self.total_chunks;
            Ok(ChunkLinkFetchResult {
                links,
                has_more,
                next_chunk_index: if has_more { Some(end) } else { None },
                next_row_offset: None,
            })
        }

        async fn refetch_link(
            &self,
            chunk_index: i64,
            _row_offset: i64,
        ) -> databricks_adbc::error::Result<CloudFetchLink> {
            Ok(CloudFetchLink {
                url: format!("{}/chunk/{}", self.base_url, chunk_index),
                chunk_index,
                row_offset: chunk_index * self.rows_per_chunk as i64,
                row_count: self.rows_per_chunk as i64,
                byte_count: 5000,
                expiration: chrono::Utc::now() + chrono::Duration::hours(1),
                http_headers: HashMap::new(),
                next_chunk_index: if chunk_index + 1 < self.total_chunks {
                    Some(chunk_index + 1)
                } else {
                    None
                },
            })
        }
    }

    let fetcher: Arc<dyn ChunkLinkFetcher> =
        Arc::new(SimpleCancelLinkFetcher::new(&server.uri(), total_chunks, rows_per_chunk));
    let http_client = Arc::new(DatabricksHttpClient::new(HttpClientConfig::default()).unwrap());
    let downloader = Arc::new(ChunkDownloader::new(
        http_client,
        CompressionCodec::None,
        0.1,
    ));

    let config = CloudFetchConfig {
        max_chunks_in_memory: 4,
        num_download_workers: 3,
        max_retries: 3,
        max_refresh_retries: 3,
        retry_delay: Duration::from_millis(10),
        ..CloudFetchConfig::default()
    };

    let provider = StreamingCloudFetchProvider::new(config, fetcher, downloader);

    // Read a few batches then cancel
    let provider_ref = Arc::clone(&provider);

    let consume_handle = tokio::spawn(async move {
        let mut count = 0;
        while let Some(_batch) = provider_ref.next_batch().await.ok().flatten() {
            count += 1;
            if count >= 3 {
                break;
            }
        }
        count
    });

    // Read a few batches
    let batches_read = consume_handle.await.expect("Consume task should not panic");
    assert!(
        batches_read >= 3,
        "Should have read at least 3 batches before cancel"
    );

    // Cancel the provider
    let cancel_start = Instant::now();
    provider.cancel();

    // Try to read another batch - should fail or return None quickly
    let _result = tokio::time::timeout(
        Duration::from_secs(2),
        provider.next_batch()
    ).await;

    let cancel_duration = cancel_start.elapsed();

    // Verify cancellation was fast
    assert!(
        cancel_duration < Duration::from_secs(2),
        "Cancellation took {:.2}s, expected < 2s",
        cancel_duration.as_secs_f64()
    );

    // Drop the provider - should complete quickly
    let drop_start = Instant::now();
    drop(provider);
    let drop_duration = drop_start.elapsed();

    assert!(
        drop_duration < Duration::from_secs(1),
        "Drop took {:.2}s, expected < 1s. Possible deadlock.",
        drop_duration.as_secs_f64()
    );

    println!(
        "✓ end_to_end_cancellation_mid_stream: read {} batches, cancel in {:.2}s, drop in {:.2}s",
        batches_read,
        cancel_duration.as_secs_f64(),
        drop_duration.as_secs_f64()
    );
}

// ===========================================================================
// REAL DATABRICKS TEST: Cancellation during active multi-chunk CloudFetch download
//             terminates cleanly within 5 seconds with no deadlock, panic,
//             or resource leak.
// ===========================================================================

#[test]
#[ignore]
fn test_cloudfetch_cancellation_terminates_cleanly_real() {
    // Initialize tracing for debug visibility
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    let (_db, mut conn) = create_databricks_connection();

    // --- Phase 1: Execute a large query and read a few batches ---
    let rows_before_cancel;
    let batches_before_cancel;
    let cleanup_duration;
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");

        // Use a large query so CloudFetch has many chunks in flight
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
                CONCAT('row_', CAST(a AS STRING), '_', CAST(b AS STRING)) as label,
                REPEAT('x', 100) as padding
            FROM expanded
        "#;

        println!("Executing 1M-row query for cancellation test...");

        stmt.set_sql_query(sql).expect("Failed to set query");
        let mut reader = stmt.execute().expect("Failed to execute query");

        // Read a few batches to ensure CloudFetch pipeline is actively streaming
        let mut rows: u64 = 0;
        let mut batches: u64 = 0;

        for batch_result in &mut reader {
            let batch = batch_result.expect("Error reading batch before cancel");
            rows += batch.num_rows() as u64;
            batches += 1;

            // Read at least a few batches to ensure multi-chunk download is active
            if batches >= 5 {
                break;
            }
        }

        rows_before_cancel = rows;
        batches_before_cancel = batches;

        println!(
            "Read {} batches ({} rows) before cancellation",
            batches_before_cancel, rows_before_cancel
        );
        assert!(
            batches_before_cancel >= 5,
            "Should have read at least 5 batches before cancel"
        );

        // --- Phase 2: Drop reader and statement to trigger pipeline cancellation ---
        // Dropping the reader triggers StreamingCloudFetchProvider::drop which cancels
        // the CancellationToken, stopping all background tasks.
        // Dropping the statement sends a cancel/close to Databricks.
        println!("Dropping reader and statement (triggering cancellation)...");
        let cleanup_start = Instant::now();
        drop(reader);
        drop(stmt);
        cleanup_duration = cleanup_start.elapsed();
        println!(
            "Cleanup completed in {:.2}s",
            cleanup_duration.as_secs_f64()
        );
    }

    // --- Verify: cleanup < 5 seconds (no deadlock) ---
    assert!(
        cleanup_duration < Duration::from_secs(5),
        "Cancellation + cleanup took {:.2}s, exceeding the 5-second threshold. \
         Possible deadlock or resource leak.",
        cleanup_duration.as_secs_f64()
    );

    // --- Verify: no panic (if we reach here, no panic occurred) ---

    // --- Verify: connection can still be used after cancel ---
    // This confirms no resource leak corrupted the connection state.
    println!("Verifying connection is still usable...");
    let mut stmt2 = conn
        .new_statement()
        .expect("Should be able to create new statement after cancel");
    stmt2
        .set_sql_query("SELECT 1 AS health_check")
        .expect("Failed to set query");
    let mut reader2 = stmt2
        .execute()
        .expect("Connection should still work after cancel");
    let batch = reader2
        .next()
        .expect("Should get a result")
        .expect("Result should not be an error");
    assert_eq!(
        batch.num_rows(),
        1,
        "Health check query should return 1 row"
    );

    println!(
        "\n✓ Cancellation completed cleanly in {:.2}s",
        cleanup_duration.as_secs_f64()
    );
    println!("  No deadlock, no panic, no resource leak");
    println!("  Connection remains usable after cancellation");
}
