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

//! Integration tests for the CloudFetch channel-based pipeline.
//!
//! These tests exercise the full pipeline: scheduler → workers → consumer,
//! using a mock HTTP server (wiremock) to simulate cloud storage downloads.

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use databricks_adbc::auth::PersonalAccessToken;
use databricks_adbc::client::{ChunkLinkFetchResult, DatabricksHttpClient, HttpClientConfig};
use databricks_adbc::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use databricks_adbc::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use databricks_adbc::reader::cloudfetch::streaming_provider::StreamingCloudFetchProvider;
use databricks_adbc::types::cloudfetch::{CloudFetchConfig, CloudFetchLink};
use databricks_adbc::types::sea::CompressionCodec;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Create Arrow IPC bytes from RecordBatches.
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

/// Create a test RecordBatch with `num_rows` rows.
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

/// Mock link fetcher that returns links pointing to the mock server.
#[derive(Debug)]
struct MockLinkFetcher {
    base_url: String,
    total_chunks: i64,
    /// Track refetch_link calls.
    refetch_count: AtomicU32,
}

impl MockLinkFetcher {
    fn new(base_url: &str, total_chunks: i64) -> Self {
        Self {
            base_url: base_url.to_string(),
            total_chunks,
            refetch_count: AtomicU32::new(0),
        }
    }

    fn make_link(&self, chunk_index: i64) -> CloudFetchLink {
        CloudFetchLink {
            url: format!("{}/chunk/{}", self.base_url, chunk_index),
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
}

#[async_trait::async_trait]
impl ChunkLinkFetcher for MockLinkFetcher {
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
            .map(|i| self.make_link(i))
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
        Ok(self.make_link(chunk_index))
    }
}

/// Mock link fetcher where chunk 1 initially returns an expired link
/// on first fetch, forcing a 401 from the mock server and triggering recovery.
#[derive(Debug)]
struct Auth401LinkFetcher {
    base_url: String,
    total_chunks: i64,
    /// How many times refetch_link has been called per chunk
    refetch_counts: tokio::sync::Mutex<HashMap<i64, u32>>,
}

impl Auth401LinkFetcher {
    fn new(base_url: &str, total_chunks: i64) -> Self {
        Self {
            base_url: base_url.to_string(),
            total_chunks,
            refetch_counts: tokio::sync::Mutex::new(HashMap::new()),
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
}

#[async_trait::async_trait]
impl ChunkLinkFetcher for Auth401LinkFetcher {
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
                if i == 1 {
                    // Chunk 1: first link points to /expired path → 401
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
        let mut counts = self.refetch_counts.lock().await;
        let count = counts.entry(chunk_index).or_insert(0);
        *count += 1;
        // After refetch, return the correct (non-expired) link
        Ok(self.make_link(chunk_index, ""))
    }
}

/// End-to-end test: all chunks downloaded and consumed in order.
#[tokio::test]
async fn end_to_end_sequential_consumption() {
    let total_chunks = 5;

    // Start mock server
    let server = MockServer::start().await;

    // Mount responses for each chunk
    for i in 0..total_chunks {
        let batch = create_test_batch(i, 100);
        let ipc_data = create_arrow_ipc(&[batch]);

        Mock::given(method("GET"))
            .and(path(format!("/chunk/{}", i)))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ipc_data))
            .mount(&server)
            .await;
    }

    // Create pipeline
    let fetcher: Arc<dyn ChunkLinkFetcher> =
        Arc::new(MockLinkFetcher::new(&server.uri(), total_chunks));
    let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
    let http_client =
        Arc::new(DatabricksHttpClient::new(HttpClientConfig::default(), auth).unwrap());
    let downloader = Arc::new(ChunkDownloader::new(
        http_client,
        CompressionCodec::None,
        0.1,
    ));

    let config = CloudFetchConfig {
        max_chunks_in_memory: 4,
        num_download_workers: 2,
        max_retries: 3,
        retry_delay: Duration::from_millis(10),
        ..CloudFetchConfig::default()
    };

    let provider = StreamingCloudFetchProvider::new(
        config,
        fetcher,
        downloader,
        tokio::runtime::Handle::current(),
    );

    // Consume all batches and verify ordering
    let mut chunk_ids_seen = Vec::new();
    while let Some(batch) = provider.next_batch().await.unwrap() {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        // First value tells us which chunk this came from (chunk_index * 1000 + row)
        let first_id = id_col.value(0);
        let chunk_index = first_id / 1000;
        chunk_ids_seen.push(chunk_index);

        assert_eq!(batch.num_rows(), 100);
        assert_eq!(batch.num_columns(), 2);
    }

    // Verify all chunks arrived in order
    assert_eq!(chunk_ids_seen, vec![0, 1, 2, 3, 4]);
}

/// End-to-end test: cancel mid-stream — no deadlock or panic.
#[tokio::test]
async fn end_to_end_cancellation_mid_stream() {
    let total_chunks = 10;

    let server = MockServer::start().await;

    for i in 0..total_chunks {
        let batch = create_test_batch(i, 50);
        let ipc_data = create_arrow_ipc(&[batch]);

        Mock::given(method("GET"))
            .and(path(format!("/chunk/{}", i)))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(ipc_data)
                    // Small delay to simulate real downloads
                    .set_delay(Duration::from_millis(10)),
            )
            .mount(&server)
            .await;
    }

    let fetcher: Arc<dyn ChunkLinkFetcher> =
        Arc::new(MockLinkFetcher::new(&server.uri(), total_chunks));
    let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
    let http_client =
        Arc::new(DatabricksHttpClient::new(HttpClientConfig::default(), auth).unwrap());
    let downloader = Arc::new(ChunkDownloader::new(
        http_client,
        CompressionCodec::None,
        0.1,
    ));

    let config = CloudFetchConfig {
        max_chunks_in_memory: 4,
        num_download_workers: 2,
        max_retries: 3,
        retry_delay: Duration::from_millis(10),
        ..CloudFetchConfig::default()
    };

    let provider = StreamingCloudFetchProvider::new(
        config,
        fetcher,
        downloader,
        tokio::runtime::Handle::current(),
    );

    // Read first 2 chunks
    let batch1 = provider.next_batch().await.unwrap();
    assert!(batch1.is_some());

    let batch2 = provider.next_batch().await.unwrap();
    assert!(batch2.is_some());

    // Cancel mid-stream
    provider.cancel();

    // After cancel, next_batch should return an error (not hang or panic)
    let result = tokio::time::timeout(Duration::from_secs(5), provider.next_batch()).await;
    assert!(result.is_ok(), "next_batch should not hang after cancel");

    let inner = result.unwrap();
    // It's ok if this is Err (cancelled) — the important thing is no deadlock
    assert!(
        inner.is_err(),
        "Expected error after cancellation, got {:?}",
        inner
    );
}

/// End-to-end test: presigned URL expires mid-stream, driver refetches and continues.
#[tokio::test]
async fn end_to_end_401_recovery() {
    let total_chunks = 3;

    let server = MockServer::start().await;

    // Chunk 0 and 2: normal
    for i in [0, 2] {
        let batch = create_test_batch(i, 100);
        let ipc_data = create_arrow_ipc(&[batch]);

        Mock::given(method("GET"))
            .and(path(format!("/chunk/{}", i)))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ipc_data))
            .mount(&server)
            .await;
    }

    // Chunk 1: /expired path returns 401
    Mock::given(method("GET"))
        .and(path("/chunk/1/expired"))
        .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
        .mount(&server)
        .await;

    // Chunk 1: normal path (after refetch) returns data
    let batch1 = create_test_batch(1, 100);
    let ipc_data1 = create_arrow_ipc(&[batch1]);
    Mock::given(method("GET"))
        .and(path("/chunk/1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(ipc_data1))
        .mount(&server)
        .await;

    // Use the Auth401LinkFetcher
    let fetcher: Arc<dyn ChunkLinkFetcher> =
        Arc::new(Auth401LinkFetcher::new(&server.uri(), total_chunks));
    let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
    let http_client =
        Arc::new(DatabricksHttpClient::new(HttpClientConfig::default(), auth).unwrap());
    let downloader = Arc::new(ChunkDownloader::new(
        http_client,
        CompressionCodec::None,
        0.1,
    ));

    let config = CloudFetchConfig {
        max_chunks_in_memory: 4,
        num_download_workers: 2,
        max_retries: 3,
        max_refresh_retries: 3,
        retry_delay: Duration::from_millis(10),
        ..CloudFetchConfig::default()
    };

    let provider = StreamingCloudFetchProvider::new(
        config,
        fetcher,
        downloader,
        tokio::runtime::Handle::current(),
    );

    // Consume all batches
    let mut chunk_ids_seen = Vec::new();
    while let Some(batch) = provider.next_batch().await.unwrap() {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let first_id = id_col.value(0);
        let chunk_index = first_id / 1000;
        chunk_ids_seen.push(chunk_index);
    }

    // All chunks should have been consumed in order, including chunk 1
    // which required a 401 → refetch → retry recovery
    assert_eq!(chunk_ids_seen, vec![0, 1, 2]);
}
