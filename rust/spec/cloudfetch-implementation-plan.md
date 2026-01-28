# CloudFetch Implementation Plan for Rust ADBC Driver

## Overview

This document outlines the implementation plan for CloudFetch in the Rust ADBC driver using the **streaming approach** for **SEA (Statement Execution API) mode only**. The implementation is based on the reference Databricks JDBC driver's `StreamingChunkProvider` architecture.

CloudFetch enables efficient downloading of Arrow-formatted query results directly from cloud storage (S3, Azure Blob, GCS) instead of through the Databricks server, significantly improving performance for large result sets.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Statement::execute()                          │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      SeaClient (HTTP Client Layer)                      │
│  - POST /api/2.0/sql/statements (execute)                               │
│  - GET /api/2.0/sql/statements/{id}/result/chunks (fetch links)         │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    StreamingCloudFetchProvider                          │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │
│  │ Link Prefetch    │  │ Download Pool    │  │ Consumer Interface   │  │
│  │ Task             │  │ (tokio tasks)    │  │ (RecordBatchReader)  │  │
│  │ - Fetches links  │  │ - Downloads      │  │ - next_batch()       │  │
│  │   ahead of       │  │   chunks in      │  │ - Signals prefetch   │  │
│  │   consumption    │  │   parallel       │  │ - Releases memory    │  │
│  └──────────────────┘  └──────────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Cloud Storage Download                           │
│  - HTTP GET to pre-signed URLs                                          │
│  - LZ4 decompression                                                    │
│  - Arrow IPC parsing                                                    │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Phases

### Phase 1: Core Infrastructure

#### 1.1 HTTP Client Implementation

**File:** `src/client/http.rs` (extend existing stub)

**Dependencies to add:**
```toml
reqwest = { version = "0.12", features = ["json", "gzip", "stream"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

**Components:**
- `DatabricksHttpClient` - Core HTTP client with retry logic
  - Connection pooling (reuse connections)
  - Configurable timeouts (connect, read, total)
  - Automatic retry with exponential backoff
  - User-Agent header: `databricks-adbc-rust/{version}`
  - Bearer token authentication

**Key Methods:**
```rust
impl DatabricksHttpClient {
    /// Execute HTTP request with retry logic
    /// Used for both API calls and CloudFetch downloads
    pub async fn execute(&self, request: Request) -> Result<Response>;
}
```

The `execute` method handles all HTTP requests uniformly. Callers build their own `Request` objects with the appropriate method, URL, headers, and body. This keeps the HTTP client simple and avoids inconsistent convenience methods.

**Configuration:**
```rust
pub struct HttpClientConfig {
    pub connect_timeout: Duration,      // Default: 30s
    pub read_timeout: Duration,         // Default: 60s
    pub max_retries: u32,               // Default: 5
    pub retry_delay: Duration,          // Default: 1.5s
    pub max_connections: usize,         // Default: 100
    pub user_agent: String,
}
```

---

#### 1.2 SEA Client Implementation

**File:** `src/client/sea.rs` (new file)

**Purpose:** Implements the Databricks SQL Statement Execution API

**API Endpoints:**
1. `POST /api/2.0/sql/statements` - Execute SQL statement
2. `GET /api/2.0/sql/statements/{statement_id}` - Get statement status
3. `GET /api/2.0/sql/statements/{statement_id}/result/chunks?chunk_index={n}` - Fetch result chunks
4. `POST /api/2.0/sql/statements/{statement_id}/cancel` - Cancel statement

**Types:** See `src/types/sea.rs` for all SEA API data structures (`StatementExecutionResponse`, `ResultData`, `ExternalLink`, etc.)

**Key Methods:**
```rust
impl SeaClient {
    /// Execute a SQL statement
    pub async fn execute_statement(&self, sql: &str, params: ExecuteParams) -> Result<StatementExecutionResponse>;

    /// Poll statement status
    pub async fn get_statement_status(&self, statement_id: &str) -> Result<StatementExecutionResponse>;

    /// Fetch result chunks starting from chunk_index
    pub async fn get_result_chunks(&self, statement_id: &str, chunk_index: i64) -> Result<ResultData>;

    /// Cancel statement
    pub async fn cancel_statement(&self, statement_id: &str) -> Result<()>;

    /// Close statement (cleanup resources)
    pub async fn close_statement(&self, statement_id: &str) -> Result<()>;
}
```

---

### Phase 2: CloudFetch Core Components

#### 2.1 Types Module

**File:** `src/types/sea.rs` (new file)

**Purpose:** SEA API request/response types that map directly to JSON

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct StatementExecutionResponse {
    pub statement_id: String,
    pub status: StatementStatus,
    pub manifest: Option<ResultManifest>,
    pub result: Option<ResultData>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StatementStatus {
    pub state: StatementState,
    pub error: Option<ServiceError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatementState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
    Closed,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceError {
    pub error_code: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResultManifest {
    pub format: String,
    pub schema: ResultSchema,
    pub total_chunk_count: Option<i64>,
    pub total_row_count: Option<i64>,
    pub total_byte_count: Option<i64>,
    pub truncated: bool,
    pub chunks: Option<Vec<ChunkInfo>>,
    pub result_compression: Option<String>,  // "LZ4_FRAME" or null/absent for none
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionCodec {
    #[default]
    None,
    Lz4Frame,
}

impl CompressionCodec {
    pub fn from_manifest(value: Option<&str>) -> Self {
        match value {
            Some("LZ4_FRAME") => Self::Lz4Frame,
            _ => Self::None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResultSchema {
    pub column_count: i32,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String,
    pub type_text: String,
    pub position: i32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkInfo {
    pub chunk_index: i64,
    pub row_offset: i64,
    pub row_count: i64,
    pub byte_count: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResultData {
    pub chunk_index: i64,
    pub row_offset: i64,
    pub row_count: i64,
    pub byte_count: i64,
    pub next_chunk_index: Option<i64>,
    pub next_chunk_internal_link: Option<String>,
    pub external_links: Option<Vec<ExternalLink>>,
    pub data_array: Option<Vec<Vec<String>>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExternalLink {
    pub external_link: String,
    pub expiration: String,  // ISO 8601 timestamp
    pub chunk_index: i64,
    pub row_offset: i64,
    pub row_count: i64,
    pub byte_count: i64,
    pub http_headers: Option<HashMap<String, String>>,
    pub next_chunk_index: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecuteStatementRequest {
    pub warehouse_id: String,
    pub statement: String,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub disposition: String,  // "EXTERNAL_LINKS" for CloudFetch
    pub format: String,       // "ARROW_STREAM"
    pub wait_timeout: Option<String>,
}
```

---

**File:** `src/types/cloudfetch.rs` (new file)

**Purpose:** Internal CloudFetch types with parsed/validated data

```rust
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for CloudFetch streaming
#[derive(Debug, Clone)]
pub struct CloudFetchConfig {
    pub parallel_downloads: usize,       // Default: 16
    pub link_prefetch_window: usize,     // Default: 128
    pub max_chunks_in_memory: usize,     // Default: parallel_downloads
    pub max_retries: u32,                // Default: 5
    pub retry_delay: Duration,           // Default: 1.5s
    pub chunk_ready_timeout: Option<Duration>,
    pub speed_threshold_mbps: f64,       // Default: 0.1
    pub enabled: bool,                   // Default: true
}

/// Parsed external link with validated expiration
#[derive(Debug, Clone)]
pub struct CloudFetchLink {
    pub url: String,
    pub chunk_index: i64,
    pub row_offset: i64,
    pub row_count: i64,
    pub byte_count: i64,
    pub expiration: DateTime<Utc>,
    pub http_headers: HashMap<String, String>,
    pub next_chunk_index: Option<i64>,
}

impl CloudFetchLink {
    /// Check if link is expired (with 30s buffer)
    pub fn is_expired(&self) -> bool {
        Utc::now() + chrono::Duration::seconds(30) >= self.expiration
    }

    /// Convert from SEA API response
    pub fn from_external_link(link: &ExternalLink) -> Result<Self> {
        let expiration = DateTime::parse_from_rfc3339(&link.expiration)?
            .with_timezone(&Utc);
        Ok(Self {
            url: link.external_link.clone(),
            chunk_index: link.chunk_index,
            row_offset: link.row_offset,
            row_count: link.row_count,
            byte_count: link.byte_count,
            expiration,
            http_headers: link.http_headers.clone().unwrap_or_default(),
            next_chunk_index: link.next_chunk_index,
        })
    }
}

/// State of a chunk in the download pipeline
#[derive(Debug)]
pub enum ChunkState {
    Pending,
    Downloading,
    Downloaded(Vec<RecordBatch>),
    Failed(String),
    Released,
}

/// A downloaded Arrow chunk
pub struct ArrowChunk {
    pub chunk_index: i64,
    pub row_offset: i64,
    pub row_count: i64,
    state: std::sync::Mutex<ChunkState>,
}
```

---

#### 2.2 Arrow IPC Parsing

**File:** `src/reader/cloudfetch/arrow_parser.rs` (new file)

**What CloudFetch Returns:**

CloudFetch downloads return **Arrow IPC Streaming format** bytes from cloud storage. This is a standard Arrow format - we don't need to write a custom parser.

```
┌─────────────────────────────────────────────────────────────┐
│                    Cloud Storage Response                    │
├─────────────────────────────────────────────────────────────┤
│  Optional: LZ4 Frame Compression                            │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              Arrow IPC Stream Format                   │  │
│  │  ┌─────────────┐                                      │  │
│  │  │   Schema    │  (column names, types)               │  │
│  │  ├─────────────┤                                      │  │
│  │  │  Batch 0    │  (N rows of columnar data)           │  │
│  │  ├─────────────┤                                      │  │
│  │  │  Batch 1    │  (N rows of columnar data)           │  │
│  │  ├─────────────┤                                      │  │
│  │  │    ...      │                                      │  │
│  │  ├─────────────┤                                      │  │
│  │  │  EOS marker │  (end of stream)                     │  │
│  │  └─────────────┘                                      │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**Dependencies:**
```toml
arrow-ipc = "57"        # Provides StreamReader for Arrow IPC format
lz4_flex = "0.11"       # LZ4 frame decompression
```

**Implementation:**

We use `arrow_ipc::reader::StreamReader` - no custom parsing needed:

```rust
use arrow_ipc::reader::StreamReader;
use arrow_array::RecordBatch;
use std::io::Cursor;

use crate::types::sea::CompressionCodec;

/// Parse CloudFetch response into RecordBatches
///
/// The response is Arrow IPC streaming format, optionally LZ4 compressed.
/// The compression codec is specified in the ResultManifest from the initial
/// statement execution response (manifest.result_compression field).
/// A single chunk may contain multiple RecordBatches.
pub fn parse_cloudfetch_response(
    data: &[u8],
    compression: CompressionCodec,
) -> Result<Vec<RecordBatch>> {
    // 1. Decompress if needed (based on manifest.result_compression)
    let decompressed: Vec<u8>;
    let bytes: &[u8] = match compression {
        CompressionCodec::Lz4Frame => {
            decompressed = lz4_flex::frame::decompress_size_prepended(data)?;
            &decompressed
        }
        CompressionCodec::None => data,
    };

    // 2. Parse Arrow IPC stream using arrow-ipc crate
    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None)?;

    // 3. Collect all batches from the stream
    let batches: Vec<RecordBatch> = reader
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    Ok(batches)
}
```

**Why This Works:**

- `StreamReader` handles the Arrow IPC streaming format (schema + batches + EOS marker)
- The schema is embedded in the IPC stream, so we don't need to pass it separately
- Each chunk from CloudFetch is a self-contained IPC stream
- Multiple `RecordBatch`es may be returned from a single chunk (the server decides batch sizes)

---

### Phase 3: Streaming CloudFetch Provider

This phase implements three separate components with clear responsibilities:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         StreamingCloudFetchProvider                         │
│                            (Orchestrator)                                   │
│                                                                             │
│   Coordinates link fetching and chunk downloading, provides consumer API    │
│                                                                             │
│         ┌─────────────────────┐         ┌─────────────────────┐            │
│         │    LinkFetcher      │         │   ChunkDownloader   │            │
│         │                     │         │                     │            │
│         │ - Fetches links     │         │ - Downloads bytes   │            │
│         │   from SEA API      │         │   from cloud storage│            │
│         │ - Handles           │         │ - Decompresses LZ4  │            │
│         │   pagination        │         │ - Parses Arrow IPC  │            │
│         │ - Refetches expired │         │ - Retry logic       │            │
│         │   links             │         │                     │            │
│         └─────────────────────┘         └─────────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

#### 3.1 LinkFetcher

**File:** `src/reader/cloudfetch/link_fetcher.rs` (new file)

**Purpose:** Fetches chunk links from the SEA API. Single responsibility: link management.

```rust
/// Fetches external links for result chunks from the SEA API.
///
/// Handles:
/// - Initial link population from statement response
/// - Fetching additional links as consumer advances
/// - Refetching expired links for retry scenarios
pub struct LinkFetcher {
    sea_client: Arc<SeaClient>,
    statement_id: String,
}

impl LinkFetcher {
    pub fn new(sea_client: Arc<SeaClient>, statement_id: String) -> Self;

    /// Fetch links starting from the given chunk index.
    /// Returns the links and the next chunk index (None if end of stream).
    pub async fn fetch_links(&self, start_chunk_index: i64) -> Result<FetchLinksResult>;

    /// Refetch a single link by chunk index (used when link expires before download).
    pub async fn refetch_link(&self, chunk_index: i64) -> Result<CloudFetchLink>;
}

pub struct FetchLinksResult {
    pub links: Vec<CloudFetchLink>,
    pub next_chunk_index: Option<i64>,  // None = end of stream
}
```

---

#### 3.2 ChunkDownloader

**File:** `src/reader/cloudfetch/chunk_downloader.rs` (new file)

**Purpose:** Downloads chunk data from cloud storage URLs. Single responsibility: HTTP download + parsing.

```rust
/// Downloads Arrow data from cloud storage presigned URLs.
///
/// Handles:
/// - HTTP GET to presigned URL with custom headers
/// - LZ4 decompression (if compression enabled)
/// - Arrow IPC stream parsing into RecordBatches
/// - Download speed monitoring
pub struct ChunkDownloader {
    http_client: Arc<DatabricksHttpClient>,
    compression: CompressionCodec,
    speed_threshold_mbps: f64,
}

impl ChunkDownloader {
    pub fn new(
        http_client: Arc<DatabricksHttpClient>,
        compression: CompressionCodec,
        speed_threshold_mbps: f64,
    ) -> Self;

    /// Download a chunk from the given link.
    /// Returns parsed RecordBatches.
    pub async fn download(&self, link: &CloudFetchLink) -> Result<Vec<RecordBatch>>;
}
```

**Implementation:**
```rust
impl ChunkDownloader {
    pub async fn download(&self, link: &CloudFetchLink) -> Result<Vec<RecordBatch>> {
        let start = Instant::now();

        // 1. Build and execute HTTP request
        let request = Request::builder()
            .method(Method::GET)
            .uri(&link.url)
            .headers_from_map(&link.http_headers)
            .build()?;

        let response = self.http_client.execute(request).await?;
        let bytes = response.bytes().await?;

        // 2. Log download metrics
        let elapsed = start.elapsed();
        let speed_mbps = (bytes.len() as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();

        if speed_mbps < self.speed_threshold_mbps {
            tracing::warn!(
                "CloudFetch download slower than threshold: {:.2} MB/s (threshold: {:.2} MB/s)",
                speed_mbps,
                self.speed_threshold_mbps
            );
        }

        // 3. Decompress and parse Arrow IPC
        let batches = parse_cloudfetch_response(&bytes, self.compression)?;

        Ok(batches)
    }
}
```

---

#### 3.3 StreamingCloudFetchProvider

**File:** `src/reader/cloudfetch/streaming_provider.rs` (new file)

**Purpose:** Orchestrates link fetching and chunk downloading. Manages concurrency and provides the consumer API.

**Architecture:**
```
┌─────────────────────────────────────────────────────────────────┐
│                  StreamingCloudFetchProvider                    │
│                                                                 │
│  ┌───────────────────┐                                         │
│  │   LinkFetcher     │◄─── Fetches links from SEA API          │
│  └─────────┬─────────┘                                         │
│            │                                                    │
│            ▼                                                    │
│  ┌───────────────────┐                                         │
│  │   Links Map       │  DashMap<chunk_index, CloudFetchLink>   │
│  │   (prefetched)    │                                         │
│  └─────────┬─────────┘                                         │
│            │                                                    │
│            ▼                                                    │
│  ┌───────────────────┐      ┌─────────────────────┐            │
│  │ Download Semaphore│─────►│  ChunkDownloader    │            │
│  │ (parallel limit)  │      │  (per download)     │            │
│  └───────────────────┘      └──────────┬──────────┘            │
│                                        │                        │
│                                        ▼                        │
│                             ┌─────────────────────┐            │
│                             │   Chunks Map        │            │
│                             │   (downloaded)      │            │
│                             └─────────────────────┘            │
└─────────────────────────────────────────────────────────────────┘
```

**Struct:**
```rust
pub struct StreamingCloudFetchProvider {
    // Dependencies (injected)
    link_fetcher: LinkFetcher,
    chunk_downloader: ChunkDownloader,
    config: CloudFetchConfig,

    // State
    current_chunk_index: AtomicI64,
    next_link_fetch_index: AtomicI64,
    end_of_stream: AtomicBool,

    // Storage
    links: DashMap<i64, CloudFetchLink>,
    chunks: DashMap<i64, Vec<RecordBatch>>,

    // Concurrency control
    download_semaphore: Arc<Semaphore>,
    cancel_token: CancellationToken,
}
```

**Key Methods:**
```rust
impl StreamingCloudFetchProvider {
    /// Create new provider with initial links from statement response.
    pub fn new(
        config: CloudFetchConfig,
        link_fetcher: LinkFetcher,
        chunk_downloader: ChunkDownloader,
        initial_links: Vec<CloudFetchLink>,
    ) -> Self;

    /// Get next record batch. Main consumer interface.
    /// Blocks until the next batch is available or end of stream.
    pub async fn next_batch(&self) -> Result<Option<RecordBatch>>;

    /// Cancel all pending operations.
    pub fn cancel(&self);

    // --- Internal methods ---

    /// Background task: fetch links ahead of consumer position.
    async fn run_link_prefetch_loop(&self);

    /// Spawn download tasks for available links up to concurrency limit.
    fn schedule_chunk_downloads(&self);

    /// Download a single chunk with retry and link refresh on expiry.
    async fn download_chunk_with_retry(&self, chunk_index: i64) -> Result<Vec<RecordBatch>>;

    /// Release a consumed chunk to free memory and allow more downloads.
    fn release_chunk(&self, chunk_index: i64);
}
```

---

#### 3.4 Link Prefetch Loop

**Purpose:** Background task that keeps links prefetched ahead of consumption.

```rust
async fn run_link_prefetch_loop(&self) {
    loop {
        tokio::select! {
            _ = self.cancel_token.cancelled() => break,
            _ = async {
                let current = self.current_chunk_index.load(Ordering::Acquire);
                let prefetched = self.next_link_fetch_index.load(Ordering::Acquire);
                let window = self.config.link_prefetch_window as i64;

                // Fetch more links if we're running low
                if prefetched - current < window && !self.end_of_stream.load(Ordering::Acquire) {
                    match self.link_fetcher.fetch_links(prefetched).await {
                        Ok(result) => {
                            // Store fetched links
                            for link in result.links {
                                self.links.insert(link.chunk_index, link);
                            }

                            // Update state
                            match result.next_chunk_index {
                                Some(next) => {
                                    self.next_link_fetch_index.store(next, Ordering::Release);
                                }
                                None => {
                                    self.end_of_stream.store(true, Ordering::Release);
                                }
                            }

                            // Trigger downloads for newly fetched links
                            self.schedule_chunk_downloads();
                        }
                        Err(e) => {
                            tracing::warn!("Failed to fetch links: {}", e);
                        }
                    }
                }

                // Small delay before next check
                tokio::time::sleep(Duration::from_millis(10)).await;
            } => {}
        }
    }
}
```

---

#### 3.5 Chunk Download with Retry

**Purpose:** Downloads a chunk, handling retries and link expiration.

```rust
async fn download_chunk_with_retry(&self, chunk_index: i64) -> Result<Vec<RecordBatch>> {
    let mut attempts = 0;

    loop {
        // Get link (may need to refetch if expired)
        let link = match self.links.get(&chunk_index) {
            Some(link) if !link.is_expired() => link.clone(),
            _ => {
                // Link missing or expired - refetch it
                self.link_fetcher.refetch_link(chunk_index).await?
            }
        };

        // Attempt download
        match self.chunk_downloader.download(&link).await {
            Ok(batches) => return Ok(batches),
            Err(e) => {
                attempts += 1;
                if attempts >= self.config.max_retries {
                    return Err(e);
                }
                tracing::warn!(
                    "Chunk {} download failed (attempt {}/{}): {}",
                    chunk_index, attempts, self.config.max_retries, e
                );
                tokio::time::sleep(self.config.retry_delay).await;
            }
        }
    }
}
```

---

### Phase 4: Integration with ADBC Traits

#### 4.1 ResultReaderFactory

**File:** `src/reader/mod.rs` (add factory)

The `Statement` shouldn't know about CloudFetch internals. We introduce a factory that inspects the response and creates the appropriate reader.

```rust
use crate::types::sea::StatementExecutionResponse;

/// Factory that creates the appropriate reader based on the response type.
/// Encapsulates the decision of CloudFetch vs inline vs other result formats.
pub struct ResultReaderFactory {
    sea_client: Arc<SeaClient>,
    http_client: Arc<DatabricksHttpClient>,
    config: CloudFetchConfig,
}

impl ResultReaderFactory {
    pub fn new(
        sea_client: Arc<SeaClient>,
        http_client: Arc<DatabricksHttpClient>,
        config: CloudFetchConfig,
    ) -> Self {
        Self { sea_client, http_client, config }
    }

    /// Create a reader from a statement execution response.
    /// Automatically selects CloudFetch, inline, or other reader based on response.
    pub fn create_reader(
        &self,
        response: StatementExecutionResponse,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let compression = response.manifest
            .as_ref()
            .and_then(|m| m.result_compression.as_deref())
            .map(CompressionCodec::from_manifest)
            .unwrap_or_default();

        // CloudFetch: external_links present
        if let Some(ref result) = response.result {
            if let Some(ref external_links) = result.external_links {
                return self.create_cloudfetch_reader(
                    &response.statement_id,
                    external_links,
                    compression,
                );
            }

            // Inline results: data_array present (future implementation)
            if result.data_array.is_some() {
                return Err(Error::not_implemented()
                    .message("Inline results not yet supported"));
            }
        }

        Err(Error::invalid_state().message("No result data in response"))
    }

    fn create_cloudfetch_reader(
        &self,
        statement_id: &str,
        external_links: &[ExternalLink],
        compression: CompressionCodec,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let initial_links: Vec<CloudFetchLink> = external_links
            .iter()
            .map(CloudFetchLink::from_external_link)
            .collect::<Result<Vec<_>, _>>()?;

        let link_fetcher = LinkFetcher::new(
            self.sea_client.clone(),
            statement_id.to_string(),
        );

        let chunk_downloader = ChunkDownloader::new(
            self.http_client.clone(),
            compression,
            self.config.speed_threshold_mbps,
        );

        let provider = StreamingCloudFetchProvider::new(
            self.config.clone(),
            link_fetcher,
            chunk_downloader,
            initial_links,
        );

        Ok(Box::new(provider))
    }
}
```

---

#### 4.2 RecordBatchReader on StreamingCloudFetchProvider

**File:** `src/reader/cloudfetch/streaming_provider.rs` (add trait impl)

```rust
impl RecordBatchReader for StreamingCloudFetchProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Iterator for StreamingCloudFetchProvider {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Block on async (ADBC trait is synchronous)
        match futures::executor::block_on(self.next_batch()) {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(e) => Some(Err(ArrowError::ExternalError(Box::new(e)))),
        }
    }
}
```

---

#### 4.3 Statement Implementation

**File:** `src/statement.rs` (modify existing)

Now `Statement` is clean - it delegates reader creation to the factory.

```rust
pub struct Statement {
    query: Option<String>,
    sea_client: Arc<SeaClient>,
    reader_factory: ResultReaderFactory,
    runtime: tokio::runtime::Handle,
}

impl adbc_core::Statement for Statement {
    fn execute(&mut self) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        let query = self.query.as_ref()
            .ok_or_else(|| Error::invalid_state().message("No query set"))?;

        // Execute via SEA API
        let response = self.runtime.block_on(
            self.sea_client.execute_statement(query, ExecuteParams::default())
        ).to_adbc()?;

        // Wait for completion if async
        let response = self.wait_for_completion(response)?;

        // Factory decides which reader to create based on response
        self.reader_factory.create_reader(response).to_adbc()
    }
}
```

**Benefits:**
- `Statement` doesn't know about CloudFetch, inline, or any other reader type
- Easy to add new reader types (just add a branch in the factory)
- Reader creation logic is testable in isolation
- Single Responsibility: Statement executes queries, factory creates readers

---

### Phase 5: Connection and Configuration

This phase covers how configuration flows through the ADBC hierarchy and where clients are created.

**ADBC Object Hierarchy:**
```
Driver
  └── Database (configuration set here via set_option)
        └── Connection (clients created here, shared across statements)
              └── Statement (uses shared clients via ResultReaderFactory)
```

---

#### 5.1 Database Configuration

**File:** `src/database.rs` (modify existing)

Users configure the driver at the `Database` level via ADBC's `set_option()` API.
These options are parsed and stored, then passed to `Connection` on creation.

**New CloudFetch Options:**
```rust
// Option keys (strings passed to set_option)
"databricks.cloudfetch.enabled"             // bool, default: true
"databricks.cloudfetch.parallel_downloads"  // usize, default: 16
"databricks.cloudfetch.link_prefetch_window"// usize, default: 128
"databricks.cloudfetch.max_chunks_in_memory"// usize, default: 16
"databricks.cloudfetch.max_retries"         // u32, default: 5
"databricks.cloudfetch.retry_delay_ms"      // u64, default: 1500
"databricks.cloudfetch.speed_threshold_mbps"// f64, default: 0.1
```

**Usage Example:**
```rust
let mut db = driver.new_database()?;
db.set_option("uri", "https://my-workspace.databricks.com")?;
db.set_option("databricks.http_path", "/sql/1.0/warehouses/abc123")?;
db.set_option("databricks.access_token", "dapi...")?;

// Optional: tune CloudFetch settings
db.set_option("databricks.cloudfetch.parallel_downloads", "8")?;

let conn = db.new_connection()?;
```

---

#### 5.2 Connection Updates

**File:** `src/connection.rs` (modify existing)

`Connection` creates and owns the HTTP clients. These are shared across all statements
created from this connection (via `Arc`).

```rust
pub struct Connection {
    // Configuration (from Database)
    host: String,
    http_path: String,
    warehouse_id: String,

    // Shared clients (created once, reused by all statements)
    sea_client: Arc<SeaClient>,
    http_client: Arc<DatabricksHttpClient>,

    // CloudFetch settings (from Database options)
    cloudfetch_config: CloudFetchConfig,

    // Tokio runtime for async operations
    runtime: tokio::runtime::Runtime,
}

impl Connection {
    /// Called by Database::new_connection()
    pub fn new(config: ConnectionConfig) -> Result<Self> {
        let runtime = tokio::runtime::Runtime::new()?;

        // Create HTTP client with connection pooling
        let http_client = Arc::new(DatabricksHttpClient::new(
            config.http_config,
            config.auth_provider,
        )?);

        // Create SEA client (wraps HTTP client for API calls)
        let sea_client = Arc::new(SeaClient::new(
            http_client.clone(),
            &config.host,
            &config.http_path,
        ));

        Ok(Self {
            host: config.host,
            http_path: config.http_path,
            warehouse_id: config.warehouse_id,
            sea_client,
            http_client,
            cloudfetch_config: config.cloudfetch_config,
            runtime,
        })
    }

    /// Create a new statement. Passes shared clients via ResultReaderFactory.
    pub fn new_statement(&self) -> Result<Statement> {
        let reader_factory = ResultReaderFactory::new(
            self.sea_client.clone(),
            self.http_client.clone(),
            self.cloudfetch_config.clone(),
        );

        Ok(Statement::new(
            self.sea_client.clone(),
            reader_factory,
            self.runtime.handle().clone(),
        ))
    }
}
```

**Why this design:**
- **Connection pooling:** `DatabricksHttpClient` maintains a connection pool. Sharing it across statements avoids redundant connections.
- **Single runtime:** One tokio runtime per connection, shared by all async operations.
- **Configuration inheritance:** CloudFetch config flows from Database → Connection → Statement.

---

## File Structure Summary

```
rust/src/
├── lib.rs                          # Add new module exports
├── types/                          # [NEW] Data structures / models
│   ├── mod.rs                      # Re-exports all types
│   ├── sea.rs                      # [NEW] SEA API request/response types
│   │                               #   - StatementExecutionResponse
│   │                               #   - StatementStatus, StatementState
│   │                               #   - ResultManifest, ResultData
│   │                               #   - ExternalLink, ChunkInfo
│   │                               #   - ExecuteStatementRequest, ServiceError
│   │                               #   - CompressionCodec
│   └── cloudfetch.rs               # [NEW] CloudFetch-specific types
│                                   #   - CloudFetchConfig
│                                   #   - CloudFetchLink (parsed from ExternalLink)
│                                   #   - ChunkState, ArrowChunk
├── client/
│   ├── mod.rs                      # Update exports
│   ├── http.rs                     # [MODIFY] Implement HTTP client
│   └── sea.rs                      # [NEW] SEA API client (uses types::sea)
├── reader/
│   ├── mod.rs                      # Update exports
│   └── cloudfetch/                 # [NEW] CloudFetch implementation
│       ├── mod.rs                  # Re-exports cloudfetch components
│       ├── arrow_parser.rs         # [NEW] Arrow IPC parsing + LZ4 decompression
│       ├── link_fetcher.rs         # [NEW] Fetches links from SEA API
│       ├── chunk_downloader.rs     # [NEW] Downloads chunks from cloud storage
│       └── streaming_provider.rs   # [NEW] Orchestrates link fetching + downloading
│                                   #       Also implements RecordBatchReader trait
├── statement.rs                    # [MODIFY] Execute with CloudFetch
├── connection.rs                   # [MODIFY] Store clients
└── database.rs                     # [MODIFY] CloudFetch options
```

### Module Organization Rationale

- **`types/`** - All data structures separated by domain. This keeps client/reader modules focused on behavior, not type definitions. Types are re-exported from `types/mod.rs` for convenient access (`use crate::types::{StatementExecutionResponse, CloudFetchLink}`).

- **`types/sea.rs`** - SEA API types that map directly to JSON request/response structures. These have `#[derive(Serialize, Deserialize)]` for serde.

- **`types/cloudfetch.rs`** - Internal types for CloudFetch processing. `CloudFetchLink` is a parsed/validated version of `ExternalLink` with `DateTime<Utc>` instead of string timestamps.

- **`reader/`** - Parent module for all result fetching strategies. Could define a common `ChunkProvider` trait here.

- **`reader/cloudfetch/`** - CloudFetch-specific implementation. Isolated so future readers (e.g., `reader/inline/` for inline JSON results) can be added without cluttering the namespace.

---

## Dependencies to Add

```toml
[dependencies]
# Existing
adbc_core = "0.22"
driverbase = { git = "..." }
arrow-array = "57"
arrow-schema = "57"
tracing = "0.1"

# New dependencies
arrow-ipc = "57"                    # Arrow IPC format parsing
reqwest = { version = "0.12", features = ["json", "gzip", "stream"] }
tokio = { version = "1", features = ["full", "sync"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
chrono = { version = "0.4", features = ["serde"] }
lz4_flex = "0.11"                   # LZ4 decompression
dashmap = "6"                       # Concurrent HashMap
futures = "0.3"                     # Async utilities
tokio-util = "0.7"                  # Cancellation tokens
thiserror = "2"                     # Error handling
```

---

## Testing Strategy

Tests are organized by component, following the separation of concerns in the implementation.

### Unit Tests

Each component is tested in isolation with mocked dependencies.

#### 1. Types Tests (`types/`)

```rust
#[cfg(test)]
mod tests {
    // CloudFetchLink
    - from_external_link() parses valid ExternalLink
    - from_external_link() fails on invalid timestamp
    - is_expired() returns false for future expiration
    - is_expired() returns true when within 30s buffer

    // CompressionCodec
    - from_manifest() parses "LZ4_FRAME"
    - from_manifest() defaults to None for unknown values

    // CloudFetchConfig
    - default values are correct
}
```

#### 2. Arrow Parser Tests (`reader/cloudfetch/arrow_parser.rs`)

```rust
#[cfg(test)]
mod tests {
    // Use pre-generated Arrow IPC test files
    - parse_cloudfetch_response() parses uncompressed Arrow IPC
    - parse_cloudfetch_response() decompresses and parses LZ4 data
    - parse_cloudfetch_response() handles multiple batches in one stream
    - parse_cloudfetch_response() returns error on invalid data
    - parse_cloudfetch_response() returns error on truncated data
}
```

#### 3. LinkFetcher Tests (`reader/cloudfetch/link_fetcher.rs`)

```rust
#[cfg(test)]
mod tests {
    // Mock SeaClient
    - fetch_links() returns links from API response
    - fetch_links() returns next_chunk_index for pagination
    - fetch_links() returns None for end of stream
    - refetch_link() fetches specific chunk by index
    - refetch_link() returns error if chunk not found
}
```

#### 4. ChunkDownloader Tests (`reader/cloudfetch/chunk_downloader.rs`)

```rust
#[cfg(test)]
mod tests {
    // Mock HTTP client, use test Arrow data
    - download() fetches and parses uncompressed chunk
    - download() fetches, decompresses, and parses LZ4 chunk
    - download() logs warning when speed below threshold
    - download() propagates HTTP errors
    - download() propagates parse errors
}
```

#### 5. ResultReaderFactory Tests (`reader/mod.rs`)

```rust
#[cfg(test)]
mod tests {
    // Mock clients
    - create_reader() returns CloudFetch reader when external_links present
    - create_reader() returns error for inline results (not yet implemented)
    - create_reader() returns error when no result data
    - create_reader() correctly extracts compression codec from manifest
}
```

#### 6. HTTP Client Tests (`client/http.rs`)

```rust
#[cfg(test)]
mod tests {
    // Use wiremock or similar
    - execute() retries on transient failures
    - execute() respects max_retries config
    - execute() applies exponential backoff
    - execute() adds authorization header
    - execute() adds user-agent header
    - execute() times out after configured duration
}
```

#### 7. SEA Client Tests (`client/sea.rs`)

```rust
#[cfg(test)]
mod tests {
    // Mock HTTP client
    - execute_statement() sends correct request body
    - execute_statement() parses success response
    - execute_statement() parses error response
    - get_result_chunks() requests correct chunk_index
    - get_result_chunks() parses external_links
    - cancel_statement() sends POST to cancel endpoint
}
```

---

### Integration Tests

Test multiple components working together with mocked external services.

#### 1. StreamingCloudFetchProvider Tests

```rust
// tests/cloudfetch_integration.rs

// Mock both LinkFetcher and ChunkDownloader
- provider returns batches in order
- provider prefetches links ahead of consumption
- provider handles single chunk result
- provider handles multi-chunk result
- provider stops at end of stream
- provider respects parallel download limit (semaphore)
- provider retries failed downloads
- provider refetches expired links before retry
- cancel() stops background tasks
- provider implements RecordBatchReader correctly
```

#### 2. Statement Execution Tests

```rust
// tests/statement_integration.rs

// Mock SEA client responses
- execute() with CloudFetch response returns working reader
- execute() polls for completion on PENDING state
- execute() returns error on FAILED state
- execute() without query returns error
```

#### 3. Full ADBC Flow Tests

```rust
// tests/adbc_flow.rs

// Mock HTTP layer
- Driver -> Database -> Connection -> Statement -> execute -> iterate batches
- Configuration options flow through hierarchy correctly
- Multiple statements share same connection clients
```

---

### End-to-End Tests

Test against real Databricks with real data. Requires environment variables:
- `DATABRICKS_HOST`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_TOKEN`

```rust
// tests/e2e.rs (ignored by default, run with --ignored)

#[test]
#[ignore]
fn test_cloudfetch_small_result() {
    // Query returning < 1 chunk
    // Verify row count and data types
}

#[test]
#[ignore]
fn test_cloudfetch_large_result() {
    // Query returning multiple chunks
    // Verify all rows received
    // Verify ordering preserved
}

#[test]
#[ignore]
fn test_cloudfetch_with_compression() {
    // Verify LZ4 decompression works with real data
}

#[test]
#[ignore]
fn test_concurrent_statements() {
    // Multiple statements on same connection
    // Verify no interference
}
```

---

### Test Utilities

#### Mock Arrow Data Generator

```rust
// tests/utils/arrow_test_data.rs

/// Generate Arrow IPC bytes for testing
pub fn create_test_arrow_ipc(
    schema: &Schema,
    batches: &[RecordBatch],
    compress: bool,
) -> Vec<u8>;

/// Create simple test schema (id: i64, name: string, value: f64)
pub fn test_schema() -> Schema;

/// Create test batches with deterministic data
pub fn test_batches(num_batches: usize, rows_per_batch: usize) -> Vec<RecordBatch>;
```

#### Mock SEA Server

```rust
// tests/utils/mock_sea_server.rs

/// In-memory mock that responds to SEA API calls
pub struct MockSeaServer {
    responses: HashMap<String, StatementExecutionResponse>,
    chunk_data: HashMap<(String, i64), Vec<u8>>,  // (statement_id, chunk_index) -> Arrow bytes
}

impl MockSeaServer {
    pub fn with_cloudfetch_response(chunks: Vec<Vec<u8>>) -> Self;
    pub fn handle_execute(&self, req: ExecuteStatementRequest) -> StatementExecutionResponse;
    pub fn handle_get_chunks(&self, statement_id: &str, chunk_index: i64) -> ResultData;
}
```

#### Mock HTTP Server for CloudFetch Downloads

```rust
// tests/utils/mock_cloud_storage.rs

/// HTTP server that serves presigned URL downloads
/// Uses wiremock or similar
pub async fn start_mock_cloud_storage(
    chunks: HashMap<String, Vec<u8>>,  // url_path -> Arrow bytes
) -> MockServer;
```

---

## Implementation Order

1. **Phase 1: Types & HTTP Client**
   - Define types in `types/sea.rs` and `types/cloudfetch.rs`
   - Implement `DatabricksHttpClient` with retry logic
   - Implement `SeaClient` with all SEA endpoints
   - Unit tests for API calls

2. **Phase 2: CloudFetch Components**
   - Implement `arrow_parser.rs` (LZ4 + Arrow IPC parsing)
   - Implement `LinkFetcher`
   - Implement `ChunkDownloader`
   - Unit tests for each component

3. **Phase 3: Streaming Provider**
   - Implement `StreamingCloudFetchProvider`
   - Implement `RecordBatchReader` trait on provider
   - Link prefetch background task
   - Download scheduling with semaphore

4. **Phase 4: ADBC Integration**
   - Update `Statement::execute()` to use CloudFetch
   - Update `Connection` to create clients
   - Update `Database` for CloudFetch configuration options

5. **Phase 5: Testing & Polish**
   - Integration tests with mock SEA server
   - End-to-end tests against live Databricks
   - Performance tuning

---

## Key Design Decisions

### 1. Async Runtime Strategy
- Use `tokio` runtime internally
- Block on async in ADBC trait methods (ADBC is sync)
- Keep `StreamingCloudFetchProvider` fully async

### 2. Memory Management
- Limit concurrent chunks via semaphore
- Release chunks immediately after consumption
- No caching (streaming model)

### 3. Error Handling
- Retry transient failures (network, timeouts)
- Refetch expired links automatically
- Propagate fatal errors to consumer

### 4. Concurrency Control
- `DashMap` for lock-free concurrent access
- Atomic counters for position tracking
- Semaphore for download parallelism

### 5. Cancellation
- `CancellationToken` for clean shutdown
- Cancel background tasks on `Statement::cancel()`
- Close statement on drop

---

## References

- [ADBC Core Rust Documentation](https://docs.rs/adbc_core/)
- [Apache Arrow IPC Format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
- [Databricks SQL Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution)
- Databricks JDBC Driver (internal reference for CloudFetch implementation patterns)
