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

//! ChunkDownloader for downloading Arrow data from cloud storage.
//!
//! This module handles downloading chunk data from presigned URLs,
//! including HTTP header handling, speed monitoring, decompression,
//! and Arrow IPC parsing.

use crate::client::DatabricksHttpClient;
use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::arrow_parser::parse_arrow_ipc;
use crate::types::cloudfetch::CloudFetchLink;
use crate::types::sea::CompressionCodec;
use arrow_array::RecordBatch;
use driverbase::error::ErrorHelper;
use reqwest::Method;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn};

/// Downloads Arrow data from cloud storage presigned URLs.
///
/// Handles:
/// - HTTP GET to presigned URL with custom headers
/// - LZ4 decompression (if compression enabled)
/// - Arrow IPC stream parsing into RecordBatches
/// - Download speed monitoring
#[derive(Debug)]
pub struct ChunkDownloader {
    http_client: Arc<DatabricksHttpClient>,
    compression: CompressionCodec,
    speed_threshold_mbps: f64,
}

impl ChunkDownloader {
    /// Create a new chunk downloader.
    ///
    /// # Arguments
    /// * `http_client` - HTTP client for making requests
    /// * `compression` - Compression codec used by the server
    /// * `speed_threshold_mbps` - Log warning if speed falls below this (MB/s)
    pub fn new(
        http_client: Arc<DatabricksHttpClient>,
        compression: CompressionCodec,
        speed_threshold_mbps: f64,
    ) -> Self {
        Self {
            http_client,
            compression,
            speed_threshold_mbps,
        }
    }

    /// Download a chunk from the given link.
    ///
    /// # Arguments
    /// * `link` - CloudFetch link containing URL and headers
    ///
    /// # Returns
    /// Parsed RecordBatches from the downloaded Arrow IPC data
    pub async fn download(&self, link: &CloudFetchLink) -> Result<Vec<RecordBatch>> {
        let start = Instant::now();

        debug!(
            "Downloading chunk {} from {} ({} bytes expected)",
            link.chunk_index, link.url, link.byte_count
        );

        // Build request with custom headers from the link
        let mut request_builder = self.http_client.inner().request(Method::GET, &link.url);

        for (key, value) in &link.http_headers {
            request_builder = request_builder.header(key, value);
        }

        let request = request_builder.build().map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to build download request: {}", e))
        })?;

        // Execute without auth (presigned URLs have their own auth)
        let response = self.http_client.execute_without_auth(request).await?;

        // Read response body
        let bytes = response.bytes().await.map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to read download response: {}", e))
        })?;

        let elapsed = start.elapsed();
        let size_mb = bytes.len() as f64 / 1024.0 / 1024.0;
        let speed_mbps = size_mb / elapsed.as_secs_f64();

        debug!(
            "Downloaded chunk {}: {:.2} MB in {:.2}s ({:.2} MB/s)",
            link.chunk_index,
            size_mb,
            elapsed.as_secs_f64(),
            speed_mbps
        );

        if speed_mbps < self.speed_threshold_mbps {
            warn!(
                "CloudFetch download slower than threshold: {:.2} MB/s (threshold: {:.2} MB/s)",
                speed_mbps, self.speed_threshold_mbps
            );
        }

        // Parse Arrow IPC data
        let batches = parse_arrow_ipc(&bytes, self.compression)?;

        debug!(
            "Parsed chunk {}: {} batches, {} total rows",
            link.chunk_index,
            batches.len(),
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        );

        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::PersonalAccessToken;
    use crate::client::HttpClientConfig;
    use std::collections::HashMap;

    fn create_test_link() -> CloudFetchLink {
        CloudFetchLink {
            url: "https://storage.example.com/chunk0".to_string(),
            chunk_index: 0,
            row_offset: 0,
            row_count: 1000,
            byte_count: 50000,
            expiration: chrono::Utc::now() + chrono::Duration::hours(1),
            http_headers: HashMap::from([("x-custom".to_string(), "value".to_string())]),
            next_chunk_index: Some(1),
        }
    }

    #[test]
    fn test_chunk_downloader_creation() {
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
        let http_client =
            Arc::new(DatabricksHttpClient::new(HttpClientConfig::default(), auth).unwrap());
        let downloader = ChunkDownloader::new(http_client, CompressionCodec::None, 0.1);

        assert_eq!(downloader.speed_threshold_mbps, 0.1);
    }

    // Note: Full download tests require mocking the HTTP layer
    // Integration tests should be added in a separate test module
}
