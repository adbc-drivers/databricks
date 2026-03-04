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
//!
//! The [`DownloadError`] enum classifies HTTP failures so that download workers
//! can distinguish authentication/authorization errors (401/403/404) from
//! transient network or server errors and apply the correct retry strategy.

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

/// Classifies download errors for the retry logic in download workers.
///
/// Workers use this to decide whether to refetch the URL (auth errors)
/// or sleep-and-retry (transient errors).
#[derive(Debug)]
pub enum DownloadError {
    /// The presigned URL was rejected (401, 403, or 404).
    /// The worker should call `refetch_link()` and retry immediately without sleeping.
    AuthError { status_code: u16, message: String },
    /// A transient or unexpected error (network failure, 5xx, etc.).
    /// The worker should sleep with linear backoff and retry.
    TransientError { message: String },
}

impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadError::AuthError {
                status_code,
                message,
            } => {
                write!(f, "Auth error (HTTP {}): {}", status_code, message)
            }
            DownloadError::TransientError { message } => {
                write!(f, "Transient error: {}", message)
            }
        }
    }
}

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
    /// This is the convenience method that converts [`DownloadError`] into a
    /// generic driver error. For the classified variant used by download workers,
    /// see [`download_with_status`](Self::download_with_status).
    pub async fn download(&self, link: &CloudFetchLink) -> Result<Vec<RecordBatch>> {
        match self.download_with_status(link).await {
            Ok(batches) => Ok(batches),
            Err(DownloadError::AuthError { message, .. }) => {
                Err(DatabricksErrorHelper::io().message(message))
            }
            Err(DownloadError::TransientError { message }) => {
                Err(DatabricksErrorHelper::io().message(message))
            }
        }
    }

    /// Download a chunk, returning a classified [`DownloadError`] on failure.
    ///
    /// This is the primary entry point used by download workers. The error
    /// classification lets workers decide:
    /// - [`DownloadError::AuthError`] → refetch the presigned URL, retry immediately
    /// - [`DownloadError::TransientError`] → sleep with linear backoff, retry
    pub async fn download_with_status(
        &self,
        link: &CloudFetchLink,
    ) -> std::result::Result<Vec<RecordBatch>, DownloadError> {
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

        let request = request_builder
            .build()
            .map_err(|e| DownloadError::TransientError {
                message: format!("Failed to build download request: {}", e),
            })?;

        // Execute using the inner reqwest client directly to avoid the HTTP
        // client's own retry logic — download workers manage retries themselves.
        let response = self
            .http_client
            .inner()
            .execute(request)
            .await
            .map_err(|e| DownloadError::TransientError {
                message: format!("HTTP request failed: {}", e),
            })?;

        // Check for HTTP errors before reading the body
        let status = response.status();
        if !status.is_success() {
            let status_code = status.as_u16();
            let body = response.text().await.unwrap_or_default();
            return if Self::is_auth_error(status_code) {
                Err(DownloadError::AuthError {
                    status_code,
                    message: format!("HTTP {} - {}", status_code, body),
                })
            } else {
                Err(DownloadError::TransientError {
                    message: format!("HTTP {} - {}", status_code, body),
                })
            };
        }

        // Read response body
        let bytes = response
            .bytes()
            .await
            .map_err(|e| DownloadError::TransientError {
                message: format!("Failed to read download response: {}", e),
            })?;

        let elapsed = start.elapsed();
        let size_mb = bytes.len() as f64 / 1024.0 / 1024.0;
        let speed_mbps = if elapsed.as_secs_f64() > 0.0 {
            size_mb / elapsed.as_secs_f64()
        } else {
            0.0
        };

        debug!(
            "Downloaded chunk {}: {:.2} MB in {:.2}s ({:.2} MB/s)",
            link.chunk_index,
            size_mb,
            elapsed.as_secs_f64(),
            speed_mbps
        );

        if speed_mbps < self.speed_threshold_mbps && speed_mbps > 0.0 {
            warn!(
                "CloudFetch download slower than threshold: {:.2} MB/s (threshold: {:.2} MB/s)",
                speed_mbps, self.speed_threshold_mbps
            );
        }

        // Parse Arrow IPC data
        let batches = parse_arrow_ipc(&bytes, self.compression).map_err(|e| {
            DownloadError::TransientError {
                message: format!("Arrow IPC parse failed: {}", e),
            }
        })?;

        debug!(
            "Parsed chunk {}: {} batches, {} total rows",
            link.chunk_index,
            batches.len(),
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        );

        Ok(batches)
    }

    /// Returns true for HTTP status codes that indicate an expired or invalid
    /// presigned URL (the worker should refetch the link rather than sleep).
    fn is_auth_error(status_code: u16) -> bool {
        matches!(status_code, 401 | 403 | 404)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::PersonalAccessToken;
    use crate::client::HttpClientConfig;
    use std::collections::HashMap;

    #[allow(dead_code)]
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

    #[test]
    fn test_is_auth_error() {
        assert!(ChunkDownloader::is_auth_error(401));
        assert!(ChunkDownloader::is_auth_error(403));
        assert!(ChunkDownloader::is_auth_error(404));
        assert!(!ChunkDownloader::is_auth_error(500));
        assert!(!ChunkDownloader::is_auth_error(502));
        assert!(!ChunkDownloader::is_auth_error(200));
    }

    #[test]
    fn test_download_error_display() {
        let auth_err = DownloadError::AuthError {
            status_code: 401,
            message: "Unauthorized".to_string(),
        };
        assert!(format!("{}", auth_err).contains("401"));

        let transient_err = DownloadError::TransientError {
            message: "timeout".to_string(),
        };
        assert!(format!("{}", transient_err).contains("timeout"));
    }
}
