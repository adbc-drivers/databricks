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

//! CloudFetch-specific types for streaming downloads.
//!
//! These types are used by `StreamingCloudFetchProvider` and related components
//! for downloading Arrow data from cloud storage via presigned URLs.

use crate::error::{DatabricksErrorHelper, Result};
use crate::types::sea::ExternalLink;
use chrono::{DateTime, Utc};
use driverbase::error::ErrorHelper;
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for CloudFetch streaming.
#[derive(Debug, Clone)]
pub struct CloudFetchConfig {
    /// Number of chunk links to prefetch ahead of consumption.
    pub link_prefetch_window: usize,
    /// Maximum number of chunks to hold in memory (controls parallelism).
    pub max_chunks_in_memory: usize,
    /// Maximum number of retry attempts for failed downloads.
    pub max_retries: u32,
    /// Delay between retry attempts.
    pub retry_delay: Duration,
    /// Maximum number of URL refresh retries on auth/expiry errors.
    pub max_refresh_retries: u32,
    /// Number of parallel download worker tasks.
    pub num_download_workers: usize,
    /// Safety buffer (in seconds) before link expiration.
    /// Links are considered expired this many seconds before their actual expiration time
    /// to avoid race conditions during download.
    pub url_expiration_buffer_secs: u32,
    /// Log warning if download speed falls below this threshold (MB/s).
    pub speed_threshold_mbps: f64,
    /// Whether CloudFetch is enabled.
    pub enabled: bool,
}

impl Default for CloudFetchConfig {
    fn default() -> Self {
        Self {
            // Match JDBC default: prefetch 128 chunks ahead of consumer
            link_prefetch_window: 128,
            // Match JDBC default: cloudFetchThreadPoolSize = 16
            max_chunks_in_memory: 16,
            // Match C# default: MaxRetries = 3
            max_retries: 3,
            // Match C# default: RetryDelayMs = 500
            retry_delay: Duration::from_millis(500),
            // Match C# default: MaxUrlRefreshAttempts = 3
            max_refresh_retries: 3,
            // Match C# default: ParallelDownloads = 3
            num_download_workers: 3,
            // Match C# default: UrlExpirationBufferSeconds = 60
            url_expiration_buffer_secs: 60,
            speed_threshold_mbps: 0.1,
            enabled: true,
        }
    }
}

/// Parsed external link with validated expiration timestamp.
///
/// This is the internal representation of a CloudFetch link, converted from
/// the SEA API's `ExternalLink` with the expiration parsed into a proper
/// `DateTime<Utc>`.
#[derive(Debug, Clone)]
pub struct CloudFetchLink {
    /// Pre-signed URL for downloading the chunk.
    pub url: String,
    /// Index of this chunk in the result set.
    pub chunk_index: i64,
    /// Row offset of this chunk in the result set.
    pub row_offset: i64,
    /// Number of rows in this chunk.
    pub row_count: i64,
    /// Size of this chunk in bytes (compressed if applicable).
    pub byte_count: i64,
    /// When this link expires.
    pub expiration: DateTime<Utc>,
    /// Optional HTTP headers to include in the download request.
    pub http_headers: HashMap<String, String>,
    /// Index of the next chunk, if there are more.
    pub next_chunk_index: Option<i64>,
}

impl CloudFetchLink {
    /// Check if link is expired (with configurable safety buffer).
    ///
    /// Uses `buffer_secs` as a safety margin to avoid race conditions
    /// where a link expires during download.
    pub fn is_expired(&self, buffer_secs: u32) -> bool {
        Utc::now() + chrono::Duration::seconds(buffer_secs as i64) >= self.expiration
    }

    /// Convert from SEA API response type.
    pub fn from_external_link(link: &ExternalLink) -> Result<Self> {
        let expiration = DateTime::parse_from_rfc3339(&link.expiration)
            .map_err(|e| {
                DatabricksErrorHelper::invalid_state()
                    .message(format!("Invalid expiration timestamp: {}", e))
            })?
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloudfetch_config_default() {
        let config = CloudFetchConfig::default();
        assert_eq!(config.link_prefetch_window, 128); // Matches JDBC default
        assert_eq!(config.max_chunks_in_memory, 16); // Matches JDBC cloudFetchThreadPoolSize
        assert_eq!(config.max_retries, 3); // Matches C# MaxRetries
        assert_eq!(config.retry_delay, Duration::from_millis(500)); // Matches C# RetryDelayMs
        assert_eq!(config.max_refresh_retries, 3); // Matches C# MaxUrlRefreshAttempts
        assert_eq!(config.num_download_workers, 3); // Matches C# ParallelDownloads
        assert_eq!(config.url_expiration_buffer_secs, 60); // Matches C# UrlExpirationBufferSeconds
        assert!(config.enabled);
    }

    #[test]
    fn test_cloudfetch_link_from_external_link() {
        let external = ExternalLink {
            external_link: "https://storage.example.com/chunk0".to_string(),
            expiration: "2099-01-01T12:00:00Z".to_string(),
            chunk_index: 0,
            row_offset: 0,
            row_count: 1000,
            byte_count: 50000,
            http_headers: Some(HashMap::from([(
                "x-custom".to_string(),
                "value".to_string(),
            )])),
            next_chunk_index: Some(1),
        };

        let link = CloudFetchLink::from_external_link(&external).unwrap();
        assert_eq!(link.chunk_index, 0);
        assert_eq!(link.next_chunk_index, Some(1));
        assert_eq!(
            link.http_headers.get("x-custom"),
            Some(&"value".to_string())
        );
    }

    #[test]
    fn test_cloudfetch_link_invalid_expiration() {
        let external = ExternalLink {
            external_link: "https://storage.example.com/chunk0".to_string(),
            expiration: "not-a-valid-timestamp".to_string(),
            chunk_index: 0,
            row_offset: 0,
            row_count: 1000,
            byte_count: 50000,
            http_headers: None,
            next_chunk_index: None,
        };

        let result = CloudFetchLink::from_external_link(&external);
        assert!(result.is_err());
    }

    #[test]
    fn test_cloudfetch_link_is_expired() {
        let external = ExternalLink {
            external_link: "https://storage.example.com/chunk0".to_string(),
            expiration: "2000-01-01T12:00:00Z".to_string(), // Way in the past
            chunk_index: 0,
            row_offset: 0,
            row_count: 1000,
            byte_count: 50000,
            http_headers: None,
            next_chunk_index: None,
        };

        let link = CloudFetchLink::from_external_link(&external).unwrap();
        // Use default buffer of 60 seconds
        assert!(link.is_expired(60));
        // Also expired with zero buffer
        assert!(link.is_expired(0));
    }

    #[test]
    fn test_cloudfetch_link_not_expired() {
        let external = ExternalLink {
            external_link: "https://storage.example.com/chunk0".to_string(),
            expiration: "2099-01-01T12:00:00Z".to_string(), // Way in the future
            chunk_index: 0,
            row_offset: 0,
            row_count: 1000,
            byte_count: 50000,
            http_headers: None,
            next_chunk_index: None,
        };

        let link = CloudFetchLink::from_external_link(&external).unwrap();
        // Not expired with default buffer
        assert!(!link.is_expired(60));
        // Not expired with zero buffer either
        assert!(!link.is_expired(0));
    }

}
