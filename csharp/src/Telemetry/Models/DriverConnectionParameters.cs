/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System.Text.Json.Serialization;

namespace AdbcDrivers.Databricks.Telemetry.Models
{
    /// <summary>
    /// Connection parameters and feature flags for the driver.
    /// This follows the JDBC driver format for compatibility with Databricks telemetry backend.
    /// </summary>
    public class DriverConnectionParameters
    {
        /// <summary>
        /// Whether CloudFetch is enabled for result retrieval.
        /// </summary>
        [JsonPropertyName("cloud_fetch_enabled")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public bool? CloudFetchEnabled { get; set; }

        /// <summary>
        /// Whether LZ4 compression is enabled for result decompression.
        /// </summary>
        [JsonPropertyName("lz4_compression_enabled")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public bool? Lz4CompressionEnabled { get; set; }

        /// <summary>
        /// Whether direct results mode is enabled.
        /// </summary>
        [JsonPropertyName("direct_results_enabled")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public bool? DirectResultsEnabled { get; set; }

        /// <summary>
        /// Maximum number of download threads for CloudFetch.
        /// </summary>
        [JsonPropertyName("max_download_threads")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? MaxDownloadThreads { get; set; }

        /// <summary>
        /// Authentication type used for the connection.
        /// Example: "token", "oauth", "basic"
        /// </summary>
        [JsonPropertyName("auth_type")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? AuthType { get; set; }

        /// <summary>
        /// Transport mode used for the connection.
        /// Example: "http", "https"
        /// </summary>
        [JsonPropertyName("transport_mode")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? TransportMode { get; set; }

        /// <summary>
        /// Host name of the Databricks server.
        /// </summary>
        [JsonPropertyName("host")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? Host { get; set; }

        /// <summary>
        /// Port number for the connection.
        /// </summary>
        [JsonPropertyName("port")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? Port { get; set; }

        /// <summary>
        /// HTTP path for the connection.
        /// </summary>
        [JsonPropertyName("http_path")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? HttpPath { get; set; }

        /// <summary>
        /// Connection timeout in milliseconds.
        /// </summary>
        [JsonPropertyName("connect_timeout_ms")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? ConnectTimeoutMs { get; set; }

        /// <summary>
        /// Protocol used for statement execution.
        /// Example: "thrift", "rest"
        /// </summary>
        [JsonPropertyName("protocol")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? Protocol { get; set; }

        /// <summary>
        /// Warehouse ID for REST API execution.
        /// </summary>
        [JsonPropertyName("warehouse_id")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? WarehouseId { get; set; }

        /// <summary>
        /// Number of parallel CloudFetch downloads.
        /// </summary>
        [JsonPropertyName("cloudfetch_parallel_downloads")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? CloudFetchParallelDownloads { get; set; }

        /// <summary>
        /// Number of files to prefetch for CloudFetch.
        /// </summary>
        [JsonPropertyName("cloudfetch_prefetch_count")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? CloudFetchPrefetchCount { get; set; }

        /// <summary>
        /// Memory buffer size in MB for CloudFetch.
        /// </summary>
        [JsonPropertyName("cloudfetch_memory_buffer_mb")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? CloudFetchMemoryBufferMb { get; set; }

        /// <summary>
        /// Maximum bytes per file for CloudFetch.
        /// </summary>
        [JsonPropertyName("cloudfetch_max_bytes_per_file")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public long? CloudFetchMaxBytesPerFile { get; set; }

        /// <summary>
        /// Maximum retry attempts for CloudFetch downloads.
        /// </summary>
        [JsonPropertyName("cloudfetch_max_retries")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? CloudFetchMaxRetries { get; set; }

        /// <summary>
        /// Timeout in minutes for CloudFetch operations.
        /// </summary>
        [JsonPropertyName("cloudfetch_timeout_minutes")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? CloudFetchTimeoutMinutes { get; set; }

        /// <summary>
        /// Whether temporarily unavailable retry is enabled (408, 502, 503, 504).
        /// </summary>
        [JsonPropertyName("temporarily_unavailable_retry_enabled")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public bool? TemporarilyUnavailableRetryEnabled { get; set; }

        /// <summary>
        /// Timeout in seconds for temporarily unavailable retries.
        /// </summary>
        [JsonPropertyName("temporarily_unavailable_retry_timeout_seconds")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? TemporarilyUnavailableRetryTimeoutSeconds { get; set; }

        /// <summary>
        /// Whether rate limit retry is enabled (429).
        /// </summary>
        [JsonPropertyName("rate_limit_retry_enabled")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public bool? RateLimitRetryEnabled { get; set; }

        /// <summary>
        /// Timeout in seconds for rate limit retries.
        /// </summary>
        [JsonPropertyName("rate_limit_retry_timeout_seconds")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? RateLimitRetryTimeoutSeconds { get; set; }

        /// <summary>
        /// Whether multiple catalog support is enabled.
        /// </summary>
        [JsonPropertyName("multiple_catalog_support_enabled")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public bool? MultipleCatalogSupportEnabled { get; set; }

        /// <summary>
        /// Whether async execution in Thrift operations is enabled.
        /// </summary>
        [JsonPropertyName("run_async_enabled")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public bool? RunAsyncEnabled { get; set; }

        /// <summary>
        /// Heartbeat interval in seconds for long-running operations.
        /// </summary>
        [JsonPropertyName("fetch_heartbeat_interval_seconds")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? FetchHeartbeatIntervalSeconds { get; set; }

        /// <summary>
        /// User agent entry for the connection.
        /// </summary>
        [JsonPropertyName("user_agent_entry")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? UserAgentEntry { get; set; }
    }
}
