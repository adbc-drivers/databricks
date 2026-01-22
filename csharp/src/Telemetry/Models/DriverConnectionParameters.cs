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
    }
}
