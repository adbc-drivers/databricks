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
    /// SQL execution event details for telemetry.
    /// This follows the JDBC driver format for compatibility with Databricks telemetry backend.
    /// </summary>
    public class SqlExecutionEvent
    {
        /// <summary>
        /// Result format used: "inline" or "cloudfetch".
        /// </summary>
        [JsonPropertyName("result_format")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? ResultFormat { get; set; }

        /// <summary>
        /// Number of CloudFetch chunks downloaded.
        /// </summary>
        [JsonPropertyName("chunk_count")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? ChunkCount { get; set; }

        /// <summary>
        /// Total bytes downloaded from all chunks.
        /// </summary>
        [JsonPropertyName("bytes_downloaded")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public long? BytesDownloaded { get; set; }

        /// <summary>
        /// Whether compression was enabled for the results.
        /// </summary>
        [JsonPropertyName("compression_enabled")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public bool? CompressionEnabled { get; set; }

        /// <summary>
        /// Total number of rows returned.
        /// </summary>
        [JsonPropertyName("row_count")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public long? RowCount { get; set; }

        /// <summary>
        /// Number of status poll requests made.
        /// </summary>
        [JsonPropertyName("poll_count")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? PollCount { get; set; }

        /// <summary>
        /// Total polling latency in milliseconds.
        /// </summary>
        [JsonPropertyName("poll_latency_ms")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public long? PollLatencyMs { get; set; }

        /// <summary>
        /// Time to first byte in milliseconds.
        /// </summary>
        [JsonPropertyName("time_to_first_byte_ms")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public long? TimeToFirstByteMs { get; set; }

        /// <summary>
        /// Execution status: "succeeded", "failed", "cancelled".
        /// </summary>
        [JsonPropertyName("execution_status")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? ExecutionStatus { get; set; }

        /// <summary>
        /// Type of statement executed: "query", "update", "ddl", "other".
        /// </summary>
        [JsonPropertyName("statement_type")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? StatementType { get; set; }

        /// <summary>
        /// Whether retry was performed due to a transient error.
        /// </summary>
        [JsonPropertyName("retry_performed")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public bool? RetryPerformed { get; set; }

        /// <summary>
        /// Number of retry attempts made.
        /// </summary>
        [JsonPropertyName("retry_count")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? RetryCount { get; set; }
    }
}
