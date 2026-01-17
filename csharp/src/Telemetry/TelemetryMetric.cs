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

using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Represents an aggregated telemetry metric that will be exported to Databricks telemetry service.
    /// This class aggregates data from multiple activities with the same statement_id.
    /// Includes both session_id (connection-level) and statement_id (statement-level) for multi-level correlation.
    /// </summary>
    public sealed class TelemetryMetric
    {
        /// <summary>
        /// Gets or sets the type of metric: "connection", "statement", or "error".
        /// </summary>
        [JsonPropertyName("metric_type")]
        public string? MetricType { get; set; }

        /// <summary>
        /// Gets or sets the timestamp of when the metric was recorded.
        /// Derived from Activity.StartTimeUtc.
        /// </summary>
        [JsonPropertyName("timestamp")]
        public DateTimeOffset Timestamp { get; set; }

        /// <summary>
        /// Gets or sets the Databricks workspace ID.
        /// </summary>
        [JsonPropertyName("workspace_id")]
        public long? WorkspaceId { get; set; }

        /// <summary>
        /// Gets or sets the session ID (connection-level identifier).
        /// All statements within a connection share the same session_id for correlation.
        /// </summary>
        [JsonPropertyName("session_id")]
        public string? SessionId { get; set; }

        /// <summary>
        /// Gets or sets the statement ID (statement-level identifier).
        /// Unique per statement and used as the aggregation key.
        /// </summary>
        [JsonPropertyName("statement_id")]
        public string? StatementId { get; set; }

        /// <summary>
        /// Gets or sets the execution latency in milliseconds.
        /// Derived from Activity.Duration.TotalMilliseconds.
        /// </summary>
        [JsonPropertyName("execution_latency_ms")]
        public long? ExecutionLatencyMs { get; set; }

        /// <summary>
        /// Gets or sets the result format: "inline" or "cloudfetch".
        /// </summary>
        [JsonPropertyName("result_format")]
        public string? ResultFormat { get; set; }

        /// <summary>
        /// Gets or sets the number of CloudFetch chunks downloaded.
        /// </summary>
        [JsonPropertyName("chunk_count")]
        public int? ChunkCount { get; set; }

        /// <summary>
        /// Gets or sets the total bytes downloaded from CloudFetch.
        /// </summary>
        [JsonPropertyName("total_bytes_downloaded")]
        public long? TotalBytesDownloaded { get; set; }

        /// <summary>
        /// Gets or sets the number of status poll requests made.
        /// </summary>
        [JsonPropertyName("poll_count")]
        public int? PollCount { get; set; }

        /// <summary>
        /// Gets or sets the driver configuration information.
        /// Includes driver version, OS, runtime, and feature flags.
        /// </summary>
        [JsonPropertyName("driver_configuration")]
        public DriverConfiguration? DriverConfiguration { get; set; }

        /// <summary>
        /// Serializes the telemetry metric to JSON string with null fields omitted.
        /// </summary>
        /// <returns>JSON string representation of the metric.</returns>
        public string ToJson()
        {
            var options = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false
            };

            return JsonSerializer.Serialize(this, options);
        }

        /// <summary>
        /// Deserializes a JSON string to a TelemetryMetric instance.
        /// </summary>
        /// <param name="json">JSON string to deserialize.</param>
        /// <returns>TelemetryMetric instance.</returns>
        public static TelemetryMetric FromJson(string json)
        {
            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };

            return JsonSerializer.Deserialize<TelemetryMetric>(json, options)
                ?? throw new InvalidOperationException("Failed to deserialize TelemetryMetric from JSON");
        }
    }

    /// <summary>
    /// Represents driver configuration information collected at connection time.
    /// </summary>
    public sealed class DriverConfiguration
    {
        /// <summary>
        /// Gets or sets the driver version string.
        /// </summary>
        [JsonPropertyName("driver_version")]
        public string? DriverVersion { get; set; }

        /// <summary>
        /// Gets or sets the operating system.
        /// </summary>
        [JsonPropertyName("driver_os")]
        public string? DriverOS { get; set; }

        /// <summary>
        /// Gets or sets the .NET runtime version.
        /// </summary>
        [JsonPropertyName("driver_runtime")]
        public string? DriverRuntime { get; set; }

        /// <summary>
        /// Gets or sets whether CloudFetch is enabled.
        /// </summary>
        [JsonPropertyName("feature_cloudfetch")]
        public bool? FeatureCloudFetch { get; set; }

        /// <summary>
        /// Gets or sets whether LZ4 compression is enabled.
        /// </summary>
        [JsonPropertyName("feature_lz4")]
        public bool? FeatureLz4 { get; set; }
    }
}
