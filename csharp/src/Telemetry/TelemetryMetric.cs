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
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Type of telemetry metric.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public enum MetricType
    {
        /// <summary>
        /// Connection-level metric.
        /// </summary>
        Connection,

        /// <summary>
        /// Statement-level metric.
        /// </summary>
        Statement,

        /// <summary>
        /// Error metric.
        /// </summary>
        Error
    }

    /// <summary>
    /// Data model for aggregated telemetry metrics passed between the MetricsAggregator
    /// and the exporter pipeline.
    /// </summary>
    /// <remarks>
    /// This class represents aggregated telemetry data collected from multiple activities
    /// with the same statement_id. It includes connection metadata, statement execution
    /// details, result format information, and driver configuration snapshots.
    ///
    /// JSON serialization uses snake_case property names to match the Databricks schema.
    /// Null fields are omitted from serialization.
    /// </remarks>
    public sealed class TelemetryMetric
    {
        /// <summary>
        /// Type of metric (Connection, Statement, or Error).
        /// </summary>
        [JsonPropertyName("metric_type")]
        public MetricType MetricType { get; set; }

        /// <summary>
        /// Timestamp when the metric was recorded.
        /// </summary>
        [JsonPropertyName("timestamp")]
        public DateTimeOffset Timestamp { get; set; }

        /// <summary>
        /// Databricks workspace ID.
        /// </summary>
        [JsonPropertyName("workspace_id")]
        public long WorkspaceId { get; set; }

        /// <summary>
        /// Connection-level session ID (shared across all statements in a connection).
        /// </summary>
        [JsonPropertyName("session_id")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? SessionId { get; set; }

        /// <summary>
        /// Statement-level ID (unique per statement, used as aggregation key).
        /// </summary>
        [JsonPropertyName("statement_id")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? StatementId { get; set; }

        /// <summary>
        /// Total execution latency in milliseconds.
        /// </summary>
        [JsonPropertyName("execution_latency_ms")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public long? ExecutionLatencyMs { get; set; }

        /// <summary>
        /// Result format (e.g., "ARROW", "CLOUDFETCH", "INLINE").
        /// </summary>
        [JsonPropertyName("result_format")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? ResultFormat { get; set; }

        /// <summary>
        /// Number of chunks downloaded (for CloudFetch results).
        /// </summary>
        [JsonPropertyName("chunk_count")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? ChunkCount { get; set; }

        /// <summary>
        /// Total bytes downloaded across all chunks.
        /// </summary>
        [JsonPropertyName("total_bytes_downloaded")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public long? TotalBytesDownloaded { get; set; }

        /// <summary>
        /// Number of polling operations performed.
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
        /// Driver configuration snapshot at the time of metric collection.
        /// </summary>
        [JsonPropertyName("driver_configuration")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public DriverConfiguration? DriverConfiguration { get; set; }
    }

    /// <summary>
    /// Snapshot of driver configuration for telemetry.
    /// </summary>
    public sealed class DriverConfiguration
    {
        /// <summary>
        /// Driver version.
        /// </summary>
        [JsonPropertyName("driver_version")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? DriverVersion { get; set; }

        /// <summary>
        /// Driver name.
        /// </summary>
        [JsonPropertyName("driver_name")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? DriverName { get; set; }

        /// <summary>
        /// Runtime name (e.g., ".NET 8.0").
        /// </summary>
        [JsonPropertyName("runtime_name")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? RuntimeName { get; set; }

        /// <summary>
        /// Runtime version.
        /// </summary>
        [JsonPropertyName("runtime_version")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? RuntimeVersion { get; set; }

        /// <summary>
        /// Operating system name.
        /// </summary>
        [JsonPropertyName("os_name")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? OsName { get; set; }

        /// <summary>
        /// Operating system version.
        /// </summary>
        [JsonPropertyName("os_version")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? OsVersion { get; set; }

        /// <summary>
        /// Operating system architecture.
        /// </summary>
        [JsonPropertyName("os_architecture")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? OsArchitecture { get; set; }

        /// <summary>
        /// HTTP path used for the connection.
        /// </summary>
        [JsonPropertyName("http_path")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? HttpPath { get; set; }

        /// <summary>
        /// Host URL.
        /// </summary>
        [JsonPropertyName("host_url")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? HostUrl { get; set; }

        /// <summary>
        /// Whether CloudFetch is enabled.
        /// </summary>
        [JsonPropertyName("cloud_fetch_enabled")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public bool? CloudFetchEnabled { get; set; }

        /// <summary>
        /// Whether direct results are enabled.
        /// </summary>
        [JsonPropertyName("direct_results_enabled")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public bool? DirectResultsEnabled { get; set; }

        /// <summary>
        /// Additional configuration properties.
        /// </summary>
        [JsonPropertyName("additional_properties")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public Dictionary<string, string>? AdditionalProperties { get; set; }
    }
}
