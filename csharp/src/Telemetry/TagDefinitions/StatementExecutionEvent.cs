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

using System.Collections.Generic;

namespace AdbcDrivers.Databricks.Telemetry.TagDefinitions
{
    /// <summary>
    /// Tag definitions for Statement execution events.
    /// </summary>
    public static class StatementExecutionEvent
    {
        /// <summary>
        /// The event name for statement execution operations.
        /// </summary>
        public const string EventName = "Statement.Execute";

        /// <summary>
        /// Statement execution ID.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("statement.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Statement execution ID",
            Required = true)]
        public const string StatementId = "statement.id";

        /// <summary>
        /// Connection session ID.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("session.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Connection session ID",
            Required = true)]
        public const string SessionId = "session.id";

        /// <summary>
        /// Result format (inline or cloudfetch).
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("result.format",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Result format: inline, cloudfetch")]
        public const string ResultFormat = "result.format";

        /// <summary>
        /// Number of CloudFetch chunks.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("result.chunk_count",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Number of CloudFetch chunks")]
        public const string ResultChunkCount = "result.chunk_count";

        /// <summary>
        /// Total bytes downloaded.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("result.bytes_downloaded",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Total bytes downloaded")]
        public const string ResultBytesDownloaded = "result.bytes_downloaded";

        /// <summary>
        /// Whether compression is enabled for results.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("result.compression_enabled",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Compression enabled for results")]
        public const string ResultCompressionEnabled = "result.compression_enabled";

        /// <summary>
        /// Number of status poll requests.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("poll.count",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Number of status poll requests")]
        public const string PollCount = "poll.count";

        /// <summary>
        /// Total polling latency in milliseconds.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("poll.latency_ms",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Total polling latency")]
        public const string PollLatencyMs = "poll.latency_ms";

        /// <summary>
        /// Latency of first chunk download in milliseconds.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("chunk.initial_latency_ms",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Latency of first chunk download in milliseconds")]
        public const string ChunkInitialLatencyMs = "chunk.initial_latency_ms";

        /// <summary>
        /// Latency of slowest chunk download in milliseconds.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("chunk.slowest_latency_ms",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Latency of slowest chunk download in milliseconds")]
        public const string ChunkSlowestLatencyMs = "chunk.slowest_latency_ms";

        /// <summary>
        /// SQL query text.
        /// Only exported to local diagnostics (sensitive data).
        /// </summary>
        [TelemetryTag("db.statement",
            ExportScope = TagExportScope.ExportLocal,
            Description = "SQL query text (local diagnostics only)")]
        public const string DbStatement = "db.statement";

        /// <summary>
        /// Gets all tags that should be exported to Databricks telemetry service.
        /// </summary>
        /// <returns>A set of tag names for Databricks export.</returns>
        public static HashSet<string> GetDatabricksExportTags()
        {
            return new HashSet<string>
            {
                StatementId,
                SessionId,
                ResultFormat,
                ResultChunkCount,
                ResultBytesDownloaded,
                ResultCompressionEnabled,
                PollCount,
                PollLatencyMs,
                ChunkInitialLatencyMs,
                ChunkSlowestLatencyMs
            };
        }
    }
}
