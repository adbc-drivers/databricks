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
    /// Defines all Activity tags used during SQL statement execution.
    /// </summary>
    internal static class StatementExecutionEvent
    {
        /// <summary>
        /// The event name for statement execution activities.
        /// </summary>
        public const string EventName = "Statement.Execute";

        // ============================================================================
        // Statement identification tags
        // ============================================================================

        /// <summary>
        /// Statement execution ID. Required for statement-level correlation.
        /// </summary>
        [TelemetryTag("statement.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Statement execution ID",
            Required = true)]
        public const string StatementId = "statement.id";

        /// <summary>
        /// Connection session ID. Required for connection-level correlation.
        /// </summary>
        [TelemetryTag("session.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Connection session ID",
            Required = true)]
        public const string SessionId = "session.id";

        /// <summary>
        /// Workspace ID for the connection.
        /// </summary>
        [TelemetryTag("workspace.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Databricks workspace ID")]
        public const string WorkspaceId = "workspace.id";

        // ============================================================================
        // Statement type and operation tags
        // ============================================================================

        /// <summary>
        /// Type of statement (QUERY, UPDATE, etc.).
        /// </summary>
        [TelemetryTag("statement.type",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Statement type (QUERY, UPDATE, etc.)")]
        public const string StatementType = "statement.type";

        // ============================================================================
        // Result format tags
        // ============================================================================

        /// <summary>
        /// Result format: inline or cloudfetch.
        /// </summary>
        [TelemetryTag("result.format",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Result format: inline, cloudfetch")]
        public const string ResultFormat = "result.format";

        /// <summary>
        /// Number of CloudFetch chunks downloaded.
        /// </summary>
        [TelemetryTag("result.chunk_count",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Number of CloudFetch chunks")]
        public const string ResultChunkCount = "result.chunk_count";

        /// <summary>
        /// Total bytes downloaded from results.
        /// </summary>
        [TelemetryTag("result.bytes_downloaded",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Total bytes downloaded")]
        public const string ResultBytesDownloaded = "result.bytes_downloaded";

        /// <summary>
        /// Whether compression was enabled for results.
        /// </summary>
        [TelemetryTag("result.compression_enabled",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Compression enabled for results")]
        public const string ResultCompressionEnabled = "result.compression_enabled";

        /// <summary>
        /// Total number of rows returned.
        /// </summary>
        [TelemetryTag("result.row_count",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Total number of rows returned")]
        public const string ResultRowCount = "result.row_count";

        // ============================================================================
        // Polling metrics tags
        // ============================================================================

        /// <summary>
        /// Number of status poll requests made.
        /// </summary>
        [TelemetryTag("poll.count",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Number of status poll requests")]
        public const string PollCount = "poll.count";

        /// <summary>
        /// Total polling latency in milliseconds.
        /// </summary>
        [TelemetryTag("poll.latency_ms",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Total polling latency")]
        public const string PollLatencyMs = "poll.latency_ms";

        // ============================================================================
        // Retry metrics tags
        // ============================================================================

        /// <summary>
        /// Number of retry attempts made.
        /// </summary>
        [TelemetryTag("retry.count",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Number of retry attempts")]
        public const string RetryCount = "retry.count";

        // ============================================================================
        // Latency tags
        // ============================================================================

        /// <summary>
        /// Total operation latency in milliseconds.
        /// </summary>
        [TelemetryTag("operation.latency_ms",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Total operation latency in milliseconds")]
        public const string OperationLatencyMs = "operation.latency_ms";

        // ============================================================================
        // Sensitive tags - NOT exported to Databricks
        // ============================================================================

        /// <summary>
        /// SQL query text. Local diagnostics only.
        /// </summary>
        [TelemetryTag("db.statement",
            ExportScope = TagExportScope.ExportLocal,
            Description = "SQL query text (local diagnostics only)")]
        public const string DbStatement = "db.statement";

        /// <summary>
        /// Query parameters. Local diagnostics only.
        /// </summary>
        [TelemetryTag("db.parameters",
            ExportScope = TagExportScope.ExportLocal,
            Description = "Query parameters (local diagnostics only)")]
        public const string DbParameters = "db.parameters";

        // Cached set of Databricks export tags for performance
        private static readonly HashSet<string> s_databricksExportTags = new HashSet<string>
        {
            StatementId,
            SessionId,
            WorkspaceId,
            StatementType,
            ResultFormat,
            ResultChunkCount,
            ResultBytesDownloaded,
            ResultCompressionEnabled,
            ResultRowCount,
            PollCount,
            PollLatencyMs,
            RetryCount,
            OperationLatencyMs
        };

        /// <summary>
        /// Get all tags that should be exported to Databricks for this event type.
        /// </summary>
        /// <returns>Set of tag names allowed for Databricks export.</returns>
        public static IReadOnlyCollection<string> GetDatabricksExportTags()
        {
            return s_databricksExportTags;
        }

        /// <summary>
        /// Check if a tag should be exported to Databricks.
        /// </summary>
        /// <param name="tagName">The tag name to check.</param>
        /// <returns>True if the tag should be exported to Databricks.</returns>
        public static bool ShouldExportToDatabricks(string tagName)
        {
            return s_databricksExportTags.Contains(tagName);
        }
    }
}
