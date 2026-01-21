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
    /// Tag definitions for Error events.
    /// Defines all Activity tags used when errors occur.
    /// </summary>
    internal static class ErrorEvent
    {
        /// <summary>
        /// The event name for error activities.
        /// </summary>
        public const string EventName = "Error";

        // ============================================================================
        // Identification tags
        // ============================================================================

        /// <summary>
        /// Connection session ID. Required for correlation.
        /// </summary>
        [TelemetryTag("session.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Connection session ID",
            Required = true)]
        public const string SessionId = "session.id";

        /// <summary>
        /// Statement ID if error occurred during statement execution.
        /// </summary>
        [TelemetryTag("statement.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Statement execution ID")]
        public const string StatementId = "statement.id";

        /// <summary>
        /// Workspace ID for the connection.
        /// </summary>
        [TelemetryTag("workspace.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Databricks workspace ID")]
        public const string WorkspaceId = "workspace.id";

        // ============================================================================
        // Error identification tags
        // ============================================================================

        /// <summary>
        /// Error type/class name.
        /// </summary>
        [TelemetryTag("error.type",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Error type/exception class name",
            Required = true)]
        public const string ErrorType = "error.type";

        /// <summary>
        /// Error name (short identifier).
        /// </summary>
        [TelemetryTag("error.name",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Error name")]
        public const string ErrorName = "error.name";

        /// <summary>
        /// Error code if applicable.
        /// </summary>
        [TelemetryTag("error.code",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Error code")]
        public const string ErrorCode = "error.code";

        /// <summary>
        /// HTTP status code if error is HTTP-related.
        /// </summary>
        [TelemetryTag("error.http_status",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "HTTP status code")]
        public const string ErrorHttpStatus = "error.http_status";

        /// <summary>
        /// SQL state if error is SQL-related.
        /// </summary>
        [TelemetryTag("error.sql_state",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "SQL state code")]
        public const string ErrorSqlState = "error.sql_state";

        /// <summary>
        /// Whether the error is terminal (non-retryable).
        /// </summary>
        [TelemetryTag("error.terminal",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Whether error is terminal (non-retryable)")]
        public const string ErrorTerminal = "error.terminal";

        // ============================================================================
        // Sensitive tags - NOT exported to Databricks
        // ============================================================================

        /// <summary>
        /// Detailed error message. Local diagnostics only.
        /// May contain sensitive information from queries or data.
        /// </summary>
        [TelemetryTag("error.message",
            ExportScope = TagExportScope.ExportLocal,
            Description = "Detailed error message (local diagnostics only)")]
        public const string ErrorMessage = "error.message";

        /// <summary>
        /// Stack trace. Local diagnostics only.
        /// May contain sensitive paths or data.
        /// </summary>
        [TelemetryTag("error.stack_trace",
            ExportScope = TagExportScope.ExportLocal,
            Description = "Stack trace (local diagnostics only)")]
        public const string ErrorStackTrace = "error.stack_trace";

        // Cached set of Databricks export tags for performance
        private static readonly HashSet<string> s_databricksExportTags = new HashSet<string>
        {
            SessionId,
            StatementId,
            WorkspaceId,
            ErrorType,
            ErrorName,
            ErrorCode,
            ErrorHttpStatus,
            ErrorSqlState,
            ErrorTerminal
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
