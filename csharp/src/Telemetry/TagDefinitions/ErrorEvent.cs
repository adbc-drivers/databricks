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
    /// </summary>
    internal static class ErrorEvent
    {
        /// <summary>
        /// Event name for error operations.
        /// </summary>
        public const string EventName = "Error";

        // Error identification
        [TelemetryTag("error.type",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Error type/category",
            Required = true)]
        public const string ErrorType = "error.type";

        [TelemetryTag("session.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Connection session ID")]
        public const string SessionId = "session.id";

        [TelemetryTag("statement.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Statement execution ID (if applicable)")]
        public const string StatementId = "statement.id";

        // Error details
        [TelemetryTag("error.code",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Error code")]
        public const string ErrorCode = "error.code";

        [TelemetryTag("error.category",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Error category (e.g., network, auth, syntax)")]
        public const string ErrorCategory = "error.category";

        // HTTP-specific errors
        [TelemetryTag("http.status_code",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "HTTP status code (if applicable)")]
        public const string HttpStatusCode = "http.status_code";

        // Sensitive tags - NOT exported to Databricks
        [TelemetryTag("error.message",
            ExportScope = TagExportScope.ExportLocal,
            Description = "Error message (local diagnostics only)")]
        public const string ErrorMessage = "error.message";

        [TelemetryTag("error.stack_trace",
            ExportScope = TagExportScope.ExportLocal,
            Description = "Error stack trace (local diagnostics only)")]
        public const string ErrorStackTrace = "error.stack_trace";

        /// <summary>
        /// Gets all tags that should be exported to Databricks.
        /// </summary>
        /// <returns>A read-only collection of tag names that should be exported to Databricks.</returns>
        public static IReadOnlyCollection<string> GetDatabricksExportTags()
        {
            return new HashSet<string>
            {
                ErrorType,
                SessionId,
                StatementId,
                ErrorCode,
                ErrorCategory,
                HttpStatusCode
            };
        }
    }
}
