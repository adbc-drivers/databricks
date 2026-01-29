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
    public static class ErrorEvent
    {
        /// <summary>
        /// The event name for error events.
        /// </summary>
        public const string EventName = "Error";

        /// <summary>
        /// Error type or exception type name.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("error.type",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Error type or exception type",
            Required = true)]
        public const string ErrorType = "error.type";

        /// <summary>
        /// Error message (sanitized, no PII).
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("error.message",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Error message (sanitized)")]
        public const string ErrorMessage = "error.message";

        /// <summary>
        /// HTTP status code if applicable.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("error.http_status",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "HTTP status code")]
        public const string ErrorHttpStatus = "error.http_status";

        /// <summary>
        /// Statement ID associated with the error.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("statement.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Statement ID associated with error")]
        public const string StatementId = "statement.id";

        /// <summary>
        /// Session ID associated with the error.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("session.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Session ID associated with error")]
        public const string SessionId = "session.id";

        /// <summary>
        /// Full stack trace.
        /// Only exported to local diagnostics (may contain sensitive paths).
        /// </summary>
        [TelemetryTag("error.stack_trace",
            ExportScope = TagExportScope.ExportLocal,
            Description = "Full stack trace (local diagnostics only)")]
        public const string ErrorStackTrace = "error.stack_trace";

        /// <summary>
        /// Gets all tags that should be exported to Databricks telemetry service.
        /// </summary>
        /// <returns>A set of tag names for Databricks export.</returns>
        public static HashSet<string> GetDatabricksExportTags()
        {
            return new HashSet<string>
            {
                ErrorType,
                ErrorMessage,
                ErrorHttpStatus,
                StatementId,
                SessionId
            };
        }
    }
}
