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
    /// Frontend log event structure for Databricks telemetry.
    /// This follows the JDBC driver format for compatibility with Databricks telemetry backend.
    /// </summary>
    /// <remarks>
    /// Each TelemetryFrontendLog represents a single telemetry event and contains:
    /// - workspace_id: The Databricks workspace ID
    /// - frontend_log_event_id: Unique identifier for the log event
    /// - context: Additional context information
    /// - entry: The actual log entry containing the SQL driver telemetry event
    /// </remarks>
    public class TelemetryFrontendLog
    {
        /// <summary>
        /// The Databricks workspace ID.
        /// </summary>
        [JsonPropertyName("workspace_id")]
        public long WorkspaceId { get; set; }

        /// <summary>
        /// Unique identifier for this frontend log event.
        /// Typically a UUID string.
        /// </summary>
        [JsonPropertyName("frontend_log_event_id")]
        public string FrontendLogEventId { get; set; } = string.Empty;

        /// <summary>
        /// Context information for the log event.
        /// Contains client context such as user agent and timestamp.
        /// </summary>
        [JsonPropertyName("context")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public FrontendLogContext? Context { get; set; }

        /// <summary>
        /// The log entry containing the SQL driver telemetry event.
        /// </summary>
        [JsonPropertyName("entry")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public FrontendLogEntry? Entry { get; set; }
    }
}
