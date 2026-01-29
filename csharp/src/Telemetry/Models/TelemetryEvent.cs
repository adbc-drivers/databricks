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
    /// Telemetry event for SQL driver operations.
    /// This follows the JDBC driver format for compatibility with Databricks telemetry backend.
    /// </summary>
    /// <remarks>
    /// Contains:
    /// - session_id: Connection session identifier
    /// - sql_statement_id: Unique identifier for the SQL statement
    /// - system_configuration: Driver system configuration details
    /// - operation_latency_ms: Operation latency in milliseconds
    /// - sql_execution_event: Details about SQL execution (optional)
    /// - error_info: Error information if an error occurred (optional)
    /// - connection_parameters: Connection configuration parameters (optional)
    /// </remarks>
    public class TelemetryEvent
    {
        /// <summary>
        /// Connection session identifier.
        /// This ID is shared across all statements in a connection.
        /// </summary>
        [JsonPropertyName("session_id")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? SessionId { get; set; }

        /// <summary>
        /// Unique identifier for the SQL statement.
        /// Used as aggregation key for statement-level telemetry.
        /// </summary>
        [JsonPropertyName("sql_statement_id")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? SqlStatementId { get; set; }

        /// <summary>
        /// Driver system configuration details.
        /// Contains information about the driver version, OS, and runtime.
        /// </summary>
        [JsonPropertyName("system_configuration")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public DriverSystemConfiguration? SystemConfiguration { get; set; }

        /// <summary>
        /// Operation latency in milliseconds.
        /// </summary>
        [JsonPropertyName("operation_latency_ms")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public long OperationLatencyMs { get; set; }

        /// <summary>
        /// SQL execution event details.
        /// Contains information about the execution result format, chunks, etc.
        /// </summary>
        [JsonPropertyName("sql_execution_event")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public SqlExecutionEvent? SqlExecutionEvent { get; set; }

        /// <summary>
        /// Error information if an error occurred during the operation.
        /// </summary>
        [JsonPropertyName("error_info")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public DriverErrorInfo? ErrorInfo { get; set; }

        /// <summary>
        /// Connection configuration parameters.
        /// Contains information about feature flags and connection settings.
        /// </summary>
        [JsonPropertyName("connection_parameters")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public DriverConnectionParameters? ConnectionParameters { get; set; }
    }
}
