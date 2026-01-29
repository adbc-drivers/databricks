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
    /// Error information for telemetry events.
    /// This follows the JDBC driver format for compatibility with Databricks telemetry backend.
    /// </summary>
    public class DriverErrorInfo
    {
        /// <summary>
        /// Type or category of the error.
        /// Example: "AuthenticationError", "ConnectionError", "SqlError"
        /// </summary>
        [JsonPropertyName("error_type")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? ErrorType { get; set; }

        /// <summary>
        /// Error message (sanitized to remove PII).
        /// </summary>
        [JsonPropertyName("error_message")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? ErrorMessage { get; set; }

        /// <summary>
        /// Error code if available.
        /// </summary>
        [JsonPropertyName("error_code")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? ErrorCode { get; set; }

        /// <summary>
        /// SQL state code if applicable.
        /// </summary>
        [JsonPropertyName("sql_state")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? SqlState { get; set; }

        /// <summary>
        /// HTTP status code if the error was from an HTTP response.
        /// </summary>
        [JsonPropertyName("http_status_code")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? HttpStatusCode { get; set; }

        /// <summary>
        /// Whether the error is considered terminal (non-retryable).
        /// </summary>
        [JsonPropertyName("is_terminal")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public bool? IsTerminal { get; set; }

        /// <summary>
        /// Whether a retry was attempted after this error.
        /// </summary>
        [JsonPropertyName("retry_attempted")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public bool? RetryAttempted { get; set; }
    }
}
