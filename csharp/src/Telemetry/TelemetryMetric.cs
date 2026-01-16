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
using System.Text.Json;
using System.Text.Json.Serialization;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Top-level HTTP request wrapper sent to /telemetry-ext endpoint.
    /// This format is compatible with the Databricks JDBC driver telemetry format.
    /// </summary>
    public sealed class TelemetryRequest
    {
        /// <summary>
        /// Gets or sets the upload timestamp in Unix milliseconds.
        /// </summary>
        [JsonPropertyName("uploadTime")]
        public long UploadTime { get; set; }

        /// <summary>
        /// Gets or sets the items array. Always empty for driver telemetry.
        /// Required field for backend compatibility.
        /// </summary>
        [JsonPropertyName("items")]
        public List<string> Items { get; set; } = new();

        /// <summary>
        /// Gets or sets the protoLogs array containing JSON-serialized TelemetryFrontendLog objects.
        /// Each entry is a complete TelemetryFrontendLog serialized to a JSON string.
        /// </summary>
        [JsonPropertyName("protoLogs")]
        public List<string> ProtoLogs { get; set; } = new();

        /// <summary>
        /// Creates a TelemetryRequest from a collection of TelemetryFrontendLog events.
        /// </summary>
        public static TelemetryRequest Create(IEnumerable<TelemetryFrontendLog> events)
        {
            var request = new TelemetryRequest
            {
                UploadTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            var options = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                WriteIndented = false
            };

            foreach (var evt in events)
            {
                request.ProtoLogs.Add(JsonSerializer.Serialize(evt, options));
            }

            return request;
        }

        /// <summary>
        /// Serializes the telemetry request to JSON string.
        /// </summary>
        public string ToJson()
        {
            var options = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                WriteIndented = false
            };
            return JsonSerializer.Serialize(this, options);
        }
    }

    /// <summary>
    /// Frontend log wrapper that contains a single telemetry event.
    /// Each instance is serialized to JSON and added to the protoLogs array.
    /// </summary>
    public sealed class TelemetryFrontendLog
    {
        /// <summary>
        /// Gets or sets the Databricks workspace ID.
        /// </summary>
        [JsonPropertyName("workspace_id")]
        public long WorkspaceId { get; set; }

        /// <summary>
        /// Gets or sets the unique event ID (UUID) for this log entry.
        /// </summary>
        [JsonPropertyName("frontend_log_event_id")]
        public string? FrontendLogEventId { get; set; }

        /// <summary>
        /// Gets or sets the log context containing client information.
        /// </summary>
        [JsonPropertyName("context")]
        public FrontendLogContext? Context { get; set; }

        /// <summary>
        /// Gets or sets the log entry containing the actual telemetry event.
        /// </summary>
        [JsonPropertyName("entry")]
        public FrontendLogEntry? Entry { get; set; }

        /// <summary>
        /// Creates a new TelemetryFrontendLog wrapping a TelemetryEvent.
        /// </summary>
        public static TelemetryFrontendLog Create(
            long workspaceId,
            TelemetryEvent telemetryEvent,
            string userAgent)
        {
            return new TelemetryFrontendLog
            {
                WorkspaceId = workspaceId,
                FrontendLogEventId = Guid.NewGuid().ToString(),
                Context = new FrontendLogContext
                {
                    ClientContext = new TelemetryClientContext
                    {
                        TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        UserAgent = userAgent
                    }
                },
                Entry = new FrontendLogEntry
                {
                    SqlDriverLog = telemetryEvent
                }
            };
        }
    }

    /// <summary>
    /// Context information for the frontend log.
    /// </summary>
    public sealed class FrontendLogContext
    {
        /// <summary>
        /// Gets or sets the client context containing timestamp and user agent.
        /// </summary>
        [JsonPropertyName("client_context")]
        public TelemetryClientContext? ClientContext { get; set; }
    }

    /// <summary>
    /// Client context containing metadata about the telemetry client.
    /// </summary>
    public sealed class TelemetryClientContext
    {
        /// <summary>
        /// Gets or sets the timestamp when this event was created, in Unix milliseconds.
        /// </summary>
        [JsonPropertyName("timestamp_millis")]
        public long TimestampMillis { get; set; }

        /// <summary>
        /// Gets or sets the user agent string identifying the driver.
        /// Example: "DatabricksAdbcDriver/1.0.0"
        /// </summary>
        [JsonPropertyName("user_agent")]
        public string? UserAgent { get; set; }
    }

    /// <summary>
    /// Entry wrapper containing the SQL driver log.
    /// </summary>
    public sealed class FrontendLogEntry
    {
        /// <summary>
        /// Gets or sets the SQL driver telemetry event.
        /// </summary>
        [JsonPropertyName("sql_driver_log")]
        public TelemetryEvent? SqlDriverLog { get; set; }
    }

    /// <summary>
    /// Core telemetry event containing driver metrics and information.
    /// This structure matches the JDBC driver's TelemetryEvent format.
    /// </summary>
    public sealed class TelemetryEvent
    {
        /// <summary>
        /// Gets or sets the session ID (connection-level identifier).
        /// All statements within a connection share the same session_id.
        /// </summary>
        [JsonPropertyName("session_id")]
        public string? SessionId { get; set; }

        /// <summary>
        /// Gets or sets the SQL statement ID (statement-level identifier).
        /// Unique per statement execution.
        /// </summary>
        [JsonPropertyName("sql_statement_id")]
        public string? SqlStatementId { get; set; }

        /// <summary>
        /// Gets or sets the system configuration including driver and OS info.
        /// </summary>
        [JsonPropertyName("system_configuration")]
        public DriverSystemConfiguration? SystemConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the connection parameters.
        /// </summary>
        [JsonPropertyName("driver_connection_params")]
        public DriverConnectionParameters? DriverConnectionParams { get; set; }

        /// <summary>
        /// Gets or sets the authentication type used.
        /// </summary>
        [JsonPropertyName("auth_type")]
        public string? AuthType { get; set; }

        /// <summary>
        /// Gets or sets the SQL execution event details.
        /// </summary>
        [JsonPropertyName("sql_operation")]
        public SqlExecutionEvent? SqlOperation { get; set; }

        /// <summary>
        /// Gets or sets error information if an error occurred.
        /// </summary>
        [JsonPropertyName("error_info")]
        public DriverErrorInfo? ErrorInfo { get; set; }

        /// <summary>
        /// Gets or sets the operation latency in milliseconds.
        /// </summary>
        [JsonPropertyName("operation_latency_ms")]
        public long? OperationLatencyMs { get; set; }
    }

    /// <summary>
    /// Driver and system configuration information.
    /// </summary>
    public sealed class DriverSystemConfiguration
    {
        /// <summary>
        /// Gets or sets the driver name.
        /// </summary>
        [JsonPropertyName("driver_name")]
        public string? DriverName { get; set; }

        /// <summary>
        /// Gets or sets the driver version.
        /// </summary>
        [JsonPropertyName("driver_version")]
        public string? DriverVersion { get; set; }

        /// <summary>
        /// Gets or sets the operating system name.
        /// </summary>
        [JsonPropertyName("os_name")]
        public string? OsName { get; set; }

        /// <summary>
        /// Gets or sets the operating system version.
        /// </summary>
        [JsonPropertyName("os_version")]
        public string? OsVersion { get; set; }

        /// <summary>
        /// Gets or sets the operating system architecture.
        /// </summary>
        [JsonPropertyName("os_arch")]
        public string? OsArch { get; set; }

        /// <summary>
        /// Gets or sets the runtime name (e.g., ".NET").
        /// </summary>
        [JsonPropertyName("runtime_name")]
        public string? RuntimeName { get; set; }

        /// <summary>
        /// Gets or sets the runtime version.
        /// </summary>
        [JsonPropertyName("runtime_version")]
        public string? RuntimeVersion { get; set; }

        /// <summary>
        /// Gets or sets the locale name.
        /// </summary>
        [JsonPropertyName("locale_name")]
        public string? LocaleName { get; set; }

        /// <summary>
        /// Gets or sets the character set encoding.
        /// </summary>
        [JsonPropertyName("char_set_encoding")]
        public string? CharSetEncoding { get; set; }

        /// <summary>
        /// Gets or sets the client application name.
        /// </summary>
        [JsonPropertyName("client_app_name")]
        public string? ClientAppName { get; set; }

        /// <summary>
        /// Creates a DriverSystemConfiguration with current system information.
        /// </summary>
        public static DriverSystemConfiguration CreateDefault(string driverVersion)
        {
            return new DriverSystemConfiguration
            {
                DriverName = "Databricks ADBC Driver",
                DriverVersion = driverVersion,
                OsName = System.Runtime.InteropServices.RuntimeInformation.OSDescription,
                OsArch = System.Runtime.InteropServices.RuntimeInformation.OSArchitecture.ToString(),
                RuntimeName = ".NET",
                RuntimeVersion = Environment.Version.ToString(),
                LocaleName = System.Globalization.CultureInfo.CurrentCulture.Name,
                CharSetEncoding = "UTF-8"
            };
        }
    }

    /// <summary>
    /// Connection parameters for telemetry.
    /// </summary>
    public sealed class DriverConnectionParameters
    {
        /// <summary>
        /// Gets or sets the HTTP path.
        /// </summary>
        [JsonPropertyName("http_path")]
        public string? HttpPath { get; set; }

        /// <summary>
        /// Gets or sets the driver mode (e.g., "thrift").
        /// </summary>
        [JsonPropertyName("mode")]
        public string? Mode { get; set; }

        /// <summary>
        /// Gets or sets the authentication mechanism.
        /// </summary>
        [JsonPropertyName("auth_mech")]
        public string? AuthMech { get; set; }

        /// <summary>
        /// Gets or sets whether Arrow format is enabled.
        /// </summary>
        [JsonPropertyName("enable_arrow")]
        public bool? EnableArrow { get; set; }

        /// <summary>
        /// Gets or sets whether direct results are enabled.
        /// </summary>
        [JsonPropertyName("enable_direct_results")]
        public bool? EnableDirectResults { get; set; }

        /// <summary>
        /// Gets or sets whether CloudFetch is enabled.
        /// </summary>
        [JsonPropertyName("enable_cloud_fetch")]
        public bool? EnableCloudFetch { get; set; }

        /// <summary>
        /// Gets or sets whether LZ4 compression is enabled.
        /// </summary>
        [JsonPropertyName("enable_lz4_compression")]
        public bool? EnableLz4Compression { get; set; }
    }

    /// <summary>
    /// SQL execution event details.
    /// </summary>
    public sealed class SqlExecutionEvent
    {
        /// <summary>
        /// Gets or sets the statement type (e.g., "QUERY", "UPDATE").
        /// </summary>
        [JsonPropertyName("statement_type")]
        public string? StatementType { get; set; }

        /// <summary>
        /// Gets or sets whether results are compressed.
        /// </summary>
        [JsonPropertyName("is_compressed")]
        public bool? IsCompressed { get; set; }

        /// <summary>
        /// Gets or sets the execution result format (e.g., "INLINE", "CLOUD_FETCH").
        /// </summary>
        [JsonPropertyName("execution_result")]
        public string? ExecutionResult { get; set; }

        /// <summary>
        /// Gets or sets the retry count for this operation.
        /// </summary>
        [JsonPropertyName("retry_count")]
        public int? RetryCount { get; set; }

        /// <summary>
        /// Gets or sets the chunk count for CloudFetch results.
        /// </summary>
        [JsonPropertyName("chunk_count")]
        public int? ChunkCount { get; set; }

        /// <summary>
        /// Gets or sets the total bytes downloaded.
        /// </summary>
        [JsonPropertyName("total_bytes_downloaded")]
        public long? TotalBytesDownloaded { get; set; }
    }

    /// <summary>
    /// Error information for telemetry.
    /// </summary>
    public sealed class DriverErrorInfo
    {
        /// <summary>
        /// Gets or sets the error name (exception type).
        /// </summary>
        [JsonPropertyName("error_name")]
        public string? ErrorName { get; set; }

        /// <summary>
        /// Gets or sets the error message.
        /// </summary>
        [JsonPropertyName("error_message")]
        public string? ErrorMessage { get; set; }

        /// <summary>
        /// Creates a DriverErrorInfo from an exception.
        /// Note: Stack trace is intentionally omitted for privacy.
        /// </summary>
        public static DriverErrorInfo FromException(Exception ex)
        {
            return new DriverErrorInfo
            {
                ErrorName = ex.GetType().Name,
                ErrorMessage = ex.Message
            };
        }
    }

    /// <summary>
    /// Defines the type of telemetry event.
    /// </summary>
    public enum TelemetryEventType
    {
        /// <summary>
        /// Connection open event.
        /// </summary>
        ConnectionOpen,

        /// <summary>
        /// Statement execution event.
        /// </summary>
        StatementExecution,

        /// <summary>
        /// Error event.
        /// </summary>
        Error
    }
}
