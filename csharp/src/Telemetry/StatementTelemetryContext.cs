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
using System.Diagnostics;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Holds aggregated telemetry data for a single statement execution.
    /// Merges data from multiple activities (parent and children) into a single context
    /// and builds the OssSqlDriverTelemetryLog proto message.
    /// </summary>
    internal sealed class StatementTelemetryContext
    {
        #region Properties

        // Identifiers
        public string StatementId { get; set; } = string.Empty;
        public string? SessionId { get; set; }
        public string? AuthType { get; set; }

        // System configuration (populated once per connection or extracted from activity tags)
        public Proto.DriverSystemConfiguration? SystemConfiguration { get; set; }
        public Proto.DriverConnectionParameters? ConnectionParameters { get; set; }

        // Statement execution
        public Proto.StatementType? StatementType { get; set; }
        public Proto.OperationType? OperationType { get; set; }
        public bool? IsInternalCall { get; set; }
        public string? ResultFormat { get; set; }
        public bool? CompressionEnabled { get; set; }
        public long TotalLatencyMs { get; set; }
        public int? RetryCount { get; set; }

        // Result latency
        public long? ResultReadyLatencyMs { get; set; }
        public long? ResultConsumptionLatencyMs { get; set; }

        // Chunk details (CloudFetch)
        public int? TotalChunksPresent { get; set; }
        public int? TotalChunksIterated { get; set; }
        public long? SumChunksDownloadTimeMs { get; set; }
        public long? InitialChunkLatencyMs { get; set; }
        public long? SlowestChunkLatencyMs { get; set; }

        // Polling metrics
        public int? PollCount { get; set; }
        public long? PollLatencyMs { get; set; }

        // Error info
        public bool HasError { get; set; }
        public string? ErrorName { get; set; }
        public string? ErrorMessage { get; set; }

        #endregion

        #region Constructor

        /// <summary>
        /// Creates a new StatementTelemetryContext with optional connection-level data.
        /// </summary>
        /// <param name="sessionId">The connection session ID.</param>
        /// <param name="systemConfig">Pre-built system configuration from connection setup.</param>
        /// <param name="connectionParams">Pre-built connection parameters from connection setup.</param>
        public StatementTelemetryContext(
            string? sessionId = null,
            Proto.DriverSystemConfiguration? systemConfig = null,
            Proto.DriverConnectionParameters? connectionParams = null)
        {
            SessionId = sessionId;
            SystemConfiguration = systemConfig;
            ConnectionParameters = connectionParams;
        }

        #endregion

        #region MergeFrom

        /// <summary>
        /// Merge data from an activity based on its operation name.
        /// Called for each activity associated with this statement.
        /// </summary>
        /// <param name="activity">The activity to merge data from.</param>
        public void MergeFrom(Activity activity)
        {
            // Always capture identifiers (first non-null wins)
            SessionId ??= activity.GetTagItem("session.id")?.ToString();
            if (string.IsNullOrEmpty(StatementId))
            {
                StatementId = activity.GetTagItem("statement.id")?.ToString() ?? string.Empty;
            }
            AuthType ??= activity.GetTagItem("auth.type")?.ToString();

            // Merge based on operation name
            switch (activity.OperationName)
            {
                case "ExecuteQuery":
                case "ExecuteUpdate":
                case "GetCatalogs":
                case "GetSchemas":
                case "GetTables":
                case "GetColumns":
                case "GetTableTypes":
                    MergeStatementActivity(activity);
                    break;

                case "DownloadFiles":
                    MergeDownloadActivity(activity);
                    break;

                case "PollOperationStatus":
                    MergePollingMetrics(activity);
                    break;
            }

            // Try to extract system configuration from activity tags (if not already set)
            TryExtractSystemConfig(activity);

            // Always check for errors
            if (activity.Status == ActivityStatusCode.Error)
            {
                MergeErrorInfo(activity);
            }
        }

        #endregion

        #region BuildProto

        /// <summary>
        /// Builds a complete OssSqlDriverTelemetryLog proto message from the aggregated data.
        /// </summary>
        /// <returns>A fully populated OssSqlDriverTelemetryLog proto message.</returns>
        public Proto.OssSqlDriverTelemetryLog BuildProto()
        {
            var telemetryLog = new Proto.OssSqlDriverTelemetryLog
            {
                SessionId = SessionId ?? string.Empty,
                SqlStatementId = StatementId ?? string.Empty,
                AuthType = AuthType ?? string.Empty,
                OperationLatencyMs = TotalLatencyMs,
                SystemConfiguration = SystemConfiguration,
                DriverConnectionParams = ConnectionParameters,
                SqlOperation = new Proto.SqlExecutionEvent
                {
                    StatementType = StatementType ?? Proto.StatementType.Unspecified,
                    IsCompressed = CompressionEnabled ?? false,
                    ExecutionResult = ParseExecutionResult(ResultFormat),
                    RetryCount = RetryCount ?? 0,
                    ChunkDetails = new Proto.ChunkDetails
                    {
                        TotalChunksPresent = TotalChunksPresent ?? 0,
                        TotalChunksIterated = TotalChunksIterated ?? 0,
                        SumChunksDownloadTimeMillis = SumChunksDownloadTimeMs ?? 0,
                        InitialChunkLatencyMillis = InitialChunkLatencyMs ?? 0,
                        SlowestChunkLatencyMillis = SlowestChunkLatencyMs ?? 0
                    },
                    ResultLatency = new Proto.ResultLatency
                    {
                        ResultSetReadyLatencyMillis = ResultReadyLatencyMs ?? 0,
                        ResultSetConsumptionLatencyMillis = ResultConsumptionLatencyMs ?? 0
                    },
                    OperationDetail = new Proto.OperationDetail
                    {
                        NOperationStatusCalls = PollCount ?? 0,
                        OperationStatusLatencyMillis = PollLatencyMs ?? 0,
                        OperationType = OperationType ?? Proto.OperationType.Unspecified,
                        IsInternalCall = IsInternalCall ?? false
                    }
                }
            };

            // Add error info if present
            if (HasError)
            {
                telemetryLog.ErrorInfo = new Proto.DriverErrorInfo
                {
                    ErrorName = ErrorName ?? string.Empty,
                    StackTrace = TruncateMessage(ErrorMessage, MaxErrorMessageLength)
                };
            }

            return telemetryLog;
        }

        #endregion

        #region Private Merge Methods

        private void MergeStatementActivity(Activity activity)
        {
            // Capture total operation latency from activity duration
            TotalLatencyMs = (long)activity.Duration.TotalMilliseconds;

            // Statement type
            var stmtType = activity.GetTagItem("statement.type")?.ToString();
            if (stmtType != null)
            {
                StatementType = ParseStatementType(stmtType);
            }

            // Operation type
            var opType = activity.GetTagItem("operation.type")?.ToString();
            if (opType != null)
            {
                OperationType = ParseOperationType(opType);
            }

            // Internal call flag
            var isInternal = activity.GetTagItem("operation.is_internal");
            if (isInternal != null)
            {
                IsInternalCall = TryParseBool(isInternal);
            }

            // Result format
            ResultFormat ??= activity.GetTagItem("result.format")?.ToString();

            // Compression
            var compression = activity.GetTagItem("result.compression_enabled");
            if (compression != null)
            {
                CompressionEnabled = TryParseBool(compression);
            }

            // Retry count
            var retryCount = activity.GetTagItem("retry.count");
            if (retryCount != null)
            {
                RetryCount = TryParseInt(retryCount);
            }

            // Result latency
            var readyLatency = activity.GetTagItem("result.ready_latency_ms");
            if (readyLatency != null)
            {
                ResultReadyLatencyMs = TryParseLong(readyLatency);
            }

            var consumptionLatency = activity.GetTagItem("result.consumption_latency_ms");
            if (consumptionLatency != null)
            {
                ResultConsumptionLatencyMs = TryParseLong(consumptionLatency);
            }
        }

        private void MergeDownloadActivity(Activity activity)
        {
            // Process cloudfetch.download_summary events
            foreach (var evt in activity.Events)
            {
                if (evt.Name == "cloudfetch.download_summary")
                {
                    ProcessCloudFetchSummaryEvent(evt);
                }
            }
        }

        private void ProcessCloudFetchSummaryEvent(ActivityEvent evt)
        {
            foreach (var tag in evt.Tags)
            {
                switch (tag.Key)
                {
                    case "total_files":
                        TotalChunksPresent = TryParseInt(tag.Value);
                        break;
                    case "successful_downloads":
                        TotalChunksIterated = TryParseInt(tag.Value);
                        break;
                    case "total_time_ms":
                        SumChunksDownloadTimeMs = TryParseLong(tag.Value);
                        break;
                    case "initial_chunk_latency_ms":
                        InitialChunkLatencyMs = TryParseLong(tag.Value);
                        break;
                    case "slowest_chunk_latency_ms":
                        SlowestChunkLatencyMs = TryParseLong(tag.Value);
                        break;
                }
            }
        }

        private void MergePollingMetrics(Activity activity)
        {
            var pollCount = activity.GetTagItem("poll.count");
            if (pollCount != null)
            {
                PollCount = TryParseInt(pollCount);
            }

            var pollLatency = activity.GetTagItem("poll.latency_ms");
            if (pollLatency != null)
            {
                PollLatencyMs = TryParseLong(pollLatency);
            }
        }

        private void MergeErrorInfo(Activity activity)
        {
            HasError = true;
            ErrorName ??= activity.GetTagItem("error.type")?.ToString();
            ErrorMessage ??= activity.StatusDescription;
        }

        private void TryExtractSystemConfig(Activity activity)
        {
            if (SystemConfiguration != null) return;

            // Check if this activity has system configuration tags
            var driverVersion = activity.GetTagItem("driver.version")?.ToString();
            if (driverVersion == null) return;

            SystemConfiguration = new Proto.DriverSystemConfiguration
            {
                DriverVersion = driverVersion,
                DriverName = activity.GetTagItem("driver.name")?.ToString() ?? string.Empty,
                RuntimeName = activity.GetTagItem("runtime.name")?.ToString() ?? string.Empty,
                RuntimeVersion = activity.GetTagItem("runtime.version")?.ToString() ?? string.Empty,
                RuntimeVendor = activity.GetTagItem("runtime.vendor")?.ToString() ?? string.Empty,
                OsName = activity.GetTagItem("os.name")?.ToString() ?? string.Empty,
                OsVersion = activity.GetTagItem("os.version")?.ToString() ?? string.Empty,
                OsArch = activity.GetTagItem("os.arch")?.ToString() ?? string.Empty,
                ClientAppName = activity.GetTagItem("client.app_name")?.ToString() ?? string.Empty,
                LocaleName = activity.GetTagItem("locale.name")?.ToString() ?? string.Empty,
                CharSetEncoding = activity.GetTagItem("char_set_encoding")?.ToString() ?? string.Empty,
                ProcessName = activity.GetTagItem("process.name")?.ToString() ?? string.Empty,
            };
        }

        #endregion

        #region Static Helpers

        /// <summary>
        /// Maximum length for error messages stored in DriverErrorInfo.StackTrace.
        /// </summary>
        internal const int MaxErrorMessageLength = 200;

        internal static Proto.StatementType ParseStatementType(string? type)
        {
            return type?.ToLowerInvariant() switch
            {
                "query" => Proto.StatementType.StatementQuery,
                "sql" => Proto.StatementType.StatementSql,
                "update" => Proto.StatementType.StatementUpdate,
                "metadata" => Proto.StatementType.StatementMetadata,
                "volume" => Proto.StatementType.StatementVolume,
                _ => Proto.StatementType.Unspecified
            };
        }

        internal static Proto.OperationType ParseOperationType(string? type)
        {
            return type?.ToLowerInvariant() switch
            {
                "create_session" => Proto.OperationType.OperationCreateSession,
                "delete_session" => Proto.OperationType.OperationDeleteSession,
                "execute_statement" => Proto.OperationType.OperationExecuteStatement,
                "execute_statement_async" => Proto.OperationType.OperationExecuteStatementAsync,
                "close_statement" => Proto.OperationType.OperationCloseStatement,
                "cancel_statement" => Proto.OperationType.OperationCancelStatement,
                "list_type_info" => Proto.OperationType.OperationListTypeInfo,
                "list_catalogs" => Proto.OperationType.OperationListCatalogs,
                "list_schemas" => Proto.OperationType.OperationListSchemas,
                "list_tables" => Proto.OperationType.OperationListTables,
                "list_table_types" => Proto.OperationType.OperationListTableTypes,
                "list_columns" => Proto.OperationType.OperationListColumns,
                "list_functions" => Proto.OperationType.OperationListFunctions,
                "list_primary_keys" => Proto.OperationType.OperationListPrimaryKeys,
                "list_imported_keys" => Proto.OperationType.OperationListImportedKeys,
                "list_exported_keys" => Proto.OperationType.OperationListExportedKeys,
                "list_cross_references" => Proto.OperationType.OperationListCrossReferences,
                _ => Proto.OperationType.Unspecified
            };
        }

        internal static Proto.ExecutionResultFormat ParseExecutionResult(string? format)
        {
            return format?.ToLowerInvariant() switch
            {
                "cloudfetch" or "external_links" => Proto.ExecutionResultFormat.ExecutionResultExternalLinks,
                "arrow" or "inline_arrow" => Proto.ExecutionResultFormat.ExecutionResultInlineArrow,
                "json" or "inline_json" => Proto.ExecutionResultFormat.ExecutionResultInlineJson,
                "columnar" or "columnar_inline" => Proto.ExecutionResultFormat.ExecutionResultColumnarInline,
                _ => Proto.ExecutionResultFormat.Unspecified
            };
        }

        internal static string TruncateMessage(string? message, int maxLength)
        {
            if (string.IsNullOrEmpty(message)) return string.Empty;
            return message.Length <= maxLength ? message : message.Substring(0, maxLength);
        }

        private static int? TryParseInt(object? value)
        {
            return value switch
            {
                int i => i,
                long l => (int)l,
                string s when int.TryParse(s, out var parsed) => parsed,
                _ => null
            };
        }

        private static long? TryParseLong(object? value)
        {
            return value switch
            {
                long l => l,
                int i => (long)i,
                string s when long.TryParse(s, out var parsed) => parsed,
                _ => null
            };
        }

        private static bool? TryParseBool(object? value)
        {
            return value switch
            {
                bool b => b,
                string s when bool.TryParse(s, out var parsed) => parsed,
                _ => null
            };
        }

        #endregion
    }
}
