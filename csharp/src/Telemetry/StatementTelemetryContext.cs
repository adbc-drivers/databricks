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
using System.Linq;
using AdbcDrivers.Databricks.Telemetry.Proto;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Holds all aggregated telemetry data for a single statement execution.
    /// Merges data from multiple activities (root + children) into a single
    /// complete proto message for export to the Databricks telemetry service.
    /// </summary>
    /// <remarks>
    /// This is the core data model that aggregates data from multiple Activity spans
    /// (e.g., ExecuteQuery, DownloadFiles, PollOperationStatus) into a single
    /// <see cref="OssSqlDriverTelemetryLog"/> proto message. Each statement execution
    /// creates one context that is populated as activities complete.
    /// </remarks>
    internal sealed class StatementTelemetryContext
    {
        /// <summary>
        /// Maximum length for truncated error messages.
        /// </summary>
        internal const int MaxErrorMessageLength = 200;

        #region Identifiers

        /// <summary>
        /// Gets or sets the statement execution ID.
        /// </summary>
        public string? StatementId { get; set; }

        /// <summary>
        /// Gets or sets the connection session ID.
        /// </summary>
        public string? SessionId { get; set; }

        /// <summary>
        /// Gets or sets the authentication type (e.g., "oauth-m2m", "pat").
        /// </summary>
        public string? AuthType { get; set; }

        #endregion

        #region System Configuration

        /// <summary>
        /// Gets or sets the driver system configuration.
        /// Populated from connection-level data.
        /// </summary>
        public DriverSystemConfiguration? SystemConfiguration { get; set; }

        #endregion

        #region Connection Parameters

        /// <summary>
        /// Gets or sets the driver connection parameters.
        /// Populated from connection-level data.
        /// </summary>
        public DriverConnectionParameters? DriverConnectionParams { get; set; }

        #endregion

        #region Statement Execution Metrics

        /// <summary>
        /// Gets or sets the statement type (query, update, metadata, etc.).
        /// </summary>
        public StatementType? StatementTypeValue { get; set; }

        /// <summary>
        /// Gets or sets the operation type (execute statement, list catalogs, etc.).
        /// </summary>
        public OperationType? OperationTypeValue { get; set; }

        /// <summary>
        /// Gets or sets the execution result format (inline_arrow, external_links, etc.).
        /// </summary>
        public ExecutionResultFormat? ResultFormat { get; set; }

        /// <summary>
        /// Gets or sets whether compression is enabled for results.
        /// </summary>
        public bool? CompressionEnabled { get; set; }

        /// <summary>
        /// Gets or sets the total operation latency in milliseconds.
        /// Derived from the root activity's Duration.
        /// </summary>
        public long? TotalLatencyMs { get; set; }

        /// <summary>
        /// Gets or sets the retry count for the operation.
        /// </summary>
        public long? RetryCount { get; set; }

        #endregion

        #region Result Latency

        /// <summary>
        /// Gets or sets the latency for the result set to be ready (first chunk or inline result).
        /// </summary>
        public long? ResultReadyLatencyMs { get; set; }

        /// <summary>
        /// Gets or sets the time taken to consume the full result set.
        /// </summary>
        public long? ResultConsumptionLatencyMs { get; set; }

        #endregion

        #region Chunk Details (CloudFetch)

        /// <summary>
        /// Gets or sets the total number of chunks present in the result.
        /// </summary>
        public int? TotalChunksPresent { get; set; }

        /// <summary>
        /// Gets or sets the number of chunks iterated before statement/connection was closed.
        /// </summary>
        public int? TotalChunksIterated { get; set; }

        /// <summary>
        /// Gets or sets the sum of download time across all chunks in milliseconds.
        /// </summary>
        public long? SumChunksDownloadTimeMs { get; set; }

        /// <summary>
        /// Gets or sets the latency for downloading the first chunk in milliseconds.
        /// </summary>
        public long? InitialChunkLatencyMs { get; set; }

        /// <summary>
        /// Gets or sets the maximum download latency among all chunks in milliseconds.
        /// </summary>
        public long? SlowestChunkLatencyMs { get; set; }

        #endregion

        #region Polling Metrics

        /// <summary>
        /// Gets or sets the number of getOperationStatus calls made.
        /// </summary>
        public int? PollCount { get; set; }

        /// <summary>
        /// Gets or sets the total polling latency in milliseconds.
        /// </summary>
        public long? PollLatencyMs { get; set; }

        #endregion

        #region Error Info

        /// <summary>
        /// Gets or sets whether the operation encountered an error.
        /// </summary>
        public bool HasError { get; set; }

        /// <summary>
        /// Gets or sets the error name/type.
        /// </summary>
        public string? ErrorName { get; set; }

        /// <summary>
        /// Gets or sets the error message (truncated to <see cref="MaxErrorMessageLength"/> characters).
        /// </summary>
        public string? ErrorMessage { get; set; }

        #endregion

        #region MergeFrom

        /// <summary>
        /// Merges telemetry data from an Activity into this context.
        /// Routes extraction logic based on the activity's OperationName.
        /// </summary>
        /// <param name="activity">The Activity to merge data from.</param>
        public void MergeFrom(Activity activity)
        {
            if (activity == null)
            {
                return;
            }

            // Always capture session and statement IDs from any activity
            CaptureIdentifiers(activity);

            // Route based on operation name
            string operationName = activity.OperationName ?? string.Empty;

            if (operationName.Contains("ExecuteQuery") || operationName.Contains("ExecuteUpdate"))
            {
                MergeFromExecuteActivity(activity);
            }
            else if (operationName.Contains("DownloadFiles") || operationName.Contains("CloudFetch"))
            {
                MergeFromDownloadActivity(activity);
            }
            else if (operationName.Contains("PollOperationStatus") || operationName.Contains("GetOperationStatus"))
            {
                MergeFromPollActivity(activity);
            }

            // Check for error status on any activity
            MergeErrorInfo(activity);
        }

        /// <summary>
        /// Captures session and statement IDs from activity tags.
        /// </summary>
        private void CaptureIdentifiers(Activity activity)
        {
            string? sessionId = activity.GetTagItem("session.id") as string;
            if (!string.IsNullOrEmpty(sessionId))
            {
                SessionId = sessionId;
            }

            string? statementId = activity.GetTagItem("statement.id") as string;
            if (!string.IsNullOrEmpty(statementId))
            {
                StatementId = statementId;
            }

            string? authType = activity.GetTagItem("auth.type") as string;
            if (!string.IsNullOrEmpty(authType))
            {
                AuthType = authType;
            }
        }

        /// <summary>
        /// Merges data from ExecuteQuery/ExecuteUpdate activities.
        /// Captures: statement type, operation type, result format, compression,
        /// total latency, retry count, and result latency.
        /// </summary>
        private void MergeFromExecuteActivity(Activity activity)
        {
            // Total latency from activity duration
            if (activity.Duration.TotalMilliseconds > 0)
            {
                TotalLatencyMs = (long)activity.Duration.TotalMilliseconds;
            }

            // Statement type
            string? statementType = activity.GetTagItem("statement.type") as string;
            if (!string.IsNullOrEmpty(statementType))
            {
                StatementTypeValue = ParseStatementType(statementType);
            }
            else if (activity.OperationName.Contains("ExecuteQuery"))
            {
                StatementTypeValue = StatementType.StatementQuery;
            }
            else if (activity.OperationName.Contains("ExecuteUpdate"))
            {
                StatementTypeValue = StatementType.StatementUpdate;
            }

            // Operation type
            string? operationType = activity.GetTagItem("operation.type") as string;
            if (!string.IsNullOrEmpty(operationType))
            {
                OperationTypeValue = ParseOperationType(operationType);
            }

            // Result format
            string? resultFormat = activity.GetTagItem("result.format") as string;
            if (!string.IsNullOrEmpty(resultFormat))
            {
                ResultFormat = ParseExecutionResult(resultFormat);
            }

            // Compression
            object? compressionTag = activity.GetTagItem("result.compression_enabled");
            if (compressionTag != null)
            {
                if (compressionTag is bool boolVal)
                {
                    CompressionEnabled = boolVal;
                }
                else if (bool.TryParse(compressionTag.ToString(), out bool parsed))
                {
                    CompressionEnabled = parsed;
                }
            }

            // Retry count
            object? retryCountTag = activity.GetTagItem("retry.count");
            if (retryCountTag != null)
            {
                if (retryCountTag is long longVal)
                {
                    RetryCount = longVal;
                }
                else if (long.TryParse(retryCountTag.ToString(), out long parsed))
                {
                    RetryCount = parsed;
                }
            }

            // Result latency - ready
            object? resultReadyTag = activity.GetTagItem("result.ready_latency_ms");
            if (resultReadyTag != null)
            {
                if (resultReadyTag is long longVal)
                {
                    ResultReadyLatencyMs = longVal;
                }
                else if (long.TryParse(resultReadyTag.ToString(), out long parsed))
                {
                    ResultReadyLatencyMs = parsed;
                }
            }

            // Result latency - consumption
            object? resultConsumptionTag = activity.GetTagItem("result.consumption_latency_ms");
            if (resultConsumptionTag != null)
            {
                if (resultConsumptionTag is long longVal)
                {
                    ResultConsumptionLatencyMs = longVal;
                }
                else if (long.TryParse(resultConsumptionTag.ToString(), out long parsed))
                {
                    ResultConsumptionLatencyMs = parsed;
                }
            }
        }

        /// <summary>
        /// Merges data from DownloadFiles/CloudFetch activities.
        /// Captures chunk details from both activity tags and the
        /// "cloudfetch.download_summary" ActivityEvent.
        /// </summary>
        private void MergeFromDownloadActivity(Activity activity)
        {
            // Chunk details from activity tags
            object? totalChunksTag = activity.GetTagItem("cloudfetch.total_chunks");
            if (totalChunksTag != null)
            {
                if (totalChunksTag is int intVal)
                {
                    TotalChunksPresent = intVal;
                }
                else if (int.TryParse(totalChunksTag.ToString(), out int parsed))
                {
                    TotalChunksPresent = parsed;
                }
            }

            object? chunksIteratedTag = activity.GetTagItem("cloudfetch.chunks_iterated");
            if (chunksIteratedTag != null)
            {
                if (chunksIteratedTag is int intVal)
                {
                    TotalChunksIterated = intVal;
                }
                else if (int.TryParse(chunksIteratedTag.ToString(), out int parsed))
                {
                    TotalChunksIterated = parsed;
                }
            }

            object? initialLatencyTag = activity.GetTagItem("cloudfetch.initial_chunk_latency_ms");
            if (initialLatencyTag != null)
            {
                if (initialLatencyTag is long longVal)
                {
                    InitialChunkLatencyMs = longVal;
                }
                else if (long.TryParse(initialLatencyTag.ToString(), out long parsed))
                {
                    InitialChunkLatencyMs = parsed;
                }
            }

            object? slowestLatencyTag = activity.GetTagItem("cloudfetch.slowest_chunk_latency_ms");
            if (slowestLatencyTag != null)
            {
                if (slowestLatencyTag is long longVal)
                {
                    SlowestChunkLatencyMs = longVal;
                }
                else if (long.TryParse(slowestLatencyTag.ToString(), out long parsed))
                {
                    SlowestChunkLatencyMs = parsed;
                }
            }

            object? sumDownloadTag = activity.GetTagItem("cloudfetch.sum_download_time_ms");
            if (sumDownloadTag != null)
            {
                if (sumDownloadTag is long longVal)
                {
                    SumChunksDownloadTimeMs = longVal;
                }
                else if (long.TryParse(sumDownloadTag.ToString(), out long parsed))
                {
                    SumChunksDownloadTimeMs = parsed;
                }
            }

            // Also extract chunk details from the "cloudfetch.download_summary" event
            MergeFromDownloadSummaryEvents(activity);
        }

        /// <summary>
        /// Extracts chunk details from the "cloudfetch.download_summary" ActivityEvent.
        /// </summary>
        private void MergeFromDownloadSummaryEvents(Activity activity)
        {
            foreach (ActivityEvent activityEvent in activity.Events)
            {
                if (activityEvent.Name != "cloudfetch.download_summary")
                {
                    continue;
                }

                foreach (System.Collections.Generic.KeyValuePair<string, object?> tag in activityEvent.Tags)
                {
                    switch (tag.Key)
                    {
                        case "total_files":
                            if (TotalChunksPresent == null && tag.Value != null)
                            {
                                if (tag.Value is int intVal)
                                {
                                    TotalChunksPresent = intVal;
                                }
                                else if (int.TryParse(tag.Value.ToString(), out int parsed))
                                {
                                    TotalChunksPresent = parsed;
                                }
                            }
                            break;
                        case "successful_downloads":
                            if (TotalChunksIterated == null && tag.Value != null)
                            {
                                if (tag.Value is int intVal)
                                {
                                    TotalChunksIterated = intVal;
                                }
                                else if (int.TryParse(tag.Value.ToString(), out int parsed))
                                {
                                    TotalChunksIterated = parsed;
                                }
                            }
                            break;
                        case "total_time_ms":
                            if (SumChunksDownloadTimeMs == null && tag.Value != null)
                            {
                                if (tag.Value is long longVal)
                                {
                                    SumChunksDownloadTimeMs = longVal;
                                }
                                else if (long.TryParse(tag.Value.ToString(), out long parsed))
                                {
                                    SumChunksDownloadTimeMs = parsed;
                                }
                            }
                            break;
                    }
                }
            }
        }

        /// <summary>
        /// Merges data from PollOperationStatus activities.
        /// Captures poll count and total poll latency.
        /// </summary>
        private void MergeFromPollActivity(Activity activity)
        {
            object? pollCountTag = activity.GetTagItem("poll.count");
            if (pollCountTag != null)
            {
                if (pollCountTag is int intVal)
                {
                    PollCount = intVal;
                }
                else if (int.TryParse(pollCountTag.ToString(), out int parsed))
                {
                    PollCount = parsed;
                }
            }

            object? pollLatencyTag = activity.GetTagItem("poll.latency_ms");
            if (pollLatencyTag != null)
            {
                if (pollLatencyTag is long longVal)
                {
                    PollLatencyMs = longVal;
                }
                else if (long.TryParse(pollLatencyTag.ToString(), out long parsed))
                {
                    PollLatencyMs = parsed;
                }
            }
        }

        /// <summary>
        /// Merges error information from activity status and tags.
        /// </summary>
        private void MergeErrorInfo(Activity activity)
        {
            if (activity.Status == ActivityStatusCode.Error)
            {
                HasError = true;

                string? errorType = activity.GetTagItem("error.type") as string;
                if (!string.IsNullOrEmpty(errorType))
                {
                    ErrorName = errorType;
                }

                string? errorMessage = activity.GetTagItem("error.message") as string;
                if (!string.IsNullOrEmpty(errorMessage))
                {
                    ErrorMessage = TruncateMessage(errorMessage);
                }
                else if (!string.IsNullOrEmpty(activity.StatusDescription))
                {
                    ErrorMessage = TruncateMessage(activity.StatusDescription);
                }
            }
        }

        #endregion

        #region BuildTelemetryLog

        /// <summary>
        /// Creates a complete <see cref="OssSqlDriverTelemetryLog"/> proto message
        /// from the aggregated telemetry data in this context.
        /// </summary>
        /// <returns>A fully populated proto message ready for export.</returns>
        public OssSqlDriverTelemetryLog BuildTelemetryLog()
        {
            OssSqlDriverTelemetryLog log = new OssSqlDriverTelemetryLog
            {
                SessionId = SessionId ?? string.Empty,
                SqlStatementId = StatementId ?? string.Empty,
                AuthType = AuthType ?? string.Empty,
                OperationLatencyMs = TotalLatencyMs ?? 0
            };

            // System configuration
            if (SystemConfiguration != null)
            {
                log.SystemConfiguration = SystemConfiguration;
            }

            // Driver connection parameters
            if (DriverConnectionParams != null)
            {
                log.DriverConnectionParams = DriverConnectionParams;
            }

            // SQL operation
            SqlExecutionEvent sqlOperation = new SqlExecutionEvent();
            bool hasSqlOperationData = false;

            if (StatementTypeValue.HasValue)
            {
                sqlOperation.StatementType = StatementTypeValue.Value;
                hasSqlOperationData = true;
            }

            if (CompressionEnabled.HasValue)
            {
                sqlOperation.IsCompressed = CompressionEnabled.Value;
                hasSqlOperationData = true;
            }

            if (ResultFormat.HasValue)
            {
                sqlOperation.ExecutionResult = ResultFormat.Value;
                hasSqlOperationData = true;
            }

            if (RetryCount.HasValue)
            {
                sqlOperation.RetryCount = RetryCount.Value;
                hasSqlOperationData = true;
            }

            // Chunk details
            if (TotalChunksPresent.HasValue || TotalChunksIterated.HasValue ||
                SumChunksDownloadTimeMs.HasValue || InitialChunkLatencyMs.HasValue ||
                SlowestChunkLatencyMs.HasValue)
            {
                ChunkDetails chunkDetails = new ChunkDetails();

                if (TotalChunksPresent.HasValue)
                {
                    chunkDetails.TotalChunksPresent = TotalChunksPresent.Value;
                }
                if (TotalChunksIterated.HasValue)
                {
                    chunkDetails.TotalChunksIterated = TotalChunksIterated.Value;
                }
                if (SumChunksDownloadTimeMs.HasValue)
                {
                    chunkDetails.SumChunksDownloadTimeMillis = SumChunksDownloadTimeMs.Value;
                }
                if (InitialChunkLatencyMs.HasValue)
                {
                    chunkDetails.InitialChunkLatencyMillis = InitialChunkLatencyMs.Value;
                }
                if (SlowestChunkLatencyMs.HasValue)
                {
                    chunkDetails.SlowestChunkLatencyMillis = SlowestChunkLatencyMs.Value;
                }

                sqlOperation.ChunkDetails = chunkDetails;
                hasSqlOperationData = true;
            }

            // Result latency
            if (ResultReadyLatencyMs.HasValue || ResultConsumptionLatencyMs.HasValue)
            {
                ResultLatency resultLatency = new ResultLatency();

                if (ResultReadyLatencyMs.HasValue)
                {
                    resultLatency.ResultSetReadyLatencyMillis = ResultReadyLatencyMs.Value;
                }
                if (ResultConsumptionLatencyMs.HasValue)
                {
                    resultLatency.ResultSetConsumptionLatencyMillis = ResultConsumptionLatencyMs.Value;
                }

                sqlOperation.ResultLatency = resultLatency;
                hasSqlOperationData = true;
            }

            // Operation detail (polling metrics)
            if (PollCount.HasValue || PollLatencyMs.HasValue || OperationTypeValue.HasValue)
            {
                OperationDetail operationDetail = new OperationDetail();

                if (PollCount.HasValue)
                {
                    operationDetail.NOperationStatusCalls = PollCount.Value;
                }
                if (PollLatencyMs.HasValue)
                {
                    operationDetail.OperationStatusLatencyMillis = PollLatencyMs.Value;
                }
                if (OperationTypeValue.HasValue)
                {
                    operationDetail.OperationType = OperationTypeValue.Value;
                }

                sqlOperation.OperationDetail = operationDetail;
                hasSqlOperationData = true;
            }

            if (hasSqlOperationData)
            {
                log.SqlOperation = sqlOperation;
            }

            // Error info
            if (HasError)
            {
                DriverErrorInfo errorInfo = new DriverErrorInfo();

                if (!string.IsNullOrEmpty(ErrorName))
                {
                    errorInfo.ErrorName = ErrorName;
                }

                if (!string.IsNullOrEmpty(ErrorMessage))
                {
                    errorInfo.StackTrace = ErrorMessage;
                }

                log.ErrorInfo = errorInfo;
            }

            return log;
        }

        #endregion

        #region Static Helpers

        /// <summary>
        /// Parses an execution result format string to the corresponding proto enum.
        /// </summary>
        /// <param name="value">The result format string (e.g., "cloudfetch", "inline_arrow").</param>
        /// <returns>The parsed <see cref="ExecutionResultFormat"/> value.</returns>
        public static ExecutionResultFormat ParseExecutionResult(string? value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return ExecutionResultFormat.Unspecified;
            }

            string normalized = value.ToLowerInvariant().Trim();

            return normalized switch
            {
                "cloudfetch" or "cloud_fetch" or "external_links" => ExecutionResultFormat.ExecutionResultExternalLinks,
                "inline_arrow" or "inline-arrow" or "inlinearrow" => ExecutionResultFormat.ExecutionResultInlineArrow,
                "inline_json" or "inline-json" or "inlinejson" => ExecutionResultFormat.ExecutionResultInlineJson,
                "columnar_inline" or "columnar-inline" or "columnarinline" => ExecutionResultFormat.ExecutionResultColumnarInline,
                _ => ExecutionResultFormat.Unspecified
            };
        }

        /// <summary>
        /// Parses a driver mode string to the corresponding proto enum.
        /// </summary>
        /// <param name="value">The driver mode string (e.g., "thrift", "sea").</param>
        /// <returns>The parsed <see cref="DriverModeType"/> value.</returns>
        public static DriverModeType ParseDriverMode(string? value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return DriverModeType.Unspecified;
            }

            string normalized = value.ToLowerInvariant().Trim();

            return normalized switch
            {
                "thrift" => DriverModeType.DriverModeThrift,
                "sea" => DriverModeType.DriverModeSea,
                _ => DriverModeType.Unspecified
            };
        }

        /// <summary>
        /// Parses an auth mechanism string to the corresponding proto enum.
        /// </summary>
        /// <param name="value">The auth mechanism string (e.g., "pat", "oauth").</param>
        /// <returns>The parsed <see cref="DriverAuthMechType"/> value.</returns>
        public static DriverAuthMechType ParseAuthMech(string? value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return DriverAuthMechType.Unspecified;
            }

            string normalized = value.ToLowerInvariant().Trim();

            return normalized switch
            {
                "pat" or "token" => DriverAuthMechType.DriverAuthMechPat,
                "oauth" or "oauth2" => DriverAuthMechType.DriverAuthMechOauth,
                "other" => DriverAuthMechType.DriverAuthMechOther,
                _ => DriverAuthMechType.Unspecified
            };
        }

        /// <summary>
        /// Parses an auth flow string to the corresponding proto enum.
        /// </summary>
        /// <param name="value">The auth flow string (e.g., "client_credentials", "browser").</param>
        /// <returns>The parsed <see cref="DriverAuthFlowType"/> value.</returns>
        public static DriverAuthFlowType ParseAuthFlow(string? value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return DriverAuthFlowType.Unspecified;
            }

            string normalized = value.ToLowerInvariant().Trim();

            return normalized switch
            {
                "client_credentials" or "m2m" => DriverAuthFlowType.DriverAuthFlowClientCredentials,
                "token_passthrough" or "token" => DriverAuthFlowType.DriverAuthFlowTokenPassthrough,
                "browser" or "browser_based" => DriverAuthFlowType.DriverAuthFlowBrowserBasedAuthentication,
                _ => DriverAuthFlowType.Unspecified
            };
        }

        /// <summary>
        /// Parses a statement type string to the corresponding proto enum.
        /// </summary>
        /// <param name="value">The statement type string (e.g., "query", "update", "metadata").</param>
        /// <returns>The parsed <see cref="StatementType"/> value.</returns>
        public static StatementType ParseStatementType(string? value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return StatementType.Unspecified;
            }

            string normalized = value.ToLowerInvariant().Trim();

            return normalized switch
            {
                "query" => StatementType.StatementQuery,
                "sql" => StatementType.StatementSql,
                "update" => StatementType.StatementUpdate,
                "metadata" => StatementType.StatementMetadata,
                "volume" => StatementType.StatementVolume,
                _ => StatementType.Unspecified
            };
        }

        /// <summary>
        /// Parses an operation type string to the corresponding proto enum.
        /// </summary>
        /// <param name="value">The operation type string (e.g., "execute_statement", "list_catalogs").</param>
        /// <returns>The parsed <see cref="OperationType"/> value.</returns>
        public static OperationType ParseOperationType(string? value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return OperationType.Unspecified;
            }

            string normalized = value.ToLowerInvariant().Trim();

            return normalized switch
            {
                "create_session" => OperationType.OperationCreateSession,
                "delete_session" => OperationType.OperationDeleteSession,
                "execute_statement" => OperationType.OperationExecuteStatement,
                "execute_statement_async" => OperationType.OperationExecuteStatementAsync,
                "close_statement" => OperationType.OperationCloseStatement,
                "cancel_statement" => OperationType.OperationCancelStatement,
                "list_type_info" => OperationType.OperationListTypeInfo,
                "list_catalogs" => OperationType.OperationListCatalogs,
                "list_schemas" => OperationType.OperationListSchemas,
                "list_tables" => OperationType.OperationListTables,
                "list_table_types" => OperationType.OperationListTableTypes,
                "list_columns" => OperationType.OperationListColumns,
                "list_functions" => OperationType.OperationListFunctions,
                "list_primary_keys" => OperationType.OperationListPrimaryKeys,
                "list_imported_keys" => OperationType.OperationListImportedKeys,
                "list_exported_keys" => OperationType.OperationListExportedKeys,
                "list_cross_references" => OperationType.OperationListCrossReferences,
                _ => OperationType.Unspecified
            };
        }

        /// <summary>
        /// Truncates a message to <see cref="MaxErrorMessageLength"/> characters.
        /// </summary>
        /// <param name="message">The message to truncate.</param>
        /// <returns>The truncated message, or the original if within length.</returns>
        public static string TruncateMessage(string? message)
        {
            if (string.IsNullOrEmpty(message))
            {
                return string.Empty;
            }

            if (message.Length <= MaxErrorMessageLength)
            {
                return message;
            }

            return message.Substring(0, MaxErrorMessageLength);
        }

        #endregion
    }
}
