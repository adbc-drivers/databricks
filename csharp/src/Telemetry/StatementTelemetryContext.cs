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
using AdbcDrivers.Databricks.Telemetry.Proto;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Per-statement telemetry context that holds all telemetry data for a single statement execution.
    /// Populated inline by driver code as operations happen.
    /// </summary>
    /// <remarks>
    /// This class is protocol-agnostic and works for both Thrift (HiveServer2) and SEA (Statement Execution API) backends.
    /// Lives for the full duration of statement execution (ExecuteQueryAsync() to reader disposal).
    /// Uses a single Stopwatch for all timing to ensure consistency.
    /// </remarks>
    internal sealed class StatementTelemetryContext
    {
        private readonly TelemetrySessionContext _sessionContext;

        /// <summary>
        /// Initializes a new instance of the <see cref="StatementTelemetryContext"/> class.
        /// </summary>
        /// <param name="sessionContext">The immutable session-level context from the connection.</param>
        public StatementTelemetryContext(TelemetrySessionContext sessionContext)
        {
            _sessionContext = sessionContext ?? throw new ArgumentNullException(nameof(sessionContext));
            ExecuteStopwatch = Stopwatch.StartNew();
        }

        // ── Populated at construction (from connection session context) ──

        /// <summary>
        /// Gets the session ID from the connection.
        /// </summary>
        public string? SessionId => _sessionContext.SessionId;

        /// <summary>
        /// Gets the authentication type from the connection.
        /// </summary>
        public string? AuthType => _sessionContext.AuthType;

        /// <summary>
        /// Gets the workspace ID from the connection.
        /// </summary>
        public long WorkspaceId => _sessionContext.WorkspaceId;

        /// <summary>
        /// Gets the driver system configuration from the connection.
        /// </summary>
        public DriverSystemConfiguration? SystemConfiguration => _sessionContext.SystemConfiguration;

        /// <summary>
        /// Gets the driver connection parameters from the connection.
        /// </summary>
        public DriverConnectionParameters? DriverConnectionParams => _sessionContext.DriverConnectionParams;

        // ── Populated by Statement during execution ──

        /// <summary>
        /// Gets or sets the statement ID from the server.
        /// </summary>
        public string? StatementId { get; set; }

        /// <summary>
        /// Gets or sets the type of statement (QUERY, UPDATE, METADATA, etc.).
        /// </summary>
        public StatementType StatementType { get; set; }

        /// <summary>
        /// Gets or sets the type of operation (EXECUTE_STATEMENT, CLOSE_STATEMENT, etc.).
        /// </summary>
        public OperationType OperationType { get; set; }

        /// <summary>
        /// Gets or sets the result format (INLINE_ARROW, EXTERNAL_LINKS, etc.).
        /// </summary>
        public ExecutionResultFormat ResultFormat { get; set; }

        /// <summary>
        /// Gets or sets whether the results are compressed.
        /// </summary>
        public bool IsCompressed { get; set; }

        // ── Timing (all derived from single Stopwatch) ──

        /// <summary>
        /// Gets the stopwatch started at context construction.
        /// All timing measurements derive from this single stopwatch.
        /// </summary>
        public Stopwatch ExecuteStopwatch { get; }

        /// <summary>
        /// Gets or sets the number of polling operations performed.
        /// </summary>
        public int? PollCount { get; set; }

        /// <summary>
        /// Gets or sets the total latency of all polling operations in milliseconds.
        /// </summary>
        public long? PollLatencyMs { get; set; }

        /// <summary>
        /// Gets or sets the elapsed time in milliseconds from execution start to first batch ready.
        /// </summary>
        public long? FirstBatchReadyMs { get; set; }

        /// <summary>
        /// Gets or sets the elapsed time in milliseconds from execution start to results fully consumed.
        /// </summary>
        public long? ResultsConsumedMs { get; set; }

        // ── CloudFetch chunk details ──

        /// <summary>
        /// Gets or sets the total number of chunks present in the result.
        /// </summary>
        public int? TotalChunksPresent { get; set; }

        /// <summary>
        /// Gets or sets the number of chunks actually iterated by the client.
        /// </summary>
        public int? TotalChunksIterated { get; set; }

        /// <summary>
        /// Gets or sets the time taken to download the first chunk in milliseconds.
        /// </summary>
        public long? InitialChunkLatencyMs { get; set; }

        /// <summary>
        /// Gets or sets the maximum time taken to download any chunk in milliseconds.
        /// </summary>
        public long? SlowestChunkLatencyMs { get; set; }

        /// <summary>
        /// Gets or sets the sum of download times for all chunks in milliseconds.
        /// </summary>
        public long? SumChunksDownloadTimeMs { get; set; }

        // ── Error info ──

        /// <summary>
        /// Gets or sets a value indicating whether an error occurred during execution.
        /// </summary>
        public bool HasError { get; set; }

        /// <summary>
        /// Gets or sets the name of the error (exception type name).
        /// </summary>
        public string? ErrorName { get; set; }

        /// <summary>
        /// Gets or sets the error message.
        /// </summary>
        public string? ErrorMessage { get; set; }

        // ── Helper methods for recording events ──

        /// <summary>
        /// Records that the execute operation has completed.
        /// </summary>
        public void RecordExecuteComplete()
        {
            // Currently no-op, but can be extended to capture execute-specific timing
        }

        /// <summary>
        /// Records that the first batch of results is ready.
        /// Sets FirstBatchReadyMs if not already set.
        /// </summary>
        public void RecordFirstBatchReady()
        {
            if (FirstBatchReadyMs == null)
            {
                FirstBatchReadyMs = ExecuteStopwatch.ElapsedMilliseconds;
            }
        }

        /// <summary>
        /// Records that all results have been consumed.
        /// Sets ResultsConsumedMs.
        /// </summary>
        public void RecordResultsConsumed()
        {
            ResultsConsumedMs = ExecuteStopwatch.ElapsedMilliseconds;
        }

        /// <summary>
        /// Sets CloudFetch chunk details.
        /// </summary>
        /// <param name="totalPresent">Total number of chunks present in the result.</param>
        /// <param name="totalIterated">Number of chunks actually iterated.</param>
        /// <param name="initialLatencyMs">Time to download the first chunk in milliseconds.</param>
        /// <param name="slowestLatencyMs">Maximum time to download any chunk in milliseconds.</param>
        /// <param name="sumDownloadTimeMs">Sum of download times for all chunks in milliseconds.</param>
        public void SetChunkDetails(
            int totalPresent,
            int totalIterated,
            long initialLatencyMs,
            long slowestLatencyMs,
            long sumDownloadTimeMs)
        {
            TotalChunksPresent = totalPresent;
            TotalChunksIterated = totalIterated;
            InitialChunkLatencyMs = initialLatencyMs;
            SlowestChunkLatencyMs = slowestLatencyMs;
            SumChunksDownloadTimeMs = sumDownloadTimeMs;
        }

        /// <summary>
        /// Builds the telemetry proto log from the context data.
        /// </summary>
        /// <returns>The constructed <see cref="OssSqlDriverTelemetryLog"/> proto message.</returns>
        public OssSqlDriverTelemetryLog BuildTelemetryLog()
        {
            OssSqlDriverTelemetryLog log = new OssSqlDriverTelemetryLog
            {
                SessionId = SessionId ?? string.Empty,
                SqlStatementId = StatementId ?? string.Empty,
                AuthType = AuthType ?? string.Empty,
                SystemConfiguration = SystemConfiguration,
                DriverConnectionParams = DriverConnectionParams
            };

            // Set operation latency (total elapsed time)
            log.OperationLatencyMs = ExecuteStopwatch.ElapsedMilliseconds;

            // Build SQL execution event
            SqlExecutionEvent sqlEvent = new SqlExecutionEvent
            {
                StatementType = StatementType,
                IsCompressed = IsCompressed,
                ExecutionResult = ResultFormat
            };

            // Add chunk details if present
            if (TotalChunksPresent.HasValue || TotalChunksIterated.HasValue)
            {
                sqlEvent.ChunkDetails = new ChunkDetails
                {
                    TotalChunksPresent = TotalChunksPresent ?? 0,
                    TotalChunksIterated = TotalChunksIterated ?? 0,
                    InitialChunkLatencyMillis = InitialChunkLatencyMs ?? 0,
                    SlowestChunkLatencyMillis = SlowestChunkLatencyMs ?? 0,
                    SumChunksDownloadTimeMillis = SumChunksDownloadTimeMs ?? 0
                };
            }

            // Add result latency details
            if (FirstBatchReadyMs.HasValue || ResultsConsumedMs.HasValue)
            {
                sqlEvent.ResultLatency = new ResultLatency
                {
                    ResultSetReadyLatencyMillis = FirstBatchReadyMs ?? 0,
                    ResultSetConsumptionLatencyMillis = ResultsConsumedMs ?? 0
                };
            }

            // Add operation detail (polling info)
            if (PollCount.HasValue || PollLatencyMs.HasValue)
            {
                sqlEvent.OperationDetail = new OperationDetail
                {
                    NOperationStatusCalls = PollCount ?? 0,
                    OperationStatusLatencyMillis = PollLatencyMs ?? 0,
                    OperationType = OperationType,
                    IsInternalCall = false
                };
            }
            else
            {
                // Always include operation detail with at least the operation type
                sqlEvent.OperationDetail = new OperationDetail
                {
                    OperationType = OperationType,
                    IsInternalCall = false
                };
            }

            log.SqlOperation = sqlEvent;

            // Add error info if present
            if (HasError)
            {
                log.ErrorInfo = new DriverErrorInfo
                {
                    ErrorName = ErrorName ?? string.Empty,
                    StackTrace = string.Empty // Stack trace intentionally left empty per privacy requirements
                };
            }

            return log;
        }
    }
}
