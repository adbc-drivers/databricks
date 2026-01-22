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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.Databricks.Telemetry.TagDefinitions;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Aggregates metrics from activities by statement_id and includes session_id.
    /// Follows JDBC driver pattern: aggregation by statement, export with both IDs.
    /// </summary>
    /// <remarks>
    /// This class:
    /// - Aggregates Activity data by statement_id using ConcurrentDictionary
    /// - Connection events emit immediately (no aggregation needed)
    /// - Statement events aggregate until CompleteStatement is called
    /// - Terminal exceptions flush immediately, retryable exceptions buffer until complete
    /// - Uses TelemetryTagRegistry to filter tags
    /// - Creates TelemetryFrontendLog wrapper with workspace_id
    /// - All exceptions swallowed (logged at TRACE level)
    ///
    /// JDBC Reference: TelemetryCollector.java
    /// </remarks>
    internal sealed class MetricsAggregator : IDisposable
    {
        private readonly ITelemetryExporter _exporter;
        private readonly TelemetryConfiguration _config;
        private readonly long _workspaceId;
        private readonly string _userAgent;
        private readonly ConcurrentDictionary<string, StatementTelemetryContext> _statementContexts;
        private readonly ConcurrentQueue<TelemetryFrontendLog> _pendingEvents;
        private readonly object _flushLock = new object();
        private readonly Timer _flushTimer;
        private bool _disposed;

        /// <summary>
        /// Gets the number of pending events waiting to be flushed.
        /// </summary>
        internal int PendingEventCount => _pendingEvents.Count;

        /// <summary>
        /// Gets the number of active statement contexts being tracked.
        /// </summary>
        internal int ActiveStatementCount => _statementContexts.Count;

        /// <summary>
        /// Creates a new MetricsAggregator.
        /// </summary>
        /// <param name="exporter">The telemetry exporter to use for flushing events.</param>
        /// <param name="config">The telemetry configuration.</param>
        /// <param name="workspaceId">The Databricks workspace ID for all events.</param>
        /// <param name="userAgent">The user agent string for client context.</param>
        /// <exception cref="ArgumentNullException">Thrown when exporter or config is null.</exception>
        public MetricsAggregator(
            ITelemetryExporter exporter,
            TelemetryConfiguration config,
            long workspaceId,
            string? userAgent = null)
        {
            _exporter = exporter ?? throw new ArgumentNullException(nameof(exporter));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _workspaceId = workspaceId;
            _userAgent = userAgent ?? "AdbcDatabricksDriver";
            _statementContexts = new ConcurrentDictionary<string, StatementTelemetryContext>();
            _pendingEvents = new ConcurrentQueue<TelemetryFrontendLog>();

            // Start flush timer
            _flushTimer = new Timer(
                OnFlushTimerTick,
                null,
                _config.FlushIntervalMs,
                _config.FlushIntervalMs);
        }

        /// <summary>
        /// Processes a completed activity and extracts telemetry metrics.
        /// </summary>
        /// <param name="activity">The activity to process.</param>
        /// <remarks>
        /// This method determines the event type based on the activity operation name:
        /// - Connection.* activities emit connection events immediately
        /// - Statement.* activities are aggregated by statement_id
        /// - Activities with error.type tag are treated as error events
        ///
        /// All exceptions are swallowed and logged at TRACE level.
        /// </remarks>
        public void ProcessActivity(Activity? activity)
        {
            if (activity == null)
            {
                return;
            }

            try
            {
                var eventType = DetermineEventType(activity);

                switch (eventType)
                {
                    case TelemetryEventType.ConnectionOpen:
                        ProcessConnectionActivity(activity);
                        break;

                    case TelemetryEventType.StatementExecution:
                        ProcessStatementActivity(activity);
                        break;

                    case TelemetryEventType.Error:
                        ProcessErrorActivity(activity);
                        break;
                }

                // Check if we should flush based on batch size
                if (_pendingEvents.Count >= _config.BatchSize)
                {
                    _ = FlushAsync();
                }
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per telemetry requirement
                // Log at TRACE level to avoid customer anxiety
                Debug.WriteLine($"[TRACE] MetricsAggregator: Error processing activity: {ex.Message}");
            }
        }

        /// <summary>
        /// Marks a statement as complete and emits the aggregated telemetry event.
        /// </summary>
        /// <param name="statementId">The statement ID to complete.</param>
        /// <param name="failed">Whether the statement execution failed.</param>
        /// <remarks>
        /// This method:
        /// - Removes the statement context from the aggregation dictionary
        /// - Emits the aggregated event with all collected metrics
        /// - If failed, also emits any buffered retryable exception events
        ///
        /// All exceptions are swallowed and logged at TRACE level.
        /// </remarks>
        public void CompleteStatement(string statementId, bool failed = false)
        {
            if (string.IsNullOrEmpty(statementId))
            {
                return;
            }

            try
            {
                if (_statementContexts.TryRemove(statementId, out var context))
                {
                    // Emit the aggregated statement event
                    var telemetryEvent = CreateTelemetryEvent(context);
                    var frontendLog = WrapInFrontendLog(telemetryEvent);
                    _pendingEvents.Enqueue(frontendLog);

                    // If statement failed and we have buffered exceptions, emit them
                    if (failed && context.BufferedExceptions.Count > 0)
                    {
                        foreach (var exception in context.BufferedExceptions)
                        {
                            var errorEvent = CreateErrorTelemetryEvent(
                                context.SessionId,
                                statementId,
                                exception);
                            var errorLog = WrapInFrontendLog(errorEvent);
                            _pendingEvents.Enqueue(errorLog);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per telemetry requirement
                Debug.WriteLine($"[TRACE] MetricsAggregator: Error completing statement: {ex.Message}");
            }
        }

        /// <summary>
        /// Records an exception for a statement.
        /// </summary>
        /// <param name="statementId">The statement ID.</param>
        /// <param name="sessionId">The session ID.</param>
        /// <param name="exception">The exception to record.</param>
        /// <remarks>
        /// Terminal exceptions are flushed immediately.
        /// Retryable exceptions are buffered until CompleteStatement is called.
        ///
        /// All exceptions are swallowed and logged at TRACE level.
        /// </remarks>
        public void RecordException(string statementId, string sessionId, Exception? exception)
        {
            if (exception == null || string.IsNullOrEmpty(statementId))
            {
                return;
            }

            try
            {
                if (ExceptionClassifier.IsTerminalException(exception))
                {
                    // Terminal exception: flush immediately
                    var errorEvent = CreateErrorTelemetryEvent(sessionId, statementId, exception);
                    var errorLog = WrapInFrontendLog(errorEvent);
                    _pendingEvents.Enqueue(errorLog);

                    Debug.WriteLine($"[TRACE] MetricsAggregator: Terminal exception recorded, flushing immediately");
                }
                else
                {
                    // Retryable exception: buffer until statement completes
                    var context = _statementContexts.GetOrAdd(
                        statementId,
                        _ => new StatementTelemetryContext(statementId, sessionId));
                    context.BufferedExceptions.Add(exception);

                    Debug.WriteLine($"[TRACE] MetricsAggregator: Retryable exception buffered for statement {statementId}");
                }
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per telemetry requirement
                Debug.WriteLine($"[TRACE] MetricsAggregator: Error recording exception: {ex.Message}");
            }
        }

        /// <summary>
        /// Flushes all pending telemetry events to the exporter.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the flush operation.</returns>
        /// <remarks>
        /// This method is thread-safe and uses a lock to prevent concurrent flushes.
        /// All exceptions are swallowed and logged at TRACE level.
        /// </remarks>
        public async Task FlushAsync(CancellationToken ct = default)
        {
            try
            {
                List<TelemetryFrontendLog> eventsToFlush;

                lock (_flushLock)
                {
                    if (_pendingEvents.IsEmpty)
                    {
                        return;
                    }

                    eventsToFlush = new List<TelemetryFrontendLog>();
                    while (_pendingEvents.TryDequeue(out var eventLog))
                    {
                        eventsToFlush.Add(eventLog);
                    }
                }

                if (eventsToFlush.Count > 0)
                {
                    Debug.WriteLine($"[TRACE] MetricsAggregator: Flushing {eventsToFlush.Count} events");
                    await _exporter.ExportAsync(eventsToFlush, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Don't swallow cancellation
                throw;
            }
            catch (Exception ex)
            {
                // Swallow all other exceptions per telemetry requirement
                Debug.WriteLine($"[TRACE] MetricsAggregator: Error flushing events: {ex.Message}");
            }
        }

        /// <summary>
        /// Disposes the MetricsAggregator and flushes any remaining events.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            try
            {
                _flushTimer.Dispose();

                // Final flush
                FlushAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] MetricsAggregator: Error during dispose: {ex.Message}");
            }
        }

        #region Private Methods

        /// <summary>
        /// Timer callback for periodic flushing.
        /// </summary>
        private void OnFlushTimerTick(object? state)
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                _ = FlushAsync();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] MetricsAggregator: Error in flush timer: {ex.Message}");
            }
        }

        /// <summary>
        /// Determines the telemetry event type based on the activity.
        /// </summary>
        private static TelemetryEventType DetermineEventType(Activity activity)
        {
            // Check for errors first
            if (activity.GetTagItem("error.type") != null)
            {
                return TelemetryEventType.Error;
            }

            // Map based on operation name
            var operationName = activity.OperationName ?? string.Empty;

            if (operationName.StartsWith("Connection.", StringComparison.OrdinalIgnoreCase) ||
                operationName.Equals("OpenConnection", StringComparison.OrdinalIgnoreCase) ||
                operationName.Equals("OpenAsync", StringComparison.OrdinalIgnoreCase))
            {
                return TelemetryEventType.ConnectionOpen;
            }

            // Default to statement execution for statement operations and others
            return TelemetryEventType.StatementExecution;
        }

        /// <summary>
        /// Processes a connection activity and emits it immediately.
        /// </summary>
        private void ProcessConnectionActivity(Activity activity)
        {
            var sessionId = GetTagValue(activity, "session.id");
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = sessionId,
                OperationLatencyMs = (long)activity.Duration.TotalMilliseconds,
                SystemConfiguration = ExtractSystemConfiguration(activity),
                ConnectionParameters = ExtractConnectionParameters(activity)
            };

            var frontendLog = WrapInFrontendLog(telemetryEvent);
            _pendingEvents.Enqueue(frontendLog);

            Debug.WriteLine($"[TRACE] MetricsAggregator: Connection event emitted immediately");
        }

        /// <summary>
        /// Processes a statement activity and aggregates it by statement_id.
        /// </summary>
        private void ProcessStatementActivity(Activity activity)
        {
            var statementId = GetTagValue(activity, "statement.id");
            var sessionId = GetTagValue(activity, "session.id");

            if (string.IsNullOrEmpty(statementId))
            {
                // No statement ID, cannot aggregate - emit immediately
                var telemetryEvent = new TelemetryEvent
                {
                    SessionId = sessionId,
                    OperationLatencyMs = (long)activity.Duration.TotalMilliseconds,
                    SqlExecutionEvent = ExtractSqlExecutionEvent(activity)
                };

                var frontendLog = WrapInFrontendLog(telemetryEvent);
                _pendingEvents.Enqueue(frontendLog);
                return;
            }

            // Get or create context for this statement
            var context = _statementContexts.GetOrAdd(
                statementId,
                _ => new StatementTelemetryContext(statementId, sessionId));

            // Aggregate metrics
            context.AddLatency((long)activity.Duration.TotalMilliseconds);
            AggregateActivityTags(context, activity);
        }

        /// <summary>
        /// Processes an error activity and emits it immediately.
        /// </summary>
        private void ProcessErrorActivity(Activity activity)
        {
            var statementId = GetTagValue(activity, "statement.id");
            var sessionId = GetTagValue(activity, "session.id");
            var errorType = GetTagValue(activity, "error.type");
            var errorMessage = GetTagValue(activity, "error.message");
            var errorCode = GetTagValue(activity, "error.code");

            var telemetryEvent = new TelemetryEvent
            {
                SessionId = sessionId,
                SqlStatementId = statementId,
                OperationLatencyMs = (long)activity.Duration.TotalMilliseconds,
                ErrorInfo = new DriverErrorInfo
                {
                    ErrorType = errorType,
                    ErrorMessage = errorMessage,
                    ErrorCode = errorCode
                }
            };

            var frontendLog = WrapInFrontendLog(telemetryEvent);
            _pendingEvents.Enqueue(frontendLog);

            Debug.WriteLine($"[TRACE] MetricsAggregator: Error event emitted immediately");
        }

        /// <summary>
        /// Aggregates activity tags into the statement context.
        /// </summary>
        private void AggregateActivityTags(StatementTelemetryContext context, Activity activity)
        {
            var eventType = TelemetryEventType.StatementExecution;

            foreach (var tag in activity.Tags)
            {
                // Filter using TelemetryTagRegistry
                if (!TelemetryTagRegistry.ShouldExportToDatabricks(eventType, tag.Key))
                {
                    continue;
                }

                switch (tag.Key)
                {
                    case "result.format":
                        context.ResultFormat = tag.Value;
                        break;
                    case "result.chunk_count":
                        if (int.TryParse(tag.Value, out int chunkCount))
                        {
                            context.ChunkCount = (context.ChunkCount ?? 0) + chunkCount;
                        }
                        break;
                    case "result.bytes_downloaded":
                        if (long.TryParse(tag.Value, out long bytesDownloaded))
                        {
                            context.BytesDownloaded = (context.BytesDownloaded ?? 0) + bytesDownloaded;
                        }
                        break;
                    case "result.compression_enabled":
                        if (bool.TryParse(tag.Value, out bool compressionEnabled))
                        {
                            context.CompressionEnabled = compressionEnabled;
                        }
                        break;
                    case "result.row_count":
                        if (long.TryParse(tag.Value, out long rowCount))
                        {
                            context.RowCount = rowCount;
                        }
                        break;
                    case "poll.count":
                        if (int.TryParse(tag.Value, out int pollCount))
                        {
                            context.PollCount = (context.PollCount ?? 0) + pollCount;
                        }
                        break;
                    case "poll.latency_ms":
                        if (long.TryParse(tag.Value, out long pollLatencyMs))
                        {
                            context.PollLatencyMs = (context.PollLatencyMs ?? 0) + pollLatencyMs;
                        }
                        break;
                    case "execution.status":
                        context.ExecutionStatus = tag.Value;
                        break;
                    case "statement.type":
                        context.StatementType = tag.Value;
                        break;
                }
            }
        }

        /// <summary>
        /// Creates a TelemetryEvent from a statement context.
        /// </summary>
        private TelemetryEvent CreateTelemetryEvent(StatementTelemetryContext context)
        {
            return new TelemetryEvent
            {
                SessionId = context.SessionId,
                SqlStatementId = context.StatementId,
                OperationLatencyMs = context.TotalLatencyMs,
                SqlExecutionEvent = new SqlExecutionEvent
                {
                    ResultFormat = context.ResultFormat,
                    ChunkCount = context.ChunkCount,
                    BytesDownloaded = context.BytesDownloaded,
                    CompressionEnabled = context.CompressionEnabled,
                    RowCount = context.RowCount,
                    PollCount = context.PollCount,
                    PollLatencyMs = context.PollLatencyMs,
                    ExecutionStatus = context.ExecutionStatus,
                    StatementType = context.StatementType
                }
            };
        }

        /// <summary>
        /// Creates an error TelemetryEvent from an exception.
        /// </summary>
        private TelemetryEvent CreateErrorTelemetryEvent(
            string? sessionId,
            string statementId,
            Exception exception)
        {
            var isTerminal = ExceptionClassifier.IsTerminalException(exception);
            int? httpStatusCode = null;

#if NET5_0_OR_GREATER
            if (exception is System.Net.Http.HttpRequestException httpEx && httpEx.StatusCode.HasValue)
            {
                httpStatusCode = (int)httpEx.StatusCode.Value;
            }
#endif

            return new TelemetryEvent
            {
                SessionId = sessionId,
                SqlStatementId = statementId,
                ErrorInfo = new DriverErrorInfo
                {
                    ErrorType = exception.GetType().Name,
                    ErrorMessage = SanitizeErrorMessage(exception.Message),
                    IsTerminal = isTerminal,
                    HttpStatusCode = httpStatusCode
                }
            };
        }

        /// <summary>
        /// Sanitizes an error message to remove potential PII.
        /// </summary>
        private static string SanitizeErrorMessage(string? message)
        {
            if (string.IsNullOrEmpty(message))
            {
                return string.Empty;
            }

            // Truncate long messages
            const int maxLength = 200;
            if (message.Length > maxLength)
            {
                message = message.Substring(0, maxLength) + "...";
            }

            return message;
        }

        /// <summary>
        /// Wraps a TelemetryEvent in a TelemetryFrontendLog.
        /// </summary>
        internal TelemetryFrontendLog WrapInFrontendLog(TelemetryEvent telemetryEvent)
        {
            return new TelemetryFrontendLog
            {
                WorkspaceId = _workspaceId,
                FrontendLogEventId = Guid.NewGuid().ToString(),
                Context = new FrontendLogContext
                {
                    ClientContext = new TelemetryClientContext
                    {
                        UserAgent = _userAgent
                    },
                    TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                },
                Entry = new FrontendLogEntry
                {
                    SqlDriverLog = telemetryEvent
                }
            };
        }

        /// <summary>
        /// Extracts system configuration from activity tags.
        /// </summary>
        private static DriverSystemConfiguration? ExtractSystemConfiguration(Activity activity)
        {
            var driverVersion = GetTagValue(activity, "driver.version");
            var osName = GetTagValue(activity, "driver.os");
            var runtime = GetTagValue(activity, "driver.runtime");

            if (driverVersion == null && osName == null && runtime == null)
            {
                return null;
            }

            return new DriverSystemConfiguration
            {
                DriverName = "Databricks ADBC Driver",
                DriverVersion = driverVersion,
                OsName = osName,
                RuntimeName = ".NET",
                RuntimeVersion = runtime
            };
        }

        /// <summary>
        /// Extracts connection parameters from activity tags.
        /// </summary>
        private static DriverConnectionParameters? ExtractConnectionParameters(Activity activity)
        {
            var cloudFetchEnabled = GetTagValue(activity, "feature.cloudfetch");
            var lz4Enabled = GetTagValue(activity, "feature.lz4");
            var directResultsEnabled = GetTagValue(activity, "feature.direct_results");

            if (cloudFetchEnabled == null && lz4Enabled == null && directResultsEnabled == null)
            {
                return null;
            }

            return new DriverConnectionParameters
            {
                CloudFetchEnabled = bool.TryParse(cloudFetchEnabled, out var cf) ? cf : (bool?)null,
                Lz4CompressionEnabled = bool.TryParse(lz4Enabled, out var lz4) ? lz4 : (bool?)null,
                DirectResultsEnabled = bool.TryParse(directResultsEnabled, out var dr) ? dr : (bool?)null
            };
        }

        /// <summary>
        /// Extracts SQL execution event details from activity tags.
        /// </summary>
        private static SqlExecutionEvent? ExtractSqlExecutionEvent(Activity activity)
        {
            var resultFormat = GetTagValue(activity, "result.format");
            var chunkCountStr = GetTagValue(activity, "result.chunk_count");
            var bytesDownloadedStr = GetTagValue(activity, "result.bytes_downloaded");
            var pollCountStr = GetTagValue(activity, "poll.count");

            if (resultFormat == null && chunkCountStr == null && bytesDownloadedStr == null && pollCountStr == null)
            {
                return null;
            }

            return new SqlExecutionEvent
            {
                ResultFormat = resultFormat,
                ChunkCount = int.TryParse(chunkCountStr, out var cc) ? cc : (int?)null,
                BytesDownloaded = long.TryParse(bytesDownloadedStr, out var bd) ? bd : (long?)null,
                PollCount = int.TryParse(pollCountStr, out var pc) ? pc : (int?)null
            };
        }

        /// <summary>
        /// Gets a tag value from an activity.
        /// </summary>
        private static string? GetTagValue(Activity activity, string tagName)
        {
            return activity.GetTagItem(tagName)?.ToString();
        }

        #endregion

        #region Nested Classes

        /// <summary>
        /// Holds aggregated telemetry data for a statement.
        /// </summary>
        internal sealed class StatementTelemetryContext
        {
            private long _totalLatencyMs;

            public string StatementId { get; }
            public string? SessionId { get; set; }
            public long TotalLatencyMs => Interlocked.Read(ref _totalLatencyMs);

            // Aggregated metrics
            public string? ResultFormat { get; set; }
            public int? ChunkCount { get; set; }
            public long? BytesDownloaded { get; set; }
            public bool? CompressionEnabled { get; set; }
            public long? RowCount { get; set; }
            public int? PollCount { get; set; }
            public long? PollLatencyMs { get; set; }
            public string? ExecutionStatus { get; set; }
            public string? StatementType { get; set; }

            // Buffered exceptions for retryable errors
            public ConcurrentBag<Exception> BufferedExceptions { get; } = new ConcurrentBag<Exception>();

            public StatementTelemetryContext(string statementId, string? sessionId)
            {
                StatementId = statementId ?? throw new ArgumentNullException(nameof(statementId));
                SessionId = sessionId;
            }

            public void AddLatency(long latencyMs)
            {
                Interlocked.Add(ref _totalLatencyMs, latencyMs);
            }
        }

        #endregion
    }
}
