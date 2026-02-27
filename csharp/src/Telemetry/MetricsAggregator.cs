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
using AdbcDrivers.Databricks.Telemetry.Proto;
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
    /// - All exceptions swallowed (traced via ActivitySource for testing)
    ///
    /// JDBC Reference: TelemetryCollector.java
    /// </remarks>
    internal sealed class MetricsAggregator : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// Internal ActivitySource for emitting trace events. Tests can subscribe to this.
        /// </summary>
        internal static readonly ActivitySource TraceSource = new ActivitySource("AdbcDrivers.Databricks.Telemetry.MetricsAggregator");

        private readonly ITelemetryExporter _exporter;
        private readonly TelemetryConfiguration _config;
        private readonly long _workspaceId;
        private readonly string _userAgent;
        private readonly ConcurrentDictionary<string, StatementTelemetryContext> _statementContexts;
        private readonly ConcurrentQueue<TelemetryFrontendLog> _pendingEvents;
        private readonly object _flushLock = new object();
        private readonly Timer _flushTimer;
        private volatile bool _disposed;

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
        /// <exception cref="ArgumentNullException">Thrown when exporter, config, or userAgent is null.</exception>
        public MetricsAggregator(
            ITelemetryExporter exporter,
            TelemetryConfiguration config,
            long workspaceId,
            string userAgent)
        {
            _exporter = exporter ?? throw new ArgumentNullException(nameof(exporter));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _userAgent = userAgent ?? throw new ArgumentNullException(nameof(userAgent));
            _workspaceId = workspaceId;
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
                    // Fire-and-forget flush - exceptions are handled inside FlushAsync
                    // Use ContinueWith to ensure the task is observed to avoid unobserved task exceptions
                    FlushAsync().ContinueWith(
                        _ => { },
                        TaskContinuationOptions.ExecuteSynchronously);
                }
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per telemetry requirement
                // Emit trace event for testing/diagnostics
                Activity.Current?.AddEvent(new ActivityEvent("MetricsAggregator.ProcessActivity.Error",
                    tags: new ActivityTagsCollection { { "error.message", ex.Message } }));
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
                Activity.Current?.AddEvent(new ActivityEvent("MetricsAggregator.CompleteStatement.Error",
                    tags: new ActivityTagsCollection { { "error.message", ex.Message } }));
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

                    Activity.Current?.AddEvent(new ActivityEvent("MetricsAggregator.TerminalExceptionRecorded"));
                }
                else
                {
                    // Retryable exception: buffer until statement completes
                    var context = _statementContexts.GetOrAdd(
                        statementId,
                        _ => new StatementTelemetryContext(statementId, sessionId));
                    context.BufferedExceptions.Add(exception);

                    Activity.Current?.AddEvent(new ActivityEvent("MetricsAggregator.RetryableExceptionBuffered",
                        tags: new ActivityTagsCollection { { "statement.id", statementId } }));
                }
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per telemetry requirement
                Activity.Current?.AddEvent(new ActivityEvent("MetricsAggregator.RecordException.Error",
                    tags: new ActivityTagsCollection { { "error.message", ex.Message } }));
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
                    Activity.Current?.AddEvent(new ActivityEvent("MetricsAggregator.Flushing",
                        tags: new ActivityTagsCollection { { "event.count", eventsToFlush.Count } }));
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
                Activity.Current?.AddEvent(new ActivityEvent("MetricsAggregator.FlushAsync.Error",
                    tags: new ActivityTagsCollection { { "error.message", ex.Message } }));
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

                // Final flush - use Task.Run to avoid deadlock in SynchronizationContext-aware environments
                Task.Run(() => FlushAsync()).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                // Swallow exceptions during dispose per telemetry requirement
                Activity.Current?.AddEvent(new ActivityEvent("MetricsAggregator.Dispose.Error",
                    tags: new ActivityTagsCollection { { "error.message", ex.Message } }));
            }
        }

        /// <summary>
        /// Asynchronously disposes the MetricsAggregator and flushes any remaining events.
        /// Preferred over Dispose() to avoid sync-over-async patterns.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            try
            {
                _flushTimer.Dispose();
                await FlushAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Swallow exceptions during dispose per telemetry requirement
                Activity.Current?.AddEvent(new ActivityEvent("MetricsAggregator.DisposeAsync.Error",
                    tags: new ActivityTagsCollection { { "error.message", ex.Message } }));
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
                // Fire-and-forget flush - exceptions are handled inside FlushAsync
                // Use ContinueWith to ensure the task is observed to avoid unobserved task exceptions
                FlushAsync().ContinueWith(
                    _ => { },
                    TaskContinuationOptions.ExecuteSynchronously);
            }
            catch (Exception ex)
            {
                // Swallow exceptions in timer per telemetry requirement
                Activity.Current?.AddEvent(new ActivityEvent("MetricsAggregator.FlushTimer.Error",
                    tags: new ActivityTagsCollection { { "error.message", ex.Message } }));
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
            var telemetryLog = new OssSqlDriverTelemetryLog
            {
                SessionId = sessionId ?? string.Empty,
                OperationLatencyMs = (long)activity.Duration.TotalMilliseconds
            };

            var systemConfig = ExtractSystemConfiguration(activity);
            if (systemConfig != null)
            {
                telemetryLog.SystemConfiguration = systemConfig;
            }

            var connectionParams = ExtractConnectionParameters(activity);
            if (connectionParams != null)
            {
                telemetryLog.DriverConnectionParams = connectionParams;
            }

            var frontendLog = WrapInFrontendLog(telemetryLog);
            _pendingEvents.Enqueue(frontendLog);

            Activity.Current?.AddEvent(new ActivityEvent("MetricsAggregator.ConnectionEventEmitted"));
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
                var telemetryLog = new OssSqlDriverTelemetryLog
                {
                    SessionId = sessionId ?? string.Empty,
                    OperationLatencyMs = (long)activity.Duration.TotalMilliseconds
                };

                var sqlOperation = ExtractSqlExecutionEvent(activity);
                if (sqlOperation != null)
                {
                    telemetryLog.SqlOperation = sqlOperation;
                }

                var frontendLog = WrapInFrontendLog(telemetryLog);
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

            var telemetryLog = new OssSqlDriverTelemetryLog
            {
                SessionId = sessionId ?? string.Empty,
                SqlStatementId = statementId ?? string.Empty,
                OperationLatencyMs = (long)activity.Duration.TotalMilliseconds,
                ErrorInfo = new DriverErrorInfo
                {
                    ErrorName = errorType ?? string.Empty
                }
            };

            var frontendLog = WrapInFrontendLog(telemetryLog);
            _pendingEvents.Enqueue(frontendLog);

            Activity.Current?.AddEvent(new ActivityEvent("MetricsAggregator.ErrorEventEmitted"));
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
                            context.AddChunkCount(chunkCount);
                        }
                        break;
                    case "result.bytes_downloaded":
                        if (long.TryParse(tag.Value, out long bytesDownloaded))
                        {
                            context.AddBytesDownloaded(bytesDownloaded);
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
                            context.AddPollCount(pollCount);
                        }
                        break;
                    case "poll.latency_ms":
                        if (long.TryParse(tag.Value, out long pollLatencyMs))
                        {
                            context.AddPollLatencyMs(pollLatencyMs);
                        }
                        break;
                    case "chunk.initial_latency_ms":
                        if (long.TryParse(tag.Value, out long initialLatency))
                        {
                            context.SetInitialChunkLatency(initialLatency);
                        }
                        break;
                    case "chunk.slowest_latency_ms":
                        if (long.TryParse(tag.Value, out long slowestLatency))
                        {
                            context.UpdateSlowestChunkLatency(slowestLatency);
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

            // Process Activity events (like cloudfetch.download_summary)
            ProcessActivityEvents(context, activity);
        }

        /// <summary>
        /// Processes Activity events to extract telemetry metrics.
        /// </summary>
        private void ProcessActivityEvents(StatementTelemetryContext context, Activity activity)
        {
            foreach (var activityEvent in activity.Events)
            {
                if (activityEvent.Name == "cloudfetch.download_summary")
                {
                    foreach (var tag in activityEvent.Tags)
                    {
                        switch (tag.Key)
                        {
                            case "initial_chunk_latency_ms":
                                if (tag.Value is long initialLatencyLong)
                                {
                                    context.SetInitialChunkLatency(initialLatencyLong);
                                }
                                else if (tag.Value is int initialLatencyInt)
                                {
                                    context.SetInitialChunkLatency(initialLatencyInt);
                                }
                                break;

                            case "slowest_chunk_latency_ms":
                                if (tag.Value is long slowestLatencyLong)
                                {
                                    context.UpdateSlowestChunkLatency(slowestLatencyLong);
                                }
                                else if (tag.Value is int slowestLatencyInt)
                                {
                                    context.UpdateSlowestChunkLatency(slowestLatencyInt);
                                }
                                break;

                            case "total_files":
                                if (tag.Value is int totalFiles)
                                {
                                    context.AddChunkCount(totalFiles);
                                }
                                break;

                            case "total_bytes":
                                if (tag.Value is long totalBytes)
                                {
                                    context.AddBytesDownloaded(totalBytes);
                                }
                                break;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Creates a TelemetryLog from a statement context.
        /// </summary>
        private OssSqlDriverTelemetryLog CreateTelemetryEvent(StatementTelemetryContext context)
        {
            var telemetryLog = new OssSqlDriverTelemetryLog
            {
                SessionId = context.SessionId ?? string.Empty,
                SqlStatementId = context.StatementId,
                OperationLatencyMs = context.TotalLatencyMs,
                SqlOperation = new SqlExecutionEvent
                {
                    IsCompressed = context.CompressionEnabled ?? false,
                    ChunkDetails = new ChunkDetails
                    {
                        TotalChunksPresent = context.ChunkCount ?? 0,
                        SumChunksDownloadTimeMillis = context.BytesDownloaded ?? 0,
                        InitialChunkLatencyMillis = context.InitialChunkLatencyMs ?? 0,
                        SlowestChunkLatencyMillis = context.SlowestChunkLatencyMs ?? 0
                    },
                    OperationDetail = new OperationDetail
                    {
                        NOperationStatusCalls = context.PollCount ?? 0,
                        OperationStatusLatencyMillis = context.PollLatencyMs ?? 0
                    }
                }
            };

            // Map result format to ExecutionResultFormat enum
            if (!string.IsNullOrEmpty(context.ResultFormat))
            {
                telemetryLog.SqlOperation.ExecutionResult = context.ResultFormat.ToLowerInvariant() switch
                {
                    "cloudfetch" or "external_links" => ExecutionResultFormat.ExecutionResultExternalLinks,
                    "arrow" or "inline_arrow" => ExecutionResultFormat.ExecutionResultInlineArrow,
                    "json" or "inline_json" => ExecutionResultFormat.ExecutionResultInlineJson,
                    _ => ExecutionResultFormat.Unspecified
                };
            }

            // Map statement type to StatementType enum
            if (!string.IsNullOrEmpty(context.StatementType))
            {
                telemetryLog.SqlOperation.StatementType = context.StatementType.ToLowerInvariant() switch
                {
                    "query" => StatementType.StatementQuery,
                    "sql" => StatementType.StatementSql,
                    "update" => StatementType.StatementUpdate,
                    "metadata" => StatementType.StatementMetadata,
                    _ => StatementType.Unspecified
                };
            }

            return telemetryLog;
        }

        /// <summary>
        /// Creates an error TelemetryLog from an exception.
        /// </summary>
        private OssSqlDriverTelemetryLog CreateErrorTelemetryEvent(
            string? sessionId,
            string statementId,
            Exception exception)
        {
            // Note: Proto DriverErrorInfo only has ErrorName and StackTrace
            // HttpStatusCode and IsTerminal are not part of the proto schema
            return new OssSqlDriverTelemetryLog
            {
                SessionId = sessionId ?? string.Empty,
                SqlStatementId = statementId,
                ErrorInfo = new DriverErrorInfo
                {
                    ErrorName = exception.GetType().Name,
                    StackTrace = TruncateErrorMessage(exception.Message)
                }
            };
        }

        /// <summary>
        /// Truncates an error message to a maximum length.
        /// Note: This method only truncates; full PII scrubbing is the caller's responsibility.
        /// </summary>
        private static string TruncateErrorMessage(string? message)
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
        /// Wraps a TelemetryLog in a TelemetryFrontendLog.
        /// </summary>
        internal TelemetryFrontendLog WrapInFrontendLog(OssSqlDriverTelemetryLog telemetryLog)
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
                    SqlDriverLog = telemetryLog
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
                DriverVersion = driverVersion ?? string.Empty,
                OsName = osName ?? string.Empty,
                RuntimeName = ".NET",
                RuntimeVersion = runtime ?? string.Empty
            };
        }

        /// <summary>
        /// Extracts connection parameters from activity tags.
        /// </summary>
        private static DriverConnectionParameters? ExtractConnectionParameters(Activity activity)
        {
            var directResultsEnabled = GetTagValue(activity, "feature.direct_results");
            var arrowEnabled = GetTagValue(activity, "feature.arrow");

            if (directResultsEnabled == null && arrowEnabled == null)
            {
                return null;
            }

            var parameters = new DriverConnectionParameters();

            if (bool.TryParse(directResultsEnabled, out var dr))
            {
                parameters.EnableDirectResults = dr;
            }

            if (bool.TryParse(arrowEnabled, out var arrow))
            {
                parameters.EnableArrow = arrow;
            }

            return parameters;
        }

        /// <summary>
        /// Extracts SQL execution event details from activity tags.
        /// </summary>
        private static SqlExecutionEvent? ExtractSqlExecutionEvent(Activity activity)
        {
            var resultFormat = GetTagValue(activity, "result.format");
            var chunkCountStr = GetTagValue(activity, "result.chunk_count");
            var pollCountStr = GetTagValue(activity, "poll.count");

            if (resultFormat == null && chunkCountStr == null && pollCountStr == null)
            {
                return null;
            }

            var sqlEvent = new SqlExecutionEvent();

            // Map result format to ExecutionResultFormat enum
            if (!string.IsNullOrEmpty(resultFormat))
            {
                sqlEvent.ExecutionResult = resultFormat.ToLowerInvariant() switch
                {
                    "cloudfetch" or "external_links" => ExecutionResultFormat.ExecutionResultExternalLinks,
                    "arrow" or "inline_arrow" => ExecutionResultFormat.ExecutionResultInlineArrow,
                    "json" or "inline_json" => ExecutionResultFormat.ExecutionResultInlineJson,
                    _ => ExecutionResultFormat.Unspecified
                };
            }

            if (int.TryParse(chunkCountStr, out var cc))
            {
                sqlEvent.ChunkDetails = new ChunkDetails { TotalChunksPresent = cc };
            }

            if (int.TryParse(pollCountStr, out var pc))
            {
                sqlEvent.OperationDetail = new OperationDetail { NOperationStatusCalls = pc };
            }

            return sqlEvent;
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

            // Backing fields for thread-safe accumulating metrics using Interlocked
            private int _chunkCount;
            private long _bytesDownloaded;
            private int _pollCount;
            private long _pollLatencyMs;
            private long _initialChunkLatencyMs = -1;
            private long _slowestChunkLatencyMs;
            private volatile bool _hasChunkCount;
            private volatile bool _hasBytesDownloaded;
            private volatile bool _hasPollCount;
            private volatile bool _hasPollLatencyMs;
            private volatile bool _hasInitialChunkLatency;
            private volatile bool _hasSlowestChunkLatency;

            public string StatementId { get; }
            public string? SessionId { get; set; }
            public long TotalLatencyMs => Interlocked.Read(ref _totalLatencyMs);

            // Non-accumulating metrics (last value wins)
            public string? ResultFormat { get; set; }
            public bool? CompressionEnabled { get; set; }
            public long? RowCount { get; set; }
            public string? ExecutionStatus { get; set; }
            public string? StatementType { get; set; }

            // Thread-safe accumulating metrics using Interlocked
            public int? ChunkCount => _hasChunkCount ? Interlocked.CompareExchange(ref _chunkCount, 0, 0) : (int?)null;
            public long? BytesDownloaded => _hasBytesDownloaded ? Interlocked.Read(ref _bytesDownloaded) : (long?)null;
            public int? PollCount => _hasPollCount ? Interlocked.CompareExchange(ref _pollCount, 0, 0) : (int?)null;
            public long? PollLatencyMs => _hasPollLatencyMs ? Interlocked.Read(ref _pollLatencyMs) : (long?)null;
            public long? InitialChunkLatencyMs => _hasInitialChunkLatency ? Interlocked.Read(ref _initialChunkLatencyMs) : (long?)null;
            public long? SlowestChunkLatencyMs => _hasSlowestChunkLatency ? Interlocked.Read(ref _slowestChunkLatencyMs) : (long?)null;

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

            public void AddChunkCount(int count)
            {
                Interlocked.Add(ref _chunkCount, count);
                _hasChunkCount = true;
            }

            public void AddBytesDownloaded(long bytes)
            {
                Interlocked.Add(ref _bytesDownloaded, bytes);
                _hasBytesDownloaded = true;
            }

            public void AddPollCount(int count)
            {
                Interlocked.Add(ref _pollCount, count);
                _hasPollCount = true;
            }

            public void AddPollLatencyMs(long latencyMs)
            {
                Interlocked.Add(ref _pollLatencyMs, latencyMs);
                _hasPollLatencyMs = true;
            }

            /// <summary>
            /// Sets the initial chunk latency. Only the first call has effect.
            /// </summary>
            public void SetInitialChunkLatency(long latencyMs)
            {
                // Only set once (first chunk)
                if (!_hasInitialChunkLatency && latencyMs >= 0)
                {
                    Interlocked.Exchange(ref _initialChunkLatencyMs, latencyMs);
                    _hasInitialChunkLatency = true;
                }
            }

            /// <summary>
            /// Updates the slowest chunk latency if the new value is greater.
            /// Thread-safe using Interlocked.
            /// </summary>
            public void UpdateSlowestChunkLatency(long latencyMs)
            {
                if (latencyMs <= 0) return;

                // Thread-safe max update using compare-and-swap
                long current;
                do
                {
                    current = Interlocked.Read(ref _slowestChunkLatencyMs);
                    if (latencyMs <= current) return;
                } while (Interlocked.CompareExchange(ref _slowestChunkLatencyMs, latencyMs, current) != current);

                _hasSlowestChunkLatency = true;
            }
        }

        #endregion
    }
}
