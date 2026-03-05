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
    /// Aggregates Activity data by statement_id and coordinates export of telemetry metrics.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the core orchestration component of the telemetry pipeline. It processes activities
    /// from the driver, aggregates statement-level metrics, and coordinates export through the
    /// circuit breaker-protected exporter.
    /// </para>
    /// <para>
    /// Key Behaviors:
    /// - Connection events: emitted immediately (no aggregation)
    /// - Statement events: aggregated by statement_id in ConcurrentDictionary
    /// - Error events: terminal errors flush immediately; retryable errors buffer until statement complete
    /// - Batch flushing: triggered by batch size threshold or time interval
    /// - Tag filtering: uses TelemetryTagRegistry to prevent sensitive data export
    /// - Exception handling: all exceptions swallowed and logged at TRACE level
    /// </para>
    /// <para>
    /// Thread Safety: All methods are thread-safe and can be called concurrently.
    /// </para>
    /// </remarks>
    internal sealed class MetricsAggregator : IAsyncDisposable
    {
        /// <summary>
        /// Activity source for tracing aggregator operations.
        /// </summary>
        private static readonly ActivitySource s_activitySource = new ActivitySource("AdbcDrivers.Databricks.MetricsAggregator");

        private readonly ITelemetryExporter _exporter;
        private readonly int _batchSize;
        private readonly TimeSpan _flushInterval;
        private readonly ConcurrentDictionary<string, StatementAggregation> _statementAggregations;
        private readonly ConcurrentQueue<TelemetryMetric> _pendingMetrics;
        private readonly Timer _flushTimer;
        private readonly SemaphoreSlim _flushLock;
        private volatile bool _disposed;
        private DateTimeOffset _lastFlushTime;

        /// <summary>
        /// Initializes a new instance of the <see cref="MetricsAggregator"/> class.
        /// </summary>
        /// <param name="exporter">The circuit breaker-protected telemetry exporter.</param>
        /// <param name="batchSize">The number of metrics to batch before triggering a flush (default: 100).</param>
        /// <param name="flushInterval">The time interval between automatic flushes (default: 5 seconds).</param>
        public MetricsAggregator(
            ITelemetryExporter exporter,
            int batchSize = 100,
            TimeSpan? flushInterval = null)
        {
            _exporter = exporter ?? throw new ArgumentNullException(nameof(exporter));

            if (batchSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, "Batch size must be positive.");
            }

            _batchSize = batchSize;
            _flushInterval = flushInterval ?? TimeSpan.FromSeconds(5);
            _statementAggregations = new ConcurrentDictionary<string, StatementAggregation>();
            _pendingMetrics = new ConcurrentQueue<TelemetryMetric>();
            _flushLock = new SemaphoreSlim(1, 1);
            _lastFlushTime = DateTimeOffset.UtcNow;
            _flushTimer = new Timer(OnFlushTimer, null, _flushInterval, _flushInterval);
        }

        /// <summary>
        /// Processes an Activity and extracts telemetry metrics.
        /// </summary>
        /// <param name="activity">The completed Activity to process.</param>
        /// <remarks>
        /// <para>
        /// This method extracts metrics from Activity tags and events, determines the event type
        /// (connection, statement, or error), and routes to the appropriate handler.
        /// </para>
        /// <para>
        /// All exceptions are swallowed and logged at TRACE level to ensure telemetry operations
        /// never impact driver functionality.
        /// </para>
        /// </remarks>
        public void ProcessActivity(Activity? activity)
        {
            if (activity == null || _disposed)
            {
                return;
            }

            try
            {
                // Extract event type from activity
                TelemetryEventType eventType = DetermineEventType(activity);

                // Filter tags using TelemetryTagRegistry
                Dictionary<string, object?> filteredTags = FilterTags(activity, eventType);

                // Route to appropriate handler based on event type
                switch (eventType)
                {
                    case TelemetryEventType.ConnectionOpen:
                        ProcessConnectionEvent(activity, filteredTags);
                        break;

                    case TelemetryEventType.StatementExecution:
                        ProcessStatementEvent(activity, filteredTags);
                        break;

                    case TelemetryEventType.Error:
                        ProcessErrorEvent(activity, filteredTags);
                        break;

                    default:
                        // Unknown event type - silently ignore
                        Activity.Current?.AddEvent(new ActivityEvent("telemetry.aggregator.unknown_event_type",
                            tags: new ActivityTagsCollection
                            {
                                { "activity.name", activity.DisplayName ?? activity.OperationName }
                            }));
                        break;
                }

                // Check if we need to flush due to batch size
                if (_pendingMetrics.Count >= _batchSize)
                {
                    _ = Task.Run(() => FlushAsync(CancellationToken.None));
                }
            }
            catch (Exception ex)
            {
                // Swallow all exceptions and log at TRACE level
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.aggregator.process_activity_error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));
            }
        }

        /// <summary>
        /// Marks a statement as complete and emits the aggregated metric.
        /// </summary>
        /// <param name="statementId">The statement ID to complete.</param>
        public void CompleteStatement(string statementId)
        {
            if (string.IsNullOrEmpty(statementId) || _disposed)
            {
                return;
            }

            try
            {
                if (_statementAggregations.TryRemove(statementId, out StatementAggregation? aggregation))
                {
                    TelemetryMetric metric = aggregation.BuildMetric();
                    _pendingMetrics.Enqueue(metric);

                    Activity.Current?.AddEvent(new ActivityEvent("telemetry.aggregator.statement_completed",
                        tags: new ActivityTagsCollection
                        {
                            { "statement_id", statementId },
                            { "pending_count", _pendingMetrics.Count }
                        }));
                }
            }
            catch (Exception ex)
            {
                // Swallow all exceptions
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.aggregator.complete_statement_error",
                    tags: new ActivityTagsCollection
                    {
                        { "statement_id", statementId },
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));
            }
        }

        /// <summary>
        /// Records an exception for a statement.
        /// </summary>
        /// <param name="statementId">The statement ID.</param>
        /// <param name="exception">The exception that occurred.</param>
        /// <remarks>
        /// Terminal exceptions trigger immediate flush. Retryable exceptions are buffered
        /// until the statement completes.
        /// </remarks>
        public void RecordException(string statementId, Exception exception)
        {
            if (string.IsNullOrEmpty(statementId) || exception == null || _disposed)
            {
                return;
            }

            try
            {
                bool isTerminal = ExceptionClassifier.IsTerminalException(exception);

                // Get or create aggregation for this statement
                StatementAggregation aggregation = _statementAggregations.GetOrAdd(
                    statementId,
                    _ => new StatementAggregation(statementId));

                // Record the exception
                aggregation.RecordException(exception);

                if (isTerminal)
                {
                    // Terminal exception - flush immediately
                    TelemetryMetric metric = aggregation.BuildMetric();
                    _pendingMetrics.Enqueue(metric);

                    // Remove from aggregations since we're flushing it
                    _statementAggregations.TryRemove(statementId, out _);

                    // Trigger immediate flush
                    _ = Task.Run(() => FlushAsync(CancellationToken.None));

                    Activity.Current?.AddEvent(new ActivityEvent("telemetry.aggregator.terminal_exception_flushed",
                        tags: new ActivityTagsCollection
                        {
                            { "statement_id", statementId },
                            { "error.type", exception.GetType().Name }
                        }));
                }
                else
                {
                    // Retryable exception - buffer until statement complete
                    Activity.Current?.AddEvent(new ActivityEvent("telemetry.aggregator.retryable_exception_buffered",
                        tags: new ActivityTagsCollection
                        {
                            { "statement_id", statementId },
                            { "error.type", exception.GetType().Name }
                        }));
                }
            }
            catch (Exception ex)
            {
                // Swallow all exceptions
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.aggregator.record_exception_error",
                    tags: new ActivityTagsCollection
                    {
                        { "statement_id", statementId },
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));
            }
        }

        /// <summary>
        /// Flushes all pending metrics to the exporter.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task that completes when the flush operation is done.</returns>
        public async Task FlushAsync(CancellationToken ct = default)
        {
            if (_disposed)
            {
                return;
            }

            // Use semaphore to prevent concurrent flushes
            if (!await _flushLock.WaitAsync(0, ct).ConfigureAwait(false))
            {
                // Another flush is already in progress
                return;
            }

            try
            {
                // Collect all pending metrics
                List<TelemetryMetric> metricsToFlush = new List<TelemetryMetric>();
                while (_pendingMetrics.TryDequeue(out TelemetryMetric? metric))
                {
                    metricsToFlush.Add(metric);
                }

                if (metricsToFlush.Count == 0)
                {
                    return;
                }

                // Convert TelemetryMetric to TelemetryFrontendLog
                List<TelemetryFrontendLog> logs = metricsToFlush.Select(m => ConvertToFrontendLog(m)).ToList();

                // Export through the circuit breaker-protected exporter
                bool success = await _exporter.ExportAsync(logs, ct).ConfigureAwait(false);

                _lastFlushTime = DateTimeOffset.UtcNow;

                Activity.Current?.AddEvent(new ActivityEvent("telemetry.aggregator.flush_completed",
                    tags: new ActivityTagsCollection
                    {
                        { "metric_count", metricsToFlush.Count },
                        { "success", success }
                    }));
            }
            catch (Exception ex)
            {
                // Swallow all exceptions
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.aggregator.flush_error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));
            }
            finally
            {
                _flushLock.Release();
            }
        }

        /// <summary>
        /// Disposes the aggregator and flushes any remaining metrics.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                // Stop the timer
                _flushTimer.Dispose();

                // Flush any remaining metrics BEFORE setting _disposed
                await FlushAsync(CancellationToken.None).ConfigureAwait(false);

                // Now mark as disposed
                _disposed = true;

                // Dispose the semaphore
                _flushLock.Dispose();
            }
            catch (Exception ex)
            {
                // Swallow all exceptions during disposal
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.aggregator.dispose_error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));
            }
        }

        /// <summary>
        /// Timer callback to trigger periodic flushes.
        /// </summary>
        private void OnFlushTimer(object? state)
        {
            if (_disposed)
            {
                return;
            }

            // Check if enough time has elapsed since last flush
            TimeSpan elapsed = DateTimeOffset.UtcNow - _lastFlushTime;
            if (elapsed >= _flushInterval && _pendingMetrics.Count > 0)
            {
                _ = Task.Run(() => FlushAsync(CancellationToken.None));
            }
        }

        /// <summary>
        /// Determines the event type from an Activity.
        /// </summary>
        private TelemetryEventType DetermineEventType(Activity activity)
        {
            string operationName = activity.OperationName ?? activity.DisplayName ?? string.Empty;

            // Check for error event (has exception or error.type tag)
            if (activity.GetTagItem("error.type") != null || activity.Events.Any(e => e.Name.IndexOf("exception", StringComparison.OrdinalIgnoreCase) >= 0))
            {
                return TelemetryEventType.Error;
            }

            // Check for connection event
            if (operationName.IndexOf("Connection", StringComparison.OrdinalIgnoreCase) >= 0 ||
                operationName.IndexOf("OpenAsync", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return TelemetryEventType.ConnectionOpen;
            }

            // Default to statement execution
            return TelemetryEventType.StatementExecution;
        }

        /// <summary>
        /// Filters Activity tags using TelemetryTagRegistry.
        /// </summary>
        private Dictionary<string, object?> FilterTags(Activity activity, TelemetryEventType eventType)
        {
            Dictionary<string, object?> filteredTags = new Dictionary<string, object?>();

            foreach (KeyValuePair<string, object?> tag in activity.TagObjects)
            {
                if (TelemetryTagRegistry.ShouldExportToDatabricks(eventType, tag.Key))
                {
                    filteredTags[tag.Key] = tag.Value;
                }
            }

            return filteredTags;
        }

        /// <summary>
        /// Processes a connection event (emit immediately).
        /// </summary>
        private void ProcessConnectionEvent(Activity activity, Dictionary<string, object?> tags)
        {
            TelemetryMetric metric = new TelemetryMetric
            {
                MetricType = MetricType.Connection,
                Timestamp = DateTimeOffset.UtcNow,
                WorkspaceId = GetTagValue<long>(tags, "workspace.id"),
                SessionId = GetTagValue<string>(tags, "session.id"),
                DriverConfiguration = ExtractDriverConfiguration(tags)
            };

            _pendingMetrics.Enqueue(metric);

            Activity.Current?.AddEvent(new ActivityEvent("telemetry.aggregator.connection_event_queued",
                tags: new ActivityTagsCollection
                {
                    { "session_id", metric.SessionId ?? "(none)" }
                }));
        }

        /// <summary>
        /// Processes a statement event (aggregate by statement_id).
        /// </summary>
        private void ProcessStatementEvent(Activity activity, Dictionary<string, object?> tags)
        {
            string? statementId = GetTagValue<string>(tags, "statement.id");
            if (string.IsNullOrEmpty(statementId))
            {
                // No statement ID - cannot aggregate
                return;
            }

            // Get or create aggregation for this statement
            StatementAggregation aggregation = _statementAggregations.GetOrAdd(
                statementId,
                _ => new StatementAggregation(statementId));

            // Update aggregation with activity data
            aggregation.UpdateFromActivity(activity, tags);
        }

        /// <summary>
        /// Processes an error event.
        /// </summary>
        private void ProcessErrorEvent(Activity activity, Dictionary<string, object?> tags)
        {
            string? statementId = GetTagValue<string>(tags, "statement.id");
            if (string.IsNullOrEmpty(statementId))
            {
                // No statement ID - emit error event immediately
                TelemetryMetric metric = new TelemetryMetric
                {
                    MetricType = MetricType.Error,
                    Timestamp = DateTimeOffset.UtcNow,
                    WorkspaceId = GetTagValue<long>(tags, "workspace.id"),
                    SessionId = GetTagValue<string>(tags, "session.id"),
                    StatementId = null
                };

                _pendingMetrics.Enqueue(metric);
                return;
            }

            // Extract exception from activity
            Exception? exception = null;
            ActivityEvent? exceptionEvent = activity.Events.FirstOrDefault(e => e.Name.IndexOf("exception", StringComparison.OrdinalIgnoreCase) >= 0);
            if (exceptionEvent != null)
            {
                string? errorType = exceptionEvent.Value.Tags.FirstOrDefault(t => t.Key == "error.type").Value?.ToString();
                string? errorMessage = exceptionEvent.Value.Tags.FirstOrDefault(t => t.Key == "error.message").Value?.ToString();

                if (!string.IsNullOrEmpty(errorType))
                {
                    // Create a synthetic exception for classification
                    exception = new Exception(errorMessage ?? errorType);
                }
            }

            if (exception != null)
            {
                RecordException(statementId, exception);
            }
        }

        /// <summary>
        /// Extracts driver configuration from tags.
        /// </summary>
        private DriverConfiguration? ExtractDriverConfiguration(Dictionary<string, object?> tags)
        {
            DriverConfiguration? config = new DriverConfiguration
            {
                DriverVersion = GetTagValue<string>(tags, "driver.version"),
                DriverName = GetTagValue<string>(tags, "driver.name"),
                RuntimeName = GetTagValue<string>(tags, "runtime.name"),
                RuntimeVersion = GetTagValue<string>(tags, "runtime.version"),
                OsName = GetTagValue<string>(tags, "os.name"),
                OsVersion = GetTagValue<string>(tags, "os.version"),
                OsArchitecture = GetTagValue<string>(tags, "os.architecture"),
                HttpPath = GetTagValue<string>(tags, "http_path"),
                HostUrl = GetTagValue<string>(tags, "host_url"),
                CloudFetchEnabled = GetTagValue<bool?>(tags, "cloud_fetch_enabled"),
                DirectResultsEnabled = GetTagValue<bool?>(tags, "direct_results_enabled")
            };

            // Only return config if at least one field is populated
            if (config.DriverVersion != null || config.DriverName != null || config.RuntimeName != null)
            {
                return config;
            }

            return null;
        }

        /// <summary>
        /// Gets a typed tag value from the tags dictionary.
        /// </summary>
        private T? GetTagValue<T>(Dictionary<string, object?> tags, string key)
        {
            if (tags.TryGetValue(key, out object? value) && value != null)
            {
                try
                {
                    if (value is T typedValue)
                    {
                        return typedValue;
                    }

                    // Try to convert
                    return (T?)Convert.ChangeType(value, typeof(T));
                }
                catch
                {
                    // Conversion failed - return default
                    return default;
                }
            }

            return default;
        }

        /// <summary>
        /// Converts a TelemetryMetric to a TelemetryFrontendLog.
        /// </summary>
        private TelemetryFrontendLog ConvertToFrontendLog(TelemetryMetric metric)
        {
            // For now, create a simple frontend log structure
            // This will be enhanced when we integrate with the full telemetry pipeline
            return new TelemetryFrontendLog
            {
                WorkspaceId = metric.WorkspaceId,
                FrontendLogEventId = Guid.NewGuid().ToString(),
                Context = new FrontendLogContext
                {
                    // Populate context as needed
                },
                Entry = new FrontendLogEntry
                {
                    // Populate entry as needed
                }
            };
        }

        /// <summary>
        /// Holds aggregated data for a single statement execution.
        /// </summary>
        private sealed class StatementAggregation
        {
            private readonly string _statementId;
            private long _workspaceId;
            private string? _sessionId;
            private long? _executionLatencyMs;
            private string? _resultFormat;
            private int? _chunkCount;
            private long? _totalBytesDownloaded;
            private int? _pollCount;
            private long? _pollLatencyMs;
            private DriverConfiguration? _driverConfiguration;
            private readonly List<Exception> _exceptions = new List<Exception>();

            public StatementAggregation(string statementId)
            {
                _statementId = statementId;
            }

            public void UpdateFromActivity(Activity activity, Dictionary<string, object?> tags)
            {
                // Extract and aggregate values from activity
                _workspaceId = GetTagValue<long>(tags, "workspace.id");
                _sessionId = GetTagValue<string>(tags, "session.id");

                // Execution latency from activity duration
                if (activity.Duration.TotalMilliseconds > 0)
                {
                    _executionLatencyMs = (_executionLatencyMs ?? 0) + (long)activity.Duration.TotalMilliseconds;
                }

                // Result format
                string? format = GetTagValue<string>(tags, "result.format");
                if (!string.IsNullOrEmpty(format))
                {
                    _resultFormat = format;
                }

                // Chunk count
                int? chunkCount = GetTagValue<int?>(tags, "result.chunk_count");
                if (chunkCount.HasValue)
                {
                    _chunkCount = (_chunkCount ?? 0) + chunkCount.Value;
                }

                // Bytes downloaded
                long? bytesDownloaded = GetTagValue<long?>(tags, "result.bytes_downloaded");
                if (bytesDownloaded.HasValue)
                {
                    _totalBytesDownloaded = (_totalBytesDownloaded ?? 0) + bytesDownloaded.Value;
                }

                // Poll count
                int? pollCount = GetTagValue<int?>(tags, "poll.count");
                if (pollCount.HasValue)
                {
                    _pollCount = (_pollCount ?? 0) + pollCount.Value;
                }

                // Poll latency
                long? pollLatency = GetTagValue<long?>(tags, "poll.latency_ms");
                if (pollLatency.HasValue)
                {
                    _pollLatencyMs = (_pollLatencyMs ?? 0) + pollLatency.Value;
                }

                // Driver configuration (only set once)
                if (_driverConfiguration == null)
                {
                    _driverConfiguration = ExtractDriverConfiguration(tags);
                }
            }

            public void RecordException(Exception exception)
            {
                _exceptions.Add(exception);
            }

            public TelemetryMetric BuildMetric()
            {
                return new TelemetryMetric
                {
                    MetricType = MetricType.Statement,
                    Timestamp = DateTimeOffset.UtcNow,
                    WorkspaceId = _workspaceId,
                    SessionId = _sessionId,
                    StatementId = _statementId,
                    ExecutionLatencyMs = _executionLatencyMs,
                    ResultFormat = _resultFormat,
                    ChunkCount = _chunkCount,
                    TotalBytesDownloaded = _totalBytesDownloaded,
                    PollCount = _pollCount,
                    PollLatencyMs = _pollLatencyMs,
                    DriverConfiguration = _driverConfiguration
                };
            }

            private T? GetTagValue<T>(Dictionary<string, object?> tags, string key)
            {
                if (tags.TryGetValue(key, out object? value) && value != null)
                {
                    try
                    {
                        if (value is T typedValue)
                        {
                            return typedValue;
                        }

                        return (T?)Convert.ChangeType(value, typeof(T));
                    }
                    catch
                    {
                        return default;
                    }
                }

                return default;
            }

            private DriverConfiguration? ExtractDriverConfiguration(Dictionary<string, object?> tags)
            {
                DriverConfiguration? config = new DriverConfiguration
                {
                    DriverVersion = GetTagValue<string>(tags, "driver.version"),
                    DriverName = GetTagValue<string>(tags, "driver.name"),
                    RuntimeName = GetTagValue<string>(tags, "runtime.name"),
                    RuntimeVersion = GetTagValue<string>(tags, "runtime.version"),
                    OsName = GetTagValue<string>(tags, "os.name"),
                    OsVersion = GetTagValue<string>(tags, "os.version"),
                    OsArchitecture = GetTagValue<string>(tags, "os.architecture"),
                    HttpPath = GetTagValue<string>(tags, "http_path"),
                    HostUrl = GetTagValue<string>(tags, "host_url"),
                    CloudFetchEnabled = GetTagValue<bool?>(tags, "cloud_fetch_enabled"),
                    DirectResultsEnabled = GetTagValue<bool?>(tags, "direct_results_enabled")
                };

                if (config.DriverVersion != null || config.DriverName != null || config.RuntimeName != null)
                {
                    return config;
                }

                return null;
            }
        }
    }
}
