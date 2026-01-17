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
using AdbcDrivers.Databricks.Telemetry.TagDefinitions;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Aggregates metrics from activities by statement_id and includes session_id.
    /// Follows JDBC driver pattern: aggregation by statement, export with both IDs.
    /// All exceptions are swallowed and logged at TRACE level only.
    /// </summary>
    internal sealed class MetricsAggregator : IDisposable
    {
        private readonly ITelemetryExporter _exporter;
        private readonly TelemetryConfiguration _config;
        private readonly ConcurrentDictionary<string, StatementTelemetryDetails> _statementContexts;
        private readonly List<TelemetryMetric> _pendingMetrics;
        private readonly SemaphoreSlim _flushLock;
        private readonly Timer _flushTimer;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the MetricsAggregator class.
        /// </summary>
        /// <param name="exporter">The telemetry exporter to use for flushing metrics.</param>
        /// <param name="config">The telemetry configuration.</param>
        public MetricsAggregator(ITelemetryExporter exporter, TelemetryConfiguration config)
        {
            _exporter = exporter ?? throw new ArgumentNullException(nameof(exporter));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _statementContexts = new ConcurrentDictionary<string, StatementTelemetryDetails>();
            _pendingMetrics = new List<TelemetryMetric>();
            _flushLock = new SemaphoreSlim(1, 1);

            // Start periodic flush timer
            _flushTimer = new Timer(
                _ => _ = FlushAsync().ConfigureAwait(false),
                null,
                TimeSpan.FromMilliseconds(_config.FlushIntervalMs),
                TimeSpan.FromMilliseconds(_config.FlushIntervalMs));
        }

        /// <summary>
        /// Processes a completed activity and extracts metrics.
        /// Connection events are emitted immediately.
        /// Statement events are aggregated by statement_id until CompleteStatement() is called.
        /// All exceptions are swallowed and logged at TRACE level.
        /// </summary>
        /// <param name="activity">The completed activity to process.</param>
        public void ProcessActivity(Activity activity)
        {
            if (activity == null)
            {
                return;
            }

            try
            {
                var eventType = DetermineEventType(activity);

                // Connection events: emit immediately (no aggregation)
                if (eventType == TelemetryEventType.ConnectionOpen)
                {
                    var metric = CreateMetricFromActivity(activity, eventType);
                    EmitMetricImmediately(metric);
                    return;
                }

                // Statement events: aggregate by statement_id
                if (eventType == TelemetryEventType.StatementExecution)
                {
                    AggregateStatementActivity(activity);
                    return;
                }

                // Error events are handled separately via RecordException
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per requirement
                Debug.WriteLine($"[TRACE] Telemetry aggregator error in ProcessActivity: {ex.Message}");
            }
        }

        /// <summary>
        /// Records an exception for a statement.
        /// Terminal exceptions are flushed immediately.
        /// Retryable exceptions are buffered until statement completes.
        /// All exceptions are swallowed and logged at TRACE level.
        /// </summary>
        /// <param name="statementId">The statement ID.</param>
        /// <param name="ex">The exception to record.</param>
        public void RecordException(string statementId, Exception ex)
        {
            if (string.IsNullOrEmpty(statementId) || ex == null)
            {
                return;
            }

            try
            {
                if (ExceptionClassifier.IsTerminalException(ex))
                {
                    // Terminal exception: flush immediately
                    var errorMetric = CreateErrorMetric(statementId, ex);
                    EmitMetricImmediately(errorMetric);
                }
                else
                {
                    // Retryable exception: buffer until statement completes
                    var context = _statementContexts.GetOrAdd(statementId, _ => new StatementTelemetryDetails());
                    context.Exceptions.Add(ex);
                }
            }
            catch (Exception aggregatorEx)
            {
                Debug.WriteLine($"[TRACE] Error recording exception: {aggregatorEx.Message}");
            }
        }

        /// <summary>
        /// Marks a statement as complete and emits the aggregated metrics.
        /// If the statement failed, also flushes any buffered retryable exceptions.
        /// All exceptions are swallowed and logged at TRACE level.
        /// </summary>
        /// <param name="statementId">The statement ID to complete.</param>
        /// <param name="failed">Whether the statement ultimately failed.</param>
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
                    // Emit aggregated statement metric
                    var metric = context.ToMetric();
                    if (metric != null)
                    {
                        AddToPendingMetrics(metric);
                    }

                    // Only flush buffered exceptions if statement ultimately failed
                    if (failed && context.Exceptions.Any())
                    {
                        foreach (var ex in context.Exceptions)
                        {
                            var errorMetric = CreateErrorMetric(statementId, ex);
                            AddToPendingMetrics(errorMetric);
                        }
                    }

                    // Check if batch size reached
                    lock (_pendingMetrics)
                    {
                        if (_pendingMetrics.Count >= _config.BatchSize)
                        {
                            _ = FlushAsync().ConfigureAwait(false);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] Error completing statement: {ex.Message}");
            }
        }

        /// <summary>
        /// Flushes all pending metrics to the exporter.
        /// All exceptions are swallowed and logged at TRACE level.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the async operation.</returns>
        public async Task FlushAsync(CancellationToken ct = default)
        {
            if (_disposed)
            {
                return;
            }

            await _flushLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (_pendingMetrics.Count == 0)
                {
                    return;
                }

                // Copy pending metrics and clear the list
                var metricsToFlush = _pendingMetrics.ToList();
                _pendingMetrics.Clear();

                // Export metrics
                await _exporter.ExportAsync(metricsToFlush, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] Telemetry flush error: {ex.Message}");
            }
            finally
            {
                _flushLock.Release();
            }
        }

        /// <summary>
        /// Disposes the aggregator and flushes any pending metrics.
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
                // Stop the timer
                _flushTimer?.Dispose();

                // Flush any remaining metrics
                FlushAsync().GetAwaiter().GetResult();

                // Dispose the semaphore
                _flushLock?.Dispose();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] Error disposing aggregator: {ex.Message}");
            }
        }

        // Private helper methods

        private TelemetryEventType DetermineEventType(Activity activity)
        {
            // Check for errors first
            if (activity.GetTagItem("error.type") != null)
            {
                return TelemetryEventType.Error;
            }

            // Map based on operation name
            var operationName = activity.OperationName ?? string.Empty;
            if (operationName.StartsWith("Connection.", StringComparison.OrdinalIgnoreCase))
            {
                return TelemetryEventType.ConnectionOpen;
            }

            if (operationName.StartsWith("Statement.", StringComparison.OrdinalIgnoreCase))
            {
                return TelemetryEventType.StatementExecution;
            }

            // Default for unknown operations
            return TelemetryEventType.StatementExecution;
        }

        private TelemetryMetric CreateMetricFromActivity(Activity activity, TelemetryEventType eventType)
        {
            var metric = new TelemetryMetric
            {
                MetricType = eventType.ToString().ToLowerInvariant(),
                Timestamp = activity.StartTimeUtc,
                ExecutionLatencyMs = (long)activity.Duration.TotalMilliseconds
            };

            // Filter and extract tags using the registry
            foreach (var tag in activity.Tags)
            {
                if (TelemetryTagRegistry.ShouldExportToDatabricks(eventType, tag.Key))
                {
                    SetMetricProperty(metric, tag.Key, tag.Value);
                }
            }

            return metric;
        }

        private void SetMetricProperty(TelemetryMetric metric, string tagKey, object? tagValue)
        {
            if (tagValue == null)
            {
                return;
            }

            switch (tagKey)
            {
                case "session.id":
                    metric.SessionId = tagValue.ToString();
                    break;
                case "statement.id":
                    metric.StatementId = tagValue.ToString();
                    break;
                case "workspace.id":
                    if (long.TryParse(tagValue.ToString(), out var workspaceId))
                    {
                        metric.WorkspaceId = workspaceId;
                    }
                    break;
                case "result.format":
                    metric.ResultFormat = tagValue.ToString();
                    break;
                case "result.chunk_count":
                    if (int.TryParse(tagValue.ToString(), out var chunkCount))
                    {
                        metric.ChunkCount = chunkCount;
                    }
                    break;
                case "result.bytes_downloaded":
                    if (long.TryParse(tagValue.ToString(), out var bytesDownloaded))
                    {
                        metric.TotalBytesDownloaded = bytesDownloaded;
                    }
                    break;
                case "poll.count":
                    if (int.TryParse(tagValue.ToString(), out var pollCount))
                    {
                        metric.PollCount = pollCount;
                    }
                    break;
                // Add more property mappings as needed
            }
        }

        private void AggregateStatementActivity(Activity activity)
        {
            var statementId = activity.GetTagItem("statement.id")?.ToString();
            if (string.IsNullOrEmpty(statementId))
            {
                // No statement ID - cannot aggregate
                return;
            }

            var context = _statementContexts.GetOrAdd(statementId, _ => new StatementTelemetryDetails
            {
                StatementId = statementId,
                SessionId = activity.GetTagItem("session.id")?.ToString(),
                StartTime = activity.StartTimeUtc
            });

            // Aggregate metrics
            context.ExecutionLatencyMs += (long)activity.Duration.TotalMilliseconds;

            // Update other properties from activity tags
            foreach (var tag in activity.Tags)
            {
                UpdateContextProperty(context, tag.Key, tag.Value);
            }
        }

        private void UpdateContextProperty(StatementTelemetryDetails context, string tagKey, object? tagValue)
        {
            if (tagValue == null)
            {
                return;
            }

            switch (tagKey)
            {
                case "workspace.id":
                    if (long.TryParse(tagValue.ToString(), out var workspaceId))
                    {
                        context.WorkspaceId = workspaceId;
                    }
                    break;
                case "result.format":
                    context.ResultFormat = tagValue.ToString();
                    break;
                case "result.chunk_count":
                    if (int.TryParse(tagValue.ToString(), out var chunkCount))
                    {
                        context.ChunkCount = (context.ChunkCount ?? 0) + chunkCount;
                    }
                    break;
                case "result.bytes_downloaded":
                    if (long.TryParse(tagValue.ToString(), out var bytesDownloaded))
                    {
                        context.TotalBytesDownloaded = (context.TotalBytesDownloaded ?? 0) + bytesDownloaded;
                    }
                    break;
                case "poll.count":
                    if (int.TryParse(tagValue.ToString(), out var pollCount))
                    {
                        context.PollCount = (context.PollCount ?? 0) + pollCount;
                    }
                    break;
            }
        }

        private TelemetryMetric CreateErrorMetric(string statementId, Exception ex)
        {
            var metric = new TelemetryMetric
            {
                MetricType = "error",
                Timestamp = DateTimeOffset.UtcNow,
                StatementId = statementId
            };

            // Try to get session_id from statement context
            if (_statementContexts.TryGetValue(statementId, out var context))
            {
                metric.SessionId = context.SessionId;
                metric.WorkspaceId = context.WorkspaceId;
            }

            return metric;
        }

        private void EmitMetricImmediately(TelemetryMetric metric)
        {
            try
            {
                _ = _exporter.ExportAsync(new[] { metric }).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] Error emitting metric immediately: {ex.Message}");
            }
        }

        private void AddToPendingMetrics(TelemetryMetric metric)
        {
            lock (_pendingMetrics)
            {
                _pendingMetrics.Add(metric);
            }
        }
    }

    /// <summary>
    /// Holds aggregated telemetry details for a single statement.
    /// Used for per-statement aggregation following JDBC pattern.
    /// </summary>
    internal sealed class StatementTelemetryDetails
    {
        public string? StatementId { get; set; }
        public string? SessionId { get; set; }
        public long? WorkspaceId { get; set; }
        public DateTimeOffset StartTime { get; set; }
        public long ExecutionLatencyMs { get; set; }
        public string? ResultFormat { get; set; }
        public int? ChunkCount { get; set; }
        public long? TotalBytesDownloaded { get; set; }
        public int? PollCount { get; set; }
        public List<Exception> Exceptions { get; } = new List<Exception>();

        /// <summary>
        /// Converts the aggregated details to a TelemetryMetric for export.
        /// </summary>
        /// <returns>A TelemetryMetric instance with aggregated data.</returns>
        public TelemetryMetric ToMetric()
        {
            return new TelemetryMetric
            {
                MetricType = "statement",
                Timestamp = StartTime,
                StatementId = StatementId,
                SessionId = SessionId,
                WorkspaceId = WorkspaceId,
                ExecutionLatencyMs = ExecutionLatencyMs,
                ResultFormat = ResultFormat,
                ChunkCount = ChunkCount,
                TotalBytesDownloaded = TotalBytesDownloaded,
                PollCount = PollCount
            };
        }
    }
}
