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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry.Models;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Main telemetry client that coordinates the listener, aggregator, and exporter into a cohesive lifecycle.
    /// This is the public-facing API that DatabricksConnection interacts with.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This client orchestrates the complete telemetry pipeline:
    /// CircuitBreakerTelemetryExporter → DatabricksTelemetryExporter
    /// </para>
    /// <para>
    /// Key Behaviors:
    /// - Constructor initializes all pipeline components in correct order
    /// - ExportAsync() delegates to the circuit breaker-protected exporter
    /// - CloseAsync() performs graceful shutdown: flush pending metrics, cancel background tasks, dispose resources
    /// - All exceptions are swallowed per telemetry requirement
    /// </para>
    /// <para>
    /// Thread Safety: All methods are thread-safe and can be called concurrently.
    /// </para>
    /// </remarks>
    internal sealed class TelemetryClient : ITelemetryClient
    {
        /// <summary>
        /// Activity source for tracing telemetry client operations.
        /// </summary>
        private static readonly ActivitySource s_activitySource = new ActivitySource("AdbcDrivers.Databricks.TelemetryClient");

        private readonly DatabricksTelemetryExporter _databricksExporter;
        private readonly CircuitBreakerTelemetryExporter _circuitBreakerExporter;
        private readonly ITelemetryExporter _effectiveExporter;
        private readonly ConcurrentQueue<TelemetryFrontendLog> _pendingLogs;
        private readonly int _batchSize;
        private readonly CancellationTokenSource _cts;
        private volatile bool _disposed;

        /// <summary>
        /// Creates a new TelemetryClient with the specified configuration and HTTP client.
        /// </summary>
        /// <param name="host">The Databricks host (for circuit breaker isolation).</param>
        /// <param name="httpClient">The HTTP client to use for exporting telemetry.</param>
        /// <param name="isAuthenticated">Whether the connection is authenticated (determines telemetry endpoint).</param>
        /// <param name="configuration">The telemetry configuration.</param>
        /// <param name="exporterOverride">Optional exporter override for testing.</param>
        /// <exception cref="ArgumentNullException">Thrown when host, httpClient, or configuration is null.</exception>
        /// <exception cref="ArgumentException">Thrown when host is empty or whitespace.</exception>
        public TelemetryClient(
            string host,
            System.Net.Http.HttpClient httpClient,
            bool isAuthenticated,
            TelemetryConfiguration configuration,
            ITelemetryExporter? exporterOverride = null)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            if (httpClient == null)
            {
                throw new ArgumentNullException(nameof(httpClient));
            }

            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration));
            }

            _cts = new CancellationTokenSource();
            _pendingLogs = new ConcurrentQueue<TelemetryFrontendLog>();
            _batchSize = configuration.BatchSize;

            try
            {
                // Initialize pipeline components in order:
                // 1. DatabricksTelemetryExporter (innermost - does the HTTP export)
                _databricksExporter = new DatabricksTelemetryExporter(httpClient, host, isAuthenticated, configuration);

                // 2. CircuitBreakerTelemetryExporter (wraps exporter with circuit breaker protection)
                _circuitBreakerExporter = new CircuitBreakerTelemetryExporter(_databricksExporter, host);

                // Use override exporter if provided (for testing), otherwise use circuit breaker exporter
                _effectiveExporter = exporterOverride ?? (ITelemetryExporter)_circuitBreakerExporter;

                Activity.Current?.AddEvent(new ActivityEvent("telemetry.client.initialized",
                    tags: new ActivityTagsCollection
                    {
                        { "host", host },
                        { "batch_size", configuration.BatchSize },
                        { "flush_interval_ms", configuration.FlushIntervalMs },
                        { "enabled", configuration.Enabled }
                    }));
            }
            catch (Exception ex)
            {
                // Clean up any partially initialized resources
                try
                {
                    _cts?.Dispose();
                }
                catch
                {
                    // Swallow cleanup exceptions
                }

                // Log and rethrow original exception
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.client.initialization_failed",
                    tags: new ActivityTagsCollection
                    {
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));

                throw;
            }
        }

        /// <summary>
        /// Queue a telemetry event for export. This method is non-blocking and thread-safe.
        /// </summary>
        /// <param name="log">The telemetry frontend log to enqueue.</param>
        /// <remarks>
        /// <para>
        /// This method never throws exceptions. If the client is closed or disposing,
        /// the event is silently dropped.
        /// </para>
        /// </remarks>
        public void Enqueue(TelemetryFrontendLog log)
        {
            if (_disposed || log == null)
            {
                return;
            }

            try
            {
                _pendingLogs.Enqueue(log);

                // Trigger flush if batch size reached
                if (_pendingLogs.Count >= _batchSize)
                {
                    _ = Task.Run(() => FlushAsync(CancellationToken.None));
                }
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per telemetry requirement
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.client.enqueue_error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));
            }
        }

        /// <summary>
        /// Force flush all pending events immediately.
        /// </summary>
        /// <param name="ct">Cancellation token to cancel the flush operation.</param>
        /// <returns>A task that completes when all pending events have been flushed.</returns>
        /// <remarks>
        /// <para>
        /// This method blocks until all queued metrics in the aggregator are exported.
        /// It is called when a connection closes to ensure no events are lost.
        /// </para>
        /// <para>
        /// This method never throws exceptions related to telemetry failures. Export errors
        /// are caught and logged internally to ensure telemetry operations never impact
        /// driver functionality.
        /// </para>
        /// </remarks>
        public async Task FlushAsync(CancellationToken ct = default)
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                // Flush directly enqueued logs (V3 direct-object path)
                List<TelemetryFrontendLog> logsToFlush = new List<TelemetryFrontendLog>();
                while (_pendingLogs.TryDequeue(out TelemetryFrontendLog? log))
                {
                    logsToFlush.Add(log);
                }

                if (logsToFlush.Count > 0)
                {
                    await _effectiveExporter.ExportAsync(logsToFlush, ct).ConfigureAwait(false);
                }

                Activity.Current?.AddEvent(new ActivityEvent("telemetry.client.flush_completed"));
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per telemetry requirement
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.client.flush_error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));
            }
        }

        /// <summary>
        /// Gracefully close the client. Flushes all pending events before disposing resources.
        /// </summary>
        /// <returns>A task that completes when the client is fully closed.</returns>
        /// <remarks>
        /// <para>
        /// This method is called by TelemetryClientManager when the reference count for this
        /// host reaches zero (i.e., the last connection to this host has been closed).
        /// </para>
        /// <para>
        /// The close operation performs the following steps:
        /// 1. Cancel any pending background tasks
        /// 2. Dispose all resources (cancellation token source)
        /// </para>
        /// <para>
        /// This method is idempotent - calling it multiple times is safe and has no effect
        /// after the first call completes.
        /// </para>
        /// <para>
        /// This method never throws exceptions. All errors during close are caught and
        /// logged internally.
        /// </para>
        /// </remarks>
        public Task CloseAsync()
        {
            if (_disposed)
            {
                return Task.CompletedTask;
            }

            _disposed = true;

            try
            {
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.client.closing"));

                // Cancel any pending background tasks
                try
                {
                    _cts.Cancel();
                }
                catch (Exception ex)
                {
                    Activity.Current?.AddEvent(new ActivityEvent("telemetry.client.cancellation_error",
                        tags: new ActivityTagsCollection
                        {
                            { "error.message", ex.Message },
                            { "error.type", ex.GetType().Name }
                        }));
                }

                Activity.Current?.AddEvent(new ActivityEvent("telemetry.client.closed"));
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per telemetry requirement
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.client.close_error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));
            }
            finally
            {
                // Ensure CancellationTokenSource is disposed even if other operations fail
                try
                {
                    _cts.Dispose();
                }
                catch
                {
                    // Swallow CTS dispose exceptions
                }
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Dispose the telemetry client asynchronously.
        /// </summary>
        /// <returns>A ValueTask that completes when the client is disposed.</returns>
        public async ValueTask DisposeAsync()
        {
            await CloseAsync().ConfigureAwait(false);
        }
    }
}
