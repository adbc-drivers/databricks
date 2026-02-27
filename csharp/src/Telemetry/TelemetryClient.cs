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
using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry.Models;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Main telemetry client that coordinates listener, aggregator, and exporter.
    /// Manages background flush task and graceful shutdown.
    /// </summary>
    /// <remarks>
    /// This class implements the TelemetryClient from Section 9.3 of the design doc:
    /// - Coordinates lifecycle of DatabricksActivityListener, MetricsAggregator, and CircuitBreakerTelemetryExporter
    /// - Manages a background flush task for periodic export
    /// - Implements graceful shutdown via CloseAsync: cancels background task, flushes pending, disposes resources
    /// - Never throws exceptions (all swallowed and logged at TRACE level)
    ///
    /// The client is managed per-host by TelemetryClientManager with reference counting.
    ///
    /// JDBC Reference: TelemetryClient.java
    /// </remarks>
    internal sealed class TelemetryClient : ITelemetryClient
    {
        private readonly string _host;
        private readonly DatabricksActivityListener _listener;
        private readonly MetricsAggregator _aggregator;
        private readonly ITelemetryExporter _exporter;
        private readonly CancellationTokenSource _cts;
        private readonly Task _backgroundFlushTask;
        private readonly TelemetryConfiguration _config;
        private bool _closed;
        private readonly object _closeLock = new object();

        /// <summary>
        /// Gets the host URL for this telemetry client.
        /// </summary>
        public string Host => _host;

        /// <summary>
        /// Creates a new TelemetryClient that coordinates all telemetry components.
        /// </summary>
        /// <param name="host">The Databricks host URL.</param>
        /// <param name="httpClient">The HTTP client to use for sending telemetry requests.</param>
        /// <param name="config">The telemetry configuration.</param>
        /// <param name="workspaceId">The Databricks workspace ID.</param>
        /// <param name="userAgent">The user agent string for client context.</param>
        /// <exception cref="ArgumentException">Thrown when host is null or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown when httpClient or config is null.</exception>
        public TelemetryClient(
            string host,
            HttpClient httpClient,
            TelemetryConfiguration config,
            long workspaceId = 0,
            string? userAgent = null)
            : this(host, httpClient, config, workspaceId, userAgent, null, null, null)
        {
        }

        /// <summary>
        /// Creates a new TelemetryClient with optional component injection for testing.
        /// </summary>
        /// <param name="host">The Databricks host URL.</param>
        /// <param name="httpClient">The HTTP client to use for sending telemetry requests.</param>
        /// <param name="config">The telemetry configuration.</param>
        /// <param name="workspaceId">The Databricks workspace ID.</param>
        /// <param name="userAgent">The user agent string for client context.</param>
        /// <param name="exporter">Optional custom exporter (for testing).</param>
        /// <param name="aggregator">Optional custom aggregator (for testing).</param>
        /// <param name="listener">Optional custom listener (for testing).</param>
        internal TelemetryClient(
            string host,
            HttpClient httpClient,
            TelemetryConfiguration config,
            long workspaceId,
            string? userAgent,
            ITelemetryExporter? exporter,
            MetricsAggregator? aggregator,
            DatabricksActivityListener? listener)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            if (httpClient == null)
            {
                throw new ArgumentNullException(nameof(httpClient));
            }

            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            _host = host;
            _config = config;
            _cts = new CancellationTokenSource();

            // Initialize exporter: CircuitBreakerTelemetryExporter wrapping DatabricksTelemetryExporter
            if (exporter != null)
            {
                _exporter = exporter;
            }
            else
            {
                var innerExporter = new DatabricksTelemetryExporter(httpClient, host, isAuthenticated: true, config);
                _exporter = new CircuitBreakerTelemetryExporter(host, innerExporter);
            }

            // Initialize aggregator
            _aggregator = aggregator ?? new MetricsAggregator(_exporter, config, workspaceId, userAgent);

            // Initialize listener
            _listener = listener ?? new DatabricksActivityListener(_aggregator, config);

            // Start the listener to begin collecting activities
            _listener.Start();

            // Start the background flush task
            _backgroundFlushTask = StartBackgroundFlushTask();

            Debug.WriteLine($"[TRACE] TelemetryClient: Initialized for host '{host}'");
        }

        /// <summary>
        /// Exports telemetry frontend logs to the backend service.
        /// </summary>
        /// <param name="logs">The list of telemetry frontend logs to export.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the asynchronous export operation.</returns>
        /// <remarks>
        /// This method delegates to the underlying exporter (CircuitBreakerTelemetryExporter).
        /// It never throws exceptions (except for cancellation).
        /// </remarks>
        public Task ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
        {
            return _exporter.ExportAsync(logs, ct);
        }

        /// <summary>
        /// Closes the telemetry client and releases all resources.
        /// </summary>
        /// <returns>A task representing the asynchronous close operation.</returns>
        /// <remarks>
        /// This method implements graceful shutdown per Section 9.3 of the design doc:
        /// 1. Cancels the background flush task
        /// 2. Flushes all pending metrics synchronously
        /// 3. Waits for background task to complete (with timeout)
        /// 4. Disposes all resources
        ///
        /// This method never throws exceptions. All errors are swallowed and logged at TRACE level.
        /// This method is idempotent - calling it multiple times has no additional effect.
        /// </remarks>
        public async Task CloseAsync()
        {
            lock (_closeLock)
            {
                if (_closed)
                {
                    return;
                }
                _closed = true;
            }

            Debug.WriteLine($"[TRACE] TelemetryClient: Closing client for host '{_host}'");

            try
            {
                // Step 1: Cancel the background flush task
                _cts.Cancel();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] TelemetryClient: Error cancelling background task: {ex.Message}");
            }

            try
            {
                // Step 2: Stop the listener and flush pending metrics via the listener
                // This also calls _aggregator.FlushAsync internally
                await _listener.StopAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] TelemetryClient: Error stopping listener: {ex.Message}");
            }

            try
            {
                // Step 3: Wait for background task to complete (with timeout)
#if NET6_0_OR_GREATER
                await _backgroundFlushTask.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
#else
                // For older frameworks, use Task.WhenAny with a delay
                var completedTask = await Task.WhenAny(
                    _backgroundFlushTask,
                    Task.Delay(TimeSpan.FromSeconds(5))).ConfigureAwait(false);

                // If the flush task didn't complete in time, just continue
                if (completedTask != _backgroundFlushTask)
                {
                    Debug.WriteLine($"[TRACE] TelemetryClient: Background task did not complete within timeout");
                }
#endif
            }
            catch (OperationCanceledException)
            {
                // Expected when the task is cancelled
            }
            catch (TimeoutException)
            {
                Debug.WriteLine($"[TRACE] TelemetryClient: Background task did not complete within timeout");
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] TelemetryClient: Error waiting for background task: {ex.Message}");
            }

            try
            {
                // Step 4: Dispose resources
                _listener.Dispose();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] TelemetryClient: Error disposing listener: {ex.Message}");
            }

            try
            {
                _cts.Dispose();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] TelemetryClient: Error disposing cancellation token source: {ex.Message}");
            }

            Debug.WriteLine($"[TRACE] TelemetryClient: Closed client for host '{_host}'");
        }

        /// <summary>
        /// Gets the metrics aggregator for this client.
        /// </summary>
        internal MetricsAggregator Aggregator => _aggregator;

        /// <summary>
        /// Gets the activity listener for this client.
        /// </summary>
        internal DatabricksActivityListener Listener => _listener;

        /// <summary>
        /// Gets the telemetry exporter for this client.
        /// </summary>
        internal ITelemetryExporter Exporter => _exporter;

        /// <summary>
        /// Gets whether the client has been closed.
        /// </summary>
        internal bool IsClosed => _closed;

        /// <summary>
        /// Starts the background flush task that periodically flushes pending metrics.
        /// </summary>
        private Task StartBackgroundFlushTask()
        {
            return Task.Run(async () =>
            {
                Debug.WriteLine($"[TRACE] TelemetryClient: Background flush task started for host '{_host}'");

                try
                {
                    while (!_cts.Token.IsCancellationRequested)
                    {
                        try
                        {
                            // Wait for the flush interval
                            await Task.Delay(_config.FlushIntervalMs, _cts.Token).ConfigureAwait(false);

                            // Flush pending metrics
                            await _aggregator.FlushAsync(_cts.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            // Expected when cancelled - exit the loop
                            break;
                        }
                        catch (Exception ex)
                        {
                            // Swallow all other exceptions per telemetry requirement
                            Debug.WriteLine($"[TRACE] TelemetryClient: Error in background flush: {ex.Message}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Outer exception handler for any unexpected errors
                    Debug.WriteLine($"[TRACE] TelemetryClient: Background flush task error: {ex.Message}");
                }

                Debug.WriteLine($"[TRACE] TelemetryClient: Background flush task stopped for host '{_host}'");
            }, _cts.Token);
        }
    }
}
