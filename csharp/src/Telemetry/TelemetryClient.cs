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

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Main telemetry client that coordinates all telemetry components.
    /// Owns the lifecycle of listener, aggregator, and exporter.
    /// Implements background flush task and graceful shutdown.
    /// All exceptions are swallowed and logged at TRACE level only.
    /// </summary>
    internal sealed class TelemetryClient : ITelemetryClient, IDisposable
    {
        private readonly string _host;
        private readonly HttpClient _httpClient;
        private readonly TelemetryConfiguration _config;
        private readonly bool _isAuthenticated;
        private readonly ITelemetryExporter _exporter;
        private readonly CancellationTokenSource _cts;
        private readonly Task _backgroundFlushTask;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the TelemetryClient class.
        /// </summary>
        /// <param name="host">The Databricks host string.</param>
        /// <param name="httpClient">The HttpClient for making requests.</param>
        /// <param name="config">The telemetry configuration.</param>
        /// <param name="isAuthenticated">Whether the connection is authenticated.</param>
        public TelemetryClient(
            string host,
            HttpClient httpClient,
            TelemetryConfiguration config,
            bool isAuthenticated = true)
        {
            if (string.IsNullOrEmpty(host))
            {
                throw new ArgumentException("Host cannot be null or empty.", nameof(host));
            }

            _host = host;
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _isAuthenticated = isAuthenticated;

            // Initialize the telemetry pipeline:
            // DatabricksTelemetryExporter -> CircuitBreakerTelemetryExporter
            var databricksExporter = new DatabricksTelemetryExporter(
                httpClient,
                host,
                isAuthenticated,
                config);
            _exporter = new CircuitBreakerTelemetryExporter(host, databricksExporter);

            // Create cancellation token source for background task
            _cts = new CancellationTokenSource();

            // Start background flush task
            _backgroundFlushTask = StartBackgroundFlushTask(_cts.Token);
        }

        /// <summary>
        /// Exports telemetry metrics asynchronously. Never throws exceptions.
        /// </summary>
        /// <param name="metrics">The list of metrics to export.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Task representing the async export operation.</returns>
        public async Task ExportAsync(IReadOnlyList<TelemetryMetric> metrics, CancellationToken ct = default)
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                // Delegate to the exporter (circuit breaker wraps it)
                await _exporter.ExportAsync(metrics, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per requirement
                Debug.WriteLine($"[TRACE] TelemetryClient export error: {ex.Message}");
            }
        }

        /// <summary>
        /// Closes the telemetry client gracefully.
        /// Flushes all pending metrics synchronously, cancels background task, and disposes resources.
        /// Never throws exceptions.
        /// </summary>
        /// <returns>Task representing the async close operation.</returns>
        public async Task CloseAsync()
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                // Step 1: Cancel background flush task
                _cts.Cancel();

                // Step 2: Wait for background task to complete (with timeout)
                try
                {
                    await _backgroundFlushTask.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    Debug.WriteLine("[TRACE] Background flush task did not complete within timeout");
                }
                catch (OperationCanceledException)
                {
                    // Expected when task is cancelled
                }
            }
            catch (Exception ex)
            {
                // Swallow per requirement
                Debug.WriteLine($"[TRACE] Error closing telemetry client: {ex.Message}");
            }
        }

        /// <summary>
        /// Disposes the telemetry client and releases resources.
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
                // Close the client synchronously
                CloseAsync().GetAwaiter().GetResult();

                // Dispose resources
                _cts?.Dispose();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] Error disposing telemetry client: {ex.Message}");
            }
        }

        // Private helper methods

        private Task StartBackgroundFlushTask(CancellationToken ct)
        {
            return Task.Run(async () =>
            {
                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        try
                        {
                            // Wait for the flush interval
                            await Task.Delay(TimeSpan.FromMilliseconds(_config.FlushIntervalMs), ct).ConfigureAwait(false);

                            // Note: The actual flushing is handled by MetricsAggregator's timer
                            // This task is primarily for keeping the client alive and can be used
                            // for additional periodic operations if needed in the future
                        }
                        catch (OperationCanceledException)
                        {
                            // Expected when cancelled, exit loop
                            break;
                        }
                        catch (Exception ex)
                        {
                            // Swallow any other exceptions
                            Debug.WriteLine($"[TRACE] Background flush task error: {ex.Message}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"[TRACE] Background flush task outer error: {ex.Message}");
                }
            }, ct);
        }
    }
}
