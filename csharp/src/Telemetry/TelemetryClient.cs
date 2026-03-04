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
    /// Concrete implementation of <see cref="ITelemetryClient"/> that batches telemetry events
    /// and exports them periodically or when the batch size threshold is reached.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This class is the per-host telemetry client managed by <c>TelemetryClientManager</c>.
    /// Multiple connections to the same host share a single <see cref="TelemetryClient"/> instance.
    /// Events are buffered in a <see cref="ConcurrentQueue{T}"/> and exported in batches via
    /// the provided <see cref="ITelemetryExporter"/>.
    /// </para>
    /// <para>
    /// Flush triggers:
    /// <list type="bullet">
    ///   <item><description>Batch size threshold reached (via <see cref="Enqueue"/>)</description></item>
    ///   <item><description>Periodic timer interval elapses (configured via <see cref="TelemetryConfiguration.FlushIntervalMs"/>)</description></item>
    ///   <item><description>Explicit call to <see cref="FlushAsync"/></description></item>
    ///   <item><description>Graceful shutdown via <see cref="CloseAsync"/></description></item>
    /// </list>
    /// </para>
    /// <para>
    /// All exceptions are swallowed internally and logged at TRACE level via
    /// <see cref="Debug.WriteLine(string)"/>. This follows the telemetry design principle
    /// that telemetry operations should never impact driver operations.
    /// </para>
    /// </remarks>
    internal sealed class TelemetryClient : ITelemetryClient
    {
        private readonly ITelemetryExporter _exporter;
        private readonly TelemetryConfiguration _config;
        private readonly ConcurrentQueue<TelemetryFrontendLog> _queue;
        private readonly SemaphoreSlim _flushLock;
        private readonly CancellationTokenSource _cts;
        private readonly Timer _flushTimer;
        private volatile bool _disposed;

        /// <summary>
        /// Initializes a new instance of <see cref="TelemetryClient"/>.
        /// </summary>
        /// <param name="exporter">The exporter used to send batched telemetry events.</param>
        /// <param name="config">The telemetry configuration controlling batch size and flush interval.</param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="exporter"/> or <paramref name="config"/> is null.
        /// </exception>
        public TelemetryClient(ITelemetryExporter exporter, TelemetryConfiguration config)
        {
            _exporter = exporter ?? throw new ArgumentNullException(nameof(exporter));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _queue = new ConcurrentQueue<TelemetryFrontendLog>();
            _flushLock = new SemaphoreSlim(1, 1);
            _cts = new CancellationTokenSource();
            _flushTimer = new Timer(
                OnFlushTimerElapsed,
                null,
                _config.FlushIntervalMs,
                _config.FlushIntervalMs);
        }

        /// <inheritdoc />
        public void Enqueue(TelemetryFrontendLog log)
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                _queue.Enqueue(log);

                if (_queue.Count >= _config.BatchSize)
                {
                    // Fire-and-forget flush; exceptions are swallowed inside FlushCoreAsync
                    _ = FlushCoreAsync(_cts.Token);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] TelemetryClient.Enqueue error: {ex.Message}");
            }
        }

        /// <inheritdoc />
        public async Task FlushAsync(CancellationToken ct = default)
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                await FlushCoreAsync(ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] TelemetryClient.FlushAsync error: {ex.Message}");
            }
        }

        /// <inheritdoc />
        public async Task CloseAsync()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            try
            {
                // Step 1: Stop the periodic flush timer
                try
                {
                    _flushTimer.Change(Timeout.Infinite, Timeout.Infinite);
                }
                catch (ObjectDisposedException)
                {
                    // Timer already disposed, ignore
                }

                // Step 2: Cancel any pending operations
                try
                {
                    _cts.Cancel();
                }
                catch (ObjectDisposedException)
                {
                    // CTS already disposed, ignore
                }

                // Step 3: Final flush of remaining events (use CancellationToken.None
                // since we want the final flush to complete even though CTS is cancelled)
                await FlushCoreAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] TelemetryClient.CloseAsync error: {ex.Message}");
            }
            finally
            {
                // Step 4: Dispose resources
                try
                {
                    _flushTimer.Dispose();
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"[TRACE] TelemetryClient.CloseAsync timer dispose error: {ex.Message}");
                }

                try
                {
                    _cts.Dispose();
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"[TRACE] TelemetryClient.CloseAsync CTS dispose error: {ex.Message}");
                }

                try
                {
                    _flushLock.Dispose();
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"[TRACE] TelemetryClient.CloseAsync semaphore dispose error: {ex.Message}");
                }
            }
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            await CloseAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Core flush implementation that drains up to <see cref="TelemetryConfiguration.BatchSize"/>
        /// items from the queue and exports them via the exporter.
        /// Uses a <see cref="SemaphoreSlim"/> to prevent concurrent flushes.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        private async Task FlushCoreAsync(CancellationToken ct)
        {
            if (_queue.IsEmpty)
            {
                return;
            }

            bool acquired = false;
            try
            {
                acquired = _flushLock.Wait(0);
                if (!acquired)
                {
                    // Another flush is in progress; skip this one
                    return;
                }

                while (!_queue.IsEmpty)
                {
                    List<TelemetryFrontendLog> batch = new List<TelemetryFrontendLog>(_config.BatchSize);

                    // Drain up to batch size items
                    while (batch.Count < _config.BatchSize && _queue.TryDequeue(out TelemetryFrontendLog? log))
                    {
                        batch.Add(log);
                    }

                    if (batch.Count > 0)
                    {
                        try
                        {
                            await _exporter.ExportAsync(batch, ct).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (ct.IsCancellationRequested)
                        {
                            // Cancellation requested; stop flushing
                            Debug.WriteLine("[TRACE] TelemetryClient.FlushCoreAsync cancelled.");
                            return;
                        }
                        catch (Exception ex)
                        {
                            Debug.WriteLine($"[TRACE] TelemetryClient.FlushCoreAsync export error: {ex.Message}");
                        }
                    }
                }
            }
            catch (ObjectDisposedException)
            {
                // Semaphore disposed during shutdown; ignore
                Debug.WriteLine("[TRACE] TelemetryClient.FlushCoreAsync: semaphore disposed.");
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] TelemetryClient.FlushCoreAsync error: {ex.Message}");
            }
            finally
            {
                if (acquired)
                {
                    try
                    {
                        _flushLock.Release();
                    }
                    catch (ObjectDisposedException)
                    {
                        // Semaphore disposed during shutdown; ignore
                    }
                }
            }
        }

        /// <summary>
        /// Callback for the periodic flush timer. Triggers a non-blocking flush.
        /// </summary>
        private void OnFlushTimerElapsed(object? state)
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                // Fire-and-forget; exceptions are swallowed inside FlushCoreAsync
                _ = FlushCoreAsync(_cts.Token);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] TelemetryClient.OnFlushTimerElapsed error: {ex.Message}");
            }
        }
    }
}
