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
    /// Default implementation of ITelemetryClient that batches events from multiple connections
    /// and exports via HTTP on a timer or when batch size is reached.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One instance is shared per host via TelemetryClientManager to prevent rate limiting
    /// by consolidating telemetry traffic from concurrent connections to the same host.
    /// </para>
    /// <para>
    /// Thread Safety: All methods are thread-safe and can be called concurrently from
    /// multiple connections. Enqueue() is non-blocking. FlushAsync() and CloseAsync()
    /// use internal synchronization to coordinate with ongoing flush operations.
    /// </para>
    /// </remarks>
    internal sealed class TelemetryClient : ITelemetryClient
    {
        private readonly ConcurrentQueue<TelemetryFrontendLog> _queue = new ConcurrentQueue<TelemetryFrontendLog>();
        private readonly ITelemetryExporter _exporter;
        private readonly TelemetryConfiguration _config;
        private readonly Timer _flushTimer;
        private readonly SemaphoreSlim _flushLock = new SemaphoreSlim(1, 1);
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private int _queueCount;
        private volatile bool _closing;
        private volatile bool _disposed;

        /// <summary>
        /// Creates a new TelemetryClient with the specified exporter and configuration.
        /// </summary>
        /// <param name="exporter">The telemetry exporter to use for sending events.</param>
        /// <param name="config">The telemetry configuration.</param>
        public TelemetryClient(
            ITelemetryExporter exporter,
            TelemetryConfiguration config)
        {
            _exporter = exporter;
            _config = config;

            // Start periodic flush timer (default: every 5 seconds)
            _flushTimer = new Timer(
                OnFlushTimer,
                null,
                _config.FlushIntervalMs,
                _config.FlushIntervalMs);
        }

        /// <summary>
        /// Queue a telemetry event for batched export. Thread-safe, non-blocking.
        /// </summary>
        /// <param name="log">The telemetry frontend log to enqueue.</param>
        /// <remarks>
        /// <para>
        /// Events are batched and flushed periodically (configurable flush interval) or when
        /// the batch size limit is reached. This method returns immediately after adding the
        /// event to an internal queue.
        /// </para>
        /// <para>
        /// If the client is closed or disposing, the event is silently dropped.
        /// The batch-size trigger is best-effort; the periodic timer ensures all events
        /// are eventually flushed.
        /// </para>
        /// </remarks>
        public void Enqueue(TelemetryFrontendLog log)
        {
            if (_disposed) return;

            _queue.Enqueue(log);
            int count = Interlocked.Increment(ref _queueCount);

            // Trigger flush if batch size reached (best-effort)
            if (count >= _config.BatchSize)
            {
                _ = FlushAsync(); // Fire-and-forget, errors swallowed
            }
        }

        /// <summary>
        /// Force flush all pending events immediately.
        /// </summary>
        /// <param name="ct">Cancellation token to cancel the flush operation.</param>
        /// <returns>A task that completes when all pending events have been flushed.</returns>
        /// <remarks>
        /// <para>
        /// This method exports all queued events in batches to the backend service.
        /// It is called when a connection closes to ensure no events are lost.
        /// </para>
        /// <para>
        /// If a flush is already in progress, this method returns immediately rather
        /// than starting a concurrent flush operation (except during close, where it
        /// waits for the in-progress flush to complete).
        /// </para>
        /// <para>
        /// This method never throws exceptions related to telemetry failures. Export errors
        /// are caught and logged internally to ensure telemetry operations never impact
        /// driver functionality.
        /// </para>
        /// </remarks>
        public async Task FlushAsync(CancellationToken ct = default)
        {
            if (_disposed) return;

            // During close, wait for any in-progress flush to complete;
            // otherwise, skip if a flush is already running.
            if (_closing)
            {
                await _flushLock.WaitAsync(ct).ConfigureAwait(false);
            }
            else if (!await _flushLock.WaitAsync(0, ct).ConfigureAwait(false))
            {
                return;
            }

            try
            {
                // Drain all queued events in batches
                while (!_queue.IsEmpty)
                {
                    List<TelemetryFrontendLog> batch = new List<TelemetryFrontendLog>();

                    while (batch.Count < _config.BatchSize && _queue.TryDequeue(out TelemetryFrontendLog? log))
                    {
                        batch.Add(log);
                        Interlocked.Decrement(ref _queueCount);
                    }

                    if (batch.Count > 0)
                    {
                        await _exporter.ExportAsync(batch, ct).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception ex)
            {
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.flush.error",
                    tags: new ActivityTagsCollection { { "error.message", ex.Message } }));
            }
            finally
            {
                _flushLock.Release();
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
        /// 1. Cancel any pending background flush timers
        /// 2. Flush all remaining queued events
        /// 3. Dispose internal resources (timers, semaphores, etc.)
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
        public async Task CloseAsync()
        {
            if (_closing) return;
            _closing = true;

            try
            {
                // Stop timer first to prevent new timer-triggered flushes
                _flushTimer.Dispose();

                // Cancel any pending timer-triggered operations
                _cts.Cancel();

                // Final flush of all remaining events (uses blocking wait on _flushLock)
                await FlushAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.close.error",
                    tags: new ActivityTagsCollection { { "error.message", ex.Message } }));
            }
            finally
            {
                _disposed = true;
                _cts.Dispose();
                _flushLock.Dispose();
            }
        }

        /// <summary>
        /// Dispose the telemetry client asynchronously.
        /// </summary>
        /// <returns>A ValueTask that completes when the client is disposed.</returns>
        public async ValueTask DisposeAsync() => await CloseAsync().ConfigureAwait(false);

        /// <summary>
        /// Timer callback for periodic flush operations.
        /// </summary>
        /// <param name="state">Unused state object.</param>
        private void OnFlushTimer(object? state)
        {
            if (_disposed || _closing) return;
            _ = FlushAsync(_cts.Token);
        }
    }
}
