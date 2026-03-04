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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry.Models;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Defines the contract for a per-host telemetry client that batches and exports
    /// telemetry events to the Databricks telemetry backend.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This interface is used by <c>MetricsAggregator</c> instances to send completed
    /// proto messages to the shared per-host client. Multiple connections to the same host
    /// share a single <see cref="ITelemetryClient"/> instance, managed by
    /// <c>TelemetryClientManager</c> with reference counting.
    /// </para>
    /// <para>
    /// Implementations must be thread-safe, as events may be enqueued concurrently from
    /// multiple connections. All methods must follow the telemetry design principle that
    /// telemetry operations should never impact driver operations (exceptions should be
    /// swallowed internally).
    /// </para>
    /// </remarks>
    public interface ITelemetryClient : IAsyncDisposable
    {
        /// <summary>
        /// Enqueues a telemetry event for batched export.
        /// </summary>
        /// <param name="log">The telemetry frontend log to enqueue for export.</param>
        /// <remarks>
        /// <para>
        /// This method is non-blocking and thread-safe. Events are buffered internally
        /// and exported in batches either when the batch size threshold is reached or
        /// when the flush interval elapses.
        /// </para>
        /// <para>
        /// This method must never throw exceptions. If the internal queue is full or
        /// the client is closed, the event should be silently dropped.
        /// </para>
        /// </remarks>
        void Enqueue(TelemetryFrontendLog log);

        /// <summary>
        /// Forces an immediate export of all pending telemetry events.
        /// </summary>
        /// <param name="ct">Cancellation token to cancel the flush operation.</param>
        /// <returns>A task that completes when the flush operation has finished.</returns>
        /// <remarks>
        /// This method exports all currently buffered events regardless of whether
        /// the batch size threshold has been reached. It is typically called during
        /// connection close to ensure all events are exported before shutdown.
        /// </remarks>
        Task FlushAsync(CancellationToken ct = default);

        /// <summary>
        /// Performs a graceful shutdown of the telemetry client with a final flush.
        /// </summary>
        /// <returns>A task that completes when the client has been fully shut down.</returns>
        /// <remarks>
        /// <para>
        /// This method performs the following shutdown sequence:
        /// <list type="number">
        ///   <item><description>Cancels the background flush task.</description></item>
        ///   <item><description>Flushes all remaining pending events.</description></item>
        ///   <item><description>Waits for the background task to complete (with timeout).</description></item>
        ///   <item><description>Disposes internal resources.</description></item>
        /// </list>
        /// </para>
        /// <para>
        /// This method must never throw exceptions. All errors during shutdown
        /// should be caught and logged at TRACE level internally.
        /// </para>
        /// </remarks>
        Task CloseAsync();
    }
}
