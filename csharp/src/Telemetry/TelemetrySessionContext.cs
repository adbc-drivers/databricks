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
using AdbcDrivers.Databricks.Telemetry.Proto;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Client that receives telemetry events from multiple connections to the same host,
    /// batches them, and manages periodic flushing to the backend service.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One instance is shared per host via TelemetryClientManager to prevent rate limiting
    /// by consolidating telemetry traffic from concurrent connections to the same host.
    /// </para>
    /// <para>
    /// Thread Safety: All methods are thread-safe and can be called concurrently from
    /// multiple connections. Enqueue() is non-blocking and safe to call from any thread.
    /// FlushAsync() and CloseAsync() coordinate with ongoing flush operations using
    /// internal synchronization.
    /// </para>
    /// <para>
    /// Lifecycle: The client is created by TelemetryClientManager when the first connection
    /// to a host is opened. It remains active as long as any connection to that host is open
    /// (reference counting). When the last connection closes, TelemetryClientManager calls
    /// CloseAsync() to flush pending events and dispose resources.
    /// </para>
    /// </remarks>
    internal interface ITelemetryClient : IAsyncDisposable
    {
        /// <summary>
        /// Queue a telemetry event for export. This method is non-blocking and thread-safe.
        /// </summary>
        /// <param name="log">The telemetry frontend log to enqueue.</param>
        /// <remarks>
        /// <para>
        /// Events are batched and flushed periodically (configurable flush interval) or when
        /// the batch size limit is reached. This method returns immediately after adding the
        /// event to an internal queue.
        /// </para>
        /// <para>
        /// Called by DatabricksStatement when a statement completes, or by other driver
        /// components that generate telemetry events.
        /// </para>
        /// <para>
        /// This method never throws exceptions. If the client is closed or disposing,
        /// the event is silently dropped.
        /// </para>
        /// </remarks>
        void Enqueue(TelemetryFrontendLog log);

        /// <summary>
        /// Force flush all pending events immediately.
        /// </summary>
        /// <param name="ct">Cancellation token to cancel the flush operation.</param>
        /// <returns>A task that completes when all pending events have been flushed.</returns>
        /// <remarks>
        /// <para>
        /// This method blocks until all queued events are exported to the backend service.
        /// It is called when a connection closes to ensure no events are lost.
        /// </para>
        /// <para>
        /// If a flush is already in progress, this method waits for it to complete rather
        /// than starting a new flush operation.
        /// </para>
        /// <para>
        /// This method never throws exceptions related to telemetry failures. Export errors
        /// are caught and logged internally to ensure telemetry operations never impact
        /// driver functionality.
        /// </para>
        /// </remarks>
        Task FlushAsync(CancellationToken ct = default);

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
        Task CloseAsync();
    }

    /// <summary>
    /// Per-connection session-level telemetry context.
    /// Created once at connection open and shared with all statements on that connection.
    /// </summary>
    /// <remarks>
    /// This class holds session-level data that is populated during connection initialization.
    /// Properties use internal setters so that only the driver assembly can set values after
    /// construction. Once initialized, the context should be treated as read-only by consumers.
    /// </remarks>
    internal sealed class TelemetrySessionContext
    {
        /// <summary>
        /// Gets the session ID from the server.
        /// </summary>
        public string? SessionId { get; internal set; }

        /// <summary>
        /// Gets the authentication type used for the connection (e.g., "PAT", "OAuth-M2M").
        /// </summary>
        public string? AuthType { get; internal set; }

        /// <summary>
        /// Gets the Databricks workspace ID.
        /// </summary>
        public long WorkspaceId { get; internal set; }

        /// <summary>
        /// Gets the driver system configuration (OS, runtime, driver version, etc.).
        /// </summary>
        public DriverSystemConfiguration? SystemConfiguration { get; internal set; }

        /// <summary>
        /// Gets the driver connection parameters (HTTP path, protocol mode, host details, etc.).
        /// </summary>
        public DriverConnectionParameters? DriverConnectionParams { get; internal set; }

        /// <summary>
        /// Gets the default result format configured for this connection.
        /// </summary>
        public ExecutionResultFormat DefaultResultFormat { get; internal set; }

        /// <summary>
        /// Gets whether compression is enabled by default for this connection.
        /// </summary>
        public bool DefaultCompressionEnabled { get; internal set; }

        /// <summary>
        /// Gets the telemetry client for exporting telemetry events.
        /// </summary>
        public ITelemetryClient? TelemetryClient { get; internal set; }
    }
}
