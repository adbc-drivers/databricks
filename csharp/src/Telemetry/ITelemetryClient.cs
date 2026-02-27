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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry.Models;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Interface for a telemetry client that exports telemetry events to a backend service.
    /// </summary>
    /// <remarks>
    /// This interface represents a per-host telemetry client that is shared across
    /// multiple connections to the same Databricks workspace. The client is managed
    /// by <see cref="TelemetryClientManager"/> with reference counting.
    ///
    /// Implementations must:
    /// - Be thread-safe for concurrent access from multiple connections
    /// - Never throw exceptions from ExportAsync (all caught and logged at TRACE level)
    /// - Support graceful shutdown via CloseAsync
    ///
    /// JDBC Reference: TelemetryClient.java
    /// </remarks>
    public interface ITelemetryClient
    {
        /// <summary>
        /// Gets the host URL for this telemetry client.
        /// </summary>
        string Host { get; }

        /// <summary>
        /// Exports telemetry frontend logs to the backend service.
        /// </summary>
        /// <param name="logs">The list of telemetry frontend logs to export.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the asynchronous export operation.</returns>
        /// <remarks>
        /// This method must never throw exceptions (except for cancellation).
        /// All errors should be caught and logged at TRACE level internally.
        /// </remarks>
        Task ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default);

        /// <summary>
        /// Closes the telemetry client and releases all resources.
        /// </summary>
        /// <returns>A task representing the asynchronous close operation.</returns>
        /// <remarks>
        /// This method is called by TelemetryClientManager when the last connection
        /// using this client is closed. The implementation should:
        /// 1. Flush any pending telemetry events
        /// 2. Cancel any background tasks
        /// 3. Dispose all resources
        ///
        /// This method must never throw exceptions. All errors should be caught
        /// and logged at TRACE level internally.
        /// </remarks>
        Task CloseAsync();
    }
}
