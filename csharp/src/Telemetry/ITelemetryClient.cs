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
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Interface for telemetry client that manages telemetry lifecycle.
    /// Used by TelemetryClientManager to provide shared clients per host.
    /// </summary>
    internal interface ITelemetryClient
    {
        /// <summary>
        /// Exports telemetry metrics asynchronously. Never throws exceptions.
        /// </summary>
        /// <param name="metrics">The list of metrics to export.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Task representing the async export operation.</returns>
        Task ExportAsync(IReadOnlyList<TelemetryMetric> metrics, CancellationToken ct = default);

        /// <summary>
        /// Closes the telemetry client gracefully.
        /// Flushes all pending metrics, cancels background tasks, and disposes resources.
        /// Never throws exceptions.
        /// </summary>
        /// <returns>Task representing the async close operation.</returns>
        Task CloseAsync();
    }
}
