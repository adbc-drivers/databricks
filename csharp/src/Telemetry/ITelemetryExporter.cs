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
    /// Interface for exporting telemetry events to a backend service.
    /// </summary>
    /// <remarks>
    /// Implementations of this interface must be safe to call from any context.
    /// All methods should be non-blocking and should never throw exceptions
    /// (exceptions should be caught and logged at TRACE level internally).
    /// This follows the telemetry design principle that telemetry operations
    /// should never impact driver operations.
    /// </remarks>
    public interface ITelemetryExporter
    {
        /// <summary>
        /// Export telemetry frontend logs to the backend service.
        /// </summary>
        /// <param name="logs">The list of telemetry frontend logs to export.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>
        /// A task that resolves to true if the export succeeded (HTTP 2xx response),
        /// or false if the export failed or was skipped. Returns true for empty/null logs
        /// since there's nothing to export (no failure occurred).
        /// </returns>
        /// <remarks>
        /// This method must never throw exceptions. All errors should be caught
        /// and logged at TRACE level internally. The method may return early
        /// if the circuit breaker is open or if there are no logs to export.
        /// </remarks>
        Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default);
    }
}
