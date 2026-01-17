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

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Interface for exporting telemetry metrics to Databricks telemetry service.
    /// Implementations must never throw exceptions - all errors should be swallowed and logged at TRACE level.
    /// </summary>
    public interface ITelemetryExporter
    {
        /// <summary>
        /// Exports telemetry metrics to Databricks service.
        /// This method must never throw exceptions. All errors should be caught, logged at TRACE level, and swallowed.
        /// </summary>
        /// <param name="metrics">List of telemetry metrics to export.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>Task representing the async export operation.</returns>
        Task ExportAsync(IReadOnlyList<TelemetryMetric> metrics, CancellationToken ct = default);
    }
}
