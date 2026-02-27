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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry.Models;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Adapter that wraps an ITelemetryExporter and implements ITelemetryClient.
    /// </summary>
    /// <remarks>
    /// This adapter bridges the gap between the ITelemetryExporter interface
    /// (used for exporting telemetry events) and the ITelemetryClient interface
    /// (used by TelemetryClientManager for per-host client management).
    ///
    /// The adapter:
    /// - Delegates ExportAsync calls to the underlying exporter
    /// - Provides a no-op CloseAsync since the exporter doesn't have close semantics
    /// </remarks>
    internal sealed class TelemetryClientAdapter : ITelemetryClient
    {
        private readonly string _host;
        private readonly ITelemetryExporter _exporter;

        /// <summary>
        /// Gets the host URL for this telemetry client.
        /// </summary>
        public string Host => _host;

        /// <summary>
        /// Gets the underlying telemetry exporter.
        /// </summary>
        internal ITelemetryExporter Exporter => _exporter;

        /// <summary>
        /// Creates a new TelemetryClientAdapter.
        /// </summary>
        /// <param name="host">The Databricks host URL.</param>
        /// <param name="exporter">The telemetry exporter to wrap.</param>
        /// <exception cref="ArgumentException">Thrown when host is null or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown when exporter is null.</exception>
        public TelemetryClientAdapter(string host, ITelemetryExporter exporter)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            _host = host;
            _exporter = exporter ?? throw new ArgumentNullException(nameof(exporter));
        }

        /// <summary>
        /// Exports telemetry frontend logs to the backend service.
        /// </summary>
        /// <param name="logs">The list of telemetry frontend logs to export.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the asynchronous export operation.</returns>
        /// <remarks>
        /// This method delegates to the underlying exporter. It never throws
        /// exceptions (except for cancellation) as per the telemetry requirement.
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
        /// Currently a no-op since ITelemetryExporter doesn't have close semantics.
        /// If the underlying exporter implements IDisposable in the future,
        /// this method should be updated to dispose it.
        /// </remarks>
        public Task CloseAsync()
        {
            Debug.WriteLine($"[TRACE] TelemetryClientAdapter: Closing client for host '{_host}'");
            // No-op for now since ITelemetryExporter doesn't have close/dispose semantics
            // The exporter is stateless and doesn't hold any resources that need cleanup
            return Task.CompletedTask;
        }
    }
}
