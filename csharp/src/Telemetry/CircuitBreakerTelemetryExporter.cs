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

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Wraps an ITelemetryExporter with circuit breaker protection.
    /// Critical requirement: Circuit breaker must see exceptions BEFORE they are swallowed.
    /// When circuit is open, drops events silently (logged at DEBUG level).
    /// After circuit breaker processes exception, swallows it at TRACE level.
    /// </summary>
    internal sealed class CircuitBreakerTelemetryExporter : ITelemetryExporter
    {
        private readonly string _host;
        private readonly ITelemetryExporter _innerExporter;
        private readonly CircuitBreaker _circuitBreaker;

        /// <summary>
        /// Initializes a new instance of the <see cref="CircuitBreakerTelemetryExporter"/> class.
        /// </summary>
        /// <param name="host">The host this exporter is for (used to get per-host circuit breaker).</param>
        /// <param name="innerExporter">The inner telemetry exporter to wrap.</param>
        public CircuitBreakerTelemetryExporter(string host, ITelemetryExporter innerExporter)
        {
            if (string.IsNullOrEmpty(host))
            {
                throw new ArgumentException("Host cannot be null or empty", nameof(host));
            }

            _host = host;
            _innerExporter = innerExporter ?? throw new ArgumentNullException(nameof(innerExporter));
            _circuitBreaker = CircuitBreakerManager.GetInstance().GetCircuitBreaker(host);
        }

        /// <summary>
        /// Exports telemetry metrics to Databricks service through circuit breaker.
        /// This method never throws exceptions. All errors are caught, logged, and swallowed.
        /// </summary>
        /// <param name="metrics">List of telemetry metrics to export.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>Task representing the async export operation.</returns>
        public async Task ExportAsync(IReadOnlyList<TelemetryMetric> metrics, CancellationToken ct = default)
        {
            if (metrics == null || metrics.Count == 0)
            {
                return;
            }

            try
            {
                // Execute through circuit breaker
                // Critical: Circuit breaker sees exceptions BEFORE they are swallowed
                await _circuitBreaker.ExecuteAsync(async () =>
                {
                    await _innerExporter.ExportAsync(metrics, ct).ConfigureAwait(false);
                }).ConfigureAwait(false);
            }
            catch (CircuitBreakerOpenException)
            {
                // Circuit is open - drop events silently
                // Log at DEBUG level per design requirement
                Debug.WriteLine($"[DEBUG] Circuit breaker OPEN for host {_host} - dropping {metrics.Count} telemetry events");
            }
            catch (Exception ex)
            {
                // All other exceptions swallowed AFTER circuit breaker has seen them
                // Log at TRACE level only to avoid customer anxiety
                Debug.WriteLine($"[TRACE] Telemetry export error for host {_host}: {ex.Message}");
            }
        }
    }
}
