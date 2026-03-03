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
using Polly.CircuitBreaker;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Wraps an <see cref="ITelemetryExporter"/> with circuit breaker protection.
    /// </summary>
    /// <remarks>
    /// When the circuit is closed, delegates to the inner exporter and returns its result.
    /// When the circuit is open (due to accumulated failures), drops events gracefully
    /// by returning false without calling the inner exporter.
    ///
    /// The circuit breaker tracks failures from the inner exporter by seeing exceptions
    /// before they are swallowed. All exceptions are caught internally and logged at
    /// TRACE level to avoid customer anxiety per telemetry design requirements.
    ///
    /// JDBC Reference: CircuitBreakerTelemetryPushClient.java
    /// </remarks>
    internal sealed class CircuitBreakerTelemetryExporter : ITelemetryExporter
    {
        private readonly string _host;
        private readonly ITelemetryExporter _innerExporter;
        private readonly CircuitBreaker _circuitBreaker;

        /// <summary>
        /// Creates a new <see cref="CircuitBreakerTelemetryExporter"/>.
        /// </summary>
        /// <param name="host">The host used to obtain a per-host circuit breaker.</param>
        /// <param name="innerExporter">The inner exporter to delegate to when the circuit is closed.</param>
        /// <exception cref="ArgumentException">Thrown when host is null, empty, or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown when innerExporter is null.</exception>
        public CircuitBreakerTelemetryExporter(string host, ITelemetryExporter innerExporter)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null, empty, or whitespace.", nameof(host));
            }

            _host = host;
            _innerExporter = innerExporter ?? throw new ArgumentNullException(nameof(innerExporter));
            _circuitBreaker = CircuitBreakerManager.GetInstance().GetCircuitBreaker(host);
        }

        /// <summary>
        /// Export telemetry frontend logs through the circuit breaker.
        /// </summary>
        /// <param name="logs">The list of telemetry frontend logs to export.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>
        /// True if the inner exporter succeeded, false if the circuit is open or the export failed.
        /// Returns true for null or empty logs since there is nothing to export.
        /// </returns>
        /// <remarks>
        /// This method never throws exceptions. The circuit breaker sees inner exporter
        /// exceptions before they are swallowed, allowing it to track failures and
        /// transition states appropriately.
        /// </remarks>
        public async Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
        {
            if (logs == null || logs.Count == 0)
            {
                return true;
            }

            try
            {
                // Execute through the circuit breaker so it can track failures.
                // If the inner exporter throws, the circuit breaker records the failure
                // and re-throws the exception, which we then swallow below.
                return await _circuitBreaker.ExecuteAsync<bool>(async () =>
                {
                    return await _innerExporter.ExportAsync(logs, ct).ConfigureAwait(false);
                }).ConfigureAwait(false);
            }
            catch (BrokenCircuitException)
            {
                // Circuit is open - drop events silently
                Debug.WriteLine($"[TRACE] Circuit breaker open for host '{_host}' - dropping telemetry events");
                return false;
            }
            catch (Exception ex)
            {
                // All other exceptions swallowed AFTER the circuit breaker has tracked them
                Debug.WriteLine($"[TRACE] Telemetry export error for host '{_host}': {ex.Message}");
                return false;
            }
        }
    }
}
