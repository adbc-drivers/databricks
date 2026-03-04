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
    /// When the circuit is closed, delegates to the inner exporter through the circuit breaker
    /// so that failures are tracked. When the circuit is open, drops events gracefully
    /// (returns false, no exception). All exceptions are swallowed after the circuit breaker
    /// has had a chance to track them.
    ///
    /// Critical design requirement: The circuit breaker MUST see exceptions before they are
    /// swallowed, so it can properly track failures and transition states.
    ///
    /// JDBC Reference: CircuitBreakerTelemetryPushClient.java
    /// </remarks>
    internal sealed class CircuitBreakerTelemetryExporter : ITelemetryExporter
    {
        private readonly ITelemetryExporter _innerExporter;
        private readonly CircuitBreaker _circuitBreaker;
        private readonly string _host;

        /// <summary>
        /// Creates a new <see cref="CircuitBreakerTelemetryExporter"/> that wraps the given exporter
        /// with circuit breaker protection for the specified host.
        /// </summary>
        /// <param name="host">The host identifier used to obtain a per-host circuit breaker.</param>
        /// <param name="innerExporter">The inner telemetry exporter to delegate to.</param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="host"/> or <paramref name="innerExporter"/> is null.
        /// </exception>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="host"/> is empty or whitespace.
        /// </exception>
        public CircuitBreakerTelemetryExporter(string host, ITelemetryExporter innerExporter)
        {
            if (host == null)
            {
                throw new ArgumentNullException(nameof(host));
            }

            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be empty or whitespace.", nameof(host));
            }

            _innerExporter = innerExporter ?? throw new ArgumentNullException(nameof(innerExporter));
            _host = host;
            _circuitBreaker = CircuitBreakerManager.GetInstance().GetCircuitBreaker(host);
        }

        /// <summary>
        /// Export telemetry frontend logs through the circuit breaker.
        /// </summary>
        /// <param name="logs">The list of telemetry frontend logs to export.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>
        /// True if the export succeeded, false if the export failed or was dropped due to
        /// an open circuit. Returns true for empty/null logs (delegates to inner exporter).
        /// </returns>
        /// <remarks>
        /// This method never throws exceptions. When the circuit is open, events are dropped
        /// gracefully and logged at DEBUG level. When the inner exporter fails, the circuit
        /// breaker tracks the failure before the exception is swallowed.
        /// </remarks>
        public async Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
        {
            if (logs == null || logs.Count == 0)
            {
                return await _innerExporter.ExportAsync(logs, ct).ConfigureAwait(false);
            }

            try
            {
                // Execute through the circuit breaker so it can track failures.
                // The inner exporter is called inside ExecuteAsync, meaning:
                // - If it throws, the circuit breaker sees the exception and tracks the failure
                // - If it returns false (swallowed failure), we throw to let the circuit breaker track it
                bool result = await _circuitBreaker.ExecuteAsync<bool>(async () =>
                {
                    bool exportResult = await _innerExporter.ExportAsync(logs, ct).ConfigureAwait(false);

                    if (!exportResult)
                    {
                        // The inner exporter returned false (it swallowed the error internally).
                        // Throw so the circuit breaker can track this as a failure.
                        throw new TelemetryExportFailedException(
                            "Inner telemetry exporter returned false, indicating export failure.");
                    }

                    return exportResult;
                }).ConfigureAwait(false);

                return result;
            }
            catch (BrokenCircuitException)
            {
                // Circuit is open - drop events gracefully
                Debug.WriteLine($"[DEBUG] Circuit breaker OPEN for host '{_host}' - dropping {logs.Count} telemetry event(s).");
                return false;
            }
            catch (OperationCanceledException)
            {
                // Cancellation should not be swallowed - propagate it
                throw;
            }
            catch (Exception ex)
            {
                // All other exceptions swallowed AFTER the circuit breaker has seen them
                Debug.WriteLine($"[TRACE] Telemetry export error for host '{_host}': {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Internal exception used to signal that the inner exporter returned false
        /// (a swallowed failure), so the circuit breaker can track it as a failure.
        /// </summary>
        internal sealed class TelemetryExportFailedException : Exception
        {
            public TelemetryExportFailedException(string message) : base(message)
            {
            }
        }
    }
}
