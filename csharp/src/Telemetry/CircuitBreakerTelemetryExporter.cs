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
    /// Wraps a telemetry exporter with circuit breaker protection.
    /// </summary>
    /// <remarks>
    /// This exporter implements the circuit breaker pattern to protect against
    /// failing telemetry endpoints:
    /// - When circuit is closed: Exports events via the inner exporter
    /// - When circuit is open: Drops events silently (logged at DEBUG level)
    /// - Circuit breaker MUST see exceptions before they are swallowed
    ///
    /// The circuit breaker is per-host, managed by CircuitBreakerManager.
    ///
    /// JDBC Reference: CircuitBreakerTelemetryPushClient.java:15
    /// </remarks>
    internal sealed class CircuitBreakerTelemetryExporter : ITelemetryExporter
    {
        private readonly string _host;
        private readonly ITelemetryExporter _innerExporter;
        private readonly CircuitBreakerManager _circuitBreakerManager;

        /// <summary>
        /// Gets the host URL for this exporter.
        /// </summary>
        internal string Host => _host;

        /// <summary>
        /// Gets the inner telemetry exporter.
        /// </summary>
        internal ITelemetryExporter InnerExporter => _innerExporter;

        /// <summary>
        /// Creates a new CircuitBreakerTelemetryExporter.
        /// </summary>
        /// <param name="host">The Databricks host URL.</param>
        /// <param name="innerExporter">The inner telemetry exporter to wrap.</param>
        /// <exception cref="ArgumentException">Thrown when host is null or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown when innerExporter is null.</exception>
        public CircuitBreakerTelemetryExporter(string host, ITelemetryExporter innerExporter)
            : this(host, innerExporter, CircuitBreakerManager.GetInstance())
        {
        }

        /// <summary>
        /// Creates a new CircuitBreakerTelemetryExporter with a specified CircuitBreakerManager.
        /// </summary>
        /// <param name="host">The Databricks host URL.</param>
        /// <param name="innerExporter">The inner telemetry exporter to wrap.</param>
        /// <param name="circuitBreakerManager">The circuit breaker manager to use.</param>
        /// <exception cref="ArgumentException">Thrown when host is null or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown when innerExporter or circuitBreakerManager is null.</exception>
        /// <remarks>
        /// This constructor is primarily for testing to allow injecting a mock CircuitBreakerManager.
        /// </remarks>
        internal CircuitBreakerTelemetryExporter(
            string host,
            ITelemetryExporter innerExporter,
            CircuitBreakerManager circuitBreakerManager)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            _host = host;
            _innerExporter = innerExporter ?? throw new ArgumentNullException(nameof(innerExporter));
            _circuitBreakerManager = circuitBreakerManager ?? throw new ArgumentNullException(nameof(circuitBreakerManager));
        }

        /// <summary>
        /// Export telemetry frontend logs to the backend service with circuit breaker protection.
        /// </summary>
        /// <param name="logs">The list of telemetry frontend logs to export.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the asynchronous export operation.</returns>
        /// <remarks>
        /// This method never throws exceptions (except for cancellation). All errors are:
        /// 1. First seen by the circuit breaker (to track failures)
        /// 2. Then swallowed and logged at TRACE level
        ///
        /// When the circuit is open, events are dropped silently and logged at DEBUG level.
        /// </remarks>
        public async Task ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
        {
            if (logs == null || logs.Count == 0)
            {
                return;
            }

            var circuitBreaker = _circuitBreakerManager.GetCircuitBreaker(_host);

            try
            {
                // Execute through circuit breaker - it tracks failures BEFORE swallowing
                await circuitBreaker.ExecuteAsync(async () =>
                {
                    await _innerExporter.ExportAsync(logs, ct).ConfigureAwait(false);
                }).ConfigureAwait(false);
            }
            catch (CircuitBreakerOpenException)
            {
                // Circuit is open - drop events silently
                // Log at DEBUG level per design doc (circuit breaker state changes)
                Debug.WriteLine($"[DEBUG] CircuitBreakerTelemetryExporter: Circuit breaker OPEN for host '{_host}' - dropping {logs.Count} telemetry events");
            }
            catch (OperationCanceledException)
            {
                // Don't swallow cancellation - let it propagate
                throw;
            }
            catch (Exception ex)
            {
                // All other exceptions swallowed AFTER circuit breaker saw them
                // Log at TRACE level to avoid customer anxiety per design doc
                Debug.WriteLine($"[TRACE] CircuitBreakerTelemetryExporter: Error exporting telemetry for host '{_host}': {ex.Message}");
            }
        }
    }
}
