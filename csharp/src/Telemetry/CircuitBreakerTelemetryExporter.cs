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
    /// Wraps ITelemetryExporter with circuit breaker protection to prevent wasting resources
    /// on failing telemetry endpoints.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This exporter protects against failing telemetry endpoints (5xx errors, timeouts, network failures).
    /// When the circuit breaker detects too many failures, it opens the circuit and all subsequent
    /// telemetry events are silently dropped (logged at DEBUG level) until the endpoint recovers.
    /// </para>
    /// <para>
    /// Key Behaviors:
    /// - Circuit Closed: Events pass through to inner exporter. Failures are tracked.
    /// - Circuit Open: Events are silently dropped (returns true, logs at DEBUG level).
    /// - Circuit HalfOpen: Test requests are allowed through to check for recovery.
    /// - Per-host isolation: Each host gets its own circuit breaker via CircuitBreakerManager.
    /// </para>
    /// <para>
    /// Thread Safety: This class is thread-safe and can be called concurrently from multiple contexts.
    /// </para>
    /// </remarks>
    internal sealed class CircuitBreakerTelemetryExporter : ITelemetryExporter
    {
        private readonly ITelemetryExporter _innerExporter;
        private readonly CircuitBreaker _circuitBreaker;
        private readonly string _host;

        /// <summary>
        /// Gets the host for this exporter.
        /// </summary>
        internal string Host => _host;

        /// <summary>
        /// Gets the current state of the circuit breaker.
        /// </summary>
        internal CircuitBreakerState State => _circuitBreaker.State;

        /// <summary>
        /// Creates a new CircuitBreakerTelemetryExporter.
        /// </summary>
        /// <param name="innerExporter">The inner telemetry exporter to wrap with circuit breaker protection.</param>
        /// <param name="host">The host identifier for per-host circuit breaker isolation.</param>
        /// <exception cref="ArgumentNullException">Thrown when innerExporter is null.</exception>
        /// <exception cref="ArgumentException">Thrown when host is null, empty, or whitespace.</exception>
        public CircuitBreakerTelemetryExporter(ITelemetryExporter innerExporter, string host)
        {
            _innerExporter = innerExporter ?? throw new ArgumentNullException(nameof(innerExporter));

            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            _host = host;
            _circuitBreaker = CircuitBreakerManager.GetInstance().GetCircuitBreaker(host);
        }

        /// <summary>
        /// Export telemetry frontend logs with circuit breaker protection.
        /// </summary>
        /// <param name="logs">The list of telemetry frontend logs to export.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>
        /// True if the export succeeded or was silently dropped (circuit open).
        /// False if the export failed and was tracked by the circuit breaker.
        /// Returns true for empty/null logs since there's nothing to export.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method never throws exceptions. All errors are caught and traced.
        /// </para>
        /// <para>
        /// When the circuit is open, events are silently dropped and logged at DEBUG level.
        /// This prevents wasting resources on a failing endpoint while waiting for recovery.
        /// </para>
        /// <para>
        /// When the circuit is closed, events are passed through to the inner exporter.
        /// If the inner exporter fails, the failure is tracked by the circuit breaker.
        /// </para>
        /// </remarks>
        public async Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
        {
            if (logs == null || logs.Count == 0)
            {
                return true;
            }

            try
            {
                // Execute through circuit breaker
                // The circuit breaker will track failures and open if threshold is reached
                // Polly handles the open-circuit case internally by throwing BrokenCircuitException
                bool result = await _circuitBreaker.ExecuteAsync(async () =>
                {
                    bool success = await _innerExporter.ExportAsync(logs, ct).ConfigureAwait(false);

                    // If inner exporter returns false, it means the export failed
                    // We need to throw an exception so the circuit breaker can track the failure
                    if (!success)
                    {
                        throw new TelemetryExportException("Inner exporter returned false indicating export failure");
                    }

                    return success;
                }).ConfigureAwait(false);

                return result;
            }
            catch (BrokenCircuitException)
            {
                // Circuit is open - silently drop events
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.export.circuit_open",
                    tags: new ActivityTagsCollection
                    {
                        { "host", _host },
                        { "log_count", logs.Count },
                        { "action", "dropped" }
                    }));

                // Return true because dropping is expected behavior when circuit is open
                return true;
            }
            catch (OperationCanceledException)
            {
                // Cancellation should not impact driver behavior; treat as a no-op.
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.export.canceled",
                    tags: new ActivityTagsCollection
                    {
                        { "host", _host },
                        { "log_count", logs.Count },
                        { "action", "canceled" }
                    }));

                return true;
            }
            catch (Exception ex)
            {
                // All other exceptions are swallowed per telemetry requirement
                // These are already tracked by the circuit breaker
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.export.circuit_breaker_error",
                    tags: new ActivityTagsCollection
                    {
                        { "host", _host },
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name },
                        { "circuit_state", _circuitBreaker.State.ToString() }
                    }));

                return false;
            }
        }
    }

    /// <summary>
    /// Exception thrown when telemetry export fails.
    /// Used internally by CircuitBreakerTelemetryExporter to signal failures to the circuit breaker.
    /// </summary>
    internal sealed class TelemetryExportException : Exception
    {
        /// <summary>
        /// Creates a new TelemetryExportException.
        /// </summary>
        /// <param name="message">The error message.</param>
        public TelemetryExportException(string message) : base(message)
        {
        }

        /// <summary>
        /// Creates a new TelemetryExportException.
        /// </summary>
        /// <param name="message">The error message.</param>
        /// <param name="innerException">The inner exception.</param>
        public TelemetryExportException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
