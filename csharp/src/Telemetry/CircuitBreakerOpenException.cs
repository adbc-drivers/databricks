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

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Exception thrown when the circuit breaker is in Open state and rejects a request.
    /// </summary>
    /// <remarks>
    /// This exception indicates that the circuit breaker is protecting the system
    /// by rejecting requests due to previous failures. The circuit will transition
    /// to HalfOpen state after the configured timeout to test if the downstream
    /// service has recovered.
    /// </remarks>
    public sealed class CircuitBreakerOpenException : Exception
    {
        /// <summary>
        /// Gets the time when the circuit breaker opened.
        /// </summary>
        public DateTime OpenedAt { get; }

        /// <summary>
        /// Gets the timeout duration before the circuit breaker transitions to HalfOpen state.
        /// </summary>
        public TimeSpan Timeout { get; }

        /// <summary>
        /// Gets the estimated time when the circuit breaker will transition to HalfOpen state.
        /// </summary>
        public DateTime EstimatedRecoveryTime => OpenedAt + Timeout;

        /// <summary>
        /// Creates a new instance of CircuitBreakerOpenException with default message.
        /// </summary>
        public CircuitBreakerOpenException()
            : base("Circuit breaker is open and rejecting requests.")
        {
            OpenedAt = DateTime.UtcNow;
            Timeout = TimeSpan.FromMinutes(1);
        }

        /// <summary>
        /// Creates a new instance of CircuitBreakerOpenException with the specified message.
        /// </summary>
        /// <param name="message">The exception message.</param>
        public CircuitBreakerOpenException(string message)
            : base(message)
        {
            OpenedAt = DateTime.UtcNow;
            Timeout = TimeSpan.FromMinutes(1);
        }

        /// <summary>
        /// Creates a new instance of CircuitBreakerOpenException with state information.
        /// </summary>
        /// <param name="openedAt">The time when the circuit breaker opened.</param>
        /// <param name="timeout">The timeout duration before transitioning to HalfOpen state.</param>
        public CircuitBreakerOpenException(DateTime openedAt, TimeSpan timeout)
            : base($"Circuit breaker is open. Opened at {openedAt:O}. Will retry after {timeout.TotalSeconds:F0} seconds.")
        {
            OpenedAt = openedAt;
            Timeout = timeout;
        }

        /// <summary>
        /// Creates a new instance of CircuitBreakerOpenException with message and inner exception.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="innerException">The inner exception.</param>
        public CircuitBreakerOpenException(string message, Exception innerException)
            : base(message, innerException)
        {
            OpenedAt = DateTime.UtcNow;
            Timeout = TimeSpan.FromMinutes(1);
        }
    }
}
