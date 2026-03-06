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
using System.Collections.Concurrent;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Singleton factory that manages one circuit breaker per host.
    /// Ensures proper isolation by preventing circuit breaker state from being shared across different hosts.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When multiple connections are made to different hosts, each host should have its own circuit breaker
    /// to isolate failures. If one host is experiencing issues, connections to other healthy hosts should
    /// not be affected by the same circuit breaker state.
    /// </para>
    /// <para>
    /// This manager maintains a single CircuitBreaker instance per host, shared across all connections
    /// to that host. Each circuit breaker uses default configuration: 5 failures threshold, 1 minute timeout,
    /// and 2 successes to close.
    /// </para>
    /// <para>
    /// Thread Safety: All methods are thread-safe and can be called concurrently from multiple
    /// connections. Uses ConcurrentDictionary for synchronization.
    /// </para>
    /// </remarks>
    internal sealed class CircuitBreakerManager
    {
        private static readonly CircuitBreakerManager s_instance = new CircuitBreakerManager();

        private readonly ConcurrentDictionary<string, CircuitBreaker> _circuitBreakers = new ConcurrentDictionary<string, CircuitBreaker>();

        /// <summary>
        /// Private constructor to enforce singleton pattern.
        /// </summary>
        private CircuitBreakerManager()
        {
        }

        /// <summary>
        /// Gets the singleton instance of CircuitBreakerManager.
        /// </summary>
        /// <returns>The singleton CircuitBreakerManager instance.</returns>
        public static CircuitBreakerManager GetInstance() => s_instance;

        /// <summary>
        /// Gets or creates a circuit breaker for the specified host.
        /// </summary>
        /// <param name="host">The host identifier (e.g., "databricks-workspace.cloud.databricks.com").</param>
        /// <returns>The circuit breaker for the specified host.</returns>
        /// <remarks>
        /// <para>
        /// Thread Safety: This method is thread-safe. If multiple connections call this method
        /// concurrently for the same host, only one circuit breaker will be created due to
        /// ConcurrentDictionary's atomic GetOrAdd operation.
        /// </para>
        /// <para>
        /// The circuit breaker uses default configuration:
        /// - Failure threshold: 5 consecutive failures
        /// - Timeout: 1 minute (circuit stays open)
        /// - Success threshold: 2 successful calls to close the circuit in half-open state
        /// </para>
        /// </remarks>
        public CircuitBreaker GetCircuitBreaker(string host)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            return _circuitBreakers.GetOrAdd(host, _ => new CircuitBreaker());
        }

        /// <summary>
        /// Removes the circuit breaker for the specified host.
        /// Called when the last client for a host is released to prevent memory leaks.
        /// </summary>
        /// <param name="host">The host identifier.</param>
        public void RemoveCircuitBreaker(string host)
        {
            _circuitBreakers.TryRemove(host, out _);
        }
    }
}
