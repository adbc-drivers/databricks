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
    /// Singleton that manages one <see cref="CircuitBreaker"/> instance per host.
    /// Prevents creating duplicate circuit breakers when multiple connections target the same host.
    /// </summary>
    /// <remarks>
    /// This manager uses a <see cref="ConcurrentDictionary{TKey, TValue}"/> internally to ensure
    /// thread-safe access from multiple connections. The same host always returns the same
    /// <see cref="CircuitBreaker"/> instance, while different hosts get separate instances
    /// for per-host isolation.
    /// </remarks>
    internal sealed class CircuitBreakerManager
    {
        private static readonly CircuitBreakerManager s_instance = new CircuitBreakerManager();

        private readonly ConcurrentDictionary<string, CircuitBreaker> _circuitBreakers =
            new ConcurrentDictionary<string, CircuitBreaker>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Private constructor to enforce singleton pattern.
        /// </summary>
        private CircuitBreakerManager()
        {
        }

        /// <summary>
        /// Gets the singleton instance of <see cref="CircuitBreakerManager"/>.
        /// </summary>
        /// <returns>The singleton <see cref="CircuitBreakerManager"/> instance.</returns>
        public static CircuitBreakerManager GetInstance() => s_instance;

        /// <summary>
        /// Gets an existing <see cref="CircuitBreaker"/> for the specified host,
        /// or creates a new one with default configuration if none exists.
        /// </summary>
        /// <param name="host">The host identifier (e.g., "workspace.databricks.com").</param>
        /// <returns>The <see cref="CircuitBreaker"/> instance for the specified host.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="host"/> is null.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="host"/> is empty or whitespace.</exception>
        public CircuitBreaker GetCircuitBreaker(string host)
        {
            ValidateHost(host);
            return _circuitBreakers.GetOrAdd(host, _ => new CircuitBreaker());
        }

        /// <summary>
        /// Gets an existing <see cref="CircuitBreaker"/> for the specified host,
        /// or creates a new one with the specified configuration if none exists.
        /// If a breaker already exists for the host, the configuration parameters are ignored.
        /// </summary>
        /// <param name="host">The host identifier (e.g., "workspace.databricks.com").</param>
        /// <param name="failureThreshold">Number of failures before the circuit opens.</param>
        /// <param name="timeout">Duration the circuit stays open before transitioning to half-open.</param>
        /// <returns>The <see cref="CircuitBreaker"/> instance for the specified host.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="host"/> is null.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="host"/> is empty or whitespace.</exception>
        public CircuitBreaker GetCircuitBreaker(string host, int failureThreshold, TimeSpan timeout)
        {
            ValidateHost(host);
            return _circuitBreakers.GetOrAdd(host, _ => new CircuitBreaker(failureThreshold, timeout));
        }

        /// <summary>
        /// Removes the circuit breaker for the specified host, if one exists.
        /// This is intended for cleanup when the last connection to a host is closed.
        /// </summary>
        /// <param name="host">The host identifier.</param>
        /// <returns><c>true</c> if a circuit breaker was removed; otherwise, <c>false</c>.</returns>
        internal bool RemoveCircuitBreaker(string host)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                return false;
            }

            return _circuitBreakers.TryRemove(host, out _);
        }

        /// <summary>
        /// Resets the manager by removing all circuit breakers.
        /// This is primarily intended for testing purposes.
        /// </summary>
        internal void Reset()
        {
            _circuitBreakers.Clear();
        }

        private static void ValidateHost(string host)
        {
            if (host == null)
            {
                throw new ArgumentNullException(nameof(host));
            }

            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be empty or whitespace.", nameof(host));
            }
        }
    }
}
