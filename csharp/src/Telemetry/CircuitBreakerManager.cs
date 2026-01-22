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
using System.Diagnostics;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Singleton that manages circuit breakers per host.
    /// </summary>
    /// <remarks>
    /// This class implements the per-host circuit breaker pattern from the JDBC driver:
    /// - Each host gets its own circuit breaker for isolation
    /// - One failing endpoint does not affect other endpoints
    /// - Thread-safe using ConcurrentDictionary
    ///
    /// JDBC Reference: CircuitBreakerManager.java:25
    /// </remarks>
    internal sealed class CircuitBreakerManager
    {
        private static readonly CircuitBreakerManager s_instance = new CircuitBreakerManager();

        private readonly ConcurrentDictionary<string, CircuitBreaker> _circuitBreakers;
        private readonly CircuitBreakerConfig _defaultConfig;

        /// <summary>
        /// Gets the singleton instance of the CircuitBreakerManager.
        /// </summary>
        public static CircuitBreakerManager GetInstance() => s_instance;

        /// <summary>
        /// Creates a new CircuitBreakerManager with default configuration.
        /// </summary>
        internal CircuitBreakerManager()
            : this(new CircuitBreakerConfig())
        {
        }

        /// <summary>
        /// Creates a new CircuitBreakerManager with the specified default configuration.
        /// </summary>
        /// <param name="defaultConfig">The default configuration for new circuit breakers.</param>
        internal CircuitBreakerManager(CircuitBreakerConfig defaultConfig)
        {
            _circuitBreakers = new ConcurrentDictionary<string, CircuitBreaker>(StringComparer.OrdinalIgnoreCase);
            _defaultConfig = defaultConfig ?? throw new ArgumentNullException(nameof(defaultConfig));
        }

        /// <summary>
        /// Gets or creates a circuit breaker for the specified host.
        /// </summary>
        /// <param name="host">The host (Databricks workspace URL) to get or create a circuit breaker for.</param>
        /// <returns>The circuit breaker for the host.</returns>
        /// <exception cref="ArgumentException">Thrown when host is null or whitespace.</exception>
        /// <remarks>
        /// This method is thread-safe. If multiple threads call this method simultaneously
        /// for the same host, they will all receive the same circuit breaker instance.
        /// The circuit breaker is created lazily on first access.
        /// </remarks>
        public CircuitBreaker GetCircuitBreaker(string host)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            var circuitBreaker = _circuitBreakers.GetOrAdd(host, _ =>
            {
                Debug.WriteLine($"[DEBUG] CircuitBreakerManager: Creating circuit breaker for host '{host}'");
                return new CircuitBreaker(_defaultConfig);
            });

            return circuitBreaker;
        }

        /// <summary>
        /// Gets or creates a circuit breaker for the specified host with custom configuration.
        /// </summary>
        /// <param name="host">The host (Databricks workspace URL) to get or create a circuit breaker for.</param>
        /// <param name="config">The configuration to use for this host's circuit breaker.</param>
        /// <returns>The circuit breaker for the host.</returns>
        /// <exception cref="ArgumentException">Thrown when host is null or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown when config is null.</exception>
        /// <remarks>
        /// Note: If a circuit breaker already exists for the host, the existing instance
        /// with its original configuration is returned. The provided config is only used
        /// when creating a new circuit breaker.
        /// </remarks>
        public CircuitBreaker GetCircuitBreaker(string host, CircuitBreakerConfig config)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            var circuitBreaker = _circuitBreakers.GetOrAdd(host, _ =>
            {
                Debug.WriteLine($"[DEBUG] CircuitBreakerManager: Creating circuit breaker for host '{host}' with custom config");
                return new CircuitBreaker(config);
            });

            return circuitBreaker;
        }

        /// <summary>
        /// Gets the number of hosts with circuit breakers.
        /// </summary>
        internal int CircuitBreakerCount => _circuitBreakers.Count;

        /// <summary>
        /// Checks if a circuit breaker exists for the specified host.
        /// </summary>
        /// <param name="host">The host to check.</param>
        /// <returns>True if a circuit breaker exists, false otherwise.</returns>
        internal bool HasCircuitBreaker(string host)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                return false;
            }

            return _circuitBreakers.ContainsKey(host);
        }

        /// <summary>
        /// Tries to get an existing circuit breaker for the specified host.
        /// Does not create a new circuit breaker if one doesn't exist.
        /// </summary>
        /// <param name="host">The host to get the circuit breaker for.</param>
        /// <param name="circuitBreaker">The circuit breaker if found, null otherwise.</param>
        /// <returns>True if the circuit breaker was found, false otherwise.</returns>
        internal bool TryGetCircuitBreaker(string host, out CircuitBreaker? circuitBreaker)
        {
            circuitBreaker = null;

            if (string.IsNullOrWhiteSpace(host))
            {
                return false;
            }

            if (_circuitBreakers.TryGetValue(host, out var foundCircuitBreaker))
            {
                circuitBreaker = foundCircuitBreaker;
                return true;
            }

            return false;
        }

        /// <summary>
        /// Removes the circuit breaker for the specified host.
        /// </summary>
        /// <param name="host">The host to remove the circuit breaker for.</param>
        /// <returns>True if the circuit breaker was removed, false if it didn't exist.</returns>
        internal bool RemoveCircuitBreaker(string host)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                return false;
            }

            var removed = _circuitBreakers.TryRemove(host, out _);

            if (removed)
            {
                Debug.WriteLine($"[DEBUG] CircuitBreakerManager: Removed circuit breaker for host '{host}'");
            }

            return removed;
        }

        /// <summary>
        /// Clears all circuit breakers.
        /// This is primarily for testing purposes.
        /// </summary>
        internal void Clear()
        {
            _circuitBreakers.Clear();
            Debug.WriteLine("[DEBUG] CircuitBreakerManager: Cleared all circuit breakers");
        }
    }
}
