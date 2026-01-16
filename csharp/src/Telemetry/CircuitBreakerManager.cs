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
    /// Singleton manager that maintains one circuit breaker per host.
    /// Provides per-host isolation so failures on one host don't affect others.
    /// </summary>
    internal sealed class CircuitBreakerManager
    {
        private static readonly CircuitBreakerManager _instance = new CircuitBreakerManager();
        private readonly ConcurrentDictionary<string, CircuitBreaker> _circuitBreakers = new ConcurrentDictionary<string, CircuitBreaker>();
        private readonly CircuitBreakerConfig _defaultConfig = new CircuitBreakerConfig();

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
        public static CircuitBreakerManager GetInstance()
        {
            return _instance;
        }

        /// <summary>
        /// Gets or creates a circuit breaker for the specified host.
        /// Uses default configuration for new circuit breakers.
        /// </summary>
        /// <param name="host">The host to get a circuit breaker for.</param>
        /// <returns>A CircuitBreaker instance for the host.</returns>
        public CircuitBreaker GetCircuitBreaker(string host)
        {
            if (string.IsNullOrEmpty(host))
            {
                throw new ArgumentException("Host cannot be null or empty", nameof(host));
            }

            return _circuitBreakers.GetOrAdd(host, h => new CircuitBreaker(h, _defaultConfig));
        }

        /// <summary>
        /// Gets or creates a circuit breaker for the specified host with custom configuration.
        /// </summary>
        /// <param name="host">The host to get a circuit breaker for.</param>
        /// <param name="config">The circuit breaker configuration to use.</param>
        /// <returns>A CircuitBreaker instance for the host.</returns>
        public CircuitBreaker GetCircuitBreaker(string host, CircuitBreakerConfig config)
        {
            if (string.IsNullOrEmpty(host))
            {
                throw new ArgumentException("Host cannot be null or empty", nameof(host));
            }

            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            return _circuitBreakers.GetOrAdd(host, h => new CircuitBreaker(h, config));
        }

        /// <summary>
        /// Removes the circuit breaker for the specified host.
        /// Used for testing purposes.
        /// </summary>
        /// <param name="host">The host to remove the circuit breaker for.</param>
        /// <returns>True if the circuit breaker was removed; false if it didn't exist.</returns>
        internal bool RemoveCircuitBreaker(string host)
        {
            if (string.IsNullOrEmpty(host))
            {
                return false;
            }

            return _circuitBreakers.TryRemove(host, out _);
        }

        /// <summary>
        /// Clears all circuit breakers.
        /// Used for testing purposes.
        /// </summary>
        internal void Clear()
        {
            _circuitBreakers.Clear();
        }
    }
}
