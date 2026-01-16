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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Represents the state of a circuit breaker.
    /// </summary>
    internal enum CircuitBreakerState
    {
        /// <summary>
        /// Normal operation - requests pass through.
        /// </summary>
        Closed,

        /// <summary>
        /// Circuit is open - requests are rejected immediately.
        /// </summary>
        Open,

        /// <summary>
        /// Testing recovery - limited requests allowed to test if service recovered.
        /// </summary>
        HalfOpen
    }

    /// <summary>
    /// Exception thrown when circuit breaker is in Open state and rejects requests.
    /// </summary>
    internal sealed class CircuitBreakerOpenException : Exception
    {
        public CircuitBreakerOpenException(string message) : base(message)
        {
        }
    }

    /// <summary>
    /// Implements circuit breaker pattern to protect against failing telemetry endpoint.
    /// Three states: Closed (normal), Open (reject requests), Half-Open (test recovery).
    /// State transitions are logged at DEBUG level.
    /// </summary>
    internal sealed class CircuitBreaker
    {
        private readonly CircuitBreakerConfig _config;
        private readonly string _host;
        private readonly object _stateLock = new object();

        private CircuitBreakerState _state = CircuitBreakerState.Closed;
        private int _consecutiveFailures = 0;
        private int _consecutiveSuccesses = 0;
        private DateTime _lastFailureTime = DateTime.MinValue;

        /// <summary>
        /// Gets the current circuit breaker configuration.
        /// </summary>
        public CircuitBreakerConfig Config => _config;

        /// <summary>
        /// Gets the current state of the circuit breaker.
        /// </summary>
        public CircuitBreakerState State
        {
            get
            {
                lock (_stateLock)
                {
                    return _state;
                }
            }
        }

        /// <summary>
        /// Creates a new circuit breaker instance.
        /// </summary>
        /// <param name="host">The host this circuit breaker protects.</param>
        /// <param name="config">Circuit breaker configuration.</param>
        public CircuitBreaker(string host, CircuitBreakerConfig config)
        {
            _host = host ?? throw new ArgumentNullException(nameof(host));
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        /// <summary>
        /// Executes an async action through the circuit breaker.
        /// </summary>
        /// <param name="action">The async action to execute.</param>
        /// <returns>A task representing the async operation.</returns>
        /// <exception cref="CircuitBreakerOpenException">Thrown when circuit is open.</exception>
        public async Task ExecuteAsync(Func<Task> action)
        {
            // Check if circuit is open before attempting execution
            lock (_stateLock)
            {
                if (_state == CircuitBreakerState.Open)
                {
                    // Check if timeout has elapsed to transition to Half-Open
                    if (DateTime.UtcNow - _lastFailureTime >= _config.Timeout)
                    {
                        TransitionToHalfOpen();
                    }
                    else
                    {
                        // Circuit still open, reject request
                        throw new CircuitBreakerOpenException(
                            $"Circuit breaker is OPEN for host {_host}. Rejecting telemetry request.");
                    }
                }
            }

            // Execute the action and track success/failure
            try
            {
                await action().ConfigureAwait(false);
                OnSuccess();
            }
            catch (Exception)
            {
                OnFailure();
                throw; // Re-throw to allow circuit breaker wrapper to see the exception
            }
        }

        /// <summary>
        /// Records a successful execution.
        /// </summary>
        private void OnSuccess()
        {
            lock (_stateLock)
            {
                _consecutiveFailures = 0;

                if (_state == CircuitBreakerState.HalfOpen)
                {
                    _consecutiveSuccesses++;
                    if (_consecutiveSuccesses >= _config.SuccessThreshold)
                    {
                        TransitionToClosed();
                    }
                }
            }
        }

        /// <summary>
        /// Records a failed execution.
        /// </summary>
        private void OnFailure()
        {
            lock (_stateLock)
            {
                _consecutiveFailures++;
                _consecutiveSuccesses = 0;
                _lastFailureTime = DateTime.UtcNow;

                if (_state == CircuitBreakerState.Closed && _consecutiveFailures >= _config.FailureThreshold)
                {
                    TransitionToOpen();
                }
                else if (_state == CircuitBreakerState.HalfOpen)
                {
                    // Any failure in Half-Open transitions back to Open
                    TransitionToOpen();
                }
            }
        }

        /// <summary>
        /// Transitions the circuit breaker to Open state.
        /// Must be called within _stateLock.
        /// </summary>
        private void TransitionToOpen()
        {
            _state = CircuitBreakerState.Open;
            _consecutiveSuccesses = 0;
            Debug.WriteLine($"[DEBUG] Circuit breaker OPEN for host {_host} after {_consecutiveFailures} failures. " +
                          $"Will retry after {_config.Timeout.TotalSeconds}s.");
        }

        /// <summary>
        /// Transitions the circuit breaker to Half-Open state.
        /// Must be called within _stateLock.
        /// </summary>
        private void TransitionToHalfOpen()
        {
            _state = CircuitBreakerState.HalfOpen;
            _consecutiveSuccesses = 0;
            _consecutiveFailures = 0;
            Debug.WriteLine($"[DEBUG] Circuit breaker HALF-OPEN for host {_host}. Testing recovery...");
        }

        /// <summary>
        /// Transitions the circuit breaker to Closed state.
        /// Must be called within _stateLock.
        /// </summary>
        private void TransitionToClosed()
        {
            _state = CircuitBreakerState.Closed;
            _consecutiveSuccesses = 0;
            _consecutiveFailures = 0;
            Debug.WriteLine($"[DEBUG] Circuit breaker CLOSED for host {_host}. Service recovered.");
        }

        /// <summary>
        /// Resets the circuit breaker to Closed state.
        /// Used for testing purposes.
        /// </summary>
        internal void Reset()
        {
            lock (_stateLock)
            {
                _state = CircuitBreakerState.Closed;
                _consecutiveFailures = 0;
                _consecutiveSuccesses = 0;
                _lastFailureTime = DateTime.MinValue;
            }
        }
    }
}
