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
    /// Represents the state of the circuit breaker.
    /// </summary>
    internal enum CircuitBreakerState
    {
        /// <summary>
        /// Normal operation. Requests pass through and failures are tracked.
        /// </summary>
        Closed = 0,

        /// <summary>
        /// Circuit is open due to failures. All requests are rejected immediately.
        /// </summary>
        Open = 1,

        /// <summary>
        /// Testing recovery. Limited requests are allowed through to test if service has recovered.
        /// </summary>
        HalfOpen = 2
    }

    /// <summary>
    /// Implements the circuit breaker pattern to protect against failing services.
    /// </summary>
    /// <remarks>
    /// The circuit breaker has three states:
    /// - Closed: Normal operation, requests pass through, failures are tracked.
    /// - Open: After threshold failures, all requests are rejected immediately.
    /// - HalfOpen: After timeout, allows test requests to check if service recovered.
    ///
    /// State transitions:
    /// - Closed -> Open: When consecutive failures reach threshold.
    /// - Open -> HalfOpen: After timeout duration expires.
    /// - HalfOpen -> Closed: After success threshold is reached.
    /// - HalfOpen -> Open: If a request fails in HalfOpen state.
    ///
    /// Thread-safety is ensured using Interlocked operations.
    /// </remarks>
    internal sealed class CircuitBreaker
    {
        private readonly CircuitBreakerConfig _config;

        // State is stored as int for thread-safe Interlocked operations
        private int _state = (int)CircuitBreakerState.Closed;

        // Counters for tracking failures and successes
        private int _consecutiveFailures;
        private int _consecutiveSuccesses;

        // Timestamp when the circuit opened (for timeout calculation)
        private long _openedAtTicks;

        /// <summary>
        /// Gets the current configuration of the circuit breaker.
        /// </summary>
        public CircuitBreakerConfig Config => _config;

        /// <summary>
        /// Gets the current state of the circuit breaker.
        /// </summary>
        public CircuitBreakerState State => (CircuitBreakerState)Volatile.Read(ref _state);

        /// <summary>
        /// Gets the number of consecutive failures in Closed state.
        /// </summary>
        internal int ConsecutiveFailures => Volatile.Read(ref _consecutiveFailures);

        /// <summary>
        /// Gets the number of consecutive successes in HalfOpen state.
        /// </summary>
        internal int ConsecutiveSuccesses => Volatile.Read(ref _consecutiveSuccesses);

        /// <summary>
        /// Creates a new circuit breaker with default configuration.
        /// </summary>
        public CircuitBreaker()
            : this(new CircuitBreakerConfig())
        {
        }

        /// <summary>
        /// Creates a new circuit breaker with the specified configuration.
        /// </summary>
        /// <param name="config">The circuit breaker configuration.</param>
        public CircuitBreaker(CircuitBreakerConfig config)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        /// <summary>
        /// Executes an action through the circuit breaker.
        /// </summary>
        /// <param name="action">The async action to execute.</param>
        /// <returns>A task representing the operation.</returns>
        /// <exception cref="CircuitBreakerOpenException">Thrown when the circuit is open.</exception>
        public async Task ExecuteAsync(Func<Task> action)
        {
            if (action == null)
            {
                throw new ArgumentNullException(nameof(action));
            }

            // Check if we can execute
            if (!CanExecute())
            {
                var openedAt = new DateTime(Volatile.Read(ref _openedAtTicks), DateTimeKind.Utc);
                throw new CircuitBreakerOpenException(openedAt, _config.Timeout);
            }

            try
            {
                await action().ConfigureAwait(false);
                OnSuccess();
            }
            catch (Exception)
            {
                OnFailure();
                throw;
            }
        }

        /// <summary>
        /// Executes a function through the circuit breaker.
        /// </summary>
        /// <typeparam name="T">The return type.</typeparam>
        /// <param name="action">The async function to execute.</param>
        /// <returns>A task representing the operation with the result.</returns>
        /// <exception cref="CircuitBreakerOpenException">Thrown when the circuit is open.</exception>
        public async Task<T> ExecuteAsync<T>(Func<Task<T>> action)
        {
            if (action == null)
            {
                throw new ArgumentNullException(nameof(action));
            }

            // Check if we can execute
            if (!CanExecute())
            {
                var openedAt = new DateTime(Volatile.Read(ref _openedAtTicks), DateTimeKind.Utc);
                throw new CircuitBreakerOpenException(openedAt, _config.Timeout);
            }

            try
            {
                T result = await action().ConfigureAwait(false);
                OnSuccess();
                return result;
            }
            catch (Exception)
            {
                OnFailure();
                throw;
            }
        }

        /// <summary>
        /// Determines if a request can be executed based on current state.
        /// Also handles state transitions from Open to HalfOpen when timeout expires.
        /// </summary>
        private bool CanExecute()
        {
            var currentState = State;

            switch (currentState)
            {
                case CircuitBreakerState.Closed:
                    return true;

                case CircuitBreakerState.Open:
                    // Check if timeout has expired
                    if (HasTimeoutExpired())
                    {
                        // Attempt to transition to HalfOpen
                        if (TryTransitionTo(CircuitBreakerState.Open, CircuitBreakerState.HalfOpen))
                        {
                            // Reset success counter for HalfOpen state
                            Interlocked.Exchange(ref _consecutiveSuccesses, 0);
                            Debug.WriteLine("[DEBUG] Circuit breaker transitioning from Open to HalfOpen");
                        }
                        return true;
                    }
                    return false;

                case CircuitBreakerState.HalfOpen:
                    return true;

                default:
                    return false;
            }
        }

        /// <summary>
        /// Records a successful execution.
        /// </summary>
        private void OnSuccess()
        {
            var currentState = State;

            switch (currentState)
            {
                case CircuitBreakerState.Closed:
                    // Reset failure counter on success
                    Interlocked.Exchange(ref _consecutiveFailures, 0);
                    break;

                case CircuitBreakerState.HalfOpen:
                    // Increment success counter
                    int successes = Interlocked.Increment(ref _consecutiveSuccesses);

                    // Check if success threshold reached
                    if (successes >= _config.SuccessThreshold)
                    {
                        if (TryTransitionTo(CircuitBreakerState.HalfOpen, CircuitBreakerState.Closed))
                        {
                            // Reset counters
                            Interlocked.Exchange(ref _consecutiveFailures, 0);
                            Interlocked.Exchange(ref _consecutiveSuccesses, 0);
                            Debug.WriteLine("[DEBUG] Circuit breaker transitioning from HalfOpen to Closed");
                        }
                    }
                    break;
            }
        }

        /// <summary>
        /// Records a failed execution.
        /// </summary>
        private void OnFailure()
        {
            var currentState = State;

            switch (currentState)
            {
                case CircuitBreakerState.Closed:
                    // Increment failure counter
                    int failures = Interlocked.Increment(ref _consecutiveFailures);

                    // Check if failure threshold reached
                    if (failures >= _config.FailureThreshold)
                    {
                        if (TryTransitionTo(CircuitBreakerState.Closed, CircuitBreakerState.Open))
                        {
                            // Record when circuit opened
                            Interlocked.Exchange(ref _openedAtTicks, DateTime.UtcNow.Ticks);
                            Debug.WriteLine("[DEBUG] Circuit breaker transitioning from Closed to Open");
                        }
                    }
                    break;

                case CircuitBreakerState.HalfOpen:
                    // Any failure in HalfOpen immediately opens the circuit
                    if (TryTransitionTo(CircuitBreakerState.HalfOpen, CircuitBreakerState.Open))
                    {
                        // Record when circuit opened
                        Interlocked.Exchange(ref _openedAtTicks, DateTime.UtcNow.Ticks);
                        // Reset counters
                        Interlocked.Exchange(ref _consecutiveSuccesses, 0);
                        Debug.WriteLine("[DEBUG] Circuit breaker transitioning from HalfOpen to Open due to failure");
                    }
                    break;
            }
        }

        /// <summary>
        /// Attempts to transition from one state to another atomically.
        /// </summary>
        private bool TryTransitionTo(CircuitBreakerState expectedState, CircuitBreakerState newState)
        {
            int expected = (int)expectedState;
            int desired = (int)newState;
            return Interlocked.CompareExchange(ref _state, desired, expected) == expected;
        }

        /// <summary>
        /// Checks if the timeout has expired since the circuit opened.
        /// </summary>
        private bool HasTimeoutExpired()
        {
            long openedTicks = Volatile.Read(ref _openedAtTicks);
            if (openedTicks == 0)
            {
                return false;
            }

            var openedAt = new DateTime(openedTicks, DateTimeKind.Utc);
            return DateTime.UtcNow - openedAt >= _config.Timeout;
        }

        /// <summary>
        /// Resets the circuit breaker to Closed state.
        /// This is primarily for testing purposes.
        /// </summary>
        internal void Reset()
        {
            Interlocked.Exchange(ref _state, (int)CircuitBreakerState.Closed);
            Interlocked.Exchange(ref _consecutiveFailures, 0);
            Interlocked.Exchange(ref _consecutiveSuccesses, 0);
            Interlocked.Exchange(ref _openedAtTicks, 0);
        }
    }
}
