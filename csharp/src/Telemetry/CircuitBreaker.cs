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
using System.Threading;
using System.Threading.Tasks;
using Polly;
using Polly.CircuitBreaker;

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
    /// Implements the circuit breaker pattern using Polly to protect against failing services.
    /// </summary>
    /// <remarks>
    /// The circuit breaker has three main states:
    /// - Closed: Normal operation, requests pass through, failures are tracked.
    /// - Open: After threshold failures, all requests are rejected immediately.
    /// - HalfOpen: After timeout, allows test requests to check if service recovered.
    ///
    /// This implementation wraps Polly's circuit breaker for battle-tested resilience.
    /// </remarks>
    internal sealed class CircuitBreaker
    {
        private readonly ResiliencePipeline _pipeline;
        private readonly CircuitBreakerManualControl _manualControl;
        private readonly CircuitBreakerConfig _config;

        // Track state transitions for the State property
        private volatile CircuitBreakerState _lastKnownState = CircuitBreakerState.Closed;
        private readonly object _stateLock = new object();

        /// <summary>
        /// Gets the current state of the circuit breaker.
        /// </summary>
        public CircuitBreakerState State => _lastKnownState;

        /// <summary>
        /// Gets the configuration used by this circuit breaker.
        /// </summary>
        public CircuitBreakerConfig Config => _config;

        /// <summary>
        /// Creates a new circuit breaker with the specified configuration.
        /// </summary>
        /// <param name="config">The configuration for this circuit breaker.</param>
        /// <exception cref="ArgumentNullException">Thrown when config is null.</exception>
        public CircuitBreaker(CircuitBreakerConfig config)
            : this(config?.FailureThreshold ?? CircuitBreakerConfig.DefaultFailureThreshold,
                   config?.Timeout ?? CircuitBreakerConfig.DefaultTimeout,
                   config)
        {
        }

        /// <summary>
        /// Creates a new circuit breaker with the specified parameters.
        /// </summary>
        /// <param name="failureThreshold">Number of failures before the circuit opens. Default is 5.</param>
        /// <param name="timeout">Duration the circuit stays open before transitioning to half-open. Default is 1 minute.</param>
        public CircuitBreaker(int failureThreshold = CircuitBreakerConfig.DefaultFailureThreshold, TimeSpan? timeout = null)
            : this(failureThreshold, timeout, null)
        {
        }

        /// <summary>
        /// Private constructor that does the actual initialization.
        /// </summary>
        private CircuitBreaker(int failureThreshold, TimeSpan? timeout, CircuitBreakerConfig? config)
        {
            _config = config ?? new CircuitBreakerConfig
            {
                FailureThreshold = failureThreshold,
                Timeout = timeout ?? CircuitBreakerConfig.DefaultTimeout
            };
            var actualTimeout = _config.Timeout;
            _manualControl = new CircuitBreakerManualControl();

            // Polly has minimum constraints: MinimumThroughput >= 2, BreakDuration >= 500ms
            var minimumThroughput = Math.Max(2, _config.FailureThreshold);
            var breakDuration = actualTimeout < TimeSpan.FromMilliseconds(500)
                ? TimeSpan.FromMilliseconds(500)
                : actualTimeout;

            var options = new CircuitBreakerStrategyOptions
            {
                // Use failure ratio of 1.0 (100%) with minimum throughput equal to failure threshold
                // This effectively makes it behave like consecutive failure counting
                FailureRatio = 1.0,
                MinimumThroughput = minimumThroughput,
                SamplingDuration = TimeSpan.FromSeconds(30),
                BreakDuration = breakDuration,
                ManualControl = _manualControl,
                OnOpened = args =>
                {
                    lock (_stateLock)
                    {
                        _lastKnownState = CircuitBreakerState.Open;
                    }
                    return default;
                },
                OnClosed = _ =>
                {
                    lock (_stateLock)
                    {
                        _lastKnownState = CircuitBreakerState.Closed;
                    }
                    return default;
                },
                OnHalfOpened = _ =>
                {
                    lock (_stateLock)
                    {
                        _lastKnownState = CircuitBreakerState.HalfOpen;
                    }
                    return default;
                }
            };

            _pipeline = new ResiliencePipelineBuilder()
                .AddCircuitBreaker(options)
                .Build();
        }

        /// <summary>
        /// Executes an action through the circuit breaker.
        /// </summary>
        /// <param name="action">The async action to execute.</param>
        /// <returns>A task representing the operation.</returns>
        /// <exception cref="BrokenCircuitException">Thrown when the circuit is open.</exception>
        public async Task ExecuteAsync(Func<Task> action)
        {
            if (action == null)
            {
                throw new ArgumentNullException(nameof(action));
            }

            await _pipeline.ExecuteAsync(async ct => await action().ConfigureAwait(false)).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes a function through the circuit breaker.
        /// </summary>
        /// <typeparam name="T">The return type.</typeparam>
        /// <param name="action">The async function to execute.</param>
        /// <returns>A task representing the operation with the result.</returns>
        /// <exception cref="BrokenCircuitException">Thrown when the circuit is open.</exception>
        public async Task<T> ExecuteAsync<T>(Func<Task<T>> action)
        {
            if (action == null)
            {
                throw new ArgumentNullException(nameof(action));
            }

            return await _pipeline.ExecuteAsync(async ct => await action().ConfigureAwait(false)).ConfigureAwait(false);
        }

        /// <summary>
        /// Resets the circuit breaker to Closed state.
        /// This is primarily for testing purposes.
        /// </summary>
        internal void Reset()
        {
            // Close the circuit if it's open
            _manualControl.CloseAsync().GetAwaiter().GetResult();
            lock (_stateLock)
            {
                _lastKnownState = CircuitBreakerState.Closed;
            }
        }
    }
}
