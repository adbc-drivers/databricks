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
    /// Configuration for the circuit breaker pattern.
    /// </summary>
    /// <remarks>
    /// The circuit breaker protects against failing telemetry endpoints by:
    /// - Opening after a threshold of consecutive failures
    /// - Allowing test requests after a timeout period
    /// - Closing after a threshold of successful requests
    /// </remarks>
    public sealed class CircuitBreakerConfig
    {
        /// <summary>
        /// Gets or sets the number of consecutive failures before the circuit opens.
        /// Default is 5.
        /// </summary>
        public int FailureThreshold { get; set; } = 5;

        /// <summary>
        /// Gets or sets the timeout duration before transitioning from Open to HalfOpen state.
        /// After this timeout, the circuit breaker allows test requests.
        /// Default is 1 minute.
        /// </summary>
        public TimeSpan Timeout { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Gets or sets the number of consecutive successes in HalfOpen state before closing.
        /// Default is 2.
        /// </summary>
        public int SuccessThreshold { get; set; } = 2;

        /// <summary>
        /// Creates a CircuitBreakerConfig with default values.
        /// </summary>
        public CircuitBreakerConfig()
        {
        }

        /// <summary>
        /// Creates a CircuitBreakerConfig from TelemetryConfiguration settings.
        /// </summary>
        /// <param name="telemetryConfig">The telemetry configuration to read from.</param>
        /// <returns>A new CircuitBreakerConfig instance.</returns>
        public static CircuitBreakerConfig FromTelemetryConfiguration(TelemetryConfiguration telemetryConfig)
        {
            if (telemetryConfig == null)
            {
                throw new ArgumentNullException(nameof(telemetryConfig));
            }

            return new CircuitBreakerConfig
            {
                FailureThreshold = telemetryConfig.CircuitBreakerThreshold,
                Timeout = telemetryConfig.CircuitBreakerTimeout,
                // SuccessThreshold uses default value (2) as it's not in TelemetryConfiguration
            };
        }
    }
}
