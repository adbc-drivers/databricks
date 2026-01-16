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
    /// Configuration for circuit breaker behavior.
    /// Defines thresholds for opening the circuit, timeout for recovery attempts,
    /// and success threshold for closing the circuit.
    /// </summary>
    internal sealed class CircuitBreakerConfig
    {
        /// <summary>
        /// Gets or sets the number of consecutive failures before opening the circuit.
        /// Default is 5.
        /// </summary>
        public int FailureThreshold { get; set; } = 5;

        /// <summary>
        /// Gets or sets the timeout duration before transitioning from Open to Half-Open state.
        /// Default is 1 minute.
        /// </summary>
        public TimeSpan Timeout { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Gets or sets the number of consecutive successes in Half-Open state required to close the circuit.
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
        /// Creates a CircuitBreakerConfig from TelemetryConfiguration.
        /// </summary>
        /// <param name="telemetryConfig">The telemetry configuration containing circuit breaker settings.</param>
        /// <returns>A CircuitBreakerConfig with values from the telemetry configuration.</returns>
        public static CircuitBreakerConfig FromTelemetryConfiguration(TelemetryConfiguration telemetryConfig)
        {
            return new CircuitBreakerConfig
            {
                FailureThreshold = telemetryConfig.CircuitBreakerThreshold,
                Timeout = telemetryConfig.CircuitBreakerTimeout,
                SuccessThreshold = 2 // Default, not exposed in TelemetryConfiguration
            };
        }
    }
}
