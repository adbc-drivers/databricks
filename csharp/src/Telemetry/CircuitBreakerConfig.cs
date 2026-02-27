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
    /// Configuration options for a circuit breaker.
    /// </summary>
    /// <remarks>
    /// This class encapsulates all configurable parameters for the circuit breaker pattern,
    /// allowing for consistent configuration across the application.
    ///
    /// JDBC Reference: CircuitBreakerConfig.java
    /// </remarks>
    internal sealed class CircuitBreakerConfig
    {
        /// <summary>
        /// Default number of failures before the circuit opens.
        /// </summary>
        public const int DefaultFailureThreshold = 5;

        /// <summary>
        /// Default duration the circuit stays open before transitioning to half-open.
        /// </summary>
        public static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Default number of consecutive successes required in half-open state to close the circuit.
        /// </summary>
        public const int DefaultSuccessThreshold = 2;

        /// <summary>
        /// Gets or sets the number of failures before the circuit opens.
        /// </summary>
        /// <value>Default is 5.</value>
        public int FailureThreshold { get; set; } = DefaultFailureThreshold;

        /// <summary>
        /// Gets or sets the duration the circuit stays open before transitioning to half-open.
        /// </summary>
        /// <value>Default is 1 minute.</value>
        public TimeSpan Timeout { get; set; } = DefaultTimeout;

        /// <summary>
        /// Gets or sets the number of consecutive successes required in half-open state to close the circuit.
        /// </summary>
        /// <value>Default is 2.</value>
        public int SuccessThreshold { get; set; } = DefaultSuccessThreshold;
    }
}
