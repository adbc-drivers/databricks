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
using System.Collections.Generic;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Configuration class for telemetry settings including batching, circuit breaker, and retry settings.
    /// </summary>
    public sealed class TelemetryConfiguration
    {
        /// <summary>
        /// Property key for telemetry enabled flag in connection properties.
        /// </summary>
        public const string PropertyKeyEnabled = "telemetry.enabled";

        /// <summary>
        /// Property key for batch size in connection properties.
        /// </summary>
        public const string PropertyKeyBatchSize = "telemetry.batch_size";

        /// <summary>
        /// Property key for flush interval in connection properties.
        /// </summary>
        public const string PropertyKeyFlushIntervalMs = "telemetry.flush_interval_ms";

        /// <summary>
        /// Property key for max retries in connection properties.
        /// </summary>
        public const string PropertyKeyMaxRetries = "telemetry.max_retries";

        /// <summary>
        /// Property key for retry delay in connection properties.
        /// </summary>
        public const string PropertyKeyRetryDelayMs = "telemetry.retry_delay_ms";

        /// <summary>
        /// Property key for circuit breaker enabled flag in connection properties.
        /// </summary>
        public const string PropertyKeyCircuitBreakerEnabled = "telemetry.circuit_breaker_enabled";

        /// <summary>
        /// Property key for circuit breaker failure threshold in connection properties.
        /// </summary>
        public const string PropertyKeyCircuitBreakerThreshold = "telemetry.circuit_breaker_threshold";

        /// <summary>
        /// Property key for circuit breaker timeout in connection properties.
        /// </summary>
        public const string PropertyKeyCircuitBreakerTimeoutMs = "telemetry.circuit_breaker_timeout_ms";

        /// <summary>
        /// Environment variable key for telemetry enabled flag.
        /// </summary>
        public const string EnvKeyEnabled = "DATABRICKS_TELEMETRY_ENABLED";

        /// <summary>
        /// Environment variable key for batch size.
        /// </summary>
        public const string EnvKeyBatchSize = "DATABRICKS_TELEMETRY_BATCH_SIZE";

        /// <summary>
        /// Environment variable key for flush interval.
        /// </summary>
        public const string EnvKeyFlushIntervalMs = "DATABRICKS_TELEMETRY_FLUSH_INTERVAL_MS";

        /// <summary>
        /// Feature flag name for server-side telemetry control.
        /// </summary>
        public const string FeatureFlagName = "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForAdbc";

        /// <summary>
        /// Gets or sets whether telemetry is enabled.
        /// Default is true.
        /// </summary>
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Gets or sets the batch size for telemetry metrics.
        /// Default is 100.
        /// </summary>
        public int BatchSize { get; set; } = 100;

        /// <summary>
        /// Gets or sets the flush interval in milliseconds.
        /// Default is 5000 (5 seconds).
        /// </summary>
        public int FlushIntervalMs { get; set; } = 5000;

        /// <summary>
        /// Gets or sets the maximum number of retries for telemetry export.
        /// Default is 3.
        /// </summary>
        public int MaxRetries { get; set; } = 3;

        /// <summary>
        /// Gets or sets the retry delay in milliseconds.
        /// Default is 100.
        /// </summary>
        public int RetryDelayMs { get; set; } = 100;

        /// <summary>
        /// Gets or sets whether the circuit breaker is enabled.
        /// Default is true.
        /// </summary>
        public bool CircuitBreakerEnabled { get; set; } = true;

        /// <summary>
        /// Gets or sets the circuit breaker failure threshold.
        /// After this many consecutive failures, the circuit breaker opens.
        /// Default is 5.
        /// </summary>
        public int CircuitBreakerThreshold { get; set; } = 5;

        /// <summary>
        /// Gets or sets the circuit breaker timeout.
        /// After this timeout, the circuit breaker transitions to half-open state.
        /// Default is 1 minute.
        /// </summary>
        public TimeSpan CircuitBreakerTimeout { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Creates a TelemetryConfiguration with default values.
        /// </summary>
        public TelemetryConfiguration()
        {
        }

        /// <summary>
        /// Validates the configuration and returns a list of validation errors.
        /// Returns an empty list if the configuration is valid.
        /// </summary>
        /// <returns>A list of validation error messages. Empty if valid.</returns>
        public IReadOnlyList<string> Validate()
        {
            var errors = new List<string>();

            if (BatchSize <= 0)
            {
                errors.Add($"{nameof(BatchSize)} must be greater than 0, but was {BatchSize}.");
            }

            if (FlushIntervalMs <= 0)
            {
                errors.Add($"{nameof(FlushIntervalMs)} must be greater than 0, but was {FlushIntervalMs}.");
            }

            if (MaxRetries < 0)
            {
                errors.Add($"{nameof(MaxRetries)} must be non-negative, but was {MaxRetries}.");
            }

            if (RetryDelayMs < 0)
            {
                errors.Add($"{nameof(RetryDelayMs)} must be non-negative, but was {RetryDelayMs}.");
            }

            if (CircuitBreakerThreshold <= 0)
            {
                errors.Add($"{nameof(CircuitBreakerThreshold)} must be greater than 0, but was {CircuitBreakerThreshold}.");
            }

            if (CircuitBreakerTimeout <= TimeSpan.Zero)
            {
                errors.Add($"{nameof(CircuitBreakerTimeout)} must be greater than zero, but was {CircuitBreakerTimeout}.");
            }

            return errors;
        }

        /// <summary>
        /// Creates a TelemetryConfiguration from connection properties.
        /// Properties override environment variables, which override defaults.
        /// Priority: Connection Properties > Environment Variables > Defaults.
        /// </summary>
        /// <param name="properties">Connection properties dictionary.</param>
        /// <returns>TelemetryConfiguration with values parsed from properties and environment.</returns>
        public static TelemetryConfiguration FromProperties(IReadOnlyDictionary<string, string>? properties)
        {
            var config = new TelemetryConfiguration();

            if (properties == null)
            {
                // Fall back to environment variables only
                config.ApplyEnvironmentVariables();
                return config;
            }

            // Parse from properties with fallback to environment variables and defaults
            config.Enabled = GetBooleanProperty(properties, PropertyKeyEnabled, EnvKeyEnabled, config.Enabled);
            config.BatchSize = GetPositiveIntProperty(properties, PropertyKeyBatchSize, EnvKeyBatchSize, config.BatchSize);
            config.FlushIntervalMs = GetPositiveIntProperty(properties, PropertyKeyFlushIntervalMs, EnvKeyFlushIntervalMs, config.FlushIntervalMs);
            config.MaxRetries = GetNonNegativeIntProperty(properties, PropertyKeyMaxRetries, null, config.MaxRetries);
            config.RetryDelayMs = GetNonNegativeIntProperty(properties, PropertyKeyRetryDelayMs, null, config.RetryDelayMs);
            config.CircuitBreakerEnabled = GetBooleanProperty(properties, PropertyKeyCircuitBreakerEnabled, null, config.CircuitBreakerEnabled);
            config.CircuitBreakerThreshold = GetPositiveIntProperty(properties, PropertyKeyCircuitBreakerThreshold, null, config.CircuitBreakerThreshold);

            // Parse circuit breaker timeout
            if (properties.TryGetValue(PropertyKeyCircuitBreakerTimeoutMs, out string? timeoutValue))
            {
                if (int.TryParse(timeoutValue, out int timeoutMs) && timeoutMs > 0)
                {
                    config.CircuitBreakerTimeout = TimeSpan.FromMilliseconds(timeoutMs);
                }
                // Invalid values use default (don't throw exception for graceful degradation)
            }

            return config;
        }

        /// <summary>
        /// Applies environment variable overrides to the configuration.
        /// </summary>
        private void ApplyEnvironmentVariables()
        {
            Enabled = GetBooleanFromEnvironment(EnvKeyEnabled, Enabled);
            BatchSize = GetPositiveIntFromEnvironment(EnvKeyBatchSize, BatchSize);
            FlushIntervalMs = GetPositiveIntFromEnvironment(EnvKeyFlushIntervalMs, FlushIntervalMs);
        }

        /// <summary>
        /// Gets a boolean property from connection properties or environment variable.
        /// Priority: Connection Property > Environment Variable > Default.
        /// Invalid values use the default value (graceful degradation).
        /// </summary>
        private static bool GetBooleanProperty(
            IReadOnlyDictionary<string, string> properties,
            string propertyKey,
            string? envKey,
            bool defaultValue)
        {
            // Try connection property first
            if (properties.TryGetValue(propertyKey, out string? value))
            {
                if (bool.TryParse(value, out bool result))
                {
                    return result;
                }
                // Invalid value - use default (graceful degradation)
            }

            // Try environment variable
            if (!string.IsNullOrEmpty(envKey))
            {
                string? envValue = Environment.GetEnvironmentVariable(envKey);
                if (!string.IsNullOrEmpty(envValue) && bool.TryParse(envValue, out bool result))
                {
                    return result;
                }
            }

            return defaultValue;
        }

        /// <summary>
        /// Gets a positive integer property from connection properties or environment variable.
        /// Priority: Connection Property > Environment Variable > Default.
        /// Invalid values use the default value (graceful degradation).
        /// </summary>
        private static int GetPositiveIntProperty(
            IReadOnlyDictionary<string, string> properties,
            string propertyKey,
            string? envKey,
            int defaultValue)
        {
            // Try connection property first
            if (properties.TryGetValue(propertyKey, out string? value))
            {
                if (int.TryParse(value, out int result) && result > 0)
                {
                    return result;
                }
                // Invalid value - use default (graceful degradation)
            }

            // Try environment variable
            if (!string.IsNullOrEmpty(envKey))
            {
                string? envValue = Environment.GetEnvironmentVariable(envKey);
                if (!string.IsNullOrEmpty(envValue) && int.TryParse(envValue, out int result) && result > 0)
                {
                    return result;
                }
            }

            return defaultValue;
        }

        /// <summary>
        /// Gets a non-negative integer property from connection properties.
        /// Invalid values use the default value (graceful degradation).
        /// </summary>
        private static int GetNonNegativeIntProperty(
            IReadOnlyDictionary<string, string> properties,
            string propertyKey,
            string? envKey,
            int defaultValue)
        {
            // Try connection property first
            if (properties.TryGetValue(propertyKey, out string? value))
            {
                if (int.TryParse(value, out int result) && result >= 0)
                {
                    return result;
                }
                // Invalid value - use default (graceful degradation)
            }

            // Try environment variable
            if (!string.IsNullOrEmpty(envKey))
            {
                string? envValue = Environment.GetEnvironmentVariable(envKey);
                if (!string.IsNullOrEmpty(envValue) && int.TryParse(envValue, out int result) && result >= 0)
                {
                    return result;
                }
            }

            return defaultValue;
        }

        /// <summary>
        /// Gets a boolean value from environment variable.
        /// </summary>
        private static bool GetBooleanFromEnvironment(string envKey, bool defaultValue)
        {
            string? value = Environment.GetEnvironmentVariable(envKey);
            if (!string.IsNullOrEmpty(value) && bool.TryParse(value, out bool result))
            {
                return result;
            }
            return defaultValue;
        }

        /// <summary>
        /// Gets a positive integer value from environment variable.
        /// </summary>
        private static int GetPositiveIntFromEnvironment(string envKey, int defaultValue)
        {
            string? value = Environment.GetEnvironmentVariable(envKey);
            if (!string.IsNullOrEmpty(value) && int.TryParse(value, out int result) && result > 0)
            {
                return result;
            }
            return defaultValue;
        }
    }
}
