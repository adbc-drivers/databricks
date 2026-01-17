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
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for TelemetryConfiguration class.
    /// </summary>
    public class TelemetryConfigurationTests : IDisposable
    {
        private readonly List<string> _envVarsToCleanup = new List<string>();

        public void Dispose()
        {
            // Clean up environment variables after each test
            foreach (var envVar in _envVarsToCleanup)
            {
                Environment.SetEnvironmentVariable(envVar, null);
            }
        }

        private void SetEnvironmentVariable(string key, string? value)
        {
            Environment.SetEnvironmentVariable(key, value);
            _envVarsToCleanup.Add(key);
        }

        [Fact]
        public void TelemetryConfiguration_DefaultValues_AreCorrect()
        {
            // Arrange & Act
            var config = new TelemetryConfiguration();

            // Assert
            Assert.True(config.Enabled);
            Assert.Equal(100, config.BatchSize);
            Assert.Equal(5000, config.FlushIntervalMs);
            Assert.Equal(3, config.MaxRetries);
            Assert.Equal(100, config.RetryDelayMs);
            Assert.True(config.CircuitBreakerEnabled);
            Assert.Equal(5, config.CircuitBreakerThreshold);
            Assert.Equal(TimeSpan.FromMinutes(1), config.CircuitBreakerTimeout);
        }

        [Fact]
        public void TelemetryConfiguration_FromProperties_ParsesCorrectly()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyEnabled, "false" },
                { TelemetryConfiguration.PropertyKeyBatchSize, "50" },
                { TelemetryConfiguration.PropertyKeyFlushIntervalMs, "3000" },
                { TelemetryConfiguration.PropertyKeyMaxRetries, "5" },
                { TelemetryConfiguration.PropertyKeyRetryDelayMs, "200" },
                { TelemetryConfiguration.PropertyKeyCircuitBreakerEnabled, "false" },
                { TelemetryConfiguration.PropertyKeyCircuitBreakerThreshold, "10" },
                { TelemetryConfiguration.PropertyKeyCircuitBreakerTimeoutMs, "120000" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.False(config.Enabled);
            Assert.Equal(50, config.BatchSize);
            Assert.Equal(3000, config.FlushIntervalMs);
            Assert.Equal(5, config.MaxRetries);
            Assert.Equal(200, config.RetryDelayMs);
            Assert.False(config.CircuitBreakerEnabled);
            Assert.Equal(10, config.CircuitBreakerThreshold);
            Assert.Equal(TimeSpan.FromMinutes(2), config.CircuitBreakerTimeout);
        }

        [Fact]
        public void TelemetryConfiguration_InvalidProperty_UsesDefault()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyBatchSize, "invalid" },
                { TelemetryConfiguration.PropertyKeyEnabled, "not_a_boolean" },
                { TelemetryConfiguration.PropertyKeyFlushIntervalMs, "-100" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert - should use defaults for invalid values
            Assert.Equal(100, config.BatchSize); // default
            Assert.True(config.Enabled); // default
            Assert.Equal(5000, config.FlushIntervalMs); // default
        }

        [Fact]
        public void TelemetryConfiguration_NullProperties_UsesDefaults()
        {
            // Act
            var config = TelemetryConfiguration.FromProperties(null);

            // Assert
            Assert.True(config.Enabled);
            Assert.Equal(100, config.BatchSize);
            Assert.Equal(5000, config.FlushIntervalMs);
        }

        [Fact]
        public void TelemetryConfiguration_EmptyProperties_UsesDefaults()
        {
            // Arrange
            var properties = new Dictionary<string, string>();

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.True(config.Enabled);
            Assert.Equal(100, config.BatchSize);
            Assert.Equal(5000, config.FlushIntervalMs);
        }

        [Fact]
        public void TelemetryConfiguration_FromEnvironmentVariables_ParsesCorrectly()
        {
            // Arrange
            SetEnvironmentVariable(TelemetryConfiguration.EnvKeyEnabled, "false");
            SetEnvironmentVariable(TelemetryConfiguration.EnvKeyBatchSize, "75");
            SetEnvironmentVariable(TelemetryConfiguration.EnvKeyFlushIntervalMs, "7000");

            // Act - null properties to use environment variables
            var config = TelemetryConfiguration.FromProperties(null);

            // Assert
            Assert.False(config.Enabled);
            Assert.Equal(75, config.BatchSize);
            Assert.Equal(7000, config.FlushIntervalMs);
        }

        [Fact]
        public void TelemetryConfiguration_PropertiesOverrideEnvironmentVariables()
        {
            // Arrange
            SetEnvironmentVariable(TelemetryConfiguration.EnvKeyEnabled, "false");
            SetEnvironmentVariable(TelemetryConfiguration.EnvKeyBatchSize, "75");

            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyEnabled, "true" },
                { TelemetryConfiguration.PropertyKeyBatchSize, "150" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert - properties should override environment variables
            Assert.True(config.Enabled);
            Assert.Equal(150, config.BatchSize);
        }

        [Fact]
        public void TelemetryConfiguration_InvalidEnvironmentVariable_UsesDefault()
        {
            // Arrange
            SetEnvironmentVariable(TelemetryConfiguration.EnvKeyBatchSize, "not_a_number");
            SetEnvironmentVariable(TelemetryConfiguration.EnvKeyEnabled, "invalid");

            // Act
            var config = TelemetryConfiguration.FromProperties(null);

            // Assert - should use defaults for invalid environment variables
            Assert.Equal(100, config.BatchSize);
            Assert.True(config.Enabled);
        }

        [Fact]
        public void TelemetryConfiguration_NegativeBatchSize_UsesDefault()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyBatchSize, "-50" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.Equal(100, config.BatchSize); // default
        }

        [Fact]
        public void TelemetryConfiguration_ZeroBatchSize_UsesDefault()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyBatchSize, "0" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.Equal(100, config.BatchSize); // default
        }

        [Fact]
        public void TelemetryConfiguration_NegativeFlushInterval_UsesDefault()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyFlushIntervalMs, "-1000" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.Equal(5000, config.FlushIntervalMs); // default
        }

        [Fact]
        public void TelemetryConfiguration_MaxRetriesCanBeZero()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyMaxRetries, "0" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.Equal(0, config.MaxRetries);
        }

        [Fact]
        public void TelemetryConfiguration_NegativeMaxRetries_UsesDefault()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyMaxRetries, "-1" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.Equal(3, config.MaxRetries); // default
        }

        [Fact]
        public void TelemetryConfiguration_RetryDelayCanBeZero()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyRetryDelayMs, "0" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.Equal(0, config.RetryDelayMs);
        }

        [Fact]
        public void TelemetryConfiguration_InvalidCircuitBreakerTimeout_UsesDefault()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyCircuitBreakerTimeoutMs, "invalid" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.Equal(TimeSpan.FromMinutes(1), config.CircuitBreakerTimeout); // default
        }

        [Fact]
        public void TelemetryConfiguration_NegativeCircuitBreakerTimeout_UsesDefault()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyCircuitBreakerTimeoutMs, "-5000" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.Equal(TimeSpan.FromMinutes(1), config.CircuitBreakerTimeout); // default
        }

        [Fact]
        public void TelemetryConfiguration_FeatureFlagName_IsCorrect()
        {
            // Assert
            Assert.Equal(
                "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForAdbc",
                TelemetryConfiguration.FeatureFlagName);
        }

        [Fact]
        public void TelemetryConfiguration_PropertyKeys_AreCorrect()
        {
            // Assert
            Assert.Equal("telemetry.enabled", TelemetryConfiguration.PropertyKeyEnabled);
            Assert.Equal("telemetry.batch_size", TelemetryConfiguration.PropertyKeyBatchSize);
            Assert.Equal("telemetry.flush_interval_ms", TelemetryConfiguration.PropertyKeyFlushIntervalMs);
            Assert.Equal("telemetry.max_retries", TelemetryConfiguration.PropertyKeyMaxRetries);
            Assert.Equal("telemetry.retry_delay_ms", TelemetryConfiguration.PropertyKeyRetryDelayMs);
            Assert.Equal("telemetry.circuit_breaker_enabled", TelemetryConfiguration.PropertyKeyCircuitBreakerEnabled);
            Assert.Equal("telemetry.circuit_breaker_threshold", TelemetryConfiguration.PropertyKeyCircuitBreakerThreshold);
            Assert.Equal("telemetry.circuit_breaker_timeout_ms", TelemetryConfiguration.PropertyKeyCircuitBreakerTimeoutMs);
        }

        [Fact]
        public void TelemetryConfiguration_EnvironmentKeys_AreCorrect()
        {
            // Assert
            Assert.Equal("DATABRICKS_TELEMETRY_ENABLED", TelemetryConfiguration.EnvKeyEnabled);
            Assert.Equal("DATABRICKS_TELEMETRY_BATCH_SIZE", TelemetryConfiguration.EnvKeyBatchSize);
            Assert.Equal("DATABRICKS_TELEMETRY_FLUSH_INTERVAL_MS", TelemetryConfiguration.EnvKeyFlushIntervalMs);
        }

        [Fact]
        public void TelemetryConfiguration_BooleanCaseInsensitive_ParsesCorrectly()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyEnabled, "True" },
                { TelemetryConfiguration.PropertyKeyCircuitBreakerEnabled, "FALSE" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.True(config.Enabled);
            Assert.False(config.CircuitBreakerEnabled);
        }

        [Fact]
        public void TelemetryConfiguration_LargeValidValues_ParsesCorrectly()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyBatchSize, "10000" },
                { TelemetryConfiguration.PropertyKeyFlushIntervalMs, "300000" },
                { TelemetryConfiguration.PropertyKeyCircuitBreakerThreshold, "100" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.Equal(10000, config.BatchSize);
            Assert.Equal(300000, config.FlushIntervalMs);
            Assert.Equal(100, config.CircuitBreakerThreshold);
        }

        [Fact]
        public void TelemetryConfiguration_PartialProperties_UsesDefaultsForMissing()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyEnabled, "false" },
                { TelemetryConfiguration.PropertyKeyBatchSize, "200" }
                // FlushIntervalMs and other properties not specified
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.False(config.Enabled);
            Assert.Equal(200, config.BatchSize);
            Assert.Equal(5000, config.FlushIntervalMs); // default
            Assert.Equal(3, config.MaxRetries); // default
            Assert.Equal(100, config.RetryDelayMs); // default
        }
    }
}
