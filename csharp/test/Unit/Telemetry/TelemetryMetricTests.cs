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
using System.Text.Json;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for TelemetryMetric class focusing on JSON serialization and schema compliance.
    /// </summary>
    public class TelemetryMetricTests
    {
        [Fact]
        public void TelemetryMetric_Serialization_ProducesValidJson()
        {
            // Arrange
            var metric = new TelemetryMetric
            {
                MetricType = "statement",
                Timestamp = new DateTimeOffset(2025, 1, 16, 10, 30, 0, TimeSpan.Zero),
                WorkspaceId = 123456789,
                SessionId = "session-abc-123",
                StatementId = "stmt-xyz-456",
                ExecutionLatencyMs = 1500,
                ResultFormat = "cloudfetch",
                ChunkCount = 5,
                TotalBytesDownloaded = 1048576,
                PollCount = 3,
                DriverConfiguration = new DriverConfiguration
                {
                    DriverVersion = "1.0.0",
                    DriverOS = "Linux",
                    DriverRuntime = ".NET 8.0",
                    FeatureCloudFetch = true,
                    FeatureLz4 = true
                }
            };

            // Act
            string json = metric.ToJson();

            // Assert - Verify JSON is valid
            Assert.NotNull(json);
            Assert.NotEmpty(json);

            // Parse JSON to verify structure
            using var jsonDoc = JsonDocument.Parse(json);
            var root = jsonDoc.RootElement;

            // Verify all required fields are present
            Assert.True(root.TryGetProperty("metricType", out var metricType));
            Assert.Equal("statement", metricType.GetString());

            Assert.True(root.TryGetProperty("timestamp", out var timestamp));
            Assert.Equal(new DateTimeOffset(2025, 1, 16, 10, 30, 0, TimeSpan.Zero), timestamp.GetDateTimeOffset());

            Assert.True(root.TryGetProperty("workspaceId", out var workspaceId));
            Assert.Equal(123456789, workspaceId.GetInt64());

            Assert.True(root.TryGetProperty("sessionId", out var sessionId));
            Assert.Equal("session-abc-123", sessionId.GetString());

            Assert.True(root.TryGetProperty("statementId", out var statementId));
            Assert.Equal("stmt-xyz-456", statementId.GetString());

            Assert.True(root.TryGetProperty("executionLatencyMs", out var executionLatency));
            Assert.Equal(1500, executionLatency.GetInt64());

            Assert.True(root.TryGetProperty("resultFormat", out var resultFormat));
            Assert.Equal("cloudfetch", resultFormat.GetString());

            Assert.True(root.TryGetProperty("chunkCount", out var chunkCount));
            Assert.Equal(5, chunkCount.GetInt32());

            Assert.True(root.TryGetProperty("totalBytesDownloaded", out var totalBytes));
            Assert.Equal(1048576, totalBytes.GetInt64());

            Assert.True(root.TryGetProperty("pollCount", out var pollCount));
            Assert.Equal(3, pollCount.GetInt32());

            // Verify driver configuration
            Assert.True(root.TryGetProperty("driverConfiguration", out var driverConfig));
            Assert.True(driverConfig.TryGetProperty("driverVersion", out var driverVersion));
            Assert.Equal("1.0.0", driverVersion.GetString());

            Assert.True(driverConfig.TryGetProperty("driverOs", out var driverOs));
            Assert.Equal("Linux", driverOs.GetString());

            Assert.True(driverConfig.TryGetProperty("driverRuntime", out var driverRuntime));
            Assert.Equal(".NET 8.0", driverRuntime.GetString());

            Assert.True(driverConfig.TryGetProperty("featureCloudfetch", out var featureCloudfetch));
            Assert.True(featureCloudfetch.GetBoolean());

            Assert.True(driverConfig.TryGetProperty("featureLz4", out var featureLz4));
            Assert.True(featureLz4.GetBoolean());
        }

        [Fact]
        public void TelemetryMetric_Serialization_OmitsNullFields()
        {
            // Arrange - Create metric with only required fields
            var metric = new TelemetryMetric
            {
                MetricType = "connection",
                Timestamp = new DateTimeOffset(2025, 1, 16, 10, 30, 0, TimeSpan.Zero),
                SessionId = "session-abc-123"
                // All other fields are null
            };

            // Act
            string json = metric.ToJson();

            // Assert - Verify JSON does not contain null fields
            Assert.NotNull(json);
            Assert.NotEmpty(json);

            // Parse JSON to verify null fields are omitted
            using var jsonDoc = JsonDocument.Parse(json);
            var root = jsonDoc.RootElement;

            // Verify present fields
            Assert.True(root.TryGetProperty("metricType", out _));
            Assert.True(root.TryGetProperty("timestamp", out _));
            Assert.True(root.TryGetProperty("sessionId", out _));

            // Verify null fields are NOT present
            Assert.False(root.TryGetProperty("workspaceId", out _));
            Assert.False(root.TryGetProperty("statementId", out _));
            Assert.False(root.TryGetProperty("executionLatencyMs", out _));
            Assert.False(root.TryGetProperty("resultFormat", out _));
            Assert.False(root.TryGetProperty("chunkCount", out _));
            Assert.False(root.TryGetProperty("totalBytesDownloaded", out _));
            Assert.False(root.TryGetProperty("pollCount", out _));
            Assert.False(root.TryGetProperty("driverConfiguration", out _));
        }

        [Fact]
        public void TelemetryMetric_Serialization_ConnectionEvent_ProducesValidJson()
        {
            // Arrange - Connection event with driver configuration
            var metric = new TelemetryMetric
            {
                MetricType = "connection",
                Timestamp = DateTimeOffset.UtcNow,
                WorkspaceId = 987654321,
                SessionId = "session-connection-001",
                DriverConfiguration = new DriverConfiguration
                {
                    DriverVersion = "1.2.3",
                    DriverOS = "Windows",
                    DriverRuntime = ".NET Framework 4.7.2",
                    FeatureCloudFetch = false,
                    FeatureLz4 = false
                }
            };

            // Act
            string json = metric.ToJson();

            // Assert
            Assert.NotNull(json);
            using var jsonDoc = JsonDocument.Parse(json);
            var root = jsonDoc.RootElement;

            Assert.True(root.TryGetProperty("metricType", out var metricType));
            Assert.Equal("connection", metricType.GetString());

            Assert.True(root.TryGetProperty("driverConfiguration", out var driverConfig));
            Assert.True(driverConfig.TryGetProperty("featureCloudfetch", out var featureCloudfetch));
            Assert.False(featureCloudfetch.GetBoolean());
        }

        [Fact]
        public void TelemetryMetric_Serialization_StatementEvent_WithAllFields_ProducesValidJson()
        {
            // Arrange - Statement event with all fields populated
            var metric = new TelemetryMetric
            {
                MetricType = "statement",
                Timestamp = DateTimeOffset.UtcNow,
                WorkspaceId = 111222333,
                SessionId = "session-stmt-001",
                StatementId = "stmt-001",
                ExecutionLatencyMs = 5000,
                ResultFormat = "inline",
                ChunkCount = 0,
                TotalBytesDownloaded = 0,
                PollCount = 10
            };

            // Act
            string json = metric.ToJson();

            // Assert
            Assert.NotNull(json);
            using var jsonDoc = JsonDocument.Parse(json);
            var root = jsonDoc.RootElement;

            Assert.True(root.TryGetProperty("metricType", out var metricType));
            Assert.Equal("statement", metricType.GetString());

            Assert.True(root.TryGetProperty("resultFormat", out var resultFormat));
            Assert.Equal("inline", resultFormat.GetString());

            // Even zero values should be included
            Assert.True(root.TryGetProperty("chunkCount", out var chunkCount));
            Assert.Equal(0, chunkCount.GetInt32());

            Assert.True(root.TryGetProperty("totalBytesDownloaded", out var totalBytes));
            Assert.Equal(0, totalBytes.GetInt64());
        }

        [Fact]
        public void TelemetryMetric_Serialization_ErrorEvent_ProducesValidJson()
        {
            // Arrange - Error event (minimal fields)
            var metric = new TelemetryMetric
            {
                MetricType = "error",
                Timestamp = DateTimeOffset.UtcNow,
                SessionId = "session-error-001",
                StatementId = "stmt-error-001"
            };

            // Act
            string json = metric.ToJson();

            // Assert
            Assert.NotNull(json);
            using var jsonDoc = JsonDocument.Parse(json);
            var root = jsonDoc.RootElement;

            Assert.True(root.TryGetProperty("metricType", out var metricType));
            Assert.Equal("error", metricType.GetString());

            Assert.True(root.TryGetProperty("sessionId", out _));
            Assert.True(root.TryGetProperty("statementId", out _));
        }

        [Fact]
        public void TelemetryMetric_Deserialization_FromJson_ReturnsValidObject()
        {
            // Arrange
            string json = @"{
                ""metricType"": ""statement"",
                ""timestamp"": ""2025-01-16T10:30:00Z"",
                ""workspaceId"": 123456789,
                ""sessionId"": ""session-abc-123"",
                ""statementId"": ""stmt-xyz-456"",
                ""executionLatencyMs"": 1500,
                ""resultFormat"": ""cloudfetch"",
                ""chunkCount"": 5,
                ""totalBytesDownloaded"": 1048576,
                ""pollCount"": 3
            }";

            // Act
            var metric = TelemetryMetric.FromJson(json);

            // Assert
            Assert.NotNull(metric);
            Assert.Equal("statement", metric.MetricType);
            Assert.Equal(new DateTimeOffset(2025, 1, 16, 10, 30, 0, TimeSpan.Zero), metric.Timestamp);
            Assert.Equal(123456789, metric.WorkspaceId);
            Assert.Equal("session-abc-123", metric.SessionId);
            Assert.Equal("stmt-xyz-456", metric.StatementId);
            Assert.Equal(1500, metric.ExecutionLatencyMs);
            Assert.Equal("cloudfetch", metric.ResultFormat);
            Assert.Equal(5, metric.ChunkCount);
            Assert.Equal(1048576, metric.TotalBytesDownloaded);
            Assert.Equal(3, metric.PollCount);
        }

        [Fact]
        public void TelemetryMetric_Deserialization_WithMissingFields_ReturnsObjectWithNulls()
        {
            // Arrange
            string json = @"{
                ""metricType"": ""connection"",
                ""timestamp"": ""2025-01-16T10:30:00Z"",
                ""sessionId"": ""session-minimal""
            }";

            // Act
            var metric = TelemetryMetric.FromJson(json);

            // Assert
            Assert.NotNull(metric);
            Assert.Equal("connection", metric.MetricType);
            Assert.Equal("session-minimal", metric.SessionId);
            Assert.Null(metric.WorkspaceId);
            Assert.Null(metric.StatementId);
            Assert.Null(metric.ExecutionLatencyMs);
            Assert.Null(metric.ResultFormat);
            Assert.Null(metric.ChunkCount);
            Assert.Null(metric.TotalBytesDownloaded);
            Assert.Null(metric.PollCount);
            Assert.Null(metric.DriverConfiguration);
        }

        [Fact]
        public void TelemetryMetric_RoundTrip_PreservesData()
        {
            // Arrange
            var originalMetric = new TelemetryMetric
            {
                MetricType = "statement",
                Timestamp = new DateTimeOffset(2025, 1, 16, 10, 30, 45, TimeSpan.FromHours(-5)),
                WorkspaceId = 999888777,
                SessionId = "session-roundtrip",
                StatementId = "stmt-roundtrip",
                ExecutionLatencyMs = 2500,
                ResultFormat = "cloudfetch",
                ChunkCount = 10,
                TotalBytesDownloaded = 5242880,
                PollCount = 5,
                DriverConfiguration = new DriverConfiguration
                {
                    DriverVersion = "2.0.0",
                    DriverOS = "macOS",
                    DriverRuntime = ".NET 8.0",
                    FeatureCloudFetch = true,
                    FeatureLz4 = false
                }
            };

            // Act - Serialize and deserialize
            string json = originalMetric.ToJson();
            var deserializedMetric = TelemetryMetric.FromJson(json);

            // Assert - Verify all fields match
            Assert.Equal(originalMetric.MetricType, deserializedMetric.MetricType);
            Assert.Equal(originalMetric.Timestamp, deserializedMetric.Timestamp);
            Assert.Equal(originalMetric.WorkspaceId, deserializedMetric.WorkspaceId);
            Assert.Equal(originalMetric.SessionId, deserializedMetric.SessionId);
            Assert.Equal(originalMetric.StatementId, deserializedMetric.StatementId);
            Assert.Equal(originalMetric.ExecutionLatencyMs, deserializedMetric.ExecutionLatencyMs);
            Assert.Equal(originalMetric.ResultFormat, deserializedMetric.ResultFormat);
            Assert.Equal(originalMetric.ChunkCount, deserializedMetric.ChunkCount);
            Assert.Equal(originalMetric.TotalBytesDownloaded, deserializedMetric.TotalBytesDownloaded);
            Assert.Equal(originalMetric.PollCount, deserializedMetric.PollCount);

            Assert.NotNull(deserializedMetric.DriverConfiguration);
            Assert.Equal(originalMetric.DriverConfiguration.DriverVersion, deserializedMetric.DriverConfiguration.DriverVersion);
            Assert.Equal(originalMetric.DriverConfiguration.DriverOS, deserializedMetric.DriverConfiguration.DriverOS);
            Assert.Equal(originalMetric.DriverConfiguration.DriverRuntime, deserializedMetric.DriverConfiguration.DriverRuntime);
            Assert.Equal(originalMetric.DriverConfiguration.FeatureCloudFetch, deserializedMetric.DriverConfiguration.FeatureCloudFetch);
            Assert.Equal(originalMetric.DriverConfiguration.FeatureLz4, deserializedMetric.DriverConfiguration.FeatureLz4);
        }

        [Fact]
        public void DriverConfiguration_Serialization_OmitsNullFields()
        {
            // Arrange
            var config = new DriverConfiguration
            {
                DriverVersion = "1.0.0",
                DriverOS = "Linux"
                // DriverRuntime, FeatureCloudFetch, FeatureLz4 are null
            };

            var metric = new TelemetryMetric
            {
                MetricType = "connection",
                Timestamp = DateTimeOffset.UtcNow,
                SessionId = "session-test",
                DriverConfiguration = config
            };

            // Act
            string json = metric.ToJson();

            // Assert
            using var jsonDoc = JsonDocument.Parse(json);
            var root = jsonDoc.RootElement;

            Assert.True(root.TryGetProperty("driverConfiguration", out var driverConfig));
            Assert.True(driverConfig.TryGetProperty("driverVersion", out _));
            Assert.True(driverConfig.TryGetProperty("driverOs", out _));

            // Verify null fields are omitted
            Assert.False(driverConfig.TryGetProperty("driverRuntime", out _));
            Assert.False(driverConfig.TryGetProperty("featureCloudfetch", out _));
            Assert.False(driverConfig.TryGetProperty("featureLz4", out _));
        }

        [Fact]
        public void TelemetryMetric_Serialization_UsesSnakeCaseForPropertyNames()
        {
            // Arrange
            var metric = new TelemetryMetric
            {
                MetricType = "statement",
                Timestamp = DateTimeOffset.UtcNow,
                SessionId = "session-test"
            };

            // Act
            string json = metric.ToJson();

            // Assert - Verify camelCase property names (as per System.Text.Json default)
            Assert.Contains("\"metricType\"", json);
            Assert.Contains("\"timestamp\"", json);
            Assert.Contains("\"sessionId\"", json);

            // Should NOT contain PascalCase
            Assert.DoesNotContain("\"MetricType\"", json);
            Assert.DoesNotContain("\"SessionId\"", json);
        }
    }
}
