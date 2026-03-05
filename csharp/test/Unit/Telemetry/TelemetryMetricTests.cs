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
using System.Text.Json;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for TelemetryMetric data model serialization and structure.
    /// </summary>
    public class TelemetryMetricTests
    {
        private static readonly JsonSerializerOptions s_jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
        };

        [Fact]
        public void TelemetryMetric_Serialization_ProducesValidJson()
        {
            // Arrange - create a fully populated metric matching Databricks schema
            var metric = new TelemetryMetric
            {
                MetricType = MetricType.Statement,
                Timestamp = new DateTimeOffset(2024, 3, 5, 10, 30, 45, TimeSpan.Zero),
                WorkspaceId = 12345678901234L,
                SessionId = "session-uuid-123",
                StatementId = "statement-uuid-456",
                ExecutionLatencyMs = 1250L,
                ResultFormat = "ARROW_CLOUDFETCH",
                ChunkCount = 5,
                TotalBytesDownloaded = 1024000L,
                PollCount = 3,
                PollLatencyMs = 450L,
                DriverConfiguration = new DriverConfiguration
                {
                    DriverVersion = "1.0.0",
                    DriverName = "Apache Arrow ADBC Databricks Driver",
                    RuntimeName = ".NET 8.0",
                    RuntimeVersion = "8.0.0",
                    OsName = "Linux",
                    OsVersion = "5.15.0",
                    OsArchitecture = "x86_64",
                    HttpPath = "/sql/1.0/warehouses/abc123",
                    HostUrl = "https://dbc-12345678-abcd.cloud.databricks.com",
                    CloudFetchEnabled = true,
                    DirectResultsEnabled = true,
                    AdditionalProperties = new Dictionary<string, string>
                    {
                        ["enable_pk_fk"] = "true",
                        ["max_bytes_per_fetch"] = "400MB"
                    }
                }
            };

            // Act
            var json = JsonSerializer.Serialize(metric, s_jsonOptions);
            var parsed = JsonDocument.Parse(json);
            var root = parsed.RootElement;

            // Assert - verify all required fields are present with correct snake_case names
            Assert.True(root.TryGetProperty("metric_type", out var metricType));
            Assert.Equal("Statement", metricType.GetString());

            Assert.True(root.TryGetProperty("timestamp", out var timestamp));
            Assert.NotNull(timestamp.GetString());

            Assert.True(root.TryGetProperty("workspace_id", out var workspaceId));
            Assert.Equal(12345678901234L, workspaceId.GetInt64());

            Assert.True(root.TryGetProperty("session_id", out var sessionId));
            Assert.Equal("session-uuid-123", sessionId.GetString());

            Assert.True(root.TryGetProperty("statement_id", out var statementId));
            Assert.Equal("statement-uuid-456", statementId.GetString());

            Assert.True(root.TryGetProperty("execution_latency_ms", out var executionLatencyMs));
            Assert.Equal(1250L, executionLatencyMs.GetInt64());

            Assert.True(root.TryGetProperty("result_format", out var resultFormat));
            Assert.Equal("ARROW_CLOUDFETCH", resultFormat.GetString());

            Assert.True(root.TryGetProperty("chunk_count", out var chunkCount));
            Assert.Equal(5, chunkCount.GetInt32());

            Assert.True(root.TryGetProperty("total_bytes_downloaded", out var totalBytesDownloaded));
            Assert.Equal(1024000L, totalBytesDownloaded.GetInt64());

            Assert.True(root.TryGetProperty("poll_count", out var pollCount));
            Assert.Equal(3, pollCount.GetInt32());

            Assert.True(root.TryGetProperty("poll_latency_ms", out var pollLatencyMs));
            Assert.Equal(450L, pollLatencyMs.GetInt64());

            Assert.True(root.TryGetProperty("driver_configuration", out var driverConfig));
            Assert.True(driverConfig.TryGetProperty("driver_version", out var driverVersion));
            Assert.Equal("1.0.0", driverVersion.GetString());

            Assert.True(driverConfig.TryGetProperty("runtime_name", out var runtimeName));
            Assert.Equal(".NET 8.0", runtimeName.GetString());

            Assert.True(driverConfig.TryGetProperty("cloud_fetch_enabled", out var cloudFetchEnabled));
            Assert.True(cloudFetchEnabled.GetBoolean());
        }

        [Fact]
        public void TelemetryMetric_Serialization_OmitsNullFields()
        {
            // Arrange - create a metric with only required fields
            var metric = new TelemetryMetric
            {
                MetricType = MetricType.Connection,
                Timestamp = DateTimeOffset.UtcNow,
                WorkspaceId = 999L,
                // All optional fields left as null
                SessionId = null,
                StatementId = null,
                ExecutionLatencyMs = null,
                ResultFormat = null,
                ChunkCount = null,
                TotalBytesDownloaded = null,
                PollCount = null,
                PollLatencyMs = null,
                DriverConfiguration = null
            };

            // Act
            var json = JsonSerializer.Serialize(metric, s_jsonOptions);

            // Assert - null fields should be omitted from JSON
            Assert.DoesNotContain("session_id", json);
            Assert.DoesNotContain("statement_id", json);
            Assert.DoesNotContain("execution_latency_ms", json);
            Assert.DoesNotContain("result_format", json);
            Assert.DoesNotContain("chunk_count", json);
            Assert.DoesNotContain("total_bytes_downloaded", json);
            Assert.DoesNotContain("poll_count", json);
            Assert.DoesNotContain("poll_latency_ms", json);
            Assert.DoesNotContain("driver_configuration", json);

            // Required fields should be present
            Assert.Contains("\"metric_type\":", json);
            Assert.Contains("\"timestamp\":", json);
            Assert.Contains("\"workspace_id\":", json);
        }

        [Fact]
        public void TelemetryMetric_PropertyNames_AreSnakeCase()
        {
            // Arrange
            var metric = new TelemetryMetric
            {
                MetricType = MetricType.Statement,
                Timestamp = DateTimeOffset.UtcNow,
                WorkspaceId = 123L,
                SessionId = "session-123",
                StatementId = "statement-456",
                ExecutionLatencyMs = 100L,
                ResultFormat = "ARROW",
                ChunkCount = 1,
                TotalBytesDownloaded = 1000L,
                PollCount = 1,
                PollLatencyMs = 50L
            };

            // Act
            var json = JsonSerializer.Serialize(metric, s_jsonOptions);

            // Assert - all property names should be snake_case
            Assert.Contains("\"metric_type\":", json);
            Assert.Contains("\"timestamp\":", json);
            Assert.Contains("\"workspace_id\":", json);
            Assert.Contains("\"session_id\":", json);
            Assert.Contains("\"statement_id\":", json);
            Assert.Contains("\"execution_latency_ms\":", json);
            Assert.Contains("\"result_format\":", json);
            Assert.Contains("\"chunk_count\":", json);
            Assert.Contains("\"total_bytes_downloaded\":", json);
            Assert.Contains("\"poll_count\":", json);
            Assert.Contains("\"poll_latency_ms\":", json);

            // Should NOT contain PascalCase property names
            Assert.DoesNotContain("\"MetricType\":", json);
            Assert.DoesNotContain("\"WorkspaceId\":", json);
            Assert.DoesNotContain("\"SessionId\":", json);
            Assert.DoesNotContain("\"ExecutionLatencyMs\":", json);
            Assert.DoesNotContain("\"ResultFormat\":", json);
        }

        [Fact]
        public void TelemetryMetric_Deserialization_WorksCorrectly()
        {
            // Arrange - JSON with snake_case property names
            var json = @"{
                ""metric_type"": ""Statement"",
                ""timestamp"": ""2024-03-05T10:30:45Z"",
                ""workspace_id"": 999,
                ""session_id"": ""session-abc"",
                ""statement_id"": ""statement-xyz"",
                ""execution_latency_ms"": 500,
                ""result_format"": ""CLOUDFETCH"",
                ""chunk_count"": 3,
                ""total_bytes_downloaded"": 2048,
                ""poll_count"": 2,
                ""poll_latency_ms"": 100
            }";

            // Act
            var metric = JsonSerializer.Deserialize<TelemetryMetric>(json, s_jsonOptions);

            // Assert
            Assert.NotNull(metric);
            Assert.Equal(MetricType.Statement, metric.MetricType);
            Assert.Equal(999L, metric.WorkspaceId);
            Assert.Equal("session-abc", metric.SessionId);
            Assert.Equal("statement-xyz", metric.StatementId);
            Assert.Equal(500L, metric.ExecutionLatencyMs);
            Assert.Equal("CLOUDFETCH", metric.ResultFormat);
            Assert.Equal(3, metric.ChunkCount);
            Assert.Equal(2048L, metric.TotalBytesDownloaded);
            Assert.Equal(2, metric.PollCount);
            Assert.Equal(100L, metric.PollLatencyMs);
        }

        [Fact]
        public void TelemetryMetric_MetricType_Connection_SerializesCorrectly()
        {
            // Arrange
            var metric = new TelemetryMetric
            {
                MetricType = MetricType.Connection,
                Timestamp = DateTimeOffset.UtcNow,
                WorkspaceId = 123L
            };

            // Act
            var json = JsonSerializer.Serialize(metric, s_jsonOptions);

            // Assert
            Assert.Contains("\"metric_type\":\"Connection\"", json);
        }

        [Fact]
        public void TelemetryMetric_MetricType_Error_SerializesCorrectly()
        {
            // Arrange
            var metric = new TelemetryMetric
            {
                MetricType = MetricType.Error,
                Timestamp = DateTimeOffset.UtcNow,
                WorkspaceId = 123L
            };

            // Act
            var json = JsonSerializer.Serialize(metric, s_jsonOptions);

            // Assert
            Assert.Contains("\"metric_type\":\"Error\"", json);
        }

        [Fact]
        public void DriverConfiguration_Serialization_ProducesValidJson()
        {
            // Arrange
            var config = new DriverConfiguration
            {
                DriverVersion = "1.0.0",
                DriverName = "ADBC Databricks Driver",
                RuntimeName = ".NET 8.0",
                RuntimeVersion = "8.0.0",
                OsName = "Windows",
                OsVersion = "10.0.19045",
                OsArchitecture = "x64",
                HttpPath = "/sql/1.0/warehouses/test",
                HostUrl = "https://example.databricks.com",
                CloudFetchEnabled = true,
                DirectResultsEnabled = false,
                AdditionalProperties = new Dictionary<string, string>
                {
                    ["prop1"] = "value1",
                    ["prop2"] = "value2"
                }
            };

            // Act
            var json = JsonSerializer.Serialize(config, s_jsonOptions);
            var parsed = JsonDocument.Parse(json);
            var root = parsed.RootElement;

            // Assert - verify all fields are present with snake_case names
            Assert.True(root.TryGetProperty("driver_version", out var driverVersion));
            Assert.Equal("1.0.0", driverVersion.GetString());

            Assert.True(root.TryGetProperty("driver_name", out var driverName));
            Assert.Equal("ADBC Databricks Driver", driverName.GetString());

            Assert.True(root.TryGetProperty("runtime_name", out var runtimeName));
            Assert.Equal(".NET 8.0", runtimeName.GetString());

            Assert.True(root.TryGetProperty("os_name", out var osName));
            Assert.Equal("Windows", osName.GetString());

            Assert.True(root.TryGetProperty("os_architecture", out var osArch));
            Assert.Equal("x64", osArch.GetString());

            Assert.True(root.TryGetProperty("cloud_fetch_enabled", out var cloudFetch));
            Assert.True(cloudFetch.GetBoolean());

            Assert.True(root.TryGetProperty("direct_results_enabled", out var directResults));
            Assert.False(directResults.GetBoolean());

            Assert.True(root.TryGetProperty("additional_properties", out var additionalProps));
            Assert.True(additionalProps.TryGetProperty("prop1", out var prop1));
            Assert.Equal("value1", prop1.GetString());
        }

        [Fact]
        public void DriverConfiguration_NullFields_OmittedInSerialization()
        {
            // Arrange - create config with only some fields populated
            var config = new DriverConfiguration
            {
                DriverVersion = "1.0.0",
                RuntimeName = ".NET 8.0",
                // Other fields left as null
                DriverName = null,
                RuntimeVersion = null,
                OsName = null,
                OsVersion = null,
                OsArchitecture = null,
                HttpPath = null,
                HostUrl = null,
                CloudFetchEnabled = null,
                DirectResultsEnabled = null,
                AdditionalProperties = null
            };

            // Act
            var json = JsonSerializer.Serialize(config, s_jsonOptions);

            // Assert - null fields should be omitted
            Assert.Contains("\"driver_version\":", json);
            Assert.Contains("\"runtime_name\":", json);

            Assert.DoesNotContain("driver_name", json);
            Assert.DoesNotContain("runtime_version", json);
            Assert.DoesNotContain("os_name", json);
            Assert.DoesNotContain("os_version", json);
            Assert.DoesNotContain("os_architecture", json);
            Assert.DoesNotContain("http_path", json);
            Assert.DoesNotContain("host_url", json);
            Assert.DoesNotContain("cloud_fetch_enabled", json);
            Assert.DoesNotContain("direct_results_enabled", json);
            Assert.DoesNotContain("additional_properties", json);
        }

        [Fact]
        public void TelemetryMetric_DefaultValues_AreCorrect()
        {
            // Act
            var metric = new TelemetryMetric();

            // Assert
            Assert.Equal(MetricType.Connection, metric.MetricType); // Default enum value
            Assert.Equal(default(DateTimeOffset), metric.Timestamp);
            Assert.Equal(0L, metric.WorkspaceId);
            Assert.Null(metric.SessionId);
            Assert.Null(metric.StatementId);
            Assert.Null(metric.ExecutionLatencyMs);
            Assert.Null(metric.ResultFormat);
            Assert.Null(metric.ChunkCount);
            Assert.Null(metric.TotalBytesDownloaded);
            Assert.Null(metric.PollCount);
            Assert.Null(metric.PollLatencyMs);
            Assert.Null(metric.DriverConfiguration);
        }

        [Fact]
        public void TelemetryMetric_WithPartialData_SerializesCorrectly()
        {
            // Arrange - metric with some fields populated
            var metric = new TelemetryMetric
            {
                MetricType = MetricType.Statement,
                Timestamp = new DateTimeOffset(2024, 3, 5, 0, 0, 0, TimeSpan.Zero),
                WorkspaceId = 12345L,
                SessionId = "session-123",
                StatementId = "statement-456",
                ExecutionLatencyMs = 250L,
                // ResultFormat, ChunkCount, etc. left as null
            };

            // Act
            var json = JsonSerializer.Serialize(metric, s_jsonOptions);

            // Assert - only populated fields should be present
            Assert.Contains("\"metric_type\":", json);
            Assert.Contains("\"workspace_id\":", json);
            Assert.Contains("\"session_id\":", json);
            Assert.Contains("\"statement_id\":", json);
            Assert.Contains("\"execution_latency_ms\":", json);

            Assert.DoesNotContain("result_format", json);
            Assert.DoesNotContain("chunk_count", json);
            Assert.DoesNotContain("total_bytes_downloaded", json);
            Assert.DoesNotContain("poll_count", json);
            Assert.DoesNotContain("poll_latency_ms", json);
            Assert.DoesNotContain("driver_configuration", json);
        }

        [Fact]
        public void TelemetryMetric_WithZeroValues_IncludesThemInJson()
        {
            // Arrange - metric with explicit zero values (not null)
            var metric = new TelemetryMetric
            {
                MetricType = MetricType.Statement,
                Timestamp = DateTimeOffset.UtcNow,
                WorkspaceId = 0L, // Explicit zero
                ExecutionLatencyMs = 0L, // Explicit zero
                ChunkCount = 0, // Explicit zero
                PollCount = 0 // Explicit zero
            };

            // Act
            var json = JsonSerializer.Serialize(metric, s_jsonOptions);

            // Assert - zero values should be included (they're not null)
            Assert.Contains("\"workspace_id\":0", json);
            Assert.Contains("\"execution_latency_ms\":0", json);
            Assert.Contains("\"chunk_count\":0", json);
            Assert.Contains("\"poll_count\":0", json);
        }
    }
}
