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

using System.Text.Json;
using AdbcDrivers.Databricks.Telemetry.Models;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry.Models
{
    /// <summary>
    /// Tests for TelemetryEvent and related model serialization.
    /// </summary>
    public class TelemetryEventTests
    {
        [Fact]
        public void TelemetryEvent_Serialization_OmitsNullFields()
        {
            // Arrange
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "session-123",
                SqlStatementId = "statement-456",
                OperationLatencyMs = 100,
                // Leave other fields null
                SystemConfiguration = null,
                SqlExecutionEvent = null,
                ErrorInfo = null,
                ConnectionParameters = null
            };

            // Act
            var json = JsonSerializer.Serialize(telemetryEvent);

            // Assert - null fields should be omitted
            Assert.Contains("\"session_id\":", json);
            Assert.Contains("\"sql_statement_id\":", json);
            Assert.Contains("\"operation_latency_ms\":", json);
            Assert.DoesNotContain("system_configuration", json);
            Assert.DoesNotContain("sql_execution_event", json);
            Assert.DoesNotContain("error_info", json);
            Assert.DoesNotContain("connection_parameters", json);
        }

        [Fact]
        public void TelemetryEvent_Contains_RequiredFields()
        {
            // Arrange
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "session-uuid-123",
                SqlStatementId = "statement-uuid-456",
                OperationLatencyMs = 250,
                SystemConfiguration = new DriverSystemConfiguration
                {
                    DriverName = "Databricks ADBC Driver",
                    DriverVersion = "1.0.0"
                }
            };

            // Act
            var json = JsonSerializer.Serialize(telemetryEvent);
            var parsed = JsonDocument.Parse(json);
            var root = parsed.RootElement;

            // Assert - verify required fields from exit criteria
            Assert.True(root.TryGetProperty("session_id", out var sessionId));
            Assert.Equal("session-uuid-123", sessionId.GetString());

            Assert.True(root.TryGetProperty("sql_statement_id", out var statementId));
            Assert.Equal("statement-uuid-456", statementId.GetString());

            Assert.True(root.TryGetProperty("operation_latency_ms", out var latency));
            Assert.Equal(250, latency.GetInt64());

            Assert.True(root.TryGetProperty("system_configuration", out var sysConfig));
            Assert.True(sysConfig.TryGetProperty("driver_name", out _));
            Assert.True(sysConfig.TryGetProperty("driver_version", out _));
        }

        [Fact]
        public void TelemetryEvent_PropertyNames_AreSnakeCase()
        {
            // Arrange
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "test",
                SqlStatementId = "test",
                OperationLatencyMs = 100
            };

            // Act
            var json = JsonSerializer.Serialize(telemetryEvent);

            // Assert - property names should be snake_case
            Assert.Contains("\"session_id\":", json);
            Assert.Contains("\"sql_statement_id\":", json);
            Assert.Contains("\"operation_latency_ms\":", json);
            Assert.DoesNotContain("\"SessionId\":", json);
            Assert.DoesNotContain("\"SqlStatementId\":", json);
            Assert.DoesNotContain("\"OperationLatencyMs\":", json);
        }

        [Fact]
        public void TelemetryEvent_ZeroLatency_IsOmittedByDefault()
        {
            // Arrange - OperationLatencyMs defaults to 0
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "test"
            };

            // Act
            var json = JsonSerializer.Serialize(telemetryEvent);

            // Assert - default value (0) should be omitted
            Assert.DoesNotContain("operation_latency_ms", json);
        }

        [Fact]
        public void TelemetryEvent_NonZeroLatency_IsIncluded()
        {
            // Arrange
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "test",
                OperationLatencyMs = 1
            };

            // Act
            var json = JsonSerializer.Serialize(telemetryEvent);

            // Assert - non-zero latency should be included
            Assert.Contains("\"operation_latency_ms\":1", json);
        }

        [Fact]
        public void TelemetryEvent_WithAllFields_SerializesCorrectly()
        {
            // Arrange
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "session-123",
                SqlStatementId = "statement-456",
                OperationLatencyMs = 500,
                SystemConfiguration = new DriverSystemConfiguration
                {
                    DriverName = "Databricks ADBC Driver",
                    DriverVersion = "1.0.0",
                    OsName = "Windows",
                    OsVersion = "10.0.19041",
                    OsArch = "x64",
                    RuntimeName = ".NET",
                    RuntimeVersion = "8.0.0",
                    Locale = "en-US",
                    Timezone = "America/Los_Angeles"
                },
                SqlExecutionEvent = new SqlExecutionEvent
                {
                    ResultFormat = "cloudfetch",
                    ChunkCount = 10,
                    BytesDownloaded = 2048000,
                    CompressionEnabled = true,
                    RowCount = 50000,
                    PollCount = 3,
                    PollLatencyMs = 150,
                    ExecutionStatus = "succeeded",
                    StatementType = "query"
                },
                ConnectionParameters = new DriverConnectionParameters
                {
                    CloudFetchEnabled = true,
                    Lz4CompressionEnabled = true,
                    DirectResultsEnabled = false,
                    MaxDownloadThreads = 4,
                    AuthType = "oauth",
                    TransportMode = "https"
                },
                ErrorInfo = null
            };

            // Act
            var json = JsonSerializer.Serialize(telemetryEvent);
            var roundTrip = JsonSerializer.Deserialize<TelemetryEvent>(json);

            // Assert
            Assert.NotNull(roundTrip);
            Assert.Equal("session-123", roundTrip.SessionId);
            Assert.Equal("statement-456", roundTrip.SqlStatementId);
            Assert.Equal(500, roundTrip.OperationLatencyMs);

            Assert.NotNull(roundTrip.SystemConfiguration);
            Assert.Equal("Databricks ADBC Driver", roundTrip.SystemConfiguration.DriverName);
            Assert.Equal("1.0.0", roundTrip.SystemConfiguration.DriverVersion);
            Assert.Equal("Windows", roundTrip.SystemConfiguration.OsName);

            Assert.NotNull(roundTrip.SqlExecutionEvent);
            Assert.Equal("cloudfetch", roundTrip.SqlExecutionEvent.ResultFormat);
            Assert.Equal(10, roundTrip.SqlExecutionEvent.ChunkCount);
            Assert.Equal(2048000, roundTrip.SqlExecutionEvent.BytesDownloaded);

            Assert.NotNull(roundTrip.ConnectionParameters);
            Assert.True(roundTrip.ConnectionParameters.CloudFetchEnabled);
            Assert.Equal("oauth", roundTrip.ConnectionParameters.AuthType);
        }

        [Fact]
        public void TelemetryEvent_WithErrorInfo_SerializesCorrectly()
        {
            // Arrange
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "session-123",
                SqlStatementId = "statement-456",
                OperationLatencyMs = 50,
                ErrorInfo = new DriverErrorInfo
                {
                    ErrorType = "AuthenticationError",
                    ErrorMessage = "Invalid token",
                    ErrorCode = "AUTH_001",
                    HttpStatusCode = 401,
                    IsTerminal = true,
                    RetryAttempted = false
                }
            };

            // Act
            var json = JsonSerializer.Serialize(telemetryEvent);
            var parsed = JsonDocument.Parse(json);
            var root = parsed.RootElement;

            // Assert
            Assert.True(root.TryGetProperty("error_info", out var errorInfo));
            Assert.True(errorInfo.TryGetProperty("error_type", out var errorType));
            Assert.Equal("AuthenticationError", errorType.GetString());
            Assert.True(errorInfo.TryGetProperty("http_status_code", out var httpStatus));
            Assert.Equal(401, httpStatus.GetInt32());
        }

        [Fact]
        public void DriverSystemConfiguration_NullFields_AreOmitted()
        {
            // Arrange
            var sysConfig = new DriverSystemConfiguration
            {
                DriverName = "Test Driver",
                DriverVersion = "1.0.0",
                // Leave other fields null
                OsName = null,
                OsVersion = null,
                OsArch = null,
                RuntimeName = null,
                RuntimeVersion = null,
                Locale = null,
                Timezone = null
            };

            // Act
            var json = JsonSerializer.Serialize(sysConfig);

            // Assert
            Assert.Contains("\"driver_name\":", json);
            Assert.Contains("\"driver_version\":", json);
            Assert.DoesNotContain("os_name", json);
            Assert.DoesNotContain("os_version", json);
            Assert.DoesNotContain("os_arch", json);
            Assert.DoesNotContain("runtime_name", json);
            Assert.DoesNotContain("runtime_version", json);
            Assert.DoesNotContain("locale", json);
            Assert.DoesNotContain("timezone", json);
        }

        [Fact]
        public void SqlExecutionEvent_NullFields_AreOmitted()
        {
            // Arrange
            var execEvent = new SqlExecutionEvent
            {
                ResultFormat = "inline",
                ExecutionStatus = "succeeded"
                // Leave other fields null
            };

            // Act
            var json = JsonSerializer.Serialize(execEvent);

            // Assert
            Assert.Contains("\"result_format\":", json);
            Assert.Contains("\"execution_status\":", json);
            Assert.DoesNotContain("chunk_count", json);
            Assert.DoesNotContain("bytes_downloaded", json);
            Assert.DoesNotContain("compression_enabled", json);
            Assert.DoesNotContain("row_count", json);
            Assert.DoesNotContain("poll_count", json);
            Assert.DoesNotContain("poll_latency_ms", json);
        }

        [Fact]
        public void DriverConnectionParameters_NullFields_AreOmitted()
        {
            // Arrange
            var connParams = new DriverConnectionParameters
            {
                CloudFetchEnabled = true,
                // Leave other fields null
                Lz4CompressionEnabled = null,
                DirectResultsEnabled = null
            };

            // Act
            var json = JsonSerializer.Serialize(connParams);

            // Assert
            Assert.Contains("\"cloud_fetch_enabled\":true", json);
            Assert.DoesNotContain("lz4_compression_enabled", json);
            Assert.DoesNotContain("direct_results_enabled", json);
        }

        [Fact]
        public void DriverErrorInfo_NullFields_AreOmitted()
        {
            // Arrange
            var errorInfo = new DriverErrorInfo
            {
                ErrorType = "ConnectionError",
                ErrorMessage = "Connection timeout"
                // Leave other fields null
            };

            // Act
            var json = JsonSerializer.Serialize(errorInfo);

            // Assert
            Assert.Contains("\"error_type\":", json);
            Assert.Contains("\"error_message\":", json);
            Assert.DoesNotContain("error_code", json);
            Assert.DoesNotContain("sql_state", json);
            Assert.DoesNotContain("http_status_code", json);
            Assert.DoesNotContain("is_terminal", json);
            Assert.DoesNotContain("retry_attempted", json);
        }

        [Fact]
        public void TelemetryEvent_Deserialization_WorksCorrectly()
        {
            // Arrange
            var json = @"{
                ""session_id"": ""session-abc"",
                ""sql_statement_id"": ""statement-xyz"",
                ""operation_latency_ms"": 200,
                ""system_configuration"": {
                    ""driver_name"": ""Test Driver"",
                    ""driver_version"": ""2.0.0""
                },
                ""sql_execution_event"": {
                    ""result_format"": ""inline"",
                    ""row_count"": 100
                }
            }";

            // Act
            var telemetryEvent = JsonSerializer.Deserialize<TelemetryEvent>(json);

            // Assert
            Assert.NotNull(telemetryEvent);
            Assert.Equal("session-abc", telemetryEvent.SessionId);
            Assert.Equal("statement-xyz", telemetryEvent.SqlStatementId);
            Assert.Equal(200, telemetryEvent.OperationLatencyMs);
            Assert.NotNull(telemetryEvent.SystemConfiguration);
            Assert.Equal("Test Driver", telemetryEvent.SystemConfiguration.DriverName);
            Assert.NotNull(telemetryEvent.SqlExecutionEvent);
            Assert.Equal("inline", telemetryEvent.SqlExecutionEvent.ResultFormat);
            Assert.Equal(100, telemetryEvent.SqlExecutionEvent.RowCount);
        }

        [Fact]
        public void TelemetryEvent_DefaultValues_AreCorrect()
        {
            // Act
            var telemetryEvent = new TelemetryEvent();

            // Assert
            Assert.Null(telemetryEvent.SessionId);
            Assert.Null(telemetryEvent.SqlStatementId);
            Assert.Equal(0L, telemetryEvent.OperationLatencyMs);
            Assert.Null(telemetryEvent.SystemConfiguration);
            Assert.Null(telemetryEvent.SqlExecutionEvent);
            Assert.Null(telemetryEvent.ErrorInfo);
            Assert.Null(telemetryEvent.ConnectionParameters);
        }

        [Fact]
        public void SqlExecutionEvent_AllFields_SerializeWithCorrectNames()
        {
            // Arrange
            var execEvent = new SqlExecutionEvent
            {
                ResultFormat = "cloudfetch",
                ChunkCount = 5,
                BytesDownloaded = 1024,
                CompressionEnabled = true,
                RowCount = 1000,
                PollCount = 2,
                PollLatencyMs = 50,
                TimeToFirstByteMs = 100,
                ExecutionStatus = "succeeded",
                StatementType = "query",
                RetryPerformed = false,
                RetryCount = 0
            };

            // Act
            var json = JsonSerializer.Serialize(execEvent);

            // Assert - verify all property names are snake_case
            Assert.Contains("\"result_format\":", json);
            Assert.Contains("\"chunk_count\":", json);
            Assert.Contains("\"bytes_downloaded\":", json);
            Assert.Contains("\"compression_enabled\":", json);
            Assert.Contains("\"row_count\":", json);
            Assert.Contains("\"poll_count\":", json);
            Assert.Contains("\"poll_latency_ms\":", json);
            Assert.Contains("\"time_to_first_byte_ms\":", json);
            Assert.Contains("\"execution_status\":", json);
            Assert.Contains("\"statement_type\":", json);
            Assert.Contains("\"retry_performed\":", json);
            Assert.Contains("\"retry_count\":", json);
        }

        [Fact]
        public void DriverConnectionParameters_AllFields_SerializeWithCorrectNames()
        {
            // Arrange
            var connParams = new DriverConnectionParameters
            {
                CloudFetchEnabled = true,
                Lz4CompressionEnabled = true,
                DirectResultsEnabled = false,
                MaxDownloadThreads = 8,
                AuthType = "token",
                TransportMode = "https"
            };

            // Act
            var json = JsonSerializer.Serialize(connParams);

            // Assert - verify all property names are snake_case
            Assert.Contains("\"cloud_fetch_enabled\":", json);
            Assert.Contains("\"lz4_compression_enabled\":", json);
            Assert.Contains("\"direct_results_enabled\":", json);
            Assert.Contains("\"max_download_threads\":", json);
            Assert.Contains("\"auth_type\":", json);
            Assert.Contains("\"transport_mode\":", json);
        }

        [Fact]
        public void DriverSystemConfiguration_AllFields_SerializeWithCorrectNames()
        {
            // Arrange
            var sysConfig = new DriverSystemConfiguration
            {
                DriverName = "Test",
                DriverVersion = "1.0",
                OsName = "Linux",
                OsVersion = "5.4",
                OsArch = "x64",
                RuntimeName = ".NET",
                RuntimeVersion = "8.0",
                Locale = "en-US",
                Timezone = "UTC"
            };

            // Act
            var json = JsonSerializer.Serialize(sysConfig);

            // Assert - verify all property names are snake_case
            Assert.Contains("\"driver_name\":", json);
            Assert.Contains("\"driver_version\":", json);
            Assert.Contains("\"os_name\":", json);
            Assert.Contains("\"os_version\":", json);
            Assert.Contains("\"os_arch\":", json);
            Assert.Contains("\"runtime_name\":", json);
            Assert.Contains("\"runtime_version\":", json);
            Assert.Contains("\"locale\":", json);
            Assert.Contains("\"timezone\":", json);
        }

        [Fact]
        public void DriverErrorInfo_AllFields_SerializeWithCorrectNames()
        {
            // Arrange
            var errorInfo = new DriverErrorInfo
            {
                ErrorType = "SqlError",
                ErrorMessage = "Syntax error",
                ErrorCode = "SQL_001",
                SqlState = "42000",
                HttpStatusCode = 400,
                IsTerminal = true,
                RetryAttempted = false
            };

            // Act
            var json = JsonSerializer.Serialize(errorInfo);

            // Assert - verify all property names are snake_case
            Assert.Contains("\"error_type\":", json);
            Assert.Contains("\"error_message\":", json);
            Assert.Contains("\"error_code\":", json);
            Assert.Contains("\"sql_state\":", json);
            Assert.Contains("\"http_status_code\":", json);
            Assert.Contains("\"is_terminal\":", json);
            Assert.Contains("\"retry_attempted\":", json);
        }
    }
}
