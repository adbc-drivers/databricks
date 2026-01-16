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
    /// Tests for JDBC-compatible telemetry data model classes.
    /// Verifies JSON serialization format matches Databricks telemetry backend expectations.
    /// </summary>
    public class TelemetryMetricTests
    {
        #region TelemetryRequest Tests

        [Fact]
        public void TelemetryRequest_Serialization_ProducesValidJson()
        {
            // Arrange
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "session-123",
                SqlStatementId = "stmt-456",
                OperationLatencyMs = 1500
            };

            var frontendLog = TelemetryFrontendLog.Create(
                workspaceId: 123456789,
                telemetryEvent: telemetryEvent,
                userAgent: "DatabricksAdbcDriver/1.0.0");

            var request = TelemetryRequest.Create(new[] { frontendLog });

            // Act
            string json = request.ToJson();

            // Assert
            Assert.NotNull(json);
            Assert.NotEmpty(json);

            using var jsonDoc = JsonDocument.Parse(json);
            var root = jsonDoc.RootElement;

            // Verify top-level structure
            Assert.True(root.TryGetProperty("uploadTime", out var uploadTime));
            Assert.True(uploadTime.GetInt64() > 0);

            Assert.True(root.TryGetProperty("items", out var items));
            Assert.Equal(JsonValueKind.Array, items.ValueKind);
            Assert.Equal(0, items.GetArrayLength()); // items should always be empty

            Assert.True(root.TryGetProperty("protoLogs", out var protoLogs));
            Assert.Equal(JsonValueKind.Array, protoLogs.ValueKind);
            Assert.Equal(1, protoLogs.GetArrayLength());

            // Verify protoLogs contains a JSON string
            var protoLogEntry = protoLogs[0].GetString();
            Assert.NotNull(protoLogEntry);

            // Parse the nested JSON string
            using var nestedDoc = JsonDocument.Parse(protoLogEntry);
            var nestedRoot = nestedDoc.RootElement;
            Assert.True(nestedRoot.TryGetProperty("workspace_id", out var workspaceId));
            Assert.Equal(123456789, workspaceId.GetInt64());
        }

        [Fact]
        public void TelemetryRequest_Create_SetsUploadTimeToCurrentTime()
        {
            // Arrange
            var beforeTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var request = TelemetryRequest.Create(new List<TelemetryFrontendLog>());
            var afterTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Assert
            Assert.InRange(request.UploadTime, beforeTime, afterTime);
        }

        [Fact]
        public void TelemetryRequest_Create_WithMultipleEvents_SerializesAll()
        {
            // Arrange
            var events = new List<TelemetryFrontendLog>
            {
                TelemetryFrontendLog.Create(111, new TelemetryEvent { SessionId = "s1" }, "agent"),
                TelemetryFrontendLog.Create(222, new TelemetryEvent { SessionId = "s2" }, "agent"),
                TelemetryFrontendLog.Create(333, new TelemetryEvent { SessionId = "s3" }, "agent")
            };

            // Act
            var request = TelemetryRequest.Create(events);

            // Assert
            Assert.Equal(3, request.ProtoLogs.Count);
            Assert.Empty(request.Items);
        }

        #endregion

        #region TelemetryFrontendLog Tests

        [Fact]
        public void TelemetryFrontendLog_Create_SetsAllFields()
        {
            // Arrange
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "session-abc",
                SqlStatementId = "stmt-xyz",
                OperationLatencyMs = 500
            };

            // Act
            var frontendLog = TelemetryFrontendLog.Create(
                workspaceId: 987654321,
                telemetryEvent: telemetryEvent,
                userAgent: "TestAgent/2.0");

            // Assert
            Assert.Equal(987654321, frontendLog.WorkspaceId);
            Assert.NotNull(frontendLog.FrontendLogEventId);
            Assert.NotEmpty(frontendLog.FrontendLogEventId);

            Assert.NotNull(frontendLog.Context);
            Assert.NotNull(frontendLog.Context.ClientContext);
            Assert.True(frontendLog.Context.ClientContext.TimestampMillis > 0);
            Assert.Equal("TestAgent/2.0", frontendLog.Context.ClientContext.UserAgent);

            Assert.NotNull(frontendLog.Entry);
            Assert.NotNull(frontendLog.Entry.SqlDriverLog);
            Assert.Equal("session-abc", frontendLog.Entry.SqlDriverLog.SessionId);
        }

        [Fact]
        public void TelemetryFrontendLog_Create_GeneratesUniqueEventIds()
        {
            // Arrange
            var telemetryEvent = new TelemetryEvent { SessionId = "test" };

            // Act
            var log1 = TelemetryFrontendLog.Create(1, telemetryEvent, "agent");
            var log2 = TelemetryFrontendLog.Create(1, telemetryEvent, "agent");

            // Assert
            Assert.NotEqual(log1.FrontendLogEventId, log2.FrontendLogEventId);
        }

        [Fact]
        public void TelemetryFrontendLog_Serialization_ProducesCorrectJsonPropertyNames()
        {
            // Arrange
            var frontendLog = TelemetryFrontendLog.Create(
                123, new TelemetryEvent { SessionId = "s1" }, "agent");

            // Act
            var options = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
                WriteIndented = false
            };
            string json = JsonSerializer.Serialize(frontendLog, options);

            // Assert - verify snake_case property names
            Assert.Contains("\"workspace_id\"", json);
            Assert.Contains("\"frontend_log_event_id\"", json);
            Assert.Contains("\"client_context\"", json);
            Assert.Contains("\"timestamp_millis\"", json);
            Assert.Contains("\"user_agent\"", json);
            Assert.Contains("\"sql_driver_log\"", json);
        }

        #endregion

        #region TelemetryEvent Tests

        [Fact]
        public void TelemetryEvent_Serialization_IncludesAllFields()
        {
            // Arrange
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "session-full",
                SqlStatementId = "stmt-full",
                AuthType = "PAT",
                OperationLatencyMs = 2500,
                SystemConfiguration = new DriverSystemConfiguration
                {
                    DriverName = "Test Driver",
                    DriverVersion = "1.0.0"
                },
                DriverConnectionParams = new DriverConnectionParameters
                {
                    HttpPath = "/sql/1.0/warehouses/abc123",
                    Mode = "thrift"
                },
                SqlOperation = new SqlExecutionEvent
                {
                    StatementType = "QUERY",
                    ExecutionResult = "CLOUD_FETCH"
                }
            };

            // Act
            var options = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
            };
            string json = JsonSerializer.Serialize(telemetryEvent, options);

            // Assert
            using var jsonDoc = JsonDocument.Parse(json);
            var root = jsonDoc.RootElement;

            Assert.True(root.TryGetProperty("session_id", out var sessionId));
            Assert.Equal("session-full", sessionId.GetString());

            Assert.True(root.TryGetProperty("sql_statement_id", out var stmtId));
            Assert.Equal("stmt-full", stmtId.GetString());

            Assert.True(root.TryGetProperty("auth_type", out var authType));
            Assert.Equal("PAT", authType.GetString());

            Assert.True(root.TryGetProperty("operation_latency_ms", out var latency));
            Assert.Equal(2500, latency.GetInt64());

            Assert.True(root.TryGetProperty("system_configuration", out var sysConfig));
            Assert.True(sysConfig.TryGetProperty("driver_name", out var driverName));
            Assert.Equal("Test Driver", driverName.GetString());

            Assert.True(root.TryGetProperty("driver_connection_params", out var connParams));
            Assert.True(connParams.TryGetProperty("http_path", out var httpPath));
            Assert.Equal("/sql/1.0/warehouses/abc123", httpPath.GetString());

            Assert.True(root.TryGetProperty("sql_operation", out var sqlOp));
            Assert.True(sqlOp.TryGetProperty("statement_type", out var stmtType));
            Assert.Equal("QUERY", stmtType.GetString());
        }

        [Fact]
        public void TelemetryEvent_Serialization_OmitsNullFields()
        {
            // Arrange
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "session-minimal",
                OperationLatencyMs = 100
                // All other fields are null
            };

            // Act
            var options = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
            };
            string json = JsonSerializer.Serialize(telemetryEvent, options);

            // Assert
            using var jsonDoc = JsonDocument.Parse(json);
            var root = jsonDoc.RootElement;

            Assert.True(root.TryGetProperty("session_id", out _));
            Assert.True(root.TryGetProperty("operation_latency_ms", out _));

            // Verify null fields are NOT present
            Assert.False(root.TryGetProperty("sql_statement_id", out _));
            Assert.False(root.TryGetProperty("system_configuration", out _));
            Assert.False(root.TryGetProperty("driver_connection_params", out _));
            Assert.False(root.TryGetProperty("sql_operation", out _));
            Assert.False(root.TryGetProperty("error_info", out _));
        }

        #endregion

        #region DriverSystemConfiguration Tests

        [Fact]
        public void DriverSystemConfiguration_CreateDefault_PopulatesSystemInfo()
        {
            // Act
            var config = DriverSystemConfiguration.CreateDefault("1.2.3");

            // Assert
            Assert.Equal("Databricks ADBC Driver", config.DriverName);
            Assert.Equal("1.2.3", config.DriverVersion);
            Assert.NotNull(config.OsName);
            Assert.NotNull(config.OsArch);
            Assert.Equal(".NET", config.RuntimeName);
            Assert.NotNull(config.RuntimeVersion);
            Assert.Equal("UTF-8", config.CharSetEncoding);
        }

        [Fact]
        public void DriverSystemConfiguration_Serialization_UsesCorrectPropertyNames()
        {
            // Arrange
            var config = new DriverSystemConfiguration
            {
                DriverName = "Test",
                DriverVersion = "1.0",
                OsName = "Linux",
                OsVersion = "5.4",
                OsArch = "x64",
                RuntimeName = ".NET",
                RuntimeVersion = "8.0",
                LocaleName = "en-US",
                CharSetEncoding = "UTF-8"
            };

            // Act
            var options = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
            };
            string json = JsonSerializer.Serialize(config, options);

            // Assert
            Assert.Contains("\"driver_name\"", json);
            Assert.Contains("\"driver_version\"", json);
            Assert.Contains("\"os_name\"", json);
            Assert.Contains("\"os_version\"", json);
            Assert.Contains("\"os_arch\"", json);
            Assert.Contains("\"runtime_name\"", json);
            Assert.Contains("\"runtime_version\"", json);
            Assert.Contains("\"locale_name\"", json);
            Assert.Contains("\"char_set_encoding\"", json);
        }

        #endregion

        #region DriverConnectionParameters Tests

        [Fact]
        public void DriverConnectionParameters_Serialization_IncludesAllFields()
        {
            // Arrange
            var params_ = new DriverConnectionParameters
            {
                HttpPath = "/sql/1.0/warehouses/test",
                Mode = "thrift",
                AuthMech = "PAT",
                EnableArrow = true,
                EnableDirectResults = true,
                EnableCloudFetch = true,
                EnableLz4Compression = false
            };

            // Act
            var options = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
            };
            string json = JsonSerializer.Serialize(params_, options);

            // Assert
            using var jsonDoc = JsonDocument.Parse(json);
            var root = jsonDoc.RootElement;

            Assert.True(root.TryGetProperty("http_path", out var httpPath));
            Assert.Equal("/sql/1.0/warehouses/test", httpPath.GetString());

            Assert.True(root.TryGetProperty("mode", out var mode));
            Assert.Equal("thrift", mode.GetString());

            Assert.True(root.TryGetProperty("enable_cloud_fetch", out var cloudFetch));
            Assert.True(cloudFetch.GetBoolean());

            Assert.True(root.TryGetProperty("enable_lz4_compression", out var lz4));
            Assert.False(lz4.GetBoolean());
        }

        #endregion

        #region SqlExecutionEvent Tests

        [Fact]
        public void SqlExecutionEvent_Serialization_IncludesAllFields()
        {
            // Arrange
            var sqlEvent = new SqlExecutionEvent
            {
                StatementType = "QUERY",
                IsCompressed = true,
                ExecutionResult = "CLOUD_FETCH",
                RetryCount = 2,
                ChunkCount = 10,
                TotalBytesDownloaded = 5242880
            };

            // Act
            var options = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
            };
            string json = JsonSerializer.Serialize(sqlEvent, options);

            // Assert
            using var jsonDoc = JsonDocument.Parse(json);
            var root = jsonDoc.RootElement;

            Assert.True(root.TryGetProperty("statement_type", out var stmtType));
            Assert.Equal("QUERY", stmtType.GetString());

            Assert.True(root.TryGetProperty("is_compressed", out var compressed));
            Assert.True(compressed.GetBoolean());

            Assert.True(root.TryGetProperty("execution_result", out var execResult));
            Assert.Equal("CLOUD_FETCH", execResult.GetString());

            Assert.True(root.TryGetProperty("retry_count", out var retryCount));
            Assert.Equal(2, retryCount.GetInt32());

            Assert.True(root.TryGetProperty("chunk_count", out var chunkCount));
            Assert.Equal(10, chunkCount.GetInt32());

            Assert.True(root.TryGetProperty("total_bytes_downloaded", out var totalBytes));
            Assert.Equal(5242880, totalBytes.GetInt64());
        }

        #endregion

        #region DriverErrorInfo Tests

        [Fact]
        public void DriverErrorInfo_FromException_CapturesTypeAndMessage()
        {
            // Arrange
            var exception = new InvalidOperationException("Test error message");

            // Act
            var errorInfo = DriverErrorInfo.FromException(exception);

            // Assert
            Assert.Equal("InvalidOperationException", errorInfo.ErrorName);
            Assert.Equal("Test error message", errorInfo.ErrorMessage);
        }

        [Fact]
        public void DriverErrorInfo_Serialization_ProducesCorrectJson()
        {
            // Arrange
            var errorInfo = new DriverErrorInfo
            {
                ErrorName = "SqlException",
                ErrorMessage = "Syntax error near 'SELECT'"
            };

            // Act
            var options = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
            };
            string json = JsonSerializer.Serialize(errorInfo, options);

            // Assert
            Assert.Contains("\"error_name\"", json);
            Assert.Contains("\"SqlException\"", json);
            Assert.Contains("\"error_message\"", json);
            Assert.Contains("Syntax error", json);
        }

        #endregion

        #region Full Pipeline Tests

        [Fact]
        public void FullPipeline_ConnectionEvent_ProducesValidTelemetryRequest()
        {
            // Arrange - Connection open event
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "connection-session-001",
                SystemConfiguration = DriverSystemConfiguration.CreateDefault("1.0.0"),
                DriverConnectionParams = new DriverConnectionParameters
                {
                    HttpPath = "/sql/1.0/warehouses/abc",
                    Mode = "thrift",
                    EnableCloudFetch = true
                },
                AuthType = "PAT",
                OperationLatencyMs = 250
            };

            var frontendLog = TelemetryFrontendLog.Create(
                workspaceId: 123456789,
                telemetryEvent: telemetryEvent,
                userAgent: "DatabricksAdbcDriver/1.0.0");

            // Act
            var request = TelemetryRequest.Create(new[] { frontendLog });
            string json = request.ToJson();

            // Assert - Verify complete structure
            Assert.NotNull(json);
            using var jsonDoc = JsonDocument.Parse(json);
            var root = jsonDoc.RootElement;

            // Verify top-level
            Assert.True(root.TryGetProperty("uploadTime", out _));
            Assert.True(root.TryGetProperty("items", out _));
            Assert.True(root.TryGetProperty("protoLogs", out var protoLogs));

            // Parse nested protoLog
            var protoLogJson = protoLogs[0].GetString();
            using var nestedDoc = JsonDocument.Parse(protoLogJson!);
            var nestedRoot = nestedDoc.RootElement;

            // Verify nested structure
            Assert.True(nestedRoot.TryGetProperty("workspace_id", out _));
            Assert.True(nestedRoot.TryGetProperty("frontend_log_event_id", out _));
            Assert.True(nestedRoot.TryGetProperty("context", out var context));
            Assert.True(context.TryGetProperty("client_context", out _));
            Assert.True(nestedRoot.TryGetProperty("entry", out var entry));
            Assert.True(entry.TryGetProperty("sql_driver_log", out var sqlDriverLog));
            Assert.True(sqlDriverLog.TryGetProperty("session_id", out _));
            Assert.True(sqlDriverLog.TryGetProperty("system_configuration", out _));
            Assert.True(sqlDriverLog.TryGetProperty("driver_connection_params", out _));
        }

        [Fact]
        public void FullPipeline_StatementEvent_ProducesValidTelemetryRequest()
        {
            // Arrange - Statement execution event
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "session-stmt",
                SqlStatementId = "stmt-001",
                SqlOperation = new SqlExecutionEvent
                {
                    StatementType = "QUERY",
                    ExecutionResult = "CLOUD_FETCH",
                    ChunkCount = 5,
                    TotalBytesDownloaded = 1048576,
                    IsCompressed = true
                },
                OperationLatencyMs = 3500
            };

            var frontendLog = TelemetryFrontendLog.Create(
                workspaceId: 999888777,
                telemetryEvent: telemetryEvent,
                userAgent: "DatabricksAdbcDriver/1.0.0");

            // Act
            var request = TelemetryRequest.Create(new[] { frontendLog });
            string json = request.ToJson();

            // Assert
            Assert.NotNull(json);
            Assert.Contains("sql_statement_id", json);
            Assert.Contains("stmt-001", json);
            Assert.Contains("sql_operation", json);
            Assert.Contains("CLOUD_FETCH", json);
        }

        [Fact]
        public void FullPipeline_ErrorEvent_ProducesValidTelemetryRequest()
        {
            // Arrange - Error event
            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "session-error",
                SqlStatementId = "stmt-error-001",
                ErrorInfo = DriverErrorInfo.FromException(new Exception("Query failed")),
                OperationLatencyMs = 100
            };

            var frontendLog = TelemetryFrontendLog.Create(
                workspaceId: 111222333,
                telemetryEvent: telemetryEvent,
                userAgent: "DatabricksAdbcDriver/1.0.0");

            // Act
            var request = TelemetryRequest.Create(new[] { frontendLog });
            string json = request.ToJson();

            // Assert
            Assert.NotNull(json);
            Assert.Contains("error_info", json);
            Assert.Contains("error_name", json);
            Assert.Contains("Exception", json);
            Assert.Contains("Query failed", json);
        }

        #endregion
    }
}
