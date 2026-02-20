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
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.Databricks.Telemetry.Proto;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry.Models
{
    /// <summary>
    /// Tests for TelemetryFrontendLog serialization and structure.
    /// </summary>
    public class TelemetryFrontendLogTests
    {
        [Fact]
        public void TelemetryFrontendLog_Serialization_MatchesJdbcFormat()
        {
            // Arrange
            var frontendLog = new TelemetryFrontendLog
            {
                WorkspaceId = 12345678901234L,
                FrontendLogEventId = "event-uuid-123",
                Context = new FrontendLogContext
                {
                    TimestampMillis = 1700000000000L,
                    ClientContext = new TelemetryClientContext
                    {
                        UserAgent = "AdbcDatabricksDriver/1.0.0"
                    }
                },
                Entry = new FrontendLogEntry
                {
                    SqlDriverLog = new OssSqlDriverTelemetryLog
                    {
                        SessionId = "session-123",
                        SqlStatementId = "statement-456"
                    }
                }
            };

            // Act - use TelemetryJsonOptions which includes the proto converter
            var json = JsonSerializer.Serialize(frontendLog, TelemetryJsonOptions.Default);
            var parsed = JsonDocument.Parse(json);
            var root = parsed.RootElement;

            // Assert - verify JDBC format property names (snake_case for wrapper, camelCase for proto)
            Assert.True(root.TryGetProperty("workspace_id", out var workspaceId));
            Assert.Equal(12345678901234L, workspaceId.GetInt64());

            Assert.True(root.TryGetProperty("frontend_log_event_id", out var eventId));
            Assert.Equal("event-uuid-123", eventId.GetString());

            Assert.True(root.TryGetProperty("context", out var context));
            Assert.True(context.TryGetProperty("timestamp_millis", out _));
            Assert.True(context.TryGetProperty("client_context", out var clientContext));
            Assert.True(clientContext.TryGetProperty("user_agent", out _));

            Assert.True(root.TryGetProperty("entry", out var entry));
            Assert.True(entry.TryGetProperty("sql_driver_log", out _));
        }

        [Fact]
        public void TelemetryFrontendLog_NullContext_OmittedInSerialization()
        {
            // Arrange
            var frontendLog = new TelemetryFrontendLog
            {
                WorkspaceId = 12345L,
                FrontendLogEventId = "event-123",
                Context = null,
                Entry = null
            };

            // Act
            var json = JsonSerializer.Serialize(frontendLog, TelemetryJsonOptions.Default);

            // Assert - null fields should be omitted
            Assert.DoesNotContain("context", json);
            Assert.DoesNotContain("entry", json);
        }

        [Fact]
        public void TelemetryFrontendLog_PropertyNames_AreSnakeCase()
        {
            // Arrange
            var frontendLog = new TelemetryFrontendLog
            {
                WorkspaceId = 12345L,
                FrontendLogEventId = "event-123"
            };

            // Act
            var json = JsonSerializer.Serialize(frontendLog, TelemetryJsonOptions.Default);

            // Assert - property names should be snake_case
            Assert.Contains("\"workspace_id\":", json);
            Assert.Contains("\"frontend_log_event_id\":", json);
            Assert.DoesNotContain("\"WorkspaceId\":", json);
            Assert.DoesNotContain("\"FrontendLogEventId\":", json);
        }

        [Fact]
        public void TelemetryFrontendLog_FullStructure_SerializesCorrectly()
        {
            // Arrange
            var frontendLog = new TelemetryFrontendLog
            {
                WorkspaceId = 12345678901234L,
                FrontendLogEventId = "550e8400-e29b-41d4-a716-446655440000",
                Context = new FrontendLogContext
                {
                    TimestampMillis = 1700000000000L,
                    ClientContext = new TelemetryClientContext
                    {
                        UserAgent = "AdbcDatabricksDriver/1.0.0 (.NET 8.0; Windows 10)"
                    }
                },
                Entry = new FrontendLogEntry
                {
                    SqlDriverLog = new OssSqlDriverTelemetryLog
                    {
                        SessionId = "session-uuid-123",
                        SqlStatementId = "statement-uuid-456",
                        OperationLatencyMs = 150,
                        SystemConfiguration = new DriverSystemConfiguration
                        {
                            DriverName = "Databricks ADBC Driver",
                            DriverVersion = "1.0.0",
                            OsName = "Windows",
                            RuntimeName = ".NET",
                            RuntimeVersion = "8.0.0"
                        },
                        SqlOperation = new SqlExecutionEvent
                        {
                            ExecutionResult = ExecutionResultFormat.ExecutionResultExternalLinks,
                            IsCompressed = true
                        }
                    }
                }
            };

            // Act
            var json = JsonSerializer.Serialize(frontendLog, TelemetryJsonOptions.Default);

            // Assert
            Assert.NotEmpty(json);
            Assert.Contains("workspace_id", json);
            Assert.Contains("frontend_log_event_id", json);
            Assert.Contains("sql_driver_log", json);
            Assert.Contains("sessionId", json);  // Proto uses camelCase
            Assert.Contains("sqlStatementId", json);
        }

        [Fact]
        public void TelemetryFrontendLog_DefaultValues_AreCorrect()
        {
            // Act
            var frontendLog = new TelemetryFrontendLog();

            // Assert
            Assert.Equal(0L, frontendLog.WorkspaceId);
            Assert.Equal(string.Empty, frontendLog.FrontendLogEventId);
            Assert.Null(frontendLog.Context);
            Assert.Null(frontendLog.Entry);
        }

        [Fact]
        public void FrontendLogContext_NullClientContext_OmittedInSerialization()
        {
            // Arrange
            var context = new FrontendLogContext
            {
                TimestampMillis = 1700000000000L,
                ClientContext = null
            };

            // Act
            var json = JsonSerializer.Serialize(context, TelemetryJsonOptions.Default);

            // Assert
            Assert.Contains("\"timestamp_millis\":", json);
            Assert.DoesNotContain("client_context", json);
        }

        [Fact]
        public void FrontendLogEntry_NullSqlDriverLog_OmittedInSerialization()
        {
            // Arrange
            var entry = new FrontendLogEntry
            {
                SqlDriverLog = null
            };

            // Act
            var json = JsonSerializer.Serialize(entry, TelemetryJsonOptions.Default);

            // Assert
            Assert.Equal("{}", json);
        }

        [Fact]
        public void TelemetryClientContext_NullUserAgent_OmittedInSerialization()
        {
            // Arrange
            var clientContext = new TelemetryClientContext
            {
                UserAgent = null
            };

            // Act
            var json = JsonSerializer.Serialize(clientContext, TelemetryJsonOptions.Default);

            // Assert
            Assert.Equal("{}", json);
        }
    }
}
