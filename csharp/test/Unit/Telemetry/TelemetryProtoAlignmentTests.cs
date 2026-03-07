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
using System.Linq;
using System.Text.Json;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.Databricks.Telemetry.Proto;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;
using Google.Protobuf;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests to verify proto-generated telemetry classes work correctly.
    /// Proto source: universe/proto/logs/frontend/oss-sql-driver-telemetry/sql_driver_telemetry.proto
    /// </summary>
    public class TelemetryProtoAlignmentTests
    {
        #region Proto Schema Validation Tests

        /// <summary>
        /// Verifies OssSqlDriverTelemetryLog has all expected fields from proto.
        /// </summary>
        [Fact]
        public void OssSqlDriverTelemetryLog_HasExpectedFields()
        {
            var protoFields = GetProtoFieldNames<OssSqlDriverTelemetryLog>();

            var expectedFields = new[]
            {
                "session_id",
                "sql_statement_id",
                "system_configuration",
                "driver_connection_params",
                "auth_type",
                "vol_operation",
                "sql_operation",
                "error_info",
                "operation_latency_ms"
            };

            foreach (var field in expectedFields)
            {
                Assert.Contains(field, protoFields);
            }
        }

        /// <summary>
        /// Verifies SqlExecutionEvent has all expected fields including nested messages.
        /// </summary>
        [Fact]
        public void SqlExecutionEvent_HasExpectedFields()
        {
            var protoFields = GetProtoFieldNames<SqlExecutionEvent>();

            var expectedFields = new[]
            {
                "statement_type",
                "is_compressed",
                "execution_result",
                "chunk_id",
                "retry_count",
                "chunk_details",
                "result_latency",
                "operation_detail",
                "java_uses_patched_arrow"
            };

            foreach (var field in expectedFields)
            {
                Assert.Contains(field, protoFields);
            }
        }

        /// <summary>
        /// Verifies ChunkDetails has all expected fields.
        /// </summary>
        [Fact]
        public void ChunkDetails_HasExpectedFields()
        {
            var protoFields = GetProtoFieldNames<ChunkDetails>();

            var expectedFields = new[]
            {
                "initial_chunk_latency_millis",
                "slowest_chunk_latency_millis",
                "total_chunks_present",
                "total_chunks_iterated",
                "sum_chunks_download_time_millis"
            };

            foreach (var field in expectedFields)
            {
                Assert.Contains(field, protoFields);
            }
        }

        /// <summary>
        /// Verifies OperationDetail has all expected fields.
        /// </summary>
        [Fact]
        public void OperationDetail_HasExpectedFields()
        {
            var protoFields = GetProtoFieldNames<OperationDetail>();

            var expectedFields = new[]
            {
                "n_operation_status_calls",
                "operation_status_latency_millis",
                "operation_type",
                "is_internal_call"
            };

            foreach (var field in expectedFields)
            {
                Assert.Contains(field, protoFields);
            }
        }

        /// <summary>
        /// Verifies ResultLatency has all expected fields.
        /// </summary>
        [Fact]
        public void ResultLatency_HasExpectedFields()
        {
            var protoFields = GetProtoFieldNames<ResultLatency>();

            var expectedFields = new[]
            {
                "result_set_ready_latency_millis",
                "result_set_consumption_latency_millis"
            };

            foreach (var field in expectedFields)
            {
                Assert.Contains(field, protoFields);
            }
        }

        /// <summary>
        /// Verifies DriverErrorInfo has expected fields.
        /// </summary>
        [Fact]
        public void DriverErrorInfo_HasExpectedFields()
        {
            var protoFields = GetProtoFieldNames<DriverErrorInfo>();

            var expectedFields = new[]
            {
                "error_name",
                "stack_trace"
            };

            foreach (var field in expectedFields)
            {
                Assert.Contains(field, protoFields);
            }
        }

        #endregion

        #region Protobuf Binary Serialization Tests

        /// <summary>
        /// Tests protobuf binary serialization roundtrip.
        /// </summary>
        [Fact]
        public void Proto_OssSqlDriverTelemetryLog_BinaryRoundtrip()
        {
            var protoMessage = CreateFullProtoMessage();

            // Serialize to bytes
            var bytes = protoMessage.ToByteArray();
            Assert.NotEmpty(bytes);

            // Deserialize back
            var deserializedMessage = OssSqlDriverTelemetryLog.Parser.ParseFrom(bytes);

            // Verify roundtrip
            Assert.Equal(protoMessage.SessionId, deserializedMessage.SessionId);
            Assert.Equal(protoMessage.SqlStatementId, deserializedMessage.SqlStatementId);
            Assert.Equal(protoMessage.OperationLatencyMs, deserializedMessage.OperationLatencyMs);
            Assert.Equal(protoMessage.AuthType, deserializedMessage.AuthType);
            Assert.Equal(protoMessage.SystemConfiguration.DriverName, deserializedMessage.SystemConfiguration.DriverName);
            Assert.Equal(protoMessage.SqlOperation.ChunkDetails.TotalChunksPresent,
                deserializedMessage.SqlOperation.ChunkDetails.TotalChunksPresent);
            Assert.Equal(protoMessage.SqlOperation.OperationDetail.NOperationStatusCalls,
                deserializedMessage.SqlOperation.OperationDetail.NOperationStatusCalls);
        }

        #endregion

        #region Protobuf JSON Serialization Tests

        /// <summary>
        /// Tests that proto JSON formatter produces correct camelCase field names per proto3 JSON spec.
        /// </summary>
        [Fact]
        public void Proto_JsonFormatter_ProducesCamelCaseFields()
        {
            var protoMessage = CreateFullProtoMessage();

            // Use JsonFormatter.Default as per user suggestion
            var json = JsonFormatter.Default.Format(protoMessage);

            // Verify camelCase field names (proto3 JSON mapping uses camelCase)
            Assert.Contains("\"sessionId\"", json);
            Assert.Contains("\"sqlStatementId\"", json);
            Assert.Contains("\"operationLatencyMs\"", json);
            Assert.Contains("\"systemConfiguration\"", json);
            Assert.Contains("\"sqlOperation\"", json);
            Assert.Contains("\"errorInfo\"", json);
        }

        /// <summary>
        /// Tests proto JSON roundtrip serialization using default formatter/parser.
        /// </summary>
        [Fact]
        public void Proto_JsonRoundtrip()
        {
            var protoMessage = CreateFullProtoMessage();

            // Use default formatter and parser
            var json = JsonFormatter.Default.Format(protoMessage);
            var deserializedMessage = JsonParser.Default.Parse<OssSqlDriverTelemetryLog>(json);

            Assert.Equal(protoMessage.SessionId, deserializedMessage.SessionId);
            Assert.Equal(protoMessage.SqlStatementId, deserializedMessage.SqlStatementId);
            Assert.Equal(protoMessage.OperationLatencyMs, deserializedMessage.OperationLatencyMs);
        }

        /// <summary>
        /// Tests that the telemetry JSON converter serializes proto enums as integer values
        /// (not string names) to match what the Databricks telemetry endpoint expects.
        /// </summary>
        [Fact]
        public void Proto_TelemetryJsonConverter_SerializesEnumsAsIntegers()
        {
            var protoMessage = CreateFullProtoMessage();

            var frontendLog = new TelemetryFrontendLog
            {
                WorkspaceId = 12345,
                FrontendLogEventId = "test-event-id",
                Context = new FrontendLogContext
                {
                    TimestampMillis = 1000,
                },
                Entry = new FrontendLogEntry
                {
                    SqlDriverLog = protoMessage
                }
            };

            var json = JsonSerializer.Serialize(frontendLog, TelemetryJsonOptions.Default);

            // Parse the sql_driver_log portion to inspect proto enum serialization
            using var doc = JsonDocument.Parse(json);
            var sqlDriverLog = doc.RootElement
                .GetProperty("entry")
                .GetProperty("sql_driver_log");

            // DriverConnectionParams enum fields should be integers
            var connParams = sqlDriverLog.GetProperty("driver_connection_params");
            Assert.Equal(JsonValueKind.Number, connParams.GetProperty("auth_mech").ValueKind);
            Assert.Equal(2, connParams.GetProperty("auth_mech").GetInt32()); // DRIVER_AUTH_MECH_PAT
            Assert.Equal(JsonValueKind.Number, connParams.GetProperty("auth_flow").ValueKind);
            Assert.Equal(1, connParams.GetProperty("auth_flow").GetInt32()); // DRIVER_AUTH_FLOW_TOKEN_PASSTHROUGH
            Assert.Equal(JsonValueKind.Number, connParams.GetProperty("mode").ValueKind);
            Assert.Equal(1, connParams.GetProperty("mode").GetInt32()); // DRIVER_MODE_THRIFT

            // SqlOperation enum fields should be integers
            var sqlOp = sqlDriverLog.GetProperty("sql_operation");
            Assert.Equal(JsonValueKind.Number, sqlOp.GetProperty("statement_type").ValueKind);
            Assert.Equal(1, sqlOp.GetProperty("statement_type").GetInt32()); // STATEMENT_QUERY = 1
            Assert.Equal(JsonValueKind.Number, sqlOp.GetProperty("execution_result").ValueKind);
            Assert.Equal(3, sqlOp.GetProperty("execution_result").GetInt32()); // EXECUTION_RESULT_EXTERNAL_LINKS = 3

            // Enums should NOT be serialized as string names
            Assert.DoesNotContain("DRIVER_AUTH_MECH_PAT", json);
            Assert.DoesNotContain("DRIVER_AUTH_FLOW_TOKEN_PASSTHROUGH", json);
            Assert.DoesNotContain("DRIVER_MODE_THRIFT", json);
        }

        /// <summary>
        /// Tests that default/zero-value enum fields are included in serialized output
        /// (not omitted) so the server always receives complete telemetry data.
        /// </summary>
        [Fact]
        public void Proto_TelemetryJsonConverter_IncludesDefaultEnumValues()
        {
            // Create a message with default (unspecified) enum values
            var protoMessage = new OssSqlDriverTelemetryLog
            {
                SessionId = "test-session",
                DriverConnectionParams = new DriverConnectionParameters
                {
                    // AuthMech and AuthFlow default to Unspecified (0)
                    HttpPath = "/sql/1.0/warehouses/test"
                }
            };

            var frontendLog = new TelemetryFrontendLog
            {
                WorkspaceId = 12345,
                FrontendLogEventId = "test-event-id",
                Context = new FrontendLogContext { TimestampMillis = 1000 },
                Entry = new FrontendLogEntry { SqlDriverLog = protoMessage }
            };

            var json = JsonSerializer.Serialize(frontendLog, TelemetryJsonOptions.Default);

            // Default enum values (0) should still be present in the output
            using var doc = JsonDocument.Parse(json);
            var connParams = doc.RootElement
                .GetProperty("entry")
                .GetProperty("sql_driver_log")
                .GetProperty("driver_connection_params");
            Assert.Equal(0, connParams.GetProperty("auth_mech").GetInt32());
            Assert.Equal(0, connParams.GetProperty("auth_flow").GetInt32());
            Assert.Equal(0, connParams.GetProperty("mode").GetInt32());
        }

        #endregion

        #region FrontendLog Integration Tests

        /// <summary>
        /// Tests that FrontendLogEntry correctly references proto type.
        /// </summary>
        [Fact]
        public void FrontendLogEntry_UsesProtoType()
        {
            var entry = new FrontendLogEntry
            {
                SqlDriverLog = CreateFullProtoMessage()
            };

            Assert.NotNull(entry.SqlDriverLog);
            Assert.Equal("test-session-123", entry.SqlDriverLog.SessionId);
        }

        /// <summary>
        /// Tests TelemetryFrontendLog serialization with proto types using custom converter.
        /// </summary>
        [Fact]
        public void TelemetryFrontendLog_JsonSerialization_WithProtoConverter()
        {
            var frontendLog = new TelemetryFrontendLog
            {
                WorkspaceId = 12345,
                FrontendLogEventId = Guid.NewGuid().ToString(),
                Context = new FrontendLogContext
                {
                    TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ClientContext = new TelemetryClientContext
                    {
                        UserAgent = "AdbcDatabricksDriver/1.0.0"
                    }
                },
                Entry = new FrontendLogEntry
                {
                    SqlDriverLog = CreateFullProtoMessage()
                }
            };

            // Use the telemetry JSON options that include the proto converter
            var json = JsonSerializer.Serialize(frontendLog, TelemetryJsonOptions.Default);

            Assert.NotEmpty(json);
            Assert.Contains("\"workspace_id\"", json);
            Assert.Contains("\"frontend_log_event_id\"", json);
            Assert.Contains("\"sql_driver_log\"", json);

            // Verify proto fields use snake_case (PreserveProtoFieldNames)
            Assert.Contains("session_id", json);
            Assert.Contains("sql_statement_id", json);
        }

        #endregion

        #region Helper Methods

        private static OssSqlDriverTelemetryLog CreateFullProtoMessage()
        {
            return new OssSqlDriverTelemetryLog
            {
                SessionId = "test-session-123",
                SqlStatementId = "test-statement-456",
                OperationLatencyMs = 1500,
                DriverConnectionParams = new DriverConnectionParameters
                {
                    HttpPath = "/sql/1.0/warehouses/abc123",
                    Mode = DriverModeType.DriverModeThrift,
                    AuthMech = DriverAuthMechType.DriverAuthMechPat,
                    AuthFlow = DriverAuthFlowType.DriverAuthFlowTokenPassthrough,
                    HostInfo = new HostDetails
                    {
                        HostUrl = "https://test.databricks.com:443",
                        Port = 0
                    }
                },
                SystemConfiguration = new DriverSystemConfiguration
                {
                    DriverName = "adbc-databricks",
                    DriverVersion = "1.0.0",
                    OsName = "Linux",
                    RuntimeName = ".NET",
                    RuntimeVersion = "8.0"
                },
                SqlOperation = new SqlExecutionEvent
                {
                    StatementType = StatementType.Query,
                    IsCompressed = true,
                    ExecutionResult = ExecutionResultFormat.ExternalLinks,
                    RetryCount = 0,
                    ChunkDetails = new ChunkDetails
                    {
                        TotalChunksPresent = 10,
                        TotalChunksIterated = 10,
                        InitialChunkLatencyMillis = 50,
                        SlowestChunkLatencyMillis = 200,
                        SumChunksDownloadTimeMillis = 1000
                    },
                    OperationDetail = new OperationDetail
                    {
                        NOperationStatusCalls = 5,
                        OperationStatusLatencyMillis = 250,
                        OperationType = OperationType.ExecuteStatementAsync,
                        IsInternalCall = false
                    },
                    ResultLatency = new ResultLatency
                    {
                        ResultSetReadyLatencyMillis = 100,
                        ResultSetConsumptionLatencyMillis = 1400
                    }
                },
                ErrorInfo = new DriverErrorInfo
                {
                    ErrorName = "HttpRequestException",
                    StackTrace = "at Method() in File.cs:line 123"
                }
            };
        }

        private static HashSet<string> GetProtoFieldNames<T>() where T : IMessage, new()
        {
            var message = new T();
            var descriptor = message.Descriptor;
            return new HashSet<string>(descriptor.Fields.InFieldNumberOrder().Select(f => f.Name));
        }

        #endregion
    }
}
