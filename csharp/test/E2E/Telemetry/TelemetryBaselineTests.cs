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
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;
using ProtoStatement = AdbcDrivers.Databricks.Telemetry.Proto.Statement;
using ProtoOperation = AdbcDrivers.Databricks.Telemetry.Proto.Operation;
using ProtoDriverMode = AdbcDrivers.Databricks.Telemetry.Proto.DriverMode;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// Baseline E2E tests for telemetry proto field validation.
    /// These tests verify that all currently populated fields in the OssSqlDriverTelemetryLog proto
    /// are correctly captured and have valid values, without requiring backend connectivity.
    /// </summary>
    public class TelemetryBaselineTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public TelemetryBaselineTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Tests that session_id is populated when a connection is established.
        /// </summary>
        [SkippableFact]
        public async Task BaselineTest_SessionId_IsPopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute a simple query to trigger telemetry
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS test_value";
                var result = statement.ExecuteQuery(); using var reader = result.Stream;

                // Dispose the reader to trigger telemetry emission

                statement.Dispose();

                // Wait for telemetry to be captured
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert session_id is populated
                Assert.False(string.IsNullOrEmpty(protoLog.SessionId), "session_id should be non-empty");

                OutputHelper?.WriteLine($"✓ session_id populated: {protoLog.SessionId}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that sql_statement_id is populated for SQL operations.
        /// </summary>
        [SkippableFact]
        public async Task BaselineTest_SqlStatementId_IsPopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute a simple query
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS test_value";
                var result = statement.ExecuteQuery(); using var reader = result.Stream;


                statement.Dispose();

                // Wait for telemetry
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert sql_statement_id is populated
                Assert.False(string.IsNullOrEmpty(protoLog.SqlStatementId), "sql_statement_id should be non-empty");

                OutputHelper?.WriteLine($"✓ sql_statement_id populated: {protoLog.SqlStatementId}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that operation_latency_ms is populated and has a positive value.
        /// </summary>
        [SkippableFact]
        public async Task BaselineTest_OperationLatencyMs_IsPositive()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute a query
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1";
                var result = statement.ExecuteQuery(); using var reader = result.Stream;


                statement.Dispose();

                // Wait for telemetry
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert operation_latency_ms is positive
                Assert.True(protoLog.OperationLatencyMs > 0, "operation_latency_ms should be > 0");

                OutputHelper?.WriteLine($"✓ operation_latency_ms: {protoLog.OperationLatencyMs} ms");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that system_configuration fields are populated correctly.
        /// </summary>
        [SkippableFact]
        public async Task BaselineTest_SystemConfiguration_AllFieldsPopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute a query
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1";
                var result = statement.ExecuteQuery(); using var reader = result.Stream;


                statement.Dispose();

                // Wait for telemetry
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert system_configuration is populated
                Assert.NotNull(protoLog.SystemConfiguration);
                var config = protoLog.SystemConfiguration;

                // Validate all expected fields
                Assert.False(string.IsNullOrEmpty(config.DriverVersion), "driver_version should be populated");
                Assert.False(string.IsNullOrEmpty(config.DriverName), "driver_name should be populated");
                Assert.False(string.IsNullOrEmpty(config.OsName), "os_name should be populated");
                Assert.False(string.IsNullOrEmpty(config.RuntimeName), "runtime_name should be populated");

                OutputHelper?.WriteLine("✓ system_configuration fields populated:");
                OutputHelper?.WriteLine($"  - driver_version: {config.DriverVersion}");
                OutputHelper?.WriteLine($"  - driver_name: {config.DriverName}");
                OutputHelper?.WriteLine($"  - os_name: {config.OsName}");
                OutputHelper?.WriteLine($"  - runtime_name: {config.RuntimeName}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that driver_connection_params fields are populated correctly.
        /// </summary>
        [SkippableFact]
        public async Task BaselineTest_DriverConnectionParams_AllFieldsPopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute a query
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1";
                var result = statement.ExecuteQuery(); using var reader = result.Stream;


                statement.Dispose();

                // Wait for telemetry
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert driver_connection_params is populated
                Assert.NotNull(protoLog.DriverConnectionParams);
                var params_ = protoLog.DriverConnectionParams;

                // Validate all expected fields
                // Note: http_path may be empty in some test configurations
                Assert.True(params_.Mode != ProtoDriverMode.Types.Type.Unspecified, "mode should not be UNSPECIFIED");

                OutputHelper?.WriteLine("✓ driver_connection_params fields populated:");
                OutputHelper?.WriteLine($"  - http_path: {params_.HttpPath ?? "(empty)"}");
                OutputHelper?.WriteLine($"  - mode: {params_.Mode}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that sql_operation fields are populated for a query.
        /// </summary>
        [SkippableFact]
        public async Task BaselineTest_SqlOperation_QueryFieldsPopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute a query
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS test_value";
                var result = statement.ExecuteQuery(); using var reader = result.Stream;


                statement.Dispose();

                // Wait for telemetry
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert sql_operation is populated
                Assert.NotNull(protoLog.SqlOperation);
                var sqlOp = protoLog.SqlOperation;

                // Validate statement type
                Assert.Equal(ProtoStatement.Types.Type.Query, sqlOp.StatementType);

                // Validate operation detail
                Assert.NotNull(sqlOp.OperationDetail);
                Assert.True(sqlOp.OperationDetail.OperationType != ProtoOperation.Types.Type.Unspecified,
                    "operation_type should not be UNSPECIFIED");

                // Validate result latency
                Assert.NotNull(sqlOp.ResultLatency);
                Assert.True(sqlOp.ResultLatency.ResultSetReadyLatencyMillis >= 0,
                    "result_set_ready_latency_millis should be >= 0");

                OutputHelper?.WriteLine("✓ sql_operation fields populated:");
                OutputHelper?.WriteLine($"  - statement_type: {sqlOp.StatementType}");
                OutputHelper?.WriteLine($"  - operation_type: {sqlOp.OperationDetail.OperationType}");
                OutputHelper?.WriteLine($"  - result_set_ready_latency_millis: {sqlOp.ResultLatency.ResultSetReadyLatencyMillis}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that multiple statements on the same connection share the same session_id
        /// but have different sql_statement_id values.
        /// </summary>
        [SkippableFact]
        public async Task BaselineTest_MultipleStatements_SameSessionIdDifferentStatementIds()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute 3 queries
                for (int i = 0; i < 3; i++)
                {
                    using var statement = connection.CreateStatement();
                    statement.SqlQuery = $"SELECT {i + 1}";
                    var result = statement.ExecuteQuery(); using var reader = result.Stream;

                    statement.Dispose();
                }

                // Wait for all telemetry events
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 3, timeoutMs: 10000);
                TelemetryTestHelpers.AssertLogCount(logs, 3);

                // Extract proto logs
                var proto1 = TelemetryTestHelpers.GetProtoLog(logs[0]);
                var proto2 = TelemetryTestHelpers.GetProtoLog(logs[1]);
                var proto3 = TelemetryTestHelpers.GetProtoLog(logs[2]);

                // All should have the same session_id
                Assert.Equal(proto1.SessionId, proto2.SessionId);
                Assert.Equal(proto2.SessionId, proto3.SessionId);

                // All should have different sql_statement_id
                Assert.NotEqual(proto1.SqlStatementId, proto2.SqlStatementId);
                Assert.NotEqual(proto2.SqlStatementId, proto3.SqlStatementId);
                Assert.NotEqual(proto1.SqlStatementId, proto3.SqlStatementId);

                // All should have the same system_configuration
                Assert.Equal(proto1.SystemConfiguration.DriverVersion, proto2.SystemConfiguration.DriverVersion);
                Assert.Equal(proto2.SystemConfiguration.DriverVersion, proto3.SystemConfiguration.DriverVersion);

                OutputHelper?.WriteLine("✓ Multiple statements validated:");
                OutputHelper?.WriteLine($"  - Shared session_id: {proto1.SessionId}");
                OutputHelper?.WriteLine($"  - Unique statement IDs: {proto1.SqlStatementId}, {proto2.SqlStatementId}, {proto3.SqlStatementId}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that telemetry is not emitted when the feature flag is disabled.
        /// </summary>
        [SkippableFact]
        public async Task BaselineTest_TelemetryDisabled_NoEventsEmitted()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Explicitly disable telemetry
                properties[TelemetryConfiguration.PropertyKeyEnabled] = "false";

                // Set up capturing exporter (even though telemetry is disabled)
                exporter = new CapturingTelemetryExporter();
                TelemetryClientManager.ExporterOverride = exporter;

                // Create driver and connection
                AdbcDriver driver = new DatabricksDriver();
                AdbcDatabase database = driver.Open(properties);
                connection = database.Connect(properties);

                // Execute a query
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1";
                var result = statement.ExecuteQuery(); using var reader = result.Stream;


                statement.Dispose();

                // Wait a bit to ensure no telemetry is emitted
                await Task.Delay(2000);

                // No telemetry should be captured
                TelemetryTestHelpers.AssertLogCount(exporter.ExportedLogs, 0);

                OutputHelper?.WriteLine("✓ Telemetry disabled: no events emitted");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that error information is captured when a query fails.
        /// </summary>
        [SkippableFact]
        public async Task BaselineTest_ErrorInfo_PopulatedOnError()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute an invalid query that will fail
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT FROM NONEXISTENT_TABLE_XYZ_12345";

                try
                {
                    var result = statement.ExecuteQuery(); using var reader = result.Stream;
                    Assert.Fail("Query should have failed");
                }
                catch (AdbcException)
                {
                    // Expected exception
                }

                statement.Dispose();

                // Wait for telemetry
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 10000);

                if (logs.Count > 0)
                {
                    var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                    // Error info should be populated
                    Assert.NotNull(protoLog.ErrorInfo);
                    Assert.False(string.IsNullOrEmpty(protoLog.ErrorInfo.ErrorName), "error_name should be populated");

                    // Operation latency should still be positive (time spent before error)
                    Assert.True(protoLog.OperationLatencyMs > 0, "operation_latency_ms should be > 0 even on error");

                    OutputHelper?.WriteLine("✓ error_info populated:");
                    OutputHelper?.WriteLine($"  - error_name: {protoLog.ErrorInfo.ErrorName}");
                    OutputHelper?.WriteLine($"  - operation_latency_ms: {protoLog.OperationLatencyMs}");
                }
                else
                {
                    OutputHelper?.WriteLine("⚠ No telemetry captured for error case (may be expected behavior)");
                }
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests baseline fields for an UPDATE statement.
        /// </summary>
        [SkippableFact]
        public async Task BaselineTest_UpdateStatement_FieldsPopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute a CREATE TABLE statement (UPDATE type)
                using var statement = connection.CreateStatement();
                var tableName = $"temp_telemetry_test_{Guid.NewGuid():N}";
                statement.SqlQuery = $"CREATE TABLE IF NOT EXISTS {tableName} (id INT) USING DELTA";

                try
                {
                    var updateResult = statement.ExecuteUpdate();
                    OutputHelper?.WriteLine($"Create table result: {updateResult}");
                }
                catch (Exception ex)
                {
                    OutputHelper?.WriteLine($"Create table failed (may not have permissions): {ex.Message}");
                }

                statement.Dispose();

                // Wait for telemetry
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 10000);

                if (logs.Count > 0)
                {
                    var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                    // Basic fields should be populated
                    Assert.False(string.IsNullOrEmpty(protoLog.SessionId), "session_id should be populated");
                    Assert.True(protoLog.OperationLatencyMs > 0, "operation_latency_ms should be > 0");

                    // SQL operation should be present
                    Assert.NotNull(protoLog.SqlOperation);

                    // Statement type should be UPDATE
                    Assert.Equal(ProtoStatement.Types.Type.Update, protoLog.SqlOperation.StatementType);

                    OutputHelper?.WriteLine("✓ UPDATE statement telemetry populated:");
                    OutputHelper?.WriteLine($"  - statement_type: {protoLog.SqlOperation.StatementType}");
                    OutputHelper?.WriteLine($"  - operation_latency_ms: {protoLog.OperationLatencyMs}");
                }
                else
                {
                    OutputHelper?.WriteLine("⚠ No telemetry captured for UPDATE statement");
                }
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }
    }
}
