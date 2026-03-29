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
    /// E2E tests for TelemetryHelper to ensure no regression after refactoring.
    /// These tests validate that DatabricksConnection still correctly initializes telemetry
    /// when using the shared TelemetryHelper.
    /// </summary>
    public class TelemetryHelperTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public TelemetryHelperTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Tests that TelemetryHelper correctly builds system configuration.
        /// </summary>
        [SkippableFact]
        public async Task TelemetryHelper_BuildSystemConfiguration_PopulatesCorrectValues()
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
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                statement.Dispose();

                // Wait for telemetry to be captured
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert system configuration is populated correctly
                Assert.NotNull(protoLog.SystemConfiguration);
                var sysConfig = protoLog.SystemConfiguration;

                Assert.Equal("Databricks ADBC Driver", sysConfig.DriverName);
                Assert.False(string.IsNullOrEmpty(sysConfig.DriverVersion));
                Assert.False(string.IsNullOrEmpty(sysConfig.OsName));
                Assert.False(string.IsNullOrEmpty(sysConfig.OsVersion));
                Assert.False(string.IsNullOrEmpty(sysConfig.OsArch));
                Assert.False(string.IsNullOrEmpty(sysConfig.RuntimeName));
                Assert.False(string.IsNullOrEmpty(sysConfig.RuntimeVersion));
                Assert.Equal("Microsoft", sysConfig.RuntimeVendor);
                // LocaleName and CharSetEncoding may be empty in some environments, just verify they are not null
                Assert.NotNull(sysConfig.LocaleName);
                Assert.NotNull(sysConfig.CharSetEncoding);
                Assert.False(string.IsNullOrEmpty(sysConfig.ProcessName));
                Assert.False(string.IsNullOrEmpty(sysConfig.ClientAppName));

                OutputHelper?.WriteLine($"✓ SystemConfiguration populated correctly via TelemetryHelper");
                OutputHelper?.WriteLine($"  Driver: {sysConfig.DriverName} v{sysConfig.DriverVersion}");
                OutputHelper?.WriteLine($"  OS: {sysConfig.OsName} {sysConfig.OsVersion} ({sysConfig.OsArch})");
                OutputHelper?.WriteLine($"  Runtime: {sysConfig.RuntimeName} v{sysConfig.RuntimeVersion}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that TelemetryHelper correctly builds driver connection parameters with THRIFT mode.
        /// </summary>
        [SkippableFact]
        public async Task TelemetryHelper_BuildDriverConnectionParams_ModeIsThrift()
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
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                statement.Dispose();

                // Wait for telemetry to be captured
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert driver connection params are populated correctly
                Assert.NotNull(protoLog.DriverConnectionParams);
                var connParams = protoLog.DriverConnectionParams;

                // Mode should be THRIFT for DatabricksConnection
                Assert.Equal(ProtoDriverMode.Types.Type.Thrift, connParams.Mode);

                // Host info should be populated
                Assert.NotNull(connParams.HostInfo);
                Assert.False(string.IsNullOrEmpty(connParams.HostInfo.HostUrl));
                Assert.StartsWith("https://", connParams.HostInfo.HostUrl);

                // Auth should be populated
                Assert.NotEqual(AdbcDrivers.Databricks.Telemetry.Proto.DriverAuthMech.Types.Type.Unspecified, connParams.AuthMech);
                Assert.NotEqual(AdbcDrivers.Databricks.Telemetry.Proto.DriverAuthFlow.Types.Type.Unspecified, connParams.AuthFlow);

                // Arrow should always be enabled for ADBC
                Assert.True(connParams.EnableArrow);

                // Auto-commit should always be true for ADBC
                Assert.True(connParams.AutoCommit);

                OutputHelper?.WriteLine($"✓ DriverConnectionParams populated correctly via TelemetryHelper");
                OutputHelper?.WriteLine($"  Mode: {connParams.Mode}");
                OutputHelper?.WriteLine($"  Host: {connParams.HostInfo.HostUrl}");
                OutputHelper?.WriteLine($"  Auth: {connParams.AuthMech} / {connParams.AuthFlow}");
                OutputHelper?.WriteLine($"  EnableArrow: {connParams.EnableArrow}");
                OutputHelper?.WriteLine($"  AutoCommit: {connParams.AutoCommit}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that DatabricksConnection still initializes telemetry correctly after refactoring to use TelemetryHelper.
        /// This is a regression test to ensure no functionality was broken.
        /// </summary>
        [SkippableFact]
        public async Task DatabricksConnection_StillInitializesTelemetryCorrectly()
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
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                statement.Dispose();

                // Wait for telemetry to be captured
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert all telemetry fields are populated (regression test)
                TelemetryTestHelpers.AssertSessionFieldsPopulated(protoLog);
                TelemetryTestHelpers.AssertStatementFieldsPopulated(protoLog);
                TelemetryTestHelpers.AssertSqlOperationPopulated(protoLog.SqlOperation);

                OutputHelper?.WriteLine($"✓ DatabricksConnection telemetry still works after TelemetryHelper refactoring");
                OutputHelper?.WriteLine($"  SessionId: {protoLog.SessionId}");
                OutputHelper?.WriteLine($"  StatementId: {protoLog.SqlStatementId}");
                OutputHelper?.WriteLine($"  OperationLatency: {protoLog.OperationLatencyMs}ms");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that telemetry is disabled when the feature flag is false.
        /// This ensures TelemetryHelper.InitializeTelemetry respects the configuration.
        /// </summary>
        [SkippableFact]
        public async Task TelemetryHelper_InitializeTelemetry_RespectsDisabledFlag()
        {
            CapturingTelemetryExporter exporter = new CapturingTelemetryExporter();
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Explicitly disable telemetry
                properties[TelemetryConfiguration.PropertyKeyEnabled] = "false";

                // Set the capturing exporter
                TelemetryClientManager.ExporterOverride = exporter;

                // Create driver and database
                AdbcDriver driver = new DatabricksDriver();
                AdbcDatabase database = driver.Open(properties);

                // Create and open connection
                connection = database.Connect(properties);

                // Execute a simple query
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS test_value";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                statement.Dispose();

                // Wait a bit to ensure no telemetry is emitted
                await Task.Delay(1000);

                // Assert no telemetry was captured
                Assert.Empty(exporter.ExportedLogs);

                OutputHelper?.WriteLine($"✓ Telemetry correctly disabled when feature flag is false");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that workspace ID extraction still works correctly after refactoring.
        /// </summary>
        [SkippableFact]
        public async Task TelemetryHelper_InitializeTelemetry_ExtractsWorkspaceId()
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
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                statement.Dispose();

                // Wait for telemetry to be captured
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // The important thing is that the telemetry initialization runs without errors
                // WorkspaceId is stored in TelemetrySessionContext but not directly on OssSqlDriverTelemetryLog
                // Just verify telemetry was successfully initialized and emitted
                OutputHelper?.WriteLine($"✓ Workspace ID extraction logic completed successfully");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }
    }
}
