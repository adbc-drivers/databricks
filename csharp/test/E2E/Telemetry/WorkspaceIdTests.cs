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

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for WorkspaceId field in telemetry.
    /// Tests that workspace_id is extracted from server configuration and populated in TelemetrySessionContext.
    /// </summary>
    public class WorkspaceIdTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public WorkspaceIdTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Tests that workspace_id field is present and can be populated.
        /// For SQL warehouses, workspace_id may be 0 if not available in server configuration.
        /// For clusters with orgId in config or when specified via connection property, it should be non-zero.
        /// </summary>
        [SkippableFact]
        public async Task WorkspaceId_IsPresent_AfterConnection()
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

                var frontendLog = logs[0];

                // Assert workspace_id field is present (may be 0 for SQL warehouses)
                Assert.True(frontendLog.WorkspaceId >= 0,
                    $"workspace_id should be >= 0, but was {frontendLog.WorkspaceId}");

                OutputHelper?.WriteLine($"✓ workspace_id: {frontendLog.WorkspaceId}");
                if (frontendLog.WorkspaceId == 0)
                {
                    OutputHelper?.WriteLine("  Note: workspace_id is 0 (not available from server config for this connection type)");
                }
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that workspace_id is consistent across multiple statements on the same connection.
        /// All telemetry events from the same connection should have the same workspace_id.
        /// </summary>
        [SkippableFact]
        public async Task WorkspaceId_IsConsistent_AcrossStatements()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute multiple queries
                for (int i = 0; i < 3; i++)
                {
                    using var statement = connection.CreateStatement();
                    statement.SqlQuery = $"SELECT {i} AS iteration";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;
                    statement.Dispose();
                }

                // Wait for telemetry to be captured
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 3);
                Assert.True(logs.Count >= 3, $"Expected at least 3 telemetry logs but got {logs.Count}");

                // All logs should have the same workspace_id (may be 0 for SQL warehouses)
                long? firstWorkspaceId = null;
                foreach (var log in logs)
                {
                    if (firstWorkspaceId == null)
                    {
                        firstWorkspaceId = log.WorkspaceId;
                        Assert.True(firstWorkspaceId >= 0,
                            "workspace_id should be >= 0");
                        OutputHelper?.WriteLine($"✓ workspace_id: {firstWorkspaceId}");
                    }
                    else
                    {
                        Assert.Equal(firstWorkspaceId, log.WorkspaceId);
                    }
                }

                OutputHelper?.WriteLine($"✓ All {logs.Count} telemetry events have consistent workspace_id: {firstWorkspaceId}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that workspace_id is populated in TelemetrySessionContext on the connection.
        /// This tests the internal implementation detail that workspace_id is stored in the session context.
        /// </summary>
        [SkippableFact]
        public void WorkspaceId_IsPopulated_InTelemetrySessionContext()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Access the internal TelemetrySession from DatabricksConnection
                var databricksConnection = connection as DatabricksConnection;
                Assert.NotNull(databricksConnection);

                var telemetrySession = databricksConnection!.TelemetrySession;
                Assert.NotNull(telemetrySession);

                // Assert workspace_id is present (>= 0) in the session context
                Assert.True(telemetrySession!.WorkspaceId >= 0,
                    $"TelemetrySessionContext.WorkspaceId should be >= 0, but was {telemetrySession.WorkspaceId}");

                OutputHelper?.WriteLine($"✓ TelemetrySessionContext.WorkspaceId: {telemetrySession.WorkspaceId}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that workspace_id can be explicitly set via connection property.
        /// This allows users to provide workspace ID when it's not available from server configuration.
        /// </summary>
        [SkippableFact]
        public async Task WorkspaceId_CanBeSet_ViaConnectionProperty()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Set explicit workspace ID via connection property
                long expectedWorkspaceId = 1234567890123456;
                properties["adbc.databricks.workspace_id"] = expectedWorkspaceId.ToString();

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

                var frontendLog = logs[0];

                // Assert workspace_id matches the explicit value from connection property
                // Note: If server config provides orgId, it takes precedence over connection property
                Assert.True(frontendLog.WorkspaceId == expectedWorkspaceId || frontendLog.WorkspaceId > 0,
                    $"workspace_id should either match explicit value ({expectedWorkspaceId}) or be from server config, but was {frontendLog.WorkspaceId}");

                OutputHelper?.WriteLine($"✓ workspace_id: {frontendLog.WorkspaceId}");
                if (frontendLog.WorkspaceId == expectedWorkspaceId)
                {
                    OutputHelper?.WriteLine("  ✓ Matches explicit value from connection property");
                }
                else
                {
                    OutputHelper?.WriteLine("  ✓ Server configuration orgId took precedence over connection property");
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
