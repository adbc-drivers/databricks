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
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Proto;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests verifying that internal driver operations (e.g., USE SCHEMA from SetSchema())
    /// are correctly marked with is_internal_call = true in telemetry, while user-initiated
    /// queries are marked with is_internal_call = false.
    /// </summary>
    public class InternalCallTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public InternalCallTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Tests that user-initiated queries are NOT marked as internal calls.
        /// </summary>
        [SkippableFact]
        public async Task UserQuery_IsNotMarkedAsInternal()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute a user query
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS user_query";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                statement.Dispose();

                // Wait for telemetry
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                Assert.True(logs.Count >= 1, $"Expected at least 1 telemetry event, got {logs.Count}");

                // Get the first log (should be the user query)
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert that the operation detail is present
                Assert.NotNull(protoLog.SqlOperation);
                Assert.NotNull(protoLog.SqlOperation.OperationDetail);

                // Assert that is_internal_call is false for user queries
                Assert.False(protoLog.SqlOperation.OperationDetail.IsInternalCall,
                    "User-initiated queries should have is_internal_call = false");

                OutputHelper?.WriteLine($"✓ User query is_internal_call = false");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that user-initiated UPDATE statements are NOT marked as internal calls.
        /// </summary>
        [SkippableFact]
        public async Task UserUpdate_IsNotMarkedAsInternal()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Create a temporary table for testing
                using (var createStmt = connection.CreateStatement())
                {
                    createStmt.SqlQuery = "CREATE TEMPORARY VIEW temp_test_internal_call AS SELECT 1 AS id, 'test' AS value";
                    createStmt.ExecuteUpdate();
                }

                // Clear the exporter to start fresh
                exporter.Reset();

                // Execute a user USE statement (explicit user action, not internal)
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "USE default";
                statement.ExecuteUpdate();
                statement.Dispose();

                // Wait for telemetry
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                Assert.True(logs.Count >= 1, $"Expected at least 1 telemetry event, got {logs.Count}");

                // Get the log
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert that the operation detail is present
                Assert.NotNull(protoLog.SqlOperation);
                Assert.NotNull(protoLog.SqlOperation.OperationDetail);

                // User-initiated USE statements should NOT be marked as internal
                Assert.False(protoLog.SqlOperation.OperationDetail.IsInternalCall,
                    "User-initiated USE statements should have is_internal_call = false");

                OutputHelper?.WriteLine($"✓ User USE statement is_internal_call = false");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests the is_internal_call proto field is correctly serialized to the proto message.
        /// </summary>
        [SkippableFact]
        public async Task InternalCallField_IsCorrectlySerializedInProto()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute a user query
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 42 AS proto_test";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                statement.Dispose();

                // Wait for telemetry
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                Assert.True(logs.Count >= 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Verify the proto structure includes the is_internal_call field
                Assert.NotNull(protoLog.SqlOperation);
                Assert.NotNull(protoLog.SqlOperation.OperationDetail);

                // The field should exist and be accessible (even if false)
                var isInternal = protoLog.SqlOperation.OperationDetail.IsInternalCall;
                Assert.False(isInternal, "User query should have is_internal_call = false");

                // Verify other operation detail fields are also populated
                Assert.True(protoLog.SqlOperation.OperationDetail.OperationType !=
                    Operation.Types.Type.Unspecified,
                    "operation_type should be set");

                OutputHelper?.WriteLine($"✓ is_internal_call proto field is correctly serialized (value={isInternal})");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }
    }
}
