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
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for telemetry in StatementExecutionStatement (SEA protocol).
    /// Validates that SEA statements emit telemetry with:
    /// - operation_type = EXECUTE_STATEMENT_ASYNC
    /// - execution_result maps correctly from SEA disposition
    /// - Error telemetry works for SEA failures
    /// - operation_latency_ms is positive
    /// </summary>
    public class SEAStatementTelemetryTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public SEAStatementTelemetryTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Helper method to create a REST (SEA) connection with capturing telemetry.
        /// </summary>
        private (AdbcConnection Connection, CapturingTelemetryExporter Exporter) CreateRestConnectionWithCapturingTelemetry()
        {
            var properties = new Dictionary<string, string>
            {
                [DatabricksParameters.Protocol] = "rest",
            };

            // Use URI if available (connection will parse host and warehouse ID from it)
            if (!string.IsNullOrEmpty(TestConfiguration.Uri))
            {
                properties[AdbcOptions.Uri] = TestConfiguration.Uri;
            }
            else
            {
                // Fall back to individual properties
                if (!string.IsNullOrEmpty(TestConfiguration.HostName))
                {
                    properties[SparkParameters.HostName] = TestConfiguration.HostName;
                }
                if (!string.IsNullOrEmpty(TestConfiguration.Path))
                {
                    properties[SparkParameters.Path] = TestConfiguration.Path;
                }
            }

            // Token-based authentication (PAT or OAuth access token)
            if (!string.IsNullOrEmpty(TestConfiguration.Token))
            {
                properties[SparkParameters.Token] = TestConfiguration.Token;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.AccessToken))
            {
                properties[SparkParameters.AccessToken] = TestConfiguration.AccessToken;
            }

            // OAuth M2M authentication (client_credentials)
            if (!string.IsNullOrEmpty(TestConfiguration.AuthType))
            {
                properties[SparkParameters.AuthType] = TestConfiguration.AuthType;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.OAuthGrantType))
            {
                properties[DatabricksParameters.OAuthGrantType] = TestConfiguration.OAuthGrantType;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.OAuthClientId))
            {
                properties[DatabricksParameters.OAuthClientId] = TestConfiguration.OAuthClientId;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.OAuthClientSecret))
            {
                properties[DatabricksParameters.OAuthClientSecret] = TestConfiguration.OAuthClientSecret;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.OAuthScope))
            {
                properties[DatabricksParameters.OAuthScope] = TestConfiguration.OAuthScope;
            }

            // Enable telemetry
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";

            // Create and set the capturing exporter
            var exporter = new CapturingTelemetryExporter();
            TelemetryClientManager.ExporterOverride = exporter;

            // Create driver and database
            AdbcDriver driver = new DatabricksDriver();
            AdbcDatabase database = driver.Open(properties);

            // Create and open connection
            AdbcConnection connection = database.Connect(properties);

            return (connection, exporter);
        }

        /// <summary>
        /// Test that SEA query emits telemetry.
        /// Exit criterion: SEA query emits telemetry
        /// </summary>
        [SkippableFact]
        public async Task SEAStatement_QueryEmitsTelemetry()
        {
            AdbcConnection? connection = null;

            try
            {
                (connection, var exporter) = CreateRestConnectionWithCapturingTelemetry();

                // Execute a simple query
                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT 1 AS test_column";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;
                    statement.Dispose();
                }

                // Wait for telemetry to be emitted
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);

                // Assert at least one log was emitted
                Assert.NotEmpty(logs);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert basic fields are populated
                Assert.NotNull(protoLog.SqlOperation);
                Assert.Equal(StatementType.Query, protoLog.SqlOperation.StatementType);

                OutputHelper?.WriteLine($"✓ SEA query emitted telemetry");
                OutputHelper?.WriteLine($"  - statement_type: {protoLog.SqlOperation.StatementType}");
                OutputHelper?.WriteLine($"  - operation_latency_ms: {protoLog.OperationLatencyMs}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that operation_type is EXECUTE_STATEMENT_ASYNC for SEA.
        /// Exit criterion: operation_type is EXECUTE_STATEMENT_ASYNC for SEA
        /// </summary>
        [SkippableFact]
        public async Task SEAStatement_OperationTypeIsExecuteStatementAsync()
        {
            AdbcConnection? connection = null;

            try
            {
                (connection, var exporter) = CreateRestConnectionWithCapturingTelemetry();

                // Execute a simple query
                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT 42 AS answer";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;
                    statement.Dispose();
                }

                // Wait for telemetry to be emitted
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);

                // Get captured telemetry logs
                Assert.NotEmpty(logs);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert operation_type is EXECUTE_STATEMENT_ASYNC (SEA is always async)
                Assert.NotNull(protoLog.SqlOperation);
                Assert.NotNull(protoLog.SqlOperation.OperationDetail);
                Assert.Equal(OperationType.ExecuteStatementAsync, protoLog.SqlOperation.OperationDetail.OperationType);

                OutputHelper?.WriteLine($"✓ operation_type = {protoLog.SqlOperation.OperationDetail.OperationType}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that execution_result matches disposition.
        /// Exit criterion: execution_result maps correctly from SEA disposition
        /// </summary>
        [SkippableFact]
        public async Task SEAStatement_ExecutionResultMatchesDisposition()
        {
            AdbcConnection? connection = null;

            try
            {
                (connection, var exporter) = CreateRestConnectionWithCapturingTelemetry();

                // Execute a simple query
                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT 'test' AS col";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;
                    statement.Dispose();
                }

                // Wait for telemetry to be emitted
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);

                // Get captured telemetry logs
                Assert.NotEmpty(logs);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert execution_result is set (should be either INLINE_ARROW or EXTERNAL_LINKS)
                Assert.NotNull(protoLog.SqlOperation);
                var resultFormat = protoLog.SqlOperation.ExecutionResult;

                // Result format should be one of the valid formats
                Assert.True(
                    resultFormat == ExecutionResultFormat.InlineArrow ||
                    resultFormat == ExecutionResultFormat.ExternalLinks,
                    $"Expected execution_result to be INLINE_ARROW or EXTERNAL_LINKS, but got {resultFormat}");

                OutputHelper?.WriteLine($"✓ execution_result = {resultFormat}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that error_info is populated for failed queries.
        /// Exit criterion: Error telemetry works for SEA failures
        /// </summary>
        [SkippableFact]
        public async Task SEAStatement_ErrorInfoForFailedQueries()
        {
            AdbcConnection? connection = null;

            try
            {
                (connection, var exporter) = CreateRestConnectionWithCapturingTelemetry();

                // Execute a query that will fail
                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT * FROM nonexistent_table_12345";

                    try
                    {
                        var result = statement.ExecuteQuery();
                        using var reader = result.Stream;
                        Assert.Fail("Query should have failed");
                    }
                    catch (AdbcException)
                    {
                        // Expected exception - telemetry should be emitted
                    }

                    statement.Dispose();
                }

                // Wait for telemetry to be emitted
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);

                // Get captured telemetry logs
                Assert.NotEmpty(logs);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert error_info is populated
                Assert.NotNull(protoLog.ErrorInfo);
                Assert.False(string.IsNullOrEmpty(protoLog.ErrorInfo.ErrorName));

                OutputHelper?.WriteLine($"✓ error_info populated:");
                OutputHelper?.WriteLine($"  - error_name: {protoLog.ErrorInfo.ErrorName}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that operation_latency_ms is positive.
        /// Exit criterion: operation_latency_ms is positive
        /// </summary>
        [SkippableFact]
        public async Task SEAStatement_OperationLatencyIsPositive()
        {
            AdbcConnection? connection = null;

            try
            {
                (connection, var exporter) = CreateRestConnectionWithCapturingTelemetry();

                // Execute a simple query
                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT 123 AS number";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;
                    statement.Dispose();
                }

                // Wait for telemetry to be emitted
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);

                // Get captured telemetry logs
                Assert.NotEmpty(logs);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert operation_latency_ms is positive
                Assert.True(protoLog.OperationLatencyMs > 0,
                    $"operation_latency_ms should be positive, but got {protoLog.OperationLatencyMs}");

                OutputHelper?.WriteLine($"✓ operation_latency_ms = {protoLog.OperationLatencyMs}ms");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Comprehensive test validating all SEA statement telemetry exit criteria.
        /// This test ensures:
        /// 1. SEA query emits telemetry
        /// 2. operation_type is EXECUTE_STATEMENT_ASYNC
        /// 3. execution_result maps correctly from SEA disposition
        /// 4. Error telemetry works for SEA failures
        /// 5. operation_latency_ms is positive
        /// </summary>
        [SkippableFact]
        public async Task SEAStatement_AllExitCriteriaMet()
        {
            AdbcConnection? connection = null;

            try
            {
                (connection, var exporter) = CreateRestConnectionWithCapturingTelemetry();

                // Exit criterion 1: SEA query emits telemetry
                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT 'hello' AS greeting";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;
                    statement.Dispose();
                }

                // Wait for telemetry to be emitted
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);

                // Get captured telemetry logs
                Assert.NotEmpty(logs);
                OutputHelper?.WriteLine("✓ Exit criterion 1: SEA query emits telemetry");

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Exit criterion 2: operation_type is EXECUTE_STATEMENT_ASYNC
                Assert.NotNull(protoLog.SqlOperation);
                Assert.NotNull(protoLog.SqlOperation.OperationDetail);
                Assert.Equal(OperationType.ExecuteStatementAsync, protoLog.SqlOperation.OperationDetail.OperationType);
                OutputHelper?.WriteLine($"✓ Exit criterion 2: operation_type = {protoLog.SqlOperation.OperationDetail.OperationType}");

                // Exit criterion 3: execution_result maps correctly from SEA disposition
                var resultFormat = protoLog.SqlOperation.ExecutionResult;
                Assert.True(
                    resultFormat == ExecutionResultFormat.InlineArrow ||
                    resultFormat == ExecutionResultFormat.ExternalLinks);
                OutputHelper?.WriteLine($"✓ Exit criterion 3: execution_result = {resultFormat}");

                // Exit criterion 5: operation_latency_ms is positive
                Assert.True(protoLog.OperationLatencyMs > 0);
                OutputHelper?.WriteLine($"✓ Exit criterion 5: operation_latency_ms = {protoLog.OperationLatencyMs}ms");

                // Exit criterion 4: Error telemetry works for SEA failures
                // Reset exporter for error test
                exporter.Reset();

                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT * FROM nonexistent_table_xyz";

                    try
                    {
                        var result = statement.ExecuteQuery();
                        using var reader = result.Stream;
                        Assert.Fail("Query should have failed");
                    }
                    catch (AdbcException)
                    {
                        // Expected exception
                    }

                    statement.Dispose();
                }

                // Wait for telemetry
                var errorLogs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                Assert.NotEmpty(errorLogs);

                var errorProtoLog = TelemetryTestHelpers.GetProtoLog(errorLogs[0]);
                Assert.NotNull(errorProtoLog.ErrorInfo);
                Assert.False(string.IsNullOrEmpty(errorProtoLog.ErrorInfo.ErrorName));
                OutputHelper?.WriteLine($"✓ Exit criterion 4: Error telemetry works (error_name = {errorProtoLog.ErrorInfo.ErrorName})");

                OutputHelper?.WriteLine("\n✅ All SEA statement telemetry exit criteria met successfully!");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }
    }
}
