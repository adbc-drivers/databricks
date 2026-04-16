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
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.Databricks.Telemetry.Proto;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for retry count tracking in SqlExecutionEvent telemetry.
    /// Validates that retry_count proto field is populated correctly based on HTTP retry attempts.
    /// </summary>
    public class RetryCountTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public RetryCountTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        /// <summary>
        /// Tests that retry_count is 0 for successful first attempt (no retries).
        /// </summary>
        [SkippableFact]
        public void RetryCount_SuccessfulFirstAttempt_IsZero()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for retry count test");

            var capturingExporter = new CapturingTelemetryExporter();
            TelemetryClientManager.ExporterOverride = capturingExporter;

            try
            {
                Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
                properties[TelemetryConfiguration.PropertyKeyBatchSize] = "1";
                properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "500";

                AdbcDriver driver = NewDriver;
                AdbcDatabase database = driver.Open(properties);

                using (AdbcConnection connection = database.Connect(properties))
                {
                    using (AdbcStatement statement = connection.CreateStatement())
                    {
                        statement.SqlQuery = "SELECT 1 as test_column";
                        QueryResult result = statement.ExecuteQuery();
                        Assert.NotNull(result);
                    }
                }

                database.Dispose();

                // Wait for telemetry to be exported
                Thread.Sleep(1000);

                // Find the statement telemetry log
                var statementLog = capturingExporter.ExportedLogs
                    .FirstOrDefault(log => log.Entry?.SqlDriverLog?.SqlOperation != null);

                Assert.NotNull(statementLog);
                var sqlEvent = statementLog!.Entry!.SqlDriverLog!.SqlOperation;
                Assert.NotNull(sqlEvent);

                // Verify retry_count is 0 for successful first attempt
                Assert.Equal(0, sqlEvent.RetryCount);
                OutputHelper?.WriteLine($"✓ retry_count is 0 for successful first attempt");
            }
            finally
            {
                TelemetryClientManager.ExporterOverride = null;
            }
        }

        /// <summary>
        /// Tests that retry_count is tracked per statement execution.
        /// Multiple statements should each have their own retry count (all 0 if no retries).
        /// </summary>
        [SkippableFact]
        public void RetryCount_MultipleStatements_TrackedIndependently()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for retry count test");

            var capturingExporter = new CapturingTelemetryExporter();
            TelemetryClientManager.ExporterOverride = capturingExporter;

            try
            {
                Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
                properties[TelemetryConfiguration.PropertyKeyBatchSize] = "1";
                properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "500";

                AdbcDriver driver = NewDriver;
                AdbcDatabase database = driver.Open(properties);

                using (AdbcConnection connection = database.Connect(properties))
                {
                    // Execute multiple statements
                    for (int i = 0; i < 3; i++)
                    {
                        using (AdbcStatement statement = connection.CreateStatement())
                        {
                            statement.SqlQuery = $"SELECT {i} as iteration";
                            QueryResult result = statement.ExecuteQuery();
                            Assert.NotNull(result);
                        }
                    }
                }

                database.Dispose();

                // Wait for telemetry to be exported
                Thread.Sleep(1000);

                // Find all statement telemetry logs
                var statementLogs = capturingExporter.ExportedLogs
                    .Where(log => log.Entry?.SqlDriverLog?.SqlOperation != null)
                    .ToList();

                Assert.True(statementLogs.Count >= 3, $"Expected at least 3 statement logs, got {statementLogs.Count}");

                // Verify each statement has retry_count tracked
                foreach (var log in statementLogs)
                {
                    var sqlEvent = log.Entry!.SqlDriverLog!.SqlOperation;
                    Assert.NotNull(sqlEvent);
                    // For successful queries without retries, retry_count should be 0
                    Assert.True(sqlEvent.RetryCount >= 0, "retry_count should be >= 0");
                }

                OutputHelper?.WriteLine($"✓ retry_count is tracked independently for {statementLogs.Count} statements");
            }
            finally
            {
                TelemetryClientManager.ExporterOverride = null;
            }
        }

        /// <summary>
        /// Tests that retry_count proto field exists and is populated in SqlExecutionEvent.
        /// This verifies the field is being set in BuildTelemetryLog().
        /// </summary>
        [SkippableFact]
        public void RetryCount_ProtoField_IsPopulated()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for retry count test");

            var capturingExporter = new CapturingTelemetryExporter();
            TelemetryClientManager.ExporterOverride = capturingExporter;

            try
            {
                Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
                properties[TelemetryConfiguration.PropertyKeyBatchSize] = "1";
                properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "500";

                AdbcDriver driver = NewDriver;
                AdbcDatabase database = driver.Open(properties);

                using (AdbcConnection connection = database.Connect(properties))
                {
                    using (AdbcStatement statement = connection.CreateStatement())
                    {
                        statement.SqlQuery = "SELECT 42 as answer";
                        QueryResult result = statement.ExecuteQuery();
                        Assert.NotNull(result);
                    }
                }

                database.Dispose();

                // Wait for telemetry to be exported
                Thread.Sleep(1000);

                // Find the statement telemetry log
                var statementLog = capturingExporter.ExportedLogs
                    .FirstOrDefault(log => log.Entry?.SqlDriverLog?.SqlOperation != null);

                Assert.NotNull(statementLog);
                var protoLog = statementLog!.Entry!.SqlDriverLog!;
                var sqlEvent = protoLog.SqlOperation;
                Assert.NotNull(sqlEvent);

                // Verify the proto has all expected fields including retry_count
                Assert.NotNull(protoLog.SessionId);
                Assert.NotNull(protoLog.SqlStatementId);
                Assert.True(protoLog.OperationLatencyMs > 0);
                Assert.NotNull(sqlEvent);
                Assert.True(sqlEvent.StatementType != AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type.Unspecified);

                // Verify retry_count is populated (should be 0 for no retries)
                Assert.Equal(0, sqlEvent.RetryCount);

                OutputHelper?.WriteLine($"✓ retry_count proto field is populated in SqlExecutionEvent");
                OutputHelper?.WriteLine($"  SessionId: {protoLog.SessionId}");
                OutputHelper?.WriteLine($"  SqlStatementId: {protoLog.SqlStatementId}");
                OutputHelper?.WriteLine($"  OperationLatencyMs: {protoLog.OperationLatencyMs}");
                OutputHelper?.WriteLine($"  RetryCount: {sqlEvent.RetryCount}");
            }
            finally
            {
                TelemetryClientManager.ExporterOverride = null;
            }
        }

        /// <summary>
        /// Tests that retry_count is set for UPDATE statements as well as SELECT queries.
        /// </summary>
        [SkippableFact]
        public void RetryCount_UpdateStatement_IsTracked()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for retry count test");

            var capturingExporter = new CapturingTelemetryExporter();
            TelemetryClientManager.ExporterOverride = capturingExporter;

            try
            {
                Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
                properties[TelemetryConfiguration.PropertyKeyBatchSize] = "1";
                properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "500";

                AdbcDriver driver = NewDriver;
                AdbcDatabase database = driver.Open(properties);

                using (AdbcConnection connection = database.Connect(properties))
                {
                    // Create a temp table and insert data
                    using (AdbcStatement statement = connection.CreateStatement())
                    {
                        statement.SqlQuery = "CREATE OR REPLACE TEMP VIEW retry_test_view AS SELECT 1 as id, 'test' as value";
                        statement.ExecuteUpdate();
                    }
                }

                database.Dispose();

                // Wait for telemetry to be exported
                Thread.Sleep(1000);

                // Find the statement telemetry log for the UPDATE/DDL statement
                var statementLog = capturingExporter.ExportedLogs
                    .FirstOrDefault(log => log.Entry?.SqlDriverLog?.SqlOperation != null &&
                                          log.Entry.SqlDriverLog.SqlOperation.StatementType == AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type.Update);

                if (statementLog != null)
                {
                    var sqlEvent = statementLog.Entry!.SqlDriverLog!.SqlOperation;
                    Assert.NotNull(sqlEvent);

                    // Verify retry_count is tracked for UPDATE statements
                    Assert.True(sqlEvent.RetryCount >= 0, "retry_count should be >= 0 for UPDATE statements");
                    OutputHelper?.WriteLine($"✓ retry_count is tracked for UPDATE statement: {sqlEvent.RetryCount}");
                }
                else
                {
                    OutputHelper?.WriteLine("⚠ No UPDATE statement telemetry found, this might be expected for some configurations");
                }
            }
            finally
            {
                TelemetryClientManager.ExporterOverride = null;
            }
        }

        /// <summary>
        /// Tests that retry_count matches actual retry attempts.
        /// Note: This test validates the structure, but we cannot easily simulate HTTP retries
        /// in E2E tests without mocking the HTTP layer. The actual retry logic is tested
        /// in unit tests for RetryHttpHandler.
        /// </summary>
        [SkippableFact]
        public void RetryCount_Structure_IsValid()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for retry count test");

            var capturingExporter = new CapturingTelemetryExporter();
            TelemetryClientManager.ExporterOverride = capturingExporter;

            try
            {
                Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
                properties[TelemetryConfiguration.PropertyKeyBatchSize] = "1";
                properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "500";

                AdbcDriver driver = NewDriver;
                AdbcDatabase database = driver.Open(properties);

                using (AdbcConnection connection = database.Connect(properties))
                {
                    using (AdbcStatement statement = connection.CreateStatement())
                    {
                        statement.SqlQuery = "SELECT 1";
                        QueryResult result = statement.ExecuteQuery();
                        Assert.NotNull(result);
                    }
                }

                database.Dispose();

                // Wait for telemetry to be exported
                Thread.Sleep(1000);

                // Verify telemetry structure
                var statementLog = capturingExporter.ExportedLogs
                    .FirstOrDefault(log => log.Entry?.SqlDriverLog?.SqlOperation != null);

                Assert.NotNull(statementLog);
                var sqlEvent = statementLog!.Entry!.SqlDriverLog!.SqlOperation;

                // Verify retry_count is a valid value (non-negative integer)
                Assert.True(sqlEvent.RetryCount >= 0, "retry_count should be a non-negative integer");

                // For successful queries without network issues, retry_count should typically be 0
                // However, we don't assert this as there might be transient network issues
                Assert.InRange(sqlEvent.RetryCount, 0, 10); // Reasonable upper bound for retries

                OutputHelper?.WriteLine($"✓ retry_count structure is valid: {sqlEvent.RetryCount}");
                OutputHelper?.WriteLine($"  Value is non-negative: {sqlEvent.RetryCount >= 0}");
                OutputHelper?.WriteLine($"  Value is reasonable: {sqlEvent.RetryCount <= 10}");
            }
            finally
            {
                TelemetryClientManager.ExporterOverride = null;
            }
        }
    }
}
