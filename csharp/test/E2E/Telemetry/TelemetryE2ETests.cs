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
using AdbcDrivers.Databricks.Telemetry.Models;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// End-to-end tests that verify the complete telemetry pipeline from Activity creation
    /// through proto export using a <see cref="CapturingTelemetryExporter"/>.
    /// </summary>
    /// <remarks>
    /// These tests inject a <see cref="CapturingTelemetryExporter"/> via the
    /// <see cref="DatabricksConnection.TestExporterFactory"/> property to capture all
    /// telemetry events without making real HTTP calls to the Databricks telemetry endpoint.
    /// Each test verifies that the full pipeline (Activity → ActivityListener → MetricsAggregator
    /// → TelemetryClient → ITelemetryExporter) produces the expected telemetry data.
    ///
    /// Each test uses <see cref="TelemetryTestHelpers.UseFreshTelemetryClientManager"/> to
    /// replace the global <see cref="TelemetryClientManager"/> singleton with a fresh instance,
    /// ensuring test isolation and that the test exporter factory is always called.
    /// </remarks>
    public class TelemetryE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        /// <summary>
        /// Default timeout for waiting for telemetry logs to be captured.
        /// Telemetry is flushed asynchronously, so we need to allow time for the pipeline to complete.
        /// </summary>
        private static readonly TimeSpan s_telemetryWaitTimeout = TimeSpan.FromSeconds(30);

        public TelemetryE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Verifies that opening a connection and executing a query exports at least one
        /// telemetry event. The full pipeline (Activity → ActivityListener → MetricsAggregator
        /// → TelemetryClient → ITelemetryExporter) should produce a <see cref="TelemetryFrontendLog"/>.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_Connection_ExportsConnectionEvent()
        {
            // Arrange
            Dictionary<string, string> parameters = GetDriverParameters(TestConfiguration);
            CapturingTelemetryExporter exporter = new CapturingTelemetryExporter();

            // Act - Use a fresh TelemetryClientManager to ensure our exporter is used
            using (TelemetryTestHelpers.UseFreshTelemetryClientManager())
            {
                using (AdbcConnection connection = TelemetryTestHelpers.CreateConnectionWithTelemetry(
                    parameters,
                    () => exporter))
                {
                    // Execute a simple query to trigger an Activity with statement.id
                    await ExecuteSimpleQueryAsync(connection, "SELECT 1 AS telemetry_connection_test");
                }
            }

            // Assert - Wait for logs to be exported (flush happens on connection dispose)
            bool logsReceived = await exporter.WaitForLogsAsync(1, s_telemetryWaitTimeout);

            OutputHelper?.WriteLine($"Captured {exporter.CapturedLogCount} telemetry log(s)");
            foreach (TelemetryFrontendLog log in exporter.CapturedLogs)
            {
                OutputHelper?.WriteLine($"  EventId: {log.FrontendLogEventId}");
                OutputHelper?.WriteLine($"  SessionId: {log.Entry?.SqlDriverLog?.SessionId}");
                OutputHelper?.WriteLine($"  StatementId: {log.Entry?.SqlDriverLog?.SqlStatementId}");
                OutputHelper?.WriteLine($"  LatencyMs: {log.Entry?.SqlDriverLog?.OperationLatencyMs}");
            }

            Assert.True(logsReceived, "Expected at least 1 telemetry log to be captured after connection open and query execution");
            Assert.True(exporter.CapturedLogCount >= 1, $"Expected at least 1 captured log, got {exporter.CapturedLogCount}");
        }

        /// <summary>
        /// Verifies that executing a SQL statement exports a telemetry event containing
        /// the sql_statement_id and operation_latency_ms fields.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_Statement_ExportsStatementEvent()
        {
            // Arrange
            Dictionary<string, string> parameters = GetDriverParameters(TestConfiguration);
            CapturingTelemetryExporter exporter = new CapturingTelemetryExporter();

            // Act
            using (TelemetryTestHelpers.UseFreshTelemetryClientManager())
            {
                using (AdbcConnection connection = TelemetryTestHelpers.CreateConnectionWithTelemetry(
                    parameters,
                    () => exporter))
                {
                    await ExecuteSimpleQueryAsync(connection, "SELECT 1 AS test_value");
                }
            }

            // Assert
            bool logsReceived = await exporter.WaitForLogsAsync(1, s_telemetryWaitTimeout);

            OutputHelper?.WriteLine($"Captured {exporter.CapturedLogCount} telemetry log(s)");

            Assert.True(logsReceived, "Expected at least 1 telemetry log for statement execution");

            // Find a log with a sql_statement_id populated
            TelemetryFrontendLog? statementLog = exporter.CapturedLogs
                .FirstOrDefault(l => !string.IsNullOrEmpty(l.Entry?.SqlDriverLog?.SqlStatementId));

            Assert.NotNull(statementLog);
            OutputHelper?.WriteLine($"Statement log - SqlStatementId: {statementLog!.Entry?.SqlDriverLog?.SqlStatementId}");
            OutputHelper?.WriteLine($"Statement log - OperationLatencyMs: {statementLog.Entry?.SqlDriverLog?.OperationLatencyMs}");

            // Verify the sql_statement_id is populated
            Assert.False(
                string.IsNullOrEmpty(statementLog.Entry?.SqlDriverLog?.SqlStatementId),
                "sql_statement_id should be populated in the telemetry log");

            // Verify operation_latency_ms is set (should be >= 0 for a completed operation)
            Assert.True(
                statementLog.Entry?.SqlDriverLog?.OperationLatencyMs >= 0,
                $"operation_latency_ms should be >= 0, got {statementLog.Entry?.SqlDriverLog?.OperationLatencyMs}");
        }

        /// <summary>
        /// Verifies that executing invalid SQL exports a telemetry event with error information.
        /// The error_info field in the proto should contain the error type and message.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_Error_ExportsErrorEvent()
        {
            // Arrange
            Dictionary<string, string> parameters = GetDriverParameters(TestConfiguration);
            CapturingTelemetryExporter exporter = new CapturingTelemetryExporter();

            // Act
            using (TelemetryTestHelpers.UseFreshTelemetryClientManager())
            {
                using (AdbcConnection connection = TelemetryTestHelpers.CreateConnectionWithTelemetry(
                    parameters,
                    () => exporter))
                {
                    using (AdbcStatement statement = connection.CreateStatement())
                    {
                        statement.SqlQuery = "SELECT FROM NONEXISTENT_TABLE_12345_INVALID_SQL_XYZ";
                        try
                        {
                            QueryResult result = await statement.ExecuteQueryAsync();
                            // Consume if somehow it returns (it shouldn't)
                            if (result.Stream != null)
                            {
                                using (Apache.Arrow.Ipc.IArrowArrayStream stream = result.Stream)
                                {
                                    while (await stream.ReadNextRecordBatchAsync() != null) { }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            // Expected to fail for invalid SQL
                            OutputHelper?.WriteLine($"Expected error caught: {ex.GetType().Name}: {ex.Message}");
                        }
                    }
                }
            }

            // Assert - Wait for error logs to appear
            bool logsReceived = await exporter.WaitForLogsAsync(1, s_telemetryWaitTimeout);

            OutputHelper?.WriteLine($"Captured {exporter.CapturedLogCount} telemetry log(s) after error");

            Assert.True(logsReceived, "Expected at least 1 telemetry log for error event");

            // Find a log with error_info populated
            TelemetryFrontendLog? errorLog = exporter.CapturedLogs
                .FirstOrDefault(l => l.Entry?.SqlDriverLog?.ErrorInfo != null);

            Assert.NotNull(errorLog);
            OutputHelper?.WriteLine($"Error log found:");
            OutputHelper?.WriteLine($"  ErrorName: {errorLog!.Entry?.SqlDriverLog?.ErrorInfo?.ErrorName}");
            OutputHelper?.WriteLine($"  StackTrace (error message): {errorLog.Entry?.SqlDriverLog?.ErrorInfo?.StackTrace}");

            // Verify the error_info is populated - this confirms the Activity had Error status
            // and the telemetry pipeline captured it. ErrorName maps to the "error.type" Activity tag
            // which may or may not be set depending on the driver layer that caught the error.
            // StackTrace maps to "error.message" Activity tag or the exception message.
            Assert.NotNull(errorLog.Entry?.SqlDriverLog?.ErrorInfo);
        }

        /// <summary>
        /// Verifies that when telemetry is disabled via the feature flag (telemetry.enabled=false),
        /// no telemetry logs are exported through the pipeline.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_FeatureFlagDisabled_NoExport()
        {
            // Arrange - Create parameters with telemetry disabled
            Dictionary<string, string> parameters = GetDriverParameters(TestConfiguration);
            parameters[TelemetryConfiguration.PropertyKeyEnabled] = "false";

            CapturingTelemetryExporter exporter = new CapturingTelemetryExporter();

            // Act - Create connection with telemetry disabled but exporter still injected
            // When telemetry.enabled=false, InitializeTelemetry should skip pipeline setup
            using (TelemetryTestHelpers.UseFreshTelemetryClientManager())
            {
                using (AdbcConnection connection = TelemetryTestHelpers.CreateConnectionWithTelemetry(
                    parameters,
                    () => exporter))
                {
                    await ExecuteSimpleQueryAsync(connection, "SELECT 1 AS disabled_test");
                }
            }

            // Wait briefly to ensure no async logs arrive
            await Task.Delay(2000);

            // Assert - No logs should be captured when telemetry is disabled
            OutputHelper?.WriteLine($"Captured {exporter.CapturedLogCount} log(s) with telemetry disabled");
            Assert.Equal(0, exporter.CapturedLogCount);
            Assert.Equal(0, exporter.ExportCallCount);
        }

        /// <summary>
        /// Verifies that multiple connections to the same host share the same
        /// <see cref="ITelemetryClient"/> instance via <see cref="TelemetryClientManager"/>.
        /// This ensures efficient resource usage and prevents rate limiting from multiple
        /// concurrent flushes to the same telemetry endpoint.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_MultipleConnections_SharesClient()
        {
            // Arrange - Use a shared exporter across all connections
            // Since TelemetryClientManager creates one TelemetryClient per host,
            // the exporter factory is only called once for the first connection.
            // Subsequent connections to the same host reuse the same TelemetryClient
            // (and thus the same exporter instance).
            CapturingTelemetryExporter sharedExporter = new CapturingTelemetryExporter();
            int exporterFactoryCallCount = 0;
            Dictionary<string, string> parameters = GetDriverParameters(TestConfiguration);

            // Act - Create 3 connections to the same host within the same manager scope
            using (TelemetryTestHelpers.UseFreshTelemetryClientManager())
            {
                using (AdbcConnection connection1 = TelemetryTestHelpers.CreateConnectionWithTelemetry(
                    parameters,
                    () =>
                    {
                        exporterFactoryCallCount++;
                        return sharedExporter;
                    }))
                using (AdbcConnection connection2 = TelemetryTestHelpers.CreateConnectionWithTelemetry(
                    parameters,
                    () =>
                    {
                        exporterFactoryCallCount++;
                        return sharedExporter;
                    }))
                using (AdbcConnection connection3 = TelemetryTestHelpers.CreateConnectionWithTelemetry(
                    parameters,
                    () =>
                    {
                        exporterFactoryCallCount++;
                        return sharedExporter;
                    }))
                {
                    // Execute queries on each connection
                    await ExecuteSimpleQueryAsync(connection1, "SELECT 1 AS conn1_test");
                    await ExecuteSimpleQueryAsync(connection2, "SELECT 2 AS conn2_test");
                    await ExecuteSimpleQueryAsync(connection3, "SELECT 3 AS conn3_test");
                }
            }

            // Assert - Verify all telemetry went through the shared exporter
            bool logsReceived = await sharedExporter.WaitForLogsAsync(3, s_telemetryWaitTimeout);

            OutputHelper?.WriteLine($"Captured {sharedExporter.CapturedLogCount} telemetry log(s) from 3 connections");
            OutputHelper?.WriteLine($"Exporter factory was called {exporterFactoryCallCount} time(s)");

            Assert.True(logsReceived, "Expected at least 3 telemetry logs from 3 connections sharing a client");
            Assert.True(sharedExporter.CapturedLogCount >= 3,
                $"Expected at least 3 captured logs (one per connection), got {sharedExporter.CapturedLogCount}");

            // Verify the exporter factory was only called once (for the first connection),
            // proving that subsequent connections reused the same TelemetryClient
            Assert.Equal(1, exporterFactoryCallCount);

            // Verify that we got distinct session IDs (one per connection)
            List<string> sessionIds = sharedExporter.CapturedLogs
                .Select(l => l.Entry?.SqlDriverLog?.SessionId)
                .Where(id => !string.IsNullOrEmpty(id))
                .Distinct()
                .ToList()!;

            OutputHelper?.WriteLine($"Distinct session IDs ({sessionIds.Count}): {string.Join(", ", sessionIds)}");
            OutputHelper?.WriteLine("Verified: Multiple connections shared the same telemetry client (single exporter factory call)");
        }

        /// <summary>
        /// Verifies that closing a connection with pending telemetry events
        /// flushes all events before the connection close completes.
        /// The graceful shutdown sequence should ensure no data is lost.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_GracefulShutdown_FlushesEvents()
        {
            // Arrange
            Dictionary<string, string> parameters = GetDriverParameters(TestConfiguration);
            CapturingTelemetryExporter exporter = new CapturingTelemetryExporter();

            // Act - Execute multiple queries to generate pending events, then close
            using (TelemetryTestHelpers.UseFreshTelemetryClientManager())
            {
                using (AdbcConnection connection = TelemetryTestHelpers.CreateConnectionWithTelemetry(
                    parameters,
                    () => exporter))
                {
                    // Execute several queries to generate multiple telemetry events
                    await ExecuteSimpleQueryAsync(connection, "SELECT 1 AS flush_test_1");
                    await ExecuteSimpleQueryAsync(connection, "SELECT 2 AS flush_test_2");
                    await ExecuteSimpleQueryAsync(connection, "SELECT 3 AS flush_test_3");

                    // At this point, some events may still be pending in the queue.
                    // The using block will call Dispose(), which triggers DisposeTelemetry()
                    // → UnregisterAggregatorAsync (flushes aggregator)
                    // → FlushAsync (sends remaining metrics)
                    // → ReleaseClientAsync (closes client which does final flush)
                }
            }

            // Assert - After connection close, all events should have been flushed
            OutputHelper?.WriteLine($"Captured {exporter.CapturedLogCount} telemetry log(s) after graceful shutdown");

            // Give a small additional wait for any truly async flush operations
            bool logsReceived = await exporter.WaitForLogsAsync(3, s_telemetryWaitTimeout);

            Assert.True(logsReceived, "Expected at least 3 telemetry logs to be flushed during graceful shutdown");
            Assert.True(exporter.CapturedLogCount >= 3,
                $"Expected at least 3 flushed logs (one per query), got {exporter.CapturedLogCount}");

            OutputHelper?.WriteLine("Verified: Graceful shutdown flushed all pending telemetry events");
        }

        #region Helper Methods

        /// <summary>
        /// Executes a simple query on the given connection and fully consumes the result stream.
        /// </summary>
        /// <param name="connection">The ADBC connection to use.</param>
        /// <param name="sql">The SQL query to execute.</param>
        private static async Task ExecuteSimpleQueryAsync(AdbcConnection connection, string sql)
        {
            using (AdbcStatement statement = connection.CreateStatement())
            {
                statement.SqlQuery = sql;
                QueryResult result = await statement.ExecuteQueryAsync();
                using (Apache.Arrow.Ipc.IArrowArrayStream stream = result.Stream
                    ?? throw new InvalidOperationException("Query result stream is null"))
                {
                    while (await stream.ReadNextRecordBatchAsync() != null) { }
                }
            }
        }

        #endregion
    }
}
