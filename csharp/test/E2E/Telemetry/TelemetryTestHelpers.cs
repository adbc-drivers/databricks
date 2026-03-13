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
using AdbcDrivers.Databricks.Telemetry.Proto;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// Test helper utilities for telemetry testing.
    /// Provides methods for creating connections with CapturingTelemetryExporter
    /// and helper methods for asserting on proto field values.
    /// </summary>
    internal static class TelemetryTestHelpers
    {
        /// <summary>
        /// Creates a connection with a capturing exporter for testing.
        /// The exporter override is set globally and must be cleared in a finally block.
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <returns>A tuple containing the connection and the capturing exporter.</returns>
        public static (AdbcConnection Connection, CapturingTelemetryExporter Exporter) CreateConnectionWithCapturingTelemetry(
            Dictionary<string, string> properties)
        {
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
        /// Clears the exporter override. Must be called in a finally block after using CreateConnectionWithCapturingTelemetry.
        /// </summary>
        public static void ClearExporterOverride()
        {
            TelemetryClientManager.ExporterOverride = null;
        }

        /// <summary>
        /// Waits for telemetry events to be captured and returns them.
        /// </summary>
        /// <param name="exporter">The capturing exporter.</param>
        /// <param name="expectedCount">Expected number of telemetry events.</param>
        /// <param name="timeoutMs">Timeout in milliseconds.</param>
        /// <returns>List of captured telemetry logs.</returns>
        public static async Task<List<TelemetryFrontendLog>> WaitForTelemetryEvents(
            CapturingTelemetryExporter exporter,
            int expectedCount,
            int timeoutMs = 5000)
        {
            var startTime = DateTime.UtcNow;
            while ((DateTime.UtcNow - startTime).TotalMilliseconds < timeoutMs)
            {
                if (exporter.ExportedLogs.Count >= expectedCount)
                {
                    return exporter.ExportedLogs.ToList();
                }
                await Task.Delay(100);
            }

            return exporter.ExportedLogs.ToList();
        }

        /// <summary>
        /// Extracts the OssSqlDriverTelemetryLog proto from a TelemetryFrontendLog.
        /// </summary>
        public static OssSqlDriverTelemetryLog GetProtoLog(TelemetryFrontendLog frontendLog)
        {
            Assert.NotNull(frontendLog.Entry);
            Assert.NotNull(frontendLog.Entry.SqlDriverLog);
            return frontendLog.Entry.SqlDriverLog;
        }

        /// <summary>
        /// Asserts that basic session-level fields are populated correctly.
        /// </summary>
        public static void AssertSessionFieldsPopulated(OssSqlDriverTelemetryLog protoLog)
        {
            // Session ID should be non-empty
            Assert.False(string.IsNullOrEmpty(protoLog.SessionId), "session_id should be populated");

            // System configuration should be present
            Assert.NotNull(protoLog.SystemConfiguration);
            AssertSystemConfigurationPopulated(protoLog.SystemConfiguration);

            // Driver connection params should be present
            Assert.NotNull(protoLog.DriverConnectionParams);
            AssertDriverConnectionParamsPopulated(protoLog.DriverConnectionParams);
        }

        /// <summary>
        /// Asserts that system configuration fields are populated.
        /// </summary>
        public static void AssertSystemConfigurationPopulated(DriverSystemConfiguration config)
        {
            Assert.NotNull(config);
            Assert.False(string.IsNullOrEmpty(config.DriverVersion), "driver_version should be populated");
            Assert.False(string.IsNullOrEmpty(config.DriverName), "driver_name should be populated");
            Assert.False(string.IsNullOrEmpty(config.OsName), "os_name should be populated");
            Assert.False(string.IsNullOrEmpty(config.RuntimeName), "runtime_name should be populated");
        }

        /// <summary>
        /// Asserts that driver connection parameters are populated.
        /// </summary>
        public static void AssertDriverConnectionParamsPopulated(DriverConnectionParameters params_)
        {
            Assert.NotNull(params_);
            // http_path may be empty in some configurations, so just check mode is set
            Assert.True(params_.Mode != DriverMode.Types.Type.Unspecified, "mode should not be UNSPECIFIED");
        }

        /// <summary>
        /// Asserts that statement-level fields are populated correctly.
        /// </summary>
        public static void AssertStatementFieldsPopulated(OssSqlDriverTelemetryLog protoLog)
        {
            // SQL statement ID should be non-empty for SQL operations
            Assert.False(string.IsNullOrEmpty(protoLog.SqlStatementId), "sql_statement_id should be populated");

            // Operation latency should be positive
            Assert.True(protoLog.OperationLatencyMs > 0, "operation_latency_ms should be > 0");

            // SQL operation should be present
            Assert.NotNull(protoLog.SqlOperation);
        }

        /// <summary>
        /// Asserts that SQL operation fields are populated for a query.
        /// </summary>
        public static void AssertSqlOperationPopulated(SqlExecutionEvent sqlOp, bool expectChunkDetails = false)
        {
            Assert.NotNull(sqlOp);

            // Statement type should be set
            Assert.True(sqlOp.StatementType != Statement.Types.Type.Unspecified,
                "statement_type should not be UNSPECIFIED");

            // Operation detail should be present
            Assert.NotNull(sqlOp.OperationDetail);
            Assert.True(sqlOp.OperationDetail.OperationType != Operation.Types.Type.Unspecified,
                "operation_type should not be UNSPECIFIED");

            // Result latency should be present for queries
            if (sqlOp.StatementType == Statement.Types.Type.Query)
            {
                Assert.NotNull(sqlOp.ResultLatency);
                Assert.True(sqlOp.ResultLatency.ResultSetReadyLatencyMillis >= 0,
                    "result_set_ready_latency_millis should be >= 0");
            }

            // Check chunk details if expected
            if (expectChunkDetails)
            {
                Assert.NotNull(sqlOp.ChunkDetails);
                Assert.True(sqlOp.ChunkDetails.TotalChunksPresent > 0,
                    "total_chunks_present should be > 0 for CloudFetch queries");
            }
        }

        /// <summary>
        /// Asserts that error fields are populated correctly.
        /// </summary>
        public static void AssertErrorFieldsPopulated(DriverErrorInfo errorInfo)
        {
            Assert.NotNull(errorInfo);
            Assert.False(string.IsNullOrEmpty(errorInfo.ErrorName), "error_name should be populated");
        }

        /// <summary>
        /// Finds a telemetry log by predicate in the captured logs.
        /// </summary>
        public static TelemetryFrontendLog? FindLog(
            IEnumerable<TelemetryFrontendLog> logs,
            Func<OssSqlDriverTelemetryLog, bool> predicate)
        {
            return logs.FirstOrDefault(log =>
                log.Entry?.SqlDriverLog != null &&
                predicate(log.Entry.SqlDriverLog));
        }

        /// <summary>
        /// Asserts that exactly the expected number of logs were captured.
        /// </summary>
        public static void AssertLogCount(IReadOnlyCollection<TelemetryFrontendLog> logs, int expectedCount)
        {
            Assert.Equal(expectedCount, logs.Count);
        }
    }
}
