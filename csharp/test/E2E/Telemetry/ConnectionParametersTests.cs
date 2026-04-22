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
using AdbcDrivers.HiveServer2.Spark;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.HiveServer2;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for DriverConnectionParameters extended fields in telemetry.
    /// Tests the additional fields: enable_arrow, rows_fetched_per_block, socket_timeout,
    /// enable_direct_results, enable_complex_datatype_support, auto_commit.
    /// </summary>
    public class ConnectionParametersTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public ConnectionParametersTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Regression test for PECO-2981: driver_connection_params.http_path was empty for
        /// 100% of ADBC rows because <c>BuildDriverConnectionParams</c> looked up the wrong
        /// property key ("adbc.spark.http_path") instead of <see cref="SparkParameters.Path"/>.
        /// </summary>
        [SkippableFact]
        public async Task ConnectionParams_HttpPath_IsPopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Force use of SparkParameters.Path to match what's emitted into telemetry.
                // The test environment may supply the path via AdbcOptions.Uri alone; in that
                // case, extract the path so we have a deterministic expected value.
                if (!properties.TryGetValue(SparkParameters.Path, out string? expectedHttpPath)
                    || string.IsNullOrEmpty(expectedHttpPath))
                {
                    Assert.True(
                        properties.TryGetValue(AdbcOptions.Uri, out string? uri) && !string.IsNullOrEmpty(uri),
                        $"Test configuration must set {SparkParameters.Path} or {AdbcOptions.Uri}");
                    expectedHttpPath = new Uri(uri!).AbsolutePath;
                    properties[SparkParameters.Path] = expectedHttpPath;
                }

                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS test_value";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                statement.Dispose();

                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.DriverConnectionParams);
                Assert.False(string.IsNullOrEmpty(protoLog.DriverConnectionParams.HttpPath),
                    "http_path should be populated (regression for PECO-2981)");
                Assert.Equal(expectedHttpPath, protoLog.DriverConnectionParams.HttpPath);

                OutputHelper?.WriteLine($"✓ http_path: {protoLog.DriverConnectionParams.HttpPath}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Regression test for PECO-2982: driver_connection_params.host_info.port was hard-coded
        /// to 0 for 100% of ADBC rows. The port should be resolved from <see cref="SparkParameters.Port"/>,
        /// then <see cref="AdbcOptions.Uri"/>, defaulting to 443.
        /// </summary>
        [SkippableFact]
        public async Task ConnectionParams_HostInfoPort_IsPopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Determine the expected port using the same precedence as the driver.
                int expectedPort = 443;
                if (properties.TryGetValue(SparkParameters.Port, out string? portStr)
                    && int.TryParse(portStr, out int configuredPort) && configuredPort > 0)
                {
                    expectedPort = configuredPort;
                }
                else if (properties.TryGetValue(AdbcOptions.Uri, out string? uri)
                    && !string.IsNullOrEmpty(uri)
                    && Uri.TryCreate(uri, UriKind.Absolute, out Uri? parsedUri)
                    && parsedUri.Port > 0)
                {
                    expectedPort = parsedUri.Port;
                }

                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS test_value";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                statement.Dispose();

                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.DriverConnectionParams);
                Assert.NotNull(protoLog.DriverConnectionParams.HostInfo);
                Assert.NotEqual(0, protoLog.DriverConnectionParams.HostInfo.Port);
                Assert.Equal(expectedPort, protoLog.DriverConnectionParams.HostInfo.Port);

                OutputHelper?.WriteLine($"✓ host_info.port: {protoLog.DriverConnectionParams.HostInfo.Port}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that enable_arrow is set to true for ADBC driver.
        /// </summary>
        [SkippableFact]
        public async Task ConnectionParams_EnableArrow_IsTrue()
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

                // Assert enable_arrow is true
                Assert.NotNull(protoLog.DriverConnectionParams);
                Assert.True(protoLog.DriverConnectionParams.EnableArrow,
                    "enable_arrow should be true for ADBC driver");

                OutputHelper?.WriteLine($"✓ enable_arrow: {protoLog.DriverConnectionParams.EnableArrow}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that rows_fetched_per_block is populated from batch size configuration.
        /// </summary>
        [SkippableFact]
        public async Task ConnectionParams_RowsFetchedPerBlock_MatchesBatchSize()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Set custom batch size
                int customBatchSize = 5000;
                properties[ApacheParameters.BatchSize] = customBatchSize.ToString();

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

                // Assert rows_fetched_per_block matches batch size
                Assert.NotNull(protoLog.DriverConnectionParams);
                Assert.Equal(customBatchSize, protoLog.DriverConnectionParams.RowsFetchedPerBlock);

                OutputHelper?.WriteLine($"✓ rows_fetched_per_block: {protoLog.DriverConnectionParams.RowsFetchedPerBlock}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that socket_timeout is populated from connection properties (converted from ms to seconds).
        /// </summary>
        [SkippableFact]
        public async Task ConnectionParams_SocketTimeout_IsPopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Set custom timeout (in milliseconds)
                int customTimeoutMs = 120000; // 120 seconds
                properties[SparkParameters.ConnectTimeoutMilliseconds] = customTimeoutMs.ToString();

                // Disable TemporarilyUnavailableRetry so it doesn't override ConnectTimeoutMilliseconds
                // (default retry timeout of 900s would bump the connect timeout above our custom value)
                properties[DatabricksParameters.TemporarilyUnavailableRetry] = "false";

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

                // Assert socket_timeout is populated and converted to seconds
                Assert.NotNull(protoLog.DriverConnectionParams);
                Assert.Equal(customTimeoutMs / 1000, protoLog.DriverConnectionParams.SocketTimeout);

                OutputHelper?.WriteLine($"✓ socket_timeout: {protoLog.DriverConnectionParams.SocketTimeout}s (from {customTimeoutMs}ms)");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that enable_direct_results is populated from connection configuration.
        /// </summary>
        [SkippableFact]
        public async Task ConnectionParams_EnableDirectResults_IsPopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Set enable_direct_results to false (default is true)
                properties[DatabricksParameters.EnableDirectResults] = "false";

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

                // Assert enable_direct_results matches configuration
                Assert.NotNull(protoLog.DriverConnectionParams);
                Assert.False(protoLog.DriverConnectionParams.EnableDirectResults,
                    "enable_direct_results should match connection configuration");

                OutputHelper?.WriteLine($"✓ enable_direct_results: {protoLog.DriverConnectionParams.EnableDirectResults}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that enable_complex_datatype_support is populated from connection properties.
        /// </summary>
        [SkippableFact]
        public async Task ConnectionParams_EnableComplexDatatypeSupport_IsPopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Enable complex datatype support explicitly
                properties[DatabricksParameters.UseDescTableExtended] = "true";

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

                // Assert enable_complex_datatype_support is populated
                Assert.NotNull(protoLog.DriverConnectionParams);
                Assert.True(protoLog.DriverConnectionParams.EnableComplexDatatypeSupport,
                    "enable_complex_datatype_support should match UseDescTableExtended config");

                OutputHelper?.WriteLine($"✓ enable_complex_datatype_support: {protoLog.DriverConnectionParams.EnableComplexDatatypeSupport}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that auto_commit is populated from connection properties.
        /// </summary>
        [SkippableFact]
        public async Task ConnectionParams_AutoCommit_IsPopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // In ADBC, auto_commit is always true (implicit commits)
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

                // Assert auto_commit is true (ADBC default)
                Assert.NotNull(protoLog.DriverConnectionParams);
                Assert.True(protoLog.DriverConnectionParams.AutoCommit,
                    "auto_commit should be true for ADBC driver");

                OutputHelper?.WriteLine($"✓ auto_commit: {protoLog.DriverConnectionParams.AutoCommit}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that all extended connection parameter fields are non-default (comprehensive check).
        /// This ensures enable_arrow, rows_fetched_per_block, socket_timeout,
        /// enable_direct_results, enable_complex_datatype_support, and auto_commit are all populated.
        /// </summary>
        [SkippableFact]
        public async Task ConnectionParams_AllExtendedFields_ArePopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Set explicit values for all configurable fields
                properties[ApacheParameters.BatchSize] = "10000";
                properties[SparkParameters.ConnectTimeoutMilliseconds] = "90000";
                properties[DatabricksParameters.EnableDirectResults] = "true";
                properties[DatabricksParameters.UseDescTableExtended] = "true";

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
                var connParams = protoLog.DriverConnectionParams;

                // Assert all extended fields are populated
                Assert.NotNull(connParams);
                Assert.True(connParams.EnableArrow, "enable_arrow should be true");
                Assert.True(connParams.RowsFetchedPerBlock > 0, "rows_fetched_per_block should be > 0");
                Assert.True(connParams.SocketTimeout > 0, "socket_timeout should be > 0");
                Assert.True(connParams.EnableDirectResults, "enable_direct_results should be populated");
                Assert.True(connParams.EnableComplexDatatypeSupport, "enable_complex_datatype_support should be populated");
                Assert.True(connParams.AutoCommit, "auto_commit should be true");

                OutputHelper?.WriteLine("✓ All extended DriverConnectionParameters fields populated:");
                OutputHelper?.WriteLine($"  - enable_arrow: {connParams.EnableArrow}");
                OutputHelper?.WriteLine($"  - rows_fetched_per_block: {connParams.RowsFetchedPerBlock}");
                OutputHelper?.WriteLine($"  - socket_timeout: {connParams.SocketTimeout}");
                OutputHelper?.WriteLine($"  - enable_direct_results: {connParams.EnableDirectResults}");
                OutputHelper?.WriteLine($"  - enable_complex_datatype_support: {connParams.EnableComplexDatatypeSupport}");
                OutputHelper?.WriteLine($"  - auto_commit: {connParams.AutoCommit}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }
    }
}
