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
using System.Diagnostics;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for DriverSystemConfiguration fields in telemetry.
    /// Tests the missing fields: runtime_vendor and client_app_name.
    /// </summary>
    public class SystemConfigurationTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public SystemConfigurationTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Tests that runtime_vendor is set to 'Microsoft' for .NET runtime.
        /// </summary>
        [SkippableFact]
        public async Task SystemConfig_RuntimeVendor_IsMicrosoft()
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

                // Assert runtime_vendor is set to "Microsoft"
                Assert.NotNull(protoLog.SystemConfiguration);
                Assert.Equal("Microsoft", protoLog.SystemConfiguration.RuntimeVendor);

                OutputHelper?.WriteLine($"✓ runtime_vendor: {protoLog.SystemConfiguration.RuntimeVendor}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that client_app_name is always set to the process name.
        /// </summary>
        [SkippableFact]
        public async Task SystemConfig_ClientAppName_IsProcessName()
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

                // Assert client_app_name is set to the current process name
                Assert.NotNull(protoLog.SystemConfiguration);
                Assert.False(string.IsNullOrEmpty(protoLog.SystemConfiguration.ClientAppName),
                    "client_app_name should be populated with process name when property not set");

                // Verify it matches the actual process name
                string expectedProcessName = Process.GetCurrentProcess().ProcessName;
                Assert.Equal(expectedProcessName, protoLog.SystemConfiguration.ClientAppName);

                OutputHelper?.WriteLine($"✓ client_app_name defaulted to process name: {protoLog.SystemConfiguration.ClientAppName}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that all 12 DriverSystemConfiguration fields are populated (comprehensive check).
        /// This ensures runtime_vendor and client_app_name are included alongside existing fields.
        /// </summary>
        [SkippableFact]
        public async Task SystemConfig_AllTwelveFields_ArePopulated()
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
                var config = protoLog.SystemConfiguration;

                // Assert all 12 fields are populated
                Assert.NotNull(config);
                Assert.False(string.IsNullOrEmpty(config.DriverVersion), "driver_version should be populated");
                Assert.False(string.IsNullOrEmpty(config.RuntimeName), "runtime_name should be populated");
                Assert.False(string.IsNullOrEmpty(config.RuntimeVersion), "runtime_version should be populated");
                Assert.False(string.IsNullOrEmpty(config.RuntimeVendor), "runtime_vendor should be populated");
                Assert.False(string.IsNullOrEmpty(config.OsName), "os_name should be populated");
                Assert.False(string.IsNullOrEmpty(config.OsVersion), "os_version should be populated");
                Assert.False(string.IsNullOrEmpty(config.OsArch), "os_arch should be populated");
                Assert.False(string.IsNullOrEmpty(config.DriverName), "driver_name should be populated");
                Assert.False(string.IsNullOrEmpty(config.ClientAppName), "client_app_name should be populated");
                Assert.NotNull(config.LocaleName); // locale_name can be empty string in some environments, but should not be null
                Assert.NotNull(config.CharSetEncoding); // char_set_encoding can be empty in some environments, but should not be null
                Assert.False(string.IsNullOrEmpty(config.ProcessName), "process_name should be populated");

                OutputHelper?.WriteLine("✓ All 12 DriverSystemConfiguration fields populated:");
                OutputHelper?.WriteLine($"  1. driver_version: {config.DriverVersion}");
                OutputHelper?.WriteLine($"  2. runtime_name: {config.RuntimeName}");
                OutputHelper?.WriteLine($"  3. runtime_version: {config.RuntimeVersion}");
                OutputHelper?.WriteLine($"  4. runtime_vendor: {config.RuntimeVendor}");
                OutputHelper?.WriteLine($"  5. os_name: {config.OsName}");
                OutputHelper?.WriteLine($"  6. os_version: {config.OsVersion}");
                OutputHelper?.WriteLine($"  7. os_arch: {config.OsArch}");
                OutputHelper?.WriteLine($"  8. driver_name: {config.DriverName}");
                OutputHelper?.WriteLine($"  9. client_app_name: {config.ClientAppName}");
                OutputHelper?.WriteLine($" 10. locale_name: {config.LocaleName}");
                OutputHelper?.WriteLine($" 11. char_set_encoding: {config.CharSetEncoding}");
                OutputHelper?.WriteLine($" 12. process_name: {config.ProcessName}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }
    }
}
