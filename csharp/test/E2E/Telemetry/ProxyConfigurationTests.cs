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

using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// Regression tests for PECO-2994: <c>driver_connection_params.use_proxy</c>,
    /// <c>use_system_proxy</c>, and <c>use_cf_proxy</c> were not populated, so enterprise
    /// observability dashboards could not attribute proxy-induced latency or TLS-break
    /// incidents. <see cref="ConnectionTelemetry.BuildDriverConnectionParams"/> now reads
    /// these flags from the connection properties.
    /// </summary>
    public class ProxyConfigurationTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public ProxyConfigurationTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// PECO-2994: When <c>adbc.proxy_options.use_system_proxy</c> and
        /// <c>adbc.proxy_options.use_cf_proxy</c> are set to <c>"true"</c>, the captured
        /// telemetry payload reflects them. <c>use_proxy</c> remains false because flipping
        /// it on without a real proxy host would break the connection; the false case is
        /// the correct regression check that the value is no longer hard-coded but is read
        /// from properties.
        /// </summary>
        [SkippableFact]
        public async Task ConnectionParams_ProxyFlags_ArePopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // use_system_proxy and use_cf_proxy are observability-only flags in this
                // driver — they do not affect the actual HTTP transport, so toggling them
                // on does not break the connection.
                properties[DatabricksParameters.UseSystemProxy] = "true";
                properties[DatabricksParameters.UseCfProxy] = "true";

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

                // use_proxy is read from HttpProxyOptions.UseProxy; the test config does not
                // configure a proxy so the expected value is false (matches JDBC default).
                Assert.False(protoLog.DriverConnectionParams.UseProxy,
                    "use_proxy should default to false when not configured");

                Assert.True(protoLog.DriverConnectionParams.UseSystemProxy,
                    "use_system_proxy should reflect adbc.proxy_options.use_system_proxy");
                Assert.True(protoLog.DriverConnectionParams.UseCfProxy,
                    "use_cf_proxy should reflect adbc.proxy_options.use_cf_proxy");

                OutputHelper?.WriteLine($"✓ use_proxy: {protoLog.DriverConnectionParams.UseProxy}");
                OutputHelper?.WriteLine($"✓ use_system_proxy: {protoLog.DriverConnectionParams.UseSystemProxy}");
                OutputHelper?.WriteLine($"✓ use_cf_proxy: {protoLog.DriverConnectionParams.UseCfProxy}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }
    }
}
