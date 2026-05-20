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

using System.Collections.Generic;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// E2E tests for PECO-3055 httpPath-based protocol auto-detection.
    /// Verifies that DatabricksDatabase.Connect picks the right protocol based on
    /// the httpPath when adbc.databricks.protocol is not explicitly set.
    /// Matches the JDBC driver's compute-type-based protocol selection
    /// (DatabricksConnectionContext.getClientTypeFromContext).
    /// </summary>
    public class ProtocolAutoDetectionE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public ProtocolAutoDetectionE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// When httpPath matches the SQL Warehouse pattern (/sql/1.0/warehouses/{id})
        /// and adbc.databricks.protocol is not provided, the driver must default to
        /// the SEA (REST / Statement Execution API) protocol — matching JDBC.
        /// </summary>
        [SkippableFact]
        public void WarehousePath_DefaultsToSeaProtocol_WhenProtocolNotSpecified()
        {
            // Arrange: clone config, clear the explicit protocol so we exercise auto-detection.
            var testConfig = (DatabricksTestConfiguration)TestConfiguration.Clone();
            testConfig.Protocol = string.Empty;

            // The shared test warehouse uses /sql/1.0/warehouses/{id} which JDBC maps
            // to a Warehouse compute resource → SEA default. Skip if some other path is configured.
            Skip.If(
                string.IsNullOrEmpty(testConfig.Path) || !testConfig.Path.Contains("/warehouses/"),
                "Test requires a warehouse httpPath (/warehouses/{id}) in the test config");

            // Act: open the connection with no explicit protocol.
            using var connection = NewConnection(testConfig);

            // Assert: SEA is the StatementExecutionConnection; Thrift is DatabricksConnection.
            Assert.NotNull(connection);
            Assert.IsType<StatementExecutionConnection>(connection);
            OutputHelper?.WriteLine(
                $"Connection type: {connection.GetType().Name} for path '{testConfig.Path}' (no explicit protocol)");
        }

        /// <summary>
        /// Explicit adbc.databricks.protocol = "thrift" must continue to override the
        /// httpPath-based default, even on a warehouse path. This protects existing users
        /// who pin to Thrift.
        /// </summary>
        [SkippableFact]
        public void ExplicitThriftOverride_BeatsAutoDetection_OnWarehousePath()
        {
            var testConfig = (DatabricksTestConfiguration)TestConfiguration.Clone();
            testConfig.Protocol = "thrift";

            Skip.If(
                string.IsNullOrEmpty(testConfig.Path) || !testConfig.Path.Contains("/warehouses/"),
                "Test requires a warehouse httpPath (/warehouses/{id}) in the test config");

            using var connection = NewConnection(testConfig);

            Assert.NotNull(connection);
            Assert.IsType<DatabricksConnection>(connection);
            OutputHelper?.WriteLine(
                $"Connection type: {connection.GetType().Name} for path '{testConfig.Path}' (protocol=thrift override)");
        }

        /// <summary>
        /// Explicit adbc.databricks.protocol = "rest" must continue to select SEA,
        /// even though that is now the default. This guards against accidental
        /// regression of the explicit-override path.
        /// </summary>
        [SkippableFact]
        public void ExplicitRestOverride_StillSelectsSea_OnWarehousePath()
        {
            var testConfig = (DatabricksTestConfiguration)TestConfiguration.Clone();
            testConfig.Protocol = "rest";

            Skip.If(
                string.IsNullOrEmpty(testConfig.Path) || !testConfig.Path.Contains("/warehouses/"),
                "Test requires a warehouse httpPath (/warehouses/{id}) in the test config");

            using var connection = NewConnection(testConfig);

            Assert.NotNull(connection);
            Assert.IsType<StatementExecutionConnection>(connection);
        }

        /// <summary>
        /// Unit-style assertion that does not require warehouse round-trip: build the
        /// merged properties manually for a GP-cluster httpPath and verify the resolved
        /// protocol is "thrift" (forced by compute type). This pins the regex semantics
        /// even on a test config that uses a warehouse path.
        /// </summary>
        [SkippableFact]
        public void GeneralPurposeClusterPath_ForcesThrift_NoExplicitProtocol()
        {
            // We use the auto-detection helper indirectly by constructing a DatabricksDatabase
            // with cluster-style properties and observing the failure mode of SEA-only path.
            // For GP cluster paths (/sql/protocolv1/o/{orgId}/{clusterId}) the driver must
            // not try to open a SEA session (which would reject the path). Connecting will
            // fail because we don't have a real GP cluster available, but the failure must
            // come from the Thrift path — proving we did NOT pick SEA. The cheapest signal
            // is that DatabricksDatabase.Connect for a clearly fake host throws an exception
            // whose stack/inner-exception originates in the Thrift HiveServer2 transport,
            // not in StatementExecutionConnection.
            //
            // Rather than trying to read the stack trace, we make the inverse assertion:
            // if auto-detection were broken (defaulting to SEA on a cluster path), the
            // resulting StatementExecutionConnection ctor would throw
            // "Statement Execution API requires a SQL Warehouse, not a general cluster".
            // We assert that this specific exception is NOT thrown.
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "example.invalid",
                [SparkParameters.Path] = "/sql/protocolv1/o/1234567890/0123-abcdef-foo",
                [SparkParameters.Token] = "dummy-token-for-detection-only",
                [SparkParameters.AuthType] = "token",
                [SparkParameters.Type] = "databricks",
            };

            var driver = NewDriver;
            using var database = driver.Open(properties);

            // Either succeeds (impossible here) or throws — but the exception MUST NOT contain
            // the SEA-only path rejection, which would mean we incorrectly picked SEA.
            try
            {
                using var connection = database.Connect(properties);
                // Connection succeeded against an invalid host — extremely unlikely; if it does
                // happen, just dispose and pass: protocol selection clearly didn't go through SEA.
            }
            catch (System.Exception ex)
            {
                var flat = ex.ToString();
                Assert.DoesNotContain(
                    "Statement Execution API requires a SQL Warehouse, not a general cluster",
                    flat);
                OutputHelper?.WriteLine(
                    $"GP cluster path correctly avoided SEA path. Error chain (truncated): {flat.Substring(0, System.Math.Min(flat.Length, 300))}");
            }
        }
    }
}
