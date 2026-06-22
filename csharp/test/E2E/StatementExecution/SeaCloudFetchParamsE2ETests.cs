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

namespace AdbcDrivers.Databricks.Tests.E2E.StatementExecution
{
    /// <summary>
    /// E2E tests proving that <c>adbc.databricks.cloudfetch.lz4.enabled</c> is
    /// honored on the SEA path (PECO-3056).
    ///
    /// Tests assert on the <c>result_compression</c> field of the request the
    /// driver actually built, exposed via the internal
    /// <see cref="StatementExecutionStatement.LastExecuteRequest"/> test seam.
    /// A behavioral "query succeeds" test is not a substitute: a query can succeed
    /// regardless of what compression value was sent, so the seam is the only way
    /// to verify the driver's intent without full HTTP interception.
    ///
    /// Note: <c>cloudfetch.enabled=false</c> is not yet honored on SEA (requires
    /// server-side support for ARROW_STREAM with INLINE disposition) and is
    /// intentionally left as a silent no-op.
    /// </summary>
    public class SeaCloudFetchParamsE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public SeaCloudFetchParamsE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        private void SkipIfNotConfigured()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable), "Test configuration not available");
        }

        private AdbcConnection CreateRestConnection(Dictionary<string, string> extraProperties)
        {
            var properties = new Dictionary<string, string>
            {
                [DatabricksParameters.Protocol] = "rest",
            };

            if (!string.IsNullOrEmpty(TestConfiguration.Uri))
            {
                properties[AdbcOptions.Uri] = TestConfiguration.Uri;
            }
            else
            {
                if (!string.IsNullOrEmpty(TestConfiguration.HostName))
                    properties[SparkParameters.HostName] = TestConfiguration.HostName;
                if (!string.IsNullOrEmpty(TestConfiguration.Path))
                    properties[SparkParameters.Path] = TestConfiguration.Path;
            }

            if (!string.IsNullOrEmpty(TestConfiguration.Token))
                properties[SparkParameters.Token] = TestConfiguration.Token;
            if (!string.IsNullOrEmpty(TestConfiguration.AccessToken))
                properties[SparkParameters.AccessToken] = TestConfiguration.AccessToken;

            foreach (var kvp in extraProperties)
                properties[kvp.Key] = kvp.Value;

            var driver = new DatabricksDriver();
            var database = driver.Open(properties);
            return database.Connect(null);
        }

        private static void DrainStream(QueryResult result)
        {
            using var stream = result.Stream;
            while (stream != null && stream.ReadNextRecordBatchAsync().Result != null) { }
        }

        /// <summary>
        /// With no explicit params the driver uses the default disposition
        /// <c>INLINE_OR_EXTERNAL_LINKS</c>. Locks in the no-regression contract
        /// for the default path.
        /// </summary>
        [SkippableFact]
        public void DefaultConfig_UsesDefaultDisposition()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection(new Dictionary<string, string>());
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 AS value";
            DrainStream(statement.ExecuteQuery());

            var seaStmt = Assert.IsType<StatementExecutionStatement>(statement);
            Assert.NotNull(seaStmt.LastExecuteRequest);
            Assert.Equal("INLINE_OR_EXTERNAL_LINKS", seaStmt.LastExecuteRequest!.Disposition);
        }

        /// <summary>
        /// With <c>cloudfetch.lz4.enabled=false</c> the driver must clear
        /// <c>result_compression</c> on the wire (null / unset).
        /// </summary>
        [SkippableFact]
        public void Lz4EnabledFalse_ClearsResultCompression()
        {
            SkipIfNotConfigured();

            var extras = new Dictionary<string, string>
            {
                [DatabricksParameters.CanDecompressLz4] = "false",
            };

            using var connection = CreateRestConnection(extras);
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 AS value";
            DrainStream(statement.ExecuteQuery());

            var seaStmt = Assert.IsType<StatementExecutionStatement>(statement);
            Assert.NotNull(seaStmt.LastExecuteRequest);
            Assert.Null(seaStmt.LastExecuteRequest!.ResultCompression);
        }

        /// <summary>
        /// With <c>cloudfetch.lz4.enabled=true</c> (default) the driver must
        /// request <c>LZ4_FRAME</c> compression on the wire.
        /// </summary>
        [SkippableFact]
        public void Lz4EnabledTrue_RequestsLz4Compression()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection(new Dictionary<string, string>());
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 AS value";
            DrainStream(statement.ExecuteQuery());

            var seaStmt = Assert.IsType<StatementExecutionStatement>(statement);
            Assert.NotNull(seaStmt.LastExecuteRequest);
            Assert.Equal("LZ4_FRAME", seaStmt.LastExecuteRequest!.ResultCompression);
        }
    }
}
