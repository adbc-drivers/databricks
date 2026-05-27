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
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.StatementExecution
{
    /// <summary>
    /// E2E tests proving that <c>adbc.databricks.cloudfetch.enabled</c> and
    /// <c>adbc.databricks.cloudfetch.lz4.enabled</c> are honored on the SEA path
    /// (PECO-3056). These are connection-level params on JDBC; the SEA driver
    /// previously ignored them, so the disposition / compression flowing into
    /// <see cref="ExecuteStatementRequest"/> matched only the explicit SEA-only
    /// params (<c>result_disposition</c> / <c>result_compression</c>).
    ///
    /// The "happy-path" tests assert on the request that the driver actually
    /// built — exposed via the internal
    /// <see cref="StatementExecutionStatement.LastExecuteRequest"/> test seam —
    /// rather than on observable server-side behavior. This lets the test verify
    /// wire-level intent cheaply (one SELECT 1 round-trip) without depending on
    /// data-volume heuristics inside the warehouse.
    ///
    /// One test covers the <c>cloudfetch.enabled=false</c> case where the C#
    /// SEA driver cannot yet honor the param (the SEA server requires
    /// <c>format=JSON_ARRAY</c> for pure <c>INLINE</c> disposition, and the C#
    /// driver only ships an ARROW reader). The driver surfaces this as a clear
    /// <see cref="AdbcException"/> at connect time rather than silently ignoring
    /// the param (the pre-PECO-3056 behavior) or letting the server reject the
    /// first execute with an opaque 400.
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

        /// <summary>
        /// With <c>cloudfetch.enabled=true</c> (default) and no explicit
        /// <c>result_disposition</c>, the SEA driver continues to use the
        /// existing default of <c>INLINE_OR_EXTERNAL_LINKS</c>. Locks in the
        /// no-regression contract for the default path.
        /// </summary>
        [SkippableFact]
        public void CloudFetchEnabledTrue_KeepsDefaultDisposition()
        {
            SkipIfNotConfigured();

            // No extra params — exercise the default path.
            using var connection = CreateRestConnection(new Dictionary<string, string>());
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 AS value";
            var result = statement.ExecuteQuery();
            using (var reader = result.Stream)
            {
                while (reader != null && reader.ReadNextRecordBatchAsync().Result != null) { }
            }

            var seaStmt = Assert.IsType<StatementExecutionStatement>(statement);
            Assert.NotNull(seaStmt.LastExecuteRequest);
            Assert.Equal("INLINE_OR_EXTERNAL_LINKS", seaStmt.LastExecuteRequest!.Disposition);
        }

        /// <summary>
        /// With <c>cloudfetch.lz4.enabled=false</c> the SEA driver must clear
        /// <c>result_compression</c> on the wire (null / unset → server treats
        /// as no compression). Mirrors JDBC's <c>CompressionCodec.NONE</c> branch
        /// when LZ4 is disabled.
        /// </summary>
        [SkippableFact]
        public void Lz4EnabledFalse_ClearsResultCompression()
        {
            SkipIfNotConfigured();

            var extras = new Dictionary<string, string>
            {
                [DatabricksParameters.CanDecompressLz4] = "false",
                // Set an explicit result_compression to prove the lz4 param overrides it.
                [DatabricksParameters.ResultCompression] = "LZ4_FRAME",
            };

            using var connection = CreateRestConnection(extras);
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 AS value";
            var result = statement.ExecuteQuery();
            using (var reader = result.Stream)
            {
                while (reader != null && reader.ReadNextRecordBatchAsync().Result != null) { }
            }

            var seaStmt = Assert.IsType<StatementExecutionStatement>(statement);
            Assert.NotNull(seaStmt.LastExecuteRequest);
            Assert.Null(seaStmt.LastExecuteRequest!.ResultCompression);
        }

        /// <summary>
        /// With <c>cloudfetch.lz4.enabled=true</c> (default) and an explicit
        /// <c>result_compression</c>, the SEA driver must pass that compression
        /// through to the wire unchanged. Locks in the no-regression contract
        /// for the explicit compression path.
        /// </summary>
        [SkippableFact]
        public void Lz4EnabledTrue_PassesThroughExplicitResultCompression()
        {
            SkipIfNotConfigured();

            var extras = new Dictionary<string, string>
            {
                [DatabricksParameters.ResultCompression] = "LZ4_FRAME",
            };

            using var connection = CreateRestConnection(extras);
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 AS value";
            var result = statement.ExecuteQuery();
            using (var reader = result.Stream)
            {
                while (reader != null && reader.ReadNextRecordBatchAsync().Result != null) { }
            }

            var seaStmt = Assert.IsType<StatementExecutionStatement>(statement);
            Assert.NotNull(seaStmt.LastExecuteRequest);
            Assert.Equal("LZ4_FRAME", seaStmt.LastExecuteRequest!.ResultCompression);
        }
    }
}
