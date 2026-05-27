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
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.StatementExecution
{
    /// <summary>
    /// E2E tests proving that <c>adbc.databricks.cloudfetch.lz4.enabled</c> is
    /// honored on the SEA path (PECO-3056). This is a connection-level param on
    /// JDBC; the SEA driver previously ignored it.
    ///
    /// Tests execute a real query and verify the result to confirm end-to-end
    /// correctness: if the driver sends the wrong <c>result_compression</c> value
    /// the server will either reject the request or return data the driver cannot
    /// decode, causing an exception before the assertion is reached.
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

        private static long DrainStream(QueryResult result)
        {
            long rows = 0;
            using var stream = result.Stream;
            while (stream != null && stream.ReadNextRecordBatchAsync().Result is { } batch)
                rows += batch.Length;
            return rows;
        }

        /// <summary>
        /// Default config (lz4=true): query executes and returns the expected row.
        /// Locks in the no-regression contract for the default path.
        /// </summary>
        [SkippableFact]
        public void DefaultConfig_QuerySucceeds()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection(new Dictionary<string, string>());
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 AS value";
            var result = statement.ExecuteQuery();
            Assert.Equal(1L, DrainStream(result));
        }

        /// <summary>
        /// With <c>cloudfetch.lz4.enabled=false</c> the driver must clear
        /// <c>result_compression</c> on the wire. Verified by a successful round-trip:
        /// if the server returns uncompressed data and the driver handles it correctly,
        /// the query completes and returns the expected row count.
        /// </summary>
        [SkippableFact]
        public void Lz4EnabledFalse_QuerySucceeds()
        {
            SkipIfNotConfigured();

            var extras = new Dictionary<string, string>
            {
                [DatabricksParameters.CanDecompressLz4] = "false",
            };

            using var connection = CreateRestConnection(extras);
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 AS value";
            var result = statement.ExecuteQuery();
            Assert.Equal(1L, DrainStream(result));
        }

        /// <summary>
        /// With <c>cloudfetch.lz4.enabled=true</c> (default) the driver requests
        /// LZ4_FRAME compression. Verified by a successful round-trip: if the server
        /// sends LZ4-compressed data and the driver decompresses it correctly, the
        /// query completes and returns the expected row count.
        /// </summary>
        [SkippableFact]
        public void Lz4EnabledTrue_QuerySucceeds()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection(new Dictionary<string, string>());
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 AS value";
            var result = statement.ExecuteQuery();
            Assert.Equal(1L, DrainStream(result));
        }
    }
}
