/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
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
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Tests.Drivers.Databricks.E2E;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    /// <summary>
    /// End-to-end tests for the CloudFetch feature in the Databricks ADBC driver.
    /// </summary>
    public class CloudFetchE2ETest : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public CloudFetchE2ETest(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            // Skip the test if the DATABRICKS_TEST_CONFIG_FILE environment variable is not set
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        public static IEnumerable<object[]> TestCases()
        {
            // Test cases format: (protocol, query, expected row count, use cloud fetch, enable direct results)

            string smallQuery = $"SELECT * FROM range(1000)";
            string largeQuery = $"SELECT * FROM main.tpcds_sf10_delta.catalog_sales LIMIT 1000000";

            // Thrift protocol test cases - all combinations supported
            yield return new object[] { "thrift", smallQuery, 1000, true, true };
            yield return new object[] { "thrift", smallQuery, 1000, false, true };
            yield return new object[] { "thrift", smallQuery, 1000, true, false };
            yield return new object[] { "thrift", smallQuery, 1000, false, false };
            yield return new object[] { "thrift", largeQuery, 1000000, true, true };
            yield return new object[] { "thrift", largeQuery, 1000000, false, true };
            yield return new object[] { "thrift", largeQuery, 1000000, true, false };
            yield return new object[] { "thrift", largeQuery, 1000000, false, false };

            // REST protocol test cases - test all combinations to verify API parameter fixes
            yield return new object[] { "rest", smallQuery, 1000, true, true };
            yield return new object[] { "rest", smallQuery, 1000, false, true };
            yield return new object[] { "rest", smallQuery, 1000, true, false };
            yield return new object[] { "rest", smallQuery, 1000, false, false };
            yield return new object[] { "rest", largeQuery, 1000000, true, true };
            yield return new object[] { "rest", largeQuery, 1000000, false, true };
            yield return new object[] { "rest", largeQuery, 1000000, true, false };
            yield return new object[] { "rest", largeQuery, 1000000, false, false };
        }

        /// <summary>
        /// Integration test for running queries against a real Databricks cluster with different CloudFetch settings.
        /// </summary>
        [Theory]
        [MemberData(nameof(TestCases))]
        public async Task TestRealDatabricksCloudFetch(string protocol, string query, int rowCount, bool useCloudFetch, bool enableDirectResults)
        {
            // Skip REST tests if Statement Execution API testing is not enabled
            if (protocol == "rest")
            {
                Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true",
                    "Statement Execution API testing not enabled. Set USE_REAL_STATEMENT_EXECUTION_ENDPOINT=true to run REST protocol tests.");
            }

            // Get base properties from environment
            Dictionary<string, string> connectionProperties;

            if (protocol == "rest")
            {
                // Use helper to get REST-specific configuration (extracts host/path from URI)
                connectionProperties = DatabricksTestHelpers.GetPropertiesWithStatementExecutionEnabled(
                    TestEnvironment, TestConfiguration);
            }
            else
            {
                // Use standard Thrift configuration
                connectionProperties = TestEnvironment.GetDriverParameters(TestConfiguration);
            }

            // Apply CloudFetch-specific settings
            connectionProperties[DatabricksParameters.UseCloudFetch] = useCloudFetch.ToString();
            connectionProperties[DatabricksParameters.EnableDirectResults] = enableDirectResults.ToString();
            connectionProperties[DatabricksParameters.CanDecompressLz4] = "true";
            connectionProperties[DatabricksParameters.MaxBytesPerFile] = "10485760"; // 10MB

            // CloudFetchUrlExpirationBufferSeconds is Thrift-specific and not supported by REST API
            if (protocol == "thrift")
            {
                connectionProperties[DatabricksParameters.CloudFetchUrlExpirationBufferSeconds] = (15 * 60 - 2).ToString();
            }

            var connection = NewConnection(TestConfiguration, connectionProperties);

            // Execute a query that generates a large result set using range function
            var statement = connection.CreateStatement();
            statement.SqlQuery = query;

            // Execute the query and get the result
            var result = await statement.ExecuteQueryAsync();

            if (result.Stream == null)
            {
                throw new InvalidOperationException("Result stream is null");
            }

            // Read all the data and count rows
            long totalRows = 0;
            RecordBatch? batch;
            while ((batch = await result.Stream.ReadNextRecordBatchAsync()) != null)
            {
                totalRows += batch.Length;
            }

            // Log actual row count before assertion for debugging
            OutputHelper?.WriteLine($"Protocol: {protocol}, Query type: {(rowCount == 1000 ? "small" : "large")}, CloudFetch: {useCloudFetch}, DirectResults: {enableDirectResults}, Rows: {totalRows}");

            Assert.True(totalRows >= rowCount, $"Expected at least {rowCount} rows but got {totalRows}");

            Assert.Null(await result.Stream.ReadNextRecordBatchAsync());

            // Also log to the test output helper if available
            OutputHelper?.WriteLine($"Read {totalRows} rows from range function");
        }
    }
}
