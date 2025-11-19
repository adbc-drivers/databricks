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
            // Test cases format: (query, expected row count, use cloud fetch, enable direct results)

            string smallQuery = $"SELECT * FROM range(1000)";
            yield return new object[] { smallQuery, 1000, true, true };
            yield return new object[] { smallQuery, 1000, false, true };
            yield return new object[] { smallQuery, 1000, true, false };
            yield return new object[] { smallQuery, 1000, false, false };

            string largeQuery = $"SELECT * FROM samples.tpcds_sf1000.catalog_sales LIMIT 1000000";
            yield return new object[] { largeQuery, 1000000, true, true };
            yield return new object[] { largeQuery, 1000000, false, true };
            yield return new object[] { largeQuery, 1000000, true, false };
            yield return new object[] { largeQuery, 1000000, false, false };
        }

        /// <summary>
        /// Integration test for running queries against a real Databricks cluster with different CloudFetch settings.
        /// </summary>
        [Theory]
        [MemberData(nameof(TestCases))]
        public async Task TestRealDatabricksCloudFetch(string query, int rowCount, bool useCloudFetch, bool enableDirectResults)
        {
            var testStartTime = DateTime.UtcNow;
            OutputHelper?.WriteLine($"=== TEST START at {testStartTime:yyyy-MM-dd HH:mm:ss.fff} UTC ===");
            OutputHelper?.WriteLine($"Query: {query}");
            OutputHelper?.WriteLine($"Expected rows: {rowCount}");
            OutputHelper?.WriteLine($"UseCloudFetch: {useCloudFetch}");
            OutputHelper?.WriteLine($"EnableDirectResults: {enableDirectResults}");

            // Add unique comment to query to avoid Databricks result cache
            string cacheBypassQuery = $"{query} /* test_id:{Guid.NewGuid()} */";
            OutputHelper?.WriteLine($"Cache-bypass query: {cacheBypassQuery}");

            var connection = NewConnection(TestConfiguration, new Dictionary<string, string>
            {
                [DatabricksParameters.UseCloudFetch] = useCloudFetch.ToString(),
                [DatabricksParameters.EnableDirectResults] = enableDirectResults.ToString(),
                [DatabricksParameters.CanDecompressLz4] = "true",
                [DatabricksParameters.MaxBytesPerFile] = "10485760", // 10MB
                [DatabricksParameters.CloudFetchUrlExpirationBufferSeconds] = (15 * 60 - 2).ToString(),
            });

            var statement = connection.CreateStatement();
            statement.SqlQuery = cacheBypassQuery;

            var queryStartTime = DateTime.UtcNow;
            OutputHelper?.WriteLine($"Executing query at {queryStartTime:yyyy-MM-dd HH:mm:ss.fff} UTC");

            var result = await statement.ExecuteQueryAsync();
            var queryEndTime = DateTime.UtcNow;
            OutputHelper?.WriteLine($"Query execution completed at {queryEndTime:yyyy-MM-dd HH:mm:ss.fff} UTC (duration: {(queryEndTime - queryStartTime).TotalSeconds:F2}s)");

            if (result.Stream == null)
            {
                throw new InvalidOperationException("Result stream is null");
            }

            // Read all the data, count rows, and ACCESS ACTUAL COLUMN DATA to force materialization
            long totalRows = 0;
            int batchCount = 0;
            object? firstValue = null;
            object? lastValue = null;
            var readStartTime = DateTime.UtcNow;

            RecordBatch? batch;
            while ((batch = await result.Stream.ReadNextRecordBatchAsync()) != null)
            {
                totalRows += batch.Length;
                batchCount++;

                // ACCESS ACTUAL DATA to ensure CloudFetch downloads/decompresses files
                if (batch.Length > 0 && batch.ColumnCount > 0)
                {
                    var column = batch.Column(0);
                    if (firstValue == null)
                    {
                        firstValue = column.GetValue(0);
                    }
                    lastValue = column.GetValue(batch.Length - 1);
                }

                // Log progress every 100 batches for large result sets
                if (batchCount % 100 == 0)
                {
                    var elapsed = (DateTime.UtcNow - readStartTime).TotalSeconds;
                    OutputHelper?.WriteLine($"  Progress: {batchCount} batches, {totalRows} rows, {elapsed:F2}s elapsed");
                }
            }

            var readEndTime = DateTime.UtcNow;
            var readDuration = (readEndTime - readStartTime).TotalSeconds;
            var totalDuration = (readEndTime - testStartTime).TotalSeconds;

            OutputHelper?.WriteLine($"=== RESULTS ===");
            OutputHelper?.WriteLine($"Total rows read: {totalRows}");
            OutputHelper?.WriteLine($"Total batches: {batchCount}");
            OutputHelper?.WriteLine($"First value: {firstValue}");
            OutputHelper?.WriteLine($"Last value: {lastValue}");
            OutputHelper?.WriteLine($"Read duration: {readDuration:F2}s");
            OutputHelper?.WriteLine($"Total test duration: {totalDuration:F2}s");
            OutputHelper?.WriteLine($"=== TEST END at {readEndTime:yyyy-MM-dd HH:mm:ss.fff} UTC ===");

            // Use exact assertion instead of >=
            Assert.Equal(rowCount, totalRows);

            Assert.Null(await result.Stream.ReadNextRecordBatchAsync());
        }
    }
}
