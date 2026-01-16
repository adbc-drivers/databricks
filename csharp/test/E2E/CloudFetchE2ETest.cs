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
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// End-to-end tests for the CloudFetch feature in the Databricks ADBC driver.
    /// Tests both Thrift and Statement Execution REST API protocols.
    /// </summary>
    public class CloudFetchE2ETest : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>, IDisposable
    {
        private readonly ActivityListener? _activityListener;
        private bool _disposed;

        // Activity source names for Databricks drivers
        private static readonly string[] s_activitySourceNames = new[]
        {
            "AdbcDrivers.Databricks",
            "AdbcDrivers.Databricks.StatementExecution"
        };

        public CloudFetchE2ETest(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            // Skip the test if the DATABRICKS_TEST_CONFIG_FILE environment variable is not set
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));

            // Set up activity listener to capture and output trace information
            _activityListener = new ActivityListener
            {
                ShouldListenTo = source => s_activitySourceNames.Contains(source.Name),
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStarted = activity =>
                {
                    var msg = $"[TRACE START] {activity.OperationName} | TraceId: {activity.TraceId} | SpanId: {activity.SpanId}";
                    Debug.WriteLine(msg);
                    OutputHelper?.WriteLine(msg);
                    foreach (var tag in activity.Tags)
                    {
                        var tagMsg = $"  Tag: {tag.Key} = {tag.Value}";
                        Debug.WriteLine(tagMsg);
                        OutputHelper?.WriteLine(tagMsg);
                    }
                },
                ActivityStopped = activity =>
                {
                    var duration = activity.Duration.TotalMilliseconds;
                    var msg = $"[TRACE END] {activity.OperationName} | Duration: {duration:F2}ms | Status: {activity.Status}";
                    Debug.WriteLine(msg);
                    OutputHelper?.WriteLine(msg);
                    foreach (var evt in activity.Events)
                    {
                        var evtMsg = $"  Event: {evt.Name} at {evt.Timestamp:O}";
                        Debug.WriteLine(evtMsg);
                        OutputHelper?.WriteLine(evtMsg);
                        foreach (var tag in evt.Tags)
                        {
                            var tagMsg = $"    {tag.Key} = {tag.Value}";
                            Debug.WriteLine(tagMsg);
                            OutputHelper?.WriteLine(tagMsg);
                        }
                    }
                }
            };
            ActivitySource.AddActivityListener(_activityListener);
        }

        public new void Dispose()
        {
            if (!_disposed)
            {
                _activityListener?.Dispose();
                _disposed = true;
            }
            base.Dispose();
        }

        /// <summary>
        /// Test cases for CloudFetch with protocol dimension.
        /// Format: (query, expected row count, use cloud fetch, enable direct results, protocol)
        /// </summary>
        public static IEnumerable<object[]> TestCases()
        {
            string[] protocols = { "thrift", "rest" };

            string zeroQuery = "SELECT * FROM range(1000) LIMIT 0";
            string smallQuery = "SELECT * FROM range(1000)";
            string largeQuery = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";

            foreach (var protocol in protocols)
            {
                // LIMIT 0 test cases - edge case for empty result set (PECO-2524)
                yield return new object[] { zeroQuery, 0, true, true, protocol };
                yield return new object[] { zeroQuery, 0, false, true, protocol };

                // Small query test cases
                yield return new object[] { smallQuery, 1000, true, true, protocol };
                yield return new object[] { smallQuery, 1000, false, true, protocol };
                yield return new object[] { smallQuery, 1000, true, false, protocol };
                yield return new object[] { smallQuery, 1000, false, false, protocol };

                // Large query test cases
                yield return new object[] { largeQuery, 1000000, true, true, protocol };
                yield return new object[] { largeQuery, 1000000, false, true, protocol };
                yield return new object[] { largeQuery, 1000000, true, false, protocol };
                yield return new object[] { largeQuery, 1000000, false, false, protocol };
            }
        }

        /// <summary>
        /// Integration test for running queries against a real Databricks cluster with different CloudFetch settings.
        /// Tests both Thrift and Statement Execution REST API protocols.
        /// </summary>
        [Theory]
        [MemberData(nameof(TestCases))]
        public async Task TestCloudFetch(string query, int rowCount, bool useCloudFetch, bool enableDirectResults, string protocol)
        {
            var parameters = new Dictionary<string, string>
            {
                [DatabricksParameters.Protocol] = protocol,
                [DatabricksParameters.UseCloudFetch] = useCloudFetch.ToString(),
                [DatabricksParameters.EnableDirectResults] = enableDirectResults.ToString(),
                [DatabricksParameters.CanDecompressLz4] = "true",
                [DatabricksParameters.MaxBytesPerFile] = "10485760", // 10MB
                [DatabricksParameters.CloudFetchUrlExpirationBufferSeconds] = (15 * 60 - 2).ToString(),
            };

            // For REST API, configure result disposition based on CloudFetch setting
            if (protocol == "rest")
            {
                // Map useCloudFetch to result disposition:
                // - useCloudFetch=true -> EXTERNAL_LINKS (forces CloudFetch)
                // - useCloudFetch=false -> INLINE_OR_EXTERNAL_LINKS (server decides, prefers inline for small results)
                // Note: API expects uppercase values
                parameters[DatabricksParameters.ResultDisposition] = "INLINE_OR_EXTERNAL_LINKS";
                parameters[DatabricksParameters.ResultFormat] = "ARROW_STREAM";
                parameters[DatabricksParameters.ResultCompression] = "LZ4_FRAME";
            }

            var connection = NewConnection(TestConfiguration, parameters);
            var protocolName = protocol == "rest" ? "REST API" : "Thrift";

            await ExecuteAndValidateQuery(connection, query, rowCount, protocolName);
        }

        /// <summary>
        /// Executes a query and validates the row count.
        /// Validates exact row count to ensure the driver correctly respects LIMIT N in queries (PECO-2524).
        /// </summary>
        private async Task ExecuteAndValidateQuery(AdbcConnection connection, string query, int expectedRowCount, string protocolName)
        {
            Console.WriteLine($"[TEST] ExecuteAndValidateQuery START - {protocolName}");
            // Execute a query that generates a large result set
            var statement = connection.CreateStatement();
            Console.WriteLine($"[TEST] Statement created");
            statement.SqlQuery = query;

            // Execute the query and get the result
            Console.WriteLine($"[TEST] Executing query...");
            var result = await statement.ExecuteQueryAsync();
            Console.WriteLine($"[TEST] Query executed, RowCount={result.RowCount}");

            if (result.Stream == null)
            {
                throw new InvalidOperationException("Result stream is null");
            }

            // Read all the data and count rows
            long totalRows = 0;
            int batchCount = 0;
            RecordBatch? batch;
            Console.WriteLine($"[TEST] Reading batches...");
            while ((batch = await result.Stream.ReadNextRecordBatchAsync()) != null)
            {
                totalRows += batch.Length;
                batchCount++;
                if (batchCount % 10 == 0)
                {
                    Console.WriteLine($"[TEST] Read {batchCount} batches, {totalRows} rows so far");
                }
            }
            Console.WriteLine($"[TEST] Finished reading {batchCount} batches, {totalRows} total rows");

            // Validate exact row count - driver must respect LIMIT N and trim excess rows (PECO-2524)
            // For Thrift: sum of all batch.RowCount = total expected rows
            // For REST API (SEA): manifest.TotalRowCount = total expected rows
            Assert.Equal(expectedRowCount, totalRows);

            Assert.Null(await result.Stream.ReadNextRecordBatchAsync());
            statement.Dispose();

            // Also log to the test output helper if available
            OutputHelper?.WriteLine($"[{protocolName}] Read exactly {totalRows} rows as expected");
        }
    }
}
