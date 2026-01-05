/*
* Copyright (c) 2025 ADBC Drivers Contributors
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
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.ThriftProtocol
{
    /// <summary>
    /// Tests that validate driver behavior when CloudFetch operations fail.
    /// CloudFetch is Databricks' high-performance result retrieval system that downloads
    /// results directly from cloud storage (Azure Blob, S3, GCS).
    /// </summary>
    public class CloudFetchTests : ProxyTestBase
    {
        private const string TestQuery = "SELECT * FROM main.tpcds_sf1_delta.catalog_returns";

        [Fact]
        public async Task CloudFetchExpiredLink_RefreshesLinkViaFetchResults()
        {
            // Arrange - First establish baseline by running query without failure scenario
            int baselineFetchResults;
            using (var connection = CreateProxiedConnection())
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = TestQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
                baselineFetchResults = await ControlClient.CountThriftMethodCallsAsync("FetchResults");
            }

            // Arrange - Enable expired link scenario
            await ControlClient.EnableScenarioAsync("cloudfetch_expired_link");

            // Act - Execute the same query with expired link scenario enabled
            // When the CloudFetch download link expires, the driver should call FetchResults
            // again with the same offset to get a fresh download link, then retry the download.
            using var connection2 = CreateProxiedConnection();
            using var statement2 = connection2.CreateStatement();
            statement2.SqlQuery = TestQuery;

            var result2 = statement2.ExecuteQuery();
            Assert.NotNull(result2);

            using var reader2 = result2.Stream;
            Assert.NotNull(reader2);

            // Assert - Driver should refresh the CloudFetch link by calling FetchResults again
            var schema = reader2.Schema;
            Assert.NotNull(schema);
            Assert.True(schema.FieldsList.Count > 0);

            var batch = reader2.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);

            // Verify FetchResults was called exactly (baseline + 1) times
            // The extra call proves the driver refreshed the URL after link expiry
            var actualFetchResults = await ControlClient.CountThriftMethodCallsAsync("FetchResults");
            var expectedFetchResults = baselineFetchResults + 1;
            Assert.Equal(expectedFetchResults, actualFetchResults);
        }

        [Fact]
        public async Task CloudFetch403_RefreshesLinkViaFetchResults()
        {
            // Arrange - First establish baseline by running query without failure scenario
            int baselineFetchResults;
            using (var connection = CreateProxiedConnection())
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = TestQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
                baselineFetchResults = await ControlClient.CountThriftMethodCallsAsync("FetchResults");
            }

            // Arrange - Enable 403 Forbidden scenario
            await ControlClient.EnableScenarioAsync("cloudfetch_403");

            // Act - Execute the same query with 403 scenario enabled
            // When CloudFetch returns 403 Forbidden, the driver should refresh the link
            // by calling FetchResults again and retrying the download.
            using var connection2 = CreateProxiedConnection();
            using var statement2 = connection2.CreateStatement();
            statement2.SqlQuery = TestQuery;

            var result2 = statement2.ExecuteQuery();
            Assert.NotNull(result2);

            using var reader2 = result2.Stream;
            Assert.NotNull(reader2);

            // Assert - Driver should handle 403 and refresh the link via FetchResults
            var schema = reader2.Schema;
            Assert.NotNull(schema);

            var batch = reader2.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);

            // Verify FetchResults was called exactly (baseline + 1) times
            // The extra call proves the driver refreshed the URL after 403 Forbidden
            var actualFetchResults = await ControlClient.CountThriftMethodCallsAsync("FetchResults");
            var expectedFetchResults = baselineFetchResults + 1;
            Assert.Equal(expectedFetchResults, actualFetchResults);
        }

        [Fact]
        public async Task CloudFetchTimeout_RetriesWithExponentialBackoff()
        {
            // Arrange - First establish baseline by running query without failure scenario
            // Set CloudFetch timeout to 1 minute so it will timeout during the 65s delay
            var timeoutParams = new Dictionary<string, string>
            {
                ["adbc.databricks.cloudfetch.timeout_minutes"] = "1"
            };

            int baselineCloudDownloads;
            using (var connection = CreateProxiedConnectionWithParameters(timeoutParams))
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = TestQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
                baselineCloudDownloads = await ControlClient.CountCloudDownloadsAsync();
            }

            // Arrange - Enable timeout scenario (65s delay) - driver will timeout at 60s and retry
            await ControlClient.EnableScenarioAsync("cloudfetch_timeout");

            // Act - Execute a query that triggers CloudFetch (>5MB result set)
            // When CloudFetch download times out, the driver retries with exponential backoff
            // (does NOT refresh URL via FetchResults - timeout is not treated as expired link).
            // The generic retry logic will attempt up to 3 times with increasing delays.
            // Using TPC-DS catalog_returns table which has large result sets
            using var connection2 = CreateProxiedConnectionWithParameters(timeoutParams);
            using var statement2 = connection2.CreateStatement();
            statement2.SqlQuery = TestQuery;

            var result2 = statement2.ExecuteQuery();
            Assert.NotNull(result2);

            using var reader2 = result2.Stream;
            Assert.NotNull(reader2);

            // Assert - Driver should retry on timeout with exponential backoff
            // Note: This test may take 60+ seconds as it waits for CloudFetch timeout
            var schema = reader2.Schema;
            Assert.NotNull(schema);

            var batch = reader2.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);

            // Get detailed call history to understand what's happening
            var callHistory = await ControlClient.GetThriftCallsAsync();
            var actualCloudDownloads = callHistory.Calls?.Count(c => c.Type == "cloud_download") ?? 0;
            var fetchResultsCalls = callHistory.Calls?.Count(c => c.Type == "thrift" && c.Method == "FetchResults") ?? 0;

            // Extract cloud download URLs to see if file 2 was requested
            var cloudDownloadUrls = callHistory.Calls?
                .Where(c => c.Type == "cloud_download")
                .Select(c => c.Url)
                .ToList() ?? new List<string>();

            var expectedCloudDownloads = baselineCloudDownloads + 1;

            // Provide detailed diagnostics
            var diagnosticMessage = $"CloudFetch timeout verification:\n" +
                                   $"  Baseline cloud downloads: {baselineCloudDownloads}\n" +
                                   $"  Expected cloud downloads: {expectedCloudDownloads}\n" +
                                   $"  Actual cloud downloads: {actualCloudDownloads}\n" +
                                   $"  FetchResults calls: {fetchResultsCalls}\n" +
                                   $"  Cloud download URLs:\n" +
                                   string.Join("\n", cloudDownloadUrls.Select((url, i) => $"    [{i + 1}] {url}"));

            if (actualCloudDownloads < expectedCloudDownloads)
            {
                throw new Xunit.Sdk.XunitException(diagnosticMessage);
            }

            Assert.Equal(expectedCloudDownloads, actualCloudDownloads);
        }

        [Fact]
        public async Task CloudFetchConnectionReset_RetriesWithExponentialBackoff()
        {
            // Arrange - First establish baseline by running query without failure scenario
            int baselineCloudDownloads;
            using (var connection = CreateProxiedConnection())
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = TestQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
                baselineCloudDownloads = await ControlClient.CountCloudDownloadsAsync();
            }

            // Arrange - Enable connection reset scenario
            await ControlClient.EnableScenarioAsync("cloudfetch_connection_reset");

            // Act - Execute a query that triggers CloudFetch (>5MB result set)
            // When connection is reset during CloudFetch download, the driver retries with
            // exponential backoff (does NOT refresh URL via FetchResults - connection errors
            // are not treated as expired links). The generic retry logic attempts up to 3 times
            // with delays of 1s, 2s, 3s between retries.
            // Using TPC-DS catalog_returns table which has large result sets
            using var connection2 = CreateProxiedConnection();
            using var statement2 = connection2.CreateStatement();
            statement2.SqlQuery = TestQuery;

            var result2 = statement2.ExecuteQuery();
            Assert.NotNull(result2);

            using var reader2 = result2.Stream;
            Assert.NotNull(reader2);

            // Assert - Driver should retry on connection reset with exponential backoff
            var schema = reader2.Schema;
            Assert.NotNull(schema);

            var batch = reader2.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);

            // Verify cloud downloads were called exactly (baseline + 1) times
            // The extra call proves the driver retried the download after connection reset
            var actualCloudDownloads = await ControlClient.CountCloudDownloadsAsync();
            var expectedCloudDownloads = baselineCloudDownloads + 1;
            Assert.Equal(expectedCloudDownloads, actualCloudDownloads);
        }

        [Fact]
        public async Task NormalCloudFetch_SucceedsWithoutFailureScenarios()
        {
            // Arrange - No failure scenarios enabled (all disabled by ProxyTestBase.InitializeAsync)

            // Act - Execute a query that triggers CloudFetch (large result set)
            // Using TPC-DS catalog_returns table which has large result sets
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = TestQuery;

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            // Assert - Verify CloudFetch executed successfully
            var schema = reader.Schema;
            Assert.NotNull(schema);
            Assert.True(schema.FieldsList.Count > 0);

            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);

            // Verify normal Thrift call pattern (ExecuteStatement, FetchResults, CloseSession)
            await ControlClient.AssertThriftMethodCalledAsync("ExecuteStatement", minCalls: 1);

            // Verify FetchResults was called (should have at least 1 call for normal operation)
            var actualFetchResults = await ControlClient.CountThriftMethodCallsAsync("FetchResults");
            Assert.True(actualFetchResults >= 1,
                $"Expected FetchResults to be called at least once, but was called {actualFetchResults} times");
        }
    }
}
