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
            // Arrange - Reset call history and enable expired link scenario
            await ControlClient.ResetCallHistoryAsync();
            await ControlClient.EnableScenarioAsync("cloudfetch_expired_link");

            // Act - Execute query with expired link scenario enabled
            // When the CloudFetch download link expires, the driver should call FetchResults
            // again with the same offset to get a fresh download link, then retry the download.
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = TestQuery;

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            // Assert - Driver should refresh the CloudFetch link by calling FetchResults again
            var schema = reader.Schema;
            Assert.NotNull(schema);
            Assert.True(schema.FieldsList.Count > 0);

            // Read multiple batches to trigger CloudFetch downloads
            var totalRows = 0;
            while (true)
            {
                var batch = reader.ReadNextRecordBatchAsync().Result;
                if (batch == null || batch.Length == 0)
                    break;
                totalRows += batch.Length;
                // Read a few batches to ensure CloudFetch URLs are actually used
                if (totalRows > 10000)
                    break;
            }
            Assert.True(totalRows > 0);

            // Verify the driver called FetchResults with offset=0 to refresh the expired URL
            // Note: ExecuteStatement returns initial links via directResults, so we won't see
            // an initial FetchResults(0) call. We only see FetchResults(0) when refreshing.
            var fetchResultsCalls = await ControlClient.GetThriftMethodCallsAsync("FetchResults");

            // Check if there's a FetchResults call with offset=0 (the refresh call)
            var refreshCalls = fetchResultsCalls
                .Where(call => ThriftFieldExtractor.GetLongValue(call, "startRowOffset") == 0)
                .ToList();

            Assert.True(refreshCalls.Count > 0,
                $"Expected to find at least one FetchResults call with startRowOffset=0 (proving URL refresh), " +
                $"but found none. This means the driver did not refresh the expired CloudFetch URL.");
        }

        [Fact]
        public async Task CloudFetch403_RefreshesLinkViaFetchResults()
        {
            // Arrange - Reset call history and enable 403 Forbidden scenario
            await ControlClient.ResetCallHistoryAsync();
            await ControlClient.EnableScenarioAsync("cloudfetch_403");

            // Act - Execute query with 403 scenario enabled
            // When CloudFetch returns 403 Forbidden, the driver should refresh the link
            // by calling FetchResults again and retrying the download.
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = TestQuery;

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            // Assert - Driver should handle 403 and refresh the link via FetchResults
            var schema = reader.Schema;
            Assert.NotNull(schema);

            // Read multiple batches to trigger CloudFetch downloads
            var totalRows = 0;
            while (true)
            {
                var batch = reader.ReadNextRecordBatchAsync().Result;
                if (batch == null || batch.Length == 0)
                    break;
                totalRows += batch.Length;
                // Read a few batches to ensure CloudFetch URLs are actually used
                if (totalRows > 10000)
                    break;
            }
            Assert.True(totalRows > 0);

            // Verify the driver called FetchResults with offset=0 to refresh the expired URL
            // Note: ExecuteStatement returns initial links via directResults, so we won't see
            // an initial FetchResults(0) call. We only see FetchResults(0) when refreshing.
            var fetchResultsCalls = await ControlClient.GetThriftMethodCallsAsync("FetchResults");

            // Check if there's a FetchResults call with offset=0 (the refresh call)
            var refreshCalls = fetchResultsCalls
                .Where(call => ThriftFieldExtractor.GetLongValue(call, "startRowOffset") == 0)
                .ToList();

            Assert.True(refreshCalls.Count > 0,
                $"Expected to find at least one FetchResults call with startRowOffset=0 (proving URL refresh), " +
                $"but found none. This means the driver did not refresh the expired CloudFetch URL.");
        }

        [Fact]
        public async Task CloudFetchTimeout_RetriesWithExponentialBackoff()
        {
            // Arrange - Enable timeout scenario (65s delay)
            // Set CloudFetch timeout to 1 minute so it will timeout during the 65s delay
            await ControlClient.ResetCallHistoryAsync();
            await ControlClient.EnableScenarioAsync("cloudfetch_timeout");

            var timeoutParams = new Dictionary<string, string>
            {
                ["adbc.databricks.cloudfetch.timeout_minutes"] = "1"
            };

            // Act - Execute a query that triggers CloudFetch (>5MB result set)
            // When CloudFetch download times out, the driver retries with exponential backoff
            // (does NOT refresh URL via FetchResults - timeout is not treated as expired link).
            // The generic retry logic will attempt up to 3 times with increasing delays.
            // Using TPC-DS catalog_returns table which has large result sets
            using var connection = CreateProxiedConnectionWithParameters(timeoutParams);
            using var statement = connection.CreateStatement();
            statement.SqlQuery = TestQuery;

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            // Assert - Driver should retry on timeout with exponential backoff
            // Note: This test may take 60+ seconds as it waits for CloudFetch timeout
            var schema = reader.Schema;
            Assert.NotNull(schema);

            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);

            // Verify duplicate cloud fetch calls - driver should retry the same URL
            var allCalls = await ControlClient.GetThriftCallsAsync();
            var cloudDownloads = allCalls.Calls?
                .Where(c => c.Type == "cloud_download")
                .ToList() ?? new List<ThriftCall>();

            var duplicateUrls = cloudDownloads
                .GroupBy(c => c.Url)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .ToList();

            Assert.NotEmpty(duplicateUrls);
        }

        [Fact]
        public async Task CloudFetchConnectionReset_RetriesWithExponentialBackoff()
        {
            // Arrange - Enable connection reset scenario
            await ControlClient.ResetCallHistoryAsync();
            await ControlClient.EnableScenarioAsync("cloudfetch_connection_reset");

            // Act - Execute a query that triggers CloudFetch (>5MB result set)
            // When connection is reset during CloudFetch download, the driver retries with
            // exponential backoff (does NOT refresh URL via FetchResults - connection errors
            // are not treated as expired links). The generic retry logic attempts up to 3 times
            // with delays of 1s, 2s, 3s between retries.
            // Using TPC-DS catalog_returns table which has large result sets
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = TestQuery;

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            // Assert - Driver should retry on connection reset with exponential backoff
            var schema = reader.Schema;
            Assert.NotNull(schema);

            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);

            // Verify duplicate cloud fetch calls - driver should retry the same URL
            var allCalls = await ControlClient.GetThriftCallsAsync();
            var cloudDownloads = allCalls.Calls?
                .Where(c => c.Type == "cloud_download")
                .ToList() ?? new List<ThriftCall>();

            var duplicateUrls = cloudDownloads
                .GroupBy(c => c.Url)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .ToList();

            Assert.NotEmpty(duplicateUrls);
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
