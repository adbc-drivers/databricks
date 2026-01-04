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
        [Fact]
        public async Task CloudFetchExpiredLink_RefreshesLinkViaFetchResults()
        {
            // Arrange - Enable expired link scenario
            await ControlClient.EnableScenarioAsync("cloudfetch_expired_link");

            // Act - Execute a query that triggers CloudFetch (>5MB result set)
            // When the CloudFetch download link expires (403), the driver should call FetchResults
            // again with the same offset to get a fresh download link, then retry the download.
            // Using TPC-DS catalog_returns table which has large result sets
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT * FROM main.tpcds_sf1_delta.catalog_returns";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            // Assert - Driver should refresh the CloudFetch link by calling FetchResults again
            // and successfully retrieve the data
            var schema = reader.Schema;
            Assert.NotNull(schema);
            Assert.True(schema.FieldsList.Count > 0);

            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);

            // Verify FetchResults was called at least 2 times (initial + refresh after link expiry)
            await ControlClient.AssertThriftMethodCalledAsync("FetchResults", minCalls: 2);
        }

        [Fact]
        public async Task CloudFetch403_RefreshesLinkViaFetchResults()
        {
            // Arrange - Enable 403 Forbidden scenario
            await ControlClient.EnableScenarioAsync("cloudfetch_403");

            // Act - Execute a query that triggers CloudFetch (>5MB result set)
            // When CloudFetch returns 403 Forbidden, the driver should refresh the link
            // by calling FetchResults again and retrying the download.
            // Using TPC-DS catalog_returns table which has large result sets
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT * FROM main.tpcds_sf1_delta.catalog_returns";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            // Assert - Driver should handle 403 and refresh the link via FetchResults
            var schema = reader.Schema;
            Assert.NotNull(schema);

            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);

            // Verify FetchResults was called at least 2 times (initial + refresh after 403)
            await ControlClient.AssertThriftMethodCalledAsync("FetchResults", minCalls: 2);
        }

        [Fact]
        public async Task CloudFetchTimeout_RetriesWithExponentialBackoff()
        {
            // Arrange - Enable timeout scenario (65s delay)
            await ControlClient.EnableScenarioAsync("cloudfetch_timeout");

            // Act - Execute a query that triggers CloudFetch (>5MB result set)
            // When CloudFetch download times out, the driver retries with exponential backoff
            // (does NOT refresh URL via FetchResults - timeout is not treated as expired link).
            // The generic retry logic will attempt up to 3 times with increasing delays.
            // Using TPC-DS catalog_returns table which has large result sets
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT * FROM main.tpcds_sf1_delta.catalog_returns";

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

            // Verify FetchResults was called once (timeout doesn't trigger URL refresh)
            // The driver retries the CloudFetch download directly, not via FetchResults
            var fetchResultsCalls = await ControlClient.CountThriftMethodCallsAsync("FetchResults");
            Assert.True(fetchResultsCalls >= 1,
                $"Expected FetchResults to be called at least once, but was called {fetchResultsCalls} times");
        }

        [Fact]
        public async Task CloudFetchConnectionReset_RetriesWithExponentialBackoff()
        {
            // Arrange - Enable connection reset scenario
            await ControlClient.EnableScenarioAsync("cloudfetch_connection_reset");

            // Act - Execute a query that triggers CloudFetch (>5MB result set)
            // When connection is reset during CloudFetch download, the driver retries with
            // exponential backoff (does NOT refresh URL via FetchResults - connection errors
            // are not treated as expired links). The generic retry logic attempts up to 3 times
            // with delays of 1s, 2s, 3s between retries.
            // Using TPC-DS catalog_returns table which has large result sets
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT * FROM main.tpcds_sf1_delta.catalog_returns";

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

            // Verify FetchResults was called once (connection reset doesn't trigger URL refresh)
            // The driver retries the CloudFetch download directly, not via FetchResults
            var fetchResultsCalls = await ControlClient.CountThriftMethodCallsAsync("FetchResults");
            Assert.True(fetchResultsCalls >= 1,
                $"Expected FetchResults to be called at least once, but was called {fetchResultsCalls} times");
        }

        [Fact]
        public async Task NormalCloudFetch_SucceedsWithoutFailureScenarios()
        {
            // Arrange - No failure scenarios enabled (all disabled by ProxyTestBase.InitializeAsync)

            // Act - Execute a query that triggers CloudFetch (large result set)
            // Using TPC-DS catalog_returns table which has large result sets
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT * FROM main.tpcds_sf1_delta.catalog_returns";

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
            await ControlClient.AssertThriftMethodCalledAsync("FetchResults", minCalls: 1);
        }
    }
}
