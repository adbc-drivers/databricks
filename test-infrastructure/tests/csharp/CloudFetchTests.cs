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
        public async Task CloudFetchExpiredLink_FallsBackToFetchResults()
        {
            // Arrange - Enable expired link scenario
            await ControlClient.EnableScenarioAsync("cloudfetch_expired_link");

            // Act - Execute a query that triggers CloudFetch
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT * FROM samples.nyctaxi.trips LIMIT 10000";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            // Assert - Driver should fall back to FetchResults and succeed
            var schema = reader.Schema;
            Assert.NotNull(schema);
            Assert.True(schema.FieldsList.Count > 0);

            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        [Fact]
        public async Task CloudFetchAzure403_FallsBackToFetchResults()
        {
            // Arrange - Enable Azure 403 scenario
            await ControlClient.EnableScenarioAsync("cloudfetch_azure_403");

            // Act - Execute a query that triggers CloudFetch
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT * FROM samples.nyctaxi.trips LIMIT 10000";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            // Assert - Driver should handle 403 and fall back to FetchResults
            var schema = reader.Schema;
            Assert.NotNull(schema);

            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        [Fact]
        public async Task CloudFetchTimeout_FallsBackToFetchResults()
        {
            // Arrange - Enable timeout scenario (65s delay)
            await ControlClient.EnableScenarioAsync("cloudfetch_timeout");

            // Act - Execute a query that triggers CloudFetch
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT * FROM samples.nyctaxi.trips LIMIT 10000";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            // Assert - Driver should timeout and fall back to FetchResults
            // Note: This test may take 60+ seconds as it waits for CloudFetch timeout
            var schema = reader.Schema;
            Assert.NotNull(schema);

            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        [Fact]
        public async Task CloudFetchConnectionReset_FallsBackToFetchResults()
        {
            // Arrange - Enable connection reset scenario
            await ControlClient.EnableScenarioAsync("cloudfetch_connection_reset");

            // Act - Execute a query that triggers CloudFetch
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT * FROM samples.nyctaxi.trips LIMIT 10000";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            // Assert - Driver should handle connection reset and fall back
            var schema = reader.Schema;
            Assert.NotNull(schema);

            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        [Fact]
        public async Task NormalCloudFetch_SucceedsWithoutFailureScenarios()
        {
            // Arrange - No failure scenarios enabled (all disabled by ProxyTestBase.InitializeAsync)

            // Act - Execute a query that triggers CloudFetch (large result set)
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT * FROM samples.nyctaxi.trips LIMIT 10000";

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
        }
    }
}
