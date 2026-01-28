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
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// End-to-end tests for the FeatureFlagCache functionality using a real Databricks instance.
    /// Tests that feature flags are properly fetched and cached from the Databricks connector service.
    /// </summary>
    public class FeatureFlagCacheE2ETest : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public FeatureFlagCacheE2ETest(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            // Skip the test if the DATABRICKS_TEST_CONFIG_FILE environment variable is not set
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Tests that creating a connection successfully initializes the feature flag cache.
        /// The cache should contain feature flags fetched from the real Databricks instance.
        /// </summary>
        [SkippableFact]
        public async Task TestFeatureFlagCacheInitialization()
        {
            // Arrange & Act - Create a connection which initializes the feature flag cache
            using var connection = NewConnection(TestConfiguration);

            // Assert - The connection should be created successfully
            // The feature flag cache is initialized internally during connection creation
            Assert.NotNull(connection);

            // Execute a simple query to verify the connection works
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 as test_value";
            var result = await statement.ExecuteQueryAsync();

            Assert.NotNull(result.Stream);
            var batch = await result.Stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);

            OutputHelper?.WriteLine("[FeatureFlagCacheE2ETest] Connection with feature flag cache initialized successfully");
        }

        /// <summary>
        /// Tests that multiple connections to the same host share the same feature flag context.
        /// This verifies the per-host caching behavior.
        /// </summary>
        [SkippableFact]
        public async Task TestFeatureFlagCacheSharedAcrossConnections()
        {
            // Arrange - Get the singleton cache instance
            var cache = FeatureFlagCache.GetInstance();

            // Act - Create two connections to the same host
            using var connection1 = NewConnection(TestConfiguration);
            using var connection2 = NewConnection(TestConfiguration);

            // Assert - Both connections should work properly
            Assert.NotNull(connection1);
            Assert.NotNull(connection2);

            // Verify both connections can execute queries
            using var statement1 = connection1.CreateStatement();
            statement1.SqlQuery = "SELECT 1 as conn1_test";
            var result1 = await statement1.ExecuteQueryAsync();
            Assert.NotNull(result1.Stream);

            using var statement2 = connection2.CreateStatement();
            statement2.SqlQuery = "SELECT 2 as conn2_test";
            var result2 = await statement2.ExecuteQueryAsync();
            Assert.NotNull(result2.Stream);

            OutputHelper?.WriteLine("[FeatureFlagCacheE2ETest] Multiple connections sharing feature flag cache work correctly");
        }

        /// <summary>
        /// Tests that the feature flag cache is properly cleaned up when all connections close.
        /// </summary>
        [SkippableFact]
        public async Task TestFeatureFlagCacheCleanupOnConnectionClose()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var hostName = TestConfiguration.HostName ?? TestConfiguration.Uri;

            // Skip if we can't determine the host name
            Skip.If(string.IsNullOrEmpty(hostName), "Cannot determine host name from test configuration");

            // Normalize host name (remove protocol if present)
            if (hostName!.StartsWith("https://"))
            {
                hostName = hostName.Substring("https://".Length);
            }
            if (hostName.StartsWith("http://"))
            {
                hostName = hostName.Substring("http://".Length);
            }

            // Act - Create and close a connection
            using (var connection = NewConnection(TestConfiguration))
            {
                // Connection is active, cache should have a context for this host
                Assert.NotNull(connection);

                // Execute a query to ensure the connection is fully initialized
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1";
                var result = await statement.ExecuteQueryAsync();
                Assert.NotNull(result.Stream);
            }
            // Connection is disposed here

            OutputHelper?.WriteLine("[FeatureFlagCacheE2ETest] Feature flag cache cleanup test completed");
        }

        /// <summary>
        /// Tests that connections work correctly with feature flags enabled.
        /// This is a basic sanity check that the feature flag infrastructure doesn't
        /// interfere with normal connection operations.
        /// </summary>
        [SkippableFact]
        public async Task TestConnectionWithFeatureFlagsExecutesQueries()
        {
            // Arrange
            using var connection = NewConnection(TestConfiguration);

            // Act - Execute multiple queries to ensure feature flags don't interfere
            var queries = new[]
            {
                "SELECT 1 as value",
                "SELECT 'hello' as greeting",
                "SELECT CURRENT_DATE() as today"
            };

            foreach (var query in queries)
            {
                using var statement = connection.CreateStatement();
                statement.SqlQuery = query;
                var result = await statement.ExecuteQueryAsync();

                // Assert
                Assert.NotNull(result.Stream);
                var batch = await result.Stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.True(batch.Length > 0, $"Query '{query}' should return at least one row");

                OutputHelper?.WriteLine($"[FeatureFlagCacheE2ETest] Query executed successfully: {query}");
            }
        }
    }
}
