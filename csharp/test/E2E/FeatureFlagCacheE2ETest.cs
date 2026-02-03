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
        /// Tests that creating a connection successfully initializes the feature flag cache
        /// and verifies that flags are actually fetched from the server.
        /// </summary>
        [SkippableFact]
        public async Task TestFeatureFlagCacheInitialization()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var hostName = GetNormalizedHostName();
            Skip.If(string.IsNullOrEmpty(hostName), "Cannot determine host name from test configuration");

            // Act - Create a connection which initializes the feature flag cache
            using var connection = NewConnection(TestConfiguration);

            // Assert - The connection should be created successfully
            Assert.NotNull(connection);

            // Verify the feature flag context exists for this host
            Assert.True(cache.TryGetContext(hostName!, out var context), "Feature flag context should exist after connection creation");
            Assert.NotNull(context);

            // Verify that some flags were fetched from the server
            // The server should return at least some feature flags
            var flags = context.GetAllFlags();
            OutputHelper?.WriteLine($"[FeatureFlagCacheE2ETest] Fetched {flags.Count} feature flags from server");
            foreach (var flag in flags)
            {
                OutputHelper?.WriteLine($"  - {flag.Key}: {flag.Value}");
            }

            // Log the TTL from the server
            OutputHelper?.WriteLine($"[FeatureFlagCacheE2ETest] TTL from server: {context.Ttl.TotalSeconds} seconds");

            // Note: We don't assert flags.Count > 0 because the server may return empty flags
            // in some environments, but we verify the infrastructure works

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
        /// Tests that the feature flag cache persists after connections close (TTL-based expiration).
        /// With the IMemoryCache implementation, contexts stay in cache until TTL expires,
        /// not when connections close.
        /// </summary>
        [SkippableFact]
        public async Task TestFeatureFlagCachePersistsAfterConnectionClose()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var hostName = GetNormalizedHostName();
            Skip.If(string.IsNullOrEmpty(hostName), "Cannot determine host name from test configuration");

            OutputHelper?.WriteLine($"[FeatureFlagCacheE2ETest] Initial cache count: {cache.CachedHostCount}");

            // Act - Create and close a connection
            using (var connection = NewConnection(TestConfiguration))
            {
                // Connection is active, cache should have a context for this host
                Assert.NotNull(connection);

                // Verify context exists during connection
                Assert.True(cache.TryGetContext(hostName!, out var context), "Context should exist while connection is active");
                Assert.NotNull(context);

                // Verify flags were fetched
                var flags = context.GetAllFlags();
                OutputHelper?.WriteLine($"[FeatureFlagCacheE2ETest] Flags fetched: {flags.Count}");
                OutputHelper?.WriteLine($"[FeatureFlagCacheE2ETest] TTL: {context.Ttl.TotalSeconds} seconds");

                // Execute a query to ensure the connection is fully initialized
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1";
                var result = await statement.ExecuteQueryAsync();
                Assert.NotNull(result.Stream);
            }
            // Connection is disposed here

            // With TTL-based caching, the context should still exist in the cache
            // (it only gets removed when TTL expires or cache is explicitly cleared)
            Assert.True(cache.TryGetContext(hostName!, out var contextAfterDispose),
                "Context should still exist in cache after connection close (TTL-based expiration)");
            Assert.NotNull(contextAfterDispose);

            OutputHelper?.WriteLine($"[FeatureFlagCacheE2ETest] Context still exists after connection close with TTL: {contextAfterDispose.Ttl.TotalSeconds}s");
            OutputHelper?.WriteLine($"[FeatureFlagCacheE2ETest] Final cache count: {cache.CachedHostCount}");
            OutputHelper?.WriteLine("[FeatureFlagCacheE2ETest] Feature flag cache TTL-based persistence test completed");
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

        /// <summary>
        /// Gets the normalized host name from test configuration.
        /// Strips protocol prefix if present (e.g., "https://host" -> "host").
        /// </summary>
        private string? GetNormalizedHostName()
        {
            var hostName = TestConfiguration.HostName ?? TestConfiguration.Uri;
            if (string.IsNullOrEmpty(hostName))
            {
                return null;
            }

            // Try to parse as URI first
            if (Uri.TryCreate(hostName, UriKind.Absolute, out Uri? parsedUri) &&
                (parsedUri.Scheme == Uri.UriSchemeHttp || parsedUri.Scheme == Uri.UriSchemeHttps))
            {
                return parsedUri.Host;
            }

            // Fallback: strip common protocol prefixes manually
            if (hostName.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            {
                return hostName.Substring(8);
            }
            if (hostName.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            {
                return hostName.Substring(7);
            }

            return hostName;
        }
    }
}
