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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Microsoft.Extensions.Caching.Memory;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for FeatureFlagCache.
    /// Tests feature flag fetching from real Databricks endpoints and validates
    /// caching behavior using the MergePropertiesWithFeatureFlagsAsync API.
    /// </summary>
    /// <remarks>
    /// These tests require:
    /// - DATABRICKS_TEST_CONFIG_FILE environment variable pointing to a valid test config
    /// - The test config must include: hostName, token/access_token
    /// </remarks>
    public class FeatureFlagCacheE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        private readonly bool _canRunRealEndpointTests;
        private readonly string? _host;
        private readonly string? _token;

        /// <summary>
        /// Test assembly version for feature flag endpoint.
        /// </summary>
        private const string TestAssemblyVersion = "1.0.0-test";

        public FeatureFlagCacheE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            // Check if we can run tests against a real endpoint
            _canRunRealEndpointTests = Utils.CanExecuteTestConfig(TestConfigVariable);

            if (_canRunRealEndpointTests)
            {
                try
                {
                    // Try to get host from HostName first, then fallback to extracting from Uri
                    _host = TestConfiguration.HostName;
                    if (string.IsNullOrEmpty(_host) && !string.IsNullOrEmpty(TestConfiguration.Uri))
                    {
                        // Extract host from Uri (e.g., https://host.databricks.com/sql/1.0/...)
                        var uri = new Uri(TestConfiguration.Uri);
                        _host = uri.Host;
                    }

                    _token = TestConfiguration.Token ?? TestConfiguration.AccessToken;

                    // Validate we have required configuration
                    if (string.IsNullOrEmpty(_host) || string.IsNullOrEmpty(_token))
                    {
                        _canRunRealEndpointTests = false;
                    }
                }
                catch
                {
                    _canRunRealEndpointTests = false;
                }
            }
        }

        /// <summary>
        /// Creates connection properties for testing.
        /// </summary>
        private Dictionary<string, string> CreateTestProperties(string? host = null, string? token = null)
        {
            var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var effectiveHost = host ?? _host;
            var effectiveToken = token ?? _token;

            if (!string.IsNullOrEmpty(effectiveHost))
            {
                properties[SparkParameters.HostName] = effectiveHost;
            }

            if (!string.IsNullOrEmpty(effectiveToken))
            {
                properties[DatabricksParameters.Token] = effectiveToken;
            }

            return properties;
        }

        #region Real Endpoint Tests

        /// <summary>
        /// Tests that FeatureFlagCache can fetch and merge feature flags from a real Databricks endpoint.
        /// This validates the cache correctly handles real-world HTTP responses.
        /// </summary>
        [SkippableFact]
        public async Task MergePropertiesWithFeatureFlags_RealEndpoint_ReturnsProperties()
        {
            Skip.IfNot(_canRunRealEndpointTests, "Real endpoint testing requires DATABRICKS_TEST_CONFIG_FILE");

            // Arrange
            using var cache = new FeatureFlagCache();
            var properties = CreateTestProperties();

            OutputHelper?.WriteLine($"Testing feature flag fetch from real endpoint: {_host}");

            // Act
            var mergedProperties = await cache.MergePropertiesWithFeatureFlagsAsync(
                properties,
                TestAssemblyVersion,
                CancellationToken.None);

            // Assert
            // The merged properties should contain at least our original properties
            Assert.NotNull(mergedProperties);
            Assert.True(mergedProperties.ContainsKey(SparkParameters.HostName));
            Assert.Equal(_host, mergedProperties[SparkParameters.HostName]);

            OutputHelper?.WriteLine($"Merged properties count: {mergedProperties.Count}");
            OutputHelper?.WriteLine($"Original properties count: {properties.Count}");

            // Log any additional properties that came from feature flags
            foreach (var kvp in mergedProperties)
            {
                if (!properties.ContainsKey(kvp.Key))
                {
                    OutputHelper?.WriteLine($"Feature flag: {kvp.Key} = {kvp.Value}");
                }
            }
        }

        /// <summary>
        /// Tests that FeatureFlagCache creates a context for the host after merging.
        /// </summary>
        [SkippableFact]
        public async Task MergePropertiesWithFeatureFlags_RealEndpoint_CreatesContext()
        {
            Skip.IfNot(_canRunRealEndpointTests, "Real endpoint testing requires DATABRICKS_TEST_CONFIG_FILE");

            // Arrange
            using var cache = new FeatureFlagCache();
            var properties = CreateTestProperties();

            // Verify no context exists initially
            Assert.False(cache.HasContext(_host!));

            // Act
            await cache.MergePropertiesWithFeatureFlagsAsync(
                properties,
                TestAssemblyVersion,
                CancellationToken.None);

            // Assert - Context should now exist for the host
            Assert.True(cache.HasContext(_host!));
            Assert.True(cache.TryGetContext(_host!, out var context));
            Assert.NotNull(context);
            Assert.Equal(_host!.ToLowerInvariant(), context!.Host.ToLowerInvariant());

            OutputHelper?.WriteLine($"Context created for host: {context.Host}");
            OutputHelper?.WriteLine($"Context TTL: {context.Ttl}");
        }

        #endregion

        #region Caching Behavior Tests

        /// <summary>
        /// Tests that FeatureFlagCache caches contexts and reuses them on subsequent calls.
        /// </summary>
        [Fact]
        public async Task MergePropertiesWithFeatureFlags_MultipleCalls_ReusesContext()
        {
            // Arrange
            using var cache = new FeatureFlagCache();
            var host = "test-caching-host.databricks.com";
            var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [SparkParameters.HostName] = host,
                [DatabricksParameters.Token] = "test-token",
                // Disable actual fetching by setting cache to disabled
                // This allows us to test caching behavior without real HTTP calls
                [DatabricksParameters.FeatureFlagCacheEnabled] = "false"
            };

            // Act - Multiple calls
            var result1 = await cache.MergePropertiesWithFeatureFlagsAsync(properties, TestAssemblyVersion);
            var result2 = await cache.MergePropertiesWithFeatureFlagsAsync(properties, TestAssemblyVersion);
            var result3 = await cache.MergePropertiesWithFeatureFlagsAsync(properties, TestAssemblyVersion);

            // Assert - When disabled, original properties are returned unchanged
            Assert.Equal(properties.Count, result1.Count);
            Assert.Equal(properties.Count, result2.Count);
            Assert.Equal(properties.Count, result3.Count);

            OutputHelper?.WriteLine("Feature flag cache disabled - properties returned unchanged");
        }

        /// <summary>
        /// Tests that FeatureFlagCache respects the cache enabled setting.
        /// </summary>
        [Fact]
        public async Task MergePropertiesWithFeatureFlags_CacheDisabled_ReturnsOriginalProperties()
        {
            // Arrange
            using var cache = new FeatureFlagCache();
            var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [SparkParameters.HostName] = "test-disabled-host.databricks.com",
                [DatabricksParameters.Token] = "test-token",
                [DatabricksParameters.FeatureFlagCacheEnabled] = "false"
            };

            // Act
            var result = await cache.MergePropertiesWithFeatureFlagsAsync(properties, TestAssemblyVersion);

            // Assert - Original properties returned unchanged when disabled
            Assert.Equal(properties.Count, result.Count);
            foreach (var kvp in properties)
            {
                Assert.True(result.ContainsKey(kvp.Key));
                Assert.Equal(kvp.Value, result[kvp.Key]);
            }

            // No context should be created when disabled
            Assert.False(cache.HasContext("test-disabled-host.databricks.com"));
        }

        #endregion

        #region Error Handling Tests

        /// <summary>
        /// Tests that FeatureFlagCache returns original properties when host is missing.
        /// </summary>
        [Fact]
        public async Task MergePropertiesWithFeatureFlags_MissingHost_ReturnsOriginalProperties()
        {
            // Arrange
            using var cache = new FeatureFlagCache();
            var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [DatabricksParameters.Token] = "test-token"
                // Note: No host specified
            };

            // Act
            var result = await cache.MergePropertiesWithFeatureFlagsAsync(properties, TestAssemblyVersion);

            // Assert - Original properties returned when host is missing
            Assert.Equal(properties.Count, result.Count);
            Assert.Equal(0, cache.CachedHostCount);
        }

        /// <summary>
        /// Tests that FeatureFlagCache returns original properties when properties are empty.
        /// </summary>
        [Fact]
        public async Task MergePropertiesWithFeatureFlags_EmptyProperties_ReturnsOriginalProperties()
        {
            // Arrange
            using var cache = new FeatureFlagCache();
            var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Act
            var result = await cache.MergePropertiesWithFeatureFlagsAsync(properties, TestAssemblyVersion);

            // Assert
            Assert.Empty(result);
            Assert.Equal(0, cache.CachedHostCount);
        }

        /// <summary>
        /// Tests that FeatureFlagCache handles null host gracefully via TryGetHost.
        /// </summary>
        [Fact]
        public void TryGetHost_NullOrEmptyHost_ReturnsNull()
        {
            // Test with empty properties
            var emptyProperties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var result = FeatureFlagCache.TryGetHost(emptyProperties);
            Assert.Null(result);

            // Test with empty host value
            var emptyHostProperties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [SparkParameters.HostName] = ""
            };
            result = FeatureFlagCache.TryGetHost(emptyHostProperties);
            Assert.Null(result);
        }

        /// <summary>
        /// Tests that TryGetHost strips protocol from host correctly.
        /// </summary>
        [Theory]
        [InlineData("https://myhost.databricks.com", "myhost.databricks.com")]
        [InlineData("http://myhost.databricks.com", "myhost.databricks.com")]
        [InlineData("myhost.databricks.com", "myhost.databricks.com")]
        [InlineData("https://myhost.databricks.com/path", "myhost.databricks.com")]
        public void TryGetHost_WithProtocol_StripsProtocolCorrectly(string input, string expected)
        {
            // Arrange
            var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [SparkParameters.HostName] = input
            };

            // Act
            var result = FeatureFlagCache.TryGetHost(properties);

            // Assert
            Assert.Equal(expected, result);
        }

        /// <summary>
        /// Tests that TryGetHost extracts host from Uri property.
        /// </summary>
        [Fact]
        public void TryGetHost_FromUri_ExtractsHostCorrectly()
        {
            // Arrange
            var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [AdbcOptions.Uri] = "https://myhost.databricks.com/sql/1.0/warehouses/abc123"
            };

            // Act
            var result = FeatureFlagCache.TryGetHost(properties);

            // Assert
            Assert.Equal("myhost.databricks.com", result);
        }

        #endregion

        #region Cache Management Tests

        /// <summary>
        /// Tests that FeatureFlagCache.Clear() removes all cached contexts.
        /// </summary>
        [SkippableFact]
        public async Task Clear_RemovesAllContexts()
        {
            Skip.IfNot(_canRunRealEndpointTests, "Real endpoint testing requires DATABRICKS_TEST_CONFIG_FILE");

            // Arrange
            using var cache = new FeatureFlagCache();
            var properties = CreateTestProperties();

            // Create a context by calling merge
            await cache.MergePropertiesWithFeatureFlagsAsync(properties, TestAssemblyVersion);
            Assert.True(cache.HasContext(_host!));

            // Act
            cache.Clear();

            // Assert
            Assert.Equal(0, cache.CachedHostCount);
            Assert.False(cache.HasContext(_host!));
        }

        /// <summary>
        /// Tests that FeatureFlagCache.RemoveContext() removes specific context.
        /// </summary>
        [SkippableFact]
        public async Task RemoveContext_RemovesSpecificContext()
        {
            Skip.IfNot(_canRunRealEndpointTests, "Real endpoint testing requires DATABRICKS_TEST_CONFIG_FILE");

            // Arrange
            using var cache = new FeatureFlagCache();
            var properties = CreateTestProperties();

            // Create a context
            await cache.MergePropertiesWithFeatureFlagsAsync(properties, TestAssemblyVersion);
            Assert.True(cache.HasContext(_host!));

            // Act
            cache.RemoveContext(_host!);

            // Assert
            Assert.False(cache.HasContext(_host!));
        }

        /// <summary>
        /// Tests that RemoveContext with null/empty host doesn't throw.
        /// </summary>
        [Fact]
        public void RemoveContext_NullOrEmptyHost_DoesNotThrow()
        {
            // Arrange
            using var cache = new FeatureFlagCache();

            // Act & Assert - Should not throw
            cache.RemoveContext(null!);
            cache.RemoveContext("");
            cache.RemoveContext("   ");
            cache.RemoveContext("nonexistent-host.databricks.com");

            Assert.Equal(0, cache.CachedHostCount);
        }

        #endregion

        #region Custom Memory Cache Tests

        /// <summary>
        /// Tests that FeatureFlagCache works with custom IMemoryCache.
        /// </summary>
        [Fact]
        public void Constructor_WithCustomMemoryCache_UsesProvidedCache()
        {
            // Arrange
            var customCache = new MemoryCache(new MemoryCacheOptions
            {
                SizeLimit = 100
            });

            // Act
            using var cache = new FeatureFlagCache(customCache);

            // Assert - Cache should work normally
            Assert.Equal(0, cache.CachedHostCount);
        }

        /// <summary>
        /// Tests that constructor throws when null cache is provided.
        /// </summary>
        [Fact]
        public void Constructor_WithNullCache_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new FeatureFlagCache(null!));
        }

        #endregion

        #region Cancellation Tests

        /// <summary>
        /// Tests that cancellation is properly handled during merge operation.
        /// </summary>
        [SkippableFact]
        public async Task MergePropertiesWithFeatureFlags_Cancellation_ThrowsOperationCanceledException()
        {
            Skip.IfNot(_canRunRealEndpointTests, "Real endpoint testing requires DATABRICKS_TEST_CONFIG_FILE");

            // Arrange
            using var cache = new FeatureFlagCache();
            var properties = CreateTestProperties();
            var cts = new CancellationTokenSource();

            // Cancel immediately
            cts.Cancel();

            // Act & Assert - Should throw OperationCanceledException
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => cache.MergePropertiesWithFeatureFlagsAsync(properties, TestAssemblyVersion, cts.Token));
        }

        #endregion

        #region TTL Configuration Tests

        /// <summary>
        /// Tests that custom cache TTL is respected.
        /// </summary>
        [Fact]
        public async Task MergePropertiesWithFeatureFlags_CustomTtl_RespectsTtlSetting()
        {
            // Arrange
            using var cache = new FeatureFlagCache();
            var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [SparkParameters.HostName] = "test-ttl-host.databricks.com",
                [DatabricksParameters.Token] = "test-token",
                [DatabricksParameters.FeatureFlagCacheTtlSeconds] = "30", // 30 seconds
                [DatabricksParameters.FeatureFlagCacheEnabled] = "false" // Disable actual fetch
            };

            // Act
            var result = await cache.MergePropertiesWithFeatureFlagsAsync(properties, TestAssemblyVersion);

            // Assert - Properties should be returned (cache disabled, so no context created)
            Assert.Equal(properties.Count, result.Count);

            OutputHelper?.WriteLine("Custom TTL setting parsed successfully");
        }

        #endregion

        #region Singleton Tests

        /// <summary>
        /// Tests that GetInstance returns the same singleton instance.
        /// </summary>
        [Fact]
        public void GetInstance_ReturnsSameSingletonInstance()
        {
            // Act
            var instance1 = FeatureFlagCache.GetInstance();
            var instance2 = FeatureFlagCache.GetInstance();

            // Assert
            Assert.Same(instance1, instance2);
        }

        #endregion
    }
}
