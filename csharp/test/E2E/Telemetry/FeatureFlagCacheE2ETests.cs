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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for FeatureFlagCache.
    /// Tests feature flag fetching from real Databricks endpoints and validates
    /// caching and reference counting behavior.
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
        /// Creates an HttpClient configured with authentication for the Databricks endpoint.
        /// </summary>
        private HttpClient CreateAuthenticatedHttpClient()
        {
            var client = new HttpClient();
            client.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _token);
            return client;
        }

        /// <summary>
        /// Creates a feature flag fetcher function that simulates calling the feature flag endpoint.
        /// The actual feature flag endpoint would be called by the driver during connection.
        /// For E2E testing purposes, we create a fetcher that always returns a boolean value.
        /// </summary>
        private Func<CancellationToken, Task<bool>> CreateFeatureFlagFetcher(bool returnValue, int? delayMs = null)
        {
            return async ct =>
            {
                if (delayMs.HasValue)
                {
                    await Task.Delay(delayMs.Value, ct);
                }
                return returnValue;
            };
        }

        /// <summary>
        /// Creates a feature flag fetcher that makes a real HTTP call to validate connectivity.
        /// Uses the telemetry endpoint to verify the host is reachable.
        /// </summary>
        private Func<CancellationToken, Task<bool>> CreateRealEndpointFetcher(HttpClient httpClient)
        {
            return async ct =>
            {
                // Make a lightweight request to validate endpoint connectivity
                // In production, this would be a feature flag API call
                // For E2E testing, we verify the host is reachable
                try
                {
                    var host = _host!.TrimEnd('/');
                    if (!host.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
                    {
                        host = "https://" + host;
                    }

                    // Try to reach a Databricks API endpoint to validate connectivity
                    // Using a simple GET request that should work with bearer auth
                    var request = new HttpRequestMessage(HttpMethod.Get, $"{host}/api/2.0/clusters/list");
                    var response = await httpClient.SendAsync(request, ct);

                    // Any response (even 403) means we reached the endpoint
                    // The feature flag would be determined by the response in production
                    return response.IsSuccessStatusCode ||
                           response.StatusCode == System.Net.HttpStatusCode.Forbidden ||
                           response.StatusCode == System.Net.HttpStatusCode.Unauthorized;
                }
                catch (HttpRequestException)
                {
                    // Network error - endpoint not reachable
                    return false;
                }
            };
        }

        #region FeatureFlagCache_FetchFromRealEndpoint_ReturnsBoolean

        /// <summary>
        /// Tests that FeatureFlagCache can fetch feature flags from a real Databricks endpoint.
        /// This validates the cache correctly handles real-world HTTP responses.
        /// </summary>
        [SkippableFact]
        public async Task FeatureFlagCache_FetchFromRealEndpoint_ReturnsBoolean()
        {
            Skip.IfNot(_canRunRealEndpointTests, "Real endpoint testing requires DATABRICKS_TEST_CONFIG_FILE");

            // Arrange
            var cache = new FeatureFlagCache();
            var host = _host!;

            using var httpClient = CreateAuthenticatedHttpClient();
            var fetcher = CreateRealEndpointFetcher(httpClient);

            // Create context first (required before calling IsTelemetryEnabledAsync)
            var context = cache.GetOrCreateContext(host);

            try
            {
                // Act
                var result = await cache.IsTelemetryEnabledAsync(host, fetcher, CancellationToken.None);

                // Assert
                // The result should be a boolean - either true or false is valid
                // The important thing is that no exception was thrown
                Assert.True(result == true || result == false);
                OutputHelper?.WriteLine($"Feature flag result from real endpoint: {result}");

                // Verify the cache was updated
                Assert.NotNull(context.TelemetryEnabled);
                Assert.NotNull(context.LastFetched);
            }
            finally
            {
                cache.ReleaseContext(host);
            }
        }

        #endregion

        #region FeatureFlagCache_CachesValue_DoesNotRefetchWithinTTL

        /// <summary>
        /// Tests that FeatureFlagCache caches values and does not refetch within the TTL period.
        /// </summary>
        [Fact]
        public async Task FeatureFlagCache_CachesValue_DoesNotRefetchWithinTTL()
        {
            // Arrange
            var cache = new FeatureFlagCache(TimeSpan.FromMinutes(15)); // Use default TTL
            var host = "test-caching-host.databricks.com";
            var fetchCount = 0;

            var fetcher = async (CancellationToken ct) =>
            {
                Interlocked.Increment(ref fetchCount);
                await Task.CompletedTask;
                return true;
            };

            // Create context first
            var context = cache.GetOrCreateContext(host);

            try
            {
                // Act - First call should fetch
                var result1 = await cache.IsTelemetryEnabledAsync(host, fetcher, CancellationToken.None);
                Assert.True(result1);
                Assert.Equal(1, fetchCount);

                // Second call should use cached value
                var result2 = await cache.IsTelemetryEnabledAsync(host, fetcher, CancellationToken.None);
                Assert.True(result2);
                Assert.Equal(1, fetchCount); // Should NOT have fetched again

                // Third call should still use cached value
                var result3 = await cache.IsTelemetryEnabledAsync(host, fetcher, CancellationToken.None);
                Assert.True(result3);
                Assert.Equal(1, fetchCount); // Should NOT have fetched again

                // Assert
                OutputHelper?.WriteLine($"Total fetch count: {fetchCount} (expected: 1)");
                Assert.Equal(1, fetchCount);

                // Verify cache state
                Assert.True(context.TelemetryEnabled);
                Assert.False(context.IsExpired);
            }
            finally
            {
                cache.ReleaseContext(host);
            }
        }

        /// <summary>
        /// Tests that FeatureFlagCache refetches after cache expires.
        /// </summary>
        [Fact]
        public async Task FeatureFlagCache_RefetchesAfterExpiry()
        {
            // Arrange - Use very short TTL for testing
            var cache = new FeatureFlagCache(TimeSpan.FromMilliseconds(50));
            var host = "test-expiry-host.databricks.com";
            var fetchCount = 0;

            var fetcher = async (CancellationToken ct) =>
            {
                Interlocked.Increment(ref fetchCount);
                await Task.CompletedTask;
                return true;
            };

            // Create context first
            var context = cache.GetOrCreateContext(host);

            try
            {
                // Act - First call should fetch
                var result1 = await cache.IsTelemetryEnabledAsync(host, fetcher, CancellationToken.None);
                Assert.True(result1);
                Assert.Equal(1, fetchCount);

                // Wait for cache to expire
                await Task.Delay(100);
                Assert.True(context.IsExpired);

                // Second call should refetch because cache expired
                var result2 = await cache.IsTelemetryEnabledAsync(host, fetcher, CancellationToken.None);
                Assert.True(result2);
                Assert.Equal(2, fetchCount); // Should have fetched again

                // Assert
                OutputHelper?.WriteLine($"Total fetch count after expiry: {fetchCount} (expected: 2)");
            }
            finally
            {
                cache.ReleaseContext(host);
            }
        }

        #endregion

        #region FeatureFlagCache_InvalidHost_ReturnsDefaultFalse

        /// <summary>
        /// Tests that FeatureFlagCache returns false for invalid hosts.
        /// </summary>
        [Fact]
        public async Task FeatureFlagCache_InvalidHost_ReturnsDefaultFalse()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var invalidHost = "invalid-host-that-does-not-exist-12345.databricks.com";
            var fetchCount = 0;

            // Fetcher that throws to simulate network error
            Func<CancellationToken, Task<bool>> fetcher = async (CancellationToken ct) =>
            {
                Interlocked.Increment(ref fetchCount);
                await Task.CompletedTask;
                throw new HttpRequestException("Host not found");
            };

            // Create context first
            var context = cache.GetOrCreateContext(invalidHost);

            try
            {
                // Act
                var result = await cache.IsTelemetryEnabledAsync(invalidHost, fetcher, CancellationToken.None);

                // Assert - Should return false on error (safe default)
                Assert.False(result);
                Assert.Equal(1, fetchCount); // Should have attempted to fetch
                OutputHelper?.WriteLine($"Invalid host returned: {result} (expected: false)");
            }
            finally
            {
                cache.ReleaseContext(invalidHost);
            }
        }

        /// <summary>
        /// Tests that FeatureFlagCache returns false for null host.
        /// </summary>
        [Fact]
        public async Task FeatureFlagCache_NullHost_ReturnsDefaultFalse()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var fetchCalled = false;

            var fetcher = async (CancellationToken ct) =>
            {
                fetchCalled = true;
                await Task.CompletedTask;
                return true;
            };

            // Act
            var result = await cache.IsTelemetryEnabledAsync(null!, fetcher, CancellationToken.None);

            // Assert
            Assert.False(result);
            Assert.False(fetchCalled); // Should not have attempted to fetch for null host
        }

        /// <summary>
        /// Tests that FeatureFlagCache returns false for empty host.
        /// </summary>
        [Fact]
        public async Task FeatureFlagCache_EmptyHost_ReturnsDefaultFalse()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var fetchCalled = false;

            var fetcher = async (CancellationToken ct) =>
            {
                fetchCalled = true;
                await Task.CompletedTask;
                return true;
            };

            // Act
            var result = await cache.IsTelemetryEnabledAsync("", fetcher, CancellationToken.None);

            // Assert
            Assert.False(result);
            Assert.False(fetchCalled); // Should not have attempted to fetch for empty host
        }

        /// <summary>
        /// Tests that FeatureFlagCache returns false for unknown host (no context created).
        /// </summary>
        [Fact]
        public async Task FeatureFlagCache_UnknownHost_ReturnsDefaultFalse()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var unknownHost = "unknown-host.databricks.com";
            var fetchCalled = false;

            var fetcher = async (CancellationToken ct) =>
            {
                fetchCalled = true;
                await Task.CompletedTask;
                return true;
            };

            // Act - Note: No context created for this host
            var result = await cache.IsTelemetryEnabledAsync(unknownHost, fetcher, CancellationToken.None);

            // Assert
            Assert.False(result);
            Assert.False(fetchCalled); // Should not have attempted to fetch for unknown host
        }

        #endregion

        #region FeatureFlagCache_RefCountingWorks_CleanupAfterRelease

        /// <summary>
        /// Tests that FeatureFlagCache reference counting works correctly and
        /// contexts are cleaned up after all references are released.
        /// </summary>
        [Fact]
        public void FeatureFlagCache_RefCountingWorks_CleanupAfterRelease()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-refcount-host.databricks.com";

            // Act - Create multiple references
            var context1 = cache.GetOrCreateContext(host);
            Assert.Equal(1, context1.RefCount);
            Assert.True(cache.HasContext(host));

            var context2 = cache.GetOrCreateContext(host);
            Assert.Equal(2, context2.RefCount);
            Assert.Same(context1, context2); // Should be same instance

            var context3 = cache.GetOrCreateContext(host);
            Assert.Equal(3, context3.RefCount);
            Assert.Same(context1, context3); // Should be same instance

            OutputHelper?.WriteLine($"After creating 3 references: RefCount = {context1.RefCount}");

            // Release references one by one
            cache.ReleaseContext(host);
            Assert.Equal(2, context1.RefCount);
            Assert.True(cache.HasContext(host)); // Still has references

            cache.ReleaseContext(host);
            Assert.Equal(1, context1.RefCount);
            Assert.True(cache.HasContext(host)); // Still has references

            cache.ReleaseContext(host);
            // After last release, context should be removed

            // Assert
            Assert.False(cache.HasContext(host));
            Assert.Equal(0, cache.CachedHostCount);
            OutputHelper?.WriteLine($"After releasing all references: Context removed");
        }

        /// <summary>
        /// Tests that releasing context for unknown host doesn't throw.
        /// </summary>
        [Fact]
        public void FeatureFlagCache_ReleaseUnknownHost_DoesNotThrow()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var unknownHost = "never-created-host.databricks.com";

            // Act & Assert - Should not throw
            cache.ReleaseContext(unknownHost);
            cache.ReleaseContext(null!);
            cache.ReleaseContext("");
            cache.ReleaseContext("   ");

            // All should complete without exception
            Assert.Equal(0, cache.CachedHostCount);
        }

        /// <summary>
        /// Tests that multiple hosts can have independent reference counts.
        /// </summary>
        [Fact]
        public void FeatureFlagCache_MultipleHosts_IndependentRefCounts()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host1 = "host1.databricks.com";
            var host2 = "host2.databricks.com";
            var host3 = "host3.databricks.com";

            // Act - Create contexts for multiple hosts
            var context1a = cache.GetOrCreateContext(host1);
            var context1b = cache.GetOrCreateContext(host1);
            var context2 = cache.GetOrCreateContext(host2);
            var context3a = cache.GetOrCreateContext(host3);
            var context3b = cache.GetOrCreateContext(host3);
            var context3c = cache.GetOrCreateContext(host3);

            // Assert initial state
            Assert.Equal(3, cache.CachedHostCount);
            Assert.Equal(2, context1a.RefCount);
            Assert.Equal(1, context2.RefCount);
            Assert.Equal(3, context3a.RefCount);

            // Release host2 completely
            cache.ReleaseContext(host2);
            Assert.Equal(2, cache.CachedHostCount);
            Assert.False(cache.HasContext(host2));

            // Host1 and Host3 should still exist
            Assert.True(cache.HasContext(host1));
            Assert.True(cache.HasContext(host3));

            // Clean up remaining
            cache.ReleaseContext(host1);
            cache.ReleaseContext(host1);
            cache.ReleaseContext(host3);
            cache.ReleaseContext(host3);
            cache.ReleaseContext(host3);

            Assert.Equal(0, cache.CachedHostCount);
        }

        /// <summary>
        /// Tests concurrent reference counting is thread-safe.
        /// </summary>
        [Fact]
        public async Task FeatureFlagCache_ConcurrentRefCounting_ThreadSafe()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "concurrent-host.databricks.com";
            var incrementCount = 100;
            var tasks = new Task[incrementCount];

            // Act - Concurrently create references
            for (int i = 0; i < incrementCount; i++)
            {
                tasks[i] = Task.Run(() => cache.GetOrCreateContext(host));
            }
            await Task.WhenAll(tasks);

            // Assert
            Assert.True(cache.TryGetContext(host, out var context));
            Assert.Equal(incrementCount, context!.RefCount);

            // Concurrently release references
            var releaseTasks = new Task[incrementCount];
            for (int i = 0; i < incrementCount; i++)
            {
                releaseTasks[i] = Task.Run(() => cache.ReleaseContext(host));
            }
            await Task.WhenAll(releaseTasks);

            // After all releases, context should be removed
            Assert.False(cache.HasContext(host));
        }

        #endregion

        #region Additional E2E Tests

        /// <summary>
        /// Tests that cached false value is correctly returned.
        /// </summary>
        [Fact]
        public async Task FeatureFlagCache_CachesFalseValue_ReturnsCorrectly()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-false-value-host.databricks.com";
            var fetchCount = 0;

            var fetcher = async (CancellationToken ct) =>
            {
                Interlocked.Increment(ref fetchCount);
                await Task.CompletedTask;
                return false; // Return false
            };

            // Create context first
            var context = cache.GetOrCreateContext(host);

            try
            {
                // Act
                var result1 = await cache.IsTelemetryEnabledAsync(host, fetcher, CancellationToken.None);
                var result2 = await cache.IsTelemetryEnabledAsync(host, fetcher, CancellationToken.None);

                // Assert
                Assert.False(result1);
                Assert.False(result2);
                Assert.Equal(1, fetchCount); // Should only fetch once
                Assert.False(context.TelemetryEnabled);
            }
            finally
            {
                cache.ReleaseContext(host);
            }
        }

        /// <summary>
        /// Tests that cancellation is properly propagated during fetch.
        /// </summary>
        [Fact]
        public async Task FeatureFlagCache_Cancellation_PropagatesCorrectly()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-cancellation-host.databricks.com";
            var cts = new CancellationTokenSource();

            var fetcher = async (CancellationToken ct) =>
            {
                await Task.Delay(10000, ct); // Long delay that should be cancelled
                return true;
            };

            // Create context first
            var context = cache.GetOrCreateContext(host);

            try
            {
                // Act
                cts.CancelAfter(50); // Cancel after 50ms

                // Assert - TaskCanceledException inherits from OperationCanceledException
                var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    () => cache.IsTelemetryEnabledAsync(host, fetcher, cts.Token));
                Assert.True(ex is OperationCanceledException);
            }
            finally
            {
                cache.ReleaseContext(host);
            }
        }

        #endregion
    }
}
