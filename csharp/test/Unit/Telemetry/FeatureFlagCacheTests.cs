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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for FeatureFlagCache and FeatureFlagContext classes.
    /// </summary>
    public class FeatureFlagCacheTests
    {
        #region FeatureFlagContext Tests

        [Fact]
        public void FeatureFlagContext_DefaultConstructor_SetsDefaultCacheDuration()
        {
            // Arrange & Act
            var context = new FeatureFlagContext();

            // Assert
            Assert.Equal(TimeSpan.FromMinutes(15), context.CacheDuration);
            Assert.Equal(0, context.RefCount);
            Assert.Null(context.TelemetryEnabled);
            Assert.Null(context.LastFetched);
            Assert.True(context.IsExpired);
        }

        [Fact]
        public void FeatureFlagContext_CustomCacheDuration_SetsCorrectly()
        {
            // Arrange & Act
            var duration = TimeSpan.FromMinutes(30);
            var context = new FeatureFlagContext(duration);

            // Assert
            Assert.Equal(duration, context.CacheDuration);
        }

        [Fact]
        public void FeatureFlagContext_ZeroCacheDuration_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => new FeatureFlagContext(TimeSpan.Zero));
        }

        [Fact]
        public void FeatureFlagContext_NegativeCacheDuration_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => new FeatureFlagContext(TimeSpan.FromMinutes(-5)));
        }

        [Fact]
        public void FeatureFlagContext_SetTelemetryEnabled_UpdatesCachedValue()
        {
            // Arrange
            var context = new FeatureFlagContext();

            // Act
            context.SetTelemetryEnabled(true);

            // Assert
            Assert.True(context.TelemetryEnabled);
            Assert.NotNull(context.LastFetched);
            Assert.False(context.IsExpired);
        }

        [Fact]
        public void FeatureFlagContext_SetTelemetryEnabled_False_UpdatesCachedValue()
        {
            // Arrange
            var context = new FeatureFlagContext();

            // Act
            context.SetTelemetryEnabled(false);

            // Assert
            Assert.False(context.TelemetryEnabled);
            Assert.NotNull(context.LastFetched);
            Assert.False(context.IsExpired);
        }

        [Fact]
        public void FeatureFlagContext_TryGetCachedValue_NoCache_ReturnsFalse()
        {
            // Arrange
            var context = new FeatureFlagContext();

            // Act
            var result = context.TryGetCachedValue(out var value);

            // Assert
            Assert.False(result);
            Assert.False(value);
        }

        [Fact]
        public void FeatureFlagContext_TryGetCachedValue_WithValidCache_ReturnsTrue()
        {
            // Arrange
            var context = new FeatureFlagContext();
            context.SetTelemetryEnabled(true);

            // Act
            var result = context.TryGetCachedValue(out var value);

            // Assert
            Assert.True(result);
            Assert.True(value);
        }

        [Fact]
        public void FeatureFlagContext_TryGetCachedValue_ExpiredCache_ReturnsFalse()
        {
            // Arrange - use very short cache duration
            var context = new FeatureFlagContext(TimeSpan.FromMilliseconds(1));
            context.SetTelemetryEnabled(true);

            // Wait for cache to expire
            Thread.Sleep(10);

            // Act
            var result = context.TryGetCachedValue(out var value);

            // Assert
            Assert.False(result);
            Assert.False(value);
        }

        [Fact]
        public void FeatureFlagContext_IsExpired_NoCache_ReturnsTrue()
        {
            // Arrange
            var context = new FeatureFlagContext();

            // Act & Assert
            Assert.True(context.IsExpired);
        }

        [Fact]
        public void FeatureFlagContext_IsExpired_ValidCache_ReturnsFalse()
        {
            // Arrange
            var context = new FeatureFlagContext();
            context.SetTelemetryEnabled(true);

            // Act & Assert
            Assert.False(context.IsExpired);
        }

        [Fact]
        public void FeatureFlagContext_IsExpired_ExpiredCache_ReturnsTrue()
        {
            // Arrange - use very short cache duration
            var context = new FeatureFlagContext(TimeSpan.FromMilliseconds(1));
            context.SetTelemetryEnabled(true);

            // Wait for cache to expire
            Thread.Sleep(10);

            // Act & Assert
            Assert.True(context.IsExpired);
        }

        [Fact]
        public void FeatureFlagContext_IncrementRefCount_IncrementsCorrectly()
        {
            // Arrange
            var context = new FeatureFlagContext();

            // Act & Assert
            Assert.Equal(0, context.RefCount);
            Assert.Equal(1, context.IncrementRefCount());
            Assert.Equal(1, context.RefCount);
            Assert.Equal(2, context.IncrementRefCount());
            Assert.Equal(2, context.RefCount);
        }

        [Fact]
        public void FeatureFlagContext_DecrementRefCount_DecrementsCorrectly()
        {
            // Arrange
            var context = new FeatureFlagContext();
            context.IncrementRefCount();
            context.IncrementRefCount();

            // Act & Assert
            Assert.Equal(2, context.RefCount);
            Assert.Equal(1, context.DecrementRefCount());
            Assert.Equal(1, context.RefCount);
            Assert.Equal(0, context.DecrementRefCount());
            Assert.Equal(0, context.RefCount);
        }

        [Fact]
        public void FeatureFlagContext_ResetCache_ClearsCache()
        {
            // Arrange
            var context = new FeatureFlagContext();
            context.SetTelemetryEnabled(true);
            context.IncrementRefCount();

            // Act
            context.ResetCache();

            // Assert
            Assert.Null(context.TelemetryEnabled);
            Assert.Null(context.LastFetched);
            Assert.True(context.IsExpired);
            // RefCount should not be affected
            Assert.Equal(1, context.RefCount);
        }

        #endregion

        #region FeatureFlagCache Singleton Tests

        [Fact]
        public void FeatureFlagCache_GetInstance_ReturnsSingleton()
        {
            // Act
            var instance1 = FeatureFlagCache.GetInstance();
            var instance2 = FeatureFlagCache.GetInstance();

            // Assert
            Assert.Same(instance1, instance2);
        }

        #endregion

        #region FeatureFlagCache_GetOrCreateContext Tests

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_NewHost_CreatesContext()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-1.databricks.com";

            // Act
            var context = cache.GetOrCreateContext(host);

            // Assert
            Assert.NotNull(context);
            Assert.Equal(1, context.RefCount);
            Assert.True(cache.HasContext(host));
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_ExistingHost_IncrementsRefCount()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-2.databricks.com";

            // Act
            var context1 = cache.GetOrCreateContext(host);
            var context2 = cache.GetOrCreateContext(host);

            // Assert
            Assert.Same(context1, context2);
            Assert.Equal(2, context1.RefCount);
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_MultipleHosts_CreatesMultipleContexts()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host1 = "host1.databricks.com";
            var host2 = "host2.databricks.com";

            // Act
            var context1 = cache.GetOrCreateContext(host1);
            var context2 = cache.GetOrCreateContext(host2);

            // Assert
            Assert.NotSame(context1, context2);
            Assert.Equal(1, context1.RefCount);
            Assert.Equal(1, context2.RefCount);
            Assert.Equal(2, cache.CachedHostCount);
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_NullHost_ThrowsException()
        {
            // Arrange
            var cache = new FeatureFlagCache();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => cache.GetOrCreateContext(null!));
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_EmptyHost_ThrowsException()
        {
            // Arrange
            var cache = new FeatureFlagCache();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => cache.GetOrCreateContext(""));
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_WhitespaceHost_ThrowsException()
        {
            // Arrange
            var cache = new FeatureFlagCache();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => cache.GetOrCreateContext("   "));
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_CaseInsensitive()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "Test-Host.Databricks.com";

            // Act
            var context1 = cache.GetOrCreateContext(host.ToLower());
            var context2 = cache.GetOrCreateContext(host.ToUpper());

            // Assert
            Assert.Same(context1, context2);
            Assert.Equal(2, context1.RefCount);
            Assert.Equal(1, cache.CachedHostCount);
        }

        #endregion

        #region FeatureFlagCache_ReleaseContext Tests

        [Fact]
        public void FeatureFlagCache_ReleaseContext_LastReference_RemovesContext()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-3.databricks.com";
            var context = cache.GetOrCreateContext(host);
            Assert.Equal(1, context.RefCount);

            // Act
            cache.ReleaseContext(host);

            // Assert
            Assert.False(cache.HasContext(host));
            Assert.Equal(0, cache.CachedHostCount);
        }

        [Fact]
        public void FeatureFlagCache_ReleaseContext_MultipleReferences_DecrementsOnly()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-4.databricks.com";
            var context = cache.GetOrCreateContext(host);
            cache.GetOrCreateContext(host); // Second reference
            Assert.Equal(2, context.RefCount);

            // Act
            cache.ReleaseContext(host);

            // Assert
            Assert.True(cache.HasContext(host));
            Assert.Equal(1, context.RefCount);
        }

        [Fact]
        public void FeatureFlagCache_ReleaseContext_UnknownHost_DoesNothing()
        {
            // Arrange
            var cache = new FeatureFlagCache();

            // Act - should not throw
            cache.ReleaseContext("unknown-host.databricks.com");

            // Assert
            Assert.Equal(0, cache.CachedHostCount);
        }

        [Fact]
        public void FeatureFlagCache_ReleaseContext_NullHost_DoesNothing()
        {
            // Arrange
            var cache = new FeatureFlagCache();

            // Act - should not throw
            cache.ReleaseContext(null!);

            // Assert - no exception thrown
        }

        [Fact]
        public void FeatureFlagCache_ReleaseContext_EmptyHost_DoesNothing()
        {
            // Arrange
            var cache = new FeatureFlagCache();

            // Act - should not throw
            cache.ReleaseContext("");

            // Assert - no exception thrown
        }

        [Fact]
        public void FeatureFlagCache_ReleaseContext_AllReleased_RemovesContext()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-5.databricks.com";

            // Create 3 references
            cache.GetOrCreateContext(host);
            cache.GetOrCreateContext(host);
            cache.GetOrCreateContext(host);
            Assert.Equal(1, cache.CachedHostCount);

            // Act - Release all
            cache.ReleaseContext(host);
            Assert.True(cache.HasContext(host)); // Still has 2 references

            cache.ReleaseContext(host);
            Assert.True(cache.HasContext(host)); // Still has 1 reference

            cache.ReleaseContext(host);

            // Assert
            Assert.False(cache.HasContext(host));
            Assert.Equal(0, cache.CachedHostCount);
        }

        #endregion

        #region FeatureFlagCache_IsTelemetryEnabledAsync Tests

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_CachedValue_DoesNotFetch()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-6.databricks.com";
            var fetchCount = 0;
            var context = cache.GetOrCreateContext(host);
            context.SetTelemetryEnabled(true);

            // Act
            var result = await cache.IsTelemetryEnabledAsync(
                host,
                async ct =>
                {
                    fetchCount++;
                    await Task.CompletedTask;
                    return false; // Different value from cached
                });

            // Assert
            Assert.True(result); // Should return cached value
            Assert.Equal(0, fetchCount); // Should not have fetched
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_ExpiredCache_RefetchesValue()
        {
            // Arrange
            var cache = new FeatureFlagCache(TimeSpan.FromMilliseconds(1));
            var host = "test-host-7.databricks.com";
            var fetchCount = 0;
            var context = cache.GetOrCreateContext(host);
            context.SetTelemetryEnabled(false);

            // Wait for cache to expire
            await Task.Delay(10);

            // Act
            var result = await cache.IsTelemetryEnabledAsync(
                host,
                async ct =>
                {
                    fetchCount++;
                    await Task.CompletedTask;
                    return true; // New value
                });

            // Assert
            Assert.True(result); // Should return new fetched value
            Assert.Equal(1, fetchCount); // Should have fetched once
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_NoCache_Fetches()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-8.databricks.com";
            var fetchCount = 0;
            cache.GetOrCreateContext(host); // Create context but don't set value

            // Act
            var result = await cache.IsTelemetryEnabledAsync(
                host,
                async ct =>
                {
                    fetchCount++;
                    await Task.CompletedTask;
                    return true;
                });

            // Assert
            Assert.True(result);
            Assert.Equal(1, fetchCount);
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_FetcherThrows_ReturnsFalse()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-9.databricks.com";
            cache.GetOrCreateContext(host);

            // Act
            var result = await cache.IsTelemetryEnabledAsync(
                host,
                ct => throw new InvalidOperationException("Fetch failed"));

            // Assert
            Assert.False(result); // Should return false on error
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_Cancellation_Propagates()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-10.databricks.com";
            cache.GetOrCreateContext(host);
            var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act & Assert
            await Assert.ThrowsAsync<OperationCanceledException>(
                () => cache.IsTelemetryEnabledAsync(
                    host,
                    async ct =>
                    {
                        ct.ThrowIfCancellationRequested();
                        await Task.CompletedTask;
                        return true;
                    },
                    cts.Token));
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_UnknownHost_ReturnsFalse()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var fetchCount = 0;

            // Act
            var result = await cache.IsTelemetryEnabledAsync(
                "unknown-host.databricks.com",
                async ct =>
                {
                    fetchCount++;
                    await Task.CompletedTask;
                    return true;
                });

            // Assert
            Assert.False(result);
            Assert.Equal(0, fetchCount); // Should not have fetched for unknown host
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_NullHost_ReturnsFalse()
        {
            // Arrange
            var cache = new FeatureFlagCache();

            // Act
            var result = await cache.IsTelemetryEnabledAsync(
                null!,
                ct => Task.FromResult(true));

            // Assert
            Assert.False(result);
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_NullFetcher_ReturnsFalse()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-11.databricks.com";
            cache.GetOrCreateContext(host);

            // Act
            var result = await cache.IsTelemetryEnabledAsync(host, null!);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_UpdatesCache()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-12.databricks.com";
            var context = cache.GetOrCreateContext(host);

            // Act
            await cache.IsTelemetryEnabledAsync(
                host,
                ct => Task.FromResult(true));

            // Assert
            Assert.True(context.TelemetryEnabled);
            Assert.NotNull(context.LastFetched);
            Assert.False(context.IsExpired);
        }

        #endregion

        #region FeatureFlagCache Thread Safety Tests

        [Fact]
        public async Task FeatureFlagCache_ConcurrentGetOrCreateContext_ThreadSafe()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "concurrent-host.databricks.com";
            var tasks = new Task<FeatureFlagContext>[100];

            // Act
            for (int i = 0; i < 100; i++)
            {
                tasks[i] = Task.Run(() => cache.GetOrCreateContext(host));
            }

            var contexts = await Task.WhenAll(tasks);

            // Assert - All should be the same context
            var firstContext = contexts[0];
            Assert.All(contexts, ctx => Assert.Same(firstContext, ctx));
            Assert.Equal(100, firstContext.RefCount);
        }

        [Fact]
        public async Task FeatureFlagCache_ConcurrentReleaseContext_ThreadSafe()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "concurrent-release-host.databricks.com";

            // Create 100 references
            for (int i = 0; i < 100; i++)
            {
                cache.GetOrCreateContext(host);
            }

            var tasks = new Task[100];

            // Act - Release all concurrently
            for (int i = 0; i < 100; i++)
            {
                tasks[i] = Task.Run(() => cache.ReleaseContext(host));
            }

            await Task.WhenAll(tasks);

            // Assert - Context should be removed
            Assert.False(cache.HasContext(host));
        }

        [Fact]
        public async Task FeatureFlagCache_ConcurrentIsTelemetryEnabled_ThreadSafe()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "concurrent-fetch-host.databricks.com";
            var fetchCount = 0;
            cache.GetOrCreateContext(host);

            var tasks = new Task<bool>[100];

            // Act
            for (int i = 0; i < 100; i++)
            {
                tasks[i] = cache.IsTelemetryEnabledAsync(
                    host,
                    async ct =>
                    {
                        Interlocked.Increment(ref fetchCount);
                        await Task.Delay(1); // Small delay to increase contention
                        return true;
                    });
            }

            var results = await Task.WhenAll(tasks);

            // Assert - All results should be true
            Assert.All(results, r => Assert.True(r));
            // Multiple fetches may occur due to race conditions, but that's OK
            // The important thing is no exceptions and correct results
        }

        #endregion

        #region FeatureFlagCache Helper Method Tests

        [Fact]
        public void FeatureFlagCache_TryGetContext_ExistingContext_ReturnsTrue()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "try-get-host.databricks.com";
            var expectedContext = cache.GetOrCreateContext(host);

            // Act
            var result = cache.TryGetContext(host, out var context);

            // Assert
            Assert.True(result);
            Assert.Same(expectedContext, context);
        }

        [Fact]
        public void FeatureFlagCache_TryGetContext_UnknownHost_ReturnsFalse()
        {
            // Arrange
            var cache = new FeatureFlagCache();

            // Act
            var result = cache.TryGetContext("unknown.databricks.com", out var context);

            // Assert
            Assert.False(result);
            Assert.Null(context);
        }

        [Fact]
        public void FeatureFlagCache_TryGetContext_NullHost_ReturnsFalse()
        {
            // Arrange
            var cache = new FeatureFlagCache();

            // Act
            var result = cache.TryGetContext(null!, out var context);

            // Assert
            Assert.False(result);
            Assert.Null(context);
        }

        [Fact]
        public void FeatureFlagCache_Clear_RemovesAllContexts()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            cache.GetOrCreateContext("host1.databricks.com");
            cache.GetOrCreateContext("host2.databricks.com");
            cache.GetOrCreateContext("host3.databricks.com");
            Assert.Equal(3, cache.CachedHostCount);

            // Act
            cache.Clear();

            // Assert
            Assert.Equal(0, cache.CachedHostCount);
        }

        [Fact]
        public void FeatureFlagCache_Constructor_InvalidCacheDuration_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => new FeatureFlagCache(TimeSpan.Zero));
            Assert.Throws<ArgumentOutOfRangeException>(() => new FeatureFlagCache(TimeSpan.FromMinutes(-1)));
        }

        #endregion
    }
}
