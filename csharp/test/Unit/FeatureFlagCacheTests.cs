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
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks;
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Tests for FeatureFlagCache and FeatureFlagContext classes.
    /// </summary>
    public class FeatureFlagCacheTests
    {
        private const string TestHost = "test-host.databricks.com";
        private const string DriverVersion = "1.0.0";

        #region FeatureFlagContext Tests - Basic Functionality

        [Fact]
        public void FeatureFlagContext_GetFlagValue_ReturnsValue()
        {
            // Arrange
            var flags = new Dictionary<string, string>
            {
                ["flag1"] = "value1",
                ["flag2"] = "value2"
            };
            var context = CreateTestContext(flags);

            // Act & Assert
            Assert.Equal("value1", context.GetFlagValue("flag1"));
            Assert.Equal("value2", context.GetFlagValue("flag2"));
        }

        [Fact]
        public void FeatureFlagContext_GetFlagValue_NotFound_ReturnsNull()
        {
            // Arrange
            var context = CreateTestContext();

            // Act & Assert
            Assert.Null(context.GetFlagValue("nonexistent"));
        }

        [Fact]
        public void FeatureFlagContext_GetFlagValue_NullOrEmpty_ReturnsNull()
        {
            // Arrange
            var context = CreateTestContext();

            // Act & Assert
            Assert.Null(context.GetFlagValue(null!));
            Assert.Null(context.GetFlagValue(""));
            Assert.Null(context.GetFlagValue("   "));
        }

        [Fact]
        public void FeatureFlagContext_GetFlagValue_CaseInsensitive()
        {
            // Arrange
            var flags = new Dictionary<string, string>
            {
                ["MyFlag"] = "value"
            };
            var context = CreateTestContext(flags);

            // Act & Assert
            Assert.Equal("value", context.GetFlagValue("myflag"));
            Assert.Equal("value", context.GetFlagValue("MYFLAG"));
            Assert.Equal("value", context.GetFlagValue("MyFlag"));
        }

        [Fact]
        public void FeatureFlagContext_GetAllFlags_ReturnsAllFlags()
        {
            // Arrange
            var flags = new Dictionary<string, string>
            {
                ["flag1"] = "value1",
                ["flag2"] = "value2",
                ["flag3"] = "value3"
            };
            var context = CreateTestContext(flags);

            // Act
            var allFlags = context.GetAllFlags();

            // Assert
            Assert.Equal(3, allFlags.Count);
            Assert.Equal("value1", allFlags["flag1"]);
            Assert.Equal("value2", allFlags["flag2"]);
            Assert.Equal("value3", allFlags["flag3"]);
        }

        [Fact]
        public void FeatureFlagContext_GetAllFlags_ReturnsSnapshot()
        {
            // Arrange
            var context = CreateTestContext();
            context.SetFlag("flag1", "value1");

            // Act
            var snapshot = context.GetAllFlags();
            context.SetFlag("flag2", "value2");

            // Assert - snapshot should not include new flag
            Assert.Single(snapshot);
            Assert.Equal("value1", snapshot["flag1"]);
        }

        [Fact]
        public void FeatureFlagContext_GetAllFlags_Empty_ReturnsEmptyDictionary()
        {
            // Arrange
            var context = CreateTestContext();

            // Act
            var allFlags = context.GetAllFlags();

            // Assert
            Assert.Empty(allFlags);
        }

        #endregion

        #region FeatureFlagContext Tests - TTL

        [Fact]
        public void FeatureFlagContext_DefaultTtl_Is15Minutes()
        {
            // Arrange
            var context = CreateTestContext();

            // Assert
            Assert.Equal(TimeSpan.FromMinutes(15), context.Ttl);
            Assert.Equal(TimeSpan.FromMinutes(15), context.RefreshInterval); // Alias
        }

        [Fact]
        public void FeatureFlagContext_CustomTtl()
        {
            // Arrange
            var customTtl = TimeSpan.FromMinutes(5);
            var context = CreateTestContext(null, customTtl);

            // Assert
            Assert.Equal(customTtl, context.Ttl);
            Assert.Equal(customTtl, context.RefreshInterval);
        }

        #endregion

        #region FeatureFlagContext Tests - Dispose

        [Fact]
        public void FeatureFlagContext_Dispose_CanBeCalledMultipleTimes()
        {
            // Arrange
            var context = CreateTestContext();

            // Act - should not throw
            context.Dispose();
            context.Dispose();
            context.Dispose();
        }

        #endregion

        #region FeatureFlagContext Tests - Internal Methods

        [Fact]
        public void FeatureFlagContext_SetFlag_AddsOrUpdatesFlag()
        {
            // Arrange
            var context = CreateTestContext();

            // Act
            context.SetFlag("flag1", "value1");
            context.SetFlag("flag2", "value2");
            context.SetFlag("flag1", "updated");

            // Assert
            Assert.Equal("updated", context.GetFlagValue("flag1"));
            Assert.Equal("value2", context.GetFlagValue("flag2"));
        }

        [Fact]
        public void FeatureFlagContext_ClearFlags_RemovesAllFlags()
        {
            // Arrange
            var flags = new Dictionary<string, string>
            {
                ["flag1"] = "value1",
                ["flag2"] = "value2"
            };
            var context = CreateTestContext(flags);

            // Act
            context.ClearFlags();

            // Assert
            Assert.Empty(context.GetAllFlags());
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
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());

            // Act
            var context = cache.GetOrCreateContext("test-host-1.databricks.com", httpClient, DriverVersion);

            // Assert
            Assert.NotNull(context);
            Assert.True(cache.HasContext("test-host-1.databricks.com"));

            // Cleanup
            cache.Clear();
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_ExistingHost_ReturnsSameContext()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-2.databricks.com";
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());

            // Act
            var context1 = cache.GetOrCreateContext(host, httpClient, DriverVersion);
            var context2 = cache.GetOrCreateContext(host, httpClient, DriverVersion);

            // Assert
            Assert.Same(context1, context2);

            // Cleanup
            cache.Clear();
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_MultipleHosts_CreatesMultipleContexts()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());

            // Act
            var context1 = cache.GetOrCreateContext("host1.databricks.com", httpClient, DriverVersion);
            var context2 = cache.GetOrCreateContext("host2.databricks.com", httpClient, DriverVersion);

            // Assert
            Assert.NotSame(context1, context2);
            Assert.Equal(2, cache.CachedHostCount);

            // Cleanup
            cache.Clear();
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_NullHost_ThrowsException()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());

            // Act & Assert
            Assert.Throws<ArgumentException>(() => cache.GetOrCreateContext(null!, httpClient, DriverVersion));
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_EmptyHost_ThrowsException()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());

            // Act & Assert
            Assert.Throws<ArgumentException>(() => cache.GetOrCreateContext("", httpClient, DriverVersion));
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_NullHttpClient_ThrowsException()
        {
            // Arrange
            var cache = new FeatureFlagCache();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => cache.GetOrCreateContext(TestHost, null!, DriverVersion));
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_CaseInsensitive()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "Test-Host.Databricks.com";
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());

            // Act
            var context1 = cache.GetOrCreateContext(host.ToLower(), httpClient, DriverVersion);
            var context2 = cache.GetOrCreateContext(host.ToUpper(), httpClient, DriverVersion);

            // Assert
            Assert.Same(context1, context2);
            Assert.Equal(1, cache.CachedHostCount);

            // Cleanup
            cache.Clear();
        }

        #endregion

        #region FeatureFlagCache_RemoveContext Tests

        [Fact]
        public void FeatureFlagCache_RemoveContext_RemovesContext()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-3.databricks.com";
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());
            cache.GetOrCreateContext(host, httpClient, DriverVersion);

            // Act
            cache.RemoveContext(host);

            // Assert
            Assert.False(cache.HasContext(host));
            Assert.Equal(0, cache.CachedHostCount);
        }

        [Fact]
        public void FeatureFlagCache_RemoveContext_UnknownHost_DoesNothing()
        {
            // Arrange
            var cache = new FeatureFlagCache();

            // Act - should not throw
            cache.RemoveContext("unknown-host.databricks.com");

            // Assert
            Assert.Equal(0, cache.CachedHostCount);
        }

        [Fact]
        public void FeatureFlagCache_RemoveContext_NullHost_DoesNothing()
        {
            // Arrange
            var cache = new FeatureFlagCache();

            // Act - should not throw
            cache.RemoveContext(null!);
        }

        #endregion

        #region FeatureFlagCache with API Response Tests

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_ParsesFlags()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var response = new FeatureFlagsResponse
            {
                Flags = new List<FeatureFlagEntry>
                {
                    new FeatureFlagEntry { Name = "flag1", Value = "value1" },
                    new FeatureFlagEntry { Name = "flag2", Value = "true" }
                },
                TtlSeconds = 300
            };
            var httpClient = CreateMockHttpClient(response);

            // Act
            var context = cache.GetOrCreateContext("test-api.databricks.com", httpClient, DriverVersion);

            // Assert
            Assert.Equal("value1", context.GetFlagValue("flag1"));
            Assert.Equal("true", context.GetFlagValue("flag2"));

            // Cleanup
            cache.Clear();
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_UpdatesTtl()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var response = new FeatureFlagsResponse
            {
                Flags = new List<FeatureFlagEntry>(),
                TtlSeconds = 300 // 5 minutes
            };
            var httpClient = CreateMockHttpClient(response);

            // Act
            var context = cache.GetOrCreateContext("test-ttl.databricks.com", httpClient, DriverVersion);

            // Assert
            Assert.Equal(TimeSpan.FromSeconds(300), context.Ttl);

            // Cleanup
            cache.Clear();
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_ApiError_DoesNotThrow()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var httpClient = CreateMockHttpClient(HttpStatusCode.InternalServerError);

            // Act - should not throw
            var context = cache.GetOrCreateContext("test-error.databricks.com", httpClient, DriverVersion);

            // Assert
            Assert.NotNull(context);
            Assert.Empty(context.GetAllFlags());

            // Cleanup
            cache.Clear();
        }

        #endregion

        #region FeatureFlagCache Helper Method Tests

        [Fact]
        public void FeatureFlagCache_TryGetContext_ExistingContext_ReturnsTrue()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "try-get-host.databricks.com";
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());
            var expectedContext = cache.GetOrCreateContext(host, httpClient, DriverVersion);

            // Act
            var result = cache.TryGetContext(host, out var context);

            // Assert
            Assert.True(result);
            Assert.Same(expectedContext, context);

            // Cleanup
            cache.Clear();
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
        public void FeatureFlagCache_Clear_RemovesAllContexts()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());
            cache.GetOrCreateContext("host1.databricks.com", httpClient, DriverVersion);
            cache.GetOrCreateContext("host2.databricks.com", httpClient, DriverVersion);
            cache.GetOrCreateContext("host3.databricks.com", httpClient, DriverVersion);
            Assert.Equal(3, cache.CachedHostCount);

            // Act
            cache.Clear();

            // Assert
            Assert.Equal(0, cache.CachedHostCount);
        }

        #endregion

        #region Async Initial Fetch Tests

        [Fact]
        public async Task FeatureFlagCache_GetOrCreateContextAsync_AwaitsInitialFetch_FlagsAvailableImmediately()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var response = new FeatureFlagsResponse
            {
                Flags = new List<FeatureFlagEntry>
                {
                    new FeatureFlagEntry { Name = "async_flag1", Value = "async_value1" },
                    new FeatureFlagEntry { Name = "async_flag2", Value = "async_value2" }
                },
                TtlSeconds = 300
            };
            var httpClient = CreateMockHttpClient(response);

            // Act - Use async method explicitly
            var context = await cache.GetOrCreateContextAsync("test-async.databricks.com", httpClient, DriverVersion);

            // Assert - Flags should be immediately available after await completes
            // This verifies that GetOrCreateContextAsync waits for the initial fetch
            Assert.Equal("async_value1", context.GetFlagValue("async_flag1"));
            Assert.Equal("async_value2", context.GetFlagValue("async_flag2"));
            Assert.Equal(2, context.GetAllFlags().Count);

            // Cleanup
            cache.Clear();
        }

        [Fact]
        public async Task FeatureFlagCache_GetOrCreateContextAsync_WithDelayedResponse_StillAwaitsInitialFetch()
        {
            // Arrange - Create a mock that simulates network delay
            var cache = new FeatureFlagCache();
            var response = new FeatureFlagsResponse
            {
                Flags = new List<FeatureFlagEntry>
                {
                    new FeatureFlagEntry { Name = "delayed_flag", Value = "delayed_value" }
                },
                TtlSeconds = 300
            };
            var httpClient = CreateDelayedMockHttpClient(response, delayMs: 100);

            // Act - Measure time to verify we actually waited
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var context = await cache.GetOrCreateContextAsync("test-delayed.databricks.com", httpClient, DriverVersion);
            stopwatch.Stop();

            // Assert - Should have waited for the delayed response
            Assert.True(stopwatch.ElapsedMilliseconds >= 50, "Should have waited for the delayed fetch");

            // Flags should be available immediately after await
            Assert.Equal("delayed_value", context.GetFlagValue("delayed_flag"));

            // Cleanup
            cache.Clear();
        }

        [Fact]
        public async Task FeatureFlagContext_CreateAsync_AwaitsInitialFetch_FlagsPopulated()
        {
            // Arrange
            var response = new FeatureFlagsResponse
            {
                Flags = new List<FeatureFlagEntry>
                {
                    new FeatureFlagEntry { Name = "create_async_flag", Value = "create_async_value" }
                },
                TtlSeconds = 600
            };
            var httpClient = CreateMockHttpClient(response);

            // Act - Call CreateAsync directly
            var context = await FeatureFlagContext.CreateAsync(
                "test-create-async.databricks.com",
                httpClient,
                DriverVersion);

            // Assert - Flags should be populated after CreateAsync completes
            Assert.Equal("create_async_value", context.GetFlagValue("create_async_flag"));
            Assert.Equal(TimeSpan.FromSeconds(600), context.Ttl);

            // Cleanup
            context.Dispose();
        }

        #endregion

        #region Thread Safety Tests

        [Fact]
        public async Task FeatureFlagCache_ConcurrentGetOrCreateContext_ThreadSafe()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "concurrent-host.databricks.com";
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());
            var tasks = new Task<FeatureFlagContext>[100];

            // Act
            for (int i = 0; i < 100; i++)
            {
                tasks[i] = Task.Run(() => cache.GetOrCreateContext(host, httpClient, DriverVersion));
            }

            var contexts = await Task.WhenAll(tasks);

            // Assert - All should be the same context
            var firstContext = contexts[0];
            Assert.All(contexts, ctx => Assert.Same(firstContext, ctx));

            // Cleanup
            cache.Clear();
        }

        [Fact]
        public async Task FeatureFlagContext_ConcurrentFlagAccess_ThreadSafe()
        {
            // Arrange
            var flags = new Dictionary<string, string>
            {
                ["flag1"] = "value1",
                ["flag2"] = "value2"
            };
            var context = CreateTestContext(flags);
            var tasks = new Task[100];

            // Act - Concurrent reads and writes
            for (int i = 0; i < 100; i++)
            {
                var index = i;
                tasks[i] = Task.Run(() =>
                {
                    // Read
                    var value = context.GetFlagValue("flag1");
                    var all = context.GetAllFlags();
                    var flag2Value = context.GetFlagValue("flag2");

                    // Write
                    context.SetFlag($"new_flag_{index}", $"value_{index}");
                });
            }

            await Task.WhenAll(tasks);

            // Assert - No exceptions thrown, all flags accessible
            Assert.Equal("value1", context.GetFlagValue("flag1"));
            var allFlags = context.GetAllFlags();
            Assert.True(allFlags.Count >= 2); // At least original flags
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Creates a FeatureFlagContext for unit testing with pre-populated flags.
        /// Does not make API calls or start background refresh.
        /// </summary>
        private static FeatureFlagContext CreateTestContext(
            IReadOnlyDictionary<string, string>? initialFlags = null,
            TimeSpan? ttl = null)
        {
            var context = new FeatureFlagContext(
                host: "test-host",
                httpClient: null,
                driverVersion: DriverVersion,
                endpointFormat: null);

            if (ttl.HasValue)
            {
                context.Ttl = ttl.Value;
            }

            if (initialFlags != null)
            {
                foreach (var kvp in initialFlags)
                {
                    context.SetFlag(kvp.Key, kvp.Value);
                }
            }

            return context;
        }

        private static HttpClient CreateMockHttpClient(FeatureFlagsResponse response)
        {
            var json = JsonSerializer.Serialize(response);
            return CreateMockHttpClient(HttpStatusCode.OK, json);
        }

        private static HttpClient CreateMockHttpClient(HttpStatusCode statusCode, string content = "")
        {
            var mockHandler = new Mock<HttpMessageHandler>();
            mockHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    StatusCode = statusCode,
                    Content = new StringContent(content)
                });

            return new HttpClient(mockHandler.Object)
            {
                BaseAddress = new Uri("https://test.databricks.com")
            };
        }

        private static HttpClient CreateMockHttpClient(HttpStatusCode statusCode)
        {
            return CreateMockHttpClient(statusCode, "");
        }

        private static HttpClient CreateDelayedMockHttpClient(FeatureFlagsResponse response, int delayMs)
        {
            var json = JsonSerializer.Serialize(response);
            var mockHandler = new Mock<HttpMessageHandler>();
            mockHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns(async (HttpRequestMessage request, CancellationToken token) =>
                {
                    await Task.Delay(delayMs, token);
                    return new HttpResponseMessage
                    {
                        StatusCode = HttpStatusCode.OK,
                        Content = new StringContent(json)
                    };
                });

            return new HttpClient(mockHandler.Object)
            {
                BaseAddress = new Uri("https://test.databricks.com")
            };
        }

        #endregion

        #region Cache TTL Expiration Tests

        [Fact]
        public async Task FeatureFlagCache_ShortTtl_EntryExpiresAfterTtl()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "ttl-test-host.databricks.com";
            var response = new FeatureFlagsResponse
            {
                Flags = new List<FeatureFlagEntry>
                {
                    new FeatureFlagEntry { Name = "ttl_flag", Value = "ttl_value" }
                },
                TtlSeconds = 300
            };
            var httpClient = CreateMockHttpClient(response);

            // Very short TTL for testing (1 second)
            var shortTtl = TimeSpan.FromSeconds(1);

            // Act - Create context with short TTL
            var context = await cache.GetOrCreateContextAsync(host, httpClient, DriverVersion, cacheTtl: shortTtl);

            // Verify context exists immediately after creation
            Assert.True(cache.HasContext(host), "Context should exist immediately after creation");
            Assert.Equal("ttl_value", context.GetFlagValue("ttl_flag"));

            // Wait for TTL to expire (add buffer for cache cleanup)
            await Task.Delay(TimeSpan.FromSeconds(2));

            // Assert - Context should be evicted after TTL expires
            // Note: IMemoryCache uses lazy eviction, so we need to trigger a check
            // by trying to access the entry or calling TryGetContext
            var stillExists = cache.TryGetContext(host, out var expiredContext);

            // The entry should have been evicted due to sliding expiration
            Assert.False(stillExists, "Context should have been evicted after TTL expired");
            Assert.Null(expiredContext);

            // Cleanup
            cache.Dispose();
        }

        [Fact]
        public async Task FeatureFlagCache_SlidingExpiration_AccessExtendsLifetime()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "sliding-ttl-host.databricks.com";
            var response = new FeatureFlagsResponse
            {
                Flags = new List<FeatureFlagEntry>
                {
                    new FeatureFlagEntry { Name = "sliding_flag", Value = "sliding_value" }
                },
                TtlSeconds = 300
            };
            var httpClient = CreateMockHttpClient(response);

            // Short TTL for testing (2 seconds)
            var shortTtl = TimeSpan.FromSeconds(2);

            // Act - Create context with short TTL
            var context = await cache.GetOrCreateContextAsync(host, httpClient, DriverVersion, cacheTtl: shortTtl);
            Assert.True(cache.HasContext(host), "Context should exist after creation");

            // Access the cache entry multiple times before TTL expires to extend lifetime
            for (int i = 0; i < 3; i++)
            {
                await Task.Delay(TimeSpan.FromSeconds(1)); // Wait 1 second (less than TTL)
                var accessed = cache.TryGetContext(host, out var accessedContext);
                Assert.True(accessed, $"Context should still exist after access {i + 1}");
                Assert.NotNull(accessedContext);
            }

            // Total elapsed: ~3 seconds, but TTL was 2 seconds
            // Entry should still exist because each access extends the sliding window

            // Now wait for TTL to expire without accessing
            await Task.Delay(TimeSpan.FromSeconds(3));

            // Assert - Context should be evicted after TTL expires without access
            var stillExists = cache.TryGetContext(host, out _);
            Assert.False(stillExists, "Context should have been evicted after TTL expired without access");

            // Cleanup
            cache.Dispose();
        }

        [Fact]
        public async Task FeatureFlagCache_CustomTtl_OverridesDefault()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "custom-ttl-host.databricks.com";
            var response = new FeatureFlagsResponse
            {
                Flags = new List<FeatureFlagEntry>(),
                TtlSeconds = 900 // Server returns 15 minutes
            };
            var httpClient = CreateMockHttpClient(response);

            // Custom TTL (30 seconds) - different from default (15 min) and server response
            var customTtl = TimeSpan.FromSeconds(30);

            // Act
            var context = await cache.GetOrCreateContextAsync(host, httpClient, DriverVersion, cacheTtl: customTtl);

            // Assert - Context's TTL should be from server response (stored in context)
            // but cache entry TTL is controlled by cacheTtl parameter
            Assert.Equal(TimeSpan.FromSeconds(900), context.Ttl); // Server's TTL stored in context
            Assert.True(cache.HasContext(host));

            // Cleanup
            cache.Clear();
        }

        #endregion
    }
}
