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
            var context = new FeatureFlagContext(flags);

            // Act & Assert
            Assert.Equal("value1", context.GetFlagValue("flag1"));
            Assert.Equal("value2", context.GetFlagValue("flag2"));
        }

        [Fact]
        public void FeatureFlagContext_GetFlagValue_NotFound_ReturnsNull()
        {
            // Arrange
            var context = new FeatureFlagContext();

            // Act & Assert
            Assert.Null(context.GetFlagValue("nonexistent"));
        }

        [Fact]
        public void FeatureFlagContext_GetFlagValue_NullOrEmpty_ReturnsNull()
        {
            // Arrange
            var context = new FeatureFlagContext();

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
            var context = new FeatureFlagContext(flags);

            // Act & Assert
            Assert.Equal("value", context.GetFlagValue("myflag"));
            Assert.Equal("value", context.GetFlagValue("MYFLAG"));
            Assert.Equal("value", context.GetFlagValue("MyFlag"));
        }

        [Fact]
        public void FeatureFlagContext_IsFeatureEnabled_True()
        {
            // Arrange
            var flags = new Dictionary<string, string>
            {
                ["enabled_flag"] = "true",
                ["enabled_flag_caps"] = "TRUE",
                ["enabled_flag_mixed"] = "True"
            };
            var context = new FeatureFlagContext(flags);

            // Act & Assert
            Assert.True(context.IsFeatureEnabled("enabled_flag"));
            Assert.True(context.IsFeatureEnabled("enabled_flag_caps"));
            Assert.True(context.IsFeatureEnabled("enabled_flag_mixed"));
        }

        [Fact]
        public void FeatureFlagContext_IsFeatureEnabled_False()
        {
            // Arrange
            var flags = new Dictionary<string, string>
            {
                ["disabled_flag"] = "false",
                ["other_value"] = "yes",
                ["numeric_value"] = "1"
            };
            var context = new FeatureFlagContext(flags);

            // Act & Assert
            Assert.False(context.IsFeatureEnabled("disabled_flag"));
            Assert.False(context.IsFeatureEnabled("other_value"));
            Assert.False(context.IsFeatureEnabled("numeric_value"));
            Assert.False(context.IsFeatureEnabled("nonexistent"));
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
            var context = new FeatureFlagContext(flags);

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
            var context = new FeatureFlagContext();
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
            var context = new FeatureFlagContext();

            // Act
            var allFlags = context.GetAllFlags();

            // Assert
            Assert.Empty(allFlags);
        }

        #endregion

        #region FeatureFlagContext Tests - Reference Counting

        [Fact]
        public void FeatureFlagContext_RefCount_StartsAtZero()
        {
            // Arrange & Act
            var context = new FeatureFlagContext();

            // Assert
            Assert.Equal(0, context.RefCount);
        }

        [Fact]
        public void FeatureFlagContext_IncrementRefCount_IncrementsCorrectly()
        {
            // Arrange
            var context = new FeatureFlagContext();

            // Act & Assert
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

        #endregion

        #region FeatureFlagContext Tests - Refresh Interval

        [Fact]
        public void FeatureFlagContext_DefaultRefreshInterval_Is15Minutes()
        {
            // Arrange
            var context = new FeatureFlagContext();

            // Assert
            Assert.Equal(TimeSpan.FromMinutes(15), context.RefreshInterval);
        }

        [Fact]
        public void FeatureFlagContext_CustomRefreshInterval()
        {
            // Arrange
            var customInterval = TimeSpan.FromMinutes(5);
            var context = new FeatureFlagContext(null, customInterval);

            // Assert
            Assert.Equal(customInterval, context.RefreshInterval);
        }

        #endregion

        #region FeatureFlagContext Tests - Shutdown and Dispose

        [Fact]
        public void FeatureFlagContext_Shutdown_CanBeCalledMultipleTimes()
        {
            // Arrange
            var context = new FeatureFlagContext();

            // Act - should not throw
            context.Shutdown();
            context.Shutdown();
            context.Shutdown();
        }

        [Fact]
        public void FeatureFlagContext_Dispose_CanBeCalledMultipleTimes()
        {
            // Arrange
            var context = new FeatureFlagContext();

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
            var context = new FeatureFlagContext();

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
            var context = new FeatureFlagContext(flags);

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
            Assert.Equal(1, context.RefCount);
            Assert.True(cache.HasContext("test-host-1.databricks.com"));

            // Cleanup
            cache.Clear();
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_ExistingHost_IncrementsRefCount()
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
            Assert.Equal(2, context1.RefCount);

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
            Assert.Equal(1, context1.RefCount);
            Assert.Equal(1, context2.RefCount);
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
            Assert.Equal(2, context1.RefCount);
            Assert.Equal(1, cache.CachedHostCount);

            // Cleanup
            cache.Clear();
        }

        #endregion

        #region FeatureFlagCache_ReleaseContext Tests

        [Fact]
        public void FeatureFlagCache_ReleaseContext_LastReference_RemovesContext()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-3.databricks.com";
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());
            var context = cache.GetOrCreateContext(host, httpClient, DriverVersion);
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
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());
            var context = cache.GetOrCreateContext(host, httpClient, DriverVersion);
            cache.GetOrCreateContext(host, httpClient, DriverVersion); // Second reference
            Assert.Equal(2, context.RefCount);

            // Act
            cache.ReleaseContext(host);

            // Assert
            Assert.True(cache.HasContext(host));
            Assert.Equal(1, context.RefCount);

            // Cleanup
            cache.Clear();
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
        }

        [Fact]
        public void FeatureFlagCache_ReleaseContext_AllReleased_RemovesContext()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "test-host-5.databricks.com";
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());

            // Create 3 references
            cache.GetOrCreateContext(host, httpClient, DriverVersion);
            cache.GetOrCreateContext(host, httpClient, DriverVersion);
            cache.GetOrCreateContext(host, httpClient, DriverVersion);
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
            Assert.True(context.IsFeatureEnabled("flag2"));

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
            Assert.Equal(TimeSpan.FromSeconds(300), context.RefreshInterval);

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
            Assert.Equal(100, firstContext.RefCount);

            // Cleanup
            cache.Clear();
        }

        [Fact]
        public async Task FeatureFlagCache_ConcurrentReleaseContext_ThreadSafe()
        {
            // Arrange
            var cache = new FeatureFlagCache();
            var host = "concurrent-release-host.databricks.com";
            var httpClient = CreateMockHttpClient(new FeatureFlagsResponse());

            // Create 100 references
            for (int i = 0; i < 100; i++)
            {
                cache.GetOrCreateContext(host, httpClient, DriverVersion);
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
        public async Task FeatureFlagContext_ConcurrentFlagAccess_ThreadSafe()
        {
            // Arrange
            var flags = new Dictionary<string, string>
            {
                ["flag1"] = "value1",
                ["flag2"] = "value2"
            };
            var context = new FeatureFlagContext(flags);
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
                    var enabled = context.IsFeatureEnabled("flag2");

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

        #endregion
    }
}
