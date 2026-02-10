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

        #region FeatureFlagContext with API Response Tests

        [Fact]
        public async Task FeatureFlagContext_CreateAsync_ParsesFlags()
        {
            // Arrange
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
            var context = await FeatureFlagContext.CreateAsync("test-api.databricks.com", httpClient, DriverVersion);

            // Assert
            Assert.Equal("value1", context.GetFlagValue("flag1"));
            Assert.Equal("true", context.GetFlagValue("flag2"));

            // Cleanup
            context.Dispose();
        }

        [Fact]
        public async Task FeatureFlagContext_CreateAsync_UpdatesTtl()
        {
            // Arrange
            var response = new FeatureFlagsResponse
            {
                Flags = new List<FeatureFlagEntry>(),
                TtlSeconds = 300 // 5 minutes
            };
            var httpClient = CreateMockHttpClient(response);

            // Act
            var context = await FeatureFlagContext.CreateAsync("test-ttl.databricks.com", httpClient, DriverVersion);

            // Assert
            Assert.Equal(TimeSpan.FromSeconds(300), context.Ttl);

            // Cleanup
            context.Dispose();
        }

        [Fact]
        public async Task FeatureFlagContext_CreateAsync_ApiError_DoesNotThrow()
        {
            // Arrange
            var httpClient = CreateMockHttpClient(HttpStatusCode.InternalServerError);

            // Act - should not throw
            var context = await FeatureFlagContext.CreateAsync("test-error.databricks.com", httpClient, DriverVersion);

            // Assert
            Assert.NotNull(context);
            Assert.Empty(context.GetAllFlags());

            // Cleanup
            context.Dispose();
        }

        #endregion

        #region Async Initial Fetch Tests

        [Fact]
        public async Task FeatureFlagContext_CreateAsync_AwaitsInitialFetch_FlagsAvailableImmediately()
        {
            // Arrange
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

            // Act - Use CreateAsync directly
            var context = await FeatureFlagContext.CreateAsync("test-async.databricks.com", httpClient, DriverVersion);

            // Assert - Flags should be immediately available after await completes
            // This verifies that CreateAsync waits for the initial fetch
            Assert.Equal("async_value1", context.GetFlagValue("async_flag1"));
            Assert.Equal("async_value2", context.GetFlagValue("async_flag2"));
            Assert.Equal(2, context.GetAllFlags().Count);

            // Cleanup
            context.Dispose();
        }

        [Fact]
        public async Task FeatureFlagContext_CreateAsync_WithDelayedResponse_StillAwaitsInitialFetch()
        {
            // Arrange - Create a mock that simulates network delay
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
            var context = await FeatureFlagContext.CreateAsync("test-delayed.databricks.com", httpClient, DriverVersion);
            stopwatch.Stop();

            // Assert - Should have waited for the delayed response
            Assert.True(stopwatch.ElapsedMilliseconds >= 50, "Should have waited for the delayed fetch");

            // Flags should be available immediately after await
            Assert.Equal("delayed_value", context.GetFlagValue("delayed_flag"));

            // Cleanup
            context.Dispose();
        }

        #endregion

        #region Thread Safety Tests

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
    }
}
