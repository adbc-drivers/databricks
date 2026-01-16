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
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for FeatureFlagCache class.
    /// </summary>
    public class FeatureFlagCacheTests
    {
        [Fact]
        public void FeatureFlagCache_GetInstance_ReturnsSingletonInstance()
        {
            // Act
            var instance1 = FeatureFlagCache.GetInstance();
            var instance2 = FeatureFlagCache.GetInstance();

            // Assert
            Assert.NotNull(instance1);
            Assert.Same(instance1, instance2);
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_NewHost_CreatesContext()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";

            // Act
            var context = cache.GetOrCreateContext(host);

            // Assert
            Assert.NotNull(context);
            Assert.Equal(1, context.RefCount);
            Assert.Null(context.TelemetryEnabled);
            Assert.Null(context.LastFetched);

            // Cleanup
            cache.ReleaseContext(host);
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_ExistingHost_IncrementsRefCount()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";

            // Act
            var context1 = cache.GetOrCreateContext(host);
            var context2 = cache.GetOrCreateContext(host);

            // Assert
            Assert.Same(context1, context2);
            Assert.Equal(2, context2.RefCount);

            // Cleanup
            cache.ReleaseContext(host);
            cache.ReleaseContext(host);
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_NullHost_ThrowsArgumentNullException()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => cache.GetOrCreateContext(null!));
        }

        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_EmptyHost_ThrowsArgumentNullException()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => cache.GetOrCreateContext(string.Empty));
        }

        [Fact]
        public void FeatureFlagCache_ReleaseContext_LastReference_RemovesContext()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";
            var context = cache.GetOrCreateContext(host);

            // Act
            cache.ReleaseContext(host);

            // Assert - get context again should create new one with RefCount=1
            var newContext = cache.GetOrCreateContext(host);
            Assert.NotSame(context, newContext);
            Assert.Equal(1, newContext.RefCount);

            // Cleanup
            cache.ReleaseContext(host);
        }

        [Fact]
        public void FeatureFlagCache_ReleaseContext_MultipleReferences_DecrementsOnly()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";
            var context1 = cache.GetOrCreateContext(host);
            var context2 = cache.GetOrCreateContext(host);

            // Act
            cache.ReleaseContext(host);

            // Assert - get context again should return same instance with RefCount=1
            var context3 = cache.GetOrCreateContext(host);
            Assert.Same(context1, context3);
            Assert.Equal(2, context3.RefCount);

            // Cleanup
            cache.ReleaseContext(host);
            cache.ReleaseContext(host);
        }

        [Fact]
        public void FeatureFlagCache_ReleaseContext_NullHost_DoesNotThrow()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();

            // Act & Assert - should not throw
            cache.ReleaseContext(null!);
        }

        [Fact]
        public void FeatureFlagCache_ReleaseContext_NonExistentHost_DoesNotThrow()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";

            // Act & Assert - should not throw
            cache.ReleaseContext(host);
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_CachedValue_DoesNotFetch()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";
            var context = cache.GetOrCreateContext(host);
            context.TelemetryEnabled = true;
            context.LastFetched = DateTime.UtcNow;

            var handler = new TestHttpMessageHandler(HttpStatusCode.InternalServerError, "Should not be called");
            var httpClient = new HttpClient(handler);

            // Act
            var enabled = await cache.IsTelemetryEnabledAsync(host, httpClient);

            // Assert
            Assert.True(enabled);
            Assert.Equal(0, handler.RequestCount); // No HTTP call should be made

            // Cleanup
            cache.ReleaseContext(host);
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_ExpiredCache_RefetchesValue()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";
            var context = cache.GetOrCreateContext(host);
            context.TelemetryEnabled = false;
            context.LastFetched = DateTime.UtcNow.AddMinutes(-20); // Expired (>15 minutes)

            var responseJson = JsonSerializer.Serialize(new { flag = TelemetryConfiguration.FeatureFlagName, enabled = true });
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, responseJson);
            var httpClient = new HttpClient(handler);

            // Act
            var enabled = await cache.IsTelemetryEnabledAsync(host, httpClient);

            // Assert
            Assert.True(enabled); // Should return new value (true)
            Assert.True(context.TelemetryEnabled); // Context should be updated
            Assert.NotNull(context.LastFetched);
            Assert.True((DateTime.UtcNow - context.LastFetched.Value).TotalSeconds < 5); // Recently fetched
            Assert.Equal(1, handler.RequestCount); // HTTP call should be made once

            // Cleanup
            cache.ReleaseContext(host);
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_NoCache_FetchesFromServer()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";

            var responseJson = JsonSerializer.Serialize(new { flag = TelemetryConfiguration.FeatureFlagName, enabled = true });
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, responseJson);
            var httpClient = new HttpClient(handler);

            // Act
            var enabled = await cache.IsTelemetryEnabledAsync(host, httpClient);

            // Assert
            Assert.True(enabled);
            Assert.Equal(1, handler.RequestCount); // HTTP call should be made once
            Assert.Contains($"https://{host}/api/2.0/feature-flags", handler.LastRequestUri?.ToString());
            Assert.Contains(TelemetryConfiguration.FeatureFlagName, handler.LastRequestUri?.ToString());
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_ServerReturnsDisabled_ReturnsFalse()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";

            var responseJson = JsonSerializer.Serialize(new { flag = TelemetryConfiguration.FeatureFlagName, enabled = false });
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, responseJson);
            var httpClient = new HttpClient(handler);

            // Act
            var enabled = await cache.IsTelemetryEnabledAsync(host, httpClient);

            // Assert
            Assert.False(enabled);
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_ServerError_ReturnsFalse()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";

            var handler = new TestHttpMessageHandler(HttpStatusCode.InternalServerError, "Server Error");
            var httpClient = new HttpClient(handler);

            // Act
            var enabled = await cache.IsTelemetryEnabledAsync(host, httpClient);

            // Assert
            Assert.False(enabled); // Default to disabled on error
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_InvalidJson_ReturnsFalse()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";

            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "invalid json");
            var httpClient = new HttpClient(handler);

            // Act
            var enabled = await cache.IsTelemetryEnabledAsync(host, httpClient);

            // Assert
            Assert.False(enabled); // Default to disabled on parse error
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_MissingEnabledField_ReturnsFalse()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";

            var responseJson = JsonSerializer.Serialize(new { flag = TelemetryConfiguration.FeatureFlagName });
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, responseJson);
            var httpClient = new HttpClient(handler);

            // Act
            var enabled = await cache.IsTelemetryEnabledAsync(host, httpClient);

            // Assert
            Assert.False(enabled); // Default to disabled if enabled field missing
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_NullHost_ReturnsFalse()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var httpClient = new HttpClient();

            // Act
            var enabled = await cache.IsTelemetryEnabledAsync(null!, httpClient);

            // Assert
            Assert.False(enabled);
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_NullHttpClient_ThrowsArgumentNullException()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(() =>
                cache.IsTelemetryEnabledAsync(host, null!));
        }

        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_ThreadSafe_ConcurrentAccess()
        {
            // Arrange
            var cache = FeatureFlagCache.GetInstance();
            var host = $"test-{Guid.NewGuid()}.databricks.com";

            var responseJson = JsonSerializer.Serialize(new { flag = TelemetryConfiguration.FeatureFlagName, enabled = true });
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, responseJson);
            var httpClient = new HttpClient(handler);

            // Act - simulate 10 concurrent requests
            var tasks = Enumerable.Range(0, 10)
                .Select(_ => cache.IsTelemetryEnabledAsync(host, httpClient))
                .ToArray();

            var results = await Task.WhenAll(tasks);

            // Assert
            Assert.All(results, result => Assert.True(result));
            // Verify the HTTP call was made at least once
            Assert.True(handler.RequestCount >= 1);
        }

        [Fact]
        public void FeatureFlagContext_IsExpired_NeverFetched_ReturnsTrue()
        {
            // Arrange
            var context = new FeatureFlagContext();

            // Act & Assert
            Assert.True(context.IsExpired);
        }

        [Fact]
        public void FeatureFlagContext_IsExpired_RecentlyFetched_ReturnsFalse()
        {
            // Arrange
            var context = new FeatureFlagContext
            {
                LastFetched = DateTime.UtcNow.AddMinutes(-5) // Within 15 minute cache duration
            };

            // Act & Assert
            Assert.False(context.IsExpired);
        }

        [Fact]
        public void FeatureFlagContext_IsExpired_FetchedLongAgo_ReturnsTrue()
        {
            // Arrange
            var context = new FeatureFlagContext
            {
                LastFetched = DateTime.UtcNow.AddMinutes(-20) // Older than 15 minute cache duration
            };

            // Act & Assert
            Assert.True(context.IsExpired);
        }

        [Fact]
        public void FeatureFlagContext_CacheDuration_DefaultIs15Minutes()
        {
            // Arrange & Act
            var context = new FeatureFlagContext();

            // Assert
            Assert.Equal(TimeSpan.FromMinutes(15), context.CacheDuration);
        }

        /// <summary>
        /// Test HttpMessageHandler for simulating HTTP responses without Moq.
        /// </summary>
        private class TestHttpMessageHandler : HttpMessageHandler
        {
            private readonly HttpStatusCode _statusCode;
            private readonly string _content;

            public int RequestCount { get; private set; }
            public Uri? LastRequestUri { get; private set; }

            public TestHttpMessageHandler(HttpStatusCode statusCode, string content)
            {
                _statusCode = statusCode;
                _content = content;
            }

            protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                RequestCount++;
                LastRequestUri = request.RequestUri;

                return Task.FromResult(new HttpResponseMessage
                {
                    StatusCode = _statusCode,
                    Content = new StringContent(_content)
                });
            }
        }
    }
}
