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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Unit tests for DatabricksConnection telemetry integration.
    /// Tests the integration points between DatabricksConnection and the telemetry system.
    /// </summary>
    /// <remarks>
    /// These tests verify:
    /// - OpenAsync initializes telemetry when feature flag is enabled
    /// - OpenAsync skips telemetry when feature flag is disabled (local config)
    /// - Dispose releases telemetry client and feature flag context
    /// - All telemetry exceptions are swallowed during connection lifecycle
    ///
    /// Note: Full integration testing of OpenAsync requires a real Databricks connection.
    /// These unit tests focus on the telemetry components in isolation.
    /// </remarks>
    public class DatabricksConnectionTelemetryTests : IDisposable
    {
        private readonly TelemetryClientManager _telemetryClientManager;
        private readonly FeatureFlagCache _featureFlagCache;
        private const string TestHost = "test-telemetry.databricks.com";

        public DatabricksConnectionTelemetryTests()
        {
            // Create test instances of the managers
            // Note: Using non-singleton instances for test isolation
            _telemetryClientManager = new TelemetryClientManager(CreateMockTelemetryClient);
            _featureFlagCache = new FeatureFlagCache();
        }

        public void Dispose()
        {
            // Clear the singleton managers after each test to avoid test pollution
            TelemetryClientManager.GetInstance().Clear();
            FeatureFlagCache.GetInstance().Clear();
        }

        /// <summary>
        /// Creates a mock telemetry client for testing.
        /// </summary>
        private static ITelemetryClient CreateMockTelemetryClient(string host, HttpClient httpClient, TelemetryConfiguration config)
        {
            return new MockTelemetryClient(host);
        }

        #region Test: DatabricksConnection_OpenAsync_InitializesTelemetry

        /// <summary>
        /// Tests that TelemetryClientManager creates a client when telemetry is enabled.
        /// This simulates what happens during OpenAsync when telemetry initialization succeeds.
        /// </summary>
        [Fact]
        public void TelemetryClientManager_GetOrCreateClient_CreatesClientWhenEnabled()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration { Enabled = true };

            // Act
            var client = _telemetryClientManager.GetOrCreateClient(TestHost, httpClient, config);

            // Assert
            Assert.NotNull(client);
            Assert.Equal(TestHost, client.Host);
            Assert.True(_telemetryClientManager.HasClient(TestHost));
            Assert.True(_telemetryClientManager.TryGetHolder(TestHost, out var holder));
            Assert.Equal(1, holder!.RefCount);
        }

        /// <summary>
        /// Tests that FeatureFlagCache creates context during OpenAsync telemetry initialization.
        /// </summary>
        [Fact]
        public void FeatureFlagCache_GetOrCreateContext_CreatesContextForHost()
        {
            // Act
            var context = _featureFlagCache.GetOrCreateContext(TestHost);

            // Assert
            Assert.NotNull(context);
            Assert.Equal(1, context.RefCount);
            Assert.True(_featureFlagCache.HasContext(TestHost));
        }

        /// <summary>
        /// Tests that telemetry initialization respects the feature flag check.
        /// When IsTelemetryEnabledAsync returns true, telemetry should be initialized.
        /// </summary>
        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_ReturnsTrueWhenEnabled()
        {
            // Arrange
            _featureFlagCache.GetOrCreateContext(TestHost);

            // Act
            var isEnabled = await _featureFlagCache.IsTelemetryEnabledAsync(
                TestHost,
                _ => Task.FromResult(true), // Feature flag fetcher returns true
                CancellationToken.None);

            // Assert
            Assert.True(isEnabled);
        }

        #endregion

        #region Test: DatabricksConnection_OpenAsync_FeatureFlagDisabled_NoTelemetry

        /// <summary>
        /// Tests that telemetry is not initialized when local configuration disables it.
        /// </summary>
        [Fact]
        public void TelemetryConfiguration_FromProperties_DisabledByLocalConfig()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                [TelemetryConfiguration.PropertyKeyEnabled] = "false"
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.False(config.Enabled);
        }

        /// <summary>
        /// Tests that feature flag check returns false when server disables telemetry.
        /// </summary>
        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_ReturnsFalseWhenServerDisabled()
        {
            // Arrange
            _featureFlagCache.GetOrCreateContext(TestHost);

            // Act
            var isEnabled = await _featureFlagCache.IsTelemetryEnabledAsync(
                TestHost,
                _ => Task.FromResult(false), // Feature flag fetcher returns false
                CancellationToken.None);

            // Assert
            Assert.False(isEnabled);
        }

        /// <summary>
        /// Tests that telemetry configuration defaults to enabled.
        /// </summary>
        [Fact]
        public void TelemetryConfiguration_Default_IsEnabled()
        {
            // Act
            var config = new TelemetryConfiguration();

            // Assert
            Assert.True(config.Enabled);
        }

        #endregion

        #region Test: DatabricksConnection_Dispose_ReleasesTelemetryClient

        /// <summary>
        /// Tests that releasing a telemetry client decrements the reference count.
        /// </summary>
        [Fact]
        public async Task TelemetryClientManager_ReleaseClientAsync_DecrementsRefCount()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration { Enabled = true };

            // Create two references
            _telemetryClientManager.GetOrCreateClient(TestHost, httpClient, config);
            _telemetryClientManager.GetOrCreateClient(TestHost, httpClient, config);

            Assert.True(_telemetryClientManager.TryGetHolder(TestHost, out var holder));
            Assert.Equal(2, holder!.RefCount);

            // Act - Release one reference
            await _telemetryClientManager.ReleaseClientAsync(TestHost);

            // Assert - Client still exists with one reference
            Assert.True(_telemetryClientManager.HasClient(TestHost));
            Assert.Equal(1, holder.RefCount);
        }

        /// <summary>
        /// Tests that releasing the last telemetry client reference closes and removes the client.
        /// </summary>
        [Fact]
        public async Task TelemetryClientManager_ReleaseClientAsync_LastReference_RemovesClient()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration { Enabled = true };

            _telemetryClientManager.GetOrCreateClient(TestHost, httpClient, config);

            // Act - Release the only reference
            await _telemetryClientManager.ReleaseClientAsync(TestHost);

            // Assert - Client is removed
            Assert.False(_telemetryClientManager.HasClient(TestHost));
        }

        /// <summary>
        /// Tests that releasing feature flag context decrements the reference count.
        /// </summary>
        [Fact]
        public void FeatureFlagCache_ReleaseContext_DecrementsRefCount()
        {
            // Arrange - Create two references
            _featureFlagCache.GetOrCreateContext(TestHost);
            _featureFlagCache.GetOrCreateContext(TestHost);

            Assert.True(_featureFlagCache.TryGetContext(TestHost, out var context));
            Assert.Equal(2, context!.RefCount);

            // Act - Release one reference
            _featureFlagCache.ReleaseContext(TestHost);

            // Assert - Context still exists with one reference
            Assert.True(_featureFlagCache.HasContext(TestHost));
            Assert.Equal(1, context.RefCount);
        }

        /// <summary>
        /// Tests that releasing the last feature flag context reference removes the context.
        /// </summary>
        [Fact]
        public void FeatureFlagCache_ReleaseContext_LastReference_RemovesContext()
        {
            // Arrange
            _featureFlagCache.GetOrCreateContext(TestHost);

            // Act - Release the only reference
            _featureFlagCache.ReleaseContext(TestHost);

            // Assert - Context is removed
            Assert.False(_featureFlagCache.HasContext(TestHost));
        }

        #endregion

        #region Test: DatabricksConnection_Dispose_FlushesMetricsBeforeRelease

        /// <summary>
        /// Tests that CloseAsync is called when the last reference is released.
        /// CloseAsync should flush any pending metrics.
        /// </summary>
        [Fact]
        public async Task TelemetryClient_CloseAsync_CalledOnLastRelease()
        {
            // Arrange
            var closeCalled = false;
            var mockClient = new MockTelemetryClient(TestHost) { OnClose = () => closeCalled = true };
            var testManager = new TelemetryClientManager((host, httpClient, config) => mockClient);

            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration { Enabled = true };

            testManager.GetOrCreateClient(TestHost, httpClient, config);

            // Act - Release the only reference
            await testManager.ReleaseClientAsync(TestHost);

            // Assert - CloseAsync was called
            Assert.True(closeCalled);
        }

        #endregion

        #region Test: Telemetry Exceptions Swallowed

        /// <summary>
        /// Tests that exceptions during feature flag fetch are swallowed.
        /// </summary>
        [Fact]
        public async Task FeatureFlagCache_IsTelemetryEnabledAsync_SwallowsExceptions()
        {
            // Arrange
            _featureFlagCache.GetOrCreateContext(TestHost);

            // Act - Fetch throws exception
            var isEnabled = await _featureFlagCache.IsTelemetryEnabledAsync(
                TestHost,
                _ => throw new HttpRequestException("Network error"),
                CancellationToken.None);

            // Assert - Exception was swallowed, returns false as safe default
            Assert.False(isEnabled);
        }

        /// <summary>
        /// Tests that exceptions during telemetry client close are swallowed.
        /// </summary>
        [Fact]
        public async Task TelemetryClientManager_ReleaseClientAsync_SwallowsCloseExceptions()
        {
            // Arrange
            var mockClient = new MockTelemetryClient(TestHost)
            {
                OnClose = () => throw new InvalidOperationException("Close failed")
            };
            var testManager = new TelemetryClientManager((host, httpClient, config) => mockClient);

            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration { Enabled = true };

            testManager.GetOrCreateClient(TestHost, httpClient, config);

            // Act & Assert - No exception thrown
            await testManager.ReleaseClientAsync(TestHost);

            // Client should still be removed even though close threw
            Assert.False(testManager.HasClient(TestHost));
        }

        /// <summary>
        /// Tests that releasing unknown hosts doesn't throw exceptions.
        /// </summary>
        [Fact]
        public async Task TelemetryClientManager_ReleaseClientAsync_UnknownHost_DoesNotThrow()
        {
            // Act & Assert - No exception thrown
            await _telemetryClientManager.ReleaseClientAsync("unknown-host.databricks.com");
            await _telemetryClientManager.ReleaseClientAsync(null!);
            await _telemetryClientManager.ReleaseClientAsync("");
            await _telemetryClientManager.ReleaseClientAsync("   ");
        }

        /// <summary>
        /// Tests that releasing unknown feature flag contexts doesn't throw exceptions.
        /// </summary>
        [Fact]
        public void FeatureFlagCache_ReleaseContext_UnknownHost_DoesNotThrow()
        {
            // Act & Assert - No exception thrown
            _featureFlagCache.ReleaseContext("unknown-host.databricks.com");
            _featureFlagCache.ReleaseContext(null!);
            _featureFlagCache.ReleaseContext("");
            _featureFlagCache.ReleaseContext("   ");
        }

        #endregion

        #region Mock Classes

        /// <summary>
        /// Mock implementation of ITelemetryClient for testing.
        /// </summary>
        private class MockTelemetryClient : ITelemetryClient
        {
            public string Host { get; }
            public Action? OnClose { get; set; }
            public IReadOnlyList<TelemetryFrontendLog>? LastExportedLogs { get; private set; }

            public MockTelemetryClient(string host)
            {
                Host = host;
            }

            public Task ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                LastExportedLogs = logs;
                return Task.CompletedTask;
            }

            public Task CloseAsync()
            {
                OnClose?.Invoke();
                return Task.CompletedTask;
            }
        }

        #endregion
    }
}
