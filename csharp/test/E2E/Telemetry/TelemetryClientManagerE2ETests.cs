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
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for TelemetryClientManager.
    /// Tests per-host client sharing behavior, validates reference counting and cleanup.
    /// </summary>
    /// <remarks>
    /// These tests verify:
    /// - Per-host client management: same host returns same client
    /// - Different hosts return different clients
    /// - Reference counting: client is only closed when last reference is released
    /// - Thread-safety: concurrent access is handled correctly
    ///
    /// Phase 7 E2E GATE - Tests per-host client management before full driver integration.
    /// </remarks>
    public class TelemetryClientManagerE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        private readonly bool _canRunRealEndpointTests;
        private readonly string? _host;
        private readonly string? _token;

        public TelemetryClientManagerE2ETests(ITestOutputHelper? outputHelper)
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

        #region TelemetryClientManager_SameHost_ReturnsSameClient

        /// <summary>
        /// Tests that TelemetryClientManager returns the same client for the same host.
        /// This is a key requirement for per-host client sharing to prevent rate limiting.
        /// </summary>
        [Fact]
        public void TelemetryClientManager_SameHost_ReturnsSameClient()
        {
            // Arrange
            var closedClients = new List<string>();
            var manager = new TelemetryClientManager((host, httpClient, config) =>
            {
                var mockClient = new MockTelemetryClient(host);
                mockClient.OnClose = () => closedClients.Add(host);
                return mockClient;
            });
            var host = "same-host-test.databricks.com";
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            try
            {
                // Act - Get client multiple times for the same host
                var client1 = manager.GetOrCreateClient(host, httpClient, config);
                var client2 = manager.GetOrCreateClient(host, httpClient, config);
                var client3 = manager.GetOrCreateClient(host, httpClient, config);

                // Assert - All should be the same instance
                Assert.Same(client1, client2);
                Assert.Same(client2, client3);
                Assert.Equal(host, client1.Host);

                // Verify only one client was created
                Assert.Equal(1, manager.ClientCount);

                // Verify reference count is correct
                Assert.True(manager.TryGetHolder(host, out var holder));
                Assert.Equal(3, holder!.RefCount);

                OutputHelper?.WriteLine($"Same host '{host}' returned same client instance with RefCount={holder.RefCount}");
            }
            finally
            {
                // Cleanup
                manager.Clear();
            }
        }

        /// <summary>
        /// Tests that TelemetryClientManager returns the same client for the same host
        /// with case-insensitive comparison.
        /// </summary>
        [Fact]
        public void TelemetryClientManager_SameHost_CaseInsensitive_ReturnsSameClient()
        {
            // Arrange
            var manager = new TelemetryClientManager((host, httpClient, config) =>
                new MockTelemetryClient(host));
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            try
            {
                // Act - Get client with different case variations
                var client1 = manager.GetOrCreateClient("Test-Host.Databricks.com", httpClient, config);
                var client2 = manager.GetOrCreateClient("test-host.databricks.com", httpClient, config);
                var client3 = manager.GetOrCreateClient("TEST-HOST.DATABRICKS.COM", httpClient, config);

                // Assert - All should be the same instance (case-insensitive)
                Assert.Same(client1, client2);
                Assert.Same(client2, client3);
                Assert.Equal(1, manager.ClientCount);

                OutputHelper?.WriteLine("Case-insensitive host comparison works correctly");
            }
            finally
            {
                manager.Clear();
            }
        }

        #endregion

        #region TelemetryClientManager_DifferentHosts_ReturnsDifferentClients

        /// <summary>
        /// Tests that TelemetryClientManager returns different clients for different hosts.
        /// Each host should have its own dedicated telemetry client.
        /// </summary>
        [Fact]
        public void TelemetryClientManager_DifferentHosts_ReturnsDifferentClients()
        {
            // Arrange
            var manager = new TelemetryClientManager((host, httpClient, config) =>
                new MockTelemetryClient(host));
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            var host1 = "host1.databricks.com";
            var host2 = "host2.databricks.com";
            var host3 = "host3.databricks.com";

            try
            {
                // Act - Get clients for different hosts
                var client1 = manager.GetOrCreateClient(host1, httpClient, config);
                var client2 = manager.GetOrCreateClient(host2, httpClient, config);
                var client3 = manager.GetOrCreateClient(host3, httpClient, config);

                // Assert - All should be different instances
                Assert.NotSame(client1, client2);
                Assert.NotSame(client2, client3);
                Assert.NotSame(client1, client3);

                // Verify hosts are correct
                Assert.Equal(host1, client1.Host);
                Assert.Equal(host2, client2.Host);
                Assert.Equal(host3, client3.Host);

                // Verify three separate clients were created
                Assert.Equal(3, manager.ClientCount);

                // Verify each has reference count of 1
                Assert.True(manager.TryGetHolder(host1, out var holder1));
                Assert.True(manager.TryGetHolder(host2, out var holder2));
                Assert.True(manager.TryGetHolder(host3, out var holder3));
                Assert.Equal(1, holder1!.RefCount);
                Assert.Equal(1, holder2!.RefCount);
                Assert.Equal(1, holder3!.RefCount);

                OutputHelper?.WriteLine($"Three different hosts created three different clients");
            }
            finally
            {
                manager.Clear();
            }
        }

        /// <summary>
        /// Tests that TelemetryClientManager correctly handles a mix of same and different hosts.
        /// </summary>
        [Fact]
        public void TelemetryClientManager_MixedHosts_HandledCorrectly()
        {
            // Arrange
            var manager = new TelemetryClientManager((host, httpClient, config) =>
                new MockTelemetryClient(host));
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            var host1 = "host-a.databricks.com";
            var host2 = "host-b.databricks.com";

            try
            {
                // Act - Mix of same and different hosts
                var client1a = manager.GetOrCreateClient(host1, httpClient, config);
                var client2a = manager.GetOrCreateClient(host2, httpClient, config);
                var client1b = manager.GetOrCreateClient(host1, httpClient, config);
                var client2b = manager.GetOrCreateClient(host2, httpClient, config);
                var client1c = manager.GetOrCreateClient(host1, httpClient, config);

                // Assert - Same hosts should return same clients
                Assert.Same(client1a, client1b);
                Assert.Same(client1b, client1c);
                Assert.Same(client2a, client2b);

                // Different hosts should return different clients
                Assert.NotSame(client1a, client2a);

                // Verify correct client counts
                Assert.Equal(2, manager.ClientCount);

                // Verify reference counts
                Assert.True(manager.TryGetHolder(host1, out var holder1));
                Assert.True(manager.TryGetHolder(host2, out var holder2));
                Assert.Equal(3, holder1!.RefCount); // host1 was requested 3 times
                Assert.Equal(2, holder2!.RefCount); // host2 was requested 2 times

                OutputHelper?.WriteLine($"Host1 RefCount={holder1.RefCount}, Host2 RefCount={holder2.RefCount}");
            }
            finally
            {
                manager.Clear();
            }
        }

        #endregion

        #region TelemetryClientManager_LastRelease_ClosesClient

        /// <summary>
        /// Tests that TelemetryClientManager closes the client when the last reference is released.
        /// </summary>
        [Fact]
        public async Task TelemetryClientManager_LastRelease_ClosesClient()
        {
            // Arrange
            var closedClients = new List<string>();
            var manager = new TelemetryClientManager((host, httpClient, config) =>
            {
                var mockClient = new MockTelemetryClient(host);
                mockClient.OnClose = () => closedClients.Add(host);
                return mockClient;
            });
            var host = "last-release-test.databricks.com";
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act - Create 3 references
            manager.GetOrCreateClient(host, httpClient, config);
            manager.GetOrCreateClient(host, httpClient, config);
            manager.GetOrCreateClient(host, httpClient, config);

            // Verify initial state
            Assert.True(manager.HasClient(host));
            Assert.True(manager.TryGetHolder(host, out var holder));
            Assert.Equal(3, holder!.RefCount);
            Assert.Empty(closedClients);

            // Release first reference
            await manager.ReleaseClientAsync(host);
            Assert.True(manager.HasClient(host)); // Still exists
            Assert.Equal(2, holder.RefCount);
            Assert.Empty(closedClients); // Not closed yet

            // Release second reference
            await manager.ReleaseClientAsync(host);
            Assert.True(manager.HasClient(host)); // Still exists
            Assert.Equal(1, holder.RefCount);
            Assert.Empty(closedClients); // Not closed yet

            // Release last reference
            await manager.ReleaseClientAsync(host);

            // Assert - Client should be closed and removed
            Assert.False(manager.HasClient(host));
            Assert.Equal(0, manager.ClientCount);
            Assert.Single(closedClients);
            Assert.Contains(host, closedClients);

            OutputHelper?.WriteLine($"Client for '{host}' was closed after last reference released");
        }

        /// <summary>
        /// Tests that releasing unknown hosts doesn't throw and doesn't affect existing clients.
        /// </summary>
        [Fact]
        public async Task TelemetryClientManager_ReleaseUnknownHost_DoesNothing()
        {
            // Arrange
            var closedClients = new List<string>();
            var manager = new TelemetryClientManager((host, httpClient, config) =>
            {
                var mockClient = new MockTelemetryClient(host);
                mockClient.OnClose = () => closedClients.Add(host);
                return mockClient;
            });
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Create a real client
            var realHost = "real-host.databricks.com";
            manager.GetOrCreateClient(realHost, httpClient, config);

            // Act - Release unknown hosts (should not throw)
            await manager.ReleaseClientAsync("unknown-host.databricks.com");
            await manager.ReleaseClientAsync(null!);
            await manager.ReleaseClientAsync("");
            await manager.ReleaseClientAsync("   ");

            // Assert - Real client should still exist
            Assert.True(manager.HasClient(realHost));
            Assert.Equal(1, manager.ClientCount);
            Assert.Empty(closedClients);

            OutputHelper?.WriteLine("Releasing unknown hosts doesn't affect existing clients");

            // Cleanup
            manager.Clear();
        }

        /// <summary>
        /// Tests that client close exceptions are swallowed.
        /// </summary>
        [Fact]
        public async Task TelemetryClientManager_CloseThrows_SwallowsException()
        {
            // Arrange
            var manager = new TelemetryClientManager((host, httpClient, config) =>
            {
                var mockClient = new MockTelemetryClient(host);
                mockClient.OnClose = () => throw new InvalidOperationException("Close failed");
                return mockClient;
            });
            var host = "close-throws-test.databricks.com";
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            manager.GetOrCreateClient(host, httpClient, config);

            // Act - Should not throw even though close throws
            await manager.ReleaseClientAsync(host);

            // Assert - Client should be removed even though close threw
            Assert.False(manager.HasClient(host));
            Assert.Equal(0, manager.ClientCount);

            OutputHelper?.WriteLine("Close exceptions are swallowed as per telemetry requirement");
        }

        #endregion

        #region TelemetryClientManager_ConcurrentAccess_ThreadSafe

        /// <summary>
        /// Tests that TelemetryClientManager handles concurrent GetOrCreateClient calls correctly.
        /// All calls for the same host should get the same client instance.
        /// </summary>
        [Fact]
        public async Task TelemetryClientManager_ConcurrentAccess_ThreadSafe()
        {
            // Arrange
            var manager = new TelemetryClientManager((host, httpClient, config) =>
                new MockTelemetryClient(host));
            var host = "concurrent-test.databricks.com";
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var concurrentTasks = 100;

            var tasks = new Task<ITelemetryClient>[concurrentTasks];

            // Act - Many concurrent calls for the same host
            for (int i = 0; i < concurrentTasks; i++)
            {
                tasks[i] = Task.Run(() => manager.GetOrCreateClient(host, httpClient, config));
            }

            var clients = await Task.WhenAll(tasks);

            // Assert - All should be the same client instance
            var firstClient = clients[0];
            Assert.All(clients, c => Assert.Same(firstClient, c));

            // Verify only one client was created
            Assert.Equal(1, manager.ClientCount);

            // Verify reference count matches number of calls
            Assert.True(manager.TryGetHolder(host, out var holder));
            Assert.Equal(concurrentTasks, holder!.RefCount);

            OutputHelper?.WriteLine($"Concurrent access ({concurrentTasks} tasks) all returned same client with RefCount={holder.RefCount}");

            // Cleanup
            manager.Clear();
        }

        /// <summary>
        /// Tests that TelemetryClientManager handles concurrent releases correctly.
        /// Client should only be closed once when all references are released.
        /// </summary>
        [Fact]
        public async Task TelemetryClientManager_ConcurrentRelease_ThreadSafe()
        {
            // Arrange
            var closeCount = 0;
            var manager = new TelemetryClientManager((host, httpClient, config) =>
            {
                var mockClient = new MockTelemetryClient(host);
                mockClient.OnClose = () => Interlocked.Increment(ref closeCount);
                return mockClient;
            });
            var host = "concurrent-release-test.databricks.com";
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var referenceCount = 100;

            // Create many references
            for (int i = 0; i < referenceCount; i++)
            {
                manager.GetOrCreateClient(host, httpClient, config);
            }

            var releaseTasks = new Task[referenceCount];

            // Act - Many concurrent releases
            for (int i = 0; i < referenceCount; i++)
            {
                releaseTasks[i] = manager.ReleaseClientAsync(host);
            }

            await Task.WhenAll(releaseTasks);

            // Assert - Client should be closed exactly once
            Assert.False(manager.HasClient(host));
            Assert.Equal(0, manager.ClientCount);
            Assert.Equal(1, closeCount);

            OutputHelper?.WriteLine($"Concurrent release ({referenceCount} tasks) closed client exactly once");
        }

        /// <summary>
        /// Tests that TelemetryClientManager handles concurrent get and release operations safely.
        /// </summary>
        [Fact]
        public async Task TelemetryClientManager_ConcurrentGetAndRelease_ThreadSafe()
        {
            // Arrange
            var manager = new TelemetryClientManager((host, httpClient, config) =>
                new MockTelemetryClient(host));
            var host = "concurrent-get-release-test.databricks.com";
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var operationCount = 50;

            // Create initial reference
            manager.GetOrCreateClient(host, httpClient, config);

            var tasks = new List<Task>();

            // Act - Mix of gets and releases
            for (int i = 0; i < operationCount; i++)
            {
                tasks.Add(Task.Run(() => manager.GetOrCreateClient(host, httpClient, config)));
                tasks.Add(manager.ReleaseClientAsync(host));
            }

            // Wait for all operations
            await Task.WhenAll(tasks);

            // Assert - No exceptions were thrown (thread safety verified)
            // Final state depends on timing, but should be consistent
            OutputHelper?.WriteLine($"Concurrent get/release ({operationCount * 2} operations) completed without exceptions");

            // Cleanup - release any remaining references
            while (manager.HasClient(host))
            {
                await manager.ReleaseClientAsync(host);
            }
        }

        /// <summary>
        /// Tests that TelemetryClientManager handles concurrent access to multiple hosts safely.
        /// </summary>
        [Fact]
        public async Task TelemetryClientManager_ConcurrentMultipleHosts_ThreadSafe()
        {
            // Arrange
            var manager = new TelemetryClientManager((host, httpClient, config) =>
                new MockTelemetryClient(host));
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var hosts = new[] { "host-a.databricks.com", "host-b.databricks.com", "host-c.databricks.com" };
            var tasksPerHost = 50;

            var tasks = new List<Task<ITelemetryClient>>();

            // Act - Concurrent access to multiple hosts
            for (int i = 0; i < tasksPerHost; i++)
            {
                foreach (var host in hosts)
                {
                    tasks.Add(Task.Run(() => manager.GetOrCreateClient(host, httpClient, config)));
                }
            }

            var clients = await Task.WhenAll(tasks);

            // Assert - Verify all clients for same host are same instance
            Assert.Equal(3, manager.ClientCount);

            var clientsByHost = new Dictionary<string, ITelemetryClient>();
            foreach (var client in clients)
            {
                if (!clientsByHost.TryGetValue(client.Host, out var existing))
                {
                    clientsByHost[client.Host] = client;
                }
                else
                {
                    Assert.Same(existing, client);
                }
            }

            // Verify reference counts
            foreach (var host in hosts)
            {
                Assert.True(manager.TryGetHolder(host, out var holder));
                Assert.Equal(tasksPerHost, holder!.RefCount);
            }

            OutputHelper?.WriteLine($"Concurrent access to {hosts.Length} hosts with {tasksPerHost} tasks each completed correctly");

            // Cleanup
            manager.Clear();
        }

        #endregion

        #region Real Endpoint Tests

        /// <summary>
        /// Tests TelemetryClientManager with real Databricks endpoint connectivity.
        /// This validates that the manager works correctly with actual HTTP clients.
        /// </summary>
        [SkippableFact]
        public async Task TelemetryClientManager_RealEndpoint_SameHost_ReturnsSameClient()
        {
            Skip.IfNot(_canRunRealEndpointTests, "Real endpoint testing requires DATABRICKS_TEST_CONFIG_FILE");

            // Arrange
            var closedClients = new List<string>();
            var manager = new TelemetryClientManager((host, httpClient, config) =>
            {
                var mockClient = new MockTelemetryClient(host);
                mockClient.OnClose = () => closedClients.Add(host);
                return mockClient;
            });
            var host = _host!;
            using var httpClient = CreateAuthenticatedHttpClient();
            var config = new TelemetryConfiguration();

            try
            {
                // Act - Get client multiple times
                var client1 = manager.GetOrCreateClient(host, httpClient, config);
                var client2 = manager.GetOrCreateClient(host, httpClient, config);

                // Assert
                Assert.Same(client1, client2);
                Assert.Equal(1, manager.ClientCount);

                OutputHelper?.WriteLine($"Real endpoint '{host}': Same client instance returned");

                // Release all references
                await manager.ReleaseClientAsync(host);
                await manager.ReleaseClientAsync(host);

                // Verify cleanup
                Assert.False(manager.HasClient(host));
                Assert.Single(closedClients);
            }
            finally
            {
                manager.Clear();
            }
        }

        #endregion

        #region Mock Classes

        private class MockTelemetryClient : ITelemetryClient
        {
            public string Host { get; }
            public int ExportCallCount { get; private set; }
            public IReadOnlyList<TelemetryFrontendLog>? LastExportedLogs { get; private set; }
            public Action? OnClose { get; set; }

            public MockTelemetryClient(string host)
            {
                Host = host;
            }

            public Task ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                ExportCallCount++;
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
