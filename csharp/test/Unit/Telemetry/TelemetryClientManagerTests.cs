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
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for TelemetryClientManager, TelemetryClientHolder, TelemetryClientAdapter, and ITelemetryClient.
    /// </summary>
    public class TelemetryClientManagerTests
    {
        #region TelemetryClientHolder Tests

        [Fact]
        public void TelemetryClientHolder_Constructor_InitializesCorrectly()
        {
            // Arrange
            var mockClient = new MockTelemetryClient("test-host");

            // Act
            var holder = new TelemetryClientHolder(mockClient);

            // Assert
            Assert.Same(mockClient, holder.Client);
            Assert.Equal(0, holder.RefCount);
        }

        [Fact]
        public void TelemetryClientHolder_Constructor_NullClient_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new TelemetryClientHolder(null!));
        }

        [Fact]
        public void TelemetryClientHolder_IncrementRefCount_IncrementsCorrectly()
        {
            // Arrange
            var mockClient = new MockTelemetryClient("test-host");
            var holder = new TelemetryClientHolder(mockClient);

            // Act & Assert
            Assert.Equal(0, holder.RefCount);
            Assert.Equal(1, holder.IncrementRefCount());
            Assert.Equal(1, holder.RefCount);
            Assert.Equal(2, holder.IncrementRefCount());
            Assert.Equal(2, holder.RefCount);
        }

        [Fact]
        public void TelemetryClientHolder_DecrementRefCount_DecrementsCorrectly()
        {
            // Arrange
            var mockClient = new MockTelemetryClient("test-host");
            var holder = new TelemetryClientHolder(mockClient);
            holder.IncrementRefCount();
            holder.IncrementRefCount();

            // Act & Assert
            Assert.Equal(2, holder.RefCount);
            Assert.Equal(1, holder.DecrementRefCount());
            Assert.Equal(1, holder.RefCount);
            Assert.Equal(0, holder.DecrementRefCount());
            Assert.Equal(0, holder.RefCount);
        }

        #endregion

        #region TelemetryClientAdapter Tests

        [Fact]
        public void TelemetryClientAdapter_Constructor_InitializesCorrectly()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();

            // Act
            var adapter = new TelemetryClientAdapter("test-host", mockExporter);

            // Assert
            Assert.Equal("test-host", adapter.Host);
            Assert.Same(mockExporter, adapter.Exporter);
        }

        [Fact]
        public void TelemetryClientAdapter_Constructor_NullHost_ThrowsException()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new TelemetryClientAdapter(null!, mockExporter));
        }

        [Fact]
        public void TelemetryClientAdapter_Constructor_EmptyHost_ThrowsException()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new TelemetryClientAdapter("", mockExporter));
        }

        [Fact]
        public void TelemetryClientAdapter_Constructor_WhitespaceHost_ThrowsException()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new TelemetryClientAdapter("   ", mockExporter));
        }

        [Fact]
        public void TelemetryClientAdapter_Constructor_NullExporter_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new TelemetryClientAdapter("test-host", null!));
        }

        [Fact]
        public async Task TelemetryClientAdapter_ExportAsync_DelegatesToExporter()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var adapter = new TelemetryClientAdapter("test-host", mockExporter);
            var logs = new List<TelemetryFrontendLog> { CreateTestLog() };

            // Act
            await adapter.ExportAsync(logs);

            // Assert
            Assert.Equal(1, mockExporter.ExportCallCount);
            Assert.Same(logs, mockExporter.LastExportedLogs);
        }

        [Fact]
        public async Task TelemetryClientAdapter_CloseAsync_CompletesWithoutException()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var adapter = new TelemetryClientAdapter("test-host", mockExporter);

            // Act & Assert - should not throw
            await adapter.CloseAsync();
        }

        #endregion

        #region TelemetryClientManager Singleton Tests

        [Fact]
        public void TelemetryClientManager_GetInstance_ReturnsSingleton()
        {
            // Act
            var instance1 = TelemetryClientManager.GetInstance();
            var instance2 = TelemetryClientManager.GetInstance();

            // Assert
            Assert.Same(instance1, instance2);
        }

        #endregion

        #region TelemetryClientManager_GetOrCreateClient Tests

        [Fact]
        public void TelemetryClientManager_GetOrCreateClient_NewHost_CreatesClient()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();
            var host = "test-host-1.databricks.com";
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act
            var client = manager.GetOrCreateClient(host, httpClient, config);

            // Assert
            Assert.NotNull(client);
            Assert.Equal(host, client.Host);
            Assert.True(manager.HasClient(host));
            Assert.Equal(1, manager.ClientCount);
        }

        [Fact]
        public void TelemetryClientManager_GetOrCreateClient_ExistingHost_ReturnsSameClient()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();
            var host = "test-host-2.databricks.com";
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act
            var client1 = manager.GetOrCreateClient(host, httpClient, config);
            var client2 = manager.GetOrCreateClient(host, httpClient, config);

            // Assert
            Assert.Same(client1, client2);
            Assert.Equal(1, manager.ClientCount);

            // Verify reference count incremented
            manager.TryGetHolder(host, out var holder);
            Assert.Equal(2, holder!.RefCount);
        }

        [Fact]
        public void TelemetryClientManager_GetOrCreateClient_MultipleHosts_CreatesMultipleClients()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();
            var host1 = "host1.databricks.com";
            var host2 = "host2.databricks.com";
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act
            var client1 = manager.GetOrCreateClient(host1, httpClient, config);
            var client2 = manager.GetOrCreateClient(host2, httpClient, config);

            // Assert
            Assert.NotSame(client1, client2);
            Assert.Equal(host1, client1.Host);
            Assert.Equal(host2, client2.Host);
            Assert.Equal(2, manager.ClientCount);
        }

        [Fact]
        public void TelemetryClientManager_GetOrCreateClient_NullHost_ThrowsException()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => manager.GetOrCreateClient(null!, httpClient, config));
        }

        [Fact]
        public void TelemetryClientManager_GetOrCreateClient_EmptyHost_ThrowsException()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => manager.GetOrCreateClient("", httpClient, config));
        }

        [Fact]
        public void TelemetryClientManager_GetOrCreateClient_WhitespaceHost_ThrowsException()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => manager.GetOrCreateClient("   ", httpClient, config));
        }

        [Fact]
        public void TelemetryClientManager_GetOrCreateClient_NullHttpClient_ThrowsException()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => manager.GetOrCreateClient("host", null!, config));
        }

        [Fact]
        public void TelemetryClientManager_GetOrCreateClient_NullConfig_ThrowsException()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();
            var httpClient = new HttpClient();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => manager.GetOrCreateClient("host", httpClient, null!));
        }

        [Fact]
        public void TelemetryClientManager_GetOrCreateClient_CaseInsensitive()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();
            var host = "Test-Host.Databricks.com";
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act
            var client1 = manager.GetOrCreateClient(host.ToLower(), httpClient, config);
            var client2 = manager.GetOrCreateClient(host.ToUpper(), httpClient, config);

            // Assert
            Assert.Same(client1, client2);
            Assert.Equal(1, manager.ClientCount);
        }

        #endregion

        #region TelemetryClientManager_ReleaseClientAsync Tests

        [Fact]
        public async Task TelemetryClientManager_ReleaseClientAsync_LastReference_ClosesClient()
        {
            // Arrange
            var closedClients = new List<string>();
            var manager = new TelemetryClientManager((host, httpClient, config) =>
            {
                var mockClient = new MockTelemetryClient(host);
                mockClient.OnClose = () => closedClients.Add(host);
                return mockClient;
            });
            var host = "test-host-3.databricks.com";
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var client = manager.GetOrCreateClient(host, httpClient, config);

            // Act
            await manager.ReleaseClientAsync(host);

            // Assert
            Assert.False(manager.HasClient(host));
            Assert.Equal(0, manager.ClientCount);
            Assert.Contains(host, closedClients);
        }

        [Fact]
        public async Task TelemetryClientManager_ReleaseClientAsync_MultipleReferences_KeepsClient()
        {
            // Arrange
            var closedClients = new List<string>();
            var manager = new TelemetryClientManager((host, httpClient, config) =>
            {
                var mockClient = new MockTelemetryClient(host);
                mockClient.OnClose = () => closedClients.Add(host);
                return mockClient;
            });
            var host = "test-host-4.databricks.com";
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            manager.GetOrCreateClient(host, httpClient, config);
            manager.GetOrCreateClient(host, httpClient, config); // Second reference

            // Act
            await manager.ReleaseClientAsync(host);

            // Assert
            Assert.True(manager.HasClient(host));
            manager.TryGetHolder(host, out var holder);
            Assert.Equal(1, holder!.RefCount);
            Assert.Empty(closedClients); // Client not closed
        }

        [Fact]
        public async Task TelemetryClientManager_ReleaseClientAsync_UnknownHost_DoesNothing()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();

            // Act - should not throw
            await manager.ReleaseClientAsync("unknown-host.databricks.com");

            // Assert
            Assert.Equal(0, manager.ClientCount);
        }

        [Fact]
        public async Task TelemetryClientManager_ReleaseClientAsync_NullHost_DoesNothing()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();

            // Act - should not throw
            await manager.ReleaseClientAsync(null!);

            // Assert - no exception thrown
        }

        [Fact]
        public async Task TelemetryClientManager_ReleaseClientAsync_EmptyHost_DoesNothing()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();

            // Act - should not throw
            await manager.ReleaseClientAsync("");

            // Assert - no exception thrown
        }

        [Fact]
        public async Task TelemetryClientManager_ReleaseClientAsync_AllReleased_ClosesClient()
        {
            // Arrange
            var closedClients = new List<string>();
            var manager = new TelemetryClientManager((host, httpClient, config) =>
            {
                var mockClient = new MockTelemetryClient(host);
                mockClient.OnClose = () => closedClients.Add(host);
                return mockClient;
            });
            var host = "test-host-5.databricks.com";
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Create 3 references
            manager.GetOrCreateClient(host, httpClient, config);
            manager.GetOrCreateClient(host, httpClient, config);
            manager.GetOrCreateClient(host, httpClient, config);
            Assert.Equal(1, manager.ClientCount);

            // Act - Release all
            await manager.ReleaseClientAsync(host);
            Assert.True(manager.HasClient(host)); // Still has 2 references

            await manager.ReleaseClientAsync(host);
            Assert.True(manager.HasClient(host)); // Still has 1 reference

            await manager.ReleaseClientAsync(host);

            // Assert
            Assert.False(manager.HasClient(host));
            Assert.Equal(0, manager.ClientCount);
            Assert.Single(closedClients);
        }

        [Fact]
        public async Task TelemetryClientManager_ReleaseClientAsync_CloseThrows_SwallowsException()
        {
            // Arrange
            var manager = new TelemetryClientManager((host, httpClient, config) =>
            {
                var mockClient = new MockTelemetryClient(host);
                mockClient.OnClose = () => throw new InvalidOperationException("Close failed");
                return mockClient;
            });
            var host = "test-host-6.databricks.com";
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            manager.GetOrCreateClient(host, httpClient, config);

            // Act - should not throw
            await manager.ReleaseClientAsync(host);

            // Assert - client was removed despite exception
            Assert.False(manager.HasClient(host));
        }

        #endregion

        #region TelemetryClientManager Thread Safety Tests

        [Fact]
        public async Task TelemetryClientManager_ConcurrentGetOrCreateClient_ThreadSafe_NoDuplicates()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();
            var host = "concurrent-host.databricks.com";
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var tasks = new Task<ITelemetryClient>[100];

            // Act
            for (int i = 0; i < 100; i++)
            {
                tasks[i] = Task.Run(() => manager.GetOrCreateClient(host, httpClient, config));
            }

            var clients = await Task.WhenAll(tasks);

            // Assert - All should be the same client
            var firstClient = clients[0];
            Assert.All(clients, c => Assert.Same(firstClient, c));
            Assert.Equal(1, manager.ClientCount);

            // Verify reference count
            manager.TryGetHolder(host, out var holder);
            Assert.Equal(100, holder!.RefCount);
        }

        [Fact]
        public async Task TelemetryClientManager_ConcurrentReleaseClient_ThreadSafe()
        {
            // Arrange
            var closeCount = 0;
            var manager = new TelemetryClientManager((host, httpClient, config) =>
            {
                var mockClient = new MockTelemetryClient(host);
                mockClient.OnClose = () => Interlocked.Increment(ref closeCount);
                return mockClient;
            });
            var host = "concurrent-release-host.databricks.com";
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Create 100 references
            for (int i = 0; i < 100; i++)
            {
                manager.GetOrCreateClient(host, httpClient, config);
            }

            var tasks = new Task[100];

            // Act - Release all concurrently
            for (int i = 0; i < 100; i++)
            {
                tasks[i] = manager.ReleaseClientAsync(host);
            }

            await Task.WhenAll(tasks);

            // Assert - Client should be closed (exactly once)
            Assert.False(manager.HasClient(host));
            Assert.Equal(1, closeCount);
        }

        [Fact]
        public async Task TelemetryClientManager_ConcurrentGetAndRelease_ThreadSafe()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();
            var host = "get-release-host.databricks.com";
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Create initial reference
            manager.GetOrCreateClient(host, httpClient, config);

            var tasks = new List<Task>();

            // Act - Mix of gets and releases
            for (int i = 0; i < 50; i++)
            {
                tasks.Add(Task.Run(() => manager.GetOrCreateClient(host, httpClient, config)));
                tasks.Add(manager.ReleaseClientAsync(host));
            }

            await Task.WhenAll(tasks);

            // Assert - No exceptions thrown (thread safety verified)
            // Final state depends on timing, but should be consistent
        }

        #endregion

        #region TelemetryClientManager Helper Method Tests

        [Fact]
        public void TelemetryClientManager_TryGetHolder_ExistingHost_ReturnsTrue()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();
            var host = "try-get-host.databricks.com";
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            manager.GetOrCreateClient(host, httpClient, config);

            // Act
            var result = manager.TryGetHolder(host, out var holder);

            // Assert
            Assert.True(result);
            Assert.NotNull(holder);
            Assert.Equal(1, holder!.RefCount);
        }

        [Fact]
        public void TelemetryClientManager_TryGetHolder_UnknownHost_ReturnsFalse()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();

            // Act
            var result = manager.TryGetHolder("unknown.databricks.com", out var holder);

            // Assert
            Assert.False(result);
            Assert.Null(holder);
        }

        [Fact]
        public void TelemetryClientManager_TryGetHolder_NullHost_ReturnsFalse()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();

            // Act
            var result = manager.TryGetHolder(null!, out var holder);

            // Assert
            Assert.False(result);
            Assert.Null(holder);
        }

        [Fact]
        public void TelemetryClientManager_HasClient_NullHost_ReturnsFalse()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();

            // Act
            var result = manager.HasClient(null!);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void TelemetryClientManager_Clear_RemovesAllClients()
        {
            // Arrange
            var manager = CreateManagerWithMockFactory();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            manager.GetOrCreateClient("host1.databricks.com", httpClient, config);
            manager.GetOrCreateClient("host2.databricks.com", httpClient, config);
            manager.GetOrCreateClient("host3.databricks.com", httpClient, config);
            Assert.Equal(3, manager.ClientCount);

            // Act
            manager.Clear();

            // Assert
            Assert.Equal(0, manager.ClientCount);
        }

        #endregion

        #region Helper Methods

        private TelemetryClientManager CreateManagerWithMockFactory()
        {
            return new TelemetryClientManager((host, httpClient, config) =>
                new MockTelemetryClient(host));
        }

        private TelemetryFrontendLog CreateTestLog()
        {
            return new TelemetryFrontendLog
            {
                FrontendLogEventId = "test-event-id",
                WorkspaceId = 12345
            };
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

        private class MockTelemetryExporter : ITelemetryExporter
        {
            public int ExportCallCount { get; private set; }
            public IReadOnlyList<TelemetryFrontendLog>? LastExportedLogs { get; private set; }

            public Task ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                ExportCallCount++;
                LastExportedLogs = logs;
                return Task.CompletedTask;
            }
        }

        #endregion
    }
}
