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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for TelemetryClientManager class.
    /// </summary>
    public class TelemetryClientManagerTests
    {
        /// <summary>
        /// Mock telemetry client for testing.
        /// </summary>
        private sealed class MockTelemetryClient : ITelemetryClient
        {
            public int EnqueueCount { get; private set; }
            public int FlushCount { get; private set; }
            public int CloseCount { get; private set; }
            public bool IsDisposed { get; private set; }

            public void Enqueue(TelemetryFrontendLog log)
            {
                EnqueueCount++;
            }

            public Task FlushAsync(CancellationToken ct = default)
            {
                FlushCount++;
                return Task.CompletedTask;
            }

            public Task CloseAsync()
            {
                CloseCount++;
                IsDisposed = true;
                return Task.CompletedTask;
            }

            public ValueTask DisposeAsync()
            {
                IsDisposed = true;
                return ValueTask.CompletedTask;
            }
        }

        /// <summary>
        /// Mock telemetry exporter for testing.
        /// </summary>
        private sealed class MockTelemetryExporter : ITelemetryExporter
        {
            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                return Task.FromResult(true);
            }
        }

        private static Dictionary<string, TelemetryClientHolder> GetClients(TelemetryClientManager manager)
        {
            System.Reflection.FieldInfo? clientsField = typeof(TelemetryClientManager)
                .GetField("_clients", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.NotNull(clientsField);
            Dictionary<string, TelemetryClientHolder>? clients =
                clientsField.GetValue(manager) as Dictionary<string, TelemetryClientHolder>;
            Assert.NotNull(clients);
            return clients;
        }

        [Fact]
        public void GetInstance_ReturnsSingleton()
        {
            // Act
            TelemetryClientManager instance1 = TelemetryClientManager.GetInstance();
            TelemetryClientManager instance2 = TelemetryClientManager.GetInstance();

            // Assert
            Assert.NotNull(instance1);
            Assert.NotNull(instance2);
            Assert.Same(instance1, instance2);
        }

        [Fact]
        public async Task GetOrCreateClient_NewHost_CreatesClient()
        {
            // Arrange - use test instance to avoid singleton pollution
            TelemetryClientManager manager = new TelemetryClientManager(forTesting: true);
            string host = "test-host.databricks.com";
            TelemetryConfiguration config = new TelemetryConfiguration();
            Dictionary<string, TelemetryClientHolder> clients = GetClients(manager);

            // Act
            ITelemetryClient client = manager.GetOrCreateClient(
                host,
                () => new MockTelemetryExporter(),
                config);

            try
            {
                // Assert
                Assert.NotNull(client);
                Assert.True(clients.ContainsKey(host));
                Assert.True(clients.TryGetValue(host, out TelemetryClientHolder? holder));
                Assert.NotNull(holder);
                Assert.Equal(1, holder._refCount);
            }
            finally
            {
                await manager.ReleaseClientAsync(host);
            }
        }

        [Fact]
        public async Task GetOrCreateClient_ExistingHost_ReturnsSameClient()
        {
            // Arrange
            TelemetryClientManager manager = new TelemetryClientManager(forTesting: true);
            string host = "test-host.databricks.com";
            TelemetryConfiguration config = new TelemetryConfiguration();
            Dictionary<string, TelemetryClientHolder> clients = GetClients(manager);

            // Act
            ITelemetryClient client1 = manager.GetOrCreateClient(
                host,
                () => new MockTelemetryExporter(),
                config);
            ITelemetryClient client2 = manager.GetOrCreateClient(
                host,
                () => new MockTelemetryExporter(),
                config);

            try
            {
                // Assert
                Assert.NotNull(client1);
                Assert.NotNull(client2);
                Assert.Same(client1, client2);
                Assert.True(clients.TryGetValue(host, out TelemetryClientHolder? holder));
                Assert.NotNull(holder);
                Assert.Equal(2, holder._refCount);
            }
            finally
            {
                await manager.ReleaseClientAsync(host);
                await manager.ReleaseClientAsync(host);
            }
        }

        [Fact]
        public async Task ReleaseClientAsync_LastReference_ClosesClient()
        {
            // Arrange
            TelemetryClientManager manager = new TelemetryClientManager(forTesting: true);
            string host = "test-host.databricks.com";
            MockTelemetryClient mockClient = new MockTelemetryClient();
            Dictionary<string, TelemetryClientHolder> clients = GetClients(manager);

            // Add mock client directly to dictionary
            TelemetryClientHolder holder = new TelemetryClientHolder(mockClient);
            clients[host] = holder;

            // Act
            await manager.ReleaseClientAsync(host);

            // Assert
            Assert.Equal(0, holder._refCount);
            Assert.False(clients.ContainsKey(host));
            Assert.Equal(1, mockClient.CloseCount);
            Assert.True(mockClient.IsDisposed);
        }

        [Fact]
        public async Task ReleaseClientAsync_MultipleReferences_KeepsClient()
        {
            // Arrange
            TelemetryClientManager manager = new TelemetryClientManager(forTesting: true);
            string host = "test-host.databricks.com";
            MockTelemetryClient mockClient = new MockTelemetryClient();
            Dictionary<string, TelemetryClientHolder> clients = GetClients(manager);

            // Add mock client with RefCount=2
            TelemetryClientHolder holder = new TelemetryClientHolder(mockClient);
            holder._refCount = 2;
            clients[host] = holder;

            // Act - release first reference
            await manager.ReleaseClientAsync(host);

            // Assert - client still exists
            Assert.Equal(1, holder._refCount);
            Assert.True(clients.ContainsKey(host));
            Assert.Equal(0, mockClient.CloseCount);
            Assert.False(mockClient.IsDisposed);

            // Cleanup
            await manager.ReleaseClientAsync(host);
        }

        [Fact]
        public async Task GetOrCreateClient_ThreadSafe_NoDuplicates()
        {
            // Arrange
            TelemetryClientManager manager = new TelemetryClientManager(forTesting: true);
            string host = "test-host.databricks.com";
            TelemetryConfiguration config = new TelemetryConfiguration();
            int threadCount = 10;
            List<ITelemetryClient> clientsList = new List<ITelemetryClient>();
            object lockObj = new object();
            Dictionary<string, TelemetryClientHolder> clients = GetClients(manager);

            // Act - create clients concurrently from multiple threads
            Task[] tasks = Enumerable.Range(0, threadCount).Select(_ => Task.Run(() =>
            {
                ITelemetryClient client = manager.GetOrCreateClient(
                    host,
                    () => new MockTelemetryExporter(),
                    config);
                lock (lockObj)
                {
                    clientsList.Add(client);
                }
            })).ToArray();

            await Task.WhenAll(tasks);

            try
            {
                // Assert - all clients should be the same instance
                Assert.Equal(threadCount, clientsList.Count);
                ITelemetryClient firstClient = clientsList[0];
                foreach (ITelemetryClient client in clientsList)
                {
                    Assert.Same(firstClient, client);
                }

                // Assert - ref count should be incremented correctly
                Assert.True(clients.TryGetValue(host, out TelemetryClientHolder? holder));
                Assert.NotNull(holder);
                Assert.Equal(threadCount, holder._refCount);
            }
            finally
            {
                // Cleanup - release all references
                for (int i = 0; i < threadCount; i++)
                {
                    await manager.ReleaseClientAsync(host);
                }
            }
        }

        [Fact]
        public async Task ReleaseClientAsync_NonExistentHost_NoError()
        {
            // Arrange
            TelemetryClientManager manager = new TelemetryClientManager(forTesting: true);
            string host = "non-existent-host.databricks.com";

            // Act & Assert - should not throw
            await manager.ReleaseClientAsync(host);
        }

        [Fact]
        public async Task GetOrCreateClient_ThenRelease_MultipleHosts()
        {
            // Arrange
            TelemetryClientManager manager = new TelemetryClientManager(forTesting: true);
            string host1 = "test-host1.databricks.com";
            string host2 = "test-host2.databricks.com";
            TelemetryConfiguration config = new TelemetryConfiguration();
            Dictionary<string, TelemetryClientHolder> clients = GetClients(manager);

            // Act - create clients for two different hosts
            ITelemetryClient client1 = manager.GetOrCreateClient(
                host1,
                () => new MockTelemetryExporter(),
                config);
            ITelemetryClient client2 = manager.GetOrCreateClient(
                host2,
                () => new MockTelemetryExporter(),
                config);

            // Assert - should have separate clients
            Assert.NotSame(client1, client2);
            Assert.True(clients.ContainsKey(host1));
            Assert.True(clients.ContainsKey(host2));

            // Act - release both clients
            await manager.ReleaseClientAsync(host1);
            await manager.ReleaseClientAsync(host2);

            // Assert - both clients should be removed
            Assert.False(clients.ContainsKey(host1));
            Assert.False(clients.ContainsKey(host2));
        }
    }
}
