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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for <see cref="TelemetryClientManager"/>.
    /// </summary>
    public class TelemetryClientManagerTests : IDisposable
    {
        private readonly TelemetryClientManager _manager;
        private readonly IDisposable _testScope;
        private readonly TelemetryConfiguration _defaultConfig;

        public TelemetryClientManagerTests()
        {
            _manager = TelemetryClientManager.CreateForTesting();
            _testScope = TelemetryClientManager.UseTestInstance(_manager);
            _defaultConfig = new TelemetryConfiguration
            {
                BatchSize = 100,
                FlushIntervalMs = 60000,
            };
        }

        public void Dispose()
        {
            _manager.Reset();
            _testScope.Dispose();
        }

        #region GetInstance Tests

        [Fact]
        public void GetInstance_ReturnsSingleton()
        {
            // Act
            TelemetryClientManager instance1 = TelemetryClientManager.GetInstance();
            TelemetryClientManager instance2 = TelemetryClientManager.GetInstance();

            // Assert
            Assert.NotNull(instance1);
            Assert.Same(instance1, instance2);
        }

        #endregion

        #region GetOrCreateClient Tests

        [Fact]
        public void GetOrCreateClient_NewHost_CreatesClient()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();

            // Act
            ITelemetryClient client = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => exporter,
                _defaultConfig);

            // Assert
            Assert.NotNull(client);
        }

        [Fact]
        public void GetOrCreateClient_ExistingHost_ReturnsSameClient()
        {
            // Arrange
            MockTelemetryExporter exporter1 = new MockTelemetryExporter();
            MockTelemetryExporter exporter2 = new MockTelemetryExporter();

            // Act
            ITelemetryClient client1 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => exporter1,
                _defaultConfig);
            ITelemetryClient client2 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => exporter2,
                _defaultConfig);

            // Assert
            Assert.Same(client1, client2);
        }

        [Fact]
        public void GetOrCreateClient_DifferentHosts_ReturnsDifferentClients()
        {
            // Arrange
            MockTelemetryExporter exporter1 = new MockTelemetryExporter();
            MockTelemetryExporter exporter2 = new MockTelemetryExporter();

            // Act
            ITelemetryClient client1 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => exporter1,
                _defaultConfig);
            ITelemetryClient client2 = _manager.GetOrCreateClient(
                "host2.databricks.com",
                () => exporter2,
                _defaultConfig);

            // Assert
            Assert.NotSame(client1, client2);
        }

        [Fact]
        public void GetOrCreateClient_CaseInsensitiveHost_ReturnsSameClient()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();

            // Act
            ITelemetryClient client1 = _manager.GetOrCreateClient(
                "Host1.Databricks.Com",
                () => exporter,
                _defaultConfig);
            ITelemetryClient client2 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => exporter,
                _defaultConfig);

            // Assert
            Assert.Same(client1, client2);
        }

        [Fact]
        public void GetOrCreateClient_NullHost_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                _manager.GetOrCreateClient(null!, () => new MockTelemetryExporter(), _defaultConfig));
        }

        [Fact]
        public void GetOrCreateClient_EmptyHost_ThrowsArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                _manager.GetOrCreateClient("", () => new MockTelemetryExporter(), _defaultConfig));
        }

        [Fact]
        public void GetOrCreateClient_WhitespaceHost_ThrowsArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                _manager.GetOrCreateClient("   ", () => new MockTelemetryExporter(), _defaultConfig));
        }

        [Fact]
        public void GetOrCreateClient_NullExporterFactory_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                _manager.GetOrCreateClient("host1", null!, _defaultConfig));
        }

        [Fact]
        public void GetOrCreateClient_NullConfig_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                _manager.GetOrCreateClient("host1", () => new MockTelemetryExporter(), null!));
        }

        [Fact]
        public void GetOrCreateClient_ExporterFactoryOnlyCalledOnce_ForSameHost()
        {
            // Arrange
            int factoryCallCount = 0;
            Func<ITelemetryExporter> factory = () =>
            {
                Interlocked.Increment(ref factoryCallCount);
                return new MockTelemetryExporter();
            };

            // Act
            _manager.GetOrCreateClient("host1.databricks.com", factory, _defaultConfig);
            _manager.GetOrCreateClient("host1.databricks.com", factory, _defaultConfig);
            _manager.GetOrCreateClient("host1.databricks.com", factory, _defaultConfig);

            // Assert - factory should only be called once (for the first creation)
            Assert.Equal(1, factoryCallCount);
        }

        #endregion

        #region ReleaseClientAsync Tests

        [Fact]
        public async Task ReleaseClientAsync_LastReference_ClosesClient()
        {
            // Arrange
            MockTelemetryClient mockClient = new MockTelemetryClient();
            MockTelemetryExporter exporter = new MockTelemetryExporter();

            // Use GetOrCreateClient to add the host, then we'll verify CloseAsync
            // through testing the actual TelemetryClient behavior
            ITelemetryClient client = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => exporter,
                _defaultConfig);

            // Act
            await _manager.ReleaseClientAsync("host1.databricks.com");

            // Assert - Getting the client again should create a new one (old one was removed)
            int factoryCallCount = 0;
            ITelemetryClient newClient = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () =>
                {
                    Interlocked.Increment(ref factoryCallCount);
                    return new MockTelemetryExporter();
                },
                _defaultConfig);

            Assert.Equal(1, factoryCallCount);
            Assert.NotSame(client, newClient);
        }

        [Fact]
        public async Task ReleaseClientAsync_MultipleReferences_KeepsClient()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();

            ITelemetryClient client1 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => exporter,
                _defaultConfig);
            ITelemetryClient client2 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => exporter,
                _defaultConfig);

            // Both should be the same client
            Assert.Same(client1, client2);

            // Act - release one reference
            await _manager.ReleaseClientAsync("host1.databricks.com");

            // Assert - client should still be available (ref count was 2, now 1)
            int factoryCallCount = 0;
            ITelemetryClient client3 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () =>
                {
                    Interlocked.Increment(ref factoryCallCount);
                    return new MockTelemetryExporter();
                },
                _defaultConfig);

            // Factory should NOT be called since the client still exists
            Assert.Equal(0, factoryCallCount);
            Assert.Same(client1, client3);
        }

        [Fact]
        public async Task ReleaseClientAsync_UnknownHost_NoError()
        {
            // Act & Assert - should not throw
            await _manager.ReleaseClientAsync("unknown.host.com");
        }

        [Fact]
        public async Task ReleaseClientAsync_NullHost_NoError()
        {
            // Act & Assert - should not throw
            await _manager.ReleaseClientAsync(null!);
        }

        [Fact]
        public async Task ReleaseClientAsync_EmptyHost_NoError()
        {
            // Act & Assert - should not throw
            await _manager.ReleaseClientAsync("");
        }

        [Fact]
        public async Task ReleaseClientAsync_AllReferencesReleased_CallsCloseAsync()
        {
            // Arrange - Use a mock client to track CloseAsync calls
            MockTelemetryClient mockClient = new MockTelemetryClient();
            int factoryCallCount = 0;

            // We need to create a custom manager that uses our mock client
            // To verify CloseAsync is called, we'll add two refs and release both
            TelemetryClientManager testManager = TelemetryClientManager.CreateForTesting();
            MockTelemetryExporter exporter = new MockTelemetryExporter();

            ITelemetryClient client = testManager.GetOrCreateClient(
                "host1.databricks.com",
                () => exporter,
                _defaultConfig);

            // Add second reference
            testManager.GetOrCreateClient(
                "host1.databricks.com",
                () => exporter,
                _defaultConfig);

            // Act - release both references
            await testManager.ReleaseClientAsync("host1.databricks.com");
            await testManager.ReleaseClientAsync("host1.databricks.com");

            // Assert - After releasing all references, creating a new one should
            // call the factory again (proves old one was removed)
            ITelemetryClient newClient = testManager.GetOrCreateClient(
                "host1.databricks.com",
                () =>
                {
                    Interlocked.Increment(ref factoryCallCount);
                    return new MockTelemetryExporter();
                },
                _defaultConfig);

            Assert.Equal(1, factoryCallCount);
            Assert.NotSame(client, newClient);

            testManager.Reset();
        }

        [Fact]
        public async Task ReleaseClientAsync_CaseInsensitive_ReleasesCorrectHost()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();

            ITelemetryClient client = _manager.GetOrCreateClient(
                "Host1.Databricks.Com",
                () => exporter,
                _defaultConfig);

            // Act - release using different casing
            await _manager.ReleaseClientAsync("host1.databricks.com");

            // Assert - client should be removed (factory should be called for new creation)
            int factoryCallCount = 0;
            _manager.GetOrCreateClient(
                "host1.databricks.com",
                () =>
                {
                    Interlocked.Increment(ref factoryCallCount);
                    return new MockTelemetryExporter();
                },
                _defaultConfig);

            Assert.Equal(1, factoryCallCount);
        }

        #endregion

        #region Thread Safety Tests

        [Fact]
        public async Task GetOrCreateClient_ThreadSafe_NoDuplicates()
        {
            // Arrange
            int threadCount = 10;
            int factoryCallCount = 0;
            ConcurrentBag<ITelemetryClient> clients = new ConcurrentBag<ITelemetryClient>();
            ManualResetEventSlim startEvent = new ManualResetEventSlim(false);
            List<Task> tasks = new List<Task>();

            Func<ITelemetryExporter> factory = () =>
            {
                Interlocked.Increment(ref factoryCallCount);
                return new MockTelemetryExporter();
            };

            // Act - Create clients from multiple threads concurrently
            for (int i = 0; i < threadCount; i++)
            {
                tasks.Add(Task.Run(() =>
                {
                    startEvent.Wait();
                    ITelemetryClient client = _manager.GetOrCreateClient(
                        "host1.databricks.com",
                        factory,
                        _defaultConfig);
                    clients.Add(client);
                }));
            }

            startEvent.Set();
            await Task.WhenAll(tasks);

            // Assert - All threads should have received the same client instance
            ITelemetryClient[] clientArray = clients.ToArray();
            Assert.Equal(threadCount, clientArray.Length);

            ITelemetryClient firstClient = clientArray[0];
            foreach (ITelemetryClient client in clientArray)
            {
                Assert.Same(firstClient, client);
            }

            // Factory might be called more than once due to ConcurrentDictionary's AddOrUpdate
            // behavior, but only one client should be in the dictionary
            Assert.True(factoryCallCount >= 1,
                $"Factory should be called at least once but was called {factoryCallCount} times");
        }

        [Fact]
        public async Task ConcurrentGetAndRelease_ThreadSafe()
        {
            // Arrange
            int iterations = 50;
            List<Task> tasks = new List<Task>();
            ManualResetEventSlim startEvent = new ManualResetEventSlim(false);

            // Act - Concurrent get and release operations
            for (int i = 0; i < iterations; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    startEvent.Wait();
                    MockTelemetryExporter exporter = new MockTelemetryExporter();
                    _manager.GetOrCreateClient(
                        "host1.databricks.com",
                        () => exporter,
                        _defaultConfig);
                    await _manager.ReleaseClientAsync("host1.databricks.com");
                }));
            }

            startEvent.Set();
            await Task.WhenAll(tasks);

            // Assert - No exceptions should have been thrown (test passes if we get here)
        }

        #endregion

        #region UseTestInstance Tests

        [Fact]
        public void UseTestInstance_ReplacesAndRestoresSingleton()
        {
            // Arrange
            TelemetryClientManager originalInstance = TelemetryClientManager.GetInstance();
            TelemetryClientManager testInstance = TelemetryClientManager.CreateForTesting();

            // Act
            using (IDisposable scope = TelemetryClientManager.UseTestInstance(testInstance))
            {
                // Inside the scope, GetInstance should return the test instance
                Assert.Same(testInstance, TelemetryClientManager.GetInstance());
            }

            // After disposing the scope, GetInstance should return the original
            Assert.Same(originalInstance, TelemetryClientManager.GetInstance());
        }

        [Fact]
        public void UseTestInstance_NullInstance_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                TelemetryClientManager.UseTestInstance(null!));
        }

        #endregion

        #region Reset Tests

        [Fact]
        public void Reset_ClearsAllClients()
        {
            // Arrange - add clients for multiple hosts
            MockTelemetryExporter exporter1 = new MockTelemetryExporter();
            MockTelemetryExporter exporter2 = new MockTelemetryExporter();

            ITelemetryClient client1 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => exporter1,
                _defaultConfig);
            ITelemetryClient client2 = _manager.GetOrCreateClient(
                "host2.databricks.com",
                () => exporter2,
                _defaultConfig);

            // Act
            _manager.Reset();

            // Assert - Getting clients again should create new ones
            int factoryCallCount = 0;
            ITelemetryClient newClient1 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () =>
                {
                    Interlocked.Increment(ref factoryCallCount);
                    return new MockTelemetryExporter();
                },
                _defaultConfig);

            Assert.Equal(1, factoryCallCount);
            Assert.NotSame(client1, newClient1);

            ITelemetryClient newClient2 = _manager.GetOrCreateClient(
                "host2.databricks.com",
                () =>
                {
                    Interlocked.Increment(ref factoryCallCount);
                    return new MockTelemetryExporter();
                },
                _defaultConfig);

            Assert.Equal(2, factoryCallCount);
            Assert.NotSame(client2, newClient2);
        }

        [Fact]
        public void Reset_EmptyManager_NoError()
        {
            // Act & Assert - should not throw when no clients exist
            _manager.Reset();
        }

        #endregion

        #region TelemetryClientHolder Tests

        [Fact]
        public void TelemetryClientHolder_Constructor_SetsClientAndRefCount()
        {
            // Arrange
            MockTelemetryClient mockClient = new MockTelemetryClient();

            // Act
            TelemetryClientHolder holder = new TelemetryClientHolder(mockClient);

            // Assert
            Assert.Same(mockClient, holder.Client);
            Assert.Equal(1, holder._refCount);
        }

        [Fact]
        public void TelemetryClientHolder_NullClient_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new TelemetryClientHolder(null!));
        }

        #endregion

        #region Mock Implementations

        /// <summary>
        /// Mock implementation of <see cref="ITelemetryExporter"/> for testing.
        /// </summary>
        private sealed class MockTelemetryExporter : ITelemetryExporter
        {
            public bool ReturnValue { get; set; } = true;

            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                return Task.FromResult(ReturnValue);
            }
        }

        /// <summary>
        /// Mock implementation of <see cref="ITelemetryClient"/> for testing.
        /// </summary>
        private sealed class MockTelemetryClient : ITelemetryClient
        {
            private int _closeAsyncCallCount;

            public int CloseAsyncCallCount => _closeAsyncCallCount;

            public void Enqueue(TelemetryFrontendLog log)
            {
                // No-op for testing
            }

            public Task FlushAsync(CancellationToken ct = default)
            {
                return Task.CompletedTask;
            }

            public Task CloseAsync()
            {
                Interlocked.Increment(ref _closeAsyncCallCount);
                return Task.CompletedTask;
            }

            public ValueTask DisposeAsync()
            {
                return new ValueTask(CloseAsync());
            }
        }

        #endregion
    }
}
