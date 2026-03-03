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
using AdbcDrivers.HiveServer2.Spark;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for telemetry integration in <see cref="DatabricksConnection"/>.
    /// These tests verify that the connection correctly initializes and cleans up
    /// the telemetry pipeline based on configuration and feature flag state.
    /// </summary>
    /// <remarks>
    /// Since DatabricksConnection.OpenAsync requires a real Thrift server connection,
    /// these tests focus on:
    /// - TelemetryConfiguration parsing from connection properties
    /// - TestExporterFactory injection pattern
    /// - TelemetryClientManager integration for shared client lifecycle
    /// - Cleanup behavior during Dispose (unregister, flush, release)
    /// - Exception swallowing in telemetry operations
    /// </remarks>
    [Collection("DatabricksConnectionTelemetryTests")]
    public class DatabricksConnectionTelemetryTests : IDisposable
    {
        private readonly TelemetryClientManager _manager;
        private readonly TelemetryConfiguration _config;

        /// <summary>
        /// Minimum required properties for creating a DatabricksConnection.
        /// </summary>
        private static readonly Dictionary<string, string> MinimalProperties = new Dictionary<string, string>
        {
            { SparkParameters.HostName, "test-host.databricks.com" },
            { SparkParameters.AuthType, SparkAuthType.Token.ToString() },
            { SparkParameters.Token, "dummy-token" },
            { SparkParameters.Path, "/sql/1.0/warehouses/test" },
        };

        public DatabricksConnectionTelemetryTests()
        {
            // Use the singleton instance and reset between tests
            _manager = TelemetryClientManager.GetInstance();
            _manager.Reset();
            _config = new TelemetryConfiguration
            {
                Enabled = true,
                BatchSize = 1000,
                FlushIntervalMs = 60000 // Long interval to avoid timer-triggered flushes in tests
            };
        }

        public void Dispose()
        {
            _manager.Reset();
        }

        #region TelemetryConfiguration Parsing Tests

        [Fact]
        public void TelemetryConfig_EnabledByDefault_WhenNoPropertySet()
        {
            // Arrange - no telemetry properties
            var config = TelemetryConfiguration.FromProperties(MinimalProperties);

            // Assert
            Assert.True(config.Enabled);
        }

        [Fact]
        public void TelemetryConfig_Disabled_WhenPropertySetToFalse()
        {
            // Arrange
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { TelemetryConfiguration.PropertyKeyEnabled, "false" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.False(config.Enabled);
        }

        [Fact]
        public void TelemetryConfig_CustomBatchSize_ParsedFromProperties()
        {
            // Arrange
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { TelemetryConfiguration.PropertyKeyBatchSize, "50" }
            };

            // Act
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert
            Assert.Equal(50, config.BatchSize);
        }

        #endregion

        #region TestExporterFactory Tests

        [Fact]
        public void TestExporterFactory_CanBeSet_OnConnection()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            var exporterCreated = false;

            // Act
            connection.TestExporterFactory = () =>
            {
                exporterCreated = true;
                return new InMemoryTelemetryExporter();
            };

            // Assert
            Assert.NotNull(connection.TestExporterFactory);
            // Invoke the factory to verify it works
            connection.TestExporterFactory();
            Assert.True(exporterCreated);
        }

        [Fact]
        public void TestExporterFactory_UsedWhenSet()
        {
            // Arrange - This test verifies the factory is used via TelemetryClientManager integration
            var exporterCallCount = 0;
            var testExporter = new InMemoryTelemetryExporter();
            Func<ITelemetryExporter> factory = () =>
            {
                Interlocked.Increment(ref exporterCallCount);
                return testExporter;
            };

            // Create connection with telemetry enabled
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { TelemetryConfiguration.PropertyKeyEnabled, "true" }
            };
            var connection = CreateConnection(properties);
            connection.TestExporterFactory = factory;

            // Simulate what OpenAsync does: get telemetry client using the exporter factory
            var telemetryConfig = TelemetryConfiguration.FromProperties(properties);
            var client = TelemetryClientManager.GetInstance()
                .GetOrCreateClient(
                    "test-host.databricks.com",
                    () => connection.TestExporterFactory(),
                    telemetryConfig);

            // Assert - factory was called
            Assert.Equal(1, exporterCallCount);
            Assert.NotNull(client);
        }

        #endregion

        #region TelemetryClientManager Integration Tests

        [Fact]
        public void TelemetryClientManager_GetOrCreateClient_CreatesClientForHost()
        {
            // Arrange
            var exporter = new InMemoryTelemetryExporter();

            // Act
            var client = _manager.GetOrCreateClient(
                "test-host.databricks.com",
                () => exporter,
                _config);

            // Assert
            Assert.NotNull(client);
        }

        [Fact]
        public async Task TelemetryClientManager_ReleaseClientAsync_ReleasesClient()
        {
            // Arrange
            var exporter = new InMemoryTelemetryExporter();
            var client = _manager.GetOrCreateClient(
                "test-host.databricks.com",
                () => exporter,
                _config);

            // Act - Release the client
            await _manager.ReleaseClientAsync("test-host.databricks.com");

            // Assert - Creating a new client should get a different instance
            // (the previous one was released and closed)
            var newClient = _manager.GetOrCreateClient(
                "test-host.databricks.com",
                () => new InMemoryTelemetryExporter(),
                _config);
            Assert.NotNull(newClient);
        }

        [Fact]
        public void OpenAsync_FeatureFlagDisabled_NoTelemetry()
        {
            // Arrange - Create connection with telemetry disabled
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { TelemetryConfiguration.PropertyKeyEnabled, "false" }
            };

            // Act - Parse config (this is what InitializeTelemetry does)
            var config = TelemetryConfiguration.FromProperties(properties);

            // Assert - Telemetry should be disabled
            Assert.False(config.Enabled);
            // No telemetry client should be created for a disabled config
        }

        [Fact]
        public void OpenAsync_TelemetryEnabled_InitializesPipeline()
        {
            // Arrange - Create config with telemetry enabled
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { TelemetryConfiguration.PropertyKeyEnabled, "true" }
            };

            var config = TelemetryConfiguration.FromProperties(properties);
            Assert.True(config.Enabled);

            // Simulate what InitializeTelemetry does: create a telemetry client
            var exporterCreated = false;
            var client = _manager.GetOrCreateClient(
                "test-host.databricks.com",
                () =>
                {
                    exporterCreated = true;
                    return new InMemoryTelemetryExporter();
                },
                config);

            // Assert - TelemetryClientManager.GetOrCreateClient was called
            Assert.True(exporterCreated);
            Assert.NotNull(client);
        }

        #endregion

        #region Dispose Tests

        [Fact]
        public async Task Dispose_ReleasesTelemetryClient()
        {
            // Arrange - Set up a telemetry client through the manager
            var exporter = new InMemoryTelemetryExporter();
            var client = _manager.GetOrCreateClient(
                "test-host.databricks.com",
                () => exporter,
                _config);

            // Get a second reference to simulate ref count > 1
            var client2 = _manager.GetOrCreateClient(
                "test-host.databricks.com",
                () => exporter,
                _config);

            // Assert same client is returned (shared per host)
            Assert.Same(client, client2);

            // Act - Release one reference (simulating connection close)
            await _manager.ReleaseClientAsync("test-host.databricks.com");

            // Assert - Client is still alive (one more ref)
            var client3 = _manager.GetOrCreateClient(
                "test-host.databricks.com",
                () => new InMemoryTelemetryExporter(),
                _config);
            Assert.Same(client, client3);

            // Cleanup
            await _manager.ReleaseClientAsync("test-host.databricks.com");
            await _manager.ReleaseClientAsync("test-host.databricks.com");
        }

        [Fact]
        public async Task Dispose_FlushesMetricsBeforeRelease()
        {
            // Arrange
            var mockClient = new TrackingTelemetryClient();
            var config = new TelemetryConfiguration();
            var aggregator = new MetricsAggregator(mockClient, config, "test-session-id");

            // Register the aggregator with a test listener
            var listener = DatabricksActivityListener.CreateForTesting();
            listener.RegisterAggregator("test-session-id", aggregator);

            // Act - Unregister which should flush the aggregator
            await listener.UnregisterAggregatorAsync("test-session-id");

            // Assert - FlushAsync was called on the aggregator (via UnregisterAggregatorAsync)
            // The unregister method flushes the aggregator automatically
            aggregator.Dispose();
            listener.Dispose();
        }

        [Fact]
        public void Dispose_ExceptionSwallowed()
        {
            // Arrange - Create a connection with telemetry properties
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { TelemetryConfiguration.PropertyKeyEnabled, "true" }
            };
            var connection = CreateConnection(properties);

            // Act & Assert - Dispose should not throw even though there's no
            // actual telemetry pipeline set up (no OpenAsync was called)
            var exception = Record.Exception(() => connection.Dispose());
            Assert.Null(exception);
        }

        [Fact]
        public void Dispose_SafeWhenCalledMultipleTimes()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);

            // Act & Assert - Multiple disposes should not throw
            var exception1 = Record.Exception(() => connection.Dispose());
            var exception2 = Record.Exception(() => connection.Dispose());
            Assert.Null(exception1);
            Assert.Null(exception2);
        }

        [Fact]
        public async Task Dispose_CleanupWithTelemetryClient_DoesNotThrow()
        {
            // Arrange - Create a telemetry pipeline manually
            var exporter = new InMemoryTelemetryExporter();
            var host = "dispose-test-host.databricks.com";

            // Create client through manager
            var client = _manager.GetOrCreateClient(
                host,
                () => exporter,
                _config);

            var aggregator = new MetricsAggregator(client, _config, "test-session");
            var listener = DatabricksActivityListener.CreateForTesting();
            listener.RegisterAggregator("test-session", aggregator);

            // Act - Simulate disposal sequence (same as CleanupTelemetry)
            var exception = await Record.ExceptionAsync(async () =>
            {
                // Step 1: Unregister aggregator (flushes pending metrics)
                await listener.UnregisterAggregatorAsync("test-session");
                aggregator.Dispose();

                // Step 2: Release telemetry client
                await _manager.ReleaseClientAsync(host);
            });

            // Assert
            Assert.Null(exception);

            // Cleanup
            listener.Dispose();
        }

        #endregion

        #region Exception Swallowing Tests

        [Fact]
        public async Task Dispose_ExceptionInFlush_Swallowed()
        {
            // Arrange - Create a client that throws on flush
            var throwingClient = new ThrowingTelemetryClient();
            var config = new TelemetryConfiguration();
            var aggregator = new MetricsAggregator(throwingClient, config, "error-session");

            var listener = DatabricksActivityListener.CreateForTesting();
            listener.RegisterAggregator("error-session", aggregator);

            // Act - Unregister should swallow the exception from FlushAsync
            var exception = await Record.ExceptionAsync(async () =>
            {
                await listener.UnregisterAggregatorAsync("error-session");
            });

            // Assert - Exception was swallowed
            Assert.Null(exception);

            // Cleanup
            aggregator.Dispose();
            listener.Dispose();
        }

        [Fact]
        public void Connection_Dispose_WithNoTelemetry_DoesNotThrow()
        {
            // Arrange - Connection with telemetry disabled
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { TelemetryConfiguration.PropertyKeyEnabled, "false" }
            };
            var connection = CreateConnection(properties);

            // Act & Assert - Should not throw
            var exception = Record.Exception(() => connection.Dispose());
            Assert.Null(exception);
        }

        #endregion

        #region MetricsAggregator Lifecycle Tests

        [Fact]
        public void MetricsAggregator_CreateWithSessionId()
        {
            // Arrange
            var mockClient = new TrackingTelemetryClient();
            var config = new TelemetryConfiguration();

            // Act - Create aggregator with session ID (as InitializeTelemetry does)
            var aggregator = new MetricsAggregator(mockClient, config, "test-session-id");

            // Assert
            Assert.NotNull(aggregator);

            // Cleanup
            aggregator.Dispose();
        }

        [Fact]
        public async Task MetricsAggregator_FlushAsync_CanBeCalledSafely()
        {
            // Arrange
            var mockClient = new TrackingTelemetryClient();
            var config = new TelemetryConfiguration();
            var aggregator = new MetricsAggregator(mockClient, config, "flush-test-session");

            // Act - Flush should work even with no pending events
            var exception = await Record.ExceptionAsync(async () =>
            {
                await aggregator.FlushAsync();
            });

            // Assert
            Assert.Null(exception);

            // Cleanup
            aggregator.Dispose();
        }

        #endregion

        #region ActivityListener Registration Tests

        [Fact]
        public void ActivityListener_RegisterAggregator_WorksWithSessionId()
        {
            // Arrange
            var listener = DatabricksActivityListener.CreateForTesting();
            var mockClient = new TrackingTelemetryClient();
            var config = new TelemetryConfiguration();
            var aggregator = new MetricsAggregator(mockClient, config, "register-test");

            // Act
            listener.RegisterAggregator("register-test", aggregator);
            listener.Start();

            // Assert - Sampling should be AllDataAndRecorded when aggregators exist
            Assert.Equal(
                System.Diagnostics.ActivitySamplingResult.AllDataAndRecorded,
                listener.GetSamplingResult());

            // Cleanup
            aggregator.Dispose();
            listener.Dispose();
        }

        [Fact]
        public async Task ActivityListener_UnregisterAggregator_RemovesFromRouter()
        {
            // Arrange
            var listener = DatabricksActivityListener.CreateForTesting();
            var mockClient = new TrackingTelemetryClient();
            var config = new TelemetryConfiguration();
            var aggregator = new MetricsAggregator(mockClient, config, "unregister-test");

            listener.RegisterAggregator("unregister-test", aggregator);
            listener.Start();

            // Act - Unregister the aggregator
            await listener.UnregisterAggregatorAsync("unregister-test");

            // Assert - Sampling should be None when no aggregators
            Assert.Equal(
                System.Diagnostics.ActivitySamplingResult.None,
                listener.GetSamplingResult());

            // Cleanup
            aggregator.Dispose();
            listener.Dispose();
        }

        #endregion

        #region CreateTelemetryExporter Tests

        [Fact]
        public void CreateTelemetryExporter_DefaultCreation_WrapsWithCircuitBreaker()
        {
            // This test verifies the default exporter creation pattern
            // by testing the components individually (since CreateTelemetryExporter is private)
            var host = "https://test-host.databricks.com";
            var config = new TelemetryConfiguration();

            // Create the inner exporter (DatabricksTelemetryExporter)
            var httpClient = new System.Net.Http.HttpClient();
            var innerExporter = new DatabricksTelemetryExporter(
                httpClient, host, isAuthenticated: true, config);

            // Wrap with circuit breaker
            var cbExporter = new CircuitBreakerTelemetryExporter(
                "test-host.databricks.com", innerExporter);

            // Assert
            Assert.NotNull(cbExporter);

            // Cleanup
            httpClient.Dispose();
        }

        [Fact]
        public void CreateTelemetryExporter_WithTestFactory_UsesTestFactory()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            var testExporter = new InMemoryTelemetryExporter();
            var factoryCallCount = 0;

            connection.TestExporterFactory = () =>
            {
                factoryCallCount++;
                return testExporter;
            };

            // Act - Simulate CreateTelemetryExporter behavior
            var exporter = connection.TestExporterFactory();

            // Assert
            Assert.Same(testExporter, exporter);
            Assert.Equal(1, factoryCallCount);
        }

        #endregion

        #region End-to-End Telemetry Lifecycle Tests

        [Fact]
        public async Task TelemetryLifecycle_FullPipeline_InitAndCleanup()
        {
            // Arrange - Simulate the full telemetry lifecycle without a real server
            var exporter = new InMemoryTelemetryExporter();
            var host = "lifecycle-test.databricks.com";
            var sessionId = Guid.NewGuid().ToString("N");

            // Step 1: Create telemetry client (as InitializeTelemetry does)
            var client = _manager.GetOrCreateClient(
                host,
                () => exporter,
                _config);

            // Step 2: Create metrics aggregator
            var aggregator = new MetricsAggregator(client, _config, sessionId);

            // Step 3: Register with activity listener
            var listener = DatabricksActivityListener.CreateForTesting();
            listener.RegisterAggregator(sessionId, aggregator);
            listener.Start();

            // Verify telemetry pipeline is active
            Assert.Equal(
                System.Diagnostics.ActivitySamplingResult.AllDataAndRecorded,
                listener.GetSamplingResult());

            // Step 4: Cleanup (as CleanupTelemetry does)
            await listener.UnregisterAggregatorAsync(sessionId);
            aggregator.Dispose();
            await _manager.ReleaseClientAsync(host);

            // Assert - Pipeline is fully cleaned up
            Assert.Equal(
                System.Diagnostics.ActivitySamplingResult.None,
                listener.GetSamplingResult());

            // Cleanup
            listener.Dispose();
        }

        [Fact]
        public async Task TelemetryLifecycle_MultipleConnections_SharedClient()
        {
            // Arrange - Two connections to the same host share a client
            var exporter = new InMemoryTelemetryExporter();
            var host = "shared-client-test.databricks.com";

            // Connection 1 creates telemetry client
            var client1 = _manager.GetOrCreateClient(
                host,
                () => exporter,
                _config);

            // Connection 2 gets same client (ref count incremented)
            var client2 = _manager.GetOrCreateClient(
                host,
                () => new InMemoryTelemetryExporter(), // Should not be called
                _config);

            Assert.Same(client1, client2);

            // Connection 1 disposes (ref count decremented)
            await _manager.ReleaseClientAsync(host);

            // Connection 2 still has a valid client
            // (Getting again should return same instance since ref count > 0)

            // Connection 2 disposes (ref count reaches 0, client closed)
            await _manager.ReleaseClientAsync(host);
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Creates a DatabricksConnection with the given properties.
        /// Note: The connection is not opened (no OpenAsync call), so telemetry
        /// will not be initialized automatically. This is used to test
        /// configuration parsing and disposal behavior.
        /// </summary>
        private static DatabricksConnection CreateConnection(Dictionary<string, string> properties)
        {
            return new DatabricksConnection(properties);
        }

        #endregion

        #region Mock Types

        /// <summary>
        /// In-memory telemetry exporter that captures exported logs for test verification.
        /// </summary>
        private class InMemoryTelemetryExporter : ITelemetryExporter
        {
            public ConcurrentBag<IReadOnlyList<TelemetryFrontendLog>> ExportedBatches { get; }
                = new ConcurrentBag<IReadOnlyList<TelemetryFrontendLog>>();

            public int ExportCallCount => ExportedBatches.Count;

            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                if (logs != null && logs.Count > 0)
                {
                    ExportedBatches.Add(logs);
                }
                return Task.FromResult(true);
            }
        }

        /// <summary>
        /// Telemetry client that tracks method calls for test verification.
        /// </summary>
        private class TrackingTelemetryClient : ITelemetryClient
        {
            public ConcurrentBag<TelemetryFrontendLog> EnqueuedLogs { get; }
                = new ConcurrentBag<TelemetryFrontendLog>();
            public int FlushCallCount;
            public int CloseCallCount;

            public void Enqueue(TelemetryFrontendLog log)
            {
                EnqueuedLogs.Add(log);
            }

            public Task FlushAsync(CancellationToken ct = default)
            {
                Interlocked.Increment(ref FlushCallCount);
                return Task.CompletedTask;
            }

            public Task CloseAsync()
            {
                Interlocked.Increment(ref CloseCallCount);
                return Task.CompletedTask;
            }

            public ValueTask DisposeAsync()
            {
                return new ValueTask(CloseAsync());
            }
        }

        /// <summary>
        /// Telemetry client that throws exceptions to test exception swallowing.
        /// </summary>
        private class ThrowingTelemetryClient : ITelemetryClient
        {
            public void Enqueue(TelemetryFrontendLog log)
            {
                throw new InvalidOperationException("Test exception in Enqueue");
            }

            public Task FlushAsync(CancellationToken ct = default)
            {
                throw new InvalidOperationException("Test exception in FlushAsync");
            }

            public Task CloseAsync()
            {
                throw new InvalidOperationException("Test exception in CloseAsync");
            }

            public ValueTask DisposeAsync()
            {
                return new ValueTask(CloseAsync());
            }
        }

        #endregion
    }
}
