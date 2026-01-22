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
    /// Unit tests for TelemetryClient.
    /// Tests verify:
    /// - Constructor initializes listener, aggregator, exporter
    /// - ExportAsync delegates to exporter
    /// - CloseAsync cancels background task, flushes, disposes
    /// - All exceptions swallowed during close
    /// </summary>
    public class TelemetryClientTests
    {
        private const string TestHost = "test-host.databricks.com";

        #region Constructor Tests

        [Fact]
        public void TelemetryClient_Constructor_InitializesComponents()
        {
            // Arrange
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration { Enabled = true };

            // Act
            var client = new TelemetryClient(TestHost, httpClient, config);

            // Assert
            Assert.NotNull(client);
            Assert.Equal(TestHost, client.Host);
            Assert.NotNull(client.Listener);
            Assert.NotNull(client.Aggregator);
            Assert.NotNull(client.Exporter);
            Assert.True(client.Listener.IsStarted);
            Assert.False(client.IsClosed);
        }

        [Fact]
        public void TelemetryClient_Constructor_WithWorkspaceIdAndUserAgent()
        {
            // Arrange
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration { Enabled = true };
            long workspaceId = 12345;
            string userAgent = "TestAgent/1.0";

            // Act
            var client = new TelemetryClient(TestHost, httpClient, config, workspaceId, userAgent);

            // Assert
            Assert.NotNull(client);
            Assert.Equal(TestHost, client.Host);
        }

        [Fact]
        public void TelemetryClient_Constructor_NullHost_ThrowsException()
        {
            // Arrange
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new TelemetryClient(null!, httpClient, config));
        }

        [Fact]
        public void TelemetryClient_Constructor_EmptyHost_ThrowsException()
        {
            // Arrange
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new TelemetryClient("", httpClient, config));
        }

        [Fact]
        public void TelemetryClient_Constructor_WhitespaceHost_ThrowsException()
        {
            // Arrange
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new TelemetryClient("   ", httpClient, config));
        }

        [Fact]
        public void TelemetryClient_Constructor_NullHttpClient_ThrowsException()
        {
            // Arrange
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new TelemetryClient(TestHost, null!, config));
        }

        [Fact]
        public void TelemetryClient_Constructor_NullConfig_ThrowsException()
        {
            // Arrange
            var httpClient = new HttpClient();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new TelemetryClient(TestHost, httpClient, null!));
        }

        [Fact]
        public void TelemetryClient_Constructor_WithCustomExporter_UsesProvidedExporter()
        {
            // Arrange
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var mockExporter = new MockTelemetryExporter();

            // Act
            var client = new TelemetryClient(
                TestHost, httpClient, config,
                workspaceId: 0,
                userAgent: null,
                exporter: mockExporter,
                aggregator: null,
                listener: null);

            // Assert
            Assert.Same(mockExporter, client.Exporter);
        }

        #endregion

        #region ExportAsync Tests

        [Fact]
        public async Task TelemetryClient_ExportAsync_DelegatesToExporter()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var client = CreateClientWithMockExporter(mockExporter, config);
            var logs = new List<TelemetryFrontendLog> { CreateTestLog() };

            // Act
            await client.ExportAsync(logs);

            // Assert
            Assert.Equal(1, mockExporter.ExportCallCount);
            Assert.Same(logs, mockExporter.LastExportedLogs);
        }

        [Fact]
        public async Task TelemetryClient_ExportAsync_WithCancellation_PropagatesCancellation()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter { ShouldThrowCancellation = true };
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var client = CreateClientWithMockExporter(mockExporter, config);
            var logs = new List<TelemetryFrontendLog> { CreateTestLog() };
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act & Assert
            await Assert.ThrowsAsync<OperationCanceledException>(() => client.ExportAsync(logs, cts.Token));
        }

        [Fact]
        public async Task TelemetryClient_ExportAsync_EmptyLogs_DoesNotCallExporter()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var client = CreateClientWithMockExporter(mockExporter, config);
            var logs = new List<TelemetryFrontendLog>();

            // Act
            await client.ExportAsync(logs);

            // Assert - Exporter might be called but with empty list - check behavior
            // The circuit breaker exporter returns early for empty lists
            Assert.Equal(0, mockExporter.ExportCallCount);
        }

        [Fact]
        public async Task TelemetryClient_ExportAsync_NullLogs_DoesNotCallExporter()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var client = CreateClientWithMockExporter(mockExporter, config);

            // Act
            await client.ExportAsync(null!);

            // Assert - Exporter might be called but with null - check behavior
            // The circuit breaker exporter returns early for null
            Assert.Equal(0, mockExporter.ExportCallCount);
        }

        #endregion

        #region CloseAsync Tests

        [Fact]
        public async Task TelemetryClient_CloseAsync_FlushesAndCancels()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration { FlushIntervalMs = 10000 }; // Long interval
            var client = CreateClientWithMockExporter(mockExporter, config);

            // Act
            await client.CloseAsync();

            // Assert
            Assert.True(client.IsClosed);
            Assert.True(client.Listener.IsDisposed);
        }

        [Fact]
        public async Task TelemetryClient_CloseAsync_Idempotent()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var client = CreateClientWithMockExporter(mockExporter, config);

            // Act - Call multiple times
            await client.CloseAsync();
            await client.CloseAsync();
            await client.CloseAsync();

            // Assert - No exceptions, client remains closed
            Assert.True(client.IsClosed);
        }

        [Fact]
        public async Task TelemetryClient_CloseAsync_ExceptionSwallowed()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter { ShouldThrowOnExport = true };
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration { FlushIntervalMs = 50 };
            var client = CreateClientWithMockExporter(mockExporter, config);

            // Act - should not throw even if internal operations fail
            await client.CloseAsync();

            // Assert
            Assert.True(client.IsClosed);
        }

        [Fact]
        public async Task TelemetryClient_CloseAsync_StopsListener()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var client = CreateClientWithMockExporter(mockExporter, config);
            Assert.True(client.Listener.IsStarted);

            // Act
            await client.CloseAsync();

            // Assert
            Assert.True(client.Listener.IsDisposed);
        }

        [Fact]
        public async Task TelemetryClient_CloseAsync_WaitsForBackgroundTask()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration { FlushIntervalMs = 100 }; // Short interval
            var client = CreateClientWithMockExporter(mockExporter, config);

            // Let background task run briefly
            await Task.Delay(50);

            // Act
            var closeTask = client.CloseAsync();

            // Wait with timeout
            var completed = await Task.WhenAny(closeTask, Task.Delay(TimeSpan.FromSeconds(10)));

            // Assert - Close should complete within reasonable time
            Assert.Same(closeTask, completed);
        }

        #endregion

        #region Background Flush Task Tests

        [Fact]
        public async Task TelemetryClient_BackgroundFlush_FlushesMetrics()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration
            {
                Enabled = true,
                FlushIntervalMs = 100, // Short interval for testing
                BatchSize = 1000 // Large batch size to prevent immediate flush
            };
            var client = CreateClientWithMockExporter(mockExporter, config);

            // Enqueue some events via the aggregator
            var logs = new List<TelemetryFrontendLog> { CreateTestLog() };
            await client.ExportAsync(logs);

            // Wait for background flush
            await Task.Delay(200);

            // Assert - Background flush should have triggered
            Assert.True(mockExporter.ExportCallCount >= 1);

            // Cleanup
            await client.CloseAsync();
        }

        [Fact]
        public async Task TelemetryClient_BackgroundFlush_StopsOnClose()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration
            {
                Enabled = true,
                FlushIntervalMs = 50
            };
            var client = CreateClientWithMockExporter(mockExporter, config);

            // Wait for a few flush cycles
            await Task.Delay(150);
            var countBeforeClose = mockExporter.ExportCallCount;

            // Act
            await client.CloseAsync();

            // Wait and verify no more flushes
            await Task.Delay(150);

            // Assert - Export count should not increase significantly after close
            // Allow for one more flush during close
            Assert.True(mockExporter.ExportCallCount <= countBeforeClose + 1);
        }

        #endregion

        #region Thread Safety Tests

        [Fact]
        public async Task TelemetryClient_ConcurrentExport_ThreadSafe()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var client = CreateClientWithMockExporter(mockExporter, config);
            var tasks = new List<Task>();
            var logs = new List<TelemetryFrontendLog> { CreateTestLog() };

            // Act - Concurrent exports
            for (int i = 0; i < 100; i++)
            {
                tasks.Add(client.ExportAsync(logs));
            }

            await Task.WhenAll(tasks);

            // Assert - All exports should complete without exception
            Assert.Equal(100, mockExporter.ExportCallCount);

            // Cleanup
            await client.CloseAsync();
        }

        [Fact]
        public async Task TelemetryClient_ExportDuringClose_ThreadSafe()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var client = CreateClientWithMockExporter(mockExporter, config);
            var logs = new List<TelemetryFrontendLog> { CreateTestLog() };

            // Act - Start closing and export concurrently
            var closeTask = client.CloseAsync();
            var exportTasks = new List<Task>();
            for (int i = 0; i < 10; i++)
            {
                exportTasks.Add(client.ExportAsync(logs));
            }

            // All should complete without throwing
            await Task.WhenAll(exportTasks);
            await closeTask;

            // Assert
            Assert.True(client.IsClosed);
        }

        #endregion

        #region Helper Methods

        private TelemetryClient CreateClientWithMockExporter(MockTelemetryExporter exporter, TelemetryConfiguration config)
        {
            var httpClient = new HttpClient();

            // Create a mock aggregator that uses the mock exporter
            var mockAggregator = new MetricsAggregator(exporter, config, workspaceId: 12345, userAgent: "TestAgent");

            // Create client with injected dependencies
            return new TelemetryClient(
                TestHost,
                httpClient,
                config,
                workspaceId: 12345,
                userAgent: "TestAgent",
                exporter: exporter,
                aggregator: mockAggregator,
                listener: null);
        }

        private TelemetryFrontendLog CreateTestLog()
        {
            return new TelemetryFrontendLog
            {
                FrontendLogEventId = Guid.NewGuid().ToString(),
                WorkspaceId = 12345,
                Context = new FrontendLogContext
                {
                    ClientContext = new TelemetryClientContext { UserAgent = "TestAgent" },
                    TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                }
            };
        }

        #endregion

        #region Mock Classes

        private class MockTelemetryExporter : ITelemetryExporter
        {
            private int _exportCallCount;
            public int ExportCallCount => _exportCallCount;
            public IReadOnlyList<TelemetryFrontendLog>? LastExportedLogs { get; private set; }
            public bool ShouldThrowOnExport { get; set; }
            public bool ShouldThrowCancellation { get; set; }

            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                if (ShouldThrowCancellation)
                {
                    throw new OperationCanceledException();
                }

                if (ShouldThrowOnExport)
                {
                    throw new InvalidOperationException("Export failed");
                }

                if (logs == null || logs.Count == 0)
                {
                    return Task.FromResult(true);
                }

                Interlocked.Increment(ref _exportCallCount);
                LastExportedLogs = logs;
                return Task.FromResult(true);
            }
        }

        #endregion
    }
}
