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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for <see cref="TelemetryClient"/>.
    /// </summary>
    public class TelemetryClientTests : IAsyncLifetime
    {
        private MockTelemetryExporter _mockExporter = null!;
        private TelemetryConfiguration _defaultConfig = null!;

        public Task InitializeAsync()
        {
            _mockExporter = new MockTelemetryExporter();
            _defaultConfig = new TelemetryConfiguration
            {
                BatchSize = 10,
                FlushIntervalMs = 5000,
            };
            return Task.CompletedTask;
        }

        public Task DisposeAsync()
        {
            return Task.CompletedTask;
        }

        #region Constructor Tests

        [Fact]
        public void Constructor_NullExporter_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new TelemetryClient(null!, _defaultConfig));
        }

        [Fact]
        public void Constructor_NullConfig_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new TelemetryClient(_mockExporter, null!));
        }

        [Fact]
        public void Constructor_ValidParameters_CreatesInstance()
        {
            // Act
            TelemetryClient client = new TelemetryClient(_mockExporter, _defaultConfig);

            // Assert
            Assert.NotNull(client);
        }

        #endregion

        #region Enqueue Tests

        [Fact]
        public async Task Enqueue_AddsToQueue_NonBlocking()
        {
            // Arrange
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);
            TelemetryFrontendLog log = CreateTestLog(1);

            // Act - Enqueue should return immediately (non-blocking)
            client.Enqueue(log);

            // Give a moment for any async work to complete
            await Task.Delay(50);

            // Assert - The event was queued (no export yet since batch size not reached)
            Assert.Equal(0, _mockExporter.ExportCallCount);

            // Flush to verify the event was in the queue
            await client.FlushAsync();
            Assert.Equal(1, _mockExporter.ExportCallCount);
            Assert.Single(_mockExporter.ExportedBatches);
            Assert.Single(_mockExporter.ExportedBatches[0]);

            await client.CloseAsync();
        }

        [Fact]
        public async Task Enqueue_BatchSizeReached_TriggersFlush()
        {
            // Arrange
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 5, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            // Act - Enqueue enough events to trigger flush
            for (int i = 0; i < 5; i++)
            {
                client.Enqueue(CreateTestLog(i));
            }

            // Wait for the fire-and-forget flush to complete
            await Task.Delay(200);

            // Assert - ExportAsync should have been called
            Assert.True(_mockExporter.ExportCallCount >= 1,
                $"Expected at least 1 export call but got {_mockExporter.ExportCallCount}");

            int totalExported = _mockExporter.ExportedBatches.Sum(b => b.Count);
            Assert.Equal(5, totalExported);

            await client.CloseAsync();
        }

        [Fact]
        public async Task Enqueue_MultipleBatches_ExportsAll()
        {
            // Arrange
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 3, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            // Act - Enqueue more than one batch worth
            for (int i = 0; i < 7; i++)
            {
                client.Enqueue(CreateTestLog(i));
            }

            // Wait for the fire-and-forget flush to complete
            await Task.Delay(200);

            // Then do a final flush to export any remaining items
            await client.FlushAsync();

            // Assert - All events should have been exported across batches
            int totalExported = _mockExporter.ExportedBatches.Sum(b => b.Count);
            Assert.Equal(7, totalExported);

            await client.CloseAsync();
        }

        #endregion

        #region FlushAsync Tests

        [Fact]
        public async Task FlushAsync_DrainsQueueAndExports()
        {
            // Arrange
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            for (int i = 0; i < 5; i++)
            {
                client.Enqueue(CreateTestLog(i));
            }

            // Act
            await client.FlushAsync();

            // Assert
            Assert.Equal(1, _mockExporter.ExportCallCount);
            Assert.Single(_mockExporter.ExportedBatches);
            Assert.Equal(5, _mockExporter.ExportedBatches[0].Count);

            await client.CloseAsync();
        }

        [Fact]
        public async Task FlushAsync_EmptyQueue_NoExport()
        {
            // Arrange
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            // Act
            await client.FlushAsync();

            // Assert
            Assert.Equal(0, _mockExporter.ExportCallCount);

            await client.CloseAsync();
        }

        [Fact]
        public async Task FlushAsync_ConcurrentCalls_OnlyOneFlushAtATime()
        {
            // Arrange - slow exporter to increase chance of overlap
            ManualResetEventSlim exportStarted = new ManualResetEventSlim(false);
            ManualResetEventSlim exportCanProceed = new ManualResetEventSlim(false);

            MockTelemetryExporter slowExporter = new MockTelemetryExporter
            {
                OnExportCalled = (logs, ct) =>
                {
                    exportStarted.Set();
                    exportCanProceed.Wait(TimeSpan.FromSeconds(5));
                }
            };

            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(slowExporter, config);

            for (int i = 0; i < 5; i++)
            {
                client.Enqueue(CreateTestLog(i));
            }

            // Act - Start first flush
            Task flush1 = client.FlushAsync();

            // Wait for the export to start
            exportStarted.Wait(TimeSpan.FromSeconds(5));

            // Start second flush while first is in progress
            Task flush2 = client.FlushAsync();

            // The second flush should return quickly since the lock isn't available
            await flush2;

            // Allow first flush to complete
            exportCanProceed.Set();
            await flush1;

            // Assert - only one export should have happened
            Assert.Equal(1, slowExporter.ExportCallCount);

            await client.CloseAsync();
        }

        [Fact]
        public async Task FlushAsync_LargeQueue_DrainsBatchByBatch()
        {
            // Arrange
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 3, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            // Enqueue 10 items (won't auto-flush since we check count at batch size threshold)
            for (int i = 0; i < 10; i++)
            {
                client.Enqueue(CreateTestLog(i));
            }

            // Wait for any auto-flushes triggered by enqueue
            await Task.Delay(200);

            // Explicit flush to drain remaining
            await client.FlushAsync();

            // Assert - all items exported
            int totalExported = _mockExporter.ExportedBatches.Sum(b => b.Count);
            Assert.Equal(10, totalExported);

            // Each batch should have at most 3 items
            foreach (List<TelemetryFrontendLog> batch in _mockExporter.ExportedBatches)
            {
                Assert.True(batch.Count <= 3,
                    $"Expected batch size <= 3 but got {batch.Count}");
            }

            await client.CloseAsync();
        }

        #endregion

        #region CloseAsync Tests

        [Fact]
        public async Task CloseAsync_FlushesRemainingEvents()
        {
            // Arrange
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            for (int i = 0; i < 3; i++)
            {
                client.Enqueue(CreateTestLog(i));
            }

            // Act
            await client.CloseAsync();

            // Assert - the remaining events should have been flushed
            int totalExported = _mockExporter.ExportedBatches.Sum(b => b.Count);
            Assert.Equal(3, totalExported);
        }

        [Fact]
        public async Task CloseAsync_DisposesResources()
        {
            // Arrange
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            // Act
            await client.CloseAsync();

            // Assert - After close, enqueue should be a no-op (no exceptions)
            client.Enqueue(CreateTestLog(1));
            Assert.Equal(0, _mockExporter.ExportCallCount);
        }

        [Fact]
        public async Task CloseAsync_CalledMultipleTimes_NoException()
        {
            // Arrange
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            // Act & Assert - Should not throw even when called multiple times
            await client.CloseAsync();
            await client.CloseAsync();
        }

        #endregion

        #region DisposeAsync Tests

        [Fact]
        public async Task DisposeAsync_DelegatesToCloseAsync()
        {
            // Arrange
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            client.Enqueue(CreateTestLog(1));
            client.Enqueue(CreateTestLog(2));

            // Act
            await ((IAsyncDisposable)client).DisposeAsync();

            // Assert - Events should have been flushed
            int totalExported = _mockExporter.ExportedBatches.Sum(b => b.Count);
            Assert.Equal(2, totalExported);

            // After dispose, enqueue is a no-op
            client.Enqueue(CreateTestLog(3));
            int totalAfterDispose = _mockExporter.ExportedBatches.Sum(b => b.Count);
            Assert.Equal(2, totalAfterDispose);
        }

        #endregion

        #region Exception Handling Tests

        [Fact]
        public async Task Exception_Swallowed_NoThrow_OnEnqueue()
        {
            // Arrange
            MockTelemetryExporter throwingExporter = new MockTelemetryExporter
            {
                ThrowException = new InvalidOperationException("Export failure")
            };
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 2, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(throwingExporter, config);

            // Act & Assert - Should not throw even when exporter throws
            Exception? caughtException = null;
            try
            {
                // Enqueue enough to trigger flush
                client.Enqueue(CreateTestLog(1));
                client.Enqueue(CreateTestLog(2));

                // Wait for fire-and-forget flush
                await Task.Delay(200);
            }
            catch (Exception ex)
            {
                caughtException = ex;
            }

            Assert.Null(caughtException);

            await client.CloseAsync();
        }

        [Fact]
        public async Task Exception_Swallowed_NoThrow_OnFlushAsync()
        {
            // Arrange
            MockTelemetryExporter throwingExporter = new MockTelemetryExporter
            {
                ThrowException = new InvalidOperationException("Export failure")
            };
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(throwingExporter, config);

            client.Enqueue(CreateTestLog(1));

            // Act & Assert - FlushAsync should not throw
            Exception? caughtException = null;
            try
            {
                await client.FlushAsync();
            }
            catch (Exception ex)
            {
                caughtException = ex;
            }

            Assert.Null(caughtException);

            await client.CloseAsync();
        }

        [Fact]
        public async Task Exception_Swallowed_NoThrow_OnCloseAsync()
        {
            // Arrange
            MockTelemetryExporter throwingExporter = new MockTelemetryExporter
            {
                ThrowException = new InvalidOperationException("Export failure")
            };
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(throwingExporter, config);

            client.Enqueue(CreateTestLog(1));

            // Act & Assert - CloseAsync should not throw
            Exception? caughtException = null;
            try
            {
                await client.CloseAsync();
            }
            catch (Exception ex)
            {
                caughtException = ex;
            }

            Assert.Null(caughtException);
        }

        #endregion

        #region After Dispose Tests

        [Fact]
        public async Task AfterDispose_EnqueueIsNoOp()
        {
            // Arrange
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            await client.CloseAsync();

            // Act - Enqueue after close should be silently ignored
            client.Enqueue(CreateTestLog(1));
            client.Enqueue(CreateTestLog(2));
            client.Enqueue(CreateTestLog(3));

            // Assert - No events should have been exported
            Assert.Equal(0, _mockExporter.ExportCallCount);
        }

        [Fact]
        public async Task AfterDispose_FlushAsyncIsNoOp()
        {
            // Arrange
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            await client.CloseAsync();

            // Act & Assert - FlushAsync after close should not throw
            await client.FlushAsync();
            Assert.Equal(0, _mockExporter.ExportCallCount);
        }

        #endregion

        #region Timer Tests

        [Fact]
        public async Task FlushTimer_Elapses_ExportsEvents()
        {
            // Arrange - Use a short flush interval for testing
            TelemetryConfiguration config = new TelemetryConfiguration
            {
                BatchSize = 1000, // large enough to not trigger batch-based flush
                FlushIntervalMs = 200 // short interval for testing
            };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            // Enqueue events (below batch threshold)
            for (int i = 0; i < 3; i++)
            {
                client.Enqueue(CreateTestLog(i));
            }

            // Act - Wait for timer to elapse
            await Task.Delay(500);

            // Assert - Events should have been exported by the timer
            Assert.True(_mockExporter.ExportCallCount >= 1,
                $"Expected at least 1 export call from timer but got {_mockExporter.ExportCallCount}");

            int totalExported = _mockExporter.ExportedBatches.Sum(b => b.Count);
            Assert.Equal(3, totalExported);

            await client.CloseAsync();
        }

        [Fact]
        public async Task FlushTimer_NoEvents_DoesNotExport()
        {
            // Arrange - Use a short flush interval for testing
            TelemetryConfiguration config = new TelemetryConfiguration
            {
                BatchSize = 1000,
                FlushIntervalMs = 100
            };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            // Act - Wait for timer to elapse with empty queue
            await Task.Delay(300);

            // Assert - No export should have happened
            Assert.Equal(0, _mockExporter.ExportCallCount);

            await client.CloseAsync();
        }

        #endregion

        #region Thread Safety Tests

        [Fact]
        public async Task Enqueue_ConcurrentFromMultipleThreads_AllEventsProcessed()
        {
            // Arrange
            TelemetryConfiguration config = new TelemetryConfiguration { BatchSize = 1000, FlushIntervalMs = 60000 };
            TelemetryClient client = new TelemetryClient(_mockExporter, config);

            int threadCount = 10;
            int eventsPerThread = 50;
            ManualResetEventSlim startEvent = new ManualResetEventSlim(false);
            List<Task> tasks = new List<Task>();

            // Act - Enqueue from multiple threads concurrently
            for (int t = 0; t < threadCount; t++)
            {
                int threadIndex = t;
                tasks.Add(Task.Run(() =>
                {
                    startEvent.Wait();
                    for (int i = 0; i < eventsPerThread; i++)
                    {
                        client.Enqueue(new TelemetryFrontendLog
                        {
                            WorkspaceId = threadIndex,
                            FrontendLogEventId = $"thread-{threadIndex}-event-{i}"
                        });
                    }
                }));
            }

            startEvent.Set();
            await Task.WhenAll(tasks);

            // Flush all remaining events
            await client.FlushAsync();

            // Assert - All events should be exported
            int totalExported = _mockExporter.ExportedBatches.Sum(b => b.Count);
            Assert.Equal(threadCount * eventsPerThread, totalExported);

            await client.CloseAsync();
        }

        #endregion

        #region ITelemetryClient Interface Tests

        [Fact]
        public void TelemetryClient_ImplementsITelemetryClient()
        {
            // Assert
            Assert.True(typeof(ITelemetryClient).IsAssignableFrom(typeof(TelemetryClient)));
        }

        [Fact]
        public void TelemetryClient_ImplementsIAsyncDisposable()
        {
            // Assert
            Assert.True(typeof(IAsyncDisposable).IsAssignableFrom(typeof(TelemetryClient)));
        }

        #endregion

        #region Helper Methods

        private static TelemetryFrontendLog CreateTestLog(int index)
        {
            return new TelemetryFrontendLog
            {
                WorkspaceId = 12345 + index,
                FrontendLogEventId = $"test-event-{index}"
            };
        }

        #endregion

        #region Mock Telemetry Exporter

        /// <summary>
        /// Mock implementation of <see cref="ITelemetryExporter"/> for testing.
        /// Thread-safe for concurrent access.
        /// </summary>
        private sealed class MockTelemetryExporter : ITelemetryExporter
        {
            private int _exportCallCount;
            private readonly ConcurrentBag<List<TelemetryFrontendLog>> _exportedBatches =
                new ConcurrentBag<List<TelemetryFrontendLog>>();

            /// <summary>
            /// The value to return from ExportAsync. Ignored if ThrowException is set.
            /// </summary>
            public bool ReturnValue { get; set; } = true;

            /// <summary>
            /// If set, ExportAsync will throw this exception.
            /// </summary>
            public Exception? ThrowException { get; set; }

            /// <summary>
            /// Count of times ExportAsync has been called.
            /// </summary>
            public int ExportCallCount => _exportCallCount;

            /// <summary>
            /// All batches that were exported.
            /// </summary>
            public List<List<TelemetryFrontendLog>> ExportedBatches
            {
                get { return new List<List<TelemetryFrontendLog>>(_exportedBatches); }
            }

            /// <summary>
            /// Optional callback invoked when ExportAsync is called.
            /// </summary>
            public Action<IReadOnlyList<TelemetryFrontendLog>, CancellationToken>? OnExportCalled { get; set; }

            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                Interlocked.Increment(ref _exportCallCount);
                _exportedBatches.Add(new List<TelemetryFrontendLog>(logs));

                OnExportCalled?.Invoke(logs, ct);

                if (ThrowException != null)
                {
                    throw ThrowException;
                }

                return Task.FromResult(ReturnValue);
            }
        }

        #endregion
    }
}
