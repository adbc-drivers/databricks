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
    /// Tests for the <see cref="ITelemetryClient"/> interface contract.
    /// Uses a mock implementation to verify the interface is correctly defined
    /// and can be implemented as expected.
    /// </summary>
    public class ITelemetryClientTests
    {
        #region Interface Contract Tests

        [Fact]
        public void ITelemetryClient_ExtendsIAsyncDisposable()
        {
            // Verify that ITelemetryClient extends IAsyncDisposable
            Assert.True(typeof(IAsyncDisposable).IsAssignableFrom(typeof(ITelemetryClient)));
        }

        [Fact]
        public void ITelemetryClient_HasEnqueueMethod()
        {
            // Verify Enqueue method exists with correct signature
            System.Reflection.MethodInfo? enqueueMethod = typeof(ITelemetryClient).GetMethod(
                "Enqueue",
                new[] { typeof(TelemetryFrontendLog) });

            Assert.NotNull(enqueueMethod);
            Assert.Equal(typeof(void), enqueueMethod!.ReturnType);
        }

        [Fact]
        public void ITelemetryClient_HasFlushAsyncMethod()
        {
            // Verify FlushAsync method exists with correct signature
            System.Reflection.MethodInfo? flushMethod = typeof(ITelemetryClient).GetMethod(
                "FlushAsync",
                new[] { typeof(CancellationToken) });

            Assert.NotNull(flushMethod);
            Assert.Equal(typeof(Task), flushMethod!.ReturnType);
        }

        [Fact]
        public void ITelemetryClient_HasCloseAsyncMethod()
        {
            // Verify CloseAsync method exists with correct signature
            System.Reflection.MethodInfo? closeMethod = typeof(ITelemetryClient).GetMethod(
                "CloseAsync",
                Type.EmptyTypes);

            Assert.NotNull(closeMethod);
            Assert.Equal(typeof(Task), closeMethod!.ReturnType);
        }

        #endregion

        #region Mock Implementation Tests

        [Fact]
        public void Enqueue_WithValidLog_AddsToInternalQueue()
        {
            // Arrange
            MockTelemetryClient client = new MockTelemetryClient();
            TelemetryFrontendLog log = new TelemetryFrontendLog
            {
                WorkspaceId = 12345,
                FrontendLogEventId = "test-event-1"
            };

            // Act
            client.Enqueue(log);

            // Assert
            TelemetryFrontendLog enqueuedLog = Assert.Single(client.EnqueuedLogs);
            Assert.Equal(log, enqueuedLog);
        }

        [Fact]
        public void Enqueue_MultipleLogs_AllQueued()
        {
            // Arrange
            MockTelemetryClient client = new MockTelemetryClient();

            // Act
            for (int i = 0; i < 5; i++)
            {
                client.Enqueue(new TelemetryFrontendLog
                {
                    WorkspaceId = i,
                    FrontendLogEventId = $"event-{i}"
                });
            }

            // Assert
            Assert.Equal(5, client.EnqueuedLogs.Count);
        }

        [Fact]
        public async Task FlushAsync_InvokesFlush()
        {
            // Arrange
            MockTelemetryClient client = new MockTelemetryClient();
            client.Enqueue(new TelemetryFrontendLog { WorkspaceId = 1 });

            // Act
            await client.FlushAsync();

            // Assert
            Assert.Equal(1, client.FlushCallCount);
        }

        [Fact]
        public async Task FlushAsync_WithCancellationToken_PropagatesToken()
        {
            // Arrange
            MockTelemetryClient client = new MockTelemetryClient();
            CancellationTokenSource cts = new CancellationTokenSource();

            // Act
            await client.FlushAsync(cts.Token);

            // Assert
            Assert.Equal(1, client.FlushCallCount);
            Assert.Equal(cts.Token, client.LastFlushCancellationToken);
        }

        [Fact]
        public async Task CloseAsync_InvokesClose()
        {
            // Arrange
            MockTelemetryClient client = new MockTelemetryClient();

            // Act
            await client.CloseAsync();

            // Assert
            Assert.True(client.IsCloseCalled);
        }

        [Fact]
        public async Task DisposeAsync_CanBeCalledOnInterface()
        {
            // Arrange
            MockTelemetryClient client = new MockTelemetryClient();

            // Act - verify IAsyncDisposable works through the interface
            ITelemetryClient interfaceRef = client;
            await interfaceRef.DisposeAsync();

            // Assert
            Assert.True(client.IsDisposed);
        }

        [Fact]
        public async Task Enqueue_IsThreadSafe_ConcurrentAccess()
        {
            // Arrange
            MockTelemetryClient client = new MockTelemetryClient();
            int threadCount = 10;
            int logsPerThread = 100;
            ManualResetEventSlim startEvent = new ManualResetEventSlim(false);
            List<Task> tasks = new List<Task>();

            // Act - enqueue from multiple threads concurrently
            for (int t = 0; t < threadCount; t++)
            {
                int threadIndex = t;
                tasks.Add(Task.Run(() =>
                {
                    startEvent.Wait();
                    for (int i = 0; i < logsPerThread; i++)
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

            // Assert - all logs should be enqueued
            Assert.Equal(threadCount * logsPerThread, client.EnqueuedLogs.Count);
        }

        #endregion

        #region Mock Implementation

        /// <summary>
        /// Mock implementation of <see cref="ITelemetryClient"/> for testing purposes.
        /// </summary>
        private sealed class MockTelemetryClient : ITelemetryClient
        {
            private readonly ConcurrentBag<TelemetryFrontendLog> _enqueuedLogs = new ConcurrentBag<TelemetryFrontendLog>();
            private int _flushCallCount;

            public IReadOnlyList<TelemetryFrontendLog> EnqueuedLogs
            {
                get
                {
                    List<TelemetryFrontendLog> list = new List<TelemetryFrontendLog>(_enqueuedLogs);
                    return list;
                }
            }

            public int FlushCallCount => _flushCallCount;
            public CancellationToken LastFlushCancellationToken { get; private set; }
            public bool IsCloseCalled { get; private set; }
            public bool IsDisposed { get; private set; }

            public void Enqueue(TelemetryFrontendLog log)
            {
                _enqueuedLogs.Add(log);
            }

            public Task FlushAsync(CancellationToken ct = default)
            {
                Interlocked.Increment(ref _flushCallCount);
                LastFlushCancellationToken = ct;
                return Task.CompletedTask;
            }

            public Task CloseAsync()
            {
                IsCloseCalled = true;
                return Task.CompletedTask;
            }

            public ValueTask DisposeAsync()
            {
                IsDisposed = true;
                return default;
            }
        }

        #endregion
    }
}
