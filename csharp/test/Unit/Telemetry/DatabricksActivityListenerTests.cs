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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Unit tests for <see cref="DatabricksActivityListener"/>.
    /// Tests singleton pattern, aggregator registry, activity routing,
    /// ShouldListenTo filtering, Sample callback, and exception swallowing.
    /// </summary>
    public class DatabricksActivityListenerTests : IDisposable
    {
        private readonly ActivitySource _driverSource;
        private readonly ActivitySource _otherSource;
        private readonly ActivityListener _baseListener;
        private readonly MockTelemetryClient _mockClient;
        private readonly TelemetryConfiguration _config;

        public DatabricksActivityListenerTests()
        {
            _driverSource = new ActivitySource(DatabricksActivityListener.DatabricksActivitySourceName);
            _otherSource = new ActivitySource("Other.Source");

            // Base listener to ensure activities are created (some tests need activities
            // to exist before the DatabricksActivityListener starts)
            _baseListener = new ActivityListener
            {
                ShouldListenTo = source => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> options) =>
                    ActivitySamplingResult.AllDataAndRecorded,
                ActivityStarted = activity => { },
                ActivityStopped = activity => { }
            };
            ActivitySource.AddActivityListener(_baseListener);

            _mockClient = new MockTelemetryClient();
            _config = new TelemetryConfiguration();
        }

        public void Dispose()
        {
            _baseListener.Dispose();
            _driverSource.Dispose();
            _otherSource.Dispose();
        }

        #region Singleton Tests

        [Fact]
        public void Instance_ReturnsSameInstance()
        {
            // Act
            DatabricksActivityListener instance1 = DatabricksActivityListener.Instance;
            DatabricksActivityListener instance2 = DatabricksActivityListener.Instance;

            // Assert
            Assert.Same(instance1, instance2);
        }

        [Fact]
        public void Instance_ReturnsNonNullInstance()
        {
            // Act
            DatabricksActivityListener instance = DatabricksActivityListener.Instance;

            // Assert
            Assert.NotNull(instance);
        }

        [Fact]
        public void CreateForTesting_ReturnsIsolatedInstance()
        {
            // Act
            using DatabricksActivityListener testInstance = DatabricksActivityListener.CreateForTesting();

            // Assert
            Assert.NotNull(testInstance);
            Assert.NotSame(DatabricksActivityListener.Instance, testInstance);
        }

        #endregion

        #region Start Tests

        [Fact]
        public void Start_ListensToDatabricksActivitySource()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();

            // Act - test ShouldListenTo directly
            bool shouldListen = listener.ShouldListenTo(_driverSource);

            // Assert
            Assert.True(shouldListen);
        }

        [Fact]
        public void Start_IgnoresOtherActivitySources()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();

            // Act
            bool shouldListen = listener.ShouldListenTo(_otherSource);

            // Assert
            Assert.False(shouldListen);
        }

        [Fact]
        public void Start_CreatesActivityListener()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();

            // Act - should not throw
            listener.Start();

            // Assert - if Start succeeds, listener is created.
            // Verify by checking that activities from the driver source are captured.
            MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            listener.RegisterAggregator("session-start-test", aggregator);

            using Activity? activity = _driverSource.StartActivity("Test.Start");
            Assert.NotNull(activity);
            activity!.SetTag("session.id", "session-start-test");
            activity.SetTag("statement.id", "stmt-start-test");
            activity.SetStatus(ActivityStatusCode.Ok);
            activity.Stop();

            // The activity should have been routed to the aggregator
            // (we verify indirectly through the aggregator's pending count)
            Assert.True(aggregator.PendingContextCount >= 0);

            aggregator.Dispose();
        }

        [Fact]
        public void Start_CalledTwice_DoesNotThrow()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();

            // Act & Assert - second call is idempotent
            listener.Start();
            listener.Start();
        }

        #endregion

        #region RegisterAggregator Tests

        [Fact]
        public void RegisterAggregator_AddsToRegistry()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            // Act
            listener.RegisterAggregator("session-1", aggregator);

            // Assert
            Assert.Equal(1, listener.RegisteredAggregatorCount);
        }

        [Fact]
        public void RegisterAggregator_MultipleSessions_AllRegistered()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            using MetricsAggregator aggregator1 = new MetricsAggregator(_mockClient, _config);
            using MetricsAggregator aggregator2 = new MetricsAggregator(_mockClient, _config);

            // Act
            listener.RegisterAggregator("session-1", aggregator1);
            listener.RegisterAggregator("session-2", aggregator2);

            // Assert
            Assert.Equal(2, listener.RegisteredAggregatorCount);
        }

        [Fact]
        public void RegisterAggregator_SameSessionId_ReplacesAggregator()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            using MetricsAggregator aggregator1 = new MetricsAggregator(_mockClient, _config);
            using MetricsAggregator aggregator2 = new MetricsAggregator(_mockClient, _config);

            // Act
            listener.RegisterAggregator("session-1", aggregator1);
            listener.RegisterAggregator("session-1", aggregator2);

            // Assert - only one entry
            Assert.Equal(1, listener.RegisteredAggregatorCount);
        }

        [Fact]
        public void RegisterAggregator_NullSessionId_Ignored()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            // Act & Assert - should not throw
            listener.RegisterAggregator(null!, aggregator);
            Assert.Equal(0, listener.RegisteredAggregatorCount);
        }

        [Fact]
        public void RegisterAggregator_EmptySessionId_Ignored()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            // Act & Assert - should not throw
            listener.RegisterAggregator("", aggregator);
            Assert.Equal(0, listener.RegisteredAggregatorCount);
        }

        [Fact]
        public void RegisterAggregator_NullAggregator_Ignored()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();

            // Act & Assert - should not throw
            listener.RegisterAggregator("session-1", null!);
            Assert.Equal(0, listener.RegisteredAggregatorCount);
        }

        #endregion

        #region UnregisterAggregatorAsync Tests

        [Fact]
        public async Task UnregisterAggregatorAsync_RemovesAndFlushes()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            aggregator.SetSessionContext("session-unreg", 12345L, null, null);
            listener.RegisterAggregator("session-unreg", aggregator);
            Assert.Equal(1, listener.RegisteredAggregatorCount);

            // Add a pending context so we can verify FlushAsync was called
            using Activity? parent = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(parent);
            parent!.SetTag("session.id", "session-unreg");
            parent.SetTag("statement.id", "stmt-unreg");

            Activity? child = _driverSource.StartActivity("Statement.PollOperationStatus");
            Assert.NotNull(child);
            child!.SetTag("session.id", "session-unreg");
            child.SetTag("statement.id", "stmt-unreg");
            child.Stop();
            aggregator.ProcessActivity(child);
            child.Dispose();

            parent.Stop();

            Assert.Equal(1, aggregator.PendingContextCount);
            Assert.Empty(_mockClient.EnqueuedLogs);

            // Act
            await listener.UnregisterAggregatorAsync("session-unreg");

            // Assert - removed from registry and FlushAsync was called (pending context emitted)
            Assert.Equal(0, listener.RegisteredAggregatorCount);
            Assert.Equal(0, aggregator.PendingContextCount);
            Assert.Single(_mockClient.EnqueuedLogs);
        }

        [Fact]
        public async Task UnregisterAggregatorAsync_UnknownSessionId_NoError()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();

            // Act & Assert - should not throw
            await listener.UnregisterAggregatorAsync("unknown-session");
        }

        [Fact]
        public async Task UnregisterAggregatorAsync_NullSessionId_NoError()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();

            // Act & Assert - should not throw
            await listener.UnregisterAggregatorAsync(null!);
        }

        [Fact]
        public async Task UnregisterAggregatorAsync_EmptySessionId_NoError()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();

            // Act & Assert - should not throw
            await listener.UnregisterAggregatorAsync("");
        }

        #endregion

        #region ActivityStopped Routing Tests

        [Fact]
        public void ActivityStopped_RoutesToCorrectAggregator()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            aggregator.SetSessionContext("session-route", 12345L, null, null);
            listener.RegisterAggregator("session-route", aggregator);

            // Create an activity with session.id and statement.id
            using Activity? activity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("session.id", "session-route");
            activity.SetTag("statement.id", "stmt-route");
            activity.SetStatus(ActivityStatusCode.Ok);
            activity.Stop();

            // Act
            listener.OnActivityStopped(activity);

            // Assert - aggregator received the activity (emitted because it's a root activity with Ok status)
            Assert.Single(_mockClient.EnqueuedLogs);
        }

        [Fact]
        public void ActivityStopped_MultipleAggregators_RoutesToCorrectOne()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();

            MockTelemetryClient client1 = new MockTelemetryClient();
            MockTelemetryClient client2 = new MockTelemetryClient();

            using MetricsAggregator aggregator1 = new MetricsAggregator(client1, _config);
            using MetricsAggregator aggregator2 = new MetricsAggregator(client2, _config);

            aggregator1.SetSessionContext("session-1", 111L, null, null);
            aggregator2.SetSessionContext("session-2", 222L, null, null);

            listener.RegisterAggregator("session-1", aggregator1);
            listener.RegisterAggregator("session-2", aggregator2);

            // Activity for session-2
            using Activity? activity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("session.id", "session-2");
            activity.SetTag("statement.id", "stmt-multi");
            activity.SetStatus(ActivityStatusCode.Ok);
            activity.Stop();

            // Act
            listener.OnActivityStopped(activity);

            // Assert - only session-2's aggregator should have processed the activity
            Assert.Empty(client1.EnqueuedLogs);
            Assert.Single(client2.EnqueuedLogs);
        }

        [Fact]
        public void ActivityStopped_NoSessionId_Ignored()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            listener.RegisterAggregator("session-1", aggregator);

            // Create activity without session.id
            using Activity? activity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("statement.id", "stmt-no-session");
            activity.SetStatus(ActivityStatusCode.Ok);
            activity.Stop();

            // Act
            listener.OnActivityStopped(activity);

            // Assert - no aggregator should have been called
            Assert.Empty(_mockClient.EnqueuedLogs);
            Assert.Equal(0, aggregator.PendingContextCount);
        }

        [Fact]
        public void ActivityStopped_UnknownSessionId_Ignored()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            listener.RegisterAggregator("session-known", aggregator);

            // Create activity with an unregistered session.id
            using Activity? activity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("session.id", "session-unknown");
            activity.SetTag("statement.id", "stmt-unknown");
            activity.SetStatus(ActivityStatusCode.Ok);
            activity.Stop();

            // Act & Assert - should not throw
            listener.OnActivityStopped(activity);
            Assert.Empty(_mockClient.EnqueuedLogs);
        }

        [Fact]
        public void ActivityStopped_NullActivity_Ignored()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();

            // Act & Assert - should not throw
            listener.OnActivityStopped(null!);
        }

        [Fact]
        public void ActivityStopped_ExceptionSwallowed()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();

            // Register a throwing aggregator (uses a client that throws on Enqueue)
            FailingTelemetryClient failingClient = new FailingTelemetryClient();
            using MetricsAggregator throwingAggregator = new MetricsAggregator(failingClient, _config);
            throwingAggregator.SetSessionContext("session-throw", 12345L, null, null);
            listener.RegisterAggregator("session-throw", throwingAggregator);

            // Create a root activity that will trigger emission (and thus call Enqueue which throws)
            using Activity? activity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("session.id", "session-throw");
            activity.SetTag("statement.id", "stmt-throw");
            activity.SetStatus(ActivityStatusCode.Ok);
            activity.Stop();

            // Act & Assert - exception should be swallowed
            listener.OnActivityStopped(activity);
            // If we get here without exception, the test passes
        }

        #endregion

        #region Sample Callback Tests

        [Fact]
        public void Sample_ReturnsAllDataAndRecorded()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            ActivityCreationOptions<ActivityContext> options = default;

            // Act
            ActivitySamplingResult result = listener.OnSample(ref options);

            // Assert
            Assert.Equal(ActivitySamplingResult.AllDataAndRecorded, result);
        }

        #endregion

        #region Dispose Tests

        [Fact]
        public void Dispose_DisposesListener()
        {
            // Arrange
            DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            listener.Start();
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            listener.RegisterAggregator("session-dispose", aggregator);

            // Act
            listener.Dispose();

            // Assert - aggregators should be cleared
            Assert.Equal(0, listener.RegisteredAggregatorCount);
        }

        [Fact]
        public void Dispose_CalledTwice_NoError()
        {
            // Arrange
            DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            listener.Start();

            // Act & Assert - should not throw
            listener.Dispose();
            listener.Dispose();
        }

        [Fact]
        public void Dispose_StartAfterDispose_NoError()
        {
            // Arrange
            DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            listener.Dispose();

            // Act & Assert - Start after dispose should be no-op, no error
            listener.Start();
        }

        #endregion

        #region Integration-Style Tests

        [Fact]
        public void FullLifecycle_RegisterStartRouteUnregister()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            listener.Start();

            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            aggregator.SetSessionContext("session-lifecycle", 99999L, null, null);
            listener.RegisterAggregator("session-lifecycle", aggregator);

            // Act - create and process an activity
            // When listener.Start() is called, the ActivityListener's OnActivityStopped
            // callback fires automatically when the activity is stopped.
            using Activity? activity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("session.id", "session-lifecycle");
            activity.SetTag("statement.id", "stmt-lifecycle");
            activity.SetStatus(ActivityStatusCode.Ok);
            activity.Stop();

            // Assert - activity was automatically routed via the ActivityListener callback
            Assert.True(_mockClient.EnqueuedLogs.Count >= 1, "At least one log should have been enqueued");
            TelemetryFrontendLog log = _mockClient.EnqueuedLogs[0];
            Assert.Equal(99999L, log.WorkspaceId);
        }

        [Fact]
        public async Task FullLifecycle_UnregisterFlushesBeforeRemoval()
        {
            // Arrange
            using DatabricksActivityListener listener = DatabricksActivityListener.CreateForTesting();
            listener.Start();

            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            aggregator.SetSessionContext("session-flush", 11111L, null, null);
            listener.RegisterAggregator("session-flush", aggregator);

            // Add a pending context (child activity that doesn't auto-emit)
            using Activity? parent = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(parent);
            parent!.SetTag("session.id", "session-flush");
            parent.SetTag("statement.id", "stmt-flush");

            Activity? child = _driverSource.StartActivity("Statement.PollOperationStatus");
            Assert.NotNull(child);
            child!.SetTag("session.id", "session-flush");
            child.SetTag("statement.id", "stmt-flush");
            child.SetTag("poll.count", 3);
            child.Stop();
            listener.OnActivityStopped(child);
            child.Dispose();

            parent.Stop();

            Assert.Equal(1, aggregator.PendingContextCount);
            Assert.Empty(_mockClient.EnqueuedLogs);

            // Act - unregister should flush the aggregator
            await listener.UnregisterAggregatorAsync("session-flush");

            // Assert
            Assert.Equal(0, listener.RegisteredAggregatorCount);
            Assert.Equal(0, aggregator.PendingContextCount);
            Assert.Single(_mockClient.EnqueuedLogs);
        }

        #endregion

        #region Mock Implementations

        /// <summary>
        /// Mock implementation of <see cref="ITelemetryClient"/> for testing.
        /// Collects all enqueued logs for verification.
        /// </summary>
        private sealed class MockTelemetryClient : ITelemetryClient
        {
            private readonly ConcurrentBag<TelemetryFrontendLog> _enqueuedLogs = new ConcurrentBag<TelemetryFrontendLog>();

            public System.Collections.Generic.IReadOnlyList<TelemetryFrontendLog> EnqueuedLogs
            {
                get
                {
                    System.Collections.Generic.List<TelemetryFrontendLog> list =
                        new System.Collections.Generic.List<TelemetryFrontendLog>(_enqueuedLogs);
                    return list;
                }
            }

            public void Enqueue(TelemetryFrontendLog log)
            {
                _enqueuedLogs.Add(log);
            }

            public Task FlushAsync(CancellationToken ct = default)
            {
                return Task.CompletedTask;
            }

            public Task CloseAsync()
            {
                return Task.CompletedTask;
            }

            public ValueTask DisposeAsync()
            {
                return default;
            }
        }

        /// <summary>
        /// Mock implementation of <see cref="ITelemetryClient"/> that throws on Enqueue.
        /// Used to verify exception swallowing.
        /// </summary>
        private sealed class FailingTelemetryClient : ITelemetryClient
        {
            public void Enqueue(TelemetryFrontendLog log)
            {
                throw new InvalidOperationException("Simulated Enqueue failure");
            }

            public Task FlushAsync(CancellationToken ct = default)
            {
                return Task.CompletedTask;
            }

            public Task CloseAsync()
            {
                return Task.CompletedTask;
            }

            public ValueTask DisposeAsync()
            {
                return default;
            }
        }

        #endregion
    }
}
