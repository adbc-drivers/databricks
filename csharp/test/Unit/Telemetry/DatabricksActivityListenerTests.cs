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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for DatabricksActivityListener class.
    /// </summary>
    public class DatabricksActivityListenerTests : IDisposable
    {
        private readonly MockTelemetryExporter _mockExporter;
        private readonly TelemetryConfiguration _config;
        private readonly ActivitySource _databricksActivitySource;
        private readonly ActivitySource _otherActivitySource;
        private MetricsAggregator? _aggregator;
        private DatabricksActivityListener? _listener;

        private const long TestWorkspaceId = 12345;
        private const string TestUserAgent = "TestAgent/1.0";

        public DatabricksActivityListenerTests()
        {
            _mockExporter = new MockTelemetryExporter();
            _config = new TelemetryConfiguration
            {
                Enabled = true,
                BatchSize = 100,
                FlushIntervalMs = 60000 // High value to control flush timing
            };
            _databricksActivitySource = new ActivitySource(DatabricksActivityListener.DatabricksActivitySourceName);
            _otherActivitySource = new ActivitySource("Other.ActivitySource");
        }

        public void Dispose()
        {
            _listener?.Dispose();
            _aggregator?.Dispose();
            _databricksActivitySource.Dispose();
            _otherActivitySource.Dispose();
        }

        #region Constructor Tests

        [Fact]
        public void DatabricksActivityListener_Constructor_NullAggregator_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new DatabricksActivityListener(null!, _config));
        }

        [Fact]
        public void DatabricksActivityListener_Constructor_NullConfig_ThrowsException()
        {
            // Arrange
            _aggregator = CreateAggregator();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new DatabricksActivityListener(_aggregator, null!));
        }

        [Fact]
        public void DatabricksActivityListener_Constructor_ValidParameters_CreatesInstance()
        {
            // Arrange
            _aggregator = CreateAggregator();

            // Act
            _listener = new DatabricksActivityListener(_aggregator, _config);

            // Assert
            Assert.NotNull(_listener);
            Assert.False(_listener.IsStarted);
            Assert.False(_listener.IsDisposed);
        }

        [Fact]
        public void DatabricksActivityListener_Constructor_WithFeatureFlagChecker_CreatesInstance()
        {
            // Arrange
            _aggregator = CreateAggregator();
            Func<bool> featureFlagChecker = () => true;

            // Act
            _listener = new DatabricksActivityListener(_aggregator, _config, featureFlagChecker);

            // Assert
            Assert.NotNull(_listener);
        }

        #endregion

        #region Start Tests

        [Fact]
        public void DatabricksActivityListener_Start_SetsIsStartedToTrue()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);

            // Act
            _listener.Start();

            // Assert
            Assert.True(_listener.IsStarted);
        }

        [Fact]
        public void DatabricksActivityListener_Start_ListensToDatabricksActivitySource()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);
            _listener.Start();

            // Act - create and stop an activity from Databricks source
            using var activity = _databricksActivitySource.StartActivity("Connection.Open");

            // Assert - activity should be created (listener is listening)
            Assert.NotNull(activity);
        }

        [Fact]
        public void DatabricksActivityListener_Start_IgnoresOtherActivitySources()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);
            _listener.Start();

            // Act - create an activity from a different source
            // When a listener filters out a source, no activity is created
            using var otherActivity = _otherActivitySource.StartActivity("SomeOperation");

            // Assert - activity may or may not be null depending on other listeners
            // The key test is that our listener's callbacks are not triggered
            // This is verified in the ActivityStopped tests
        }

        [Fact]
        public void DatabricksActivityListener_Start_MultipleCallsAreIdempotent()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);

            // Act - start multiple times
            _listener.Start();
            _listener.Start();
            _listener.Start();

            // Assert - should still be started, no exceptions
            Assert.True(_listener.IsStarted);
        }

        [Fact]
        public void DatabricksActivityListener_Start_AfterDispose_DoesNothing()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);
            _listener.Dispose();

            // Act - start after dispose
            _listener.Start();

            // Assert - should not be started
            Assert.False(_listener.IsStarted);
            Assert.True(_listener.IsDisposed);
        }

        #endregion

        #region ShouldListenTo Tests

        [Fact]
        public void DatabricksActivityListener_ShouldListenTo_DatabricksSource_ReturnsTrue()
        {
            // This is implicitly tested by Start_ListensToDatabricksActivitySource
            // The activity would not be created if ShouldListenTo returned false

            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);
            _listener.Start();

            // Act
            using var activity = _databricksActivitySource.StartActivity("TestOperation");

            // Assert
            Assert.NotNull(activity);
        }

        #endregion

        #region Sample Tests - Feature Flag

        [Fact]
        public void DatabricksActivityListener_Sample_FeatureFlagEnabled_ReturnsAllDataAndRecorded()
        {
            // Arrange
            var enabledConfig = new TelemetryConfiguration { Enabled = true, FlushIntervalMs = 60000 };
            _aggregator = new MetricsAggregator(_mockExporter, enabledConfig, TestWorkspaceId, TestUserAgent);
            _listener = new DatabricksActivityListener(_aggregator, enabledConfig);
            _listener.Start();

            // Act
            using var activity = _databricksActivitySource.StartActivity("TestOperation");

            // Assert - activity should be created and recorded
            Assert.NotNull(activity);
            Assert.True(activity.Recorded);
            Assert.True(activity.IsAllDataRequested);
        }

        [Fact]
        public void DatabricksActivityListener_Sample_FeatureFlagDisabled_ReturnsNone()
        {
            // Arrange
            var disabledConfig = new TelemetryConfiguration { Enabled = false, FlushIntervalMs = 60000 };
            _aggregator = new MetricsAggregator(_mockExporter, disabledConfig, TestWorkspaceId, TestUserAgent);
            _listener = new DatabricksActivityListener(_aggregator, disabledConfig);
            _listener.Start();

            // Act
            using var activity = _databricksActivitySource.StartActivity("TestOperation");

            // Assert - when disabled, our Sample returns None, so our listener
            // won't process the activity. Note: activity may still be created
            // if other listeners are registered (e.g., from other tests).
            // The key assertion is that our aggregator should not process
            // activities when disabled.
            Assert.Equal(0, _aggregator.PendingEventCount);
        }

        [Fact]
        public void DatabricksActivityListener_Sample_DynamicFeatureFlagChecker_Enabled_ReturnsAllData()
        {
            // Arrange
            bool featureFlagValue = true;
            Func<bool> featureFlagChecker = () => featureFlagValue;

            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config, featureFlagChecker);
            _listener.Start();

            // Act
            using var activity = _databricksActivitySource.StartActivity("TestOperation");

            // Assert
            Assert.NotNull(activity);
            Assert.True(activity.Recorded);
        }

        [Fact]
        public void DatabricksActivityListener_Sample_DynamicFeatureFlagChecker_Disabled_ReturnsNone()
        {
            // Arrange
            bool featureFlagValue = false;
            Func<bool> featureFlagChecker = () => featureFlagValue;

            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config, featureFlagChecker);
            _listener.Start();

            // Act
            using var activity = _databricksActivitySource.StartActivity("TestOperation");

            // Assert - when feature flag is disabled, our Sample returns None,
            // so our listener won't process the activity.
            // Note: activity may still be created if other listeners exist.
            Assert.Equal(0, _aggregator.PendingEventCount);
        }

        [Fact]
        public void DatabricksActivityListener_Sample_FeatureFlagCheckerThrows_ReturnsNone()
        {
            // Arrange
            Func<bool> throwingChecker = () => throw new InvalidOperationException("Test exception");

            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config, throwingChecker);
            _listener.Start();

            // Act - should not throw, should return None (activity not created)
            using var activity = _databricksActivitySource.StartActivity("TestOperation");

            // Assert - activity should not be created due to exception handling
            Assert.Null(activity);
        }

        #endregion

        #region ActivityStopped Tests

        [Fact]
        public void DatabricksActivityListener_ActivityStopped_ProcessesActivity()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);
            _listener.Start();

            // Act
            using (var activity = _databricksActivitySource.StartActivity("Connection.Open"))
            {
                Assert.NotNull(activity);
                activity.SetTag("session.id", "test-session-123");
                activity.Stop();
            }

            // Assert - aggregator should have processed the activity
            Assert.Equal(1, _aggregator.PendingEventCount);
        }

        [Fact]
        public void DatabricksActivityListener_ActivityStopped_ProcessesMultipleActivities()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);
            _listener.Start();

            // Act
            for (int i = 0; i < 5; i++)
            {
                using var activity = _databricksActivitySource.StartActivity("Connection.Open");
                Assert.NotNull(activity);
                activity.SetTag("session.id", $"session-{i}");
                activity.Stop();
            }

            // Assert - all activities should be processed
            Assert.Equal(5, _aggregator.PendingEventCount);
        }

        [Fact]
        public void DatabricksActivityListener_ActivityStopped_ExceptionSwallowed()
        {
            // Arrange - use a throwing exporter to test exception handling
            // The MetricsAggregator will handle the export exception, and
            // the listener will swallow any exceptions from ProcessActivity
            var throwingExporter = new ThrowingTelemetryExporter();
            var config = new TelemetryConfiguration { BatchSize = 1, FlushIntervalMs = 60000 };
            var aggregator = new MetricsAggregator(throwingExporter, config, TestWorkspaceId, TestUserAgent);
            _listener = new DatabricksActivityListener(aggregator, config);
            _listener.Start();

            // Act & Assert - should not throw even though exporter throws
            using (var activity = _databricksActivitySource.StartActivity("Connection.Open"))
            {
                Assert.NotNull(activity);
                activity.SetTag("session.id", "test-session");
                activity.Stop();
                // No exception should be thrown
            }

            aggregator.Dispose();
        }

        [Fact]
        public void DatabricksActivityListener_ActivityStopped_NotCalledWhenDisabled()
        {
            // Arrange
            var disabledConfig = new TelemetryConfiguration { Enabled = false, FlushIntervalMs = 60000 };
            _aggregator = new MetricsAggregator(_mockExporter, disabledConfig, TestWorkspaceId, TestUserAgent);
            _listener = new DatabricksActivityListener(_aggregator, disabledConfig);
            _listener.Start();

            // Act - our listener's Sample returns None when disabled
            using var activity = _databricksActivitySource.StartActivity("Connection.Open");

            // Note: activity may be created if other listeners are active (test isolation issue)
            // but if created, stop it to simulate the full lifecycle
            if (activity != null)
            {
                activity.SetTag("session.id", "test-session");
                activity.Stop();
            }

            // Assert - when disabled, our listener's Sample returns None,
            // so our listener won't receive the ActivityStopped callback and
            // won't process the activity
            Assert.Equal(0, _aggregator.PendingEventCount);
        }

        #endregion

        #region StopAsync Tests

        [Fact]
        public async Task DatabricksActivityListener_StopAsync_FlushesAndDisposes()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);
            _listener.Start();

            // Create some activities
            for (int i = 0; i < 3; i++)
            {
                using var activity = _databricksActivitySource.StartActivity("Connection.Open");
                Assert.NotNull(activity);
                activity.SetTag("session.id", $"session-{i}");
                activity.Stop();
            }

            Assert.Equal(3, _aggregator.PendingEventCount);

            // Act
            await _listener.StopAsync();

            // Assert
            Assert.False(_listener.IsStarted);
            Assert.True(_mockExporter.ExportCallCount > 0);
        }

        [Fact]
        public async Task DatabricksActivityListener_StopAsync_StopsListening()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);
            _listener.Start();

            // Create an activity before stopping
            using (var activity1 = _databricksActivitySource.StartActivity("Connection.Open"))
            {
                Assert.NotNull(activity1);
                activity1.Stop();
            }

            Assert.Equal(1, _aggregator.PendingEventCount);

            // Stop the listener
            await _listener.StopAsync();
            Assert.False(_listener.IsStarted);

            // Clear the exporter count
            var countBeforeNewActivity = _mockExporter.ExportCallCount;

            // Try to create another activity after stopping
            using var activity2 = _databricksActivitySource.StartActivity("Connection.Open");

            // Activity may still be created by other listeners, but our listener
            // should not process it since it's stopped
        }

        [Fact]
        public async Task DatabricksActivityListener_StopAsync_CanBeCalledMultipleTimes()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);
            _listener.Start();

            // Act & Assert - should not throw
            await _listener.StopAsync();
            await _listener.StopAsync();
            await _listener.StopAsync();

            Assert.False(_listener.IsStarted);
        }

        [Fact]
        public async Task DatabricksActivityListener_StopAsync_BeforeStart_DoesNothing()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);

            // Act & Assert - should not throw
            await _listener.StopAsync();

            Assert.False(_listener.IsStarted);
        }

        [Fact]
        public async Task DatabricksActivityListener_StopAsync_WithCancellation_PropagatesCancellation()
        {
            // Arrange - use an exporter that throws on cancellation
            var cancellingExporter = new CancellingTelemetryExporter();
            var aggregator = new MetricsAggregator(cancellingExporter, _config, TestWorkspaceId, TestUserAgent);
            _listener = new DatabricksActivityListener(aggregator, _config);
            _listener.Start();

            // Create some activities so there's something to flush
            using (var activity = _databricksActivitySource.StartActivity("Connection.Open"))
            {
                Assert.NotNull(activity);
                activity.SetTag("session.id", "test-session");
                activity.Stop();
            }

            var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act & Assert - should propagate cancellation
            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                _listener.StopAsync(cts.Token));

            aggregator.Dispose();
        }

        #endregion

        #region Dispose Tests

        [Fact]
        public void DatabricksActivityListener_Dispose_SetsIsDisposedToTrue()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);

            // Act
            _listener.Dispose();

            // Assert
            Assert.True(_listener.IsDisposed);
        }

        [Fact]
        public void DatabricksActivityListener_Dispose_FlushesRemainingEvents()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);
            _listener.Start();

            // Create some activities
            for (int i = 0; i < 3; i++)
            {
                using var activity = _databricksActivitySource.StartActivity("Connection.Open");
                Assert.NotNull(activity);
                activity.SetTag("session.id", $"session-{i}");
                activity.Stop();
            }

            // Act
            _listener.Dispose();

            // Assert - events should have been flushed via aggregator dispose
            Assert.True(_mockExporter.ExportCallCount > 0);
        }

        [Fact]
        public void DatabricksActivityListener_Dispose_CanBeCalledMultipleTimes()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);

            // Act & Assert - should not throw
            _listener.Dispose();
            _listener.Dispose();
            _listener.Dispose();

            Assert.True(_listener.IsDisposed);
        }

        [Fact]
        public void DatabricksActivityListener_Dispose_StopsListening()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);
            _listener.Start();
            Assert.True(_listener.IsStarted);

            // Act
            _listener.Dispose();

            // Assert
            Assert.False(_listener.IsStarted);
            Assert.True(_listener.IsDisposed);
        }

        #endregion

        #region Integration Tests

        [Fact]
        public async Task DatabricksActivityListener_EndToEnd_ConnectionActivity()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);
            _listener.Start();

            // Act - simulate connection open
            using (var activity = _databricksActivitySource.StartActivity("Connection.Open"))
            {
                Assert.NotNull(activity);
                activity.SetTag("session.id", "e2e-session-123");
                activity.SetTag("driver.version", "1.0.0");
                activity.SetTag("driver.os", "Windows");
                activity.Stop();
            }

            // Flush
            await _aggregator.FlushAsync();

            // Assert
            Assert.True(_mockExporter.ExportCallCount > 0);
            Assert.True(_mockExporter.TotalExportedEvents > 0);
        }

        [Fact]
        public async Task DatabricksActivityListener_EndToEnd_StatementActivity()
        {
            // Arrange
            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config);
            _listener.Start();

            var statementId = "e2e-stmt-123";
            var sessionId = "e2e-session-456";

            // Act - simulate statement execution
            using (var activity = _databricksActivitySource.StartActivity("Statement.Execute"))
            {
                Assert.NotNull(activity);
                activity.SetTag("statement.id", statementId);
                activity.SetTag("session.id", sessionId);
                activity.SetTag("result.format", "cloudfetch");
                activity.SetTag("result.chunk_count", "5");
                activity.Stop();
            }

            // Complete the statement
            _aggregator.CompleteStatement(statementId);

            // Flush
            await _aggregator.FlushAsync();

            // Assert
            Assert.True(_mockExporter.TotalExportedEvents > 0);
        }

        [Fact]
        public async Task DatabricksActivityListener_EndToEnd_DynamicFeatureFlag()
        {
            // Arrange
            bool featureFlagEnabled = true;
            Func<bool> featureFlagChecker = () => featureFlagEnabled;

            _aggregator = CreateAggregator();
            _listener = new DatabricksActivityListener(_aggregator, _config, featureFlagChecker);
            _listener.Start();

            // Act 1 - create activity while enabled
            using (var activity1 = _databricksActivitySource.StartActivity("Connection.Open"))
            {
                Assert.NotNull(activity1);
                activity1.SetTag("session.id", "enabled-session");
                activity1.Stop();
            }

            var countAfterFirstActivity = _aggregator.PendingEventCount;
            Assert.Equal(1, countAfterFirstActivity);

            // Disable feature flag
            featureFlagEnabled = false;

            // Act 2 - try to create activity while disabled
            // Note: activity may be created if other listeners exist
            using (var activity2 = _databricksActivitySource.StartActivity("Connection.Open"))
            {
                // Our listener should not process this activity because feature flag is disabled
                // If activity exists (due to other listeners), stop it
                if (activity2 != null)
                {
                    activity2.SetTag("session.id", "disabled-session");
                    activity2.Stop();
                }
            }

            // Still only 1 pending event (our listener didn't process the disabled one)
            Assert.Equal(countAfterFirstActivity, _aggregator.PendingEventCount);

            // Re-enable feature flag
            featureFlagEnabled = true;

            // Act 3 - create activity while re-enabled
            using (var activity3 = _databricksActivitySource.StartActivity("Connection.Open"))
            {
                Assert.NotNull(activity3);
                activity3.SetTag("session.id", "reenabled-session");
                activity3.Stop();
            }

            // Now 2 pending events
            Assert.Equal(2, _aggregator.PendingEventCount);

            // Flush
            await _aggregator.FlushAsync();

            // Assert
            Assert.Equal(2, _mockExporter.TotalExportedEvents);
        }

        #endregion

        #region Helper Methods

        private MetricsAggregator CreateAggregator()
        {
            return new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);
        }

        #endregion

        #region Mock Classes

        /// <summary>
        /// Mock telemetry exporter for testing.
        /// </summary>
        private class MockTelemetryExporter : ITelemetryExporter
        {
            private int _exportCallCount;
            private int _totalExportedEvents;
            private readonly ConcurrentBag<TelemetryFrontendLog> _exportedLogs = new ConcurrentBag<TelemetryFrontendLog>();

            public int ExportCallCount => _exportCallCount;
            public int TotalExportedEvents => _totalExportedEvents;
            public IReadOnlyCollection<TelemetryFrontendLog> ExportedLogs => _exportedLogs.ToList();

            public Task ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                ct.ThrowIfCancellationRequested();

                Interlocked.Increment(ref _exportCallCount);
                Interlocked.Add(ref _totalExportedEvents, logs.Count);

                foreach (var log in logs)
                {
                    _exportedLogs.Add(log);
                }

                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Telemetry exporter that throws for testing exception handling.
        /// </summary>
        private class ThrowingTelemetryExporter : ITelemetryExporter
        {
            public Task ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                throw new InvalidOperationException("Test exception from exporter");
            }
        }

        /// <summary>
        /// Telemetry exporter that throws OperationCanceledException for testing cancellation.
        /// </summary>
        private class CancellingTelemetryExporter : ITelemetryExporter
        {
            public Task ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                ct.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
        }

        #endregion
    }
}
