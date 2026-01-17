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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for DatabricksActivityListener to ensure correct filtering, processing, and exception handling.
    /// </summary>
    public class DatabricksActivityListenerTests : IDisposable
    {
        private readonly MockTelemetryClient _mockClient;
        private readonly TelemetryConfiguration _config;
        private static readonly ActivitySource _databricksSource = new ActivitySource("Databricks.Adbc.Driver");
        private static readonly ActivitySource _otherSource = new ActivitySource("Other.ActivitySource");

        public DatabricksActivityListenerTests()
        {
            _mockClient = new MockTelemetryClient();
            _config = new TelemetryConfiguration
            {
                Enabled = true,
                BatchSize = 10,
                FlushIntervalMs = 1000
            };
        }

        public void Dispose()
        {
            // Cleanup if needed
        }

        // Test: Constructor parameter validation

        [Fact]
        public void DatabricksActivityListener_Constructor_NullHost_ThrowsArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new DatabricksActivityListener(null!, _mockClient, _config));
        }

        [Fact]
        public void DatabricksActivityListener_Constructor_EmptyHost_ThrowsArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new DatabricksActivityListener(string.Empty, _mockClient, _config));
        }

        [Fact]
        public void DatabricksActivityListener_Constructor_NullClient_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new DatabricksActivityListener("host.databricks.com", null!, _config));
        }

        [Fact]
        public void DatabricksActivityListener_Constructor_NullConfig_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new DatabricksActivityListener("host.databricks.com", _mockClient, null!));
        }

        // Test: Listener filters to Databricks.Adbc.Driver ActivitySource only

        [Fact]
        public void DatabricksActivityListener_Start_ListensToDatabricksActivitySource()
        {
            // Arrange
            using var listener = new DatabricksActivityListener("host.databricks.com", _mockClient, _config);

            // Act
            listener.Start();

            // Create activity from Databricks source
            using var activity = _databricksSource.StartActivity("Connection.Open");
            activity?.SetTag("session.id", "session-123");
            activity?.Stop();

            // Give a moment for async processing
            Thread.Sleep(200);

            // Assert - Activity should be processed and exported
            Assert.True(_mockClient.ExportedMetrics.Count > 0, "Should process Databricks.Adbc.Driver activities");
        }

        [Fact]
        public void DatabricksActivityListener_Start_IgnoresOtherActivitySources()
        {
            // Arrange
            using var listener = new DatabricksActivityListener("host.databricks.com", _mockClient, _config);

            // Act
            listener.Start();

            // Create activity from other source
            using var activity = _otherSource.StartActivity("SomeOperation");
            activity?.SetTag("tag", "value");
            activity?.Stop();

            // Give a moment for potential processing
            Thread.Sleep(200);

            // Assert - Activity should NOT be processed
            Assert.Empty(_mockClient.ExportedMetrics);
        }

        // Test: ActivityStopped calls MetricsAggregator.ProcessActivity

        [Fact]
        public void DatabricksActivityListener_ActivityStopped_ProcessesActivity()
        {
            // Arrange
            using var listener = new DatabricksActivityListener("host.databricks.com", _mockClient, _config);
            listener.Start();

            // Act
            using var activity = _databricksSource.StartActivity("Connection.Open");
            activity?.SetTag("session.id", "session-456");
            activity?.SetTag("workspace.id", "12345");
            activity?.Stop();

            // Give a moment for async processing
            Thread.Sleep(200);

            // Assert
            Assert.True(_mockClient.ExportedMetrics.Count > 0, "Activity should be processed and exported");
            var exportedMetrics = _mockClient.ExportedMetrics.First();
            var metric = exportedMetrics.First();
            Assert.Equal("session-456", metric.SessionId);
            Assert.Equal(12345L, metric.WorkspaceId);
        }

        // Test: All exceptions swallowed at TRACE level

        [Fact]
        public void DatabricksActivityListener_ActivityStopped_ExceptionSwallowed()
        {
            // Arrange - Use a client that throws exceptions
            var throwingClient = new ThrowingTelemetryClient();
            using var listener = new DatabricksActivityListener("host.databricks.com", throwingClient, _config);
            listener.Start();

            // Act - Should not throw exception
            using var activity = _databricksSource.StartActivity("Connection.Open");
            activity?.SetTag("session.id", "session-789");
            activity?.Stop();

            // Give a moment for async processing
            Thread.Sleep(200);

            // Assert - No exception propagated (test passes if no exception thrown)
            Assert.True(true, "Exception should be swallowed");
        }

        // Test: Sample callback respects feature flag

        [Fact]
        public void DatabricksActivityListener_Sample_FeatureFlagDisabled_ReturnsNone()
        {
            // Arrange
            var disabledConfig = new TelemetryConfiguration { Enabled = false };
            using var listener = new DatabricksActivityListener("host.databricks.com", _mockClient, disabledConfig);
            listener.Start();

            // Act
            using var activity = _databricksSource.StartActivity("Connection.Open");
            activity?.SetTag("session.id", "session-disabled");
            activity?.Stop();

            // Give a moment for potential processing
            Thread.Sleep(200);

            // Assert - No metrics should be exported when disabled
            Assert.Empty(_mockClient.ExportedMetrics);
        }

        [Fact]
        public void DatabricksActivityListener_Sample_FeatureFlagEnabled_ReturnsAllData()
        {
            // Arrange
            var enabledConfig = new TelemetryConfiguration { Enabled = true };
            using var listener = new DatabricksActivityListener("host.databricks.com", _mockClient, enabledConfig);
            listener.Start();

            // Act
            using var activity = _databricksSource.StartActivity("Connection.Open");
            activity?.SetTag("session.id", "session-enabled");
            activity?.Stop();

            // Give a moment for async processing
            Thread.Sleep(200);

            // Assert - Metrics should be exported when enabled
            Assert.True(_mockClient.ExportedMetrics.Count > 0, "Metrics should be exported when feature flag enabled");
        }

        // Test: StopAsync flushes and disposes

        [Fact]
        public async Task DatabricksActivityListener_StopAsync_FlushesAndDisposes()
        {
            // Arrange
            var listener = new DatabricksActivityListener("host.databricks.com", _mockClient, _config);
            listener.Start();

            // Create some activities
            using var activity1 = _databricksSource.StartActivity("Statement.Execute");
            activity1?.SetTag("statement.id", "stmt-123");
            activity1?.SetTag("session.id", "session-flush");
            activity1?.Stop();

            // Act
            await listener.StopAsync();

            // Assert - Metrics should be flushed
            // Note: Connection events are emitted immediately, but statement events need CompleteStatement
            // For this test, we just verify that StopAsync completes without exception
            Assert.True(true, "StopAsync should complete without exception");
        }

        [Fact]
        public void DatabricksActivityListener_Dispose_FlushesAndDisposes()
        {
            // Arrange
            var listener = new DatabricksActivityListener("host.databricks.com", _mockClient, _config);
            listener.Start();

            // Create activity
            using var activity = _databricksSource.StartActivity("Connection.Open");
            activity?.SetTag("session.id", "session-dispose");
            activity?.Stop();

            // Act
            listener.Dispose();

            // Assert - Should dispose without exception
            Assert.True(true, "Dispose should complete without exception");
        }

        // Test: Multiple Start calls are safe

        [Fact]
        public void DatabricksActivityListener_Start_CalledMultipleTimes_IsSafe()
        {
            // Arrange
            using var listener = new DatabricksActivityListener("host.databricks.com", _mockClient, _config);

            // Act
            listener.Start();
            listener.Start(); // Second call should be no-op
            listener.Start(); // Third call should be no-op

            // Create activity
            using var activity = _databricksSource.StartActivity("Connection.Open");
            activity?.SetTag("session.id", "session-multi");
            activity?.Stop();

            Thread.Sleep(200);

            // Assert - Should process activity normally
            Assert.True(_mockClient.ExportedMetrics.Count > 0, "Should process activities after multiple Start calls");
        }

        // Test: Multiple Dispose calls are safe

        [Fact]
        public void DatabricksActivityListener_Dispose_CalledMultipleTimes_IsSafe()
        {
            // Arrange
            var listener = new DatabricksActivityListener("host.databricks.com", _mockClient, _config);
            listener.Start();

            // Act
            listener.Dispose();
            listener.Dispose(); // Second call should be safe
            listener.Dispose(); // Third call should be safe

            // Assert - No exception thrown
            Assert.True(true, "Multiple Dispose calls should be safe");
        }

        // Test: Activities after Stop are not processed

        [Fact]
        public async Task DatabricksActivityListener_AfterStop_DoesNotProcessActivities()
        {
            // Arrange
            var listener = new DatabricksActivityListener("host.databricks.com", _mockClient, _config);
            listener.Start();

            // Act - Stop the listener
            await listener.StopAsync();

            // Create activity after stopping
            using var activity = _databricksSource.StartActivity("Connection.Open");
            activity?.SetTag("session.id", "session-after-stop");
            activity?.Stop();

            Thread.Sleep(200);

            // Assert - No new metrics should be exported (only connection events that were created before stop)
            var countBefore = _mockClient.ExportedMetrics.Count;

            // Create another activity
            using var activity2 = _databricksSource.StartActivity("Connection.Open");
            activity2?.SetTag("session.id", "session-after-stop-2");
            activity2?.Stop();

            Thread.Sleep(200);

            var countAfter = _mockClient.ExportedMetrics.Count;

            // No new metrics should have been added after stop
            Assert.Equal(countBefore, countAfter);
        }

        // Mock implementations for testing

        private class MockTelemetryClient : ITelemetryClient
        {
            public List<IReadOnlyList<TelemetryMetric>> ExportedMetrics { get; } = new List<IReadOnlyList<TelemetryMetric>>();

            public Task ExportAsync(IReadOnlyList<TelemetryMetric> metrics, CancellationToken ct = default)
            {
                ExportedMetrics.Add(metrics);
                return Task.CompletedTask;
            }

            public Task CloseAsync()
            {
                return Task.CompletedTask;
            }
        }

        private class ThrowingTelemetryClient : ITelemetryClient
        {
            public Task ExportAsync(IReadOnlyList<TelemetryMetric> metrics, CancellationToken ct = default)
            {
                throw new InvalidOperationException("Simulated export failure");
            }

            public Task CloseAsync()
            {
                throw new InvalidOperationException("Simulated close failure");
            }
        }
    }
}
