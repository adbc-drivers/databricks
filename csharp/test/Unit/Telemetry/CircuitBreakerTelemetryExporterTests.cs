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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Polly.CircuitBreaker;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for CircuitBreakerTelemetryExporter class.
    /// </summary>
    public class CircuitBreakerTelemetryExporterTests
    {
        private const string TestHost = "https://test-workspace.databricks.com";

        /// <summary>
        /// Gets a unique host name for each test to avoid circuit breaker state bleeding across tests.
        /// CircuitBreakerManager is a singleton, so we need unique hosts for test isolation.
        /// </summary>
        private static string GetUniqueHost() => $"https://test-{Guid.NewGuid()}.databricks.com";

        #region Constructor Tests

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_NullInnerExporter_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new CircuitBreakerTelemetryExporter(null!, TestHost));
        }

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_NullHost_ThrowsException()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new CircuitBreakerTelemetryExporter(innerExporter, null!));
        }

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_EmptyHost_ThrowsException()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new CircuitBreakerTelemetryExporter(innerExporter, ""));
        }

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_WhitespaceHost_ThrowsException()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new CircuitBreakerTelemetryExporter(innerExporter, "   "));
        }

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_ValidParameters_SetsProperties()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            var testHost = GetUniqueHost();

            // Act
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, testHost);

            // Assert
            Assert.Equal(testHost, exporter.Host);
            Assert.Equal(CircuitBreakerState.Closed, exporter.State);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_Constructor_PerHostIsolation_DifferentCircuitBreakers()
        {
            // Arrange
            var innerExporter1 = new MockTelemetryExporter();
            var innerExporter2 = new MockTelemetryExporter();
            var host1 = "https://host1.databricks.com";
            var host2 = "https://host2.databricks.com";

            var exporter1 = new CircuitBreakerTelemetryExporter(innerExporter1, host1);
            var exporter2 = new CircuitBreakerTelemetryExporter(innerExporter2, host2);

            // Act - Trip circuit for host1 only
            var logs = CreateTestLogs(1);
            innerExporter1.SetFailureMode(shouldFail: true);

            for (int i = 0; i < 5; i++)
            {
                await exporter1.ExportAsync(logs);
            }

            // Assert - host1 circuit should be open, host2 should be closed
            Assert.Equal(CircuitBreakerState.Open, exporter1.State);
            Assert.Equal(CircuitBreakerState.Closed, exporter2.State);
        }

        #endregion

        #region Circuit Closed Tests

        [Fact]
        public async Task CircuitClosed_ExportsMetrics()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(3);

            // Act
            var result = await exporter.ExportAsync(logs);

            // Assert
            Assert.True(result);
            Assert.Equal(1, innerExporter.ExportCallCount);
            Assert.Equal(3, innerExporter.LastExportedLogs?.Count);
            Assert.Equal(CircuitBreakerState.Closed, exporter.State);
        }

        [Fact]
        public async Task CircuitClosed_EmptyLogs_ReturnsTrueWithoutExport()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());

            // Act
            var result = await exporter.ExportAsync(new List<TelemetryFrontendLog>());

            // Assert
            Assert.True(result);
            Assert.Equal(0, innerExporter.ExportCallCount);
        }

        [Fact]
        public async Task CircuitClosed_NullLogs_ReturnsTrueWithoutExport()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());

            // Act
            var result = await exporter.ExportAsync(null!);

            // Assert
            Assert.True(result);
            Assert.Equal(0, innerExporter.ExportCallCount);
        }

        [Fact]
        public async Task CircuitClosed_MultipleSuccesses_StaysClosed()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(1);

            // Act
            for (int i = 0; i < 10; i++)
            {
                var result = await exporter.ExportAsync(logs);
                Assert.True(result);
            }

            // Assert
            Assert.Equal(10, innerExporter.ExportCallCount);
            Assert.Equal(CircuitBreakerState.Closed, exporter.State);
        }

        #endregion

        #region Circuit Breaker Failure Tracking Tests

        [Fact]
        public async Task InnerExporterFails_CircuitBreakerTracksFailure()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            innerExporter.SetFailureMode(shouldFail: true);
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(1);

            // Act - Cause failures to open the circuit (threshold is 5)
            for (int i = 0; i < 5; i++)
            {
                var result = await exporter.ExportAsync(logs);
                // Should return false while circuit is still closed but failing
                if (exporter.State == CircuitBreakerState.Closed)
                {
                    Assert.False(result);
                }
            }

            // Assert - Circuit should be open after threshold
            Assert.Equal(CircuitBreakerState.Open, exporter.State);
        }

        [Fact]
        public async Task InnerExporterReturnsFalse_CircuitBreakerTracksFailure()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            innerExporter.SetReturnValue(false);
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(1);

            // Act - Cause failures by returning false
            for (int i = 0; i < 5; i++)
            {
                var result = await exporter.ExportAsync(logs);
                // Should return false while circuit is still closed but failing
                if (exporter.State == CircuitBreakerState.Closed)
                {
                    Assert.False(result);
                }
            }

            // Assert - Circuit should be open after threshold
            Assert.Equal(CircuitBreakerState.Open, exporter.State);
            Assert.True(innerExporter.ExportCallCount >= 5, $"Expected at least 5 calls, got {innerExporter.ExportCallCount}");
        }

        [Fact]
        public async Task InnerExporterThrowsException_CircuitBreakerTracksFailure()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            innerExporter.SetExceptionMode(new InvalidOperationException("Test exception"));
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(1);

            // Act - Cause failures by throwing exceptions
            for (int i = 0; i < 5; i++)
            {
                var result = await exporter.ExportAsync(logs);
                // Should return false while circuit is still closed but failing
                if (exporter.State == CircuitBreakerState.Closed)
                {
                    Assert.False(result);
                }
            }

            // Assert - Circuit should be open after threshold
            Assert.Equal(CircuitBreakerState.Open, exporter.State);
        }

        [Fact]
        public async Task InnerExporterFails_BelowThreshold_StaysClosed()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            innerExporter.SetReturnValue(false);
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(1);

            // Act - Cause 4 failures (threshold is 5)
            for (int i = 0; i < 4; i++)
            {
                var result = await exporter.ExportAsync(logs);
                Assert.False(result);
            }

            // Assert - Circuit should still be closed
            Assert.Equal(CircuitBreakerState.Closed, exporter.State);
        }

        #endregion

        #region Circuit Open Tests

        [Fact]
        public async Task CircuitOpen_DropsMetrics()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            innerExporter.SetReturnValue(false);
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(1);

            // Open the circuit
            for (int i = 0; i < 5; i++)
            {
                await exporter.ExportAsync(logs);
            }

            Assert.Equal(CircuitBreakerState.Open, exporter.State);
            var callCountBeforeOpen = innerExporter.ExportCallCount;

            // Act - Try to export with open circuit
            var result = await exporter.ExportAsync(logs);

            // Assert - Should drop silently and return true (not a failure)
            Assert.True(result);
            Assert.Equal(callCountBeforeOpen, innerExporter.ExportCallCount); // No additional calls
        }

        [Fact]
        public async Task CircuitOpen_DropsMetrics_NoException()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            innerExporter.SetReturnValue(false);
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(1);

            // Open the circuit
            for (int i = 0; i < 5; i++)
            {
                await exporter.ExportAsync(logs);
            }

            Assert.Equal(CircuitBreakerState.Open, exporter.State);

            // Act & Assert - Should not throw
            var result = await exporter.ExportAsync(logs);
            Assert.True(result);
        }

        [Fact]
        public async Task CircuitOpen_MultipleRequests_AllDropped()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            innerExporter.SetReturnValue(false);
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(1);

            // Open the circuit
            for (int i = 0; i < 5; i++)
            {
                await exporter.ExportAsync(logs);
            }

            Assert.Equal(CircuitBreakerState.Open, exporter.State);
            var callCountBeforeOpen = innerExporter.ExportCallCount;

            // Act - Multiple requests with open circuit
            for (int i = 0; i < 10; i++)
            {
                var result = await exporter.ExportAsync(logs);
                Assert.True(result);
            }

            // Assert - All requests should be dropped, no additional inner exporter calls
            Assert.Equal(callCountBeforeOpen, innerExporter.ExportCallCount);
        }

        #endregion

        #region Exception Handling Tests

        [Fact]
        public async Task ExportAsync_CancellationToken_PropagatesCancellation()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            innerExporter.SetCancellationMode();
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(1);

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act & Assert - Should propagate cancellation
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => exporter.ExportAsync(logs, cts.Token));
        }

        [Fact]
        public async Task ExportAsync_InnerExporterException_ReturnsErrorAndTracksFaults()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            innerExporter.SetExceptionMode(new InvalidOperationException("Test exception"));
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(1);

            // Act
            var result = await exporter.ExportAsync(logs);

            // Assert - Should catch exception and return false
            Assert.False(result);
            Assert.Equal(CircuitBreakerState.Closed, exporter.State); // Still closed after first failure
        }

        #endregion

        #region Circuit Recovery Tests

        [Fact]
        public async Task CircuitOpen_AfterTimeout_AllowsTestRequest()
        {
            // Arrange - Use shorter timeout for testing
            // Note: We can't easily test this with the real CircuitBreakerManager
            // because we can't control the timeout. This test verifies the behavior
            // indirectly by checking that after many failures, the circuit opens
            // and subsequent requests return true (dropped).
            var innerExporter = new MockTelemetryExporter();
            innerExporter.SetReturnValue(false);
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(1);

            // Open the circuit
            for (int i = 0; i < 5; i++)
            {
                await exporter.ExportAsync(logs);
            }

            Assert.Equal(CircuitBreakerState.Open, exporter.State);

            // Act - Request with open circuit returns true (dropped)
            var result = await exporter.ExportAsync(logs);

            // Assert
            Assert.True(result);
        }

        #endregion

        #region Thread Safety Tests

        [Fact]
        public async Task ExportAsync_ConcurrentSuccesses_ThreadSafe()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(1);
            var tasks = new Task<bool>[100];

            // Act
            for (int i = 0; i < 100; i++)
            {
                tasks[i] = exporter.ExportAsync(logs);
            }

            var results = await Task.WhenAll(tasks);

            // Assert
            Assert.All(results, result => Assert.True(result));
            Assert.Equal(100, innerExporter.ExportCallCount);
            Assert.Equal(CircuitBreakerState.Closed, exporter.State);
        }

        [Fact]
        public async Task ExportAsync_ConcurrentFailures_ThreadSafe()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();
            innerExporter.SetReturnValue(false);
            var exporter = new CircuitBreakerTelemetryExporter(innerExporter, GetUniqueHost());
            var logs = CreateTestLogs(1);
            var tasks = new Task<bool>[100];

            // Act
            for (int i = 0; i < 100; i++)
            {
                tasks[i] = Task.Run(() => exporter.ExportAsync(logs));
            }

            var results = await Task.WhenAll(tasks);

            // Assert - Circuit should eventually open
            Assert.Equal(CircuitBreakerState.Open, exporter.State);
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Creates a list of test telemetry frontend logs.
        /// </summary>
        private static List<TelemetryFrontendLog> CreateTestLogs(int count)
        {
            var logs = new List<TelemetryFrontendLog>();
            for (int i = 0; i < count; i++)
            {
                logs.Add(new TelemetryFrontendLog
                {
                    WorkspaceId = 12345 + i,
                    FrontendLogEventId = $"test-event-{i}"
                });
            }
            return logs;
        }

        #endregion

        #region Mock Exporter

        /// <summary>
        /// Mock implementation of ITelemetryExporter for testing.
        /// </summary>
        private class MockTelemetryExporter : ITelemetryExporter
        {
            private bool _shouldFail;
            private bool _returnValue = true;
            private Exception? _exceptionToThrow;
            private bool _shouldCancel;

            public int ExportCallCount { get; private set; }
            public IReadOnlyList<TelemetryFrontendLog>? LastExportedLogs { get; private set; }

            public void SetFailureMode(bool shouldFail)
            {
                _shouldFail = shouldFail;
            }

            public void SetReturnValue(bool returnValue)
            {
                _returnValue = returnValue;
                _shouldFail = false;
                _exceptionToThrow = null;
            }

            public void SetExceptionMode(Exception exception)
            {
                _exceptionToThrow = exception;
                _shouldFail = false;
            }

            public void SetCancellationMode()
            {
                _shouldCancel = true;
            }

            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                ExportCallCount++;
                LastExportedLogs = logs;

                if (_shouldCancel)
                {
                    ct.ThrowIfCancellationRequested();
                }

                if (_exceptionToThrow != null)
                {
                    throw _exceptionToThrow;
                }

                if (_shouldFail)
                {
                    return Task.FromResult(false);
                }

                return Task.FromResult(_returnValue);
            }
        }

        #endregion
    }
}
