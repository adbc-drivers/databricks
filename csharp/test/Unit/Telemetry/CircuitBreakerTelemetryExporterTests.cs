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
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for CircuitBreakerTelemetryExporter class.
    /// </summary>
    public class CircuitBreakerTelemetryExporterTests
    {
        private const string TestHost = "https://test-workspace.databricks.com";

        #region Constructor Tests

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_NullHost_ThrowsException()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new CircuitBreakerTelemetryExporter(null!, mockExporter));
        }

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_EmptyHost_ThrowsException()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new CircuitBreakerTelemetryExporter(string.Empty, mockExporter));
        }

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_WhitespaceHost_ThrowsException()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new CircuitBreakerTelemetryExporter("   ", mockExporter));
        }

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_NullInnerExporter_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new CircuitBreakerTelemetryExporter(TestHost, null!));
        }

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_NullCircuitBreakerManager_ThrowsException()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new CircuitBreakerTelemetryExporter(TestHost, mockExporter, null!));
        }

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_ValidParameters_SetsProperties()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();

            // Act
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, mockExporter);

            // Assert
            Assert.Equal(TestHost, exporter.Host);
            Assert.Same(mockExporter, exporter.InnerExporter);
        }

        #endregion

        #region Circuit Closed Tests

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_CircuitClosed_ExportsEvents()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var manager = new CircuitBreakerManager();
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, mockExporter, manager);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345, FrontendLogEventId = "event-1" },
                new TelemetryFrontendLog { WorkspaceId = 12345, FrontendLogEventId = "event-2" }
            };

            // Act
            await exporter.ExportAsync(logs);

            // Assert
            Assert.Equal(1, mockExporter.ExportCallCount);
            Assert.Equal(logs, mockExporter.LastExportedLogs);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_CircuitClosed_MultipleExports_AllSucceed()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var manager = new CircuitBreakerManager();
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, mockExporter, manager);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Act
            await exporter.ExportAsync(logs);
            await exporter.ExportAsync(logs);
            await exporter.ExportAsync(logs);

            // Assert
            Assert.Equal(3, mockExporter.ExportCallCount);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_EmptyList_DoesNotCallInnerExporter()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var manager = new CircuitBreakerManager();
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, mockExporter, manager);

            // Act
            await exporter.ExportAsync(new List<TelemetryFrontendLog>());

            // Assert
            Assert.Equal(0, mockExporter.ExportCallCount);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_NullList_DoesNotCallInnerExporter()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var manager = new CircuitBreakerManager();
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, mockExporter, manager);

            // Act
            await exporter.ExportAsync(null!);

            // Assert
            Assert.Equal(0, mockExporter.ExportCallCount);
        }

        #endregion

        #region Circuit Open Tests

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_CircuitOpen_DropsEvents()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 2 };
            var manager = new CircuitBreakerManager(config);
            var failingExporter = new MockTelemetryExporter { ShouldThrow = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, failingExporter, manager);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Cause failures to open the circuit
            await exporter.ExportAsync(logs);
            await exporter.ExportAsync(logs);

            // Verify circuit is open
            var circuitBreaker = manager.GetCircuitBreaker(TestHost);
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Reset call count after opening circuit
            failingExporter.ExportCallCount = 0;

            // Act - Try to export while circuit is open
            await exporter.ExportAsync(logs);

            // Assert - Inner exporter should NOT be called (events dropped)
            Assert.Equal(0, failingExporter.ExportCallCount);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_CircuitOpen_DoesNotThrow()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 1 };
            var manager = new CircuitBreakerManager(config);
            var failingExporter = new MockTelemetryExporter { ShouldThrow = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, failingExporter, manager);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Cause failure to open the circuit
            await exporter.ExportAsync(logs);

            // Verify circuit is open
            var circuitBreaker = manager.GetCircuitBreaker(TestHost);
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Act & Assert - Should not throw even though circuit is open
            var exception = await Record.ExceptionAsync(() => exporter.ExportAsync(logs));
            Assert.Null(exception);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_CircuitOpen_MultipleExportAttempts_AllDropped()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 1, Timeout = TimeSpan.FromHours(1) };
            var manager = new CircuitBreakerManager(config);
            var failingExporter = new MockTelemetryExporter { ShouldThrow = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, failingExporter, manager);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Cause failure to open the circuit
            await exporter.ExportAsync(logs);

            // Reset call count and stop throwing
            failingExporter.ExportCallCount = 0;
            failingExporter.ShouldThrow = false;

            // Act - Try multiple exports while circuit is open
            await exporter.ExportAsync(logs);
            await exporter.ExportAsync(logs);
            await exporter.ExportAsync(logs);

            // Assert - All should be dropped (inner exporter not called)
            Assert.Equal(0, failingExporter.ExportCallCount);
        }

        #endregion

        #region Circuit Breaker Tracks Failure Tests

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_InnerExporterFails_CircuitBreakerTracksFailure()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 3 };
            var manager = new CircuitBreakerManager(config);
            var failingExporter = new MockTelemetryExporter { ShouldThrow = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, failingExporter, manager);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            var circuitBreaker = manager.GetCircuitBreaker(TestHost);

            // Act - Cause failures
            await exporter.ExportAsync(logs);
            Assert.Equal(1, circuitBreaker.ConsecutiveFailures);
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);

            await exporter.ExportAsync(logs);
            Assert.Equal(2, circuitBreaker.ConsecutiveFailures);
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);

            await exporter.ExportAsync(logs);

            // Assert - Circuit should now be open after 3 failures
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_InnerExporterFails_ExceptionSwallowed()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var failingExporter = new MockTelemetryExporter { ShouldThrow = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, failingExporter, manager);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Act & Assert - Should not throw even though inner exporter fails
            var exception = await Record.ExceptionAsync(() => exporter.ExportAsync(logs));
            Assert.Null(exception);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_InnerExporterSucceeds_CircuitBreakerResetsFailures()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 5 };
            var manager = new CircuitBreakerManager(config);
            var mockExporter = new MockTelemetryExporter { ShouldThrow = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, mockExporter, manager);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            var circuitBreaker = manager.GetCircuitBreaker(TestHost);

            // Cause 2 failures
            await exporter.ExportAsync(logs);
            await exporter.ExportAsync(logs);
            Assert.Equal(2, circuitBreaker.ConsecutiveFailures);

            // Now succeed
            mockExporter.ShouldThrow = false;
            await exporter.ExportAsync(logs);

            // Assert - Failures should be reset
            Assert.Equal(0, circuitBreaker.ConsecutiveFailures);
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
        }

        #endregion

        #region Cancellation Tests

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_Cancelled_PropagatesCancellation()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter { ShouldDelay = true };
            var manager = new CircuitBreakerManager();
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, mockExporter, manager);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act & Assert - Cancellation should propagate
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => exporter.ExportAsync(logs, cts.Token));
        }

        #endregion

        #region Per-Host Isolation Tests

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_DifferentHosts_IndependentCircuitBreakers()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 1 };
            var manager = new CircuitBreakerManager(config);

            var failingExporter = new MockTelemetryExporter { ShouldThrow = true };
            var successExporter = new MockTelemetryExporter { ShouldThrow = false };

            var host1 = "https://host1.databricks.com";
            var host2 = "https://host2.databricks.com";

            var exporter1 = new CircuitBreakerTelemetryExporter(host1, failingExporter, manager);
            var exporter2 = new CircuitBreakerTelemetryExporter(host2, successExporter, manager);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Act - Cause failure on host1 to open its circuit
            await exporter1.ExportAsync(logs);

            // Assert - Host1 circuit is open, Host2 circuit is still closed
            var cb1 = manager.GetCircuitBreaker(host1);
            var cb2 = manager.GetCircuitBreaker(host2);

            Assert.Equal(CircuitBreakerState.Open, cb1.State);
            Assert.Equal(CircuitBreakerState.Closed, cb2.State);

            // Reset call count
            successExporter.ExportCallCount = 0;

            // Act - Export on host2 should still work
            await exporter2.ExportAsync(logs);

            // Assert
            Assert.Equal(1, successExporter.ExportCallCount);
        }

        #endregion

        #region HalfOpen State Tests

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_HalfOpen_SuccessClosesCircuit()
        {
            // Arrange
            var config = new CircuitBreakerConfig
            {
                FailureThreshold = 1,
                Timeout = TimeSpan.FromMilliseconds(50),
                SuccessThreshold = 1
            };
            var manager = new CircuitBreakerManager(config);
            var mockExporter = new MockTelemetryExporter { ShouldThrow = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, mockExporter, manager);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Cause failure to open the circuit
            await exporter.ExportAsync(logs);

            var circuitBreaker = manager.GetCircuitBreaker(TestHost);
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Wait for timeout to allow transition to HalfOpen
            await Task.Delay(100);

            // Now succeed
            mockExporter.ShouldThrow = false;
            await exporter.ExportAsync(logs);

            // Assert - Circuit should be closed after success in HalfOpen
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_HalfOpen_FailureReopensCircuit()
        {
            // Arrange
            var config = new CircuitBreakerConfig
            {
                FailureThreshold = 1,
                Timeout = TimeSpan.FromMilliseconds(50),
                SuccessThreshold = 2
            };
            var manager = new CircuitBreakerManager(config);
            var mockExporter = new MockTelemetryExporter { ShouldThrow = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, mockExporter, manager);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Cause failure to open the circuit
            await exporter.ExportAsync(logs);

            var circuitBreaker = manager.GetCircuitBreaker(TestHost);
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Wait for timeout to allow transition to HalfOpen
            await Task.Delay(100);

            // Fail again - this will transition from HalfOpen to Open
            await exporter.ExportAsync(logs);

            // Assert - Circuit should be open again
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
        }

        #endregion

        #region Mock Telemetry Exporter

        /// <summary>
        /// Mock telemetry exporter for testing.
        /// </summary>
        private class MockTelemetryExporter : ITelemetryExporter
        {
            public int ExportCallCount { get; set; }
            public IReadOnlyList<TelemetryFrontendLog>? LastExportedLogs { get; private set; }
            public bool ShouldThrow { get; set; }
            public bool ShouldDelay { get; set; }

            public async Task ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                ct.ThrowIfCancellationRequested();

                if (ShouldDelay)
                {
                    await Task.Delay(1000, ct);
                }

                ExportCallCount++;
                LastExportedLogs = logs;

                if (ShouldThrow)
                {
                    throw new Exception("Simulated export failure");
                }
            }
        }

        #endregion
    }
}
