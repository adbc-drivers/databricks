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

        #region Circuit Breaker Tracks Failure Tests

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

            public async Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
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

                return true;
            }
        }

        #endregion
    }
}
