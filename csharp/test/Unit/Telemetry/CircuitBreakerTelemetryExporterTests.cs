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
    /// Tests for <see cref="CircuitBreakerTelemetryExporter"/>.
    /// Uses the "CircuitBreakerTests" collection to avoid parallel execution
    /// with other tests that call <see cref="CircuitBreakerManager.Reset"/>.
    /// </summary>
    [Collection("CircuitBreakerTests")]
    public class CircuitBreakerTelemetryExporterTests : IDisposable
    {
        private const string TestHost = "cb-exporter-test-host.databricks.com";

        public CircuitBreakerTelemetryExporterTests()
        {
            // Reset the circuit breaker manager before each test to ensure isolation
            CircuitBreakerManager.GetInstance().Reset();
        }

        public void Dispose()
        {
            CircuitBreakerManager.GetInstance().Reset();
        }

        #region Constructor Tests

        [Fact]
        public void Constructor_NullHost_ThrowsArgumentException()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new CircuitBreakerTelemetryExporter(null!, innerExporter));
        }

        [Fact]
        public void Constructor_EmptyHost_ThrowsArgumentException()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new CircuitBreakerTelemetryExporter("", innerExporter));
        }

        [Fact]
        public void Constructor_WhitespaceHost_ThrowsArgumentException()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new CircuitBreakerTelemetryExporter("   ", innerExporter));
        }

        [Fact]
        public void Constructor_NullInnerExporter_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new CircuitBreakerTelemetryExporter(TestHost, null!));
        }

        [Fact]
        public void Constructor_ValidParameters_CreatesInstance()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter();

            // Act
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);

            // Assert - no exception thrown, instance is created
            Assert.NotNull(exporter);
        }

        #endregion

        #region Circuit Closed - Delegation Tests

        [Fact]
        public async Task CircuitClosed_ExportsMetrics()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter { ResultToReturn = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);
            var logs = CreateTestLogs(1);

            // Act
            var result = await exporter.ExportAsync(logs);

            // Assert
            Assert.True(result);
            Assert.Equal(1, innerExporter.ExportCallCount);
        }

        [Fact]
        public async Task InnerExporterReturnsTrue_ReturnsTrue()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter { ResultToReturn = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);
            var logs = CreateTestLogs(1);

            // Act
            var result = await exporter.ExportAsync(logs);

            // Assert
            Assert.True(result);
            Assert.Equal(1, innerExporter.ExportCallCount);
        }

        [Fact]
        public async Task InnerExporterReturnsFalse_ReturnsFalse()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter { ResultToReturn = false };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);
            var logs = CreateTestLogs(1);

            // Act
            var result = await exporter.ExportAsync(logs);

            // Assert
            Assert.False(result);
            Assert.Equal(1, innerExporter.ExportCallCount);
        }

        [Fact]
        public async Task CircuitClosed_MultipleCalls_AllDelegated()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter { ResultToReturn = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);
            var logs = CreateTestLogs(1);

            // Act
            for (int i = 0; i < 5; i++)
            {
                await exporter.ExportAsync(logs);
            }

            // Assert
            Assert.Equal(5, innerExporter.ExportCallCount);
        }

        [Fact]
        public async Task CircuitClosed_PassesLogsToInnerExporter()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter { ResultToReturn = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);
            var logs = CreateTestLogs(3);

            // Act
            await exporter.ExportAsync(logs);

            // Assert
            Assert.Same(logs, innerExporter.LastReceivedLogs);
        }

        #endregion

        #region Circuit Open - Drop Events Tests

        [Fact]
        public async Task CircuitOpen_DropsMetrics()
        {
            // Arrange
            var failingExporter = new MockTelemetryExporter
            {
                ExceptionToThrow = new InvalidOperationException("Simulated failure")
            };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, failingExporter);
            var logs = CreateTestLogs(1);

            // Force the circuit to open by causing enough failures
            // CircuitBreaker default: failureThreshold=5, MinimumThroughput=max(2,5)=5
            for (int i = 0; i < 5; i++)
            {
                await exporter.ExportAsync(logs);
            }

            // Reset the inner exporter call count to track new calls
            var callCountBeforeOpenTest = failingExporter.ExportCallCount;

            // Act - Try to export when circuit is open
            var result = await exporter.ExportAsync(logs);

            // Assert - Should return false (dropped), no exception, inner exporter NOT called
            Assert.False(result);
            Assert.Equal(callCountBeforeOpenTest, failingExporter.ExportCallCount);
        }

        [Fact]
        public async Task CircuitOpen_DoesNotThrow()
        {
            // Arrange
            var failingExporter = new MockTelemetryExporter
            {
                ExceptionToThrow = new InvalidOperationException("Simulated failure")
            };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, failingExporter);
            var logs = CreateTestLogs(1);

            // Force the circuit open
            for (int i = 0; i < 5; i++)
            {
                await exporter.ExportAsync(logs);
            }

            // Act & Assert - Should not throw, just return false
            var exception = await Record.ExceptionAsync(() => exporter.ExportAsync(logs));
            Assert.Null(exception);
        }

        #endregion

        #region Failure Tracking Tests

        [Fact]
        public async Task InnerExporterFails_CircuitBreakerTracksFailure()
        {
            // Arrange
            var failingExporter = new MockTelemetryExporter
            {
                ExceptionToThrow = new InvalidOperationException("Simulated failure")
            };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, failingExporter);
            var logs = CreateTestLogs(1);

            // Act - Cause enough failures to trip the circuit breaker (default threshold = 5)
            for (int i = 0; i < 5; i++)
            {
                var result = await exporter.ExportAsync(logs);
                Assert.False(result); // Each failure returns false
            }

            // Record call count after failures
            var callCountAfterTripping = failingExporter.ExportCallCount;
            Assert.Equal(5, callCountAfterTripping);

            // Assert - Verify circuit is open by checking that the next call
            // does NOT invoke the inner exporter (behavioral verification)
            await exporter.ExportAsync(logs);
            Assert.Equal(callCountAfterTripping, failingExporter.ExportCallCount);
        }

        [Fact]
        public async Task InnerExporterFails_ExceptionSwallowed_ReturnsFalse()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter
            {
                ExceptionToThrow = new InvalidOperationException("Something went wrong")
            };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);
            var logs = CreateTestLogs(1);

            // Act
            var result = await exporter.ExportAsync(logs);

            // Assert - Exception is swallowed, returns false
            Assert.False(result);
            Assert.Equal(1, innerExporter.ExportCallCount);
        }

        [Fact]
        public async Task InnerExporterFails_BelowThreshold_CircuitStaysClosed()
        {
            // Arrange
            var failingExporter = new MockTelemetryExporter
            {
                ExceptionToThrow = new InvalidOperationException("Simulated failure")
            };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, failingExporter);
            var logs = CreateTestLogs(1);

            // Act - Cause fewer failures than the threshold (default is 5)
            for (int i = 0; i < 3; i++)
            {
                await exporter.ExportAsync(logs);
            }

            // Verify circuit is still closed by switching to a success and checking it's called
            failingExporter.ExceptionToThrow = null;
            failingExporter.ResultToReturn = true;
            var callCountBefore = failingExporter.ExportCallCount;

            var result = await exporter.ExportAsync(logs);

            // Assert - Inner exporter was still called (circuit not open)
            Assert.True(result);
            Assert.Equal(callCountBefore + 1, failingExporter.ExportCallCount);
        }

        #endregion

        #region Null/Empty Input Tests

        [Fact]
        public async Task NullLogs_HandledGracefully()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter { ResultToReturn = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);

            // Act
            var result = await exporter.ExportAsync(null!);

            // Assert - Returns true for null (nothing to export), inner exporter NOT called
            Assert.True(result);
            Assert.Equal(0, innerExporter.ExportCallCount);
        }

        [Fact]
        public async Task EmptyLogs_HandledGracefully()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter { ResultToReturn = true };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);

            // Act
            var result = await exporter.ExportAsync(new List<TelemetryFrontendLog>());

            // Assert - Returns true for empty list (nothing to export), inner exporter NOT called
            Assert.True(result);
            Assert.Equal(0, innerExporter.ExportCallCount);
        }

        #endregion

        #region Exception Swallowing Tests

        [Fact]
        public async Task GenericException_SwallowedInternally_ReturnsFalse()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter
            {
                ExceptionToThrow = new Exception("Generic error")
            };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);
            var logs = CreateTestLogs(1);

            // Act
            var exception = await Record.ExceptionAsync(() => exporter.ExportAsync(logs));

            // Assert - No exception propagated
            Assert.Null(exception);
        }

        [Fact]
        public async Task HttpException_SwallowedInternally_ReturnsFalse()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter
            {
                ExceptionToThrow = new System.Net.Http.HttpRequestException("Network error")
            };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);
            var logs = CreateTestLogs(1);

            // Act
            var result = await exporter.ExportAsync(logs);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public async Task TimeoutException_SwallowedInternally_ReturnsFalse()
        {
            // Arrange
            var innerExporter = new MockTelemetryExporter
            {
                ExceptionToThrow = new TimeoutException("Request timed out")
            };
            var exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);
            var logs = CreateTestLogs(1);

            // Act
            var result = await exporter.ExportAsync(logs);

            // Assert
            Assert.False(result);
        }

        #endregion

        #region Per-Host Circuit Breaker Isolation Tests

        [Fact]
        public async Task DifferentHosts_HaveIndependentCircuitBreakers()
        {
            // Arrange
            var host1 = "cb-exporter-host1.databricks.com";
            var host2 = "cb-exporter-host2.databricks.com";

            var failingExporter = new MockTelemetryExporter
            {
                ExceptionToThrow = new InvalidOperationException("Simulated failure")
            };
            var successExporter = new MockTelemetryExporter { ResultToReturn = true };

            var exporter1 = new CircuitBreakerTelemetryExporter(host1, failingExporter);
            var exporter2 = new CircuitBreakerTelemetryExporter(host2, successExporter);
            var logs = CreateTestLogs(1);

            // Act - Trip the circuit breaker for host1
            for (int i = 0; i < 5; i++)
            {
                await exporter1.ExportAsync(logs);
            }

            // Assert - host1 is open (inner exporter not called after tripping)
            var callCountAfterTripping = failingExporter.ExportCallCount;
            await exporter1.ExportAsync(logs);
            Assert.Equal(callCountAfterTripping, failingExporter.ExportCallCount);

            // host2 exporter should still work
            var result = await exporter2.ExportAsync(logs);
            Assert.True(result);
        }

        #endregion

        #region Helper Methods

        private static IReadOnlyList<TelemetryFrontendLog> CreateTestLogs(int count)
        {
            var logs = new List<TelemetryFrontendLog>();
            for (int i = 0; i < count; i++)
            {
                logs.Add(new TelemetryFrontendLog
                {
                    WorkspaceId = 12345,
                    FrontendLogEventId = $"test-event-{i}"
                });
            }
            return logs;
        }

        #endregion

        #region Mock ITelemetryExporter

        /// <summary>
        /// Manual mock implementation of <see cref="ITelemetryExporter"/> for testing.
        /// No mocking framework used, following existing test patterns in the codebase.
        /// </summary>
        private class MockTelemetryExporter : ITelemetryExporter
        {
            /// <summary>
            /// The result to return from ExportAsync. Default is true.
            /// </summary>
            public bool ResultToReturn { get; set; } = true;

            /// <summary>
            /// If set, ExportAsync will throw this exception instead of returning a result.
            /// </summary>
            public Exception? ExceptionToThrow { get; set; }

            /// <summary>
            /// Tracks how many times ExportAsync was called.
            /// </summary>
            public int ExportCallCount { get; set; }

            /// <summary>
            /// The logs received in the last call to ExportAsync.
            /// </summary>
            public IReadOnlyList<TelemetryFrontendLog>? LastReceivedLogs { get; private set; }

            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                ExportCallCount++;
                LastReceivedLogs = logs;

                if (ExceptionToThrow != null)
                {
                    throw ExceptionToThrow;
                }

                return Task.FromResult(ResultToReturn);
            }
        }

        #endregion
    }
}
