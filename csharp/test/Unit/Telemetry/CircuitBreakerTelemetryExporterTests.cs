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
    /// </summary>
    public class CircuitBreakerTelemetryExporterTests : IDisposable
    {
        private const string TestHost = "test-workspace.databricks.com";

        public CircuitBreakerTelemetryExporterTests()
        {
            // Reset the circuit breaker manager before each test for isolation
            CircuitBreakerManager.GetInstance().Reset();
        }

        public void Dispose()
        {
            // Clean up circuit breakers after each test
            CircuitBreakerManager.GetInstance().Reset();
        }

        #region Constructor Tests

        [Fact]
        public void Constructor_NullHost_ThrowsArgumentNullException()
        {
            // Arrange
            MockTelemetryExporter innerExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new CircuitBreakerTelemetryExporter(null!, innerExporter));
        }

        [Fact]
        public void Constructor_EmptyHost_ThrowsArgumentException()
        {
            // Arrange
            MockTelemetryExporter innerExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new CircuitBreakerTelemetryExporter("", innerExporter));
        }

        [Fact]
        public void Constructor_WhitespaceHost_ThrowsArgumentException()
        {
            // Arrange
            MockTelemetryExporter innerExporter = new MockTelemetryExporter();

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
            MockTelemetryExporter innerExporter = new MockTelemetryExporter();

            // Act
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);

            // Assert
            Assert.NotNull(exporter);
        }

        #endregion

        #region Circuit Closed - Successful Export Tests

        [Fact]
        public async Task CircuitClosed_ExportsMetrics_InnerExporterCalledAndReturnsTrue()
        {
            // Arrange
            MockTelemetryExporter innerExporter = new MockTelemetryExporter { ReturnValue = true };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);
            List<TelemetryFrontendLog> logs = CreateTestLogs(1);

            // Act
            bool result = await exporter.ExportAsync(logs);

            // Assert
            Assert.True(result, "ExportAsync should return true when circuit is closed and inner exporter succeeds");
            Assert.Equal(1, innerExporter.ExportCallCount);
        }

        [Fact]
        public async Task CircuitClosed_MultipleExports_AllDelegateToInnerExporter()
        {
            // Arrange
            MockTelemetryExporter innerExporter = new MockTelemetryExporter { ReturnValue = true };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(TestHost, innerExporter);

            // Act
            for (int i = 0; i < 5; i++)
            {
                List<TelemetryFrontendLog> logs = CreateTestLogs(1);
                bool result = await exporter.ExportAsync(logs);
                Assert.True(result);
            }

            // Assert
            Assert.Equal(5, innerExporter.ExportCallCount);
        }

        #endregion

        #region Circuit Open - Drop Events Tests

        [Fact]
        public async Task CircuitOpen_DropsMetrics_ReturnsFalseNoException()
        {
            // Arrange - Use a unique host to avoid interference
            string host = "open-test.databricks.com";
            MockTelemetryExporter innerExporter = new MockTelemetryExporter
            {
                ThrowException = new InvalidOperationException("Simulated failure")
            };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(host, innerExporter);

            // Trip the circuit breaker by causing failures
            // CircuitBreaker default: failureThreshold=5, Polly MinimumThroughput=max(2,5)=5
            for (int i = 0; i < 5; i++)
            {
                List<TelemetryFrontendLog> logs = CreateTestLogs(1);
                await exporter.ExportAsync(logs);
            }

            // Verify circuit is open
            CircuitBreaker circuitBreaker = CircuitBreakerManager.GetInstance().GetCircuitBreaker(host);
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Reset the call count to verify no new calls happen
            innerExporter.ExportCallCount = 0;

            // Act - Try to export when circuit is open
            List<TelemetryFrontendLog> newLogs = CreateTestLogs(3);
            bool result = await exporter.ExportAsync(newLogs);

            // Assert
            Assert.False(result, "ExportAsync should return false when circuit is open");
            Assert.Equal(0, innerExporter.ExportCallCount);
        }

        [Fact]
        public async Task CircuitOpen_NoExceptionPropagated()
        {
            // Arrange
            string host = "no-exception-test.databricks.com";
            MockTelemetryExporter innerExporter = new MockTelemetryExporter
            {
                ThrowException = new InvalidOperationException("Simulated failure")
            };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(host, innerExporter);

            // Trip the circuit breaker
            for (int i = 0; i < 5; i++)
            {
                await exporter.ExportAsync(CreateTestLogs(1));
            }

            // Act & Assert - Should not throw any exception
            Exception? caughtException = null;
            try
            {
                await exporter.ExportAsync(CreateTestLogs(1));
            }
            catch (Exception ex)
            {
                caughtException = ex;
            }

            Assert.Null(caughtException);
        }

        #endregion

        #region Inner Exporter Failure Tests

        [Fact]
        public async Task InnerExporterFails_CircuitBreakerTracksFailure()
        {
            // Arrange
            string host = "failure-tracking-test.databricks.com";
            MockTelemetryExporter innerExporter = new MockTelemetryExporter
            {
                ThrowException = new InvalidOperationException("Simulated export failure")
            };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(host, innerExporter);

            // Act - Cause enough failures to trip the circuit breaker
            for (int i = 0; i < 5; i++)
            {
                await exporter.ExportAsync(CreateTestLogs(1));
            }

            // Assert - Circuit breaker should have tracked the failures and transitioned to Open
            CircuitBreaker circuitBreaker = CircuitBreakerManager.GetInstance().GetCircuitBreaker(host);
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
        }

        [Fact]
        public async Task InnerExporterFails_NoExceptionPropagated()
        {
            // Arrange
            MockTelemetryExporter innerExporter = new MockTelemetryExporter
            {
                ThrowException = new InvalidOperationException("Simulated failure")
            };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(
                "no-propagation-test.databricks.com", innerExporter);

            // Act & Assert - No exception should propagate to caller
            Exception? caughtException = null;
            try
            {
                bool result = await exporter.ExportAsync(CreateTestLogs(1));
                Assert.False(result, "ExportAsync should return false when inner exporter throws");
            }
            catch (Exception ex)
            {
                caughtException = ex;
            }

            Assert.Null(caughtException);
        }

        [Fact]
        public async Task InnerExporterReturnsFalse_CircuitBreakerTracksFailure()
        {
            // Arrange - Inner exporter swallows exceptions and returns false
            string host = "returns-false-test.databricks.com";
            MockTelemetryExporter innerExporter = new MockTelemetryExporter { ReturnValue = false };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(host, innerExporter);

            // Act - Cause enough false returns to trip the circuit breaker
            for (int i = 0; i < 5; i++)
            {
                bool result = await exporter.ExportAsync(CreateTestLogs(1));
                Assert.False(result);
            }

            // Assert - Circuit breaker should have tracked the false returns as failures
            CircuitBreaker circuitBreaker = CircuitBreakerManager.GetInstance().GetCircuitBreaker(host);
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
        }

        [Fact]
        public async Task InnerExporterReturnsFalse_NoExceptionPropagated()
        {
            // Arrange
            MockTelemetryExporter innerExporter = new MockTelemetryExporter { ReturnValue = false };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(
                "false-return-no-exception.databricks.com", innerExporter);

            // Act & Assert
            Exception? caughtException = null;
            try
            {
                bool result = await exporter.ExportAsync(CreateTestLogs(1));
                Assert.False(result);
            }
            catch (Exception ex)
            {
                caughtException = ex;
            }

            Assert.Null(caughtException);
        }

        #endregion

        #region Null/Empty Logs Tests

        [Fact]
        public async Task NullLogs_DelegatesToInnerExporter_ReturnsTrue()
        {
            // Arrange
            MockTelemetryExporter innerExporter = new MockTelemetryExporter { ReturnValue = true };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(
                "null-logs-test.databricks.com", innerExporter);

            // Act
            bool result = await exporter.ExportAsync(null!, CancellationToken.None);

            // Assert
            Assert.True(result, "ExportAsync should return true for null logs (delegates to inner)");
            Assert.Equal(1, innerExporter.ExportCallCount);
        }

        [Fact]
        public async Task EmptyLogs_DelegatesToInnerExporter_ReturnsTrue()
        {
            // Arrange
            MockTelemetryExporter innerExporter = new MockTelemetryExporter { ReturnValue = true };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(
                "empty-logs-test.databricks.com", innerExporter);
            List<TelemetryFrontendLog> emptyLogs = new List<TelemetryFrontendLog>();

            // Act
            bool result = await exporter.ExportAsync(emptyLogs);

            // Assert
            Assert.True(result, "ExportAsync should return true for empty logs (delegates to inner)");
            Assert.Equal(1, innerExporter.ExportCallCount);
        }

        [Fact]
        public async Task NullLogs_BypassesCircuitBreaker()
        {
            // Arrange - Trip the circuit breaker first
            string host = "null-bypass-test.databricks.com";
            MockTelemetryExporter innerExporter = new MockTelemetryExporter
            {
                ThrowException = new InvalidOperationException("Simulated failure")
            };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(host, innerExporter);

            // Trip the circuit breaker
            for (int i = 0; i < 5; i++)
            {
                await exporter.ExportAsync(CreateTestLogs(1));
            }

            CircuitBreaker circuitBreaker = CircuitBreakerManager.GetInstance().GetCircuitBreaker(host);
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Now set inner exporter to succeed for null/empty
            innerExporter.ThrowException = null;
            innerExporter.ReturnValue = true;
            innerExporter.ExportCallCount = 0;

            // Act - null logs should bypass the circuit breaker
            bool result = await exporter.ExportAsync(null!, CancellationToken.None);

            // Assert
            Assert.True(result, "Null logs should bypass circuit breaker and delegate to inner exporter");
            Assert.Equal(1, innerExporter.ExportCallCount);
        }

        #endregion

        #region CancellationToken Tests

        [Fact]
        public async Task CancellationToken_PropagatedToInnerExporter()
        {
            // Arrange
            CancellationToken? capturedToken = null;
            MockTelemetryExporter innerExporter = new MockTelemetryExporter
            {
                ReturnValue = true,
                OnExportCalled = (logs, ct) => capturedToken = ct
            };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(
                "cancellation-test.databricks.com", innerExporter);

            using CancellationTokenSource cts = new CancellationTokenSource();
            CancellationToken expectedToken = cts.Token;

            // Act
            await exporter.ExportAsync(CreateTestLogs(1), expectedToken);

            // Assert
            Assert.NotNull(capturedToken);
            Assert.Equal(expectedToken, capturedToken.Value);
        }

        [Fact]
        public async Task CancelledToken_PropagatesOperationCanceledException()
        {
            // Arrange
            MockTelemetryExporter innerExporter = new MockTelemetryExporter
            {
                ThrowOnCancellation = true
            };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(
                "cancelled-token-test.databricks.com", innerExporter);

            using CancellationTokenSource cts = new CancellationTokenSource();
            cts.Cancel();

            // Act & Assert - OperationCanceledException should propagate
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => exporter.ExportAsync(CreateTestLogs(1), cts.Token));
        }

        #endregion

        #region Circuit Breaker Integration Tests

        [Fact]
        public async Task CircuitBreakerFromManager_UsesPerHostBreaker()
        {
            // Arrange
            string host1 = "host1.databricks.com";
            string host2 = "host2.databricks.com";

            MockTelemetryExporter failingExporter = new MockTelemetryExporter
            {
                ThrowException = new InvalidOperationException("Failure")
            };
            MockTelemetryExporter successExporter = new MockTelemetryExporter { ReturnValue = true };

            CircuitBreakerTelemetryExporter exporter1 = new CircuitBreakerTelemetryExporter(host1, failingExporter);
            CircuitBreakerTelemetryExporter exporter2 = new CircuitBreakerTelemetryExporter(host2, successExporter);

            // Trip the circuit breaker for host1
            for (int i = 0; i < 5; i++)
            {
                await exporter1.ExportAsync(CreateTestLogs(1));
            }

            // Assert - host1 circuit is open
            CircuitBreaker cb1 = CircuitBreakerManager.GetInstance().GetCircuitBreaker(host1);
            Assert.Equal(CircuitBreakerState.Open, cb1.State);

            // Assert - host2 circuit is still closed
            CircuitBreaker cb2 = CircuitBreakerManager.GetInstance().GetCircuitBreaker(host2);
            Assert.Equal(CircuitBreakerState.Closed, cb2.State);

            // host2 should still be able to export successfully
            bool result = await exporter2.ExportAsync(CreateTestLogs(1));
            Assert.True(result);
        }

        [Fact]
        public async Task CircuitBreakerRecovery_SuccessAfterHalfOpen_ReturnsTrue()
        {
            // Arrange - Use short timeout for testing
            string host = "recovery-test.databricks.com";

            // Create circuit breaker with short timeout manually first
            CircuitBreakerManager.GetInstance().GetCircuitBreaker(host, 2, TimeSpan.FromMilliseconds(500));

            int callCount = 0;
            MockTelemetryExporter innerExporter = new MockTelemetryExporter
            {
                OnExportCalled = (logs, ct) =>
                {
                    callCount++;
                    if (callCount <= 2)
                    {
                        throw new InvalidOperationException("Simulated failure");
                    }
                },
                ReturnValue = true
            };

            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(host, innerExporter);

            // Trip the circuit breaker
            for (int i = 0; i < 2; i++)
            {
                await exporter.ExportAsync(CreateTestLogs(1));
            }

            CircuitBreaker circuitBreaker = CircuitBreakerManager.GetInstance().GetCircuitBreaker(host);
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Wait for timeout to transition to HalfOpen
            await Task.Delay(600);

            // Act - Should succeed now
            bool result = await exporter.ExportAsync(CreateTestLogs(1));

            // Assert - Should have recovered
            Assert.True(result);
            Assert.True(
                circuitBreaker.State == CircuitBreakerState.HalfOpen ||
                circuitBreaker.State == CircuitBreakerState.Closed);
        }

        #endregion

        #region Edge Cases

        [Fact]
        public async Task InnerExporterThrowsVariousExceptions_AllSwallowed()
        {
            // Arrange
            string host = "various-exceptions-test.databricks.com";
            int callCount = 0;
            MockTelemetryExporter innerExporter = new MockTelemetryExporter
            {
                OnExportCalled = (logs, ct) =>
                {
                    callCount++;
                    switch (callCount)
                    {
                        case 1: throw new InvalidOperationException("Invalid op");
                        case 2: throw new TimeoutException("Timeout");
                        case 3: throw new ArgumentException("Bad arg");
                        default: throw new Exception("Generic error");
                    }
                }
            };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(host, innerExporter);

            // Act & Assert - No exceptions should propagate
            for (int i = 0; i < 4; i++)
            {
                Exception? caughtException = null;
                try
                {
                    bool result = await exporter.ExportAsync(CreateTestLogs(1));
                    Assert.False(result);
                }
                catch (Exception ex)
                {
                    caughtException = ex;
                }
                Assert.Null(caughtException);
            }
        }

        [Fact]
        public async Task LogsPassedToInnerExporter_AreCorrect()
        {
            // Arrange
            IReadOnlyList<TelemetryFrontendLog>? capturedLogs = null;
            MockTelemetryExporter innerExporter = new MockTelemetryExporter
            {
                ReturnValue = true,
                OnExportCalled = (logs, ct) => capturedLogs = logs
            };
            CircuitBreakerTelemetryExporter exporter = new CircuitBreakerTelemetryExporter(
                "logs-passthrough-test.databricks.com", innerExporter);

            List<TelemetryFrontendLog> expectedLogs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 111, FrontendLogEventId = "event-a" },
                new TelemetryFrontendLog { WorkspaceId = 222, FrontendLogEventId = "event-b" }
            };

            // Act
            await exporter.ExportAsync(expectedLogs);

            // Assert
            Assert.NotNull(capturedLogs);
            Assert.Equal(2, capturedLogs!.Count);
            Assert.Equal(111, capturedLogs[0].WorkspaceId);
            Assert.Equal("event-a", capturedLogs[0].FrontendLogEventId);
            Assert.Equal(222, capturedLogs[1].WorkspaceId);
            Assert.Equal("event-b", capturedLogs[1].FrontendLogEventId);
        }

        #endregion

        #region Helper Methods

        private static List<TelemetryFrontendLog> CreateTestLogs(int count)
        {
            List<TelemetryFrontendLog> logs = new List<TelemetryFrontendLog>(count);
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

        #region Mock Telemetry Exporter

        /// <summary>
        /// Mock implementation of <see cref="ITelemetryExporter"/> for testing.
        /// </summary>
        private class MockTelemetryExporter : ITelemetryExporter
        {
            /// <summary>
            /// The value to return from ExportAsync. Ignored if ThrowException is set.
            /// </summary>
            public bool ReturnValue { get; set; } = true;

            /// <summary>
            /// If set, ExportAsync will throw this exception.
            /// </summary>
            public Exception? ThrowException { get; set; }

            /// <summary>
            /// If true, ExportAsync will check cancellation token and throw OperationCanceledException.
            /// </summary>
            public bool ThrowOnCancellation { get; set; }

            /// <summary>
            /// Count of times ExportAsync has been called.
            /// </summary>
            public int ExportCallCount { get; set; }

            /// <summary>
            /// Optional callback invoked when ExportAsync is called, before processing.
            /// If it throws, the exception propagates (simulating inner exporter failure).
            /// </summary>
            public Action<IReadOnlyList<TelemetryFrontendLog>, CancellationToken>? OnExportCalled { get; set; }

            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                ExportCallCount++;

                if (ThrowOnCancellation)
                {
                    ct.ThrowIfCancellationRequested();
                }

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
