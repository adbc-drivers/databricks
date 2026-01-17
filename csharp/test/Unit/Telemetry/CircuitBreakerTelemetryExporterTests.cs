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
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for CircuitBreakerTelemetryExporter class.
    /// </summary>
    public class CircuitBreakerTelemetryExporterTests
    {
        private const string TestHost = "test.databricks.com";

        /// <summary>
        /// Mock telemetry exporter for testing.
        /// </summary>
        private class MockTelemetryExporter : ITelemetryExporter
        {
            public int CallCount { get; private set; }
            public Exception? ExceptionToThrow { get; set; }
            public List<IReadOnlyList<TelemetryMetric>> ReceivedMetrics { get; } = new List<IReadOnlyList<TelemetryMetric>>();

            public void ResetCallCount() => CallCount = 0;

            public Task ExportAsync(IReadOnlyList<TelemetryMetric> metrics, CancellationToken ct = default)
            {
                CallCount++;
                ReceivedMetrics.Add(metrics);

                if (ExceptionToThrow != null)
                {
                    throw ExceptionToThrow;
                }

                return Task.CompletedTask;
            }
        }

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_NullHost_ThrowsArgumentException()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new CircuitBreakerTelemetryExporter(null!, mockExporter));
        }

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_EmptyHost_ThrowsArgumentException()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new CircuitBreakerTelemetryExporter(string.Empty, mockExporter));
        }

        [Fact]
        public void CircuitBreakerTelemetryExporter_Constructor_NullInnerExporter_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new CircuitBreakerTelemetryExporter(TestHost, null!));
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_ExportAsync_NullMetrics_DoesNotThrow()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var wrapper = new CircuitBreakerTelemetryExporter(TestHost, mockExporter);

            // Act & Assert - should not throw
            await wrapper.ExportAsync(null!);

            // Verify inner exporter was not called
            Assert.Equal(0, mockExporter.CallCount);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_ExportAsync_EmptyMetrics_DoesNotThrow()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var wrapper = new CircuitBreakerTelemetryExporter(TestHost, mockExporter);
            var emptyMetrics = new List<TelemetryMetric>();

            // Act & Assert - should not throw
            await wrapper.ExportAsync(emptyMetrics);

            // Verify inner exporter was not called
            Assert.Equal(0, mockExporter.CallCount);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_CircuitClosed_ExportsMetrics()
        {
            // Arrange
            var mockExporter = new MockTelemetryExporter();
            var wrapper = new CircuitBreakerTelemetryExporter(TestHost, mockExporter);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "test" }
            };

            // Act
            await wrapper.ExportAsync(metrics);

            // Assert
            Assert.Equal(1, mockExporter.CallCount);
            Assert.Single(mockExporter.ReceivedMetrics);
            Assert.Equal(metrics, mockExporter.ReceivedMetrics[0]);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_CircuitOpen_DropsMetrics()
        {
            // Arrange
            // Clear any existing circuit breaker for this host
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(TestHost);

            var mockExporter = new MockTelemetryExporter();
            mockExporter.ExceptionToThrow = new Exception("Simulated failure");

            var wrapper = new CircuitBreakerTelemetryExporter(TestHost, mockExporter);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "test" }
            };

            // Act - Trigger failures to open circuit breaker
            var circuitBreaker = CircuitBreakerManager.GetInstance().GetCircuitBreaker(TestHost);
            var failureThreshold = circuitBreaker.Config.FailureThreshold;

            for (int i = 0; i < failureThreshold; i++)
            {
                await wrapper.ExportAsync(metrics);
            }

            // Circuit should now be open
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Reset mock call count
            mockExporter.ResetCallCount();

            // Act - Try to export when circuit is open
            await wrapper.ExportAsync(metrics);

            // Assert - Inner exporter should not be called (circuit is open)
            Assert.Equal(0, mockExporter.CallCount);

            // Cleanup
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(TestHost);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_InnerExporterFails_CircuitBreakerTracksFailure()
        {
            // Arrange
            // Clear any existing circuit breaker for this host
            var testHost = "test-failure-tracking.databricks.com";
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(testHost);

            var mockExporter = new MockTelemetryExporter();
            mockExporter.ExceptionToThrow = new Exception("Simulated failure");

            var wrapper = new CircuitBreakerTelemetryExporter(testHost, mockExporter);
            var circuitBreaker = CircuitBreakerManager.GetInstance().GetCircuitBreaker(testHost);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "test" }
            };

            // Verify circuit starts closed
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);

            // Act - Trigger one failure
            await wrapper.ExportAsync(metrics);

            // Assert - Circuit breaker saw the failure (still closed, but tracking)
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);

            // Act - Trigger more failures to open the circuit
            var failureThreshold = circuitBreaker.Config.FailureThreshold;
            for (int i = 1; i < failureThreshold; i++)
            {
                await wrapper.ExportAsync(metrics);
            }

            // Assert - Circuit breaker opened after threshold failures
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Cleanup
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(testHost);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_ExceptionSwallowed_DoesNotThrow()
        {
            // Arrange
            var testHost = "test-exception-swallowing.databricks.com";
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(testHost);

            var mockExporter = new MockTelemetryExporter();
            mockExporter.ExceptionToThrow = new InvalidOperationException("Simulated error");

            var wrapper = new CircuitBreakerTelemetryExporter(testHost, mockExporter);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "test" }
            };

            // Act & Assert - should not throw even though inner exporter throws
            await wrapper.ExportAsync(metrics);

            // Verify inner exporter was called
            Assert.Equal(1, mockExporter.CallCount);

            // Cleanup
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(testHost);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_SuccessfulExport_CircuitStaysClosed()
        {
            // Arrange
            var testHost = "test-success.databricks.com";
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(testHost);

            var mockExporter = new MockTelemetryExporter();
            var wrapper = new CircuitBreakerTelemetryExporter(testHost, mockExporter);
            var circuitBreaker = CircuitBreakerManager.GetInstance().GetCircuitBreaker(testHost);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "test" }
            };

            // Act - Perform multiple successful exports
            for (int i = 0; i < 10; i++)
            {
                await wrapper.ExportAsync(metrics);
            }

            // Assert
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
            Assert.Equal(10, mockExporter.CallCount);

            // Cleanup
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(testHost);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_MultipleMetrics_AllForwardedToInner()
        {
            // Arrange
            var testHost = "test-multiple-metrics.databricks.com";
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(testHost);

            var mockExporter = new MockTelemetryExporter();
            var wrapper = new CircuitBreakerTelemetryExporter(testHost, mockExporter);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "connection" },
                new TelemetryMetric { MetricType = "statement" },
                new TelemetryMetric { MetricType = "error" }
            };

            // Act
            await wrapper.ExportAsync(metrics);

            // Assert
            Assert.Equal(1, mockExporter.CallCount);
            Assert.Single(mockExporter.ReceivedMetrics);
            Assert.Equal(3, mockExporter.ReceivedMetrics[0].Count);

            // Cleanup
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(testHost);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_CancellationToken_PassedToInnerExporter()
        {
            // Arrange
            var testHost = "test-cancellation.databricks.com";
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(testHost);

            var mockExporter = new MockTelemetryExporter();
            var wrapper = new CircuitBreakerTelemetryExporter(testHost, mockExporter);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "test" }
            };

            var cts = new CancellationTokenSource();
            cts.Cancel(); // Cancel immediately

            // Set mock to check for cancellation
            mockExporter.ExceptionToThrow = new OperationCanceledException();

            // Act & Assert - should not throw
            await wrapper.ExportAsync(metrics, cts.Token);

            // Cleanup
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(testHost);
        }

        [Fact]
        public async Task CircuitBreakerTelemetryExporter_PerHost_IndependentCircuitBreakers()
        {
            // Arrange
            var host1 = "host1.databricks.com";
            var host2 = "host2.databricks.com";
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(host1);
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(host2);

            var mockExporter1 = new MockTelemetryExporter();
            mockExporter1.ExceptionToThrow = new Exception("Host 1 failure");

            var mockExporter2 = new MockTelemetryExporter();

            var wrapper1 = new CircuitBreakerTelemetryExporter(host1, mockExporter1);
            var wrapper2 = new CircuitBreakerTelemetryExporter(host2, mockExporter2);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "test" }
            };

            // Act - Open circuit for host1
            var circuitBreaker1 = CircuitBreakerManager.GetInstance().GetCircuitBreaker(host1);
            var failureThreshold = circuitBreaker1.Config.FailureThreshold;

            for (int i = 0; i < failureThreshold; i++)
            {
                await wrapper1.ExportAsync(metrics);
            }

            // Assert - Host1 circuit is open, host2 circuit is closed
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker1.State);

            var circuitBreaker2 = CircuitBreakerManager.GetInstance().GetCircuitBreaker(host2);
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker2.State);

            // Act - Host2 should still work
            await wrapper2.ExportAsync(metrics);

            // Assert
            Assert.Equal(1, mockExporter2.CallCount);

            // Cleanup
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(host1);
            CircuitBreakerManager.GetInstance().RemoveCircuitBreaker(host2);
        }
    }
}
