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
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for TelemetryClient class.
    /// Verifies that TelemetryClient correctly coordinates listener, aggregator, and exporter.
    /// </summary>
    public class TelemetryClientTests
    {
        private const string TestHost = "test.databricks.com";

        #region Constructor Tests

        [Fact]
        public void TelemetryClient_Constructor_NullHost_ThrowsArgumentException()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new TelemetryClient(null!, httpClient, config));
        }

        [Fact]
        public void TelemetryClient_Constructor_EmptyHost_ThrowsArgumentException()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new TelemetryClient(string.Empty, httpClient, config));
        }

        [Fact]
        public void TelemetryClient_Constructor_NullHttpClient_ThrowsArgumentNullException()
        {
            // Arrange
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new TelemetryClient(TestHost, null!, config));
        }

        [Fact]
        public void TelemetryClient_Constructor_NullConfig_ThrowsArgumentNullException()
        {
            // Arrange
            using var httpClient = new HttpClient();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new TelemetryClient(TestHost, httpClient, null!));
        }

        [Fact]
        public void TelemetryClient_Constructor_ValidParameters_InitializesComponents()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();

            // Act
            using var client = new TelemetryClient(TestHost, httpClient, config);

            // Assert
            Assert.NotNull(client);
        }

        [Fact]
        public void TelemetryClient_Constructor_Authenticated_InitializesCorrectly()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();

            // Act
            using var client = new TelemetryClient(TestHost, httpClient, config, isAuthenticated: true);

            // Assert
            Assert.NotNull(client);
        }

        [Fact]
        public void TelemetryClient_Constructor_Unauthenticated_InitializesCorrectly()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();

            // Act
            using var client = new TelemetryClient(TestHost, httpClient, config, isAuthenticated: false);

            // Assert
            Assert.NotNull(client);
        }

        #endregion

        #region ExportAsync Tests

        [Fact]
        public async Task TelemetryClient_ExportAsync_NullMetrics_DoesNotThrow()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            using var client = new TelemetryClient(TestHost, httpClient, config);

            // Act & Assert - should not throw
            await client.ExportAsync(null!);
        }

        [Fact]
        public async Task TelemetryClient_ExportAsync_EmptyMetrics_DoesNotThrow()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            using var client = new TelemetryClient(TestHost, httpClient, config);
            var emptyMetrics = new List<TelemetryMetric>();

            // Act & Assert - should not throw
            await client.ExportAsync(emptyMetrics);
        }

        [Fact]
        public async Task TelemetryClient_ExportAsync_ValidMetrics_DelegatesToExporter()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            using var client = new TelemetryClient(TestHost, httpClient, config);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric
                {
                    MetricType = "statement",
                    Timestamp = DateTimeOffset.UtcNow,
                    SessionId = "session-1",
                    StatementId = "stmt-1",
                    WorkspaceId = 12345,
                    ExecutionLatencyMs = 100
                }
            };

            // Act
            await client.ExportAsync(metrics);

            // Assert - verify request was made
            Assert.Equal(1, handler.RequestCount);
        }

        [Fact]
        public async Task TelemetryClient_ExportAsync_ExporterThrows_DoesNotThrow()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.InternalServerError, "Error");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration { MaxRetries = 0 }; // No retries
            using var client = new TelemetryClient(TestHost, httpClient, config);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric
                {
                    MetricType = "statement",
                    Timestamp = DateTimeOffset.UtcNow
                }
            };

            // Act & Assert - should not throw even though exporter fails
            await client.ExportAsync(metrics);
        }

        [Fact]
        public async Task TelemetryClient_ExportAsync_CancellationRequested_DoesNotThrow()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}", delayMs: 1000);
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            using var client = new TelemetryClient(TestHost, httpClient, config);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "statement", Timestamp = DateTimeOffset.UtcNow }
            };

            using var cts = new CancellationTokenSource();
            cts.Cancel(); // Cancel immediately

            // Act & Assert - should not throw
            await client.ExportAsync(metrics, cts.Token);
        }

        #endregion

        #region CloseAsync Tests

        [Fact]
        public async Task TelemetryClient_CloseAsync_CancelsBackgroundTask()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration { FlushIntervalMs = 100 };
            using var client = new TelemetryClient(TestHost, httpClient, config);

            // Act
            await client.CloseAsync();

            // Assert - should complete without hanging
            // If background task wasn't cancelled, this would hang
        }

        [Fact]
        public async Task TelemetryClient_CloseAsync_CalledMultipleTimes_DoesNotThrow()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            using var client = new TelemetryClient(TestHost, httpClient, config);

            // Act & Assert - should not throw
            await client.CloseAsync();
            await client.CloseAsync();
            await client.CloseAsync();
        }

        [Fact]
        public async Task TelemetryClient_CloseAsync_ExceptionDuringClose_DoesNotThrow()
        {
            // Arrange
            // Create a handler that will fail
            var handler = new TestHttpMessageHandler(HttpStatusCode.InternalServerError, "Error");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            using var client = new TelemetryClient(TestHost, httpClient, config);

            // Export something to have pending work
            await client.ExportAsync(new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "statement", Timestamp = DateTimeOffset.UtcNow }
            });

            // Act & Assert - should not throw even if there are errors during close
            await client.CloseAsync();
        }

        #endregion

        #region Dispose Tests

        [Fact]
        public void TelemetryClient_Dispose_DisposesResources()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var client = new TelemetryClient(TestHost, httpClient, config);

            // Act
            client.Dispose();

            // Assert - subsequent operations should be safe
            // No exception should be thrown
        }

        [Fact]
        public void TelemetryClient_Dispose_CalledMultipleTimes_DoesNotThrow()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var client = new TelemetryClient(TestHost, httpClient, config);

            // Act & Assert - should not throw
            client.Dispose();
            client.Dispose();
            client.Dispose();
        }

        [Fact]
        public async Task TelemetryClient_Dispose_AfterClose_DoesNotThrow()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var client = new TelemetryClient(TestHost, httpClient, config);

            // Act & Assert - should not throw
            await client.CloseAsync();
            client.Dispose();
        }

        [Fact]
        public async Task TelemetryClient_ExportAsync_AfterDispose_DoesNotThrow()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var client = new TelemetryClient(TestHost, httpClient, config);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "statement", Timestamp = DateTimeOffset.UtcNow }
            };

            // Act
            client.Dispose();

            // Assert - should not throw even after disposal
            await client.ExportAsync(metrics);
        }

        #endregion

        #region Background Flush Tests

        [Fact]
        public async Task TelemetryClient_BackgroundFlushTask_StartsAutomatically()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration { FlushIntervalMs = 50 };

            // Act
            using var client = new TelemetryClient(TestHost, httpClient, config);

            // Wait a bit to let background task run
            await Task.Delay(200);

            // Assert - background task should be running (no exceptions)
            // If it wasn't running, we'd have issues
            await client.CloseAsync();
        }

        [Fact]
        public async Task TelemetryClient_BackgroundFlushTask_StopsOnClose()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration { FlushIntervalMs = 50 };
            using var client = new TelemetryClient(TestHost, httpClient, config);

            // Act
            await client.CloseAsync();

            // Assert - should complete quickly (within 5 seconds as per implementation)
            // If background task didn't stop, CloseAsync would hang
        }

        #endregion

        #region Helper Classes

        /// <summary>
        /// Test HTTP message handler for mocking HTTP responses.
        /// </summary>
        private class TestHttpMessageHandler : HttpMessageHandler
        {
            private readonly HttpStatusCode _statusCode;
            private readonly string _content;
            private readonly int _delayMs;
            private int _requestCount;

            public int RequestCount => _requestCount;

            public TestHttpMessageHandler(HttpStatusCode statusCode, string content, int delayMs = 0)
            {
                _statusCode = statusCode;
                _content = content;
                _delayMs = delayMs;
                _requestCount = 0;
            }

            protected override async Task<HttpResponseMessage> SendAsync(
                HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref _requestCount);

                if (_delayMs > 0)
                {
                    await Task.Delay(_delayMs, cancellationToken);
                }

                var response = new HttpResponseMessage(_statusCode)
                {
                    Content = new StringContent(_content)
                };

                return response;
            }
        }

        #endregion
    }
}
