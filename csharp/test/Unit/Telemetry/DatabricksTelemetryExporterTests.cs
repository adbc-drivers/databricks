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
    /// Tests for DatabricksTelemetryExporter class.
    /// </summary>
    public class DatabricksTelemetryExporterTests
    {
        private const string TestHost = "test.databricks.com";

        [Fact]
        public void DatabricksTelemetryExporter_Constructor_NullHttpClient_ThrowsArgumentNullException()
        {
            // Arrange
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new DatabricksTelemetryExporter(null!, TestHost, true, config));
        }

        [Fact]
        public void DatabricksTelemetryExporter_Constructor_NullHost_ThrowsArgumentNullException()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new DatabricksTelemetryExporter(httpClient, null!, true, config));
        }

        [Fact]
        public void DatabricksTelemetryExporter_Constructor_NullConfig_ThrowsArgumentNullException()
        {
            // Arrange
            using var httpClient = new HttpClient();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new DatabricksTelemetryExporter(httpClient, TestHost, true, null!));
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_NullMetrics_DoesNotThrow()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            // Act & Assert - should not throw
            await exporter.ExportAsync(null!);
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_EmptyMetrics_DoesNotThrow()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);
            var emptyMetrics = new List<TelemetryMetric>();

            // Act & Assert - should not throw
            await exporter.ExportAsync(emptyMetrics);
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_Authenticated_UsesCorrectEndpoint()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, isAuthenticated: true, config);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric
                {
                    MetricType = "statement",
                    Timestamp = DateTimeOffset.UtcNow,
                    SessionId = "session-123"
                }
            };

            // Act
            await exporter.ExportAsync(metrics);

            // Assert
            Assert.NotNull(handler.LastRequest);
            Assert.Equal(HttpMethod.Post, handler.LastRequest!.Method);
            Assert.Equal($"https://{TestHost}/telemetry-ext", handler.LastRequest.RequestUri?.ToString());
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_Unauthenticated_UsesCorrectEndpoint()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, isAuthenticated: false, config);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric
                {
                    MetricType = "statement",
                    Timestamp = DateTimeOffset.UtcNow,
                    SessionId = "session-123"
                }
            };

            // Act
            await exporter.ExportAsync(metrics);

            // Assert
            Assert.NotNull(handler.LastRequest);
            Assert.Equal(HttpMethod.Post, handler.LastRequest!.Method);
            Assert.Equal($"https://{TestHost}/telemetry-unauth", handler.LastRequest.RequestUri?.ToString());
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_Success_ReturnsWithoutError()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "statement", Timestamp = DateTimeOffset.UtcNow }
            };

            // Act & Assert - should complete without exception
            await exporter.ExportAsync(metrics);
            Assert.Equal(1, handler.RequestCount);
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_TransientFailure_Retries()
        {
            // Arrange
            var handler = new TestHttpMessageHandler();
            handler.AddResponse(HttpStatusCode.ServiceUnavailable, "{}"); // First attempt fails
            handler.AddResponse(HttpStatusCode.OK, "{}"); // Second attempt succeeds
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration
            {
                MaxRetries = 3,
                RetryDelayMs = 10
            };
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "statement", Timestamp = DateTimeOffset.UtcNow }
            };

            // Act
            await exporter.ExportAsync(metrics);

            // Assert - should have made 2 requests (1 failure + 1 retry)
            Assert.Equal(2, handler.RequestCount);
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_MaxRetries_DoesNotThrow()
        {
            // Arrange
            var handler = new TestHttpMessageHandler();
            // All attempts fail with transient error
            for (int i = 0; i < 10; i++)
            {
                handler.AddResponse(HttpStatusCode.ServiceUnavailable, "{}");
            }
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration
            {
                MaxRetries = 3,
                RetryDelayMs = 10
            };
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "statement", Timestamp = DateTimeOffset.UtcNow }
            };

            // Act & Assert - should not throw despite all failures
            await exporter.ExportAsync(metrics);

            // Should have made MaxRetries + 1 attempts (initial + 3 retries = 4 total)
            Assert.Equal(4, handler.RequestCount);
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_NonTransientFailure_DoesNotRetry()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.BadRequest, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration
            {
                MaxRetries = 3,
                RetryDelayMs = 10
            };
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "statement", Timestamp = DateTimeOffset.UtcNow }
            };

            // Act
            await exporter.ExportAsync(metrics);

            // Assert - should have made only 1 request (no retries for 400 Bad Request)
            Assert.Equal(1, handler.RequestCount);
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_TransientStatusCodes_AreRetried()
        {
            // Test each transient status code
            var transientStatusCodes = new[]
            {
                HttpStatusCode.TooManyRequests,        // 429
                HttpStatusCode.InternalServerError,    // 500
                HttpStatusCode.BadGateway,             // 502
                HttpStatusCode.ServiceUnavailable,     // 503
                HttpStatusCode.GatewayTimeout          // 504
            };

            foreach (var statusCode in transientStatusCodes)
            {
                // Arrange
                var handler = new TestHttpMessageHandler();
                handler.AddResponse(statusCode, "{}");     // First attempt fails
                handler.AddResponse(HttpStatusCode.OK, "{}"); // Second attempt succeeds
                using var httpClient = new HttpClient(handler);
                var config = new TelemetryConfiguration
                {
                    MaxRetries = 3,
                    RetryDelayMs = 10
                };
                var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

                var metrics = new List<TelemetryMetric>
                {
                    new TelemetryMetric { MetricType = "statement", Timestamp = DateTimeOffset.UtcNow }
                };

                // Act
                await exporter.ExportAsync(metrics);

                // Assert - should have made 2 requests (failure + retry)
                Assert.Equal(2, handler.RequestCount);
            }
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_HttpException_DoesNotThrow()
        {
            // Arrange
            var handler = new TestHttpMessageHandler();
            handler.ThrowException = true;
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration { MaxRetries = 0 };
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "statement", Timestamp = DateTimeOffset.UtcNow }
            };

            // Act & Assert - should swallow exception
            await exporter.ExportAsync(metrics);
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_CancellationToken_HonorsCancellation()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric { MetricType = "statement", Timestamp = DateTimeOffset.UtcNow }
            };

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act & Assert - should handle cancellation gracefully without throwing
            await exporter.ExportAsync(metrics, cts.Token);
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_SerializesMetrics_AsJson()
        {
            // Arrange
            var handler = new TestHttpMessageHandler(HttpStatusCode.OK, "{}");
            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var metrics = new List<TelemetryMetric>
            {
                new TelemetryMetric
                {
                    MetricType = "statement",
                    Timestamp = DateTimeOffset.UtcNow,
                    SessionId = "session-123",
                    StatementId = "stmt-456"
                }
            };

            // Act
            await exporter.ExportAsync(metrics);

            // Assert
            Assert.NotNull(handler.LastRequestContent);
            var contentType = handler.LastRequest?.Content?.Headers.ContentType?.MediaType;
            Assert.Equal("application/json", contentType);

            // Verify JSON contains expected fields
            var json = handler.LastRequestContent;
            Assert.Contains("\"sessionId\":", json);
            Assert.Contains("\"statementId\":", json);
        }

        /// <summary>
        /// Test HTTP message handler that returns configurable responses.
        /// </summary>
        private class TestHttpMessageHandler : HttpMessageHandler
        {
            private readonly Queue<HttpResponseMessage> _responses = new();
            private readonly HttpStatusCode _defaultStatusCode;
            private readonly string _defaultContent;

            public bool ThrowException { get; set; }
            public HttpRequestMessage? LastRequest { get; private set; }
            public string? LastRequestContent { get; private set; }
            public int RequestCount { get; private set; }

            public TestHttpMessageHandler(HttpStatusCode defaultStatusCode = HttpStatusCode.OK, string defaultContent = "{}")
            {
                _defaultStatusCode = defaultStatusCode;
                _defaultContent = defaultContent;
            }

            public void AddResponse(HttpStatusCode statusCode, string content)
            {
                _responses.Enqueue(new HttpResponseMessage(statusCode)
                {
                    Content = new StringContent(content)
                });
            }

            protected override async Task<HttpResponseMessage> SendAsync(
                HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                RequestCount++;
                LastRequest = request;

                if (request.Content != null)
                {
                    LastRequestContent = await request.Content.ReadAsStringAsync(cancellationToken);
                }

                if (ThrowException)
                {
                    throw new HttpRequestException("Test exception");
                }

                if (_responses.Count > 0)
                {
                    return _responses.Dequeue();
                }

                return new HttpResponseMessage(_defaultStatusCode)
                {
                    Content = new StringContent(_defaultContent)
                };
            }
        }
    }
}
