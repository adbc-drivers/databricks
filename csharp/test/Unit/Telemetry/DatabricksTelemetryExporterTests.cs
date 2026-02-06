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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for DatabricksTelemetryExporter class.
    /// </summary>
    public class DatabricksTelemetryExporterTests
    {
        private const string TestHost = "https://test-workspace.databricks.com";

        #region Constructor Tests

        [Fact]
        public void DatabricksTelemetryExporter_Constructor_NullHttpClient_ThrowsException()
        {
            // Arrange
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new DatabricksTelemetryExporter(null!, TestHost, true, config));
        }

        [Fact]
        public void DatabricksTelemetryExporter_Constructor_NullHost_ThrowsException()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new DatabricksTelemetryExporter(httpClient, null!, true, config));
        }

        [Fact]
        public void DatabricksTelemetryExporter_Constructor_EmptyHost_ThrowsException()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new DatabricksTelemetryExporter(httpClient, "", true, config));
        }

        [Fact]
        public void DatabricksTelemetryExporter_Constructor_WhitespaceHost_ThrowsException()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new DatabricksTelemetryExporter(httpClient, "   ", true, config));
        }

        [Fact]
        public void DatabricksTelemetryExporter_Constructor_NullConfig_ThrowsException()
        {
            // Arrange
            using var httpClient = new HttpClient();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new DatabricksTelemetryExporter(httpClient, TestHost, true, null!));
        }

        [Fact]
        public void DatabricksTelemetryExporter_Constructor_ValidParameters_SetsProperties()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();

            // Act
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            // Assert
            Assert.Equal(TestHost, exporter.Host);
            Assert.True(exporter.IsAuthenticated);
        }

        #endregion

        #region Endpoint Tests

        [Fact]
        public void DatabricksTelemetryExporter_ExportAsync_Authenticated_UsesCorrectEndpoint()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            // Act
            var endpointUrl = exporter.GetEndpointUrl();

            // Assert
            Assert.Equal($"{TestHost}/telemetry-ext", endpointUrl);
        }

        [Fact]
        public void DatabricksTelemetryExporter_ExportAsync_Unauthenticated_UsesCorrectEndpoint()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, false, config);

            // Act
            var endpointUrl = exporter.GetEndpointUrl();

            // Assert
            Assert.Equal($"{TestHost}/telemetry-unauth", endpointUrl);
        }

        [Fact]
        public void DatabricksTelemetryExporter_GetEndpointUrl_HostWithTrailingSlash_HandlesCorrectly()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, $"{TestHost}/", true, config);

            // Act
            var endpointUrl = exporter.GetEndpointUrl();

            // Assert
            Assert.Equal($"{TestHost}/telemetry-ext", endpointUrl);
        }

        #endregion

        #region TelemetryRequest Creation Tests

        [Fact]
        public void DatabricksTelemetryExporter_CreateTelemetryRequest_SingleLog_CreatesCorrectFormat()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog
                {
                    WorkspaceId = 12345,
                    FrontendLogEventId = "test-event-id"
                }
            };

            // Act
            var request = exporter.CreateTelemetryRequest(logs);

            // Assert
            Assert.True(request.UploadTime > 0);
            Assert.Single(request.ProtoLogs);
            Assert.Contains("12345", request.ProtoLogs[0]);
            Assert.Contains("test-event-id", request.ProtoLogs[0]);
        }

        [Fact]
        public void DatabricksTelemetryExporter_CreateTelemetryRequest_MultipleLogs_CreatesCorrectFormat()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 1, FrontendLogEventId = "event-1" },
                new TelemetryFrontendLog { WorkspaceId = 2, FrontendLogEventId = "event-2" },
                new TelemetryFrontendLog { WorkspaceId = 3, FrontendLogEventId = "event-3" }
            };

            // Act
            var request = exporter.CreateTelemetryRequest(logs);

            // Assert
            Assert.Equal(3, request.ProtoLogs.Count);
        }

        [Fact]
        public void DatabricksTelemetryExporter_CreateTelemetryRequest_UploadTime_IsRecentTimestamp()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 1 }
            };

            var beforeTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Act
            var request = exporter.CreateTelemetryRequest(logs);

            var afterTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Assert
            Assert.True(request.UploadTime >= beforeTime);
            Assert.True(request.UploadTime <= afterTime);
        }

        #endregion

        #region Serialization Tests

        [Fact]
        public void DatabricksTelemetryExporter_SerializeRequest_ProducesValidJson()
        {
            // Arrange
            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var request = new TelemetryRequest
            {
                UploadTime = 1234567890000,
                ProtoLogs = new List<string> { "{\"workspace_id\":12345}" }
            };

            // Act
            var json = exporter.SerializeRequest(request);

            // Assert
            Assert.NotEmpty(json);
            var parsed = JsonDocument.Parse(json);
            Assert.Equal(1234567890000, parsed.RootElement.GetProperty("uploadTime").GetInt64());
            Assert.Single(parsed.RootElement.GetProperty("protoLogs").EnumerateArray());
        }

        #endregion

        #region ExportAsync Tests with Mock Handler

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_Success_ReturnsTrue()
        {
            // Arrange
            var handler = new MockHttpMessageHandler((request, ct) =>
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            });

            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration { MaxRetries = 0 };
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Act
            var result = await exporter.ExportAsync(logs);

            // Assert
            Assert.True(result, "ExportAsync should return true on successful HTTP 200 response");
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_EmptyList_ReturnsTrueWithoutRequest()
        {
            // Arrange
            var requestCount = 0;
            var handler = new MockHttpMessageHandler((request, ct) =>
            {
                requestCount++;
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            });

            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            // Act
            var result = await exporter.ExportAsync(new List<TelemetryFrontendLog>());

            // Assert - no HTTP request should be made and should return true
            Assert.Equal(0, requestCount);
            Assert.True(result, "ExportAsync should return true for empty list (nothing to export)");
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_NullList_ReturnsTrueWithoutRequest()
        {
            // Arrange
            var requestCount = 0;
            var handler = new MockHttpMessageHandler((request, ct) =>
            {
                requestCount++;
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            });

            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            // Act
            var result = await exporter.ExportAsync(null!);

            // Assert - no HTTP request should be made and should return true
            Assert.Equal(0, requestCount);
            Assert.True(result, "ExportAsync should return true for null list (nothing to export)");
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_TransientFailure_RetriesAndReturnsTrue()
        {
            // Arrange
            var attemptCount = 0;
            var handler = new MockHttpMessageHandler((request, ct) =>
            {
                attemptCount++;
                if (attemptCount < 3)
                {
                    throw new HttpRequestException("Simulated transient failure");
                }
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            });

            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration { MaxRetries = 3, RetryDelayMs = 10 };
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Act
            var result = await exporter.ExportAsync(logs);

            // Assert - should retry and eventually succeed
            Assert.Equal(3, attemptCount);
            Assert.True(result, "ExportAsync should return true after successful retry");
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_MaxRetries_ReturnsFalse()
        {
            // Arrange
            var attemptCount = 0;
            var handler = new MockHttpMessageHandler((request, ct) =>
            {
                attemptCount++;
                throw new HttpRequestException("Simulated persistent failure");
            });

            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration { MaxRetries = 3, RetryDelayMs = 10 };
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Act
            var result = await exporter.ExportAsync(logs);

            // Assert - should have tried initial attempt + max retries and return false
            Assert.Equal(4, attemptCount); // 1 initial + 3 retries
            Assert.False(result, "ExportAsync should return false after all retries exhausted");
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_TerminalError_ReturnsFalseWithoutRetry()
        {
            // Arrange
            var attemptCount = 0;
            var handler = new MockHttpMessageHandler((request, ct) =>
            {
                attemptCount++;
                // Throw HttpRequestException with an inner UnauthorizedAccessException
                // The ExceptionClassifier checks inner exceptions for terminal types
                throw new HttpRequestException("Authentication failed",
                    new UnauthorizedAccessException("Unauthorized"));
            });

            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration { MaxRetries = 3, RetryDelayMs = 10 };
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Act
            var result = await exporter.ExportAsync(logs);

            // Assert - should not retry on terminal error and return false
            Assert.Equal(1, attemptCount);
            Assert.False(result, "ExportAsync should return false on terminal error");
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_Cancelled_ThrowsCancelledException()
        {
            // Arrange
            var handler = new MockHttpMessageHandler((request, ct) =>
            {
                ct.ThrowIfCancellationRequested();
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            });

            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act & Assert - TaskCanceledException inherits from OperationCanceledException
            var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => exporter.ExportAsync(logs, cts.Token));
            Assert.True(ex is OperationCanceledException);
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_Authenticated_SendsToCorrectEndpoint()
        {
            // Arrange
            string? capturedUrl = null;
            var handler = new MockHttpMessageHandler((request, ct) =>
            {
                capturedUrl = request.RequestUri?.ToString();
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            });

            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Act
            await exporter.ExportAsync(logs);

            // Assert
            Assert.Equal($"{TestHost}/telemetry-ext", capturedUrl);
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_Unauthenticated_SendsToCorrectEndpoint()
        {
            // Arrange
            string? capturedUrl = null;
            var handler = new MockHttpMessageHandler((request, ct) =>
            {
                capturedUrl = request.RequestUri?.ToString();
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            });

            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, false, config);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Act
            await exporter.ExportAsync(logs);

            // Assert
            Assert.Equal($"{TestHost}/telemetry-unauth", capturedUrl);
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_SendsValidJsonBody()
        {
            // Arrange
            string? capturedContent = null;
            var handler = new MockHttpMessageHandler(async (request, ct) =>
            {
                capturedContent = await request.Content!.ReadAsStringAsync();
                return new HttpResponseMessage(HttpStatusCode.OK);
            });

            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog
                {
                    WorkspaceId = 12345,
                    FrontendLogEventId = "test-event-123"
                }
            };

            // Act
            await exporter.ExportAsync(logs);

            // Assert
            Assert.NotNull(capturedContent);
            var parsedRequest = JsonDocument.Parse(capturedContent);
            Assert.True(parsedRequest.RootElement.TryGetProperty("uploadTime", out _));
            Assert.True(parsedRequest.RootElement.TryGetProperty("protoLogs", out var protoLogs));
            Assert.Single(protoLogs.EnumerateArray());

            var protoLogJson = protoLogs[0].GetString();
            Assert.Contains("12345", protoLogJson);
            Assert.Contains("test-event-123", protoLogJson);
        }

        [Fact]
        public async Task DatabricksTelemetryExporter_ExportAsync_GenericException_ReturnsFalse()
        {
            // Arrange
            var handler = new MockHttpMessageHandler((request, ct) =>
            {
                throw new InvalidOperationException("Unexpected error");
            });

            using var httpClient = new HttpClient(handler);
            var config = new TelemetryConfiguration { MaxRetries = 0 };
            var exporter = new DatabricksTelemetryExporter(httpClient, TestHost, true, config);

            var logs = new List<TelemetryFrontendLog>
            {
                new TelemetryFrontendLog { WorkspaceId = 12345 }
            };

            // Act
            var result = await exporter.ExportAsync(logs);

            // Assert - should not throw but return false
            Assert.False(result, "ExportAsync should return false on unexpected exception");
        }

        #endregion

        #region Mock HTTP Handler

        /// <summary>
        /// Mock HttpMessageHandler for testing HTTP requests.
        /// </summary>
        private class MockHttpMessageHandler : HttpMessageHandler
        {
            private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _handler;

            public MockHttpMessageHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> handler)
            {
                _handler = handler;
            }

            protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                return _handler(request, cancellationToken);
            }
        }

        #endregion
    }
}
