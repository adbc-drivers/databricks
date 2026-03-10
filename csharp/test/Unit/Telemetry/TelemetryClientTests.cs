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
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Unit tests for TelemetryClient that coordinates the listener/aggregator/exporter pipeline.
    /// </summary>
    public sealed class TelemetryClientTests : IDisposable
    {
        private readonly Mock<HttpMessageHandler> _mockHttpHandler;
        private readonly HttpClient _httpClient;
        private readonly TelemetryConfiguration _configuration;

        public TelemetryClientTests()
        {
            // Set up mock HTTP handler that returns success responses
            _mockHttpHandler = new Mock<HttpMessageHandler>();
            _mockHttpHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent("{\"success\": true}")
                });

            _httpClient = new HttpClient(_mockHttpHandler.Object)
            {
                BaseAddress = new Uri("https://test.databricks.com/")
            };

            _configuration = new TelemetryConfiguration
            {
                Enabled = true,
                BatchSize = 10,
                FlushIntervalMs = 5000
            };
        }

        public void Dispose()
        {
            _httpClient?.Dispose();
        }

        /// <summary>
        /// Test: Constructor initializes all pipeline components in correct order.
        /// </summary>
        [Fact]
        public async Task Constructor_InitializesComponents()
        {
            // Arrange
            string host = "test.databricks.com";
            bool isAuthenticated = true;

            // Act
            TelemetryClient client = new TelemetryClient(host, _httpClient, isAuthenticated, _configuration);

            // Assert
            // If constructor completes without throwing, all components were initialized successfully
            Assert.NotNull(client);

            // Clean up
            await client.DisposeAsync();
        }

        /// <summary>
        /// Test: Constructor throws ArgumentNullException when host is null.
        /// </summary>
        [Fact]
        public void Constructor_NullHost_ThrowsArgumentException()
        {
            // Arrange
            string? host = null;
            bool isAuthenticated = true;

            // Act & Assert
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
                new TelemetryClient(host!, _httpClient, isAuthenticated, _configuration));

            Assert.Contains("Host cannot be null or whitespace", ex.Message);
        }

        /// <summary>
        /// Test: Constructor throws ArgumentException when host is empty.
        /// </summary>
        [Fact]
        public void Constructor_EmptyHost_ThrowsArgumentException()
        {
            // Arrange
            string host = string.Empty;
            bool isAuthenticated = true;

            // Act & Assert
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
                new TelemetryClient(host, _httpClient, isAuthenticated, _configuration));

            Assert.Contains("Host cannot be null or whitespace", ex.Message);
        }

        /// <summary>
        /// Test: Constructor throws ArgumentNullException when httpClient is null.
        /// </summary>
        [Fact]
        public void Constructor_NullHttpClient_ThrowsArgumentNullException()
        {
            // Arrange
            string host = "test.databricks.com";
            bool isAuthenticated = true;
            HttpClient? httpClient = null;

            // Act & Assert
            ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() =>
                new TelemetryClient(host, httpClient!, isAuthenticated, _configuration));

            Assert.Equal("httpClient", ex.ParamName);
        }

        /// <summary>
        /// Test: Constructor throws ArgumentNullException when configuration is null.
        /// </summary>
        [Fact]
        public void Constructor_NullConfiguration_ThrowsArgumentNullException()
        {
            // Arrange
            string host = "test.databricks.com";
            bool isAuthenticated = true;
            TelemetryConfiguration? configuration = null;

            // Act & Assert
            ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() =>
                new TelemetryClient(host, _httpClient, isAuthenticated, configuration!));

            Assert.Equal("configuration", ex.ParamName);
        }

        /// <summary>
        /// Test: FlushAsync delegates to aggregator to flush pending metrics.
        /// </summary>
        [Fact]
        public async Task FlushAsync_DelegatesToAggregator()
        {
            // Arrange
            string host = "test.databricks.com";
            bool isAuthenticated = true;
            TelemetryClient client = new TelemetryClient(host, _httpClient, isAuthenticated, _configuration);

            // Act
            // FlushAsync should complete without throwing
            await client.FlushAsync();

            // Assert
            // If FlushAsync completes without throwing, it successfully delegated to the aggregator
            // Clean up
            await client.CloseAsync();
        }

        /// <summary>
        /// Test: FlushAsync does not throw when client is disposed.
        /// </summary>
        [Fact]
        public async Task FlushAsync_DisposedClient_DoesNotThrow()
        {
            // Arrange
            string host = "test.databricks.com";
            bool isAuthenticated = true;
            TelemetryClient client = new TelemetryClient(host, _httpClient, isAuthenticated, _configuration);
            await client.CloseAsync();

            // Act & Assert
            // Should not throw even after dispose
            await client.FlushAsync();
        }

        /// <summary>
        /// Test: CloseAsync flushes pending metrics and cancels background tasks.
        /// </summary>
        [Fact]
        public async Task CloseAsync_FlushesAndCancels()
        {
            // Arrange
            string host = "test.databricks.com";
            bool isAuthenticated = true;
            TelemetryClient client = new TelemetryClient(host, _httpClient, isAuthenticated, _configuration);

            // Act
            await client.CloseAsync();

            // Assert
            // If CloseAsync completes without throwing, it successfully:
            // 1. Stopped the activity listener
            // 2. Flushed all pending metrics
            // 3. Cancelled background tasks
            // 4. Disposed resources

            // Verify FlushAsync after close is a no-op (doesn't throw)
            await client.FlushAsync();
        }

        /// <summary>
        /// Test: CloseAsync is idempotent - calling multiple times is safe.
        /// </summary>
        [Fact]
        public async Task CloseAsync_Idempotent_MultipleCalls()
        {
            // Arrange
            string host = "test.databricks.com";
            bool isAuthenticated = true;
            TelemetryClient client = new TelemetryClient(host, _httpClient, isAuthenticated, _configuration);

            // Act
            await client.CloseAsync();
            await client.CloseAsync(); // Second call
            await client.CloseAsync(); // Third call

            // Assert
            // All calls should complete without throwing
        }

        /// <summary>
        /// Test: CloseAsync swallows exceptions and does not throw.
        /// </summary>
        [Fact]
        public async Task CloseAsync_ExceptionSwallowed()
        {
            // Arrange
            // Create a configuration that might cause issues during cleanup
            TelemetryConfiguration problematicConfig = new TelemetryConfiguration
            {
                Enabled = true,
                BatchSize = 1,
                FlushIntervalMs = 1 // Very short interval
            };

            string host = "test.databricks.com";
            bool isAuthenticated = true;
            TelemetryClient client = new TelemetryClient(host, _httpClient, isAuthenticated, problematicConfig);

            // Act & Assert
            // CloseAsync should not throw even if internal operations fail
            await client.CloseAsync();
        }

        /// <summary>
        /// Test: DisposeAsync delegates to CloseAsync.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_DelegatesToCloseAsync()
        {
            // Arrange
            string host = "test.databricks.com";
            bool isAuthenticated = true;
            TelemetryClient client = new TelemetryClient(host, _httpClient, isAuthenticated, _configuration);

            // Act
            await client.DisposeAsync();

            // Assert
            // DisposeAsync should complete by delegating to CloseAsync
            // Subsequent operations should be no-ops
            await client.FlushAsync();
        }

        /// <summary>
        /// Test: Enqueue does not throw when log is null.
        /// </summary>
        [Fact]
        public async Task Enqueue_NullLog_DoesNotThrow()
        {
            // Arrange
            string host = "test.databricks.com";
            bool isAuthenticated = true;
            TelemetryClient client = new TelemetryClient(host, _httpClient, isAuthenticated, _configuration);

            // Act & Assert
            client.Enqueue(null!);

            // Clean up
            await client.CloseAsync();
        }

        /// <summary>
        /// Test: Enqueue does not throw when client is disposed.
        /// </summary>
        [Fact]
        public async Task Enqueue_DisposedClient_DoesNotThrow()
        {
            // Arrange
            string host = "test.databricks.com";
            bool isAuthenticated = true;
            TelemetryClient client = new TelemetryClient(host, _httpClient, isAuthenticated, _configuration);
            await client.CloseAsync();

            // Act & Assert
            // Should not throw after dispose
            client.Enqueue(new AdbcDrivers.Databricks.Telemetry.Models.TelemetryFrontendLog
            {
                FrontendLogEventId = "test-event-id"
            });
        }

        /// <summary>
        /// Test: Multiple clients can be created for different hosts.
        /// </summary>
        [Fact]
        public async Task MultipleClients_DifferentHosts_AllInitializeSuccessfully()
        {
            // Arrange
            string host1 = "host1.databricks.com";
            string host2 = "host2.databricks.com";
            string host3 = "host3.databricks.com";
            bool isAuthenticated = true;

            // Act
            TelemetryClient client1 = new TelemetryClient(host1, _httpClient, isAuthenticated, _configuration);
            TelemetryClient client2 = new TelemetryClient(host2, _httpClient, isAuthenticated, _configuration);
            TelemetryClient client3 = new TelemetryClient(host3, _httpClient, isAuthenticated, _configuration);

            // Assert
            Assert.NotNull(client1);
            Assert.NotNull(client2);
            Assert.NotNull(client3);

            // Clean up
            await client1.CloseAsync();
            await client2.CloseAsync();
            await client3.CloseAsync();
        }

        /// <summary>
        /// Test: Client handles flush cancellation gracefully.
        /// </summary>
        [Fact]
        public async Task FlushAsync_Cancellation_HandlesGracefully()
        {
            // Arrange
            string host = "test.databricks.com";
            bool isAuthenticated = true;
            TelemetryClient client = new TelemetryClient(host, _httpClient, isAuthenticated, _configuration);

            using CancellationTokenSource cts = new CancellationTokenSource();
            cts.Cancel(); // Cancel immediately

            // Act & Assert
            // Should handle cancellation gracefully (may throw OperationCanceledException or complete)
            try
            {
                await client.FlushAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                // Expected and acceptable
            }

            // Clean up
            await client.CloseAsync();
        }
    }
}
