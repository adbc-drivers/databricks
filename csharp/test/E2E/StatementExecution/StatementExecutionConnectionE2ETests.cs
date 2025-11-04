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
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Common;
using Moq;
using Moq.Protected;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.E2E.StatementExecution
{
    /// <summary>
    /// E2E tests for StatementExecutionConnection with support for both mock and real environments.
    /// By default, tests use mock responses for fast, isolated testing.
    /// To run against a real Databricks endpoint:
    /// 1. Set DATABRICKS_TEST_CONFIG_FILE environment variable to point to a JSON configuration file
    /// 2. Set USE_REAL_STATEMENT_EXECUTION_ENDPOINT=true to enable real endpoint testing
    /// The configuration file should include: hostName, path (with warehouse ID), and token/access_token.
    /// </summary>
    public class StatementExecutionConnectionE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        private readonly bool _useRealEndpoint;
        private HttpClient? _httpClient;
        private Mock<HttpMessageHandler>? _mockHttpMessageHandler;
        private StatementExecutionClient? _client;

        public StatementExecutionConnectionE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            // Only use real endpoint if explicitly enabled AND config file is available
            _useRealEndpoint = Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true"
                            && Utils.CanExecuteTestConfig(TestConfigVariable);

            // Initialize mock infrastructure in constructor for mock mode
            if (!_useRealEndpoint)
            {
                _mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            }
        }

        private StatementExecutionClient CreateClient()
        {
            if (_useRealEndpoint)
            {
                return CreateRealClient();
            }
            else
            {
                return CreateMockClient();
            }
        }

        private StatementExecutionClient CreateRealClient()
        {
            var host = DatabricksTestHelpers.GetHostFromConfiguration(TestConfiguration);

            // Get access token from configuration (supports both direct token and OAuth)
            var accessToken = TestConfiguration.Token ?? TestConfiguration.AccessToken;
            if (string.IsNullOrEmpty(accessToken))
            {
                throw new InvalidOperationException(
                    "Token or AccessToken must be set in the test configuration file. " +
                    "For OAuth, ensure the connection has been established to obtain an access token.");
            }

            _httpClient = new HttpClient();
            _httpClient.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken);

            return new StatementExecutionClient(_httpClient, host);
        }

        private StatementExecutionClient CreateMockClient()
        {
            if (_mockHttpMessageHandler == null)
            {
                throw new InvalidOperationException("Mock HTTP handler not initialized");
            }

            _httpClient = new HttpClient(_mockHttpMessageHandler.Object);
            return new StatementExecutionClient(_httpClient, "mock.databricks.com");
        }

        private void SetupMockResponse(HttpStatusCode statusCode, string responseContent)
        {
            if (_useRealEndpoint)
            {
                throw new InvalidOperationException("Cannot setup mock responses when using real endpoint");
            }

            if (_mockHttpMessageHandler == null)
            {
                throw new InvalidOperationException("Mock HTTP handler not initialized");
            }

            var httpResponseMessage = new HttpResponseMessage(statusCode)
            {
                Content = new StringContent(responseContent)
            };

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(httpResponseMessage);
        }

        private Dictionary<string, string> GetConnectionProperties(bool enableSessionManagement = true)
        {
            var properties = new Dictionary<string, string>();

            if (_useRealEndpoint)
            {
                properties[SparkParameters.Path] = DatabricksTestHelpers.GetPathFromConfiguration(TestConfiguration);
            }
            else
            {
                properties[SparkParameters.Path] = "/sql/1.0/warehouses/mock-warehouse-id";
            }

            properties[DatabricksParameters.EnableSessionManagement] = enableSessionManagement.ToString().ToLower();

            return properties;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _httpClient?.Dispose();
            }
            base.Dispose(disposing);
        }

        [Fact]
        public async Task ConnectionLifecycle_OpenAndClose_CreatesAndDeletesSession()
        {
            if (!_useRealEndpoint)
            {
                // Mock: Setup create session response
                SetupMockResponse(HttpStatusCode.OK,
                    JsonSerializer.Serialize(new { session_id = "mock-session-123" }));
            }

            _client = CreateClient();
            var properties = GetConnectionProperties(enableSessionManagement: true);
            var connection = new StatementExecutionConnection(_client, properties);

            // Open connection - should create session
            await connection.OpenAsync();
            Assert.NotNull(connection.SessionId);
            Assert.False(string.IsNullOrEmpty(connection.SessionId));

            if (!_useRealEndpoint)
            {
                // Mock: Setup delete session response
                SetupMockResponse(HttpStatusCode.OK, "");
            }

            // Close connection - should delete session
            await connection.CloseAsync();
            Assert.Null(connection.SessionId);
        }

        [Fact]
        public async Task ConnectionLifecycle_WithCatalogAndSchema_PassesToSession()
        {
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK,
                    JsonSerializer.Serialize(new { session_id = "mock-session-456" }));
            }

            _client = CreateClient();
            var properties = GetConnectionProperties(enableSessionManagement: true);
            properties[AdbcOptions.Connection.CurrentCatalog] = "test_catalog";
            properties[AdbcOptions.Connection.CurrentDbSchema] = "test_schema";

            var connection = new StatementExecutionConnection(_client, properties);

            await connection.OpenAsync();
            Assert.NotNull(connection.SessionId);

            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, "");
            }

            await connection.CloseAsync();
        }

        [Fact]
        public async Task ConnectionLifecycle_SessionManagementDisabled_DoesNotCreateSession()
        {
            _client = CreateClient();
            var properties = GetConnectionProperties(enableSessionManagement: false);
            var connection = new StatementExecutionConnection(_client, properties);

            // Open connection - should NOT create session
            await connection.OpenAsync();
            Assert.Null(connection.SessionId);

            // Close connection - should NOT attempt to delete session
            await connection.CloseAsync();
            Assert.Null(connection.SessionId);
        }

        [Fact]
        public async Task Dispose_WithActiveSession_ClosesSessionProperly()
        {
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK,
                    JsonSerializer.Serialize(new { session_id = "mock-session-dispose" }));
            }

            _client = CreateClient();
            var properties = GetConnectionProperties(enableSessionManagement: true);
            var connection = new StatementExecutionConnection(_client, properties);

            await connection.OpenAsync();
            var sessionId = connection.SessionId;
            Assert.NotNull(sessionId);

            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, "");
            }

            // Dispose should close the session
            connection.Dispose();

            // After dispose, we can't check the session ID state, but we've verified it was created
            // and the dispose method should have cleaned it up
        }

        [Fact]
        public async Task OpenTwice_OnlyCreatesOneSession()
        {
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK,
                    JsonSerializer.Serialize(new { session_id = "mock-session-once" }));
            }

            _client = CreateClient();
            var properties = GetConnectionProperties(enableSessionManagement: true);
            var connection = new StatementExecutionConnection(_client, properties);

            // First open
            await connection.OpenAsync();
            var firstSessionId = connection.SessionId;
            Assert.NotNull(firstSessionId);

            // Second open - should not create a new session
            await connection.OpenAsync();
            var secondSessionId = connection.SessionId;
            Assert.Equal(firstSessionId, secondSessionId);

            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, "");
            }

            await connection.CloseAsync();
        }

        [Fact]
        public async Task CloseWithoutOpen_DoesNotThrow()
        {
            _client = CreateClient();
            var properties = GetConnectionProperties(enableSessionManagement: true);
            var connection = new StatementExecutionConnection(_client, properties);

            // Close without opening should not throw
            await connection.CloseAsync();
            Assert.Null(connection.SessionId);
        }

        [Fact]
        public async Task CloseTwice_DoesNotThrow()
        {
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK,
                    JsonSerializer.Serialize(new { session_id = "mock-session-close-twice" }));
            }

            _client = CreateClient();
            var properties = GetConnectionProperties(enableSessionManagement: true);
            var connection = new StatementExecutionConnection(_client, properties);

            await connection.OpenAsync();
            Assert.NotNull(connection.SessionId);

            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, "");
            }

            // First close
            await connection.CloseAsync();
            Assert.Null(connection.SessionId);

            // Second close - should not throw
            await connection.CloseAsync();
            Assert.Null(connection.SessionId);
        }

        [Fact]
        public async Task Close_WhenDeleteSessionFails_SwallowsException()
        {
            if (!_useRealEndpoint)
            {
                // Setup create session to succeed
                SetupMockResponse(HttpStatusCode.OK,
                    JsonSerializer.Serialize(new { session_id = "mock-session-fail-delete" }));
            }

            _client = CreateClient();
            var properties = GetConnectionProperties(enableSessionManagement: true);
            var connection = new StatementExecutionConnection(_client, properties);

            await connection.OpenAsync();
            Assert.NotNull(connection.SessionId);

            if (!_useRealEndpoint)
            {
                // Setup delete session to fail
                SetupMockResponse(HttpStatusCode.InternalServerError,
                    JsonSerializer.Serialize(new { error_code = "INTERNAL_ERROR", message = "Failed to delete session" }));
            }

            // Close should not throw even if delete fails
            await connection.CloseAsync();
            Assert.Null(connection.SessionId); // Session ID should be cleared regardless
        }

        [Fact]
        public void WarehouseIdExtraction_FromStandardPath_Succeeds()
        {
            _client = CreateClient();
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123def456" }
            };

            var connection = new StatementExecutionConnection(_client, properties);
            Assert.Equal("abc123def456", connection.WarehouseId);
        }

        [Fact]
        public void WarehouseIdExtraction_WithTrailingSlash_Succeeds()
        {
            _client = CreateClient();
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/test-warehouse/" }
            };

            var connection = new StatementExecutionConnection(_client, properties);
            Assert.Equal("test-warehouse", connection.WarehouseId);
        }

        [Fact]
        public void WarehouseIdExtraction_CaseInsensitive_Succeeds()
        {
            _client = CreateClient();
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/WAREHOUSES/uppercase-id" }
            };

            var connection = new StatementExecutionConnection(_client, properties);
            Assert.Equal("uppercase-id", connection.WarehouseId);
        }

        [Fact]
        public void Constructor_MissingPath_ThrowsArgumentException()
        {
            _client = CreateClient();
            var properties = new Dictionary<string, string>();

            var exception = Assert.Throws<ArgumentException>(() =>
                new StatementExecutionConnection(_client, properties));

            Assert.Contains("Missing required property", exception.Message);
        }

        [Fact]
        public void Constructor_InvalidPathFormat_ThrowsArgumentException()
        {
            _client = CreateClient();
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/invalid/path/format" }
            };

            var exception = Assert.Throws<ArgumentException>(() =>
                new StatementExecutionConnection(_client, properties));

            Assert.Contains("Invalid http_path format", exception.Message);
        }

        [Fact]
        public async Task FullWorkflow_CreateOpenCloseDispose_Succeeds()
        {
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK,
                    JsonSerializer.Serialize(new { session_id = "mock-session-workflow" }));
            }

            _client = CreateClient();
            var properties = GetConnectionProperties(enableSessionManagement: true);
            properties[AdbcOptions.Connection.CurrentCatalog] = "main";
            properties[AdbcOptions.Connection.CurrentDbSchema] = "default";

            var connection = new StatementExecutionConnection(_client, properties);

            // Verify warehouse ID extraction
            Assert.NotNull(connection.WarehouseId);
            Assert.NotEmpty(connection.WarehouseId);

            // Open connection
            await connection.OpenAsync();
            Assert.NotNull(connection.SessionId);
            Assert.NotEmpty(connection.SessionId);

            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, "");
            }

            // Close connection
            await connection.CloseAsync();
            Assert.Null(connection.SessionId);

            // Dispose (should be safe even after close)
            connection.Dispose();
        }

        [Fact]
        public async Task MultipleConnections_Independent_EachHasOwnSession()
        {
            if (!_useRealEndpoint)
            {
                // Will be called multiple times, return different session IDs
                var sessionIdCounter = 0;
                _mockHttpMessageHandler!.Protected()
                    .Setup<Task<HttpResponseMessage>>(
                        "SendAsync",
                        ItExpr.Is<HttpRequestMessage>(req => req.Method == HttpMethod.Post),
                        ItExpr.IsAny<CancellationToken>())
                    .ReturnsAsync(() => new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(JsonSerializer.Serialize(new
                        {
                            session_id = $"mock-session-{++sessionIdCounter}"
                        }))
                    });
            }

            _client = CreateClient();
            var properties1 = GetConnectionProperties(enableSessionManagement: true);
            var properties2 = GetConnectionProperties(enableSessionManagement: true);

            var connection1 = new StatementExecutionConnection(_client, properties1);
            var connection2 = new StatementExecutionConnection(_client, properties2);

            // Open both connections
            await connection1.OpenAsync();
            await connection2.OpenAsync();

            Assert.NotNull(connection1.SessionId);
            Assert.NotNull(connection2.SessionId);

            if (!_useRealEndpoint)
            {
                // Sessions should be different
                Assert.NotEqual(connection1.SessionId, connection2.SessionId);

                // Setup delete responses
                _mockHttpMessageHandler!.Protected()
                    .Setup<Task<HttpResponseMessage>>(
                        "SendAsync",
                        ItExpr.Is<HttpRequestMessage>(req => req.Method == HttpMethod.Delete),
                        ItExpr.IsAny<CancellationToken>())
                    .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));
            }

            // Close both connections
            await connection1.CloseAsync();
            await connection2.CloseAsync();

            Assert.Null(connection1.SessionId);
            Assert.Null(connection2.SessionId);
        }

        [Fact]
        public void CreateStatement_ReturnsStatement()
        {
            _client = CreateClient();
            var properties = GetConnectionProperties(enableSessionManagement: true);
            var connection = new StatementExecutionConnection(_client, properties);

            var statement = connection.CreateStatement();

            Assert.NotNull(statement);
            Assert.IsAssignableFrom<AdbcStatement>(statement);
        }
    }
}
