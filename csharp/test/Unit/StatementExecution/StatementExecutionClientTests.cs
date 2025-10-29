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
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;
using Moq;
using Moq.Protected;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit.StatementExecution
{
    public class StatementExecutionClientTests : IDisposable
    {
        private readonly Mock<HttpMessageHandler> _mockHttpMessageHandler;
        private readonly HttpClient _httpClient;
        private readonly string _testHost = "test.databricks.com";

        public StatementExecutionClientTests()
        {
            _mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            _httpClient = new HttpClient(_mockHttpMessageHandler.Object);
        }

        #region Constructor Tests

        [Fact]
        public void Constructor_WithValidParameters_CreatesClient()
        {
            var client = new StatementExecutionClient(_httpClient, _testHost);
            Assert.NotNull(client);
        }

        [Fact]
        public void Constructor_WithHostTrailingSlash_RemovesTrailingSlash()
        {
            var hostWithSlash = "test.databricks.com/";
            var client = new StatementExecutionClient(_httpClient, hostWithSlash);
            Assert.NotNull(client);
        }

        [Fact]
        public void Constructor_WithHttpsPrefix_AcceptsHttpsHost()
        {
            var httpsHost = "https://test.databricks.com";
            var client = new StatementExecutionClient(_httpClient, httpsHost);
            Assert.NotNull(client);
        }

        [Fact]
        public void Constructor_WithNullHttpClient_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new StatementExecutionClient(null!, _testHost));
        }

        [Fact]
        public void Constructor_WithEmptyHost_ThrowsArgumentException()
        {
            Assert.Throws<ArgumentException>(() =>
                new StatementExecutionClient(_httpClient, string.Empty));
        }

        [Fact]
        public void Constructor_WithNullHost_ThrowsArgumentException()
        {
            Assert.Throws<ArgumentException>(() =>
                new StatementExecutionClient(_httpClient, null!));
        }

        #endregion

        #region CreateSessionAsync Tests

        [Fact]
        public async Task CreateSessionAsync_WithValidRequest_ReturnsSessionId()
        {
            var expectedSessionId = "test-session-id-123";
            var request = new CreateSessionRequest
            {
                WarehouseId = "warehouse-123",
                Catalog = "main",
                Schema = "default"
            };

            var responseJson = JsonSerializer.Serialize(new
            {
                session_id = expectedSessionId
            });

            SetupMockResponse(HttpStatusCode.OK, responseJson);

            var client = new StatementExecutionClient(_httpClient, _testHost);
            var response = await client.CreateSessionAsync(request, CancellationToken.None);

            Assert.NotNull(response);
            Assert.Equal(expectedSessionId, response.SessionId);
        }

        [Fact]
        public async Task CreateSessionAsync_SendsCorrectRequestFormat()
        {
            var request = new CreateSessionRequest
            {
                WarehouseId = "warehouse-123",
                Catalog = "main",
                Schema = "default",
                SessionConfigs = new Dictionary<string, string>
                {
                    { "spark.sql.ansi.enabled", "true" }
                }
            };

            var responseJson = JsonSerializer.Serialize(new { session_id = "test-session" });
            HttpRequestMessage? capturedRequest = null;
            string? capturedContent = null;

            SetupMockResponseWithCapture(HttpStatusCode.OK, responseJson,
                req => capturedRequest = req,
                content => capturedContent = content);

            var client = new StatementExecutionClient(_httpClient, _testHost);
            await client.CreateSessionAsync(request, CancellationToken.None);

            Assert.NotNull(capturedRequest);
            Assert.Equal(HttpMethod.Post, capturedRequest.Method);
            Assert.Equal("https://test.databricks.com/api/2.0/sql/sessions",
                capturedRequest.RequestUri?.ToString());

            Assert.NotNull(capturedContent);
            Assert.Contains("\"warehouse_id\":\"warehouse-123\"", capturedContent);
            Assert.Contains("\"catalog\":\"main\"", capturedContent);
            Assert.Contains("\"schema\":\"default\"", capturedContent);
            Assert.Contains("spark.sql.ansi.enabled", capturedContent);
        }

        [Fact]
        public async Task CreateSessionAsync_WithNullRequest_ThrowsArgumentNullException()
        {
            var client = new StatementExecutionClient(_httpClient, _testHost);
            await Assert.ThrowsAsync<ArgumentNullException>(() =>
                client.CreateSessionAsync(null!, CancellationToken.None));
        }

        [Fact]
        public async Task CreateSessionAsync_WithHttpError_ThrowsDatabricksException()
        {
            var request = new CreateSessionRequest { WarehouseId = "warehouse-123" };
            SetupMockResponse(HttpStatusCode.BadRequest, "{\"error_code\":\"INVALID_REQUEST\",\"message\":\"Invalid warehouse ID\"}");

            var client = new StatementExecutionClient(_httpClient, _testHost);
            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                client.CreateSessionAsync(request, CancellationToken.None));

            Assert.Contains("400", exception.Message);
            Assert.Contains("INVALID_REQUEST", exception.Message);
        }

        #endregion

        #region DeleteSessionAsync Tests

        [Fact]
        public async Task DeleteSessionAsync_WithValidSessionId_Succeeds()
        {
            var sessionId = "test-session-id";
            SetupMockResponse(HttpStatusCode.OK, "");

            var client = new StatementExecutionClient(_httpClient, _testHost);
            await client.DeleteSessionAsync(sessionId, CancellationToken.None);

            // No exception means success
        }

        [Fact]
        public async Task DeleteSessionAsync_SendsCorrectRequest()
        {
            var sessionId = "test-session-id";
            HttpRequestMessage? capturedRequest = null;

            SetupMockResponseWithCapture(HttpStatusCode.OK, "",
                req => capturedRequest = req,
                _ => { });

            var client = new StatementExecutionClient(_httpClient, _testHost);
            await client.DeleteSessionAsync(sessionId, CancellationToken.None);

            Assert.NotNull(capturedRequest);
            Assert.Equal(HttpMethod.Delete, capturedRequest.Method);
            Assert.Equal($"https://test.databricks.com/api/2.0/sql/sessions/{sessionId}",
                capturedRequest.RequestUri?.ToString());
        }

        [Fact]
        public async Task DeleteSessionAsync_WithEmptySessionId_ThrowsArgumentException()
        {
            var client = new StatementExecutionClient(_httpClient, _testHost);
            await Assert.ThrowsAsync<ArgumentException>(() =>
                client.DeleteSessionAsync(string.Empty, CancellationToken.None));
        }

        [Fact]
        public async Task DeleteSessionAsync_WithHttpError_ThrowsDatabricksException()
        {
            var sessionId = "test-session-id";
            SetupMockResponse(HttpStatusCode.NotFound, "{\"error_code\":\"SESSION_NOT_FOUND\",\"message\":\"Session not found\"}");

            var client = new StatementExecutionClient(_httpClient, _testHost);
            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                client.DeleteSessionAsync(sessionId, CancellationToken.None));

            Assert.Contains("404", exception.Message);
        }

        #endregion

        #region ExecuteStatementAsync Tests

        [Fact]
        public async Task ExecuteStatementAsync_WithValidRequest_ReturnsResponse()
        {
            var expectedStatementId = "statement-123";
            var request = new ExecuteStatementRequest
            {
                Statement = "SELECT * FROM table",
                WarehouseId = "warehouse-123",
                Disposition = "external_links",
                Format = "arrow_stream"
            };

            var responseJson = JsonSerializer.Serialize(new
            {
                statement_id = expectedStatementId,
                status = new { state = "RUNNING" }
            });

            SetupMockResponse(HttpStatusCode.OK, responseJson);

            var client = new StatementExecutionClient(_httpClient, _testHost);
            var response = await client.ExecuteStatementAsync(request, CancellationToken.None);

            Assert.NotNull(response);
            Assert.Equal(expectedStatementId, response.StatementId);
            Assert.NotNull(response.Status);
            Assert.Equal("RUNNING", response.Status.State);
        }

        [Fact]
        public async Task ExecuteStatementAsync_WithParameters_SendsCorrectRequest()
        {
            var request = new ExecuteStatementRequest
            {
                Statement = "SELECT * FROM table WHERE id = :id",
                WarehouseId = "warehouse-123",
                Parameters = new List<StatementParameter>
                {
                    new StatementParameter { Name = "id", Value = "123", Type = "INT" }
                }
            };

            var responseJson = JsonSerializer.Serialize(new
            {
                statement_id = "stmt-123",
                status = new { state = "SUCCEEDED" }
            });

            HttpRequestMessage? capturedRequest = null;
            string? capturedContent = null;

            SetupMockResponseWithCapture(HttpStatusCode.OK, responseJson,
                req => capturedRequest = req,
                content => capturedContent = content);

            var client = new StatementExecutionClient(_httpClient, _testHost);
            await client.ExecuteStatementAsync(request, CancellationToken.None);

            Assert.NotNull(capturedRequest);
            Assert.Equal(HttpMethod.Post, capturedRequest.Method);
            Assert.Equal("https://test.databricks.com/api/2.0/sql/statements",
                capturedRequest.RequestUri?.ToString());

            Assert.NotNull(capturedContent);
            Assert.Contains("\"statement\":\"SELECT * FROM table WHERE id = :id\"", capturedContent);
            Assert.Contains("\"parameters\"", capturedContent);
            Assert.Contains("\"name\":\"id\"", capturedContent);
            Assert.Contains("\"value\":\"123\"", capturedContent);
        }

        [Fact]
        public async Task ExecuteStatementAsync_WithNullRequest_ThrowsArgumentNullException()
        {
            var client = new StatementExecutionClient(_httpClient, _testHost);
            await Assert.ThrowsAsync<ArgumentNullException>(() =>
                client.ExecuteStatementAsync(null!, CancellationToken.None));
        }

        [Fact]
        public async Task ExecuteStatementAsync_WithHttpError_ThrowsDatabricksException()
        {
            var request = new ExecuteStatementRequest { Statement = "INVALID SQL" };
            SetupMockResponse(HttpStatusCode.BadRequest,
                "{\"error_code\":\"INVALID_PARAMETER_VALUE\",\"message\":\"Invalid SQL syntax\"}");

            var client = new StatementExecutionClient(_httpClient, _testHost);
            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                client.ExecuteStatementAsync(request, CancellationToken.None));

            Assert.Contains("400", exception.Message);
            Assert.Contains("INVALID_PARAMETER_VALUE", exception.Message);
        }

        #endregion

        #region GetStatementAsync Tests

        [Fact]
        public async Task GetStatementAsync_WithValidStatementId_ReturnsResponse()
        {
            var statementId = "statement-123";
            var responseJson = JsonSerializer.Serialize(new
            {
                statement_id = statementId,
                status = new
                {
                    state = "SUCCEEDED"
                },
                manifest = new
                {
                    format = "arrow_stream",
                    total_chunk_count = 2,
                    total_row_count = 1000
                }
            });

            SetupMockResponse(HttpStatusCode.OK, responseJson);

            var client = new StatementExecutionClient(_httpClient, _testHost);
            var response = await client.GetStatementAsync(statementId, CancellationToken.None);

            Assert.NotNull(response);
            Assert.Equal(statementId, response.StatementId);
            Assert.NotNull(response.Status);
            Assert.Equal("SUCCEEDED", response.Status.State);
            Assert.NotNull(response.Manifest);
            Assert.Equal("arrow_stream", response.Manifest.Format);
            Assert.Equal(1000, response.Manifest.TotalRowCount);
        }

        [Fact]
        public async Task GetStatementAsync_SendsCorrectRequest()
        {
            var statementId = "statement-123";
            HttpRequestMessage? capturedRequest = null;

            SetupMockResponseWithCapture(HttpStatusCode.OK,
                "{\"statement_id\":\"statement-123\",\"status\":{\"state\":\"SUCCEEDED\"}}",
                req => capturedRequest = req,
                _ => { });

            var client = new StatementExecutionClient(_httpClient, _testHost);
            await client.GetStatementAsync(statementId, CancellationToken.None);

            Assert.NotNull(capturedRequest);
            Assert.Equal(HttpMethod.Get, capturedRequest.Method);
            Assert.Equal($"https://test.databricks.com/api/2.0/sql/statements/{statementId}",
                capturedRequest.RequestUri?.ToString());
        }

        [Fact]
        public async Task GetStatementAsync_WithEmptyStatementId_ThrowsArgumentException()
        {
            var client = new StatementExecutionClient(_httpClient, _testHost);
            await Assert.ThrowsAsync<ArgumentException>(() =>
                client.GetStatementAsync(string.Empty, CancellationToken.None));
        }

        [Fact]
        public async Task GetStatementAsync_WithHttpError_ThrowsDatabricksException()
        {
            var statementId = "statement-123";
            SetupMockResponse(HttpStatusCode.NotFound,
                "{\"error_code\":\"RESOURCE_DOES_NOT_EXIST\",\"message\":\"Statement not found\"}");

            var client = new StatementExecutionClient(_httpClient, _testHost);
            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                client.GetStatementAsync(statementId, CancellationToken.None));

            Assert.Contains("404", exception.Message);
        }

        #endregion

        #region GetResultChunkAsync Tests

        [Fact]
        public async Task GetResultChunkAsync_WithValidParameters_ReturnsResultData()
        {
            var statementId = "statement-123";
            var chunkIndex = 0L;
            var responseJson = JsonSerializer.Serialize(new
            {
                chunk_index = chunkIndex,
                row_count = 100,
                row_offset = 0,
                byte_count = 1024,
                external_links = new[]
                {
                    new
                    {
                        external_link = "https://s3.amazonaws.com/bucket/file.arrow",
                        row_count = 100
                    }
                }
            });

            SetupMockResponse(HttpStatusCode.OK, responseJson);

            var client = new StatementExecutionClient(_httpClient, _testHost);
            var response = await client.GetResultChunkAsync(statementId, chunkIndex, CancellationToken.None);

            Assert.NotNull(response);
            Assert.Equal(chunkIndex, response.ChunkIndex);
            Assert.Equal(100, response.RowCount);
            Assert.NotNull(response.ExternalLinks);
            Assert.Single(response.ExternalLinks);
        }

        [Fact]
        public async Task GetResultChunkAsync_SendsCorrectRequest()
        {
            var statementId = "statement-123";
            var chunkIndex = 5L;
            HttpRequestMessage? capturedRequest = null;

            SetupMockResponseWithCapture(HttpStatusCode.OK,
                "{\"chunk_index\":5,\"row_count\":100}",
                req => capturedRequest = req,
                _ => { });

            var client = new StatementExecutionClient(_httpClient, _testHost);
            await client.GetResultChunkAsync(statementId, chunkIndex, CancellationToken.None);

            Assert.NotNull(capturedRequest);
            Assert.Equal(HttpMethod.Get, capturedRequest.Method);
            Assert.Equal($"https://test.databricks.com/api/2.0/sql/statements/{statementId}/result/chunks/{chunkIndex}",
                capturedRequest.RequestUri?.ToString());
        }

        [Fact]
        public async Task GetResultChunkAsync_WithEmptyStatementId_ThrowsArgumentException()
        {
            var client = new StatementExecutionClient(_httpClient, _testHost);
            await Assert.ThrowsAsync<ArgumentException>(() =>
                client.GetResultChunkAsync(string.Empty, 0, CancellationToken.None));
        }

        [Fact]
        public async Task GetResultChunkAsync_WithNegativeChunkIndex_ThrowsArgumentException()
        {
            var client = new StatementExecutionClient(_httpClient, _testHost);
            await Assert.ThrowsAsync<ArgumentException>(() =>
                client.GetResultChunkAsync("statement-123", -1, CancellationToken.None));
        }

        [Fact]
        public async Task GetResultChunkAsync_WithHttpError_ThrowsDatabricksException()
        {
            var statementId = "statement-123";
            SetupMockResponse(HttpStatusCode.NotFound,
                "{\"error_code\":\"RESOURCE_DOES_NOT_EXIST\",\"message\":\"Chunk not found\"}");

            var client = new StatementExecutionClient(_httpClient, _testHost);
            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                client.GetResultChunkAsync(statementId, 0, CancellationToken.None));

            Assert.Contains("404", exception.Message);
        }

        #endregion

        #region CancelStatementAsync Tests

        [Fact]
        public async Task CancelStatementAsync_WithValidStatementId_Succeeds()
        {
            var statementId = "statement-123";
            SetupMockResponse(HttpStatusCode.OK, "");

            var client = new StatementExecutionClient(_httpClient, _testHost);
            await client.CancelStatementAsync(statementId, CancellationToken.None);

            // No exception means success
        }

        [Fact]
        public async Task CancelStatementAsync_SendsCorrectRequest()
        {
            var statementId = "statement-123";
            HttpRequestMessage? capturedRequest = null;

            SetupMockResponseWithCapture(HttpStatusCode.OK, "",
                req => capturedRequest = req,
                _ => { });

            var client = new StatementExecutionClient(_httpClient, _testHost);
            await client.CancelStatementAsync(statementId, CancellationToken.None);

            Assert.NotNull(capturedRequest);
            Assert.Equal(HttpMethod.Post, capturedRequest.Method);
            Assert.Equal($"https://test.databricks.com/api/2.0/sql/statements/{statementId}/cancel",
                capturedRequest.RequestUri?.ToString());
        }

        [Fact]
        public async Task CancelStatementAsync_WithEmptyStatementId_ThrowsArgumentException()
        {
            var client = new StatementExecutionClient(_httpClient, _testHost);
            await Assert.ThrowsAsync<ArgumentException>(() =>
                client.CancelStatementAsync(string.Empty, CancellationToken.None));
        }

        [Fact]
        public async Task CancelStatementAsync_WithHttpError_ThrowsDatabricksException()
        {
            var statementId = "statement-123";
            SetupMockResponse(HttpStatusCode.BadRequest,
                "{\"error_code\":\"INVALID_STATE\",\"message\":\"Cannot cancel completed statement\"}");

            var client = new StatementExecutionClient(_httpClient, _testHost);
            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                client.CancelStatementAsync(statementId, CancellationToken.None));

            Assert.Contains("400", exception.Message);
        }

        #endregion

        #region CloseStatementAsync Tests

        [Fact]
        public async Task CloseStatementAsync_WithValidStatementId_Succeeds()
        {
            var statementId = "statement-123";
            SetupMockResponse(HttpStatusCode.OK, "");

            var client = new StatementExecutionClient(_httpClient, _testHost);
            await client.CloseStatementAsync(statementId, CancellationToken.None);

            // No exception means success
        }

        [Fact]
        public async Task CloseStatementAsync_SendsCorrectRequest()
        {
            var statementId = "statement-123";
            HttpRequestMessage? capturedRequest = null;

            SetupMockResponseWithCapture(HttpStatusCode.OK, "",
                req => capturedRequest = req,
                _ => { });

            var client = new StatementExecutionClient(_httpClient, _testHost);
            await client.CloseStatementAsync(statementId, CancellationToken.None);

            Assert.NotNull(capturedRequest);
            Assert.Equal(HttpMethod.Post, capturedRequest.Method);
            Assert.Equal($"https://test.databricks.com/api/2.0/sql/statements/{statementId}/close",
                capturedRequest.RequestUri?.ToString());
        }

        [Fact]
        public async Task CloseStatementAsync_WithEmptyStatementId_ThrowsArgumentException()
        {
            var client = new StatementExecutionClient(_httpClient, _testHost);
            await Assert.ThrowsAsync<ArgumentException>(() =>
                client.CloseStatementAsync(string.Empty, CancellationToken.None));
        }

        [Fact]
        public async Task CloseStatementAsync_WithHttpError_ThrowsDatabricksException()
        {
            var statementId = "statement-123";
            SetupMockResponse(HttpStatusCode.NotFound,
                "{\"error_code\":\"RESOURCE_DOES_NOT_EXIST\",\"message\":\"Statement not found\"}");

            var client = new StatementExecutionClient(_httpClient, _testHost);
            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                client.CloseStatementAsync(statementId, CancellationToken.None));

            Assert.Contains("404", exception.Message);
        }

        #endregion

        #region Cancellation Token Tests

        [Fact]
        public async Task CreateSessionAsync_WithCancelledToken_ThrowsTaskCanceledException()
        {
            var request = new CreateSessionRequest { WarehouseId = "warehouse-123" };
            var cts = new CancellationTokenSource();
            cts.Cancel();

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ThrowsAsync(new TaskCanceledException());

            var client = new StatementExecutionClient(_httpClient, _testHost);
            await Assert.ThrowsAsync<TaskCanceledException>(() =>
                client.CreateSessionAsync(request, cts.Token));
        }

        [Fact]
        public async Task ExecuteStatementAsync_WithCancelledToken_ThrowsTaskCanceledException()
        {
            var request = new ExecuteStatementRequest { Statement = "SELECT 1" };
            var cts = new CancellationTokenSource();
            cts.Cancel();

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ThrowsAsync(new TaskCanceledException());

            var client = new StatementExecutionClient(_httpClient, _testHost);
            await Assert.ThrowsAsync<TaskCanceledException>(() =>
                client.ExecuteStatementAsync(request, cts.Token));
        }

        #endregion

        #region Helper Methods

        private void SetupMockResponse(HttpStatusCode statusCode, string responseContent)
        {
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

        private void SetupMockResponseWithCapture(
            HttpStatusCode statusCode,
            string responseContent,
            Action<HttpRequestMessage> requestCapture,
            Action<string> contentCapture)
        {
            var httpResponseMessage = new HttpResponseMessage(statusCode)
            {
                Content = new StringContent(responseContent)
            };

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (req, ct) =>
                {
                    requestCapture(req);
                    if (req.Content != null)
                    {
                        var content = await req.Content.ReadAsStringAsync();
                        contentCapture(content);
                    }
                    return httpResponseMessage;
                });
        }

        #endregion

        public void Dispose()
        {
            _httpClient?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
