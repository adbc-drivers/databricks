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

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.E2E.StatementExecution
{
    /// <summary>
    /// E2E tests for Statement Execution API with support for both mock and real environments.
    /// Set environment variable USE_REAL_DATABRICKS_ENDPOINT=true to run against real Databricks.
    /// When using real endpoint, also set: DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID, and DATABRICKS_ACCESS_TOKEN
    /// </summary>
    public class StatementExecutionClientE2ETests : IDisposable
    {
        private readonly bool _useRealEndpoint;
        private HttpClient? _httpClient;
        private Mock<HttpMessageHandler>? _mockHttpMessageHandler;

        public StatementExecutionClientE2ETests()
        {
            _useRealEndpoint = Environment.GetEnvironmentVariable("USE_REAL_DATABRICKS_ENDPOINT") == "true";
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
            var host = Environment.GetEnvironmentVariable("DATABRICKS_HOST");
            if (string.IsNullOrEmpty(host))
            {
                throw new InvalidOperationException(
                    "DATABRICKS_HOST environment variable must be set when USE_REAL_DATABRICKS_ENDPOINT=true");
            }

            var accessToken = Environment.GetEnvironmentVariable("DATABRICKS_ACCESS_TOKEN");
            if (string.IsNullOrEmpty(accessToken))
            {
                throw new InvalidOperationException(
                    "DATABRICKS_ACCESS_TOKEN environment variable must be set when USE_REAL_DATABRICKS_ENDPOINT=true");
            }

            _httpClient = new HttpClient();
            _httpClient.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken);

            return new StatementExecutionClient(_httpClient, host);
        }

        private StatementExecutionClient CreateMockClient()
        {
            _mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            _httpClient = new HttpClient(_mockHttpMessageHandler.Object);
            return new StatementExecutionClient(_httpClient, "mock.databricks.com");
        }

        private void SetupMockResponse(HttpStatusCode statusCode, string responseContent)
        {
            if (_useRealEndpoint || _mockHttpMessageHandler == null)
            {
                throw new InvalidOperationException("Cannot setup mock responses when using real endpoint");
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

        private string GetWarehouseId()
        {
            if (_useRealEndpoint)
            {
                var warehouseId = Environment.GetEnvironmentVariable("DATABRICKS_WAREHOUSE_ID");
                if (string.IsNullOrEmpty(warehouseId))
                {
                    throw new InvalidOperationException(
                        "DATABRICKS_WAREHOUSE_ID environment variable must be set when USE_REAL_DATABRICKS_ENDPOINT=true");
                }
                return warehouseId;
            }
            else
            {
                return "mock-warehouse-id";
            }
        }

        public void Dispose()
        {
            _httpClient?.Dispose();
            GC.SuppressFinalize(this);
        }

        [Fact]
        public async Task SessionLifecycle_CreateAndDelete_Succeeds()
        {
            if (!_useRealEndpoint)
            {
                // Mock: Setup create session response
                SetupMockResponse(HttpStatusCode.OK,
                    JsonSerializer.Serialize(new { session_id = "mock-session-123" }));
            }

            var client = CreateClient();
            var createRequest = new CreateSessionRequest
            {
                WarehouseId = GetWarehouseId(),
                Catalog = "main",
                Schema = "default"
            };

            // Create session
            var createResponse = await client.CreateSessionAsync(createRequest, CancellationToken.None);
            Assert.NotNull(createResponse);
            Assert.False(string.IsNullOrEmpty(createResponse.SessionId));

            if (!_useRealEndpoint)
            {
                // Mock: Setup delete session response
                SetupMockResponse(HttpStatusCode.OK, "");
            }

            // Delete session
            await client.DeleteSessionAsync(createResponse.SessionId, CancellationToken.None);
        }

        [Fact]
        public async Task ExecuteStatement_SimpleQuery_ReturnsStatementId()
        {
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, JsonSerializer.Serialize(new
                {
                    statement_id = "mock-statement-123",
                    status = new { state = "SUCCEEDED" },
                    manifest = new
                    {
                        format = "arrow_stream",
                        total_chunk_count = 1,
                        total_row_count = 1,
                        chunks = new[]
                        {
                            new
                            {
                                chunk_index = 0,
                                row_count = 1,
                                external_links = new[]
                                {
                                    new { external_link = "https://mock.s3.amazonaws.com/file.arrow" }
                                }
                            }
                        }
                    }
                }));
            }

            var client = CreateClient();
            var request = new ExecuteStatementRequest
            {
                Statement = "SELECT 1 AS value",
                WarehouseId = GetWarehouseId(),
                Disposition = "external_links",
                Format = "arrow_stream"
            };

            var response = await client.ExecuteStatementAsync(request, CancellationToken.None);

            Assert.NotNull(response);
            Assert.False(string.IsNullOrEmpty(response.StatementId));
            Assert.NotNull(response.Status);
        }

        [Fact]
        public async Task ExecuteStatement_WithParameters_Succeeds()
        {
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, JsonSerializer.Serialize(new
                {
                    statement_id = "mock-statement-456",
                    status = new { state = "SUCCEEDED" }
                }));
            }

            var client = CreateClient();
            var request = new ExecuteStatementRequest
            {
                Statement = "SELECT :value AS result",
                WarehouseId = GetWarehouseId(),
                Parameters = new List<StatementParameter>
                {
                    new StatementParameter { Name = "value", Value = "42", Type = "INT" }
                },
                Disposition = "inline",
                Format = "arrow_stream"
            };

            var response = await client.ExecuteStatementAsync(request, CancellationToken.None);

            Assert.NotNull(response);
            Assert.False(string.IsNullOrEmpty(response.StatementId));
        }

        [Fact]
        public async Task GetStatement_AfterExecution_ReturnsResults()
        {
            var client = CreateClient();
            string statementId;

            // First execute a statement
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, JsonSerializer.Serialize(new
                {
                    statement_id = "mock-statement-789",
                    status = new { state = "RUNNING" }
                }));
            }

            var executeRequest = new ExecuteStatementRequest
            {
                Statement = "SELECT 1 AS value",
                WarehouseId = GetWarehouseId(),
                Disposition = "external_links",
                Format = "arrow_stream"
            };

            var executeResponse = await client.ExecuteStatementAsync(executeRequest, CancellationToken.None);
            statementId = executeResponse.StatementId;

            // Then get the statement status
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, JsonSerializer.Serialize(new
                {
                    statement_id = statementId,
                    status = new { state = "SUCCEEDED" },
                    manifest = new
                    {
                        format = "arrow_stream",
                        total_chunk_count = 1,
                        total_row_count = 1
                    }
                }));
            }

            var getResponse = await client.GetStatementAsync(statementId, CancellationToken.None);

            Assert.NotNull(getResponse);
            Assert.Equal(statementId, getResponse.StatementId);
            Assert.NotNull(getResponse.Status);

            // For real endpoint, wait for completion if still running
            if (_useRealEndpoint && getResponse.Status.State == "RUNNING")
            {
                for (int i = 0; i < 30 && getResponse.Status.State == "RUNNING"; i++)
                {
                    await Task.Delay(1000);
                    getResponse = await client.GetStatementAsync(statementId, CancellationToken.None);
                }
            }

            // Verify final state (should be SUCCEEDED for simple SELECT 1)
            if (_useRealEndpoint)
            {
                Assert.Equal("SUCCEEDED", getResponse.Status.State);
            }
        }

        [Fact]
        public async Task GetResultChunk_WithValidChunkIndex_ReturnsChunkData()
        {
            if (!_useRealEndpoint)
            {
                // Mock: Setup execute response with chunks
                SetupMockResponse(HttpStatusCode.OK, JsonSerializer.Serialize(new
                {
                    statement_id = "mock-statement-chunk",
                    status = new { state = "SUCCEEDED" },
                    manifest = new
                    {
                        format = "arrow_stream",
                        total_chunk_count = 2,
                        total_row_count = 200
                    }
                }));
            }

            var client = CreateClient();
            var executeRequest = new ExecuteStatementRequest
            {
                Statement = "SELECT * FROM range(200)",
                WarehouseId = GetWarehouseId(),
                Disposition = "external_links",
                Format = "arrow_stream"
            };

            var executeResponse = await client.ExecuteStatementAsync(executeRequest, CancellationToken.None);

            if (!_useRealEndpoint)
            {
                // Mock: Setup get chunk response
                SetupMockResponse(HttpStatusCode.OK, JsonSerializer.Serialize(new
                {
                    chunk_index = 0,
                    row_count = 100,
                    row_offset = 0,
                    external_links = new[]
                    {
                        new { external_link = "https://mock.s3.amazonaws.com/chunk0.arrow" }
                    }
                }));
            }

            // Try to get first chunk (may not be available in all scenarios)
            try
            {
                var chunkData = await client.GetResultChunkAsync(
                    executeResponse.StatementId,
                    0,
                    CancellationToken.None);

                Assert.NotNull(chunkData);
                if (_useRealEndpoint)
                {
                    Assert.True(chunkData.RowCount > 0);
                }
            }
            catch (DatabricksException ex) when (_useRealEndpoint && ex.Message.Contains("404"))
            {
                // It's okay if chunks aren't available via this endpoint in real env
                // This depends on how the query was executed and result disposition
            }
        }

        [Fact]
        public async Task CancelStatement_RunningQuery_Succeeds()
        {
            var client = CreateClient();
            string statementId;

            // Execute a long-running statement
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, JsonSerializer.Serialize(new
                {
                    statement_id = "mock-statement-cancel",
                    status = new { state = "RUNNING" }
                }));
            }

            var executeRequest = new ExecuteStatementRequest
            {
                Statement = _useRealEndpoint
                    ? "SELECT COUNT(*) FROM range(10000000)"  // Long-running query
                    : "SELECT 1",
                WarehouseId = GetWarehouseId(),
                WaitTimeout = "1s",  // Short timeout to ensure it stays RUNNING
                OnWaitTimeout = "CONTINUE"
            };

            var executeResponse = await client.ExecuteStatementAsync(executeRequest, CancellationToken.None);
            statementId = executeResponse.StatementId;

            // Cancel the statement
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, "");
            }

            await client.CancelStatementAsync(statementId, CancellationToken.None);

            // Verify cancellation
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, JsonSerializer.Serialize(new
                {
                    statement_id = statementId,
                    status = new { state = "CANCELED" }
                }));
            }

            if (_useRealEndpoint)
            {
                // Give it a moment to process cancellation
                await Task.Delay(2000);
            }

            var getResponse = await client.GetStatementAsync(statementId, CancellationToken.None);

            if (_useRealEndpoint)
            {
                // State should be CANCELED or CLOSED after cancellation
                Assert.True(
                    getResponse.Status.State == "CANCELED" || getResponse.Status.State == "CLOSED",
                    $"Expected CANCELED or CLOSED, got {getResponse.Status.State}");
            }
        }

        [Fact]
        public async Task CloseStatement_AfterExecution_Succeeds()
        {
            var client = CreateClient();
            string statementId;

            // Execute a statement
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, JsonSerializer.Serialize(new
                {
                    statement_id = "mock-statement-close",
                    status = new { state = "SUCCEEDED" }
                }));
            }

            var executeRequest = new ExecuteStatementRequest
            {
                Statement = "SELECT 1",
                WarehouseId = GetWarehouseId()
            };

            var executeResponse = await client.ExecuteStatementAsync(executeRequest, CancellationToken.None);
            statementId = executeResponse.StatementId;

            // Close the statement
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.OK, "");
            }

            await client.CloseStatementAsync(statementId, CancellationToken.None);

            // Verify closure (getting a closed statement may return 404 or CLOSED state)
            if (_useRealEndpoint)
            {
                try
                {
                    if (!_useRealEndpoint)
                    {
                        SetupMockResponse(HttpStatusCode.OK, JsonSerializer.Serialize(new
                        {
                            statement_id = statementId,
                            status = new { state = "CLOSED" }
                        }));
                    }

                    var getResponse = await client.GetStatementAsync(statementId, CancellationToken.None);
                    Assert.Equal("CLOSED", getResponse.Status.State);
                }
                catch (DatabricksException ex) when (ex.Message.Contains("404"))
                {
                    // 404 is also acceptable - statement has been cleaned up
                }
            }
        }

        [Fact]
        public async Task ErrorHandling_InvalidSQL_ThrowsDatabricksException()
        {
            if (!_useRealEndpoint)
            {
                SetupMockResponse(HttpStatusCode.BadRequest, JsonSerializer.Serialize(new
                {
                    error_code = "INVALID_PARAMETER_VALUE",
                    message = "Syntax error in SQL statement"
                }));
            }

            var client = CreateClient();
            var request = new ExecuteStatementRequest
            {
                Statement = "INVALID SQL SYNTAX HERE",
                WarehouseId = GetWarehouseId()
            };

            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                client.ExecuteStatementAsync(request, CancellationToken.None));

            Assert.NotNull(exception.Message);
            if (!_useRealEndpoint)
            {
                Assert.Contains("400", exception.Message);
            }
        }

        [Fact]
        public async Task FullWorkflow_CreateSessionExecuteQueryClose_Succeeds()
        {
            var client = CreateClient();
            string? sessionId = null;
            string? statementId = null;

            try
            {
                // Step 1: Create session
                if (!_useRealEndpoint)
                {
                    SetupMockResponse(HttpStatusCode.OK,
                        JsonSerializer.Serialize(new { session_id = "mock-session-workflow" }));
                }

                var sessionRequest = new CreateSessionRequest
                {
                    WarehouseId = GetWarehouseId(),
                    Catalog = "main",
                    Schema = "default"
                };

                var sessionResponse = await client.CreateSessionAsync(sessionRequest, CancellationToken.None);
                sessionId = sessionResponse.SessionId;
                Assert.NotNull(sessionId);

                // Step 2: Execute statement in session
                if (!_useRealEndpoint)
                {
                    SetupMockResponse(HttpStatusCode.OK, JsonSerializer.Serialize(new
                    {
                        statement_id = "mock-statement-workflow",
                        status = new { state = "SUCCEEDED" }
                    }));
                }

                var statementRequest = new ExecuteStatementRequest
                {
                    Statement = "SELECT 'Hello from session' AS greeting",
                    SessionId = sessionId,
                    Disposition = "inline",
                    Format = "arrow_stream"
                };

                var statementResponse = await client.ExecuteStatementAsync(
                    statementRequest,
                    CancellationToken.None);
                statementId = statementResponse.StatementId;
                Assert.NotNull(statementId);

                // Step 3: Close statement
                if (!_useRealEndpoint)
                {
                    SetupMockResponse(HttpStatusCode.OK, "");
                }

                await client.CloseStatementAsync(statementId, CancellationToken.None);
            }
            finally
            {
                // Cleanup: Delete session
                if (sessionId != null)
                {
                    if (!_useRealEndpoint)
                    {
                        SetupMockResponse(HttpStatusCode.OK, "");
                    }

                    try
                    {
                        await client.DeleteSessionAsync(sessionId, CancellationToken.None);
                    }
                    catch
                    {
                        // Best effort cleanup
                    }
                }
            }
        }
    }
}
