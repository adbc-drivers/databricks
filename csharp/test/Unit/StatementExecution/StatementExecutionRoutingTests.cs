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
using AdbcDrivers.Databricks.StatementExecution;
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    public class StatementExecutionRoutingTests
    {
        [Theory]
        [InlineData(null, false)]
        [InlineData("false", false)]
        [InlineData("invalid", false)]
        [InlineData("true", true)]
        [InlineData("TRUE", true)]
        [InlineData(" true ", true)]
        public async Task SessionLifecycle_UsesCreateSessionRoute(string? realtimeHeader, bool useWarehouseUrls)
        {
            const string host = "https://test.databricks.com";
            const string warehouseId = "warehouse-123";
            const string sessionId = "session-123";
            const string statementId = "statement-123";
            var requests = new List<HttpRequestMessage>();
            var bodies = new List<string?>();
            var responses = new Queue<string>(new[]
            {
                "{\"session_id\":\"session-123\"}",
                "{\"statement_id\":\"statement-123\",\"status\":{\"state\":\"RUNNING\"}}",
                "{\"statement_id\":\"statement-123\",\"status\":{\"state\":\"SUCCEEDED\"}}",
                "{\"chunk_index\":0}",
                "{\"chunk_index\":1}",
                "",
                "",
                ""
            });
            var handler = new Mock<HttpMessageHandler>();
            handler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (request, _) =>
                {
                    requests.Add(request);
                    bodies.Add(request.Content == null ? null : await request.Content.ReadAsStringAsync());
                    var response = new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(responses.Dequeue())
                    };
                    if (requests.Count == 1 && realtimeHeader != null)
                        response.Headers.TryAddWithoutValidation("x-databricks-rt-session", realtimeHeader);
                    return response;
                });
            using var httpClient = new HttpClient(handler.Object);
            var client = new StatementExecutionClient(httpClient, host);

            await client.CreateSessionAsync(new CreateSessionRequest
            {
                WarehouseId = warehouseId,
                Catalog = "main",
                Schema = "default"
            }, CancellationToken.None);
            var executeRequest = new ExecuteStatementRequest
            {
                WarehouseId = warehouseId,
                SessionId = sessionId,
                Statement = "SELECT :value",
                Parameters = new List<StatementParameter>
                {
                    new StatementParameter { Name = "value", Value = "42", Type = "INT" }
                },
                WaitTimeout = "0s"
            };
            var executeResponse = await client.ExecuteStatementAsync(executeRequest, CancellationToken.None);
            Assert.Equal("RUNNING", executeResponse.Status?.State);
            var pollResponse = await client.GetStatementAsync(statementId, CancellationToken.None);
            Assert.Equal("SUCCEEDED", pollResponse.Status?.State);
            await client.GetResultChunkAsync(statementId, 0, CancellationToken.None);
            await client.GetResultChunkAsync(statementId, 1, CancellationToken.None);
            await client.CancelStatementAsync(statementId, CancellationToken.None);
            await client.CloseStatementAsync(statementId, CancellationToken.None);
            await client.DeleteSessionAsync(sessionId, warehouseId, CancellationToken.None);

            string warehouseSessionUrl = $"{host}/api/2.0/sql/warehouses/{warehouseId}/sessions/{sessionId}";
            string statementsUrl = useWarehouseUrls
                ? $"{warehouseSessionUrl}/statements"
                : $"{host}/api/2.0/sql/statements";
            string sessionUrl = useWarehouseUrls
                ? warehouseSessionUrl
                : $"{host}/api/2.0/sql/sessions/{sessionId}?warehouse_id={warehouseId}";
            Assert.Collection(requests,
                request => AssertRequest(request, HttpMethod.Post, $"{host}/api/2.0/sql/warehouses/{warehouseId}/sessions"),
                request => AssertRequest(request, HttpMethod.Post, statementsUrl),
                request => AssertRequest(request, HttpMethod.Get, $"{statementsUrl}/{statementId}"),
                request => AssertRequest(request, HttpMethod.Get, $"{statementsUrl}/{statementId}/result/chunks/0"),
                request => AssertRequest(request, HttpMethod.Get, $"{statementsUrl}/{statementId}/result/chunks/1"),
                request => AssertRequest(request, HttpMethod.Post, $"{statementsUrl}/{statementId}/cancel"),
                request => AssertRequest(request, HttpMethod.Delete, $"{statementsUrl}/{statementId}"),
                request => AssertRequest(request, HttpMethod.Delete, sessionUrl));
            using var createBody = JsonDocument.Parse(bodies[0]!);
            Assert.False(createBody.RootElement.TryGetProperty("warehouse_id", out _));
            Assert.Equal("main", createBody.RootElement.GetProperty("catalog").GetString());
            using var executeBody = JsonDocument.Parse(bodies[1]!);
            Assert.Equal(!useWarehouseUrls, executeBody.RootElement.TryGetProperty("warehouse_id", out var warehouse));
            Assert.Equal(!useWarehouseUrls, executeBody.RootElement.TryGetProperty("session_id", out var session));
            if (!useWarehouseUrls)
            {
                Assert.Equal(warehouseId, warehouse.GetString());
                Assert.Equal(sessionId, session.GetString());
            }
            Assert.Equal("SELECT :value", executeBody.RootElement.GetProperty("statement").GetString());
            Assert.Equal("0s", executeBody.RootElement.GetProperty("wait_timeout").GetString());
            Assert.Equal("42", executeBody.RootElement.GetProperty("parameters")[0].GetProperty("value").GetString());
            Assert.Equal(warehouseId, executeRequest.WarehouseId);
            Assert.Equal(sessionId, executeRequest.SessionId);
            Assert.All(bodies.GetRange(2, bodies.Count - 2), body => Assert.Null(body));
            Assert.Empty(responses);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task CreateSessionAsync_RequiresWarehouseId(string? warehouseId)
        {
            using var httpClient = new HttpClient(Mock.Of<HttpMessageHandler>());
            var client = new StatementExecutionClient(httpClient, "test.databricks.com");
            await Assert.ThrowsAsync<ArgumentException>(() => client.CreateSessionAsync(
                new CreateSessionRequest { WarehouseId = warehouseId! }, CancellationToken.None));
        }

        [Theory]
        [InlineData("null")]
        [InlineData("{}")]
        [InlineData("{\"session_id\":\"\"}")]
        public async Task CreateSessionAsync_RequiresSessionIdInResponse(string responseBody)
        {
            var handler = new Mock<HttpMessageHandler>();
            handler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(responseBody)
                });
            using var httpClient = new HttpClient(handler.Object);
            var client = new StatementExecutionClient(httpClient, "test.databricks.com");
            await Assert.ThrowsAsync<DatabricksException>(() => client.CreateSessionAsync(
                new CreateSessionRequest { WarehouseId = "warehouse-123" }, CancellationToken.None));
        }

        private static void AssertRequest(HttpRequestMessage request, HttpMethod method, string url)
        {
            Assert.Equal(method, request.Method);
            Assert.Equal(url, request.RequestUri?.AbsoluteUri);
            Assert.False(request.Headers.Contains("x-databricks-rt-session"));
        }
    }
}
