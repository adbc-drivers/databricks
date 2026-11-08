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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
    public class StatementExecutionSessionVersionTests : IDisposable
    {
        private readonly Mock<HttpMessageHandler> _handler = new Mock<HttpMessageHandler>();
        private readonly HttpClient _httpClient;
        private readonly StatementExecutionClient _client;
        private readonly ConcurrentQueue<JsonElement> _requestBodies = new ConcurrentQueue<JsonElement>();

        public StatementExecutionSessionVersionTests()
        {
            _httpClient = new HttpClient(_handler.Object);
            _client = new StatementExecutionClient(_httpClient, "test.databricks.com");
        }

        [Fact]
        public async Task SessionVersion_ExchangesMaximumAcrossCreateExecuteAndPoll()
        {
            SetupResponses(
                """{"session_id":"session-1","session_version":{"version_id":7}}""",
                """{"status":{"state":"SUCCEEDED","session_version":{"version_id":9}}}""",
                """{"status":{"state":"SUCCEEDED","session_version":{"version_id":8}}}""",
                """{"status":{"state":"SUCCEEDED","session_version":{"version_id":11}}}""",
                """{"status":{"state":"SUCCEEDED","session_version":{"version_id":10}}}""",
                """{"status":{"state":"SUCCEEDED"}}""");

            var session = await CreateSessionAsync();
            Assert.Equal(7, session.SessionVersion?.VersionId);
            var request = new ExecuteStatementRequest
            {
                WarehouseId = "warehouse-1",
                SessionId = session.SessionId,
                Statement = "SELECT :value",
                WaitTimeout = "0s",
                Parameters = new List<StatementParameter>
                {
                    new StatementParameter { Name = "value", Value = "42", Type = "INT" }
                }
            };

            await _client.ExecuteStatementAsync(request, CancellationToken.None);
            await _client.ExecuteStatementAsync(request, CancellationToken.None);
            await _client.GetStatementAsync("statement-1", CancellationToken.None);
            await _client.ExecuteStatementAsync(request, CancellationToken.None);
            await _client.ExecuteStatementAsync(request, CancellationToken.None);

            var bodies = _requestBodies.ToArray();
            Assert.Equal("FAST", bodies[0].GetProperty("execution_mode").GetString());
            Assert.Equal(new long[] { 7, 9, 11, 11 }, bodies.Skip(1).Select(VersionId));
            Assert.All(bodies.Skip(1), body =>
            {
                Assert.Equal("session-1", body.GetProperty("session_id").GetString());
                Assert.Equal("SELECT :value", body.GetProperty("statement").GetString());
                Assert.Equal("0s", body.GetProperty("wait_timeout").GetString());
                Assert.Equal("42", body.GetProperty("parameters")[0].GetProperty("value").GetString());
            });
            Assert.Null(request.SessionVersion);
        }

        [Theory]
        [InlineData(null, null)]
        [InlineData(null, 7L)]
        [InlineData("null", null)]
        [InlineData("null", 7L)]
        [InlineData("{}", null)]
        [InlineData("{}", 7L)]
        [InlineData("{\"version_id\":null}", null)]
        [InlineData("{\"version_id\":null}", 7L)]
        public async Task SessionVersion_MissingVersionPreservesKnownState(string? version, long? initialVersion)
        {
            string versionField = version == null ? "" : $",\"session_version\":{version}";
            string initialField = initialVersion.HasValue
                ? $",\"session_version\":{{\"version_id\":{initialVersion.Value}}}"
                : versionField;
            SetupResponses(
                $"{{\"session_id\":\"session-1\"{initialField}}}",
                $"{{\"status\":{{\"state\":\"RUNNING\"{versionField}}}}}",
                $"{{\"status\":{{\"state\":\"SUCCEEDED\"{versionField}}}}}",
                """{"status":{"state":"SUCCEEDED"}}""");

            await CreateSessionAsync();
            await ExecuteAsync();
            await _client.GetStatementAsync("statement-1", CancellationToken.None);
            await ExecuteAsync();

            foreach (var body in _requestBodies.Skip(1))
            {
                if (initialVersion.HasValue)
                    Assert.Equal(initialVersion.Value, VersionId(body));
                else
                    Assert.False(body.TryGetProperty("session_version", out _));
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task SessionVersion_FailedStatementStillAdvancesVersion(bool fromPoll)
        {
            SetupResponses(
                """{"session_id":"session-1","session_version":{"version_id":7}}""",
                """{"status":{"state":"FAILED","session_version":{"version_id":12}}}""",
                """{"status":{"state":"SUCCEEDED"}}""");
            await CreateSessionAsync();

            if (fromPoll)
            {
                var response = await _client.GetStatementAsync("statement-1", CancellationToken.None);
                Assert.Equal("FAILED", response.Status?.State);
            }
            else
            {
                await Assert.ThrowsAsync<DatabricksException>(() => ExecuteAsync());
            }

            await ExecuteAsync();
            Assert.Equal(12, VersionId(_requestBodies.Last()));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task SessionVersion_ConcurrentExecuteAndPollKeepMaximum(bool higherVersionFromPoll)
        {
            var executeStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var pollStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var executeResponse = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            var pollResponse = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            int executeCount = 0;
            SetupHandler(request =>
            {
                if (request.RequestUri!.AbsolutePath.EndsWith("/sessions"))
                    return Task.FromResult(Response("""{"session_id":"session-1","session_version":{"version_id":7}}"""));
                if (request.Method == HttpMethod.Get)
                {
                    pollStarted.SetResult(true);
                    return pollResponse.Task;
                }
                if (Interlocked.Increment(ref executeCount) == 1)
                {
                    executeStarted.SetResult(true);
                    return executeResponse.Task;
                }
                return Task.FromResult(Response("""{"status":{"state":"SUCCEEDED"}}"""));
            });

            await CreateSessionAsync();
            var execute = ExecuteAsync();
            var poll = _client.GetStatementAsync("statement-1", CancellationToken.None);
            var started = Task.WhenAll(executeStarted.Task, pollStarted.Task);
            Assert.Same(started, await Task.WhenAny(started, Task.Delay(TimeSpan.FromSeconds(10))));
            await started;

            var high = Response("""{"status":{"state":"SUCCEEDED","session_version":{"version_id":12}}}""");
            var low = Response("""{"status":{"state":"SUCCEEDED","session_version":{"version_id":9}}}""");
            if (higherVersionFromPoll)
            {
                pollResponse.SetResult(high);
                await poll;
                executeResponse.SetResult(low);
                await execute;
            }
            else
            {
                executeResponse.SetResult(high);
                await execute;
                pollResponse.SetResult(low);
                await poll;
            }

            await ExecuteAsync();
            Assert.Equal(new long[] { 7, 12 }, _requestBodies.Skip(1).Select(VersionId));
        }

        [Fact]
        public async Task SessionVersion_PreservesZeroAnd64BitVersions()
        {
            SetupResponses(
                """{"session_id":"session-1","session_version":{"version_id":0}}""",
                """{"status":{"state":"SUCCEEDED","session_version":{"version_id":9223372036854775807}}}""",
                """{"status":{"state":"SUCCEEDED","session_version":{"version_id":1}}}""",
                """{"status":{"state":"SUCCEEDED"}}""");

            await CreateSessionAsync();
            await ExecuteAsync();
            await ExecuteAsync();
            await ExecuteAsync();

            Assert.Equal(new long[] { 0, long.MaxValue, long.MaxValue }, _requestBodies.Skip(1).Select(VersionId));
        }

        [Fact]
        public async Task SessionVersion_IsolatedBetweenConnections()
        {
            SetupResponses(
                """{"session_id":"session-1","session_version":{"version_id":7}}""",
                """{"session_id":"session-2","session_version":{"version_id":3}}""",
                """{"status":{"state":"SUCCEEDED","session_version":{"version_id":10}}}""",
                """{"status":{"state":"SUCCEEDED"}}""",
                """{"status":{"state":"SUCCEEDED"}}""");
            var otherClient = new StatementExecutionClient(_httpClient, "test.databricks.com");
            await CreateSessionAsync();
            await otherClient.CreateSessionAsync(new CreateSessionRequest { WarehouseId = "warehouse-1" }, CancellationToken.None);
            await ExecuteAsync();
            await otherClient.ExecuteStatementAsync(new ExecuteStatementRequest
            {
                SessionId = "session-2",
                Statement = "SELECT 1"
            }, CancellationToken.None);
            await ExecuteAsync();

            Assert.Equal(new long[] { 7, 3, 10 }, _requestBodies.Skip(2).Select(VersionId));
        }

        private Task<CreateSessionResponse> CreateSessionAsync() => _client.CreateSessionAsync(
            new CreateSessionRequest { WarehouseId = "warehouse-1" }, CancellationToken.None);

        private Task<ExecuteStatementResponse> ExecuteAsync() => _client.ExecuteStatementAsync(
            new ExecuteStatementRequest { SessionId = "session-1", Statement = "SELECT 1" }, CancellationToken.None);

        private static long VersionId(JsonElement body) => body.GetProperty("session_version").GetProperty("version_id").GetInt64();

        private void SetupResponses(params string[] responses)
        {
            var queue = new Queue<string>(responses);
            SetupHandler(_ => Task.FromResult(Response(queue.Dequeue())));
        }

        private void SetupHandler(Func<HttpRequestMessage, Task<HttpResponseMessage>> respond)
        {
            _handler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (request, _) =>
                {
                    if (request.Content != null)
                    {
                        using var body = JsonDocument.Parse(await request.Content.ReadAsStringAsync());
                        _requestBodies.Enqueue(body.RootElement.Clone());
                    }
                    return await respond(request);
                });
        }

        private static HttpResponseMessage Response(string json) => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(json)
        };

        public void Dispose() => _httpClient.Dispose();
    }
}
