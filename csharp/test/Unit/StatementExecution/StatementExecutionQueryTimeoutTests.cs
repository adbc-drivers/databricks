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
using AdbcDrivers.HiveServer2;
using Apache.Arrow.Adbc;
using AdbcDrivers.HiveServer2.Spark;
using Microsoft.IO;
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Tests that query timeout (adbc.apache.statement.query_timeout_s) is enforced
    /// during polling in PollUntilCompleteAsync (PECO-2937).
    /// </summary>
    public class StatementExecutionQueryTimeoutTests : IDisposable
    {
        private const string StatementId = "stmt-timeout-test";
        private readonly Mock<IStatementExecutionClient> _mockClient;
        private readonly HttpClient _httpClient;
        private readonly RecyclableMemoryStreamManager _memoryManager;

        public StatementExecutionQueryTimeoutTests()
        {
            _mockClient = new Mock<IStatementExecutionClient>();
            _memoryManager = new RecyclableMemoryStreamManager();

            // Minimal HTTP client for creating StatementExecutionConnection (tracing only)
            var handler = new Mock<HttpMessageHandler>();
            handler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(JsonSerializer.Serialize(new { session_id = "s1" }))
                });
            _httpClient = new HttpClient(handler.Object);
        }

        private StatementExecutionStatement CreateStatement(int queryTimeoutSeconds, int pollingIntervalMs = 50)
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test.databricks.com" },
                { DatabricksParameters.WarehouseId, "wh-1" },
                { SparkParameters.AccessToken, "token" },
                { ApacheParameters.QueryTimeoutSeconds, queryTimeoutSeconds.ToString() }
            };

            var connection = new StatementExecutionConnection(properties, _httpClient);

            var stmt = new StatementExecutionStatement(
                _mockClient.Object,
                sessionId: "session-1",
                warehouseId: "wh-1",
                catalog: null,
                schema: null,
                resultDisposition: "INLINE_OR_EXTERNAL_LINKS",
                resultFormat: "ARROW_STREAM",
                resultCompression: null,
                waitTimeoutSeconds: 0,
                pollingIntervalMs: pollingIntervalMs,
                properties: properties,
                recyclableMemoryStreamManager: _memoryManager,
                lz4BufferPool: System.Buffers.ArrayPool<byte>.Shared,
                httpClient: _httpClient,
                connection: connection);

            stmt.SqlQuery = "SELECT 1";
            return stmt;
        }

        private void SetupExecuteReturnsRunning()
        {
            _mockClient
                .Setup(c => c.ExecuteStatementAsync(It.IsAny<ExecuteStatementRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "RUNNING" }
                });
        }

        [Fact]
        public async Task ExecuteQueryAsync_NoTimeout_CompletesNormally()
        {
            SetupExecuteReturnsRunning();

            // GetStatement: first call returns RUNNING, second returns SUCCEEDED
            var callCount = 0;
            _mockClient
                .Setup(c => c.GetStatementAsync(StatementId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(() =>
                {
                    callCount++;
                    return callCount == 1
                        ? new GetStatementResponse { StatementId = StatementId, Status = new StatementStatus { State = "RUNNING" } }
                        : new GetStatementResponse
                        {
                            StatementId = StatementId,
                            Status = new StatementStatus { State = "SUCCEEDED" },
                            Manifest = new ResultManifest { Format = "ARROW_STREAM", Chunks = new System.Collections.Generic.List<ResultChunk>() }
                        };
                });

            using var stmt = CreateStatement(queryTimeoutSeconds: 0);
            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);

            Assert.NotNull(result);
            Assert.Equal(0, result.RowCount);
        }

        [Fact]
        public async Task ExecuteQueryAsync_WithTimeout_CompletesBeforeTimeout()
        {
            SetupExecuteReturnsRunning();

            // Returns SUCCEEDED on first GetStatement poll
            _mockClient
                .Setup(c => c.GetStatementAsync(StatementId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new GetStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = new ResultManifest { Format = "ARROW_STREAM", Chunks = new System.Collections.Generic.List<ResultChunk>() }
                });

            using var stmt = CreateStatement(queryTimeoutSeconds: 30);
            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);

            Assert.NotNull(result);
        }

        [Fact]
        public async Task ExecuteQueryAsync_TimeoutExpires_ThrowsAdbcExceptionAndCancelsStatement()
        {
            SetupExecuteReturnsRunning();

            // GetStatement always returns RUNNING (query never finishes)
            _mockClient
                .Setup(c => c.GetStatementAsync(StatementId, It.IsAny<CancellationToken>()))
                .Returns<string, CancellationToken>(async (id, ct) =>
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                    return null!; // never reached
                });

            _mockClient
                .Setup(c => c.CancelStatementAsync(StatementId, It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            using var stmt = CreateStatement(queryTimeoutSeconds: 1, pollingIntervalMs: 50);

            var ex = await Assert.ThrowsAsync<AdbcException>(() =>
                stmt.ExecuteQueryAsync(CancellationToken.None));

            Assert.Contains("timed out", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(StatementId, ex.Message);
            Assert.Contains("1", ex.Message); // timeout value
            Assert.Contains(ApacheParameters.QueryTimeoutSeconds, ex.Message);

            // Server-side cancel should have been called
            _mockClient.Verify(c =>
                c.CancelStatementAsync(StatementId, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task ExecuteQueryAsync_CallerCancels_ThrowsOperationCanceledNotTimeoutMessage()
        {
            SetupExecuteReturnsRunning();

            _mockClient
                .Setup(c => c.GetStatementAsync(StatementId, It.IsAny<CancellationToken>()))
                .Returns<string, CancellationToken>(async (id, ct) =>
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                    return null!;
                });

            using var cts = new CancellationTokenSource(millisecondsDelay: 200);
            using var stmt = CreateStatement(queryTimeoutSeconds: 30, pollingIntervalMs: 50);

            // Should throw OperationCanceledException, not AdbcException with timeout message
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                stmt.ExecuteQueryAsync(cts.Token));

            // Server-side cancel should NOT have been called (it's the caller who cancelled)
            _mockClient.Verify(c =>
                c.CancelStatementAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task ExecuteQueryAsync_ZeroTimeout_DisablesTimeout()
        {
            SetupExecuteReturnsRunning();

            var callCount = 0;
            _mockClient
                .Setup(c => c.GetStatementAsync(StatementId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(() =>
                {
                    callCount++;
                    if (callCount < 3) return new GetStatementResponse { StatementId = StatementId, Status = new StatementStatus { State = "RUNNING" } };
                    return new GetStatementResponse
                    {
                        StatementId = StatementId,
                        Status = new StatementStatus { State = "SUCCEEDED" },
                        Manifest = new ResultManifest { Format = "ARROW_STREAM", Chunks = new System.Collections.Generic.List<ResultChunk>() }
                    };
                });

            using var stmt = CreateStatement(queryTimeoutSeconds: 0);
            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);

            Assert.NotNull(result);
            Assert.Equal(3, callCount);
        }

        [Fact]
        public async Task ExecuteUpdateAsync_TimeoutExpires_ThrowsAdbcExceptionAndCancelsStatement()
        {
            SetupExecuteReturnsRunning();

            _mockClient
                .Setup(c => c.GetStatementAsync(StatementId, It.IsAny<CancellationToken>()))
                .Returns<string, CancellationToken>(async (id, ct) =>
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                    return null!;
                });

            _mockClient
                .Setup(c => c.CancelStatementAsync(StatementId, It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            using var stmt = CreateStatement(queryTimeoutSeconds: 1, pollingIntervalMs: 50);

            var ex = await Assert.ThrowsAsync<AdbcException>(() =>
                stmt.ExecuteUpdateAsync(CancellationToken.None));

            Assert.Contains("timed out", ex.Message, StringComparison.OrdinalIgnoreCase);
            _mockClient.Verify(c =>
                c.CancelStatementAsync(StatementId, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task ExecuteQueryAsync_CancelStatementFails_TimeoutExceptionStillThrown()
        {
            SetupExecuteReturnsRunning();

            _mockClient
                .Setup(c => c.GetStatementAsync(StatementId, It.IsAny<CancellationToken>()))
                .Returns<string, CancellationToken>(async (id, ct) =>
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                    return null!;
                });

            // Cancel fails — should not suppress the timeout exception
            _mockClient
                .Setup(c => c.CancelStatementAsync(StatementId, It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Network error"));

            using var stmt = CreateStatement(queryTimeoutSeconds: 1, pollingIntervalMs: 50);

            var ex = await Assert.ThrowsAsync<AdbcException>(() =>
                stmt.ExecuteQueryAsync(CancellationToken.None));

            Assert.Contains("timed out", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        public void Dispose()
        {
            _httpClient?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
