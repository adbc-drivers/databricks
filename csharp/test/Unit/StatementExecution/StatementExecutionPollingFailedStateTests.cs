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
    /// Tests that a statement which resolves to FAILED *after* PENDING/RUNNING polling
    /// surfaces a DatabricksException with Status (InternalError), SqlState, and NativeError
    /// populated from the server error — the same contract as the synchronously-FAILED path
    /// (see StatementExecutionClientTests.ExecuteStatementAsync_WithFailedState_...).
    /// The polling path is a real path metadata/queries take on a slow warehouse, so the
    /// cross-protocol Status + SqlState parity that this PR establishes must hold here too.
    /// </summary>
    public class StatementExecutionPollingFailedStateTests : IDisposable
    {
        private const string StatementId = "stmt-polling-failed";
        private readonly Mock<IStatementExecutionClient> _mockClient;
        private readonly HttpClient _httpClient;
        private readonly RecyclableMemoryStreamManager _memoryManager;

        public StatementExecutionPollingFailedStateTests()
        {
            _mockClient = new Mock<IStatementExecutionClient>();
            _memoryManager = new RecyclableMemoryStreamManager();

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

        private StatementExecutionStatement CreateStatement(int pollingIntervalMs = 20)
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test.databricks.com" },
                { DatabricksParameters.WarehouseId, "wh-1" },
                { SparkParameters.AccessToken, "token" },
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
                waitTimeout: "0s",
                pollingIntervalMs: pollingIntervalMs,
                properties: properties,
                recyclableMemoryStreamManager: _memoryManager,
                lz4BufferPool: System.Buffers.ArrayPool<byte>.Shared,
                httpClient: _httpClient,
                connection: connection);

            stmt.SqlQuery = "SELECT 1";
            return stmt;
        }

        // ExecuteStatement returns RUNNING; the first GetStatement poll resolves to FAILED
        // with the given server error, forcing the polling FAILED branch (not the synchronous one).
        private void SetupRunningThenFailed(string errorCode, string message, string sqlState)
        {
            _mockClient
                .Setup(c => c.ExecuteStatementAsync(It.IsAny<ExecuteStatementRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "RUNNING" }
                });

            _mockClient
                .Setup(c => c.GetStatementAsync(StatementId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new GetStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus
                    {
                        State = "FAILED",
                        // The SEA server places sql_state at the status level (sibling of error),
                        // not inside error — mirror that real shape here (verified live).
                        SqlState = sqlState,
                        Error = new StatementError
                        {
                            ErrorCode = errorCode,
                            Message = message,
                        }
                    }
                });
        }

        [Fact]
        public async Task ExecuteQueryAsync_FailedAfterPolling_ThrowsDatabricksExceptionWithSqlStateAndNativeError()
        {
            SetupRunningThenFailed("12345", "value too long for VARCHAR", "22001");

            using var stmt = CreateStatement();

            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                stmt.ExecuteQueryAsync(CancellationToken.None));

            Assert.Equal(AdbcStatusCode.InternalError, exception.Status);
            Assert.Equal("22001", exception.SqlState);
            Assert.Equal(12345, exception.NativeError);
            Assert.Contains("value too long for VARCHAR", exception.Message);
        }

        [Fact]
        public async Task ExecuteUpdateAsync_FailedAfterPolling_ThrowsDatabricksExceptionWithSqlStateAndNativeError()
        {
            SetupRunningThenFailed("54321", "table not found", "42P01");

            using var stmt = CreateStatement();

            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                stmt.ExecuteUpdateAsync(CancellationToken.None));

            Assert.Equal(AdbcStatusCode.InternalError, exception.Status);
            Assert.Equal("42P01", exception.SqlState);
            Assert.Equal(54321, exception.NativeError);
            Assert.Contains("table not found", exception.Message);
        }

        public void Dispose()
        {
            _httpClient?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
