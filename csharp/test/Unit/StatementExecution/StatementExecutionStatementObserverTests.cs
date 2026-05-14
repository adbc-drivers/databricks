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
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Microsoft.IO;
using Moq;
using Moq.Protected;
using Xunit;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Verifies the observer hookpoints wired into <see cref="StatementExecutionStatement"/>:
    /// <c>OnExecuteStarted</c> fires before the server call, <c>OnExecuteSucceeded</c> fires
    /// after the response is received with the server-assigned statement id,
    /// <c>OnPollCompleted</c> fires exactly once on terminal poll state with the accumulated
    /// (count, latencyMs), and <c>OnError</c> fires on any failure path. These tests use a
    /// recording fake observer so we can assert exact call order and arguments — production
    /// pipes the same calls into a real <see cref="TelemetryObserver"/>, which is exercised
    /// by separate telemetry tests.
    /// </summary>
    public class StatementExecutionStatementObserverTests
    {
        private const string StatementId = "stmt-observer-test";

        // ── Recording fake observer ────────────────────────────────────────────────

        /// <summary>
        /// Captures every observer call along with its arguments and the order in which it
        /// occurred. Implements the fail-open contract by never throwing from a method body.
        /// </summary>
        private sealed class RecordingObserver : IStatementOperationObserver
        {
            public readonly List<string> Calls = new();
            public StatementType? ExecuteStartedStmtType;
            public OperationType? ExecuteStartedOpType;
            public bool? ExecuteStartedIsCompressed;
            public string? ExecuteSucceededStatementId;
            public ExecutionResultFormat? ExecuteSucceededFormat;
            public int? PollCount;
            public long? PollLatencyMs;
            public Exception? Error;
            public int OnPollCompletedCallCount;

            public void OnExecuteStarted(StatementType stmtType, OperationType opType, bool isCompressed)
            {
                ExecuteStartedStmtType = stmtType;
                ExecuteStartedOpType = opType;
                ExecuteStartedIsCompressed = isCompressed;
                Calls.Add(nameof(OnExecuteStarted));
            }

            public void OnExecuteSucceeded(string statementId, ExecutionResultFormat resultFormat)
            {
                ExecuteSucceededStatementId = statementId;
                ExecuteSucceededFormat = resultFormat;
                Calls.Add(nameof(OnExecuteSucceeded));
            }

            public void OnPollCompleted(int count, long latencyMs)
            {
                PollCount = count;
                PollLatencyMs = latencyMs;
                OnPollCompletedCallCount++;
                Calls.Add(nameof(OnPollCompleted));
            }

            public void OnFirstBatchReady(long latencyMs) => Calls.Add(nameof(OnFirstBatchReady));
            public void OnConsumed(long latencyMs) => Calls.Add(nameof(OnConsumed));
            public void OnChunksDownloaded(ChunkMetrics metrics) => Calls.Add(nameof(OnChunksDownloaded));

            public void OnError(Exception ex)
            {
                Error = ex;
                Calls.Add(nameof(OnError));
            }

            public void OnFinalized() => Calls.Add(nameof(OnFinalized));
        }

        // ── Helpers ────────────────────────────────────────────────────────────────

        private static StatementExecutionStatement CreateStatement(
            IStatementExecutionClient client,
            IStatementOperationObserver observer,
            string? resultCompression = null,
            int pollingIntervalMs = 1)
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test.databricks.com" },
                { DatabricksParameters.WarehouseId, "wh-1" },
                { SparkParameters.AccessToken, "token" },
            };

            // The StatementExecutionConnection constructor wants an HttpClient. Wire a default
            // OK response so connection construction does not blow up; the test itself talks to
            // the mock IStatementExecutionClient, never through this HttpClient.
            var handlerMock = new Mock<HttpMessageHandler>();
            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(JsonSerializer.Serialize(new { session_id = "s1" }))
                });
            var httpClient = new HttpClient(handlerMock.Object);

            var connection = new StatementExecutionConnection(properties, httpClient);
            return new StatementExecutionStatement(
                client,
                sessionId: "session-1",
                warehouseId: "wh-1",
                catalog: null,
                schema: null,
                resultDisposition: "INLINE_OR_EXTERNAL_LINKS",
                resultFormat: "ARROW_STREAM",
                resultCompression: resultCompression,
                waitTimeoutSeconds: 0,
                // Tiny poll interval so multi-iteration tests don't take seconds. Hookpoint
                // semantics are independent of the interval.
                pollingIntervalMs: pollingIntervalMs,
                properties: properties,
                recyclableMemoryStreamManager: new RecyclableMemoryStreamManager(),
                lz4BufferPool: System.Buffers.ArrayPool<byte>.Shared,
                httpClient: httpClient,
                connection: connection,
                observer: observer);
        }

        private static ResultManifest BuildManifestWithSingleColumn()
        {
            return new ResultManifest
            {
                Format = "ARROW_STREAM",
                Schema = new ResultSchema
                {
                    Columns = new List<ColumnInfo>
                    {
                        new() { Name = "c0", TypeName = "INT", TypeText = "INT" }
                    }
                },
                TotalRowCount = 0,
                Chunks = new List<ResultChunk>(),
            };
        }

        // ── Tests ──────────────────────────────────────────────────────────────────

        [Fact]
        public async Task ExecuteQuery_CallsOnExecuteStarted_BeforeClient()
        {
            // OnExecuteStarted must fire before the SEA client's ExecuteStatementAsync; once the
            // statement is on the wire it is too late to record the intent. We assert the order by
            // capturing the observer's call log inside the mock's setup callback, so the recorded
            // log reflects the state of the observer at the moment the client method was invoked.
            var observer = new RecordingObserver();
            string[]? callsAtExecuteTime = null;

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .Callback<ExecuteStatementRequest, CancellationToken>((_, _) =>
                {
                    // Snapshot the observer's call log at the moment the client call is invoked.
                    callsAtExecuteTime = observer.Calls.ToArray();
                })
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = BuildManifestWithSingleColumn(),
                    Result = new ResultData { Attachment = null },
                });

            using var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "SELECT 1";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            Assert.NotNull(callsAtExecuteTime);
            Assert.Single(callsAtExecuteTime!);
            Assert.Equal(nameof(IStatementOperationObserver.OnExecuteStarted), callsAtExecuteTime![0]);

            // Non-metadata path: stmtType is Query, opType is ExecuteStatement.
            Assert.Equal(StatementType.Query, observer.ExecuteStartedStmtType);
            Assert.Equal(OperationType.ExecuteStatement, observer.ExecuteStartedOpType);
            // resultCompression was null in this statement, so isCompressed must be false.
            Assert.False(observer.ExecuteStartedIsCompressed);
        }

        [Fact]
        public async Task ExecuteQuery_OnExecuteStarted_PassesIsCompressedFromCompressionRequest()
        {
            // Sanity: a statement built with resultCompression=LZ4_FRAME forwards isCompressed=true
            // to the observer. The downstream manifest may override based on what the server actually
            // returned, but the first signal reflects what the client asked for.
            var observer = new RecordingObserver();

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = BuildManifestWithSingleColumn(),
                    Result = new ResultData { Attachment = null },
                });

            using var stmt = CreateStatement(mockClient.Object, observer, resultCompression: "LZ4_FRAME");
            stmt.SqlQuery = "SELECT 1";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            Assert.True(observer.ExecuteStartedIsCompressed);
        }

        [Fact]
        public async Task ExecuteQuery_CallsOnExecuteSucceeded_WithStatementId()
        {
            // OnExecuteSucceeded must fire once the server has accepted the statement and a
            // statement id is known, carrying that id forward to the observer. ResultFormat is
            // stubbed as Unspecified until the SeaResultFormatMapper helper lands in the parallel
            // phase 6 PR.
            var observer = new RecordingObserver();

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = BuildManifestWithSingleColumn(),
                    Result = new ResultData { Attachment = null },
                });

            using var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "SELECT 1";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            Assert.Equal(StatementId, observer.ExecuteSucceededStatementId);
            // Stub format value until SeaResultFormatMapper lands.
            Assert.Equal(ExecutionResultFormat.Unspecified, observer.ExecuteSucceededFormat);

            // OnExecuteStarted must precede OnExecuteSucceeded — order matters for telemetry record
            // assembly downstream.
            int startedIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnExecuteStarted));
            int succeededIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnExecuteSucceeded));
            Assert.True(startedIndex >= 0);
            Assert.True(succeededIndex > startedIndex);
        }

        [Fact]
        public async Task Poll_CallsOnPollCompleted_OnceOnTerminalState_WithAccumulatedCount()
        {
            // OnPollCompleted is emitted exactly once when the polling loop reaches a terminal
            // state, with the accumulated poll count. Setup: initial Execute returns PENDING (so
            // the statement code enters the poll loop), GetStatement returns RUNNING twice then
            // SUCCEEDED — that is three GetStatement calls, so PollCount must equal 3.
            var observer = new RecordingObserver();

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "PENDING" },
                });

            var pollResponses = new Queue<GetStatementResponse>();
            pollResponses.Enqueue(new GetStatementResponse
            {
                StatementId = StatementId,
                Status = new StatementStatus { State = "RUNNING" }
            });
            pollResponses.Enqueue(new GetStatementResponse
            {
                StatementId = StatementId,
                Status = new StatementStatus { State = "RUNNING" }
            });
            pollResponses.Enqueue(new GetStatementResponse
            {
                StatementId = StatementId,
                Status = new StatementStatus { State = "SUCCEEDED" },
                Manifest = BuildManifestWithSingleColumn(),
                Result = new ResultData { Attachment = null },
            });

            mockClient
                .Setup(c => c.GetStatementAsync(
                    It.IsAny<string>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => pollResponses.Dequeue());

            using var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "SELECT 1";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            // Exactly one OnPollCompleted call. Repeated emission would inflate poll_count downstream.
            Assert.Equal(1, observer.OnPollCompletedCallCount);
            Assert.Equal(3, observer.PollCount);
            // latencyMs is wall-clock and can validly be 0 on fast in-process mocks, but it must be
            // non-negative — anything else indicates a stopwatch bug.
            Assert.NotNull(observer.PollLatencyMs);
            Assert.True(observer.PollLatencyMs >= 0);

            // OnPollCompleted must arrive between OnExecuteSucceeded and any error/finalize signal:
            // the contract is that polling happens after the server has assigned a statement id.
            int succeededIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnExecuteSucceeded));
            int pollCompletedIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnPollCompleted));
            Assert.True(succeededIndex >= 0);
            Assert.True(pollCompletedIndex > succeededIndex);
        }

        [Fact]
        public async Task ExecuteQuery_ErrorPath_CallsOnError()
        {
            // Any failure inside ExecuteQueryInternalAsync must route through OnError: the catch
            // block wrapping the body is the only place that translates execute-time exceptions
            // into the observer's error signal. Use a server FAILED response which the statement
            // converts to an AdbcException — this exercises the post-OnExecuteSucceeded error path
            // (statement id is assigned, then the terminal state is FAILED).
            var observer = new RecordingObserver();

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus
                    {
                        State = "FAILED",
                        Error = new StatementError
                        {
                            Message = "SQL syntax error",
                            ErrorCode = "SYNTAX_ERROR"
                        }
                    },
                });

            using var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "NOT VALID SQL";

            await Assert.ThrowsAsync<AdbcException>(() => stmt.ExecuteQueryAsync(CancellationToken.None));

            // OnError must have fired exactly once carrying the originating exception, and must
            // arrive after OnExecuteStarted (the statement at least began before failing).
            Assert.NotNull(observer.Error);
            Assert.IsType<AdbcException>(observer.Error);
            Assert.Contains(nameof(IStatementOperationObserver.OnError), observer.Calls);

            int startedIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnExecuteStarted));
            int errorIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnError));
            Assert.True(startedIndex >= 0);
            Assert.True(errorIndex > startedIndex);
        }

        [Fact]
        public async Task ExecuteQuery_ClientThrows_CallsOnErrorBeforeSucceeded()
        {
            // When the ExecuteStatementAsync call itself throws (network error, auth error, ...),
            // OnExecuteSucceeded must never fire — there is no statement id yet — and OnError
            // must carry the original exception forward.
            var observer = new RecordingObserver();
            var networkError = new HttpRequestException("connection refused");

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(networkError);

            using var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "SELECT 1";

            var ex = await Assert.ThrowsAsync<HttpRequestException>(
                () => stmt.ExecuteQueryAsync(CancellationToken.None));

            Assert.Same(networkError, ex);
            Assert.Same(networkError, observer.Error);
            Assert.DoesNotContain(nameof(IStatementOperationObserver.OnExecuteSucceeded), observer.Calls);
            Assert.Contains(nameof(IStatementOperationObserver.OnError), observer.Calls);
        }
    }
}
