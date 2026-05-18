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
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
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

            public Action<long>? OnFirstBatchReadyCallback;
            public void OnFirstBatchReady(long latencyMs)
            {
                OnFirstBatchReadyCallback?.Invoke(latencyMs);
                Calls.Add(nameof(OnFirstBatchReady));
            }
            public Action<long>? OnConsumedCallback;
            public void OnConsumed(long latencyMs)
            {
                OnConsumedCallback?.Invoke(latencyMs);
                Calls.Add(nameof(OnConsumed));
            }
            public ChunkMetrics? CapturedChunkMetrics;
            public void OnChunksDownloaded(ChunkMetrics metrics)
            {
                CapturedChunkMetrics = metrics;
                Calls.Add(nameof(OnChunksDownloaded));
            }

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

            // Non-metadata path: stmtType is Query, opType is ExecuteStatementAsync. SEA is
            // always async on the wire (submit + poll), so the operation_type recorded in
            // telemetry must be EXECUTE_STATEMENT_ASYNC, distinct from the synchronous
            // EXECUTE_STATEMENT that the Thrift path (DatabricksStatement) emits.
            Assert.Equal(StatementType.Query, observer.ExecuteStartedStmtType);
            Assert.Equal(OperationType.ExecuteStatementAsync, observer.ExecuteStartedOpType);
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
            // statement id is known, carrying that id forward to the observer. The result format
            // is derived by SeaResultFormatMapper from (disposition, format, response): with
            // disposition=INLINE_OR_EXTERNAL_LINKS and a manifest carrying no external_links,
            // this maps to InlineArrow (the auto-disposition + inline-attachment cell of §8).
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
            // SeaResultFormatMapper now populates a real value; gap-2 verifies the callsite
            // no longer passes the Unspecified placeholder.
            Assert.NotNull(observer.ExecuteSucceededFormat);
            Assert.NotEqual(ExecutionResultFormat.Unspecified, observer.ExecuteSucceededFormat);
            Assert.Equal(ExecutionResultFormat.InlineArrow, observer.ExecuteSucceededFormat);

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

        [Fact]
        public async Task Dispose_AfterSuccessfulExecute_CallsOnFinalizedExactlyOnce()
        {
            // OnFinalized is the terminal observer signal — it is the only path that builds an
            // OssSqlDriverTelemetryLog and enqueues it for export. After a successful execute,
            // Dispose must fire OnFinalized exactly once so SEA telemetry actually reaches
            // eng_lumberjack. Without this call every other hookpoint just mutates an in-memory
            // context that is garbage-collected on dispose.
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
            // Stub CloseStatementAsync so Dispose's awaited Task does not NRE; the production
            // dispose path swallows close errors but we want the observer call to be the only
            // assertable side-effect of dispose here.
            mockClient
                .Setup(c => c.CloseStatementAsync(
                    It.IsAny<string>(),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "SELECT 1";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            // Pre-dispose: OnFinalized must not have fired yet — production code defers it to
            // Dispose so chunk-metrics / consumed-time can be captured from the reader first.
            Assert.DoesNotContain(nameof(IStatementOperationObserver.OnFinalized), observer.Calls);

            stmt.Dispose();

            int finalizeCalls = observer.Calls.Count(c => c == nameof(IStatementOperationObserver.OnFinalized));
            Assert.Equal(1, finalizeCalls);
            // OnFinalized must be the last observer call: anything after it would mutate an
            // already-emitted log and never reach the wire.
            Assert.Equal(nameof(IStatementOperationObserver.OnFinalized), observer.Calls[observer.Calls.Count - 1]);
        }

        [Fact]
        public async Task Dispose_AfterErrorPath_CallsOnFinalizedOnce()
        {
            // Error path: ExecuteQueryInternalAsync's catch fired OnError. Dispose must still
            // fire OnFinalized so the error log reaches eng_lumberjack — without this call the
            // error case produces no telemetry at all. The TelemetryObserver enforces exactly-
            // once finalize via Interlocked.CompareExchange, so even if a future hookpoint adds
            // its own finalize call on the error path, the dispose-time call here remains
            // idempotent against the real observer; this test asserts the recorder sees a
            // single Dispose-driven OnFinalized.
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
            mockClient
                .Setup(c => c.CloseStatementAsync(
                    It.IsAny<string>(),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "NOT VALID SQL";

            await Assert.ThrowsAsync<AdbcException>(() => stmt.ExecuteQueryAsync(CancellationToken.None));

            // Sanity: error path fired OnError but not OnFinalized — the latter is dispose-driven.
            Assert.Contains(nameof(IStatementOperationObserver.OnError), observer.Calls);
            Assert.DoesNotContain(nameof(IStatementOperationObserver.OnFinalized), observer.Calls);

            stmt.Dispose();

            int finalizeCalls = observer.Calls.Count(c => c == nameof(IStatementOperationObserver.OnFinalized));
            Assert.Equal(1, finalizeCalls);
            // OnError must precede OnFinalized so the terminal log reflects the failure state.
            int errorIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnError));
            int finalizeIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnFinalized));
            Assert.True(errorIndex >= 0);
            Assert.True(finalizeIndex > errorIndex);
        }

        [Fact]
        public async Task ExecuteQuery_InlinePath_CallsOnFirstBatchReady_OnceWithNonNegativeLatency()
        {
            // OnFirstBatchReady is wired at reader construction (gap G3 / design §6 row 4). For the
            // inline path the signal fires once chunk-0 attachment bytes are already in the response
            // — i.e. immediately before the InlineArrowStreamReader ctor — carrying elapsed-since-
            // execute-start as latencyMs. This test pins:
            //   1. exactly-once invocation,
            //   2. non-negative latency (wall-clock; zero is valid on fast in-process mocks),
            //   3. ordering between OnExecuteSucceeded and OnFirstBatchReady (server must accept the
            //      statement before first batch can be "ready").
            var observer = new RecordingObserver();
            var (ipcBytes, manifest) = BuildSingleColumnInlineArrowResult();

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = manifest,
                    Result = new ResultData { Attachment = ipcBytes },
                });

            long? capturedLatency = null;
            observer.OnFirstBatchReadyCallback = ms => capturedLatency = ms;

            using var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "SELECT 1";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);
            // Dispose the returned stream so we don't leak the underlying ArrowStreamReader.
            result.Stream?.Dispose();

            int firstBatchCallCount = observer.Calls
                .Count(c => c == nameof(IStatementOperationObserver.OnFirstBatchReady));
            Assert.Equal(1, firstBatchCallCount);

            // Non-negative wall-clock: anything else indicates a stopwatch wiring bug
            // (e.g. read before Start()).
            Assert.NotNull(capturedLatency);
            Assert.True(capturedLatency >= 0,
                $"OnFirstBatchReady latency must be non-negative, got {capturedLatency}.");

            // OnExecuteSucceeded must precede OnFirstBatchReady — the statement is accepted by the
            // server first, then results become available. Reversing this order would imply we are
            // reporting first-batch latency for a statement the server hasn't acknowledged.
            int succeededIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnExecuteSucceeded));
            int firstBatchIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnFirstBatchReady));
            Assert.True(succeededIndex >= 0);
            Assert.True(firstBatchIndex > succeededIndex);
        }

        [Fact]
        public async Task ExecuteQuery_CloudFetchPath_CallsOnFirstBatchReady_OnceWithNonNegativeLatency()
        {
            // CloudFetch counterpart to the inline test above. With Manifest.Chunks[0].ExternalLinks
            // populated, CreateReader routes through CreateCloudFetchReader, which fires
            // OnFirstBatchReady at the top of the method before invoking the factory. We don't drive
            // the download itself (the factory's HTTP calls go through a mocked handler) — the test
            // only pins the observer wiring at reader construction.
            var observer = new RecordingObserver();
            long? capturedLatency = null;
            observer.OnFirstBatchReadyCallback = ms => capturedLatency = ms;

            var manifest = new ResultManifest
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
                TotalChunkCount = 1,
                Chunks = new List<ResultChunk>
                {
                    new()
                    {
                        ChunkIndex = 0,
                        RowCount = 0,
                        RowOffset = 0,
                        ByteCount = 0,
                        ExternalLinks = new List<ExternalLink>
                        {
                            // URL is not actually downloaded by this test: the CloudFetch factory
                            // queues a background fetch through the mocked HttpClient. Dispose
                            // cancels any in-flight work.
                            new() { ExternalLinkUrl = "https://example.invalid/chunk0" }
                        }
                    }
                },
            };

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = manifest,
                    Result = new ResultData
                    {
                        ExternalLinks = new List<ExternalLink>
                        {
                            new() { ExternalLinkUrl = "https://example.invalid/chunk0" }
                        }
                    },
                });

            using var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "SELECT 1";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);
            // Dispose the returned stream so the background download manager shuts down cleanly
            // without leaving tasks attempting to hit example.invalid.
            result.Stream?.Dispose();

            int firstBatchCallCount = observer.Calls
                .Count(c => c == nameof(IStatementOperationObserver.OnFirstBatchReady));
            Assert.Equal(1, firstBatchCallCount);

            Assert.NotNull(capturedLatency);
            Assert.True(capturedLatency >= 0,
                $"OnFirstBatchReady latency must be non-negative, got {capturedLatency}.");

            int succeededIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnExecuteSucceeded));
            int firstBatchIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnFirstBatchReady));
            Assert.True(succeededIndex >= 0);
            Assert.True(firstBatchIndex > succeededIndex);
        }

        [Fact]
        public async Task ExecuteQuery_InlinePath_ReaderDispose_CallsOnConsumed_OnceWithLatencyAtLeastFirstBatchReady()
        {
            // OnConsumed is wired at the outermost reader-decorator Dispose (gap G3 / design §6 row 5).
            // For the inline path this fires when the consumer disposes the IArrowArrayStream returned
            // by ExecuteQuery. This test pins:
            //   1. exactly-once invocation (idempotent on repeated Dispose),
            //   2. latency monotonicity: OnConsumed latency >= OnFirstBatchReady latency, because both
            //      read the same execute-time Stopwatch and Dispose strictly follows reader construction,
            //   3. ordering: OnConsumed fires after OnFirstBatchReady (i.e. reader construction precedes
            //      consumption end).
            var observer = new RecordingObserver();
            var (ipcBytes, manifest) = BuildSingleColumnInlineArrowResult();

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = manifest,
                    Result = new ResultData { Attachment = ipcBytes },
                });

            long? firstBatchLatency = null;
            long? consumedLatency = null;
            observer.OnFirstBatchReadyCallback = ms => firstBatchLatency = ms;
            observer.OnConsumedCallback = ms => consumedLatency = ms;

            using var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "SELECT 1";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);

            // Before Dispose, OnConsumed must NOT have fired — the consumer has not signaled
            // end-of-consumption yet. Asserting absence here guards against a wiring bug that
            // would fire OnConsumed at reader construction (effectively duplicating
            // OnFirstBatchReady).
            Assert.DoesNotContain(nameof(IStatementOperationObserver.OnConsumed), observer.Calls);

            // First Dispose triggers OnConsumed; second Dispose must be a no-op (idempotency).
            result.Stream?.Dispose();
            result.Stream?.Dispose();

            int consumedCallCount = observer.Calls
                .Count(c => c == nameof(IStatementOperationObserver.OnConsumed));
            Assert.Equal(1, consumedCallCount);

            Assert.NotNull(firstBatchLatency);
            Assert.NotNull(consumedLatency);
            Assert.True(consumedLatency >= firstBatchLatency,
                $"OnConsumed latency ({consumedLatency}) must be >= OnFirstBatchReady latency ({firstBatchLatency}).");

            // OnFirstBatchReady must precede OnConsumed: the reader can't be consumed before
            // it exists. Reversing this order would imply Dispose ran before construction.
            int firstBatchIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnFirstBatchReady));
            int consumedIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnConsumed));
            Assert.True(firstBatchIndex >= 0);
            Assert.True(consumedIndex > firstBatchIndex);
        }

        [Fact]
        public async Task ExecuteQuery_CloudFetchPath_ReaderDispose_CallsOnConsumed_OnceWithLatencyAtLeastFirstBatchReady()
        {
            // CloudFetch counterpart to the inline test above. The outermost ConsumptionObservingStream
            // wraps the CloudFetchReader, so the consumer's Dispose still drives OnConsumed even though
            // the inner reader's actual download work is async — we don't need to wait for chunk fetches
            // to complete to validate the observer wiring.
            var observer = new RecordingObserver();
            long? firstBatchLatency = null;
            long? consumedLatency = null;
            observer.OnFirstBatchReadyCallback = ms => firstBatchLatency = ms;
            observer.OnConsumedCallback = ms => consumedLatency = ms;

            var manifest = new ResultManifest
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
                TotalChunkCount = 1,
                Chunks = new List<ResultChunk>
                {
                    new()
                    {
                        ChunkIndex = 0,
                        RowCount = 0,
                        RowOffset = 0,
                        ByteCount = 0,
                        ExternalLinks = new List<ExternalLink>
                        {
                            // URL is not actually downloaded — Dispose cancels any in-flight work.
                            new() { ExternalLinkUrl = "https://example.invalid/chunk0" }
                        }
                    }
                },
            };

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = manifest,
                    Result = new ResultData
                    {
                        ExternalLinks = new List<ExternalLink>
                        {
                            new() { ExternalLinkUrl = "https://example.invalid/chunk0" }
                        }
                    },
                });

            using var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "SELECT 1";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);

            // Pre-Dispose absence: same rationale as the inline test — guards against firing
            // OnConsumed at construction time.
            Assert.DoesNotContain(nameof(IStatementOperationObserver.OnConsumed), observer.Calls);

            // Idempotent Dispose. The second call must not produce a second OnConsumed,
            // otherwise downstream telemetry would double-count consumption latency for a
            // consumer that defensively disposes multiple times.
            result.Stream?.Dispose();
            result.Stream?.Dispose();

            int consumedCallCount = observer.Calls
                .Count(c => c == nameof(IStatementOperationObserver.OnConsumed));
            Assert.Equal(1, consumedCallCount);

            Assert.NotNull(firstBatchLatency);
            Assert.NotNull(consumedLatency);
            Assert.True(consumedLatency >= firstBatchLatency,
                $"OnConsumed latency ({consumedLatency}) must be >= OnFirstBatchReady latency ({firstBatchLatency}).");

            int firstBatchIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnFirstBatchReady));
            int consumedIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnConsumed));
            Assert.True(firstBatchIndex >= 0);
            Assert.True(consumedIndex > firstBatchIndex);
        }

        [Fact]
        public async Task ExecuteQuery_CloudFetchPath_ReaderDispose_CallsOnChunksDownloaded_OnceWithNonNullMetrics()
        {
            // gap G3 / design §6 row 5–6: on the CloudFetch path, ConsumptionObservingStream
            // must signal OnChunksDownloaded exactly once when the consumer disposes the result
            // stream. This is the SEA counterpart to DatabricksStatement.FinalizeExecuteTelemetry's
            // Thrift-side emission, and it shares the same observer downstream — the
            // OssSqlDriverTelemetryLog's chunk_details field comes from this call.
            //
            // The test deliberately does NOT drive an actual chunk download (example.invalid never
            // resolves), so the captured ChunkMetrics will be the aggregator's default/empty state.
            // The contract is "fire exactly once with a non-null ChunkMetrics", not "fire only when
            // we successfully downloaded chunks" — the proto fields are nullable so an empty
            // ChunkMetrics is a valid wire payload, and dropping the signal here would silently
            // omit the field for any CloudFetch query that the consumer disposes early.
            var observer = new RecordingObserver();

            var manifest = new ResultManifest
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
                TotalChunkCount = 1,
                Chunks = new List<ResultChunk>
                {
                    new()
                    {
                        ChunkIndex = 0,
                        RowCount = 0,
                        RowOffset = 0,
                        ByteCount = 0,
                        ExternalLinks = new List<ExternalLink>
                        {
                            new() { ExternalLinkUrl = "https://example.invalid/chunk0" }
                        }
                    }
                },
            };

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = manifest,
                    Result = new ResultData
                    {
                        ExternalLinks = new List<ExternalLink>
                        {
                            new() { ExternalLinkUrl = "https://example.invalid/chunk0" }
                        }
                    },
                });

            using var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "SELECT 1";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);

            // Pre-Dispose: OnChunksDownloaded must not have fired yet — the wrapper defers it to
            // consumer Dispose so the aggregator has a chance to accumulate chunk timings.
            Assert.DoesNotContain(nameof(IStatementOperationObserver.OnChunksDownloaded), observer.Calls);

            // First Dispose fires OnChunksDownloaded; second Dispose must be a no-op (idempotency).
            // Double-firing would cause TelemetryObserver to overwrite the chunk_details on the
            // in-flight log, which is the right behavior in the singleton case but masks a wiring
            // bug if Dispose is called twice.
            result.Stream?.Dispose();
            result.Stream?.Dispose();

            int chunksDownloadedCallCount = observer.Calls
                .Count(c => c == nameof(IStatementOperationObserver.OnChunksDownloaded));
            Assert.Equal(1, chunksDownloadedCallCount);

            // The captured metrics must be a non-null ChunkMetrics — even when no chunks were
            // actually downloaded the wrapper falls back to a fresh ChunkMetrics rather than
            // dropping the signal. Pinning non-null guards against a wiring regression that
            // would pass `default(ChunkMetrics)` (== null for a reference type) and silently
            // null-coalesce on the receiving end.
            Assert.NotNull(observer.CapturedChunkMetrics);

            // Emission order vs. OnConsumed: ConsumptionObservingStream emits OnChunksDownloaded
            // BEFORE OnConsumed to match the Thrift path's emission order in
            // DatabricksStatement.FinalizeExecuteTelemetry. Both end up on the same telemetry log
            // regardless, but pinning the order here keeps SEA and Thrift behaviorally identical.
            int chunksIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnChunksDownloaded));
            int consumedIndex = observer.Calls.IndexOf(nameof(IStatementOperationObserver.OnConsumed));
            Assert.True(chunksIndex >= 0);
            Assert.True(consumedIndex > chunksIndex,
                $"OnChunksDownloaded (index {chunksIndex}) must precede OnConsumed (index {consumedIndex}).");
        }

        [Fact]
        public async Task ExecuteQuery_CloudFetchPath_ReaderDispose_FiresOnChunksDownloaded_WithEmptyMetricsWhenAggregatorEmpty()
        {
            // Gap-fix dependency fallback (gap G3 / design §6 row 6): if the ChunkMetrics
            // aggregator is unavailable or empty at Dispose time, ConsumptionObservingStream
            // must still fire OnChunksDownloaded with a non-null, default-valued ChunkMetrics
            // rather than throw or skip the signal. The design's stated fallback contract is
            // "pass ChunkMetrics.Empty" — operationally that's a fresh `new ChunkMetrics()`,
            // which is what CloudFetchReader.GetChunkMetrics returns when its download manager
            // has been disposed and no _cachedChunkMetrics was captured.
            //
            // Scenario covered: consumer disposes the result stream before any chunk has
            // actually been iterated by the reader. The CloudFetchDownloader aggregator state
            // is still at its initial defaults (TotalChunksIterated=0, etc.) — i.e. effectively
            // "no metrics yet" — and the wrapper must tolerate that without throwing.
            var observer = new RecordingObserver();

            var manifest = new ResultManifest
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
                TotalChunkCount = 1,
                Chunks = new List<ResultChunk>
                {
                    new()
                    {
                        ChunkIndex = 0,
                        RowCount = 0,
                        RowOffset = 0,
                        ByteCount = 0,
                        ExternalLinks = new List<ExternalLink>
                        {
                            new() { ExternalLinkUrl = "https://example.invalid/chunk0" }
                        }
                    }
                },
            };

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = manifest,
                    Result = new ResultData
                    {
                        ExternalLinks = new List<ExternalLink>
                        {
                            new() { ExternalLinkUrl = "https://example.invalid/chunk0" }
                        }
                    },
                });

            using var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "SELECT 1";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);

            // The fail-open contract is asserted by NOT wrapping this Dispose in a try/catch:
            // if the wrapper or the aggregator throws synchronously, the test fails outright.
            // The wrapper internally catches aggregator exceptions and substitutes a fresh
            // ChunkMetrics, but the no-chunks-iterated path here exercises the more common
            // "default values" branch of that fallback.
            result.Stream?.Dispose();

            Assert.Contains(nameof(IStatementOperationObserver.OnChunksDownloaded), observer.Calls);

            // The metrics object must be non-null. We do not assert specific field values
            // because the aggregator may report partial state (e.g. TotalChunksPresent=1 for the
            // queued-but-not-downloaded chunk). The contract this test pins is "non-null,
            // non-throwing fallback", not "exactly default-constructed".
            Assert.NotNull(observer.CapturedChunkMetrics);
        }

        [Fact]
        public async Task ExecuteQuery_InlinePath_ReaderDispose_DoesNotCallOnChunksDownloaded()
        {
            // gap G3 / design §6 row 6: OnChunksDownloaded is a CloudFetch-only signal — the
            // chunk_details proto field has no meaning for inline results (there are no chunks
            // to download). The wrapper achieves this by accepting a nullable CloudFetchReader
            // argument and only firing OnChunksDownloaded when it is non-null. The inline path
            // returns an InlineArrowStreamReader, so the `as CloudFetchReader` cast at the
            // callsite produces null and the signal is skipped.
            //
            // Firing OnChunksDownloaded on the inline path would silently emit chunk_details with
            // all-zero values, which is indistinguishable on the wire from "CloudFetch query that
            // downloaded zero chunks" — a real and useful signal. Mixing the two would make the
            // telemetry field useless for downstream analysis.
            var observer = new RecordingObserver();
            var (ipcBytes, manifest) = BuildSingleColumnInlineArrowResult();

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = manifest,
                    Result = new ResultData { Attachment = ipcBytes },
                });

            using var stmt = CreateStatement(mockClient.Object, observer);
            stmt.SqlQuery = "SELECT 1";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);

            // Dispose the result stream to drive the wrapper's OnConsumed call — that one must
            // still fire on the inline path. We rely on it firing as the witness that Dispose
            // actually ran the wrapper (otherwise the absence of OnChunksDownloaded would be a
            // false negative caused by Dispose never being invoked at all).
            result.Stream?.Dispose();

            Assert.Contains(nameof(IStatementOperationObserver.OnConsumed), observer.Calls);
            Assert.DoesNotContain(nameof(IStatementOperationObserver.OnChunksDownloaded), observer.Calls);
            Assert.Null(observer.CapturedChunkMetrics);
        }

        /// <summary>
        /// Builds a single-column ("c0", INT) inline result: a manifest + matching Arrow IPC stream
        /// bytes. InlineArrowStreamReader cross-validates that the manifest schema and the IPC
        /// embedded schema have the same field count (count mismatches throw; type mismatches are
        /// expected), so the manifest column count and the writer's schema column count must agree.
        /// </summary>
        private static (byte[] ipcBytes, ResultManifest manifest) BuildSingleColumnInlineArrowResult()
        {
            var ipcSchema = new Schema.Builder()
                .Field(new Field("c0", Int32Type.Default, nullable: true))
                .Build();

            using var ms = new MemoryStream();
            using (var writer = new ArrowStreamWriter(ms, ipcSchema, leaveOpen: true))
            {
                // A single empty record batch is sufficient: the test exercises reader construction
                // and observer wiring, not data correctness. RecordBatch requires at least one array,
                // so we pass an empty Int32Array with length 0.
                var emptyArray = new Int32Array.Builder().Build();
                var batch = new RecordBatch(ipcSchema, new IArrowArray[] { emptyArray }, 0);
                writer.WriteRecordBatch(batch);
                writer.WriteEnd();
            }
            var ipcBytes = ms.ToArray();

            var manifest = new ResultManifest
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
                TotalChunkCount = 1,
                Chunks = new List<ResultChunk>
                {
                    // Single-chunk inline: chunk count of 1 means InlineArrowStreamReader.FetchAllChunksAsync
                    // does not loop to fetch additional chunks via GetResultChunkAsync.
                    new()
                    {
                        ChunkIndex = 0,
                        RowCount = 0,
                        RowOffset = 0,
                        ByteCount = ipcBytes.Length,
                    }
                },
            };

            return (ipcBytes, manifest);
        }

        [Fact]
        public void Dispose_WithoutExecute_DoesNotCallOnFinalized()
        {
            // A statement that was never executed must not trigger OnFinalized() — doing so
            // would enqueue an empty execute-statement log with no statement id, no operation
            // type, and no latencies. The gate is the _executeStarted flag set in lockstep
            // with OnExecuteStarted; without it, every short-lived statement (e.g. a caller
            // that constructs a statement and then bails before SetSqlQuery) would pollute
            // eng_lumberjack.
            var observer = new RecordingObserver();
            var mockClient = new Mock<IStatementExecutionClient>();

            var stmt = CreateStatement(mockClient.Object, observer);

            stmt.Dispose();

            Assert.Empty(observer.Calls);
        }
    }
}
