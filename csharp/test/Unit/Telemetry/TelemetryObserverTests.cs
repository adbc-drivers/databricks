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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.Databricks.Telemetry.Proto;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Unit tests for <see cref="TelemetryObserver"/> verifying:
    /// <list type="bullet">
    ///   <item>Observer method calls propagate into the underlying <see cref="StatementTelemetryContext"/>.</item>
    ///   <item><c>OnFinalized</c> enqueues exactly one <see cref="TelemetryFrontendLog"/>.</item>
    ///   <item>The terminal call is idempotent under both serial and concurrent invocation.</item>
    ///   <item>All methods satisfy the fail-open contract even when the telemetry client throws.</item>
    /// </list>
    /// </summary>
    public class TelemetryObserverTests
    {
        // ── Test doubles ─────────────────────────────────────────────────────────────

        /// <summary>
        /// Records every enqueued log so tests can inspect the exact proto fields the
        /// observer attempted to emit, and counts enqueues to assert exactly-once semantics.
        /// </summary>
        private sealed class CapturingTelemetryClient : ITelemetryClient
        {
            public List<TelemetryFrontendLog> Logs { get; } = new List<TelemetryFrontendLog>();

            public int EnqueueCallCount;

            public void Enqueue(TelemetryFrontendLog log)
            {
                Interlocked.Increment(ref EnqueueCallCount);
                lock (Logs)
                {
                    Logs.Add(log);
                }
            }

            public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;

            public Task CloseAsync() => Task.CompletedTask;

            public ValueTask DisposeAsync() => default;
        }

        /// <summary>
        /// Simulates a corrupted / faulty telemetry client whose Enqueue path raises an
        /// exception. Used to assert the observer's fail-open guarantee.
        /// </summary>
        private sealed class ThrowingTelemetryClient : ITelemetryClient
        {
            public int EnqueueCallCount;

            public void Enqueue(TelemetryFrontendLog log)
            {
                Interlocked.Increment(ref EnqueueCallCount);
                throw new InvalidOperationException("simulated telemetry client failure");
            }

            public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;

            public Task CloseAsync() => Task.CompletedTask;

            public ValueTask DisposeAsync() => default;
        }

        // ── Fixtures ─────────────────────────────────────────────────────────────────

        private static TelemetrySessionContext CreateSession(ITelemetryClient client)
        {
            return new TelemetrySessionContext
            {
                SessionId = "session-abc",
                WorkspaceId = 4242L,
                TelemetryClient = client,
                AuthType = "pat",
                SystemConfiguration = new DriverSystemConfiguration { DriverVersion = "1.0.0" },
                DriverConnectionParams = new DriverConnectionParameters { HttpPath = "/sql/1.0/wh/x" },
            };
        }

        private static (TelemetryObserver observer, CapturingTelemetryClient client) CreateObserver()
        {
            CapturingTelemetryClient client = new CapturingTelemetryClient();
            TelemetrySessionContext session = CreateSession(client);
            TelemetryObserver observer = new TelemetryObserver(session);
            return (observer, client);
        }

        // ── Required tests (per task description) ────────────────────────────────────

        [Fact]
        public void TelemetryObserver_OnExecuteStarted_PopulatesContext()
        {
            // Arrange
            (TelemetryObserver observer, _) = CreateObserver();

            // Act
            observer.OnExecuteStarted(StatementType.Query, OperationType.ExecuteStatement, isCompressed: true);

            // Assert: scalar fields land on the underlying context.
            Assert.Equal(StatementType.Query, observer.Context.StatementType);
            Assert.Equal(OperationType.ExecuteStatement, observer.Context.OperationType);
            Assert.True(observer.Context.IsCompressed);
        }

        [Fact]
        public void TelemetryObserver_OnExecuteSucceeded_RecordsStatementId()
        {
            // Arrange
            (TelemetryObserver observer, _) = CreateObserver();

            // Act
            observer.OnExecuteSucceeded("stmt-id-42", ExecutionResultFormat.ExternalLinks);

            // Assert
            Assert.Equal("stmt-id-42", observer.Context.StatementId);
            Assert.Equal(ExecutionResultFormat.ExternalLinks, observer.Context.ResultFormat);
        }

        [Fact]
        public void TelemetryObserver_OnFinalized_EnqueuesExactlyOnce()
        {
            // Arrange
            (TelemetryObserver observer, CapturingTelemetryClient client) = CreateObserver();
            observer.OnExecuteStarted(StatementType.Query, OperationType.ExecuteStatement, isCompressed: false);
            observer.OnExecuteSucceeded("stmt-1", ExecutionResultFormat.InlineArrow);

            // Act
            observer.OnFinalized();

            // Assert
            Assert.Equal(1, client.EnqueueCallCount);
            Assert.Single(client.Logs);

            // The emitted log must reflect the recorded context.
            OssSqlDriverTelemetryLog log = client.Logs[0].Entry!.SqlDriverLog!;
            Assert.Equal("session-abc", log.SessionId);
            Assert.Equal("stmt-1", log.SqlStatementId);
            Assert.Equal(StatementType.Query, log.SqlOperation.StatementType);
            Assert.Equal(OperationType.ExecuteStatement, log.SqlOperation.OperationDetail.OperationType);
            Assert.Null(log.ErrorInfo);

            // Workspace id and frontend envelope must be populated.
            Assert.Equal(4242L, client.Logs[0].WorkspaceId);
            Assert.False(string.IsNullOrEmpty(client.Logs[0].FrontendLogEventId));
            Assert.NotNull(client.Logs[0].Context);
            Assert.True(client.Logs[0].Context!.TimestampMillis > 0);
        }

        [Fact]
        public void TelemetryObserver_OnFinalized_CalledTwice_EnqueuesOnce()
        {
            // Arrange
            (TelemetryObserver observer, CapturingTelemetryClient client) = CreateObserver();

            // Act: invoke OnFinalized twice in serial (mirrors error + dispose paths).
            observer.OnFinalized();
            observer.OnFinalized();

            // Assert: exactly one enqueue.
            Assert.Equal(1, client.EnqueueCallCount);
            Assert.True(observer.HasEmitted);
        }

        [Fact]
        public async Task TelemetryObserver_OnFinalized_ConcurrentCalls_EnqueueOnce()
        {
            // Arrange
            (TelemetryObserver observer, CapturingTelemetryClient client) = CreateObserver();

            // Act: race many threads to OnFinalized; only one must win.
            const int parallelism = 32;
            using ManualResetEventSlim start = new ManualResetEventSlim();
            Task[] tasks = new Task[parallelism];
            for (int i = 0; i < parallelism; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    start.Wait();
                    observer.OnFinalized();
                });
            }
            start.Set();
            await Task.WhenAll(tasks);

            // Assert
            Assert.Equal(1, client.EnqueueCallCount);
        }

        [Fact]
        public void TelemetryObserver_OnError_RecordsErrorAndFinalizes()
        {
            // Arrange
            (TelemetryObserver observer, CapturingTelemetryClient client) = CreateObserver();
            observer.OnExecuteStarted(StatementType.Query, OperationType.ExecuteStatement, isCompressed: false);
            InvalidOperationException error = new InvalidOperationException("simulated query failure");

            // Act
            observer.OnError(error);
            observer.OnFinalized();
            observer.OnFinalized(); // idempotent: should not double-emit even with error path

            // Assert: exactly one log, error_info populated.
            Assert.Equal(1, client.EnqueueCallCount);
            OssSqlDriverTelemetryLog log = client.Logs[0].Entry!.SqlDriverLog!;
            Assert.NotNull(log.ErrorInfo);
            Assert.Equal("InvalidOperationException", log.ErrorInfo.ErrorName);
            Assert.True(observer.Context.HasError);
            Assert.Equal("simulated query failure", observer.Context.ErrorMessage);
        }

        [Fact]
        public void TelemetryObserver_AllMethods_NeverThrow_WhenContextCorrupted()
        {
            // Arrange: build an observer whose telemetry client throws on every Enqueue
            // (this simulates a corrupted downstream dependency). The observer must
            // continue to absorb all calls without re-throwing.
            ThrowingTelemetryClient throwing = new ThrowingTelemetryClient();
            TelemetrySessionContext session = CreateSession(throwing);
            TelemetryObserver observer = new TelemetryObserver(session);

            // Act + Assert: exercise the entire surface, including pathological inputs
            // (null statementId, null exception, null ChunkMetrics).
            Exception? captured = Record.Exception(() =>
            {
                observer.OnExecuteStarted(StatementType.Query, OperationType.ExecuteStatement, isCompressed: true);
                observer.OnExecuteSucceeded(null!, ExecutionResultFormat.Unspecified);
                observer.OnPollCompleted(count: 0, latencyMs: 0);
                observer.OnFirstBatchReady(latencyMs: -1);
                observer.OnConsumed(latencyMs: -1);
                observer.OnChunksDownloaded(null!);
                observer.OnError(null!);
                observer.OnError(new InvalidOperationException("boom"));
                observer.OnFinalized();
                observer.OnFinalized();
            });

            Assert.Null(captured);

            // The throwing client must have been invoked exactly once (idempotent finalize)
            // and the observer must have swallowed its exception.
            Assert.Equal(1, throwing.EnqueueCallCount);
            Assert.True(observer.HasEmitted);
        }

        [Fact]
        public void TelemetryObserver_OnChunksDownloaded_MergesIntoChunkDetails()
        {
            // Arrange
            (TelemetryObserver observer, CapturingTelemetryClient client) = CreateObserver();
            ChunkMetrics metrics = new ChunkMetrics
            {
                TotalChunksPresent = 12,
                TotalChunksIterated = 11,
                InitialChunkLatencyMs = 75,
                SlowestChunkLatencyMs = 220,
                SumChunksDownloadTimeMs = 1430,
            };

            // Act
            observer.OnExecuteStarted(StatementType.Query, OperationType.ExecuteStatement, isCompressed: false);
            observer.OnChunksDownloaded(metrics);
            observer.OnFinalized();

            // Assert: context absorbed the metrics.
            Assert.Equal(12, observer.Context.TotalChunksPresent);
            Assert.Equal(11, observer.Context.TotalChunksIterated);
            Assert.Equal(75, observer.Context.InitialChunkLatencyMs);
            Assert.Equal(220, observer.Context.SlowestChunkLatencyMs);
            Assert.Equal(1430, observer.Context.SumChunksDownloadTimeMs);

            // The chunk_details proto block on the emitted log mirrors the input.
            ChunkDetails details = client.Logs[0].Entry!.SqlDriverLog!.SqlOperation.ChunkDetails;
            Assert.NotNull(details);
            Assert.Equal(12, details.TotalChunksPresent);
            Assert.Equal(11, details.TotalChunksIterated);
            Assert.Equal(75, details.InitialChunkLatencyMillis);
            Assert.Equal(220, details.SlowestChunkLatencyMillis);
            Assert.Equal(1430, details.SumChunksDownloadTimeMillis);
        }

        // ── Additional coverage ──────────────────────────────────────────────────────

        [Fact]
        public void TelemetryObserver_Constructor_RejectsNullSession()
        {
            Assert.Throws<ArgumentNullException>(() => new TelemetryObserver(null!));
        }

        [Fact]
        public void TelemetryObserver_OnFinalized_WithNullTelemetryClient_IsNoOp()
        {
            // Arrange: session has no telemetry client (covers the disabled case).
            TelemetrySessionContext session = new TelemetrySessionContext
            {
                SessionId = "s1",
                WorkspaceId = 1L,
                TelemetryClient = null,
            };
            TelemetryObserver observer = new TelemetryObserver(session);

            // Act + Assert: must not throw, must mark itself emitted so a later call is a no-op.
            observer.OnFinalized();
            observer.OnFinalized();
            Assert.True(observer.HasEmitted);
        }

        [Fact]
        public void TelemetryObserver_OnFirstBatchReady_OnlyFirstCallWins()
        {
            // Arrange
            (TelemetryObserver observer, _) = CreateObserver();

            // Act
            observer.OnFirstBatchReady(latencyMs: 50);
            observer.OnFirstBatchReady(latencyMs: 999);

            // Assert: subsequent calls do not overwrite the earliest observed latency.
            Assert.Equal(50, observer.Context.FirstBatchReadyMs);
        }

        [Fact]
        public void TelemetryObserver_OnFinalized_ThenLifecycleCalls_AreNoOps()
        {
            // Arrange: drive the observer to a finalized state with a known context snapshot.
            (TelemetryObserver observer, CapturingTelemetryClient client) = CreateObserver();
            observer.OnExecuteStarted(StatementType.Query, OperationType.ExecuteStatement, isCompressed: true);
            observer.OnExecuteSucceeded("stmt-pre-final", ExecutionResultFormat.InlineArrow);
            observer.OnFinalized();

            Assert.Equal(1, client.EnqueueCallCount);
            Assert.True(observer.HasEmitted);

            // Capture the post-finalize context fingerprint so we can prove subsequent
            // lifecycle calls do not mutate state.
            string? statementIdAfterFinal = observer.Context.StatementId;
            StatementType stmtTypeAfterFinal = observer.Context.StatementType;
            ExecutionResultFormat resultFormatAfterFinal = observer.Context.ResultFormat;
            bool isCompressedAfterFinal = observer.Context.IsCompressed;
            int? pollCountAfterFinal = observer.Context.PollCount;
            long? consumedAfterFinal = observer.Context.ResultsConsumedMs;
            bool hasErrorAfterFinal = observer.Context.HasError;

            // Act: call every non-terminal lifecycle method with different values that
            // would visibly mutate the context if the gate were missing.
            observer.OnExecuteStarted(StatementType.Update, OperationType.ExecuteStatementAsync, isCompressed: false);
            observer.OnExecuteSucceeded("stmt-after-final-must-not-stick", ExecutionResultFormat.ExternalLinks);
            observer.OnPollCompleted(count: 99, latencyMs: 9999);
            observer.OnFirstBatchReady(latencyMs: 7777);
            observer.OnConsumed(latencyMs: 8888);
            observer.OnChunksDownloaded(new ChunkMetrics { TotalChunksPresent = 42, TotalChunksIterated = 42 });
            observer.OnError(new InvalidOperationException("post-finalize error"));

            // Second OnFinalized must also be a no-op.
            observer.OnFinalized();

            // Assert: no second enqueue, no observable mutation.
            Assert.Equal(1, client.EnqueueCallCount);
            Assert.Equal(statementIdAfterFinal, observer.Context.StatementId);
            Assert.Equal(stmtTypeAfterFinal, observer.Context.StatementType);
            Assert.Equal(resultFormatAfterFinal, observer.Context.ResultFormat);
            Assert.Equal(isCompressedAfterFinal, observer.Context.IsCompressed);
            Assert.Equal(pollCountAfterFinal, observer.Context.PollCount);
            Assert.Equal(consumedAfterFinal, observer.Context.ResultsConsumedMs);
            Assert.Equal(hasErrorAfterFinal, observer.Context.HasError);
            Assert.Null(observer.Context.TotalChunksPresent);
        }

        [Fact]
        public void TelemetryObserver_OnPollCompleted_StoresCountAndLatency()
        {
            // Arrange
            (TelemetryObserver observer, CapturingTelemetryClient client) = CreateObserver();

            // Act
            observer.OnExecuteStarted(StatementType.Query, OperationType.ExecuteStatementAsync, isCompressed: false);
            observer.OnPollCompleted(count: 5, latencyMs: 250);
            observer.OnFinalized();

            // Assert
            Assert.Equal(5, observer.Context.PollCount);
            Assert.Equal(250, observer.Context.PollLatencyMs);
            OperationDetail detail = client.Logs[0].Entry!.SqlDriverLog!.SqlOperation.OperationDetail;
            Assert.Equal(5, detail.NOperationStatusCalls);
            Assert.Equal(250, detail.OperationStatusLatencyMillis);
        }
    }
}
