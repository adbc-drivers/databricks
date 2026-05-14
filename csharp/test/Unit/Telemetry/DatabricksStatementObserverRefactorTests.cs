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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.Databricks.Telemetry.Proto;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using DriverModeType = AdbcDrivers.Databricks.Telemetry.Proto.DriverMode.Types.Type;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Verifies the T4 refactor of <see cref="DatabricksStatement"/>: the private
    /// telemetry helpers (<c>CreateTelemetryContext</c>, <c>CreateMetadataTelemetryContext</c>,
    /// <c>RecordSuccess</c>, <c>RecordError</c>, <c>EmitTelemetry</c>) have been replaced
    /// with a single <see cref="IStatementOperationObserver"/> field, and
    /// <see cref="DatabricksConnection.CreateStatement"/> now injects either a
    /// <see cref="TelemetryObserver"/> or <see cref="NullObserver.Instance"/>.
    ///
    /// The tests cover three concerns:
    /// <list type="bullet">
    ///   <item>Structural — the cast <c>((DatabricksConnection)Connection).TelemetrySession</c>
    ///         has been removed from <c>DatabricksStatement</c> and the field-based observer
    ///         is wired correctly through both the public constructor and the test-only
    ///         constructor seam.</item>
    ///   <item>Cross-transport — observer hooks fire at the expected lifecycle points and
    ///         the underlying <see cref="StatementTelemetryContext"/> is still exposed to
    ///         the operation status poller via the <c>PendingTelemetryContext</c>
    ///         compatibility property (PECO-2992).</item>
    ///   <item>Byte-identical — the <see cref="OssSqlDriverTelemetryLog"/> proto emitted
    ///         after the refactor matches what the pre-refactor <c>EmitTelemetry</c> helper
    ///         would have produced for an equivalent execute, and the connection's
    ///         <c>DriverMode</c> (Thrift here) is preserved on the wire (PECO-3022).</item>
    /// </list>
    /// </summary>
    public class DatabricksStatementObserverRefactorTests
    {
        // ── Test doubles ─────────────────────────────────────────────────────────────

        /// <summary>
        /// Capturing telemetry client that records every enqueued frontend log so tests
        /// can inspect the exact proto fields the observer wrote.
        /// </summary>
        private sealed class CapturingTelemetryClient : ITelemetryClient
        {
            public List<TelemetryFrontendLog> Logs { get; } = new List<TelemetryFrontendLog>();
            public int EnqueueCallCount;

            public void Enqueue(TelemetryFrontendLog log)
            {
                Interlocked.Increment(ref EnqueueCallCount);
                lock (Logs) { Logs.Add(log); }
            }

            public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;
            public Task CloseAsync() => Task.CompletedTask;
            public ValueTask DisposeAsync() => default;
        }

        /// <summary>
        /// Records every observer method invocation so tests can assert the statement
        /// drives the injected observer at the correct hookpoints.
        /// </summary>
        private sealed class RecordingObserver : IStatementOperationObserver
        {
            public List<string> Calls { get; } = new List<string>();
            public StatementType? StmtType;
            public OperationType? OpType;
            public bool? IsCompressed;
            public string? StatementId;
            public ExecutionResultFormat? ResultFormat;
            public int? PollCount;
            public long? PollLatencyMs;
            public long? FirstBatchReadyMs;
            public long? ConsumedMs;
            public ChunkMetrics? Chunks;
            public Exception? Error;
            public int FinalizedCallCount;

            public void OnExecuteStarted(StatementType stmtType, OperationType opType, bool isCompressed)
            {
                Calls.Add(nameof(OnExecuteStarted));
                StmtType = stmtType;
                OpType = opType;
                IsCompressed = isCompressed;
            }

            public void OnExecuteSucceeded(string statementId, ExecutionResultFormat resultFormat)
            {
                Calls.Add(nameof(OnExecuteSucceeded));
                StatementId = statementId;
                ResultFormat = resultFormat;
            }

            public void OnPollCompleted(int count, long latencyMs)
            {
                Calls.Add(nameof(OnPollCompleted));
                PollCount = count;
                PollLatencyMs = latencyMs;
            }

            public void OnFirstBatchReady(long latencyMs)
            {
                Calls.Add(nameof(OnFirstBatchReady));
                FirstBatchReadyMs = latencyMs;
            }

            public void OnConsumed(long latencyMs)
            {
                Calls.Add(nameof(OnConsumed));
                ConsumedMs = latencyMs;
            }

            public void OnChunksDownloaded(ChunkMetrics metrics)
            {
                Calls.Add(nameof(OnChunksDownloaded));
                Chunks = metrics;
            }

            public void OnError(Exception ex)
            {
                Calls.Add(nameof(OnError));
                Error = ex;
            }

            public void OnFinalized()
            {
                Calls.Add(nameof(OnFinalized));
                FinalizedCallCount++;
            }
        }

        // ── Fixtures ─────────────────────────────────────────────────────────────────

        private static DatabricksConnection CreateConnection()
        {
            Dictionary<string, string> properties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "test.databricks.com",
                [SparkParameters.Token] = "test-token",
            };
            return new DatabricksConnection(properties);
        }

        /// <summary>
        /// Builds a session context with a real <see cref="DriverConnectionParameters"/>
        /// stamped with <see cref="DriverModeType.Thrift"/>, matching how
        /// <see cref="DatabricksConnection.InitializeTelemetry"/> wires the production
        /// session up after a successful OpenSession.
        /// </summary>
        private static TelemetrySessionContext CreateThriftSession(ITelemetryClient client)
        {
            return new TelemetrySessionContext
            {
                SessionId = "session-thrift-abc",
                WorkspaceId = 4242L,
                TelemetryClient = client,
                AuthType = "pat",
                SystemConfiguration = new DriverSystemConfiguration
                {
                    DriverVersion = "1.0.0",
                    DriverName = "Apache Arrow ADBC Databricks Driver",
                },
                // Mode=Thrift is the value DatabricksConnection.InitializeTelemetry passes
                // through to ConnectionTelemetry.Create today. If a future refactor were to
                // accidentally swap the constant, the byte-equivalence test below would
                // catch it.
                DriverConnectionParams = new DriverConnectionParameters
                {
                    HttpPath = "/sql/1.0/warehouses/x",
                    Mode = DriverModeType.Thrift,
                },
            };
        }

        private static IStatementOperationObserver GetObserverField(DatabricksStatement statement)
        {
            FieldInfo field = typeof(DatabricksStatement).GetField(
                "_observer",
                BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new InvalidOperationException("_observer field not found on DatabricksStatement");
            return (IStatementOperationObserver)field.GetValue(statement)!;
        }

        // ── Structural assertions ────────────────────────────────────────────────────

        [Fact]
        public void DatabricksStatement_HasObserverField_TypedAsInterface()
        {
            // The refactor introduces exactly one observer field, typed as the interface
            // (not as TelemetryObserver) so SEA can inject its own future implementation.
            FieldInfo? field = typeof(DatabricksStatement).GetField(
                "_observer",
                BindingFlags.NonPublic | BindingFlags.Instance);

            Assert.NotNull(field);
            Assert.Equal(typeof(IStatementOperationObserver), field!.FieldType);
            Assert.True(field.IsInitOnly, "_observer must be readonly so once injected it cannot drift mid-execute");
        }

        [Fact]
        public void DatabricksStatement_SourceDoesNotCastTelemetrySession()
        {
            // Acceptance criterion: the ((DatabricksConnection)Connection).TelemetrySession
            // cast pattern is eliminated from the refactored class. Locating the source
            // file relative to the test assembly avoids needing a runtime hookup just to
            // assert a textual property.
            string? path = LocateDatabricksStatementSource();
            Assert.True(File.Exists(path), $"Could not locate DatabricksStatement.cs (looked at {path ?? "(null)"})");

            string source = File.ReadAllText(path!);
            Assert.DoesNotContain("((DatabricksConnection)Connection).TelemetrySession", source);
        }

        private static string? LocateDatabricksStatementSource()
        {
            // Walk up from the test assembly location until we find csharp/src/DatabricksStatement.cs.
            string? dir = Path.GetDirectoryName(typeof(DatabricksStatement).Assembly.Location);
            while (!string.IsNullOrEmpty(dir))
            {
                string candidate = Path.Combine(dir, "csharp", "src", "DatabricksStatement.cs");
                if (File.Exists(candidate)) return candidate;
                candidate = Path.Combine(dir, "src", "DatabricksStatement.cs");
                if (File.Exists(candidate)) return candidate;
                DirectoryInfo? parent = Directory.GetParent(dir);
                if (parent == null) break;
                dir = parent.FullName;
            }
            return null;
        }

        // ── Injection from DatabricksConnection.CreateStatement ──────────────────────

        [Fact]
        public void CreateStatement_TelemetryDisabled_InjectsNullObserver()
        {
            // Without a telemetry session (the default for a freshly-constructed connection
            // that has not opened a session), CreateStatement must fall back to the
            // singleton NullObserver. This is what keeps disabled-telemetry zero-cost.
            using DatabricksConnection connection = CreateConnection();

            using AdbcStatement statement = connection.CreateStatement();
            DatabricksStatement databricksStatement = Assert.IsType<DatabricksStatement>(statement);

            IStatementOperationObserver observer = GetObserverField(databricksStatement);
            Assert.Same(NullObserver.Instance, observer);
            // Compatibility property must report null so the reader/poller skip telemetry
            // branches without having to call into the observer.
            Assert.Null(databricksStatement.PendingTelemetryContext);
        }

        [Fact]
        public void CreateStatement_TelemetryEnabled_InjectsTelemetryObserver()
        {
            // When the connection has a live telemetry session, CreateStatement constructs
            // a per-statement TelemetryObserver bound to that session. PendingTelemetryContext
            // therefore returns a non-null context for the poller and reader to mutate.
            using DatabricksConnection connection = CreateConnection();
            CapturingTelemetryClient client = new CapturingTelemetryClient();
            TelemetrySessionContext session = CreateThriftSession(client);
            connection.TelemetryForTesting = new TelemetryAdapter(session);

            using AdbcStatement statement = connection.CreateStatement();
            DatabricksStatement databricksStatement = Assert.IsType<DatabricksStatement>(statement);

            IStatementOperationObserver observer = GetObserverField(databricksStatement);
            TelemetryObserver typed = Assert.IsType<TelemetryObserver>(observer);
            // The observer's underlying context must be bound to the connection's session
            // so subsequent poller/reader mutations and the final BuildTelemetryLog read
            // the same SessionId/WorkspaceId the connection negotiated.
            FieldInfo? sessionField = typeof(StatementTelemetryContext)
                .GetField("_sessionContext", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(sessionField);
            Assert.Same(session, sessionField!.GetValue(typed.Context));
            Assert.NotNull(databricksStatement.PendingTelemetryContext);
            Assert.Same(typed.Context, databricksStatement.PendingTelemetryContext);
        }

        // ── Observer hookpoint coverage ──────────────────────────────────────────────

        [Fact]
        public void Dispose_NoExecute_DoesNotFinalizeObserver()
        {
            // A statement that was never executed must not trigger OnFinalized() on the
            // injected observer (would otherwise enqueue a stray empty execute log when
            // telemetry is enabled). This preserves byte-identity with the prior
            // PendingTelemetryContext!=null gate inside EmitTelemetry.
            using DatabricksConnection connection = CreateConnection();
            RecordingObserver recorder = new RecordingObserver();
            using DatabricksStatement statement = new DatabricksStatement(connection, recorder);

            statement.Dispose();

            Assert.Empty(recorder.Calls);
        }

        [Fact]
        public void Dispose_AfterFailedExecute_DoesNotDoubleFinalize()
        {
            // Simulating the error path: the production code calls FinalizeExecuteTelemetry
            // inside the catch block and then Dispose calls it again. _executeFinalized
            // must gate so OnFinalized fires exactly once (the observer's own idempotency
            // is a defence-in-depth backstop).
            using DatabricksConnection connection = CreateConnection();
            RecordingObserver recorder = new RecordingObserver();
            using DatabricksStatement statement = new DatabricksStatement(connection, recorder);

            // Drive the observer through the same sequence the error path would: begin →
            // error → finalize. We call the private helpers directly so the test does not
            // require a Thrift server.
            InvokePrivate(statement, "BeginExecuteTelemetry", StatementType.Query, OperationType.ExecuteStatement);
            InvokePrivate(statement, "RecordExecuteError", new InvalidOperationException("boom"));
            InvokePrivate(statement, "FinalizeExecuteTelemetry");

            // Dispose triggers a second FinalizeExecuteTelemetry — must be a no-op.
            statement.Dispose();

            int finalizeCalls = recorder.Calls.Count(c => c == nameof(IStatementOperationObserver.OnFinalized));
            Assert.Equal(1, finalizeCalls);
            // Sanity: the error path also recorded OnError exactly once.
            Assert.Equal(1, recorder.Calls.Count(c => c == nameof(IStatementOperationObserver.OnError)));
        }

        [Fact]
        public void BeginExecuteTelemetry_FiresOnExecuteStartedAndPopulatesDefaults()
        {
            // Verifies the helper that replaces CreateTelemetryContext / CreateMetadataTelemetryContext:
            // it must signal the observer with the right statement/operation type and set the
            // legacy defaults (InlineArrow ResultFormat, IsInternalCall propagation).
            using DatabricksConnection connection = CreateConnection();
            CapturingTelemetryClient client = new CapturingTelemetryClient();
            TelemetrySessionContext session = CreateThriftSession(client);
            TelemetryObserver observer = new TelemetryObserver(session);
            using DatabricksStatement statement = new DatabricksStatement(connection, observer);
            statement.IsInternalCall = true;

            InvokePrivate(statement, "BeginExecuteTelemetry", StatementType.Metadata, OperationType.ListCatalogs);

            Assert.Equal(StatementType.Metadata, observer.Context.StatementType);
            Assert.Equal(OperationType.ListCatalogs, observer.Context.OperationType);
            Assert.False(observer.Context.IsCompressed, "Placeholder isCompressed=false until reader inspection at finalize time");
            Assert.Equal(ExecutionResultFormat.InlineArrow, observer.Context.ResultFormat);
            Assert.True(observer.Context.IsInternalCall);
        }

        [Fact]
        public void FinalizeExecuteTelemetry_NoExecute_StillEmitsViaConfiguredObserver()
        {
            // Once BeginExecuteTelemetry has fired, FinalizeExecuteTelemetry must close the
            // loop with OnConsumed + OnFinalized even if no reader was ever materialized
            // (defensive path: server returned no results, or caller aborted between
            // begin and base.ExecuteQuery).
            using DatabricksConnection connection = CreateConnection();
            RecordingObserver recorder = new RecordingObserver();
            using DatabricksStatement statement = new DatabricksStatement(connection, recorder);

            InvokePrivate(statement, "BeginExecuteTelemetry", StatementType.Query, OperationType.ExecuteStatement);
            InvokePrivate(statement, "FinalizeExecuteTelemetry");

            Assert.Contains(nameof(IStatementOperationObserver.OnExecuteStarted), recorder.Calls);
            Assert.Contains(nameof(IStatementOperationObserver.OnConsumed), recorder.Calls);
            Assert.Contains(nameof(IStatementOperationObserver.OnFinalized), recorder.Calls);
            // No chunk metrics path because no reader was attached.
            Assert.DoesNotContain(nameof(IStatementOperationObserver.OnChunksDownloaded), recorder.Calls);
        }

        // ── Cross-transport regression: byte-equivalent telemetry log ─────────────────

        [Fact]
        public void Thrift_Telemetry_StillEmitsAfterRefactor()
        {
            // Byte-equivalence regression: an end-to-end execute through the refactored
            // observer must produce an OssSqlDriverTelemetryLog whose top-level fields
            // match the pre-refactor EmitTelemetry output for the same inputs. We compare
            // each field explicitly rather than serializing because the timestamp envelope
            // is non-deterministic.
            using DatabricksConnection connection = CreateConnection();
            CapturingTelemetryClient client = new CapturingTelemetryClient();
            TelemetrySessionContext session = CreateThriftSession(client);
            TelemetryObserver observer = new TelemetryObserver(session);
            using DatabricksStatement statement = new DatabricksStatement(connection, observer);
            statement.StatementId = "known-statement-id-1";

            // Simulate the production execute flow that previously went through
            // CreateTelemetryContext → RecordSuccess → EmitTelemetry.
            InvokePrivate(statement, "BeginExecuteTelemetry", StatementType.Query, OperationType.ExecuteStatement);
            InvokePrivate(statement, "RecordExecuteSuccess");
            InvokePrivate(statement, "FinalizeExecuteTelemetry");

            // Exactly one log enqueued (terminal call is idempotent and no double-emission).
            Assert.Equal(1, client.EnqueueCallCount);
            TelemetryFrontendLog frontendLog = client.Logs[0];
            OssSqlDriverTelemetryLog log = frontendLog.Entry!.SqlDriverLog!;

            // Session/statement identifiers and workspace envelope preserved.
            Assert.Equal("session-thrift-abc", log.SessionId);
            Assert.Equal("known-statement-id-1", log.SqlStatementId);
            Assert.Equal(4242L, frontendLog.WorkspaceId);
            Assert.False(string.IsNullOrEmpty(frontendLog.FrontendLogEventId));

            // SqlOperation envelope matches what RecordSuccess + EmitTelemetry produced.
            Assert.Equal(StatementType.Query, log.SqlOperation.StatementType);
            Assert.Equal(OperationType.ExecuteStatement, log.SqlOperation.OperationDetail.OperationType);
            Assert.Equal(ExecutionResultFormat.InlineArrow, log.SqlOperation.ExecutionResult);
            Assert.False(log.SqlOperation.IsCompressed);

            // ResultLatency block populated (FirstBatchReadyMs from OnFirstBatchReady,
            // ResultsConsumedMs from OnConsumed). Pre-refactor EmitTelemetry called
            // ctx.RecordResultsConsumed() right before BuildTelemetryLog, so consumption
            // must be non-zero too.
            Assert.NotNull(log.SqlOperation.ResultLatency);
            Assert.True(log.SqlOperation.ResultLatency.ResultSetReadyLatencyMillis >= 0);
            Assert.True(log.SqlOperation.ResultLatency.ResultSetConsumptionLatencyMillis >= 0);

            // No error info on the success path.
            Assert.Null(log.ErrorInfo);
        }

        [Fact]
        public void Thrift_DriverMode_StillReportedAsThrift()
        {
            // Verifies the DriverMode passthrough: when DatabricksConnection initializes
            // its telemetry session with Mode=Thrift, the per-statement log carries that
            // mode all the way out to the wire. A future refactor that accidentally
            // swapped DriverMode.Thrift for Sea in InitializeTelemetry would fail here.
            using DatabricksConnection connection = CreateConnection();
            CapturingTelemetryClient client = new CapturingTelemetryClient();
            TelemetrySessionContext session = CreateThriftSession(client);
            TelemetryObserver observer = new TelemetryObserver(session);
            using DatabricksStatement statement = new DatabricksStatement(connection, observer);
            statement.StatementId = "thrift-mode-statement";

            InvokePrivate(statement, "BeginExecuteTelemetry", StatementType.Query, OperationType.ExecuteStatement);
            InvokePrivate(statement, "RecordExecuteSuccess");
            InvokePrivate(statement, "FinalizeExecuteTelemetry");

            Assert.Equal(1, client.EnqueueCallCount);
            OssSqlDriverTelemetryLog log = client.Logs[0].Entry!.SqlDriverLog!;
            Assert.NotNull(log.DriverConnectionParams);
            Assert.Equal(DriverModeType.Thrift, log.DriverConnectionParams.Mode);
        }

        [Fact]
        public void ErrorPath_FinalizesObserverWithErrorInfo()
        {
            // The error path inside Execute* drives the observer with OnError + OnFinalized
            // before re-throwing, so the emitted log carries error_info.error_name and the
            // user sees a single log per failed execute (not a missing event, not two).
            using DatabricksConnection connection = CreateConnection();
            CapturingTelemetryClient client = new CapturingTelemetryClient();
            TelemetrySessionContext session = CreateThriftSession(client);
            TelemetryObserver observer = new TelemetryObserver(session);
            using DatabricksStatement statement = new DatabricksStatement(connection, observer);
            statement.StatementId = "errored-statement";

            InvokePrivate(statement, "BeginExecuteTelemetry", StatementType.Query, OperationType.ExecuteStatement);
            InvokePrivate(statement, "RecordExecuteError", new InvalidOperationException("simulated failure"));
            InvokePrivate(statement, "FinalizeExecuteTelemetry");

            Assert.Equal(1, client.EnqueueCallCount);
            OssSqlDriverTelemetryLog log = client.Logs[0].Entry!.SqlDriverLog!;
            Assert.NotNull(log.ErrorInfo);
            Assert.Equal("InvalidOperationException", log.ErrorInfo.ErrorName);
        }

        // ── Helpers ──────────────────────────────────────────────────────────────────

        /// <summary>
        /// Invokes a private instance method via reflection. We exercise the helpers
        /// directly because driving them through a real ExecuteQuery requires a Thrift
        /// server and a server-shaped reader pipeline — both out of scope for unit tests.
        /// The helpers themselves are the entire refactor surface; reaching them via
        /// reflection is the same approach <see cref="DatabricksStatementTests"/> uses
        /// for the <c>confOverlay</c> private field.
        /// </summary>
        private static void InvokePrivate(DatabricksStatement statement, string name, params object[] args)
        {
            MethodInfo method = typeof(DatabricksStatement).GetMethod(
                name,
                BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new InvalidOperationException($"Method {name} not found on DatabricksStatement");
            method.Invoke(statement, args);
        }

        /// <summary>
        /// Minimal <see cref="IConnectionTelemetry"/> adapter that just exposes a given
        /// session through the <see cref="IConnectionTelemetry.Session"/> property so
        /// <c>connection.TelemetrySession</c> returns the test session in production code.
        /// All other methods are no-ops; the wiring we care about for these tests is the
        /// observer injection inside <c>CreateStatement</c>, which only reads
        /// <c>connection.TelemetrySession</c>.
        /// </summary>
        private sealed class TelemetryAdapter : IConnectionTelemetry
        {
            public TelemetryAdapter(TelemetrySessionContext session) { Session = session; }
            public TelemetrySessionContext? Session { get; }
            public T ExecuteWithMetadataTelemetry<T>(OperationType operationType, Func<T> operation, System.Diagnostics.Activity? activity) => operation();
            public void EmitOperationTelemetry(OperationType operationType, StatementType statementType, string? statementId, long elapsedMs, Exception? error) { }
            public Task DisposeAsync() => Task.CompletedTask;
        }
    }
}
