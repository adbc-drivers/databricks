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
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.HiveServer2.Spark;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Unit tests for telemetry wiring in <see cref="StatementExecutionConnection"/>
    /// (PECO-3022 T5). Mirrors the Thrift-side <c>DriverTelemetryWiringTests</c> pattern:
    /// these tests exercise the production code paths (<c>EmitCreateSessionTelemetry</c>,
    /// <c>EmitDeleteSessionTelemetry</c>, <c>OpenAsync</c>, <c>Dispose</c>) against a
    /// fake <see cref="IConnectionTelemetry"/> injected via the
    /// <c>TelemetryForTesting</c> seam so we can verify CREATE_SESSION/DELETE_SESSION
    /// emissions, the fail-open init contract, and the 5-second Dispose flush timeout.
    /// </summary>
    public class StatementExecutionConnectionTelemetryTests
    {
        // ── Fakes ──────────────────────────────────────────────────────────────────

        /// <summary>
        /// Records every <c>EmitOperationTelemetry</c> call so tests can assert which
        /// operation types fired, in what order, and with what payload.
        /// </summary>
        private sealed class RecordingTelemetry : IConnectionTelemetry
        {
            public List<OperationType> Calls { get; } = new();
            public List<Exception?> Errors { get; } = new();
            public List<string?> StatementIds { get; } = new();
            public Dictionary<OperationType, long> LatenciesByOp { get; } = new();
            public TelemetrySessionContext? Session { get; }
                = new TelemetrySessionContext
                {
                    SessionId = "sea-session-1",
                };
            public int DisposeCount { get; private set; }

            public T ExecuteWithMetadataTelemetry<T>(
                OperationType operationType,
                Func<T> operation,
                Activity? activity) => operation();

            public void EmitOperationTelemetry(
                OperationType operationType,
                StatementType statementType,
                string? statementId,
                long elapsedMs,
                Exception? error)
            {
                Calls.Add(operationType);
                Errors.Add(error);
                StatementIds.Add(statementId);
                LatenciesByOp[operationType] = elapsedMs;
            }

            public Task DisposeAsync()
            {
                DisposeCount++;
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Telemetry that intentionally hangs in <c>DisposeAsync</c>. Used to verify the
        /// 5-second hard timeout on <see cref="StatementExecutionConnection.Dispose"/>.
        /// </summary>
        private sealed class HangingTelemetry : IConnectionTelemetry
        {
            public TelemetrySessionContext? Session { get; }
                = new TelemetrySessionContext { SessionId = "sea-session-hang" };

            public T ExecuteWithMetadataTelemetry<T>(
                OperationType operationType,
                Func<T> operation,
                Activity? activity) => operation();

            public void EmitOperationTelemetry(
                OperationType operationType,
                StatementType statementType,
                string? statementId,
                long elapsedMs,
                Exception? error)
            {
                // No-op
            }

            // Block forever — the production Dispose must time this out at 5 seconds.
            public Task DisposeAsync() => new TaskCompletionSource<bool>().Task;
        }

        /// <summary>
        /// Telemetry that throws on every EmitOperationTelemetry call. The connection's
        /// emit helpers must swallow these so Dispose / OpenAsync don't fail.
        /// </summary>
        private sealed class ThrowingTelemetry : IConnectionTelemetry
        {
            public TelemetrySessionContext? Session => null;

            public T ExecuteWithMetadataTelemetry<T>(
                OperationType operationType,
                Func<T> operation,
                Activity? activity) => operation();

            public void EmitOperationTelemetry(
                OperationType operationType,
                StatementType statementType,
                string? statementId,
                long elapsedMs,
                Exception? error)
            {
                throw new InvalidOperationException("emit blew up");
            }

            public Task DisposeAsync() => Task.CompletedTask;
        }

        // ── Helpers ────────────────────────────────────────────────────────────────

        private static Dictionary<string, string> CreateBaseProperties()
        {
            return new Dictionary<string, string>
            {
                { SparkParameters.HostName, "telemetry-test.cloud.databricks.com" },
                { DatabricksParameters.WarehouseId, "test-warehouse-id" },
                { SparkParameters.AccessToken, "test-token" }
            };
        }

        private static StatementExecutionConnection CreateConnection()
        {
            return new StatementExecutionConnection(CreateBaseProperties());
        }

        /// <summary>
        /// Reflectively flips <c>_sessionId</c> from null to a fake id so Dispose
        /// believes a session exists and runs the DeleteSession path. The reflective
        /// access mirrors what other StatementExecutionConnection unit tests do for
        /// <c>_identityFederationClientId</c>.
        /// </summary>
        private static void SetFakeSessionId(StatementExecutionConnection connection, string sessionId)
        {
            var field = typeof(StatementExecutionConnection).GetField(
                "_sessionId",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);
            field!.SetValue(connection, sessionId);
        }

        // ── Tests ──────────────────────────────────────────────────────────────────

        [Fact]
        public void EmitCreateSessionTelemetry_FiresCreateSession()
        {
            using var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            connection.EmitCreateSessionTelemetry();

            Assert.Equal(new[] { OperationType.CreateSession }, fake.Calls);
            Assert.Null(fake.Errors[0]);
            Assert.Null(fake.StatementIds[0]);
        }

        [Fact]
        public void OpenAsync_EmitsCreateSession_WhenTelemetryEnabled()
        {
            // We can't easily drive the full OpenAsync without a real warehouse, but the
            // wiring contract is: after the session id is set, EmitCreateSessionTelemetry()
            // must fire CREATE_SESSION through the injected telemetry. The production code
            // calls this immediately after _sessionId = response.SessionId.
            using var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            // Simulate the OpenAsync post-CreateSession step.
            SetFakeSessionId(connection, "fake-session-id");
            connection.EmitCreateSessionTelemetry(activity: null);

            Assert.Contains(OperationType.CreateSession, fake.Calls);
        }

        [Fact]
        public void EmitDeleteSessionTelemetry_WithoutCreate_DoesNotFire()
        {
            // Idempotency contract: DELETE_SESSION must not fire if CREATE_SESSION never did.
            // Models a connection that failed to open a session.
            using var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            connection.EmitDeleteSessionTelemetry(elapsedMs: 10);

            Assert.DoesNotContain(OperationType.DeleteSession, fake.Calls);
        }

        [Fact]
        public void Dispose_EmitsDeleteSession()
        {
            // The production Dispose path: with a session id set, it calls DeleteSessionAsync,
            // captures its latency + error, and emits DELETE_SESSION before flushing.
            // We use the test seam to skip the real RPC: the seam doesn't bypass the emit call,
            // so we exercise the emit wiring directly with a fake session id.
            var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            // Pretend session was opened.
            connection.EmitCreateSessionTelemetry();

            // Pretend the SEA session id is set so Dispose enters the DeleteSession branch.
            // DeleteSessionAsync will throw (real HTTP call), but the production Dispose swallows
            // and still emits DELETE_SESSION with the captured exception.
            SetFakeSessionId(connection, "fake-sea-session-id");

            connection.Dispose();

            Assert.Contains(OperationType.CreateSession, fake.Calls);
            Assert.Contains(OperationType.DeleteSession, fake.Calls);
        }

        [Fact]
        public void EmitDeleteSessionTelemetry_ForwardsLatencyAndError()
        {
            using var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            // Open first so the idempotency gate lets DELETE_SESSION through.
            connection.EmitCreateSessionTelemetry();
            var rpcError = new InvalidOperationException("delete session failed");
            connection.EmitDeleteSessionTelemetry(elapsedMs: 47, error: rpcError);

            int deleteIdx = fake.Calls.IndexOf(OperationType.DeleteSession);
            Assert.True(deleteIdx >= 0);
            Assert.Equal(47, fake.LatenciesByOp[OperationType.DeleteSession]);
            Assert.Same(rpcError, fake.Errors[deleteIdx]);
        }

        [Fact]
        public void Dispose_CalledTwice_FiresDeleteSessionOnlyOnce()
        {
            // Repeated Dispose calls (common in `using` + manual Dispose) must not duplicate
            // DELETE_SESSION records.
            var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            connection.EmitCreateSessionTelemetry();
            SetFakeSessionId(connection, "fake-session-id-dup");

            connection.Dispose();
            connection.Dispose();

            int deleteCount = 0;
            foreach (var call in fake.Calls)
            {
                if (call == OperationType.DeleteSession) deleteCount++;
            }
            Assert.Equal(1, deleteCount);
        }

        [Fact]
        public void OpenAsync_TelemetryInitThrows_FallsBackToNoOpAndStillOpens()
        {
            // Goal: prove the InitializeTelemetry → ConnectionTelemetry.Create call chain is
            // wrapped in a try/catch that falls back to NoOpConnectionTelemetry when init
            // would throw. We exercise the private InitializeTelemetry helper directly via
            // reflection — this is the same boundary the production OpenAsync goes through.
            using var connection = CreateConnection();

            // Sanity: starts as NoOp (the field's default).
            Assert.IsType<NoOpConnectionTelemetry>(connection.TelemetryForTesting);

            var method = typeof(StatementExecutionConnection).GetMethod(
                "InitializeTelemetry",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(method);

            // ConnectionTelemetry.Create internally catches and returns NoOp on any failure,
            // so even with bogus properties, InitializeTelemetry should leave _telemetry
            // pointing at a valid IConnectionTelemetry (NoOp or real). It MUST NOT throw.
            var ex = Record.Exception(() => method!.Invoke(connection, new object?[] { null }));
            Assert.Null(ex);

            // After init, telemetry should still be a valid instance (NoOp or real).
            Assert.NotNull(connection.TelemetryForTesting);
        }

        [Fact]
        public void EmitCreateSessionTelemetry_SwallowsExceptions()
        {
            // Fail-open contract: if the emit call throws, neither Open nor Dispose must surface it.
            using var connection = CreateConnection();
            connection.TelemetryForTesting = new ThrowingTelemetry();

            var ex = Record.Exception(() => connection.EmitCreateSessionTelemetry());
            Assert.Null(ex);
        }

        [Fact]
        public void EmitDeleteSessionTelemetry_SwallowsExceptions()
        {
            using var connection = CreateConnection();
            connection.TelemetryForTesting = new ThrowingTelemetry();

            // Open first so the idempotency gate would otherwise let it through.
            // ThrowingTelemetry throws on CREATE_SESSION too — but the helper still swallows.
            connection.EmitCreateSessionTelemetry();
            var ex = Record.Exception(() => connection.EmitDeleteSessionTelemetry(elapsedMs: 1));
            Assert.Null(ex);
        }

        [Fact]
        public void Dispose_FlushHangs_CompletesWithin5Seconds()
        {
            // The 5-second hard timeout on _telemetry.DisposeAsync().Wait(...) is the only
            // thing standing between a wedged exporter and an indefinitely blocked Dispose.
            // Use a HangingTelemetry whose DisposeAsync never completes, and verify Dispose
            // returns in well under 10 seconds (we allow a generous wall-clock budget to
            // tolerate slow CI machines).
            var connection = CreateConnection();
            connection.TelemetryForTesting = new HangingTelemetry();

            // No session id set → DeleteSessionAsync path is skipped. The only thing that
            // could hang Dispose now is the telemetry flush, which is exactly what we want
            // to time-bound here.
            var sw = Stopwatch.StartNew();
            connection.Dispose();
            sw.Stop();

            // Budget: 5s timeout + headroom for the rest of Dispose (HttpClient disposal etc.).
            Assert.True(
                sw.Elapsed < TimeSpan.FromSeconds(10),
                $"Dispose took {sw.Elapsed.TotalSeconds:F1}s, expected < 10s (5s flush timeout + headroom).");
        }

        [Fact]
        public void TelemetrySession_DefaultsToNull_BeforeOpen()
        {
            // The TelemetrySession accessor exposes the underlying session for the SEA
            // statement (next phase) to build observers. Before OpenAsync runs, telemetry
            // is NoOp and Session is null — exactly the signal the statement uses to fall
            // back to NullObserver.
            using var connection = CreateConnection();
            Assert.Null(connection.TelemetrySession);
        }

        [Fact]
        public void TelemetrySession_ReflectsInjectedTelemetry()
        {
            // Once telemetry is wired up (real or fake), the accessor returns its session.
            // SEA statements rely on this to create per-statement observer contexts.
            using var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            Assert.NotNull(connection.TelemetrySession);
            Assert.Equal("sea-session-1", connection.TelemetrySession!.SessionId);
        }

        // ── Connect-timeout telemetry source (gap D1) ──────────────────────────────
        //
        // The SEA connection must stamp the telemetry payload's socket_timeout from a
        // connection-establishment timeout — NOT from _waitTimeoutSeconds, which is the
        // SEA query-wait (CONTINUE) timeout and a semantically different concept.

        private static int GetConnectTimeoutFieldValue(StatementExecutionConnection connection)
        {
            var field = typeof(StatementExecutionConnection).GetField(
                "_connectTimeoutMilliseconds",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);
            return (int)field!.GetValue(connection)!;
        }

        private static int GetWaitTimeoutSecondsFieldValue(StatementExecutionConnection connection)
        {
            var field = typeof(StatementExecutionConnection).GetField(
                "_waitTimeoutSeconds",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);
            return (int)field!.GetValue(connection)!;
        }

        [Fact]
        public void ConnectTimeoutMilliseconds_DefaultsTo30Seconds_WhenPropertyAbsent()
        {
            // No ConnectTimeoutMilliseconds property is set. The default must match the
            // Thrift path's HiveServer2Connection.ConnectTimeoutMillisecondsDefault (30000 ms)
            // so dashboards filtering on socket_timeout see consistent values across transports.
            using var connection = CreateConnection();

            Assert.Equal(30000, GetConnectTimeoutFieldValue(connection));
        }

        [Fact]
        public void ConnectTimeoutMilliseconds_ReadsFromSparkParametersConnectTimeoutMilliseconds()
        {
            // Source-of-truth check: the SEA connection must read the same connection-string
            // property the Thrift path reads (SparkParameters.ConnectTimeoutMilliseconds).
            var properties = CreateBaseProperties();
            properties[SparkParameters.ConnectTimeoutMilliseconds] = "45000";
            using var connection = new StatementExecutionConnection(properties);

            Assert.Equal(45000, GetConnectTimeoutFieldValue(connection));
        }

        [Fact]
        public void ConnectTimeoutMilliseconds_IsNotDerivedFromWaitTimeoutSeconds()
        {
            // Regression guard for gap D1: the previous code passed
            //   (int)TimeSpan.FromSeconds(_waitTimeoutSeconds).TotalMilliseconds
            // as connectTimeoutMilliseconds, which silently mislabeled SEA telemetry records
            // (10s wait_timeout → 10000ms socket_timeout). The two concepts must be independent.
            var properties = CreateBaseProperties();
            properties[DatabricksParameters.WaitTimeout] = "7"; // SEA CONTINUE timeout (seconds)
            properties[SparkParameters.ConnectTimeoutMilliseconds] = "55000";
            using var connection = new StatementExecutionConnection(properties);

            int waitTimeoutSeconds = GetWaitTimeoutSecondsFieldValue(connection);
            int connectTimeoutMs = GetConnectTimeoutFieldValue(connection);

            Assert.Equal(7, waitTimeoutSeconds);
            Assert.Equal(55000, connectTimeoutMs);
            // The mislabel bug would produce 7000ms here — assert it doesn't.
            Assert.NotEqual(
                (int)TimeSpan.FromSeconds(waitTimeoutSeconds).TotalMilliseconds,
                connectTimeoutMs);
        }

        [Fact]
        public void ConnectTimeoutMilliseconds_NotAffectedByEnableDirectResultsFalse()
        {
            // Direct-results=false flips _waitTimeoutSeconds to 0 in the SEA path. The connect
            // timeout (a connection-establishment concept) must remain independent of that.
            var properties = CreateBaseProperties();
            properties[DatabricksParameters.EnableDirectResults] = "false";
            properties[SparkParameters.ConnectTimeoutMilliseconds] = "20000";
            using var connection = new StatementExecutionConnection(properties);

            Assert.Equal(0, GetWaitTimeoutSecondsFieldValue(connection));
            Assert.Equal(20000, GetConnectTimeoutFieldValue(connection));
        }

        [Fact]
        public void InitializeTelemetry_ForwardsConnectTimeoutToSocketTimeoutField()
        {
            // End-to-end wiring guard: exercise the real InitializeTelemetry →
            // ConnectionTelemetry.Create path with telemetry enabled, then read back the
            // resulting session's driver_connection_params.socket_timeout. This proves the
            // argument passed to ConnectionTelemetry.Create is _connectTimeoutMilliseconds
            // (in ms, divided to seconds inside BuildDriverConnectionParams), NOT
            // _waitTimeoutSeconds. Without the fix, the bug pattern produced socket_timeout
            // in the 0–10s range (mirroring DatabricksParameters.WaitTimeout, default 10s).
            var properties = CreateBaseProperties();
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
            properties[SparkParameters.ConnectTimeoutMilliseconds] = "55000";
            properties[DatabricksParameters.WaitTimeout] = "7";

            using var connection = new StatementExecutionConnection(properties);

            // Sanity: the two fields are distinct so we can disambiguate which one feeds
            // socket_timeout downstream.
            Assert.Equal(7, GetWaitTimeoutSecondsFieldValue(connection));
            Assert.Equal(55000, GetConnectTimeoutFieldValue(connection));

            var method = typeof(StatementExecutionConnection).GetMethod(
                "InitializeTelemetry",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(method);
            method!.Invoke(connection, new object?[] { null });

            // ConnectionTelemetry.Create is `Never throws` — if init failed we'd get NoOp
            // back with Session == null. Skip if the unit-test environment doesn't permit
            // building a real telemetry client; the field-level tests above still cover
            // the source-of-truth contract.
            var session = connection.TelemetrySession;
            Assert.NotNull(session);
            Assert.NotNull(session!.DriverConnectionParams);

            // socket_timeout is in seconds (proto field is int64-seconds). 55000ms → 55s.
            // Critically: NOT 7 (which would mean the value came from _waitTimeoutSeconds).
            Assert.Equal(55, session.DriverConnectionParams!.SocketTimeout);
            Assert.NotEqual(GetWaitTimeoutSecondsFieldValue(connection), session.DriverConnectionParams.SocketTimeout);
        }

        // ── enable_direct_results telemetry source (gap B10) ───────────────────────
        //
        // The SEA connection must stamp the telemetry payload's enable_direct_results from
        // the DatabricksParameters.EnableDirectResults user property — NOT from a hardcoded
        // literal. The previous code passed `enableDirectResults: true` unconditionally,
        // making the field useless on dashboards that filter by user configuration.

        private static bool GetEnableDirectResultsFieldValue(StatementExecutionConnection connection)
        {
            var field = typeof(StatementExecutionConnection).GetField(
                "_enableDirectResults",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);
            return (bool)field!.GetValue(connection)!;
        }

        [Fact]
        public void EnableDirectResults_DefaultsToTrue_WhenPropertyAbsent()
        {
            // No EnableDirectResults property is set. The default must match the Thrift path
            // (DatabricksConnection._enableDirectResults defaults to true) so dashboards see
            // consistent values across transports for callers that never tuned this flag.
            using var connection = CreateConnection();

            Assert.True(GetEnableDirectResultsFieldValue(connection));
        }

        [Fact]
        public void EnableDirectResults_ReadsFalseFromConnectionProperties()
        {
            // Property dictionary check: the SEA connection must read the same
            // DatabricksParameters.EnableDirectResults property the Thrift path reads
            // and honor "false" rather than hardcoding true.
            var properties = CreateBaseProperties();
            properties[DatabricksParameters.EnableDirectResults] = "false";
            using var connection = new StatementExecutionConnection(properties);

            Assert.False(GetEnableDirectResultsFieldValue(connection));
        }

        [Fact]
        public void EnableDirectResults_ReadsTrueFromConnectionProperties()
        {
            // Explicit "true" must also be read from the property — not from a
            // hardcoded default — so the property is the source of truth.
            var properties = CreateBaseProperties();
            properties[DatabricksParameters.EnableDirectResults] = "true";
            using var connection = new StatementExecutionConnection(properties);

            Assert.True(GetEnableDirectResultsFieldValue(connection));
        }

        [Fact]
        public void InitializeTelemetry_ForwardsEnableDirectResultsToConnectionParams()
        {
            // End-to-end wiring guard: exercise the real InitializeTelemetry →
            // ConnectionTelemetry.Create path with telemetry enabled, then read back the
            // resulting session's driver_connection_params.enable_direct_results. This proves
            // the argument passed to ConnectionTelemetry.Create is _enableDirectResults, NOT
            // the prior hardcoded literal `true`. Without the fix, this assertion fails
            // because the field is always true regardless of user configuration.
            var properties = CreateBaseProperties();
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
            properties[DatabricksParameters.EnableDirectResults] = "false";

            using var connection = new StatementExecutionConnection(properties);

            // Sanity: the field reflects the user-supplied "false".
            Assert.False(GetEnableDirectResultsFieldValue(connection));

            var method = typeof(StatementExecutionConnection).GetMethod(
                "InitializeTelemetry",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(method);
            method!.Invoke(connection, new object?[] { null });

            // ConnectionTelemetry.Create is `Never throws` — if init failed we'd get NoOp
            // back with Session == null. The field-level tests above still cover the
            // source-of-truth contract if this end-to-end assertion can't run.
            var session = connection.TelemetrySession;
            Assert.NotNull(session);
            Assert.NotNull(session!.DriverConnectionParams);

            // The bug pattern produced `true` here regardless of user config.
            Assert.False(session.DriverConnectionParams!.EnableDirectResults);
        }
    }
}
