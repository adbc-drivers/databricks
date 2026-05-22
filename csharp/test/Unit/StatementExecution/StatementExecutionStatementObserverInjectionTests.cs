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
using Apache.Arrow.Adbc;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Unit tests for the observer-injection wiring inside
    /// <see cref="StatementExecutionConnection.CreateStatement"/> (PECO-3022 T6 setup).
    /// The setup commit only plumbs an <see cref="IStatementOperationObserver"/> field
    /// onto <see cref="StatementExecutionStatement"/> and wires the connection to
    /// build a <see cref="TelemetryObserver"/> bound to the connection's
    /// <see cref="TelemetrySessionContext"/> (or fall back to
    /// <see cref="NullObserver.Instance"/>). Subsequent hookpoint commits will then have
    /// a non-null target. These tests verify:
    /// <list type="bullet">
    ///   <item>The field exists, is typed as the interface, and is readonly.</item>
    ///   <item><see cref="StatementExecutionConnection.CreateStatement"/> injects
    ///         <see cref="NullObserver.Instance"/> when telemetry is disabled (no live session).</item>
    ///   <item><see cref="StatementExecutionConnection.CreateStatement"/> injects a
    ///         <see cref="TelemetryObserver"/> bound to the connection's session when
    ///         telemetry is enabled.</item>
    /// </list>
    /// </summary>
    public class StatementExecutionStatementObserverInjectionTests
    {
        // ── Fakes ──────────────────────────────────────────────────────────────────

        /// <summary>
        /// Minimal capturing telemetry client that satisfies <c>session.TelemetryClient != null</c>
        /// so <see cref="StatementExecutionConnection.CreateStatement"/> takes the
        /// TelemetryObserver branch instead of the NullObserver fallback.
        /// </summary>
        private sealed class CapturingTelemetryClient : ITelemetryClient
        {
            public int EnqueueCallCount;

            public void Enqueue(TelemetryFrontendLog log)
            {
                Interlocked.Increment(ref EnqueueCallCount);
            }

            public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;
            public Task CloseAsync() => Task.CompletedTask;
            public ValueTask DisposeAsync() => default;
        }

        /// <summary>
        /// Minimal <see cref="IConnectionTelemetry"/> adapter that exposes a given session
        /// through <see cref="IConnectionTelemetry.Session"/>. The CreateStatement code path
        /// only consults <c>connection.TelemetrySession</c> (which reads <c>_telemetry.Session</c>)
        /// to decide which observer to inject, so this adapter is sufficient for the test
        /// without spinning up a real ConnectionTelemetry.
        /// </summary>
        private sealed class TelemetryAdapter : IConnectionTelemetry
        {
            public TelemetryAdapter(TelemetrySessionContext? session) { Session = session; }
            public TelemetrySessionContext? Session { get; }
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
            { }
            public Task DisposeAsync() => Task.CompletedTask;
        }

        // ── Helpers ────────────────────────────────────────────────────────────────

        private static Dictionary<string, string> CreateBaseProperties()
        {
            return new Dictionary<string, string>
            {
                { SparkParameters.HostName, "observer-test.cloud.databricks.com" },
                { DatabricksParameters.WarehouseId, "test-warehouse-id" },
                { SparkParameters.AccessToken, "test-token" },
            };
        }

        private static StatementExecutionConnection CreateConnection()
        {
            return new StatementExecutionConnection(CreateBaseProperties());
        }

        /// <summary>
        /// Builds a session context with a live telemetry client. The CreateStatement
        /// branch under test only checks <c>session.TelemetryClient != null</c>, so this
        /// is the minimal shape that triggers the TelemetryObserver path.
        /// </summary>
        private static TelemetrySessionContext CreateSeaSession(ITelemetryClient client)
        {
            return new TelemetrySessionContext
            {
                SessionId = "sea-session-observer",
                WorkspaceId = 4242L,
                TelemetryClient = client,
                AuthType = "pat",
            };
        }

        /// <summary>
        /// Reflectively reads the private <c>_observer</c> field on a SEA statement so
        /// we can assert the concrete type wired in by CreateStatement.
        /// </summary>
        private static IStatementOperationObserver GetObserverField(StatementExecutionStatement statement)
        {
            FieldInfo field = typeof(StatementExecutionStatement).GetField(
                "_observer",
                BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new InvalidOperationException(
                    "_observer field not found on StatementExecutionStatement");
            return (IStatementOperationObserver)field.GetValue(statement)!;
        }

        // ── Structural assertions ──────────────────────────────────────────────────

        [Fact]
        public void StatementExecutionStatement_HasObserverField_TypedAsInterface()
        {
            // Exactly one observer field, typed as the interface (not the concrete
            // TelemetryObserver) so the SEA path can equally accept NullObserver,
            // TelemetryObserver, or any future implementation without changing the field's
            // declared type. Readonly so once injected the observer cannot drift mid-execute.
            FieldInfo? field = typeof(StatementExecutionStatement).GetField(
                "_observer",
                BindingFlags.NonPublic | BindingFlags.Instance);

            Assert.NotNull(field);
            Assert.Equal(typeof(IStatementOperationObserver), field!.FieldType);
            Assert.True(
                field.IsInitOnly,
                "_observer must be readonly so once injected it cannot drift mid-execute");
        }

        [Fact]
        public void StatementExecutionStatement_NullObserverParameter_DefaultsToNullObserver()
        {
            // Direct-construction path: the constructor's IStatementOperationObserver? parameter
            // defaults to null, which the constructor coerces to NullObserver.Instance. This
            // keeps every hookpoint callsite null-check-free (design §12) regardless of how
            // the statement was constructed.
            using var connection = CreateConnection();

            using var statement = new StatementExecutionStatement(
                client: Moq.Mock.Of<IStatementExecutionClient>(),
                sessionId: "session-1",
                warehouseId: "wh-1",
                catalog: null,
                schema: null,
                resultDisposition: "INLINE_OR_EXTERNAL_LINKS",
                resultFormat: "ARROW_STREAM",
                resultCompression: null,
                waitTimeoutSeconds: 0,
                pollingIntervalMs: 50,
                properties: CreateBaseProperties(),
                recyclableMemoryStreamManager: new Microsoft.IO.RecyclableMemoryStreamManager(),
                lz4BufferPool: System.Buffers.ArrayPool<byte>.Shared,
                httpClient: new System.Net.Http.HttpClient(),
                connection: connection);

            IStatementOperationObserver observer = GetObserverField(statement);
            Assert.Same(NullObserver.Instance, observer);
        }

        // ── Injection from StatementExecutionConnection.CreateStatement ────────────

        [Fact]
        public void CreateStatement_TelemetryDisabled_InjectsNullObserver()
        {
            // Without a telemetry session (the default for a freshly-constructed connection
            // before OpenAsync runs and InitializeTelemetry has wired the real
            // ConnectionTelemetry), CreateStatement must fall back to the singleton
            // NullObserver. This is what keeps disabled-telemetry zero-cost: no allocation
            // per statement, no null-checks at the hookpoint callsites.
            using var connection = CreateConnection();

            // Sanity: the default state of the connection has no telemetry session.
            Assert.Null(connection.TelemetrySession);

            using AdbcStatement statement = connection.CreateStatement();
            var seaStatement = Assert.IsType<StatementExecutionStatement>(statement);

            IStatementOperationObserver observer = GetObserverField(seaStatement);
            Assert.Same(NullObserver.Instance, observer);
        }

        [Fact]
        public void CreateStatement_TelemetrySessionWithoutClient_InjectsNullObserver()
        {
            // Defensive case: a TelemetrySession exists but its TelemetryClient is null
            // (telemetry was opted-in but circuit-broken, or the client never initialized).
            // CreateStatement must still fall back to NullObserver — building a real
            // TelemetryObserver against a null client would later no-op anyway, but we
            // skip the allocation entirely to keep this path zero-cost.
            using var connection = CreateConnection();
            var sessionWithoutClient = new TelemetrySessionContext
            {
                SessionId = "sea-session-no-client",
                TelemetryClient = null,
            };
            connection.TelemetryForTesting = new TelemetryAdapter(sessionWithoutClient);

            using AdbcStatement statement = connection.CreateStatement();
            var seaStatement = Assert.IsType<StatementExecutionStatement>(statement);

            IStatementOperationObserver observer = GetObserverField(seaStatement);
            Assert.Same(NullObserver.Instance, observer);
        }

        [Fact]
        public void CreateStatement_TelemetryEnabled_InjectsTelemetryObserver()
        {
            // When the connection has a live telemetry session (Session non-null AND
            // TelemetryClient non-null), CreateStatement constructs a per-statement
            // TelemetryObserver bound to that session. The observer's underlying
            // StatementTelemetryContext must reference the same TelemetrySessionContext
            // the connection negotiated, so subsequent hookpoint commits can mutate
            // poll/first-batch fields on the same context that BuildTelemetryLog later reads.
            using var connection = CreateConnection();
            var client = new CapturingTelemetryClient();
            TelemetrySessionContext session = CreateSeaSession(client);
            connection.TelemetryForTesting = new TelemetryAdapter(session);

            using AdbcStatement statement = connection.CreateStatement();
            var seaStatement = Assert.IsType<StatementExecutionStatement>(statement);

            IStatementOperationObserver observer = GetObserverField(seaStatement);
            TelemetryObserver typed = Assert.IsType<TelemetryObserver>(observer);

            // The observer's underlying context must be bound to the connection's session
            // so subsequent poller/reader mutations and the final BuildTelemetryLog read
            // the same SessionId/WorkspaceId the connection negotiated.
            FieldInfo? sessionField = typeof(StatementTelemetryContext)
                .GetField("_sessionContext", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(sessionField);
            Assert.Same(session, sessionField!.GetValue(typed.Context));
        }

        [Fact]
        public void CreateStatement_TelemetryEnabled_EachCallInjectsFreshObserver()
        {
            // Each statement must get its own TelemetryObserver instance so per-statement
            // state (statementId, poll counts, error info) does not bleed across statements
            // sharing the connection. Confirm distinct observer instances back-to-back.
            using var connection = CreateConnection();
            var client = new CapturingTelemetryClient();
            TelemetrySessionContext session = CreateSeaSession(client);
            connection.TelemetryForTesting = new TelemetryAdapter(session);

            using AdbcStatement first = connection.CreateStatement();
            using AdbcStatement second = connection.CreateStatement();

            IStatementOperationObserver obs1 = GetObserverField((StatementExecutionStatement)first);
            IStatementOperationObserver obs2 = GetObserverField((StatementExecutionStatement)second);

            Assert.IsType<TelemetryObserver>(obs1);
            Assert.IsType<TelemetryObserver>(obs2);
            Assert.NotSame(obs1, obs2);
        }
    }
}
