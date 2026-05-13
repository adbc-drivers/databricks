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
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Unit tests covering PECO-2991: ADBC must emit telemetry for the operation types
    /// that were silent before this change — CREATE_SESSION, DELETE_SESSION,
    /// CANCEL_STATEMENT, and CLOSE_STATEMENT — through the new
    /// <see cref="IConnectionTelemetry.EmitOperationTelemetry"/> entry point.
    /// </summary>
    public class ConnectionTelemetryOperationEmissionTests
    {
        /// <summary>
        /// Test-only ITelemetryClient that records every Enqueue call so tests can
        /// assert the exact proto fields the driver tried to emit.
        /// </summary>
        private sealed class CapturingTelemetryClient : ITelemetryClient
        {
            public List<TelemetryFrontendLog> Logs { get; } = new List<TelemetryFrontendLog>();

            public void Enqueue(TelemetryFrontendLog log) => Logs.Add(log);

            public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;

            public Task CloseAsync() => Task.CompletedTask;

            public ValueTask DisposeAsync() => default;
        }

        private static (ConnectionTelemetry telemetry, CapturingTelemetryClient client) CreateTelemetry()
        {
            var client = new CapturingTelemetryClient();
            var session = new TelemetrySessionContext
            {
                SessionId = "test-session",
                WorkspaceId = 999L,
                TelemetryClient = client,
                AuthType = "pat",
            };
            var telemetry = new ConnectionTelemetry("test.databricks.com", client, session);
            return (telemetry, client);
        }

        [Fact]
        public void EmitOperationTelemetry_CreateSession_EnqueuesLogWithOperationType()
        {
            var (telemetry, client) = CreateTelemetry();

            telemetry.EmitOperationTelemetry(
                OperationType.CreateSession,
                StatementType.Unspecified,
                statementId: null,
                elapsedMs: 42,
                error: null);

            Assert.Single(client.Logs);
            var log = client.Logs[0].Entry.SqlDriverLog;
            Assert.Equal("test-session", log.SessionId);
            Assert.Equal(42, log.OperationLatencyMs);
            Assert.NotNull(log.SqlOperation);
            Assert.NotNull(log.SqlOperation.OperationDetail);
            Assert.Equal(OperationType.CreateSession, log.SqlOperation.OperationDetail.OperationType);
            Assert.Equal(StatementType.Unspecified, log.SqlOperation.StatementType);
            Assert.Null(log.ErrorInfo);
        }

        [Fact]
        public void EmitOperationTelemetry_DeleteSession_EnqueuesLog()
        {
            var (telemetry, client) = CreateTelemetry();

            telemetry.EmitOperationTelemetry(
                OperationType.DeleteSession,
                StatementType.Unspecified,
                statementId: null,
                elapsedMs: 1234,
                error: null);

            Assert.Single(client.Logs);
            var log = client.Logs[0].Entry.SqlDriverLog;
            Assert.Equal(1234, log.OperationLatencyMs);
            Assert.Equal(OperationType.DeleteSession, log.SqlOperation.OperationDetail.OperationType);
        }

        [Fact]
        public void EmitOperationTelemetry_CancelStatement_PopulatesStatementId()
        {
            var (telemetry, client) = CreateTelemetry();

            telemetry.EmitOperationTelemetry(
                OperationType.CancelStatement,
                StatementType.Unspecified,
                statementId: "stmt-cancel-1",
                elapsedMs: 10,
                error: null);

            Assert.Single(client.Logs);
            var log = client.Logs[0].Entry.SqlDriverLog;
            Assert.Equal("stmt-cancel-1", log.SqlStatementId);
            Assert.Equal(OperationType.CancelStatement, log.SqlOperation.OperationDetail.OperationType);
        }

        [Fact]
        public void EmitOperationTelemetry_CloseStatement_PopulatesStatementId()
        {
            var (telemetry, client) = CreateTelemetry();

            telemetry.EmitOperationTelemetry(
                OperationType.CloseStatement,
                StatementType.Unspecified,
                statementId: "stmt-close-1",
                elapsedMs: 5,
                error: null);

            Assert.Single(client.Logs);
            var log = client.Logs[0].Entry.SqlDriverLog;
            Assert.Equal("stmt-close-1", log.SqlStatementId);
            Assert.Equal(OperationType.CloseStatement, log.SqlOperation.OperationDetail.OperationType);
        }

        [Fact]
        public void EmitOperationTelemetry_WithError_PopulatesErrorInfo()
        {
            var (telemetry, client) = CreateTelemetry();

            telemetry.EmitOperationTelemetry(
                OperationType.CancelStatement,
                StatementType.Unspecified,
                statementId: "stmt-err",
                elapsedMs: 7,
                error: new InvalidOperationException("boom"));

            Assert.Single(client.Logs);
            var log = client.Logs[0].Entry.SqlDriverLog;
            Assert.NotNull(log.ErrorInfo);
            Assert.Equal("InvalidOperationException", log.ErrorInfo.ErrorName);
        }

        [Fact]
        public void EmitOperationTelemetry_DoesNotThrow_WhenStatementIdIsNull()
        {
            var (telemetry, _) = CreateTelemetry();

            // No exception should escape this call.
            telemetry.EmitOperationTelemetry(
                OperationType.CreateSession,
                StatementType.Unspecified,
                statementId: null,
                elapsedMs: 0,
                error: null);
        }

        [Fact]
        public void NoOpConnectionTelemetry_EmitOperationTelemetry_DoesNothing()
        {
            // Given a no-op telemetry implementation, EmitOperationTelemetry must be safe
            // and have no observable effect — the driver always falls back to no-op when
            // telemetry is disabled or initialisation fails.
            IConnectionTelemetry telemetry = NoOpConnectionTelemetry.Instance;

            // No exception, no side effects observable.
            telemetry.EmitOperationTelemetry(
                OperationType.CreateSession,
                StatementType.Unspecified,
                statementId: "x",
                elapsedMs: 1,
                error: new Exception("boom"));
        }
    }
}
