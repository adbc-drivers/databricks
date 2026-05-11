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
using AdbcDrivers.HiveServer2.Spark;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests that the four PECO-2991 lifecycle hooks
    /// (CREATE_SESSION / DELETE_SESSION / CANCEL_STATEMENT / CLOSE_STATEMENT) actually
    /// invoke <see cref="IConnectionTelemetry.EmitOperationTelemetry"/> from the production
    /// call sites. <see cref="ConnectionTelemetryOperationEmissionTests"/> covers the
    /// emit method itself; this file covers the wiring around it.
    /// </summary>
    public class DriverTelemetryWiringTests
    {
        /// <summary>
        /// No-op <see cref="ITelemetryClient"/> used to satisfy the
        /// <c>session?.TelemetryClient != null</c> gate in <c>DatabricksStatement</c>'s
        /// Cancel/Dispose paths so the wiring tests can observe the
        /// <see cref="RecordingTelemetry"/> calls.
        /// </summary>
        private sealed class StubTelemetryClient : ITelemetryClient
        {
            public void Enqueue(TelemetryFrontendLog log) { }
            public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;
            public Task CloseAsync() => Task.CompletedTask;
            public ValueTask DisposeAsync() => default;
        }

        /// <summary>
        /// Records every <c>EmitOperationTelemetry</c> call so tests can assert which
        /// operation types fired and in what order.
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
                    SessionId = "test-session",
                    TelemetryClient = new StubTelemetryClient(),
                };
            public int DisposeCount { get; private set; }

            public T ExecuteWithMetadataTelemetry<T>(
                OperationType operationType,
                Func<T> operation,
                System.Diagnostics.Activity? activity) => operation();

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

        private static DatabricksConnection CreateConnection()
        {
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "test.databricks.com",
                [SparkParameters.Token] = "test-token",
            };
            return new DatabricksConnection(properties);
        }

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
        public void Dispose_AfterCreateSession_FiresDeleteSession()
        {
            var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            connection.EmitCreateSessionTelemetry();
            connection.Dispose();

            Assert.Equal(
                new[] { OperationType.CreateSession, OperationType.DeleteSession },
                fake.Calls);
        }

        [Fact]
        public void Dispose_WithoutCreateSession_DoesNotFireDeleteSession()
        {
            var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            // Skip CREATE_SESSION — this models a connection that failed to open a session.
            connection.Dispose();

            Assert.DoesNotContain(OperationType.DeleteSession, fake.Calls);
        }

        [Fact]
        public void Dispose_CalledTwice_FiresDeleteSessionOnlyOnce()
        {
            var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            connection.EmitCreateSessionTelemetry();
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
        public void StatementDispose_FiresCloseStatement()
        {
            using var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            var statement = new DatabricksStatement(connection);
            statement.Dispose();

            Assert.Contains(OperationType.CloseStatement, fake.Calls);
        }

        [Fact]
        public void StatementDispose_PassesCapturedRpcLatencyToTelemetry()
        {
            // DatabricksCompositeReader stashes the TCloseOperationReq RPC latency on the
            // statement; statement Dispose should forward it as operation_latency_ms.
            using var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            var statement = new DatabricksStatement(connection);
            statement.CloseStatementRpcLatencyMs = 42;
            statement.Dispose();

            Assert.Contains(OperationType.CloseStatement, fake.Calls);
            Assert.Equal(42, fake.LatenciesByOp[OperationType.CloseStatement]);
        }

        [Fact]
        public void StatementDispose_ForwardsCapturedRpcError()
        {
            using var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            var statement = new DatabricksStatement(connection);
            var rpcError = new InvalidOperationException("close failed");
            statement.CloseStatementRpcLatencyMs = 5;
            statement.CloseStatementRpcError = rpcError;
            statement.Dispose();

            int closeIdx = fake.Calls.IndexOf(OperationType.CloseStatement);
            Assert.True(closeIdx >= 0);
            Assert.Same(rpcError, fake.Errors[closeIdx]);
        }

        [Fact]
        public void StatementDispose_CalledTwice_FiresCloseStatementOnlyOnce()
        {
            using var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            var statement = new DatabricksStatement(connection);
            statement.Dispose();
            statement.Dispose();

            int closeCount = 0;
            foreach (var call in fake.Calls)
            {
                if (call == OperationType.CloseStatement) closeCount++;
            }
            Assert.Equal(1, closeCount);
        }

        [Fact]
        public void StatementCancel_FiresCancelStatement()
        {
            using var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            using var statement = new DatabricksStatement(connection);
            statement.Cancel();

            Assert.Contains(OperationType.CancelStatement, fake.Calls);
        }

        [Fact]
        public void StatementCancel_CalledMultipleTimes_FiresOnePerInvocation()
        {
            // base.Cancel() is idempotent (just signals the token source) but each user
            // invocation is a deliberate action, so we emit one CANCEL_STATEMENT per call.
            using var connection = CreateConnection();
            var fake = new RecordingTelemetry();
            connection.TelemetryForTesting = fake;

            using var statement = new DatabricksStatement(connection);
            statement.Cancel();
            statement.Cancel();
            statement.Cancel();

            int cancelCount = 0;
            foreach (var call in fake.Calls)
            {
                if (call == OperationType.CancelStatement) cancelCount++;
            }
            Assert.Equal(3, cancelCount);
        }
    }
}
