/*
* Copyright (c) 2026 ADBC Drivers Contributors
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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Moq;
using Xunit;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    public class GeospatialExecutionTelemetryTests
    {
        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task ExecuteQuery_RecordsReadinessOnlyAfterGeospatialInitialization(
            bool synchronous, bool failPrefetch)
        {
            var source = new FirstBatchStream();
            var client = new CapturingTelemetryClient();
            using var connection = new TestConnection(source);
            connection.TelemetryForTesting = new ConnectionTelemetry(
                "test.databricks.com", client, new TelemetrySessionContext
                {
                    SessionId = "test-session",
                    TelemetryClient = client
                });
            using var statement = new TestStatement(connection, source.Schema)
            {
                SqlQuery = "SELECT geo",
                StatementId = "geospatial-statement"
            };
            StatementTelemetryContext? context = null;
            var expected = new InvalidOperationException("first batch fetch failed");
            source.OnRead = () =>
            {
                context = statement.PendingTelemetryContext;
                Assert.NotNull(context);
                Assert.Null(context.FirstBatchReadyMs);
                if (failPrefetch)
                    throw expected;
            };

            async Task<QueryResult> Execute() => synchronous
                ? statement.ExecuteQuery()
                : await statement.ExecuteQueryAsync();

            if (failPrefetch)
            {
                InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
                    async () => await Execute());
                Assert.Same(expected, error);
                Assert.Null(statement.PendingTelemetryContext);
                Assert.Single(client.Logs);
            }
            else
            {
                QueryResult result = await Execute();
                Assert.NotNull(context!.FirstBatchReadyMs);
                Assert.Empty(client.Logs);
                result.Stream!.Dispose();
            }

            Assert.Equal(1, source.ReadCount);
            Assert.Equal(1, source.DisposeCount);
            Assert.Equal("geospatial-statement", context!.StatementId);
            Assert.Equal(failPrefetch, context.HasError);
            if (failPrefetch)
                Assert.Null(context.FirstBatchReadyMs);

            statement.Dispose();
            TelemetryFrontendLog execution = Assert.Single(client.Logs, log =>
                log.Entry.SqlDriverLog.SqlOperation.OperationDetail.OperationType == OperationType.ExecuteStatement);
            Assert.Equal("geospatial-statement", execution.Entry.SqlDriverLog.SqlStatementId);
            Assert.Equal(failPrefetch, execution.Entry.SqlDriverLog.ErrorInfo != null);
        }

        private sealed class FirstBatchStream : IArrowArrayStream
        {
            public Schema Schema { get; } = new Schema.Builder()
                .Field(new Field("g", StringType.Default, true, new Dictionary<string, string>
                {
                    [ColumnMetadataHelper.ArrowMetadataKey] = "GEOMETRY(4326)"
                }))
                .Build();
            internal Action? OnRead { get; set; }
            internal int ReadCount { get; private set; }
            internal int DisposeCount { get; private set; }

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                ReadCount++;
                OnRead?.Invoke();
                return new ValueTask<RecordBatch?>(new RecordBatch(Schema,
                    new IArrowArray[] { new StringArray.Builder().Append("POINT(1 2)").Build() }, 1));
            }

            public void Dispose() => DisposeCount++;
        }

        private sealed class CapturingTelemetryClient : ITelemetryClient
        {
            internal List<TelemetryFrontendLog> Logs { get; } = new();
            public void Enqueue(TelemetryFrontendLog log) => Logs.Add(log);
            public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;
            public Task CloseAsync() => Task.CompletedTask;
            public ValueTask DisposeAsync() => default;
        }

        private sealed class TestStatement : DatabricksStatement
        {
            private readonly Schema _schema;

            internal TestStatement(DatabricksConnection connection, Schema schema) : base(connection)
            {
                _schema = schema;
            }

            protected override Schema GetSchemaFromMetadata(TGetResultSetMetadataResp metadata) => _schema;
        }

        private sealed class TestConnection : DatabricksConnection
        {
            private readonly IArrowArrayStream _stream;

            internal TestConnection(IArrowArrayStream stream) : base(new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "test.databricks.com",
                [SparkParameters.Token] = "test-token"
            })
            {
                _stream = stream;
                var client = new Mock<TCLIService.IAsync>();
                client.Setup(c => c.ExecuteStatement(It.IsAny<TExecuteStatementReq>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new TExecuteStatementResp
                    {
                        Status = new TStatus { StatusCode = TStatusCode.SUCCESS_STATUS },
                        OperationHandle = new TOperationHandle { OperationId = HandleId() },
                        DirectResults = new TSparkDirectResults
                        {
                            OperationStatus = new TGetOperationStatusResp { OperationState = TOperationState.FINISHED_STATE },
                            ResultSetMetadata = new TGetResultSetMetadataResp()
                        }
                    });
                client.Setup(c => c.CloseSession(It.IsAny<TCloseSessionReq>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new TCloseSessionResp { Status = new TStatus { StatusCode = TStatusCode.SUCCESS_STATUS } });
                typeof(HiveServer2Connection).GetField("_client", BindingFlags.Instance | BindingFlags.NonPublic)!
                    .SetValue(this, client.Object);
                typeof(HiveServer2Connection).GetProperty("SessionHandle", BindingFlags.Instance | BindingFlags.NonPublic)!
                    .SetValue(this, new TSessionHandle { SessionId = HandleId() });
            }

            internal override IArrowArrayStream NewReader<T>(
                T statement, Schema schema, IResponse response, TGetResultSetMetadataResp? metadataResp = null) => _stream;

            private static THandleIdentifier HandleId() => new THandleIdentifier
            {
                Guid = Guid.NewGuid().ToByteArray(),
                Secret = System.Array.Empty<byte>()
            };
        }
    }
}
