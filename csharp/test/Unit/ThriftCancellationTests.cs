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
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Hive.Service.Rpc.Thrift;
using Moq;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    public class ThriftCancellationTests
    {
        [Theory]
        [InlineData("GetOperationStatus", 1)]
        [InlineData("GetOperationStatus", 4)]
        [InlineData("GetOperationStatus", 10)]
        [InlineData("GetOperationStatus", 20)]
        [InlineData("FetchResults", 1)]
        [InlineData("FetchResults", 4)]
        [InlineData("FetchResults", 10)]
        [InlineData("FetchResults", 20)]
        [InlineData("GetResultSetMetadata", 1)]
        [InlineData("GetResultSetMetadata", 4)]
        [InlineData("GetResultSetMetadata", 10)]
        [InlineData("GetResultSetMetadata", 20)]
        [InlineData("ExecuteStatement", 1)]
        [InlineData("ExecuteStatement", 4)]
        [InlineData("ExecuteStatement", 10)]
        [InlineData("ExecuteStatement", 20)]
        public async Task CancellationDuringSerializationPreservesConnection(string operation, int pauseAfterWrite)
        {
            var handle = new TOperationHandle
            {
                OperationId = new THandleIdentifier { Guid = new byte[16], Secret = new byte[16] },
                OperationType = TOperationType.EXECUTE_STATEMENT,
                HasResultSet = true
            };
            var server = new Mock<TCLIService.IAsync>(MockBehavior.Strict);
            server.Setup(c => c.CancelOperation(It.IsAny<TCancelOperationReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TCancelOperationResp { Status = new TStatus(TStatusCode.SUCCESS_STATUS) });
            server.Setup(c => c.CloseOperation(It.IsAny<TCloseOperationReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TCloseOperationResp { Status = new TStatus(TStatusCode.SUCCESS_STATUS) });
            using var handler = new ThriftHandler(server.Object);
            using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("http://localhost/") };
            using var transport = new PausingHttpTransport(httpClient, pauseAfterWrite);
            using var connection = new ThriftTestConnection();
            using var client = connection.CreateClient(new TBinaryProtocol(transport));
            using var cancellation = new CancellationTokenSource();

            Task request = operation switch
            {
                "GetOperationStatus" => client.GetOperationStatus(new TGetOperationStatusReq(handle), cancellation.Token),
                "FetchResults" => client.FetchResults(new TFetchResultsReq(handle, TFetchOrientation.FETCH_NEXT, 1000), cancellation.Token),
                "GetResultSetMetadata" => client.GetResultSetMetadata(new TGetResultSetMetadataReq(handle), cancellation.Token),
                "ExecuteStatement" => client.ExecuteStatement(new TExecuteStatementReq(new TSessionHandle(handle.OperationId), "SELECT 1")
                {
                    Parameters = new List<TSparkParameter>(),
                    RunAsync = true
                }, cancellation.Token),
                _ => throw new ArgumentException(operation)
            };

            try
            {
                await WaitForSignal(transport.Paused.Task);
                cancellation.Cancel();
            }
            finally
            {
                transport.Resume.TrySetResult(true);
            }
            var exception = await Record.ExceptionAsync(() => request);
            if (exception is TTransportException transportException)
            {
                Assert.Equal(TTransportException.ExceptionType.Interrupted, transportException.Type);
                exception = transportException.InnerException;
            }
            Assert.IsAssignableFrom<OperationCanceledException>(exception);
            Assert.Equal(0, handler.RequestCount);

            await client.CancelOperation(new TCancelOperationReq(handle));
            await client.CloseOperation(new TCloseOperationReq(handle));

            server.Verify(c => c.CancelOperation(It.IsAny<TCancelOperationReq>(), It.IsAny<CancellationToken>()), Times.Once);
            server.Verify(c => c.CloseOperation(It.IsAny<TCloseOperationReq>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task CancellationWhileWaitingForClientDoesNotWriteRequest()
        {
            var handle = new TOperationHandle { OperationId = new THandleIdentifier { Guid = new byte[16], Secret = new byte[16] } };
            var server = new Mock<TCLIService.IAsync>(MockBehavior.Strict);
            server.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TGetOperationStatusResp { Status = new TStatus(TStatusCode.SUCCESS_STATUS) });
            server.Setup(c => c.CloseOperation(It.IsAny<TCloseOperationReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TCloseOperationResp { Status = new TStatus(TStatusCode.SUCCESS_STATUS) });
            using var handler = new ThriftHandler(server.Object, blockFirstRequest: true);
            using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("http://localhost/") };
            using var transport = new PausingHttpTransport(httpClient, pauseAfterWrite: int.MaxValue);
            using var connection = new ThriftTestConnection();
            using var client = connection.CreateClient(new TBinaryProtocol(transport));
            using var cancellation = new CancellationTokenSource();

            Task activeRequest = client.GetOperationStatus(new TGetOperationStatusReq(handle));
            try
            {
                await WaitForSignal(handler.Started.Task);
                int writes = transport.WriteCount;
                Task queuedRequest = client.GetResultSetMetadata(new TGetResultSetMetadataReq(handle), cancellation.Token);
                cancellation.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queuedRequest);
                Assert.Equal(writes, transport.WriteCount);
                Assert.Equal(1, handler.RequestCount);
            }
            finally
            {
                handler.Resume.TrySetResult(true);
                await activeRequest;
            }
            await client.CloseOperation(new TCloseOperationReq(handle));
            server.Verify(c => c.CloseOperation(It.IsAny<TCloseOperationReq>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        internal static async Task WaitForSignal(Task task)
        {
            Assert.Same(task, await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(10))));
            await task;
        }
    }

    internal sealed class ThriftTestConnection : DatabricksConnection
    {
        public ThriftTestConnection() : base(new Dictionary<string, string>
        {
            [SparkParameters.HostName] = "localhost",
            [SparkParameters.Token] = "test-token"
        })
        {
        }

        public ThreadSafeClient CreateClient(TProtocol protocol) => (ThreadSafeClient)CreateTCLIServiceClient(protocol);
    }

    internal sealed class PausingHttpTransport : THttpTransport
    {
        private readonly int _pauseAfterWrite;
        public int WriteCount { get; private set; }
        public TaskCompletionSource<bool> Paused { get; } = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> Resume { get; } = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        public PausingHttpTransport(HttpClient client, int pauseAfterWrite) : base(client, new TConfiguration())
        {
            _pauseAfterWrite = pauseAfterWrite;
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken)
        {
            await base.WriteAsync(buffer, offset, length, cancellationToken);
            if (++WriteCount == _pauseAfterWrite)
            {
                Paused.TrySetResult(true);
                await Resume.Task;
            }
        }
    }

    internal sealed class ThriftHandler : HttpMessageHandler
    {
        private readonly TCLIService.AsyncProcessor _processor;
        private readonly bool _blockFirstRequest;
        private int _requestCount;
        public int RequestCount => Volatile.Read(ref _requestCount);
        public TaskCompletionSource<bool> Started { get; } = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> Resume { get; } = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        public ThriftHandler(TCLIService.IAsync server, bool blockFirstRequest = false)
        {
            _processor = new TCLIService.AsyncProcessor(server);
            _blockFirstRequest = blockFirstRequest;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Interlocked.Increment(ref _requestCount) == 1 && _blockFirstRequest)
            {
                Started.TrySetResult(true);
                var cancelled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                using var registration = cancellationToken.Register(() => cancelled.TrySetResult(true));
                await Task.WhenAny(Resume.Task, cancelled.Task);
                cancellationToken.ThrowIfCancellationRequested();
            }
            using var input = new MemoryStream(await request.Content!.ReadAsByteArrayAsync());
            using var output = new MemoryStream();
            using var transport = new TStreamTransport(input, output, new TConfiguration());
            Assert.True(await _processor.ProcessAsync(new TBinaryProtocol(transport), new TBinaryProtocol(transport), cancellationToken));
            Assert.Equal(input.Length, input.Position);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(output.ToArray()) };
        }
    }
}
