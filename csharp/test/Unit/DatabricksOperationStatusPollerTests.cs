/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Proto;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Hive.Service.Rpc.Thrift;
using Apache.Arrow.Adbc.Tracing;
using Moq;
using Thrift.Protocol;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    public class DatabricksOperationStatusPollerTests
    {
        private readonly ITestOutputHelper _outputHelper;
        private readonly Mock<IHiveServer2Statement> _mockStatement;
        private readonly Mock<TCLIService.IAsync> _mockClient;
        private readonly TOperationHandle _operationHandle;
        private readonly Mock<IResponse> _mockResponse;

        private readonly int _heartbeatIntervalSeconds = 1;

        public DatabricksOperationStatusPollerTests(ITestOutputHelper outputHelper)
        {
            _outputHelper = outputHelper;
            _mockClient = new Mock<TCLIService.IAsync>();
            _mockStatement = new Mock<IHiveServer2Statement>();
            _mockResponse = new Mock<IResponse>();
            _operationHandle = new TOperationHandle
            {
                OperationId = new THandleIdentifier { Guid = new byte[] { 1, 2, 3, 4 }, Secret = new byte[16] },
                OperationType = TOperationType.EXECUTE_STATEMENT
            };

            _mockStatement.Setup(s => s.Client).Returns(_mockClient.Object);
            _mockResponse.Setup(r => r.OperationHandle!).Returns(_operationHandle);
        }

        [Fact]
        public async Task StartPollsOperationStatusAtInterval()
        {
            // Arrange
            using var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _mockResponse.Object, _heartbeatIntervalSeconds);
            var pollCount = 0;
            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TGetOperationStatusResp())
                .Callback(() => pollCount++);

            // Act
            poller.Start();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait for 3 seconds to allow multiple polls

            // Assert
            Assert.True(pollCount > 0, "Should have polled at least once");
            _mockClient.Verify(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        }

        [Fact]
        public async Task DisposeStopsPolling()
        {
            // Arrange
            using var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _mockResponse.Object, _heartbeatIntervalSeconds);
            var pollCount = 0;
            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TGetOperationStatusResp())
                .Callback(() => pollCount++);

            // Act
            poller.Start();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Let it poll for a bit
            poller.Dispose();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait to see if it continues polling

            // Assert
            int finalPollCount = pollCount;
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait another second
            Assert.Equal(finalPollCount, pollCount); // Poll count should not increase after disposal
        }

        [Fact]
        public async Task StopStopsPolling()
        {
            // Arrange
            using var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _mockResponse.Object, _heartbeatIntervalSeconds);
            var pollCount = 0;
            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TGetOperationStatusResp())
                .Callback(() => pollCount++);

            // Act
            poller.Start();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Let it poll for a bit
            poller.Stop();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait to see if it continues polling

            // Assert
            int finalPollCount = pollCount;
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait another second
            Assert.Equal(finalPollCount, pollCount); // Poll count should not increase after stopping
        }

        [Fact]
        public async Task StopsPollingOnAllTerminalOperationStates()
        {
            var terminalStates = new[]
            {
                TOperationState.CANCELED_STATE,
                TOperationState.ERROR_STATE,
                TOperationState.CLOSED_STATE,
                TOperationState.TIMEDOUT_STATE,
                TOperationState.UKNOWN_STATE
            };

            foreach (var terminalState in terminalStates)
            {
                // Arrange
                using var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _mockResponse.Object, _heartbeatIntervalSeconds);
                var pollCount = 0;
                _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new TGetOperationStatusResp { OperationState = terminalState })
                    .Callback(() => pollCount++);

                // Act
                poller.Start();
                await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait longer than heartbeat interval

                // Assert
                Assert.Equal(1, pollCount);
            }
        }

        [Fact]
        public async Task ContinuesPollingOnFinishedState()
        {
            // Arrange
            using var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _mockResponse.Object, _heartbeatIntervalSeconds);
            var pollCount = 0;
            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TGetOperationStatusResp { OperationState = TOperationState.FINISHED_STATE })
                .Callback(() => pollCount++);

            // Act
            poller.Start();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait longer than heartbeat interval

            // Assert
            // Should continue polling when in running state
            Assert.True(pollCount > 1, $"Expected multiple polls but got {pollCount}");
        }

        [Fact]
        public async Task ContinuesPollingOnException()
        {
            // Arrange
            using var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _mockResponse.Object, _heartbeatIntervalSeconds);
            var pollCount = 0;
            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Test exception"))
                .Callback(() => pollCount++);

            // Act
            poller.Start();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait longer than heartbeat interval

            // Assert
            // Should continue polling despite the exception (transient error resilience)
            Assert.True(pollCount > 1, $"Expected multiple polls despite exceptions but got {pollCount}");
        }

        [Fact]
        public async Task UsesCustomHeartbeatInterval()
        {
            // Arrange
            int customHeartbeatInterval = 2; // 2 seconds
            using var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _mockResponse.Object, customHeartbeatInterval);
            var pollCount = 0;
            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TGetOperationStatusResp())
                .Callback(() => pollCount++);

            // Act
            poller.Start();
            await Task.Delay(TimeSpan.FromSeconds(customHeartbeatInterval * 3)); // Wait for 3 intervals

            // Assert
            Assert.True(pollCount > 0, "Should have polled at least once");
            _mockClient.Verify(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        }

        [Fact]
        public async Task UpdatesTelemetryContextOnEachSuccessfulPoll()
        {
            // Arrange — pass in a per-statement telemetry context (PECO-2992) so the poller
            // can populate PollCount / PollLatencyMs which are read by
            // StatementTelemetryContext.BuildTelemetryLog() to set
            // n_operation_status_calls and operation_status_latency_millis.
            var sessionContext = new TelemetrySessionContext
            {
                SessionId = "test-session",
                WorkspaceId = 42L
            };
            var telemetryContext = new StatementTelemetryContext(sessionContext);

            using var poller = new DatabricksOperationStatusPoller(
                _mockStatement.Object,
                _mockResponse.Object,
                _heartbeatIntervalSeconds,
                telemetryContext: telemetryContext);

            int observedPolls = 0;
            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TGetOperationStatusResp { OperationState = TOperationState.RUNNING_STATE })
                .Callback(() => Interlocked.Increment(ref observedPolls));

            // Act
            poller.Start();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3));
            poller.Stop();

            // Assert — the context should have been updated with at least one poll's worth of data.
            Assert.True(observedPolls > 0, "Should have polled at least once");
            Assert.NotNull(telemetryContext.PollCount);
            Assert.True(telemetryContext.PollCount!.Value > 0,
                $"PollCount should be > 0, got {telemetryContext.PollCount}");
            Assert.NotNull(telemetryContext.PollLatencyMs);
            Assert.True(telemetryContext.PollLatencyMs!.Value >= 0,
                $"PollLatencyMs should be >= 0, got {telemetryContext.PollLatencyMs}");
            // PollCount in the context should match the number of times we observed
            // the GetOperationStatus mock complete successfully (within polling concurrency).
            Assert.True(telemetryContext.PollCount!.Value <= observedPolls + 1,
                $"PollCount {telemetryContext.PollCount} should not greatly exceed observed polls {observedPolls}");

            // Build the proto and confirm the values flow through to the wire-level fields.
            OssSqlDriverTelemetryLog log = telemetryContext.BuildTelemetryLog();
            Assert.NotNull(log.SqlOperation);
            Assert.NotNull(log.SqlOperation.OperationDetail);
            Assert.Equal(telemetryContext.PollCount!.Value,
                log.SqlOperation.OperationDetail.NOperationStatusCalls);
            Assert.Equal(telemetryContext.PollLatencyMs!.Value,
                log.SqlOperation.OperationDetail.OperationStatusLatencyMillis);
        }

        [Fact]
        public async Task DoesNotUpdateTelemetryContextWhenNoneProvided()
        {
            // Arrange — backwards-compatibility check: when no context is provided,
            // the poller still works and does not throw.
            using var poller = new DatabricksOperationStatusPoller(
                _mockStatement.Object,
                _mockResponse.Object,
                _heartbeatIntervalSeconds);
            int observedPolls = 0;
            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TGetOperationStatusResp())
                .Callback(() => Interlocked.Increment(ref observedPolls));

            // Act
            poller.Start();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 2));
            poller.Stop();

            // Assert
            Assert.True(observedPolls > 0);
        }

        [Fact]
        public async Task UsesCustomRequestTimeout()
        {
            // Arrange
            int customRequestTimeout = 5;
            using var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _mockResponse.Object, _heartbeatIntervalSeconds, customRequestTimeout);
            var pollCount = 0;

            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TGetOperationStatusResp())
                .Callback(() => pollCount++);

            // Act
            poller.Start();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 2));

            // Assert: This test verifies that the poller can be instantiated with a custom request timeout

            Assert.True(pollCount > 0, "Should have polled at least once");
            _mockClient.Verify(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        }

        [Theory]
        [InlineData(1, false)]
        [InlineData(4, false)]
        [InlineData(10, false)]
        [InlineData(20, false)]
        [InlineData(28, false)]
        [InlineData(1, true)]
        [InlineData(4, true)]
        [InlineData(10, true)]
        [InlineData(20, true)]
        [InlineData(28, true)]
        public async Task StoppingDuringSerializationPreservesCloseOperation(int pauseAfterWrite, bool cancelExternally)
        {
            _operationHandle.OperationId.Secret = new byte[16];
            _operationHandle.HasResultSet = true;
            var server = new Mock<TCLIService.IAsync>(MockBehavior.Strict);
            server.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TGetOperationStatusResp
                {
                    Status = new TStatus(TStatusCode.SUCCESS_STATUS),
                    OperationState = TOperationState.FINISHED_STATE
                });
            server.Setup(c => c.CloseOperation(It.IsAny<TCloseOperationReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TCloseOperationResp { Status = new TStatus(TStatusCode.SUCCESS_STATUS) });

            using var httpClient = new HttpClient(new ThriftHandler(server.Object))
            {
                BaseAddress = new Uri("http://localhost/")
            };
            using var transport = new PausingHttpTransport(httpClient, pauseAfterWrite);
            using var connection = new ThriftTestConnection();
            using var client = connection.CreateClient(new TBinaryProtocol(transport));
            _mockStatement.Setup(s => s.Client).Returns(client);
            using var cancellation = new CancellationTokenSource();
            using var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _mockResponse.Object, _heartbeatIntervalSeconds);

            poller.Start(cancellation.Token);
            try
            {
                await WaitForSignal(transport.Paused.Task);
                if (cancelExternally)
                {
                    cancellation.Cancel();
                }
                else
                {
                    poller.Stop();
                }
            }
            finally
            {
                transport.Resume.TrySetResult(true);
                poller.Dispose();
            }

            await client.CloseOperation(new TCloseOperationReq(_operationHandle));

            server.Verify(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()), Times.Never);
            server.Verify(c => c.CloseOperation(It.IsAny<TCloseOperationReq>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task RequestTimeoutDuringSerializationPreservesCloseOperation()
        {
            var server = new Mock<TCLIService.IAsync>(MockBehavior.Strict);
            server.Setup(c => c.CloseOperation(It.IsAny<TCloseOperationReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TCloseOperationResp { Status = new TStatus(TStatusCode.SUCCESS_STATUS) });
            using var httpClient = new HttpClient(new ThriftHandler(server.Object)) { BaseAddress = new Uri("http://localhost/") };
            using var transport = new PausingHttpTransport(httpClient, pauseAfterWrite: 20);
            using var connection = new ThriftTestConnection();
            using var client = connection.CreateClient(new TBinaryProtocol(transport));
            var started = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .Returns((TGetOperationStatusReq request, CancellationToken token) =>
                {
                    started.TrySetResult(token);
                    return client.GetOperationStatus(request, token);
                });
            using var poller = new DatabricksOperationStatusPoller(
                _mockStatement.Object, _mockResponse.Object, _heartbeatIntervalSeconds, requestTimeoutSeconds: 1);
            poller.Start();
            try
            {
                await WaitForSignal(transport.Paused.Task);
                await WaitForSignal(started.Task);
                var cancelled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                using var registration = (await started.Task).Register(() => cancelled.TrySetResult(true));
                await WaitForSignal(cancelled.Task);
            }
            finally
            {
                poller.Stop();
                transport.Resume.TrySetResult(true);
                poller.Dispose();
            }
            await client.CloseOperation(new TCloseOperationReq(_operationHandle));
            server.Verify(c => c.CloseOperation(It.IsAny<TCloseOperationReq>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task HttpCancellationOnlyReportsErrorForRequestTimeout(bool stopPoller)
        {
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
            _mockStatement.Setup(s => s.Client).Returns(client);
            using var trace = new ActivityTrace(nameof(HttpCancellationOnlyReportsErrorForRequestTimeout));
            var tracer = new Mock<IActivityTracer>();
            tracer.Setup(t => t.Trace).Returns(trace);
            var completed = new TaskCompletionSource<Activity>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var listener = new ActivityListener
            {
                ShouldListenTo = source => ReferenceEquals(source, trace.ActivitySource),
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStopped = activity => completed.TrySetResult(activity)
            };
            ActivitySource.AddActivityListener(listener);
            using var poller = new DatabricksOperationStatusPoller(
                _mockStatement.Object, _mockResponse.Object, _heartbeatIntervalSeconds,
                requestTimeoutSeconds: stopPoller ? 30 : 1, activityTracer: tracer.Object);
            poller.Start();
            try
            {
                await WaitForSignal(handler.Started.Task);
                if (stopPoller) poller.Stop();
                await WaitForSignal(completed.Task);
            }
            finally
            {
                handler.Resume.TrySetResult(true);
                poller.Dispose();
            }
            Activity activity = await completed.Task;
            if (stopPoller)
            {
                Assert.DoesNotContain(activity.Events, e => e.Name == "operation_status_poller.poll_error");
            }
            else
            {
                Assert.Contains(activity.Events, e => e.Name == "operation_status_poller.poll_error");
            }
            Assert.NotEqual(ActivityStatusCode.Error, activity.Status);
            await client.CloseOperation(new TCloseOperationReq(_operationHandle));
            server.Verify(c => c.CloseOperation(It.IsAny<TCloseOperationReq>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        private static Task WaitForSignal(Task task)
        {
            return ThriftCancellationTests.WaitForSignal(task);
        }
    }
}
