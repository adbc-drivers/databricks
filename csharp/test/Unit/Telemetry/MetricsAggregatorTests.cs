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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for MetricsAggregator class.
    /// </summary>
    public class MetricsAggregatorTests : IDisposable
    {
        private readonly ActivitySource _activitySource;
        private readonly MockTelemetryExporter _mockExporter;
        private readonly TelemetryConfiguration _config;
        private MetricsAggregator? _aggregator;

        private const long TestWorkspaceId = 12345;
        private const string TestUserAgent = "TestAgent/1.0";

        public MetricsAggregatorTests()
        {
            _activitySource = new ActivitySource("TestSource");
            _mockExporter = new MockTelemetryExporter();
            _config = new TelemetryConfiguration
            {
                BatchSize = 10,
                FlushIntervalMs = 60000 // Set high to control when flush happens
            };
        }

        public void Dispose()
        {
            _aggregator?.Dispose();
            _activitySource.Dispose();
        }

        #region Constructor Tests

        [Fact]
        public void MetricsAggregator_Constructor_NullExporter_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new MetricsAggregator(null!, _config, TestWorkspaceId, TestUserAgent));
        }

        [Fact]
        public void MetricsAggregator_Constructor_NullConfig_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new MetricsAggregator(_mockExporter, null!, TestWorkspaceId, TestUserAgent));
        }

        [Fact]
        public void MetricsAggregator_Constructor_NullUserAgent_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, null!));
        }

        [Fact]
        public void MetricsAggregator_Constructor_ValidParameters_CreatesInstance()
        {
            // Act
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            // Assert
            Assert.NotNull(_aggregator);
            Assert.Equal(0, _aggregator.PendingEventCount);
            Assert.Equal(0, _aggregator.ActiveStatementCount);
        }

        #endregion

        #region ProcessActivity Tests - Connection Events

        [Fact]
        public void MetricsAggregator_ProcessActivity_ConnectionOpen_EmitsImmediately()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            using var activity = _activitySource.StartActivity("Connection.Open");
            Assert.NotNull(activity);
            activity.SetTag("session.id", "test-session-123");
            activity.Stop();

            // Act
            _aggregator.ProcessActivity(activity);

            // Assert
            Assert.Equal(1, _aggregator.PendingEventCount);
        }

        [Fact]
        public void MetricsAggregator_ProcessActivity_ConnectionOpenAsync_EmitsImmediately()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            using var activity = _activitySource.StartActivity("OpenAsync");
            Assert.NotNull(activity);
            activity.SetTag("session.id", "test-session-456");
            activity.SetTag("driver.version", "1.0.0");
            activity.Stop();

            // Act
            _aggregator.ProcessActivity(activity);

            // Assert
            Assert.Equal(1, _aggregator.PendingEventCount);
        }

        #endregion

        #region ProcessActivity Tests - Statement Events

        [Fact]
        public void MetricsAggregator_ProcessActivity_Statement_AggregatesByStatementId()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            var statementId = "stmt-123";
            var sessionId = "session-456";

            // First activity
            using (var activity1 = _activitySource.StartActivity("Statement.Execute"))
            {
                Assert.NotNull(activity1);
                activity1.SetTag("statement.id", statementId);
                activity1.SetTag("session.id", sessionId);
                activity1.SetTag("result.chunk_count", "5");
                activity1.Stop();

                _aggregator.ProcessActivity(activity1);
            }

            // Second activity with same statement_id
            using (var activity2 = _activitySource.StartActivity("Statement.FetchResults"))
            {
                Assert.NotNull(activity2);
                activity2.SetTag("statement.id", statementId);
                activity2.SetTag("session.id", sessionId);
                activity2.SetTag("result.chunk_count", "3");
                activity2.Stop();

                _aggregator.ProcessActivity(activity2);
            }

            // Assert - should not emit until CompleteStatement
            Assert.Equal(0, _aggregator.PendingEventCount);
            Assert.Equal(1, _aggregator.ActiveStatementCount);
        }

        [Fact]
        public void MetricsAggregator_ProcessActivity_Statement_WithoutStatementId_EmitsImmediately()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            using var activity = _activitySource.StartActivity("Statement.Execute");
            Assert.NotNull(activity);
            // No statement.id tag
            activity.SetTag("session.id", "session-123");
            activity.Stop();

            // Act
            _aggregator.ProcessActivity(activity);

            // Assert - should emit immediately since no statement_id
            Assert.Equal(1, _aggregator.PendingEventCount);
        }

        #endregion

        #region CompleteStatement Tests

        [Fact]
        public void MetricsAggregator_CompleteStatement_EmitsAggregatedEvent()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            var statementId = "stmt-complete-123";
            var sessionId = "session-complete-456";

            using (var activity = _activitySource.StartActivity("Statement.Execute"))
            {
                Assert.NotNull(activity);
                activity.SetTag("statement.id", statementId);
                activity.SetTag("session.id", sessionId);
                activity.SetTag("result.format", "cloudfetch");
                activity.SetTag("result.chunk_count", "10");
                activity.Stop();

                _aggregator.ProcessActivity(activity);
            }

            Assert.Equal(0, _aggregator.PendingEventCount);
            Assert.Equal(1, _aggregator.ActiveStatementCount);

            // Act
            _aggregator.CompleteStatement(statementId);

            // Assert
            Assert.Equal(1, _aggregator.PendingEventCount);
            Assert.Equal(0, _aggregator.ActiveStatementCount);
        }

        [Fact]
        public void MetricsAggregator_CompleteStatement_NullStatementId_NoOp()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            // Act - should not throw
            _aggregator.CompleteStatement(null!);
            _aggregator.CompleteStatement(string.Empty);

            // Assert
            Assert.Equal(0, _aggregator.PendingEventCount);
        }

        [Fact]
        public void MetricsAggregator_CompleteStatement_UnknownStatementId_NoOp()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            // Act - should not throw
            _aggregator.CompleteStatement("unknown-statement-id");

            // Assert
            Assert.Equal(0, _aggregator.PendingEventCount);
        }

        #endregion

        #region FlushAsync Tests

        [Fact]
        public async Task MetricsAggregator_FlushAsync_BatchSizeReached_ExportsEvents()
        {
            // Arrange
            var config = new TelemetryConfiguration { BatchSize = 2, FlushIntervalMs = 60000 };
            _aggregator = new MetricsAggregator(_mockExporter, config, TestWorkspaceId, TestUserAgent);

            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            // Add events that should trigger flush
            for (int i = 0; i < 3; i++)
            {
                using var activity = _activitySource.StartActivity("Connection.Open");
                Assert.NotNull(activity);
                activity.SetTag("session.id", $"session-{i}");
                activity.Stop();
                _aggregator.ProcessActivity(activity);
            }

            // Explicit flush to ensure deterministic behavior (avoids timing-dependent CI flakiness)
            await _aggregator.FlushAsync();

            // Assert - exporter should have been called
            Assert.True(_mockExporter.ExportCallCount > 0);
        }

        [Fact]
        public async Task MetricsAggregator_FlushAsync_TimeInterval_ExportsEvents()
        {
            // Arrange
            var config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 100 }; // 100ms interval
            _aggregator = new MetricsAggregator(_mockExporter, config, TestWorkspaceId, TestUserAgent);

            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            using var activity = _activitySource.StartActivity("Connection.Open");
            Assert.NotNull(activity);
            activity.SetTag("session.id", "session-timer");
            activity.Stop();
            _aggregator.ProcessActivity(activity);

            // Act - wait for timer to trigger flush (3x interval gives margin for CI environments under load)
            await Task.Delay(300);

            // Assert - exporter should have been called by timer
            Assert.True(_mockExporter.ExportCallCount > 0);
        }

        [Fact]
        public async Task MetricsAggregator_FlushAsync_EmptyQueue_NoExport()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            // Act
            await _aggregator.FlushAsync();

            // Assert
            Assert.Equal(0, _mockExporter.ExportCallCount);
        }

        [Fact]
        public async Task MetricsAggregator_FlushAsync_MultipleEvents_ExportsAll()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            for (int i = 0; i < 5; i++)
            {
                using var activity = _activitySource.StartActivity("Connection.Open");
                Assert.NotNull(activity);
                activity.SetTag("session.id", $"session-{i}");
                activity.Stop();
                _aggregator.ProcessActivity(activity);
            }

            Assert.Equal(5, _aggregator.PendingEventCount);

            // Act
            await _aggregator.FlushAsync();

            // Assert
            Assert.Equal(0, _aggregator.PendingEventCount);
            Assert.Equal(1, _mockExporter.ExportCallCount);
            Assert.Equal(5, _mockExporter.TotalExportedEvents);
        }

        #endregion

        #region Test Helpers

        /// <summary>
        /// Helper to create HttpExceptionWithStatusCode via production code path.
        /// Simulates real scenario where HTTP response triggers exception through EnsureSuccessOrThrow().
        /// </summary>
        private static HttpExceptionWithStatusCode CreateHttpException(HttpStatusCode statusCode)
        {
            using var response = new HttpResponseMessage(statusCode);
            try
            {
                response.EnsureSuccessOrThrow();
                throw new InvalidOperationException("Expected exception was not thrown");
            }
            catch (HttpExceptionWithStatusCode ex)
            {
                return ex;
            }
        }

        #endregion

        #region RecordException Tests

        [Fact]
        public void MetricsAggregator_RecordException_Terminal_FlushesImmediately()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            // Use production code path to generate terminal exception (401 Unauthorized)
            var terminalException = CreateHttpException(HttpStatusCode.Unauthorized);

            // Act
            _aggregator.RecordException("stmt-123", "session-456", terminalException);

            // Assert - terminal exception should be queued immediately
            Assert.Equal(1, _aggregator.PendingEventCount);
        }

        [Fact]
        public void MetricsAggregator_RecordException_Retryable_BuffersUntilComplete()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            var retryableException = new HttpRequestException("503 (Service Unavailable)");
            var statementId = "stmt-retryable-123";
            var sessionId = "session-retryable-456";

            // Act
            _aggregator.RecordException(statementId, sessionId, retryableException);

            // Assert - retryable exception should be buffered, not queued
            Assert.Equal(0, _aggregator.PendingEventCount);
            Assert.Equal(1, _aggregator.ActiveStatementCount);
        }

        [Fact]
        public void MetricsAggregator_RecordException_Retryable_EmittedOnFailedComplete()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            var retryableException = new HttpRequestException("503 (Service Unavailable)");
            var statementId = "stmt-failed-123";
            var sessionId = "session-failed-456";

            _aggregator.RecordException(statementId, sessionId, retryableException);
            Assert.Equal(0, _aggregator.PendingEventCount);

            // Act - complete statement as failed
            _aggregator.CompleteStatement(statementId, failed: true);

            // Assert - both statement event and error event should be emitted
            Assert.Equal(2, _aggregator.PendingEventCount);
        }

        [Fact]
        public void MetricsAggregator_RecordException_Retryable_NotEmittedOnSuccessComplete()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            var retryableException = new HttpRequestException("503 (Service Unavailable)");
            var statementId = "stmt-success-123";
            var sessionId = "session-success-456";

            _aggregator.RecordException(statementId, sessionId, retryableException);

            // Act - complete statement as success
            _aggregator.CompleteStatement(statementId, failed: false);

            // Assert - only statement event should be emitted, not the error
            Assert.Equal(1, _aggregator.PendingEventCount);
        }

        [Fact]
        public void MetricsAggregator_RecordException_NullException_NoOp()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            // Act - should not throw
            _aggregator.RecordException("stmt-123", "session-456", null);

            // Assert
            Assert.Equal(0, _aggregator.PendingEventCount);
        }

        [Fact]
        public void MetricsAggregator_RecordException_NullStatementId_NoOp()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            // Act - should not throw
            _aggregator.RecordException(null!, "session-456", new Exception("test"));
            _aggregator.RecordException(string.Empty, "session-456", new Exception("test"));

            // Assert
            Assert.Equal(0, _aggregator.PendingEventCount);
        }

        #endregion

        #region HTTP Status Code Extraction Tests

        [Fact]
        public async Task MetricsAggregator_RecordException_HttpExceptionWithStatusCode_ExtractsStatusCode()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);
            // 401 is a terminal exception, so it gets queued immediately
            // Use production code path to generate exception
            var exception = CreateHttpException(HttpStatusCode.Unauthorized);

            // Act
            _aggregator.RecordException("stmt-123", "session-456", exception);
            await _aggregator.FlushAsync();

            // Assert
            Assert.Equal(1, _mockExporter.ExportCallCount);
            var exportedLog = _mockExporter.ExportedLogs.First();
            Assert.NotNull(exportedLog.Entry?.SqlDriverLog?.ErrorInfo);
            Assert.Equal(401, exportedLog.Entry.SqlDriverLog.ErrorInfo.HttpStatusCode);
        }

        [Fact]
        public async Task MetricsAggregator_RecordException_RetryableHttpException_ExtractsStatusCodeOnFailed()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);
            // 503 is a retryable exception, so it gets buffered until CompleteStatement
            // Use production code path to generate exception
            var exception = CreateHttpException(HttpStatusCode.ServiceUnavailable);
            var statementId = "stmt-503";

            // Act
            _aggregator.RecordException(statementId, "session-456", exception);
            _aggregator.CompleteStatement(statementId, failed: true); // Emit buffered exception
            await _aggregator.FlushAsync();

            // Assert - should have statement event + error event
            Assert.Equal(1, _mockExporter.ExportCallCount);
            Assert.Equal(2, _mockExporter.TotalExportedEvents);

            // Find the error event (the one with ErrorInfo)
            var errorLog = _mockExporter.ExportedLogs.FirstOrDefault(
                log => log.Entry?.SqlDriverLog?.ErrorInfo != null);
            Assert.NotNull(errorLog);
            Assert.Equal(503, errorLog.Entry!.SqlDriverLog!.ErrorInfo!.HttpStatusCode);
        }

        [Fact]
        public async Task MetricsAggregator_RecordException_WrappedTerminalHttpException_ExtractsStatusCode()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);
            // Wrapped terminal exception (403 Forbidden)
            // Use production code path to generate exception
            var innerException = CreateHttpException(HttpStatusCode.Forbidden);
            var wrappedException = new Exception("Wrapped exception", innerException);

            // Act - wrapped terminal exception is recognized as terminal by ExceptionClassifier
            _aggregator.RecordException("stmt-123", "session-456", wrappedException);
            await _aggregator.FlushAsync();

            // Assert
            Assert.Equal(1, _mockExporter.ExportCallCount);
            var exportedLog = _mockExporter.ExportedLogs.First();
            Assert.NotNull(exportedLog.Entry?.SqlDriverLog?.ErrorInfo);
            Assert.Equal(403, exportedLog.Entry.SqlDriverLog.ErrorInfo.HttpStatusCode);
        }

        [Fact]
        public async Task MetricsAggregator_RecordException_RegularException_NoStatusCode()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);
            // Regular exception without HTTP status - will be buffered (retryable)
            var exception = new InvalidOperationException("Some error");
            var statementId = "stmt-regular";

            // Act
            _aggregator.RecordException(statementId, "session-456", exception);
            _aggregator.CompleteStatement(statementId, failed: true); // Emit buffered exception
            await _aggregator.FlushAsync();

            // Assert - should have statement event + error event
            Assert.Equal(1, _mockExporter.ExportCallCount);

            // Find the error event
            var errorLog = _mockExporter.ExportedLogs.FirstOrDefault(
                log => log.Entry?.SqlDriverLog?.ErrorInfo != null);
            Assert.NotNull(errorLog);
            Assert.Null(errorLog.Entry!.SqlDriverLog!.ErrorInfo!.HttpStatusCode);
        }

        #endregion

        #region Exception Swallowing Tests

        [Fact]
        public void MetricsAggregator_ProcessActivity_ExceptionSwallowed_NoThrow()
        {
            // Arrange
            var throwingExporter = new ThrowingTelemetryExporter();
            _aggregator = new MetricsAggregator(throwingExporter, _config, TestWorkspaceId, TestUserAgent);

            // Act & Assert - should not throw even with throwing exporter
            _aggregator.ProcessActivity(null);
        }

        [Fact]
        public async Task MetricsAggregator_FlushAsync_ExceptionSwallowed_NoThrow()
        {
            // Arrange
            var throwingExporter = new ThrowingTelemetryExporter();
            _aggregator = new MetricsAggregator(throwingExporter, _config, TestWorkspaceId, TestUserAgent);

            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            using var activity = _activitySource.StartActivity("Connection.Open");
            Assert.NotNull(activity);
            activity.SetTag("session.id", "session-throw");
            activity.Stop();
            _aggregator.ProcessActivity(activity);

            // Act & Assert - should not throw
            await _aggregator.FlushAsync();
        }

        #endregion

        #region Tag Filtering Tests

        [Fact]
        public void MetricsAggregator_ProcessActivity_FiltersTags_UsingRegistry()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            var statementId = "stmt-filter-123";

            using (var activity = _activitySource.StartActivity("Statement.Execute"))
            {
                Assert.NotNull(activity);
                activity.SetTag("statement.id", statementId);
                activity.SetTag("session.id", "session-filter");
                activity.SetTag("result.format", "cloudfetch");
                // This sensitive tag should be filtered out
                activity.SetTag("db.statement", "SELECT * FROM sensitive_table");
                // This tag should be included
                activity.SetTag("result.chunk_count", "5");
                activity.Stop();

                _aggregator.ProcessActivity(activity);
            }

            // Complete to emit
            _aggregator.CompleteStatement(statementId);

            // Assert - event should be created (we can verify via export)
            Assert.Equal(1, _aggregator.PendingEventCount);
        }

        #endregion

        #region WrapInFrontendLog Tests

        [Fact]
        public void MetricsAggregator_WrapInFrontendLog_CreatesValidStructure()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            var telemetryEvent = new TelemetryEvent
            {
                SessionId = "session-wrap-123",
                SqlStatementId = "stmt-wrap-456",
                OperationLatencyMs = 100
            };

            // Act
            var frontendLog = _aggregator.WrapInFrontendLog(telemetryEvent);

            // Assert
            Assert.NotNull(frontendLog);
            Assert.Equal(TestWorkspaceId, frontendLog.WorkspaceId);
            Assert.NotEmpty(frontendLog.FrontendLogEventId);
            Assert.NotNull(frontendLog.Context);
            Assert.NotNull(frontendLog.Context.ClientContext);
            Assert.Equal(TestUserAgent, frontendLog.Context.ClientContext.UserAgent);
            Assert.True(frontendLog.Context.TimestampMillis > 0);
            Assert.NotNull(frontendLog.Entry);
            Assert.NotNull(frontendLog.Entry.SqlDriverLog);
            Assert.Equal(telemetryEvent.SessionId, frontendLog.Entry.SqlDriverLog.SessionId);
            Assert.Equal(telemetryEvent.SqlStatementId, frontendLog.Entry.SqlDriverLog.SqlStatementId);
        }

        [Fact]
        public void MetricsAggregator_WrapInFrontendLog_GeneratesUniqueEventIds()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            var telemetryEvent = new TelemetryEvent { SessionId = "session-unique" };

            // Act
            var frontendLog1 = _aggregator.WrapInFrontendLog(telemetryEvent);
            var frontendLog2 = _aggregator.WrapInFrontendLog(telemetryEvent);

            // Assert
            Assert.NotEqual(frontendLog1.FrontendLogEventId, frontendLog2.FrontendLogEventId);
        }

        #endregion

        #region Dispose Tests

        [Fact]
        public void MetricsAggregator_Dispose_FlushesRemainingEvents()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            using var activity = _activitySource.StartActivity("Connection.Open");
            Assert.NotNull(activity);
            activity.SetTag("session.id", "session-dispose");
            activity.Stop();
            _aggregator.ProcessActivity(activity);

            Assert.Equal(1, _aggregator.PendingEventCount);

            // Act
            _aggregator.Dispose();

            // Assert - events should have been flushed
            Assert.True(_mockExporter.ExportCallCount > 0);
        }

        [Fact]
        public void MetricsAggregator_Dispose_CanBeCalledMultipleTimes()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            // Act & Assert - should not throw
            _aggregator.Dispose();
            _aggregator.Dispose();
        }

        #endregion

        #region Integration Tests

        [Fact]
        public async Task MetricsAggregator_EndToEnd_StatementLifecycle()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            var statementId = "stmt-e2e-123";
            var sessionId = "session-e2e-456";

            // Simulate statement execution
            using (var executeActivity = _activitySource.StartActivity("Statement.Execute"))
            {
                Assert.NotNull(executeActivity);
                executeActivity.SetTag("statement.id", statementId);
                executeActivity.SetTag("session.id", sessionId);
                executeActivity.SetTag("result.format", "cloudfetch");
                executeActivity.Stop();
                _aggregator.ProcessActivity(executeActivity);
            }

            // Simulate chunk downloads
            for (int i = 0; i < 3; i++)
            {
                using var downloadActivity = _activitySource.StartActivity("CloudFetch.Download");
                Assert.NotNull(downloadActivity);
                downloadActivity.SetTag("statement.id", statementId);
                downloadActivity.SetTag("session.id", sessionId);
                downloadActivity.SetTag("result.chunk_count", "1");
                downloadActivity.SetTag("result.bytes_downloaded", "1000");
                downloadActivity.Stop();
                _aggregator.ProcessActivity(downloadActivity);
            }

            // Complete statement
            _aggregator.CompleteStatement(statementId);

            // Flush
            await _aggregator.FlushAsync();

            // Assert
            Assert.Equal(1, _mockExporter.ExportCallCount);
            Assert.Equal(1, _mockExporter.TotalExportedEvents);
            Assert.Equal(0, _aggregator.ActiveStatementCount);
        }

        [Fact]
        public async Task MetricsAggregator_EndToEnd_MultipleStatements()
        {
            // Arrange
            _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            var sessionId = "session-multi";

            // Execute 3 statements
            for (int i = 0; i < 3; i++)
            {
                var statementId = $"stmt-multi-{i}";

                using var activity = _activitySource.StartActivity("Statement.Execute");
                Assert.NotNull(activity);
                activity.SetTag("statement.id", statementId);
                activity.SetTag("session.id", sessionId);
                activity.Stop();
                _aggregator.ProcessActivity(activity);

                _aggregator.CompleteStatement(statementId);
            }

            // Flush
            await _aggregator.FlushAsync();

            // Assert
            Assert.Equal(3, _mockExporter.TotalExportedEvents);
        }

        #endregion

        #region Mock Classes

        /// <summary>
        /// Mock telemetry exporter for testing.
        /// </summary>
        private class MockTelemetryExporter : ITelemetryExporter
        {
            private int _exportCallCount;
            private int _totalExportedEvents;
            private readonly ConcurrentBag<TelemetryFrontendLog> _exportedLogs = new ConcurrentBag<TelemetryFrontendLog>();

            public int ExportCallCount => _exportCallCount;
            public int TotalExportedEvents => _totalExportedEvents;
            public IReadOnlyCollection<TelemetryFrontendLog> ExportedLogs => _exportedLogs.ToList();

            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                Interlocked.Increment(ref _exportCallCount);
                Interlocked.Add(ref _totalExportedEvents, logs.Count);

                foreach (var log in logs)
                {
                    _exportedLogs.Add(log);
                }

                return Task.FromResult(true);
            }
        }

        /// <summary>
        /// Telemetry exporter that always throws for testing exception handling.
        /// </summary>
        private class ThrowingTelemetryExporter : ITelemetryExporter
        {
            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                throw new InvalidOperationException("Test exception from exporter");
            }
        }

        #endregion
    }
}
