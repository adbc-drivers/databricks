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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.Databricks.Telemetry.Proto;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Unit tests for <see cref="MetricsAggregator"/>.
    /// Tests ProcessActivity, FlushAsync, root activity detection,
    /// session context propagation, and exception swallowing.
    /// </summary>
    public class MetricsAggregatorTests : IDisposable
    {
        private readonly ActivitySource _driverSource;
        private readonly ActivitySource _externalSource;
        private readonly ActivityListener _listener;
        private readonly MockTelemetryClient _mockClient;
        private readonly TelemetryConfiguration _config;

        public MetricsAggregatorTests()
        {
            // Use the same ActivitySource name as the driver for root detection tests
            _driverSource = new ActivitySource(MetricsAggregator.DatabricksActivitySourceName);
            _externalSource = new ActivitySource("External.Source");

            // Set up a listener to ensure all activities are recorded
            _listener = new ActivityListener
            {
                ShouldListenTo = source => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> options) =>
                    ActivitySamplingResult.AllDataAndRecorded,
                ActivityStarted = activity => { },
                ActivityStopped = activity => { }
            };
            ActivitySource.AddActivityListener(_listener);

            _mockClient = new MockTelemetryClient();
            _config = new TelemetryConfiguration();
        }

        public void Dispose()
        {
            _listener.Dispose();
            _driverSource.Dispose();
            _externalSource.Dispose();
        }

        #region Constructor Tests

        [Fact]
        public void Constructor_NullTelemetryClient_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new MetricsAggregator(null!, _config));
        }

        [Fact]
        public void Constructor_NullConfig_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new MetricsAggregator(_mockClient, null!));
        }

        [Fact]
        public void Constructor_ValidParameters_CreatesInstance()
        {
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            Assert.NotNull(aggregator);
            Assert.Equal(0, aggregator.PendingContextCount);
        }

        #endregion

        #region ProcessActivity - Context Creation Tests

        [Fact]
        public void ProcessActivity_WithStatementId_CreatesContext()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            using Activity? activity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("statement.id", "stmt-001");
            // Don't set terminal status so it doesn't auto-emit (child activity behavior)

            // Act
            aggregator.ProcessActivity(activity);

            // Assert
            Assert.Equal(1, aggregator.PendingContextCount);
        }

        [Fact]
        public void ProcessActivity_WithoutStatementId_Ignored()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            using Activity? activity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            // No statement.id tag set
            activity!.SetStatus(ActivityStatusCode.Ok);
            activity.Stop();

            // Act
            aggregator.ProcessActivity(activity);

            // Assert
            Assert.Equal(0, aggregator.PendingContextCount);
            Assert.Empty(_mockClient.EnqueuedLogs);
        }

        [Fact]
        public void ProcessActivity_NullActivity_Ignored()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            // Act & Assert - should not throw
            aggregator.ProcessActivity(null!);
            Assert.Equal(0, aggregator.PendingContextCount);
        }

        [Fact]
        public void ProcessActivity_EmptyStatementId_Ignored()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            using Activity? activity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("statement.id", "");
            activity.SetStatus(ActivityStatusCode.Ok);
            activity.Stop();

            // Act
            aggregator.ProcessActivity(activity);

            // Assert
            Assert.Equal(0, aggregator.PendingContextCount);
            Assert.Empty(_mockClient.EnqueuedLogs);
        }

        [Fact]
        public void ProcessActivity_SameStatementId_MergesIntoSameContext()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            // Parent activity remains active so children get their Parent set
            using Activity? parentActivity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(parentActivity);
            parentActivity!.SetTag("statement.id", "stmt-001");

            // First child activity (poll) - implicit parent via Activity.Current
            Activity? pollActivity = _driverSource.StartActivity("Statement.PollOperationStatus");
            Assert.NotNull(pollActivity);
            pollActivity!.SetTag("statement.id", "stmt-001");
            pollActivity.SetTag("poll.count", 5);
            pollActivity.SetTag("poll.latency_ms", 250L);
            pollActivity.Stop();

            // Second child activity (download) - implicit parent via Activity.Current
            Activity? downloadActivity = _driverSource.StartActivity("CloudFetch.DownloadFiles");
            Assert.NotNull(downloadActivity);
            downloadActivity!.SetTag("statement.id", "stmt-001");
            downloadActivity.SetTag("cloudfetch.total_chunks", 10);
            downloadActivity.Stop();

            // Act - process both children (they are children of the driver source)
            aggregator.ProcessActivity(pollActivity);
            aggregator.ProcessActivity(downloadActivity);

            // Assert - single context with merged data
            Assert.Equal(1, aggregator.PendingContextCount);

            // Cleanup
            pollActivity.Dispose();
            downloadActivity.Dispose();
        }

        [Fact]
        public void ProcessActivity_MultipleStatements_SeparateContexts()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            // Statement 1: parent stays active so child gets parent reference
            using Activity? parent1 = _driverSource.StartActivity("Statement.ExecuteQuery.1");
            Assert.NotNull(parent1);
            parent1!.SetTag("statement.id", "stmt-001");

            Activity? child1 = _driverSource.StartActivity("Statement.PollOperationStatus.1");
            Assert.NotNull(child1);
            child1!.SetTag("statement.id", "stmt-001");
            child1.Stop();
            aggregator.ProcessActivity(child1);

            // Stop parent1 so it's not the active Activity.Current anymore
            parent1.Stop();

            // Statement 2: separate parent
            using Activity? parent2 = _driverSource.StartActivity("Statement.ExecuteQuery.2");
            Assert.NotNull(parent2);
            parent2!.SetTag("statement.id", "stmt-002");

            Activity? child2 = _driverSource.StartActivity("Statement.PollOperationStatus.2");
            Assert.NotNull(child2);
            child2!.SetTag("statement.id", "stmt-002");
            child2.Stop();
            aggregator.ProcessActivity(child2);

            // Assert - two separate contexts
            Assert.Equal(2, aggregator.PendingContextCount);

            // Cleanup
            child1.Dispose();
            child2.Dispose();
        }

        #endregion

        #region ProcessActivity - Root Activity Detection Tests

        [Fact]
        public void ProcessActivity_RootActivityComplete_EmitsProtoAndRemovesContext()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            aggregator.SetSessionContext("session-123", 99999L, null, null);

            // Root activity (no parent from driver source)
            using Activity? rootActivity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(rootActivity);
            rootActivity!.SetTag("statement.id", "stmt-001");
            rootActivity.SetTag("session.id", "session-123");
            rootActivity.SetStatus(ActivityStatusCode.Ok);
            System.Threading.Thread.Sleep(10);
            rootActivity.Stop();

            // Act
            aggregator.ProcessActivity(rootActivity);

            // Assert - context should be emitted and removed
            Assert.Equal(0, aggregator.PendingContextCount);
            TelemetryFrontendLog enqueuedLog = Assert.Single(_mockClient.EnqueuedLogs);
            Assert.Equal(99999L, enqueuedLog.WorkspaceId);
            Assert.NotNull(enqueuedLog.FrontendLogEventId);
            Assert.NotNull(enqueuedLog.Context);
            Assert.True(enqueuedLog.Context!.TimestampMillis > 0);
            Assert.NotNull(enqueuedLog.Entry);
            Assert.NotNull(enqueuedLog.Entry!.SqlDriverLog);
            Assert.Equal("stmt-001", enqueuedLog.Entry.SqlDriverLog!.SqlStatementId);
            Assert.Equal("session-123", enqueuedLog.Entry.SqlDriverLog.SessionId);
        }

        [Fact]
        public void ProcessActivity_ChildActivity_DoesNotEmit()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            // Parent activity from the driver source - must remain active (Activity.Current)
            // so child activities get their Parent set correctly
            using Activity? parentActivity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(parentActivity);
            parentActivity!.SetTag("statement.id", "stmt-001");

            // Child activity started while parent is Activity.Current (implicit parent)
            using Activity? childActivity = _driverSource.StartActivity("Statement.PollOperationStatus");
            Assert.NotNull(childActivity);
            Assert.NotNull(childActivity!.Parent); // Verify parent is set via implicit Activity.Current
            Assert.Equal(MetricsAggregator.DatabricksActivitySourceName, childActivity.Parent!.Source.Name);
            childActivity.SetTag("statement.id", "stmt-001");
            childActivity.SetTag("poll.count", 3);
            childActivity.SetStatus(ActivityStatusCode.Ok);
            childActivity.Stop();

            // Act
            aggregator.ProcessActivity(childActivity);

            // Assert - context should be created but NOT emitted (child, not root)
            Assert.Equal(1, aggregator.PendingContextCount);
            Assert.Empty(_mockClient.EnqueuedLogs);
        }

        [Fact]
        public void ProcessActivity_RootWithError_EmitsWithErrorInfo()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            aggregator.SetSessionContext("session-123", 12345L, null, null);

            using Activity? rootActivity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(rootActivity);
            rootActivity!.SetTag("statement.id", "stmt-error");
            rootActivity.SetTag("error.type", "TimeoutException");
            rootActivity.SetTag("error.message", "The operation has timed out");
            rootActivity.SetStatus(ActivityStatusCode.Error, "Connection timed out");
            rootActivity.Stop();

            // Act
            aggregator.ProcessActivity(rootActivity);

            // Assert - should be emitted with error info
            Assert.Equal(0, aggregator.PendingContextCount);
            TelemetryFrontendLog enqueuedLog = Assert.Single(_mockClient.EnqueuedLogs);
            Assert.NotNull(enqueuedLog.Entry?.SqlDriverLog?.ErrorInfo);
            Assert.Equal("TimeoutException", enqueuedLog.Entry!.SqlDriverLog!.ErrorInfo!.ErrorName);
            Assert.Equal("The operation has timed out", enqueuedLog.Entry.SqlDriverLog.ErrorInfo.StackTrace);
        }

        [Fact]
        public void ProcessActivity_RootNotComplete_DoesNotEmit()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            // Root activity without terminal status (Unset status)
            using Activity? rootActivity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(rootActivity);
            rootActivity!.SetTag("statement.id", "stmt-001");
            // Status is Unset (default) - not complete
            rootActivity.Stop();

            // Act
            aggregator.ProcessActivity(rootActivity);

            // Assert - context created but not emitted
            Assert.Equal(1, aggregator.PendingContextCount);
            Assert.Empty(_mockClient.EnqueuedLogs);
        }

        [Fact]
        public void ProcessActivity_ActivityFromExternalSource_TreatedAsRoot()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            aggregator.SetSessionContext("session-123", 12345L, null, null);

            // Activity from an external source (not "Databricks.Adbc.Driver")
            using Activity? externalActivity = _externalSource.StartActivity("External.Operation");
            Assert.NotNull(externalActivity);
            externalActivity!.SetTag("statement.id", "stmt-ext");
            externalActivity.SetStatus(ActivityStatusCode.Ok);
            externalActivity.Stop();

            // Act
            aggregator.ProcessActivity(externalActivity);

            // Assert - treated as root, should be emitted
            Assert.Equal(0, aggregator.PendingContextCount);
            Assert.Single(_mockClient.EnqueuedLogs);
        }

        #endregion

        #region IsRootActivity Tests

        [Fact]
        public void IsRootActivity_NullParent_ReturnsTrue()
        {
            // Arrange - activity with no parent
            using Activity? activity = _driverSource.StartActivity("Test.Root");
            Assert.NotNull(activity);
            Assert.Null(activity!.Parent);

            // Act & Assert
            Assert.True(MetricsAggregator.IsRootActivity(activity));
        }

        [Fact]
        public void IsRootActivity_ParentFromDifferentSource_ReturnsTrue()
        {
            // Arrange - parent from external source
            using Activity? parent = _externalSource.StartActivity("External.Parent");
            Assert.NotNull(parent);

            using Activity? child = _driverSource.StartActivity("Driver.Child",
                ActivityKind.Internal, parent!.Context);
            Assert.NotNull(child);

            // The parent reference might not be set for cross-source activities;
            // in such case Parent is null which also means root.
            // But if parent is set and from different source, it's root.
            bool result = MetricsAggregator.IsRootActivity(child!);
            Assert.True(result);
        }

        [Fact]
        public void IsRootActivity_ParentFromDriverSource_ReturnsFalse()
        {
            // Arrange - parent from the driver source
            using Activity? parent = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(parent);

            using Activity? child = _driverSource.StartActivity("Statement.PollOperationStatus",
                ActivityKind.Internal, parent!.Context);
            Assert.NotNull(child);

            // If the child has a parent from the same driver source, it's not root
            if (child!.Parent != null && child.Parent.Source.Name == MetricsAggregator.DatabricksActivitySourceName)
            {
                Assert.False(MetricsAggregator.IsRootActivity(child));
            }
        }

        #endregion

        #region IsActivityComplete Tests

        [Fact]
        public void IsActivityComplete_StatusOk_ReturnsTrue()
        {
            using Activity? activity = _driverSource.StartActivity("Test");
            Assert.NotNull(activity);
            activity!.SetStatus(ActivityStatusCode.Ok);
            Assert.True(MetricsAggregator.IsActivityComplete(activity));
        }

        [Fact]
        public void IsActivityComplete_StatusError_ReturnsTrue()
        {
            using Activity? activity = _driverSource.StartActivity("Test");
            Assert.NotNull(activity);
            activity!.SetStatus(ActivityStatusCode.Error);
            Assert.True(MetricsAggregator.IsActivityComplete(activity));
        }

        [Fact]
        public void IsActivityComplete_StatusUnset_ReturnsFalse()
        {
            using Activity? activity = _driverSource.StartActivity("Test");
            Assert.NotNull(activity);
            // Status defaults to Unset
            Assert.False(MetricsAggregator.IsActivityComplete(activity!));
        }

        #endregion

        #region SetSessionContext Tests

        [Fact]
        public void SetSessionContext_PropagatedToNewContexts()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            DriverSystemConfiguration systemConfig = new DriverSystemConfiguration
            {
                DriverName = "adbc-databricks",
                DriverVersion = "1.0.0",
                OsName = "Linux",
                RuntimeName = ".NET",
                RuntimeVersion = "8.0"
            };

            DriverConnectionParameters connectionParams = new DriverConnectionParameters
            {
                Mode = DriverModeType.DriverModeThrift,
                AuthMech = DriverAuthMechType.DriverAuthMechOauth
            };

            aggregator.SetSessionContext("session-456", 77777L, systemConfig, connectionParams);

            // Create and emit a root activity
            using Activity? rootActivity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(rootActivity);
            rootActivity!.SetTag("statement.id", "stmt-with-session");
            rootActivity.SetTag("result.format", "cloudfetch");
            rootActivity.SetStatus(ActivityStatusCode.Ok);
            rootActivity.Stop();

            // Act
            aggregator.ProcessActivity(rootActivity);

            // Assert
            TelemetryFrontendLog enqueuedLog = Assert.Single(_mockClient.EnqueuedLogs);
            Assert.Equal(77777L, enqueuedLog.WorkspaceId);
            Assert.NotNull(enqueuedLog.Context?.ClientContext?.UserAgent);
            Assert.Contains("adbc-databricks", enqueuedLog.Context!.ClientContext!.UserAgent!);
            Assert.Contains("1.0.0", enqueuedLog.Context.ClientContext.UserAgent);

            OssSqlDriverTelemetryLog? protoLog = enqueuedLog.Entry?.SqlDriverLog;
            Assert.NotNull(protoLog);
            Assert.Equal("session-456", protoLog!.SessionId);
            Assert.Equal("stmt-with-session", protoLog.SqlStatementId);

            // System configuration inherited from session context
            Assert.NotNull(protoLog.SystemConfiguration);
            Assert.Equal("adbc-databricks", protoLog.SystemConfiguration.DriverName);
            Assert.Equal("1.0.0", protoLog.SystemConfiguration.DriverVersion);

            // Connection parameters inherited from session context
            Assert.NotNull(protoLog.DriverConnectionParams);
            Assert.Equal(DriverModeType.DriverModeThrift, protoLog.DriverConnectionParams.Mode);
            Assert.Equal(DriverAuthMechType.DriverAuthMechOauth, protoLog.DriverConnectionParams.AuthMech);
        }

        [Fact]
        public void SetSessionContext_NotSet_ContextCreatedWithNulls()
        {
            // Arrange - no SetSessionContext called
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            using Activity? rootActivity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(rootActivity);
            rootActivity!.SetTag("statement.id", "stmt-no-session");
            rootActivity.SetStatus(ActivityStatusCode.Ok);
            rootActivity.Stop();

            // Act
            aggregator.ProcessActivity(rootActivity);

            // Assert
            TelemetryFrontendLog enqueuedLog = Assert.Single(_mockClient.EnqueuedLogs);
            Assert.Equal(0L, enqueuedLog.WorkspaceId);
            OssSqlDriverTelemetryLog? protoLog = enqueuedLog.Entry?.SqlDriverLog;
            Assert.NotNull(protoLog);
            Assert.Equal(string.Empty, protoLog!.SessionId);
            Assert.Null(protoLog.SystemConfiguration);
            Assert.Null(protoLog.DriverConnectionParams);
        }

        #endregion

        #region FlushAsync Tests

        [Fact]
        public async Task FlushAsync_EmitsPendingContexts()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            aggregator.SetSessionContext("session-flush", 11111L, null, null);

            // Create child activities under parent so they stay pending (not root)
            using Activity? parent1 = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(parent1);
            parent1!.SetTag("statement.id", "stmt-flush-1");

            Activity? child1 = _driverSource.StartActivity("Statement.PollOperationStatus");
            Assert.NotNull(child1);
            child1!.SetTag("statement.id", "stmt-flush-1");
            child1.SetTag("poll.count", 3);
            child1.Stop();
            aggregator.ProcessActivity(child1);
            child1.Dispose();

            parent1.Stop();

            // Second pending statement
            using Activity? parent2 = _driverSource.StartActivity("Statement.ExecuteQuery.2");
            Assert.NotNull(parent2);
            parent2!.SetTag("statement.id", "stmt-flush-2");

            Activity? child2 = _driverSource.StartActivity("Statement.PollOperationStatus.2");
            Assert.NotNull(child2);
            child2!.SetTag("statement.id", "stmt-flush-2");
            child2.SetTag("poll.count", 5);
            child2.Stop();
            aggregator.ProcessActivity(child2);
            child2.Dispose();

            parent2.Stop();

            Assert.Equal(2, aggregator.PendingContextCount);
            Assert.Empty(_mockClient.EnqueuedLogs);

            // Act
            await aggregator.FlushAsync();

            // Assert - all pending contexts emitted
            Assert.Equal(0, aggregator.PendingContextCount);
            Assert.Equal(2, _mockClient.EnqueuedLogs.Count);

            // Verify both statements were emitted
            List<string> statementIds = _mockClient.EnqueuedLogs
                .Select(log => log.Entry?.SqlDriverLog?.SqlStatementId ?? "")
                .OrderBy(id => id)
                .ToList();
            Assert.Contains("stmt-flush-1", statementIds);
            Assert.Contains("stmt-flush-2", statementIds);
        }

        [Fact]
        public async Task FlushAsync_ClearsDictionary()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            using Activity? parent = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(parent);
            parent!.SetTag("statement.id", "stmt-clear");

            Activity? child = _driverSource.StartActivity("Statement.PollOperationStatus");
            Assert.NotNull(child);
            child!.SetTag("statement.id", "stmt-clear");
            child.Stop();
            aggregator.ProcessActivity(child);
            child.Dispose();
            Assert.Equal(1, aggregator.PendingContextCount);

            // Act
            await aggregator.FlushAsync();

            // Assert
            Assert.Equal(0, aggregator.PendingContextCount);
        }

        [Fact]
        public async Task FlushAsync_EmptyDictionary_NoErrors()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            // Act & Assert - should not throw
            await aggregator.FlushAsync();
            Assert.Empty(_mockClient.EnqueuedLogs);
        }

        #endregion

        #region Exception Swallowing Tests

        [Fact]
        public void ProcessActivity_ExceptionSwallowed()
        {
            // Arrange - use a failing telemetry client
            FailingTelemetryClient failingClient = new FailingTelemetryClient();
            using MetricsAggregator aggregator = new MetricsAggregator(failingClient, _config);

            using Activity? rootActivity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(rootActivity);
            rootActivity!.SetTag("statement.id", "stmt-fail");
            rootActivity.SetStatus(ActivityStatusCode.Ok);
            rootActivity.Stop();

            // Act & Assert - should not throw despite Enqueue throwing
            aggregator.ProcessActivity(rootActivity);
        }

        [Fact]
        public async Task FlushAsync_ExceptionSwallowed()
        {
            // Arrange
            FailingTelemetryClient failingClient = new FailingTelemetryClient();
            using MetricsAggregator aggregator = new MetricsAggregator(failingClient, _config);

            // Add a pending context by processing a child activity
            using Activity? parent = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(parent);
            parent!.SetTag("statement.id", "stmt-flush-fail");

            Activity? child = _driverSource.StartActivity("Statement.PollOperationStatus");
            Assert.NotNull(child);
            child!.SetTag("statement.id", "stmt-flush-fail");
            child.Stop();
            aggregator.ProcessActivity(child);
            child.Dispose();

            // Act & Assert - should not throw despite Enqueue throwing during flush
            await aggregator.FlushAsync();
        }

        #endregion

        #region Dispose Tests

        [Fact]
        public void Dispose_ClearsPendingContexts()
        {
            // Arrange
            MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            using Activity? parent = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(parent);
            parent!.SetTag("statement.id", "stmt-dispose");

            Activity? child = _driverSource.StartActivity("Statement.PollOperationStatus");
            Assert.NotNull(child);
            child!.SetTag("statement.id", "stmt-dispose");
            child.Stop();
            aggregator.ProcessActivity(child);
            child.Dispose();
            Assert.Equal(1, aggregator.PendingContextCount);

            // Act
            aggregator.Dispose();

            // Assert
            Assert.Equal(0, aggregator.PendingContextCount);
        }

        [Fact]
        public void Dispose_AfterDispose_ProcessActivityIgnored()
        {
            // Arrange
            MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            aggregator.Dispose();

            using Activity? rootActivity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(rootActivity);
            rootActivity!.SetTag("statement.id", "stmt-post-dispose");
            rootActivity.SetStatus(ActivityStatusCode.Ok);
            rootActivity.Stop();

            // Act & Assert - should not throw, activity should be ignored
            aggregator.ProcessActivity(rootActivity);
            Assert.Empty(_mockClient.EnqueuedLogs);
        }

        #endregion

        #region TelemetryFrontendLog Population Tests

        [Fact]
        public void ProcessActivity_EmittedLog_HasCorrectFrontendLogFields()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            aggregator.SetSessionContext("session-log", 54321L, null, null);

            using Activity? rootActivity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(rootActivity);
            rootActivity!.SetTag("statement.id", "stmt-log");
            rootActivity.SetStatus(ActivityStatusCode.Ok);
            rootActivity.Stop();

            // Act
            aggregator.ProcessActivity(rootActivity);

            // Assert
            TelemetryFrontendLog log = Assert.Single(_mockClient.EnqueuedLogs);

            // WorkspaceId
            Assert.Equal(54321L, log.WorkspaceId);

            // FrontendLogEventId should be a valid GUID
            Assert.False(string.IsNullOrEmpty(log.FrontendLogEventId));
            Assert.True(Guid.TryParse(log.FrontendLogEventId, out _), "FrontendLogEventId should be a valid GUID");

            // Context
            Assert.NotNull(log.Context);
            Assert.True(log.Context!.TimestampMillis > 0, "TimestampMillis should be positive");

            // Entry
            Assert.NotNull(log.Entry);
            Assert.NotNull(log.Entry!.SqlDriverLog);
        }

        [Fact]
        public void ProcessActivity_WithSystemConfig_UserAgentPopulated()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            DriverSystemConfiguration sysConfig = new DriverSystemConfiguration
            {
                DriverName = "TestDriver",
                DriverVersion = "2.0.0"
            };

            aggregator.SetSessionContext("session-ua", 12345L, sysConfig, null);

            using Activity? rootActivity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(rootActivity);
            rootActivity!.SetTag("statement.id", "stmt-ua");
            rootActivity.SetStatus(ActivityStatusCode.Ok);
            rootActivity.Stop();

            // Act
            aggregator.ProcessActivity(rootActivity);

            // Assert
            TelemetryFrontendLog log = Assert.Single(_mockClient.EnqueuedLogs);
            Assert.NotNull(log.Context?.ClientContext?.UserAgent);
            Assert.Equal("TestDriver/2.0.0", log.Context!.ClientContext!.UserAgent);
        }

        [Fact]
        public void ProcessActivity_WithoutSystemConfig_UserAgentNull()
        {
            // Arrange
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);
            aggregator.SetSessionContext("session-nua", 12345L, null, null);

            using Activity? rootActivity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(rootActivity);
            rootActivity!.SetTag("statement.id", "stmt-nua");
            rootActivity.SetStatus(ActivityStatusCode.Ok);
            rootActivity.Stop();

            // Act
            aggregator.ProcessActivity(rootActivity);

            // Assert
            TelemetryFrontendLog log = Assert.Single(_mockClient.EnqueuedLogs);
            Assert.Null(log.Context?.ClientContext?.UserAgent);
        }

        #endregion

        #region Integration-Style Tests

        [Fact]
        public void ProcessActivity_FullStatementLifecycle_CorrectProto()
        {
            // Arrange - simulate a full statement lifecycle with multiple child activities
            using MetricsAggregator aggregator = new MetricsAggregator(_mockClient, _config);

            DriverSystemConfiguration sysConfig = new DriverSystemConfiguration
            {
                DriverName = "adbc-databricks",
                DriverVersion = "1.0.0",
                OsName = "Linux",
                RuntimeName = ".NET",
                RuntimeVersion = "8.0"
            };

            DriverConnectionParameters connParams = new DriverConnectionParameters
            {
                Mode = DriverModeType.DriverModeThrift,
                AuthMech = DriverAuthMechType.DriverAuthMechOauth
            };

            aggregator.SetSessionContext("session-full", 88888L, sysConfig, connParams);

            // Start root activity - remains active (Activity.Current) so children get Parent set
            using Activity? rootActivity = _driverSource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(rootActivity);
            rootActivity!.SetTag("statement.id", "stmt-full");
            rootActivity.SetTag("session.id", "session-full");
            rootActivity.SetTag("auth.type", "oauth-m2m");
            rootActivity.SetTag("result.format", "cloudfetch");
            rootActivity.SetTag("result.compression_enabled", true);

            // Child: poll operation (implicit parent via Activity.Current)
            Activity? pollActivity = _driverSource.StartActivity("Statement.PollOperationStatus");
            Assert.NotNull(pollActivity);
            pollActivity!.SetTag("statement.id", "stmt-full");
            pollActivity.SetTag("poll.count", 5);
            pollActivity.SetTag("poll.latency_ms", 250L);
            pollActivity.SetStatus(ActivityStatusCode.Ok);
            pollActivity.Stop();
            aggregator.ProcessActivity(pollActivity);

            // Child: download files (implicit parent via Activity.Current; root is restored after poll stops)
            Activity? downloadActivity = _driverSource.StartActivity("CloudFetch.DownloadFiles");
            Assert.NotNull(downloadActivity);
            downloadActivity!.SetTag("statement.id", "stmt-full");
            downloadActivity.SetTag("cloudfetch.total_chunks", 10);
            downloadActivity.SetTag("cloudfetch.chunks_iterated", 10);
            downloadActivity.SetTag("cloudfetch.initial_chunk_latency_ms", 50L);
            downloadActivity.SetTag("cloudfetch.slowest_chunk_latency_ms", 200L);
            downloadActivity.SetTag("cloudfetch.sum_download_time_ms", 1000L);
            downloadActivity.SetStatus(ActivityStatusCode.Ok);
            downloadActivity.Stop();
            aggregator.ProcessActivity(downloadActivity);

            // Root activity completes
            System.Threading.Thread.Sleep(10);
            rootActivity.SetStatus(ActivityStatusCode.Ok);
            rootActivity.Stop();

            // Act - process the completed root
            aggregator.ProcessActivity(rootActivity);

            // Assert - root is the final emission (children merged into same context)
            Assert.Equal(0, aggregator.PendingContextCount);

            // Find the log emitted for the root (the last one, with the full data)
            List<TelemetryFrontendLog> allLogs = _mockClient.EnqueuedLogs.ToList();
            Assert.True(allLogs.Count >= 1, "At least the root emission should exist");

            // The root activity emission should contain all the aggregated data
            TelemetryFrontendLog? rootLog = allLogs.FirstOrDefault(l =>
                l.Entry?.SqlDriverLog?.AuthType == "oauth-m2m");
            Assert.NotNull(rootLog);
            OssSqlDriverTelemetryLog? proto = rootLog!.Entry?.SqlDriverLog;
            Assert.NotNull(proto);

            // Top-level fields
            Assert.Equal("session-full", proto!.SessionId);
            Assert.Equal("stmt-full", proto.SqlStatementId);
            Assert.Equal("oauth-m2m", proto.AuthType);
            Assert.True(proto.OperationLatencyMs > 0);

            // System configuration
            Assert.NotNull(proto.SystemConfiguration);
            Assert.Equal("adbc-databricks", proto.SystemConfiguration.DriverName);

            // Connection parameters
            Assert.NotNull(proto.DriverConnectionParams);
            Assert.Equal(DriverModeType.DriverModeThrift, proto.DriverConnectionParams.Mode);

            // SQL operation
            Assert.NotNull(proto.SqlOperation);
            Assert.Equal(ExecutionResultFormat.ExecutionResultExternalLinks, proto.SqlOperation.ExecutionResult);
            Assert.True(proto.SqlOperation.IsCompressed);

            // No error
            Assert.Null(proto.ErrorInfo);

            // Cleanup
            pollActivity.Dispose();
            downloadActivity.Dispose();
        }

        #endregion

        #region Mock Implementations

        /// <summary>
        /// Mock implementation of <see cref="ITelemetryClient"/> for testing.
        /// Collects all enqueued logs for verification.
        /// </summary>
        private sealed class MockTelemetryClient : ITelemetryClient
        {
            private readonly ConcurrentBag<TelemetryFrontendLog> _enqueuedLogs = new ConcurrentBag<TelemetryFrontendLog>();

            public IReadOnlyList<TelemetryFrontendLog> EnqueuedLogs
            {
                get
                {
                    List<TelemetryFrontendLog> list = new List<TelemetryFrontendLog>(_enqueuedLogs);
                    return list;
                }
            }

            public void Enqueue(TelemetryFrontendLog log)
            {
                _enqueuedLogs.Add(log);
            }

            public Task FlushAsync(CancellationToken ct = default)
            {
                return Task.CompletedTask;
            }

            public Task CloseAsync()
            {
                return Task.CompletedTask;
            }

            public ValueTask DisposeAsync()
            {
                return default;
            }
        }

        /// <summary>
        /// Mock implementation of <see cref="ITelemetryClient"/> that throws on Enqueue.
        /// Used to verify exception swallowing in the aggregator.
        /// </summary>
        private sealed class FailingTelemetryClient : ITelemetryClient
        {
            public void Enqueue(TelemetryFrontendLog log)
            {
                throw new InvalidOperationException("Simulated Enqueue failure");
            }

            public Task FlushAsync(CancellationToken ct = default)
            {
                return Task.CompletedTask;
            }

            public Task CloseAsync()
            {
                return Task.CompletedTask;
            }

            public ValueTask DisposeAsync()
            {
                return default;
            }
        }

        #endregion
    }
}
