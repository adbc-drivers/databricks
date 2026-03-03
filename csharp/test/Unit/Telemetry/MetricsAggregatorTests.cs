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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.Databricks.Telemetry.Proto;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for <see cref="MetricsAggregator"/>.
    /// Uses real Activity objects created from ActivitySource and a manual mock ITelemetryClient.
    /// </summary>
    public class MetricsAggregatorTests : IDisposable
    {
        /// <summary>
        /// ActivitySource that mimics the main Databricks driver source.
        /// Activities from this source are treated as "same source" for IsRootActivity checks.
        /// </summary>
        private static readonly ActivitySource DriverSource = new(MetricsAggregator.DatabricksActivitySourceName);

        /// <summary>
        /// A secondary ActivitySource representing an external/different source.
        /// Activities from this source would have parents from a different source.
        /// </summary>
        private static readonly ActivitySource ExternalSource = new("External.TestSource");

        private readonly ActivityListener _listener;
        private readonly MockTelemetryClient _mockClient;
        private readonly TelemetryConfiguration _config;

        public MetricsAggregatorTests()
        {
            // Enable all activities for our test sources
            _listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(_listener);

            _mockClient = new MockTelemetryClient();
            _config = new TelemetryConfiguration();
        }

        public void Dispose()
        {
            _listener.Dispose();
        }

        #region Constructor Tests

        [Fact]
        public void Constructor_NullTelemetryClient_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new MetricsAggregator(null!, new TelemetryConfiguration()));
        }

        [Fact]
        public void Constructor_NullConfig_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new MetricsAggregator(_mockClient, null!));
        }

        [Fact]
        public void Constructor_ValidParameters_CreatesInstance()
        {
            // Act
            var aggregator = new MetricsAggregator(_mockClient, _config);

            // Assert
            Assert.NotNull(aggregator);
        }

        [Fact]
        public void Constructor_WithOptionalParameters_CreatesInstance()
        {
            // Arrange
            var systemConfig = new DriverSystemConfiguration { DriverVersion = "1.0.0" };
            var connParams = new DriverConnectionParameters { HttpPath = "/sql/1.0/warehouses/abc" };

            // Act
            var aggregator = new MetricsAggregator(
                _mockClient, _config,
                sessionId: "sess-123",
                systemConfig: systemConfig,
                connectionParams: connParams,
                workspaceId: 12345);

            // Assert
            Assert.NotNull(aggregator);
        }

        #endregion

        #region ProcessActivity - Statement Aggregation Tests

        [Fact]
        public void ProcessActivity_Statement_AggregatesByStatementId()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config, sessionId: "sess-1");

            // Act - Simulate real activity hierarchy: root starts first, children created inside it
            // Children stop before root (stack unwinding), then root stops
            using (var rootActivity = DriverSource.StartActivity("ExecuteQuery"))
            {
                rootActivity!.SetTag("statement.id", "stmt-100");
                rootActivity.SetTag("session.id", "sess-1");
                rootActivity.SetTag("statement.type", "query");

                // Child 1: DownloadFiles (stops before root)
                using (var downloadActivity = DriverSource.StartActivity("DownloadFiles"))
                {
                    downloadActivity!.SetTag("statement.id", "stmt-100");
                    downloadActivity.SetTag("session.id", "sess-1");
                    downloadActivity.SetStatus(ActivityStatusCode.Ok);
                    downloadActivity.Stop();
                    aggregator.ProcessActivity(downloadActivity);
                }

                // No emission yet - child has parent from same source
                Assert.Equal(0, _mockClient.EnqueueCallCount);

                // Child 2: PollOperationStatus (stops before root)
                using (var pollActivity = DriverSource.StartActivity("PollOperationStatus"))
                {
                    pollActivity!.SetTag("statement.id", "stmt-100");
                    pollActivity.SetTag("session.id", "sess-1");
                    pollActivity.SetTag("poll.count", 5);
                    pollActivity.SetTag("poll.latency_ms", 200L);
                    pollActivity.SetStatus(ActivityStatusCode.Ok);
                    pollActivity.Stop();
                    aggregator.ProcessActivity(pollActivity);
                }

                // Still no emission - child activities don't trigger emission
                Assert.Equal(0, _mockClient.EnqueueCallCount);

                // Now the root activity completes
                rootActivity.SetStatus(ActivityStatusCode.Ok);
                rootActivity.Stop();
                aggregator.ProcessActivity(rootActivity);
            }

            // Assert - Root completed, so proto should be emitted with all aggregated data
            Assert.Equal(1, _mockClient.EnqueueCallCount);
            var log = _mockClient.LastEnqueuedLog;
            Assert.NotNull(log);
            Assert.NotNull(log!.Entry?.SqlDriverLog);
            Assert.Equal("stmt-100", log.Entry!.SqlDriverLog!.SqlStatementId);
        }

        [Fact]
        public void ProcessActivity_RootActivityComplete_EmitsProto()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config, sessionId: "sess-1", workspaceId: 99);

            // Act - Single root activity that completes
            using (var activity = DriverSource.StartActivity("ExecuteQuery"))
            {
                activity!.SetTag("statement.id", "stmt-200");
                activity.SetTag("session.id", "sess-1");
                activity.SetTag("statement.type", "query");
                activity.SetStatus(ActivityStatusCode.Ok);
                activity.Stop();
                aggregator.ProcessActivity(activity);
            }

            // Assert
            Assert.Equal(1, _mockClient.EnqueueCallCount);
            var log = _mockClient.LastEnqueuedLog;
            Assert.NotNull(log);
            Assert.Equal(99, log!.WorkspaceId);
            Assert.NotNull(log.FrontendLogEventId);
            Assert.NotEmpty(log.FrontendLogEventId);
            Assert.NotNull(log.Context);
            Assert.True(log.Context!.TimestampMillis > 0);
            Assert.NotNull(log.Entry);
            Assert.NotNull(log.Entry!.SqlDriverLog);
            Assert.Equal("stmt-200", log.Entry.SqlDriverLog!.SqlStatementId);
        }

        [Fact]
        public void ProcessActivity_ChildActivity_DoesNotEmit()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config, sessionId: "sess-1");

            // Act - Create a root activity, then a child under it
            using (var rootActivity = DriverSource.StartActivity("ExecuteQuery"))
            {
                rootActivity!.SetTag("statement.id", "stmt-300");
                rootActivity.SetTag("session.id", "sess-1");

                // Child activity under root (same ActivitySource)
                using (var childActivity = DriverSource.StartActivity("DownloadFiles"))
                {
                    childActivity!.SetTag("statement.id", "stmt-300");
                    childActivity.SetTag("session.id", "sess-1");
                    childActivity.SetStatus(ActivityStatusCode.Ok);
                    childActivity.Stop();
                    aggregator.ProcessActivity(childActivity);
                }

                // Assert - Child completed but it's not root, so no emission
                Assert.Equal(0, _mockClient.EnqueueCallCount);
            }
        }

        [Fact]
        public void ProcessActivity_MultipleStatements_SeparateContexts()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config, sessionId: "sess-1");

            // Act - Two different statements
            using (var activity1 = DriverSource.StartActivity("ExecuteQuery"))
            {
                activity1!.SetTag("statement.id", "stmt-A");
                activity1.SetTag("session.id", "sess-1");
                activity1.SetTag("statement.type", "query");
                activity1.SetStatus(ActivityStatusCode.Ok);
                activity1.Stop();
                aggregator.ProcessActivity(activity1);
            }

            using (var activity2 = DriverSource.StartActivity("ExecuteUpdate"))
            {
                activity2!.SetTag("statement.id", "stmt-B");
                activity2.SetTag("session.id", "sess-1");
                activity2.SetTag("statement.type", "update");
                activity2.SetStatus(ActivityStatusCode.Ok);
                activity2.Stop();
                aggregator.ProcessActivity(activity2);
            }

            // Assert - Two separate emissions
            Assert.Equal(2, _mockClient.EnqueueCallCount);
            Assert.Equal(2, _mockClient.AllEnqueuedLogs.Count);

            var statementIds = new HashSet<string>();
            foreach (var log in _mockClient.AllEnqueuedLogs)
            {
                statementIds.Add(log.Entry!.SqlDriverLog!.SqlStatementId);
            }
            Assert.Contains("stmt-A", statementIds);
            Assert.Contains("stmt-B", statementIds);
        }

        [Fact]
        public void ProcessActivity_NoStatementId_Skipped()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config);

            // Act - Activity without statement.id tag
            using (var activity = DriverSource.StartActivity("SomeActivity"))
            {
                activity!.SetTag("session.id", "sess-1");
                // No statement.id tag!
                activity.SetStatus(ActivityStatusCode.Ok);
                activity.Stop();
                aggregator.ProcessActivity(activity);
            }

            // Assert - Should be skipped, no emission
            Assert.Equal(0, _mockClient.EnqueueCallCount);
        }

        [Fact]
        public void ProcessActivity_EmptyStatementId_Skipped()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config);

            // Act - Activity with empty statement.id
            using (var activity = DriverSource.StartActivity("ExecuteQuery"))
            {
                activity!.SetTag("statement.id", "");
                activity.SetStatus(ActivityStatusCode.Ok);
                activity.Stop();
                aggregator.ProcessActivity(activity);
            }

            // Assert - Should be skipped
            Assert.Equal(0, _mockClient.EnqueueCallCount);
        }

        [Fact]
        public void ProcessActivity_NullActivity_NoException()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config);

            // Act & Assert - Should not throw
            var ex = Record.Exception(() => aggregator.ProcessActivity(null!));
            Assert.Null(ex);
            Assert.Equal(0, _mockClient.EnqueueCallCount);
        }

        #endregion

        #region ProcessActivity - Connection.Open Tests

        [Fact]
        public void ProcessActivity_ConnectionOpen_EmitsImmediately()
        {
            // Arrange
            var aggregator = new MetricsAggregator(
                _mockClient, _config,
                sessionId: "sess-1",
                workspaceId: 42);

            // Act - Connection.Open activity (no statement.id needed)
            using (var activity = DriverSource.StartActivity("Connection.Open"))
            {
                activity!.SetTag("session.id", "sess-1");
                activity.SetTag("workspace.id", "42");
                activity.SetTag("driver.version", "1.0.0");
                activity.SetStatus(ActivityStatusCode.Ok);
                activity.Stop();
                aggregator.ProcessActivity(activity);
            }

            // Assert - Should emit immediately
            Assert.Equal(1, _mockClient.EnqueueCallCount);
            var log = _mockClient.LastEnqueuedLog;
            Assert.NotNull(log);
            Assert.Equal(42, log!.WorkspaceId);
            Assert.NotNull(log.Entry?.SqlDriverLog);
        }

        [Fact]
        public void ProcessActivity_ConnectionOpenAsync_EmitsImmediately()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config, sessionId: "sess-1");

            // Act - Connection.OpenAsync variant
            using (var activity = DriverSource.StartActivity("Connection.OpenAsync"))
            {
                activity!.SetTag("session.id", "sess-1");
                activity.SetStatus(ActivityStatusCode.Ok);
                activity.Stop();
                aggregator.ProcessActivity(activity);
            }

            // Assert - Should emit immediately
            Assert.Equal(1, _mockClient.EnqueueCallCount);
        }

        [Fact]
        public void ProcessActivity_ConnectionOpen_DoesNotRequireStatementId()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config, sessionId: "sess-1");

            // Act - Connection.Open without statement.id (which is the normal case)
            using (var activity = DriverSource.StartActivity("Connection.Open"))
            {
                activity!.SetTag("session.id", "sess-1");
                // No statement.id - this is normal for connection open
                activity.SetStatus(ActivityStatusCode.Ok);
                activity.Stop();
                aggregator.ProcessActivity(activity);
            }

            // Assert - Should still emit
            Assert.Equal(1, _mockClient.EnqueueCallCount);
        }

        #endregion

        #region ProcessActivity - Root Activity Detection Tests

        [Fact]
        public void IsRootActivity_NoParent_ReturnsTrue()
        {
            // Act - Activity with no parent
            using (var activity = DriverSource.StartActivity("ExecuteQuery"))
            {
                // Assert - No parent means root
                Assert.True(MetricsAggregator.IsRootActivity(activity!));
            }
        }

        [Fact]
        public void IsRootActivity_ParentFromSameSource_ReturnsFalse()
        {
            // Act - Activity with parent from same source
            using (var parent = DriverSource.StartActivity("ExecuteQuery"))
            {
                using (var child = DriverSource.StartActivity("DownloadFiles"))
                {
                    // Assert - Child has parent from same ActivitySource
                    Assert.False(MetricsAggregator.IsRootActivity(child!));
                }
            }
        }

        [Fact]
        public void IsRootActivity_ParentFromDifferentSource_ReturnsTrue()
        {
            // Act - Activity with parent from different source
            using (var externalParent = ExternalSource.StartActivity("ExternalOp"))
            {
                using (var driverActivity = DriverSource.StartActivity("ExecuteQuery"))
                {
                    // Assert - Parent is from different source, so this is root for our purposes
                    Assert.True(MetricsAggregator.IsRootActivity(driverActivity!));
                }
            }
        }

        [Fact]
        public void IsActivityComplete_Ok_ReturnsTrue()
        {
            using (var activity = DriverSource.StartActivity("Test"))
            {
                activity!.SetStatus(ActivityStatusCode.Ok);
                Assert.True(MetricsAggregator.IsActivityComplete(activity));
            }
        }

        [Fact]
        public void IsActivityComplete_Error_ReturnsTrue()
        {
            using (var activity = DriverSource.StartActivity("Test"))
            {
                activity!.SetStatus(ActivityStatusCode.Error, "test error");
                Assert.True(MetricsAggregator.IsActivityComplete(activity));
            }
        }

        [Fact]
        public void IsActivityComplete_Unset_ReturnsFalse()
        {
            using (var activity = DriverSource.StartActivity("Test"))
            {
                // Default status is Unset
                Assert.False(MetricsAggregator.IsActivityComplete(activity!));
            }
        }

        [Fact]
        public void IsConnectionOpenActivity_ConnectionOpen_ReturnsTrue()
        {
            using (var activity = DriverSource.StartActivity("Connection.Open"))
            {
                Assert.True(MetricsAggregator.IsConnectionOpenActivity(activity!));
            }
        }

        [Fact]
        public void IsConnectionOpenActivity_ConnectionOpenAsync_ReturnsTrue()
        {
            using (var activity = DriverSource.StartActivity("Connection.OpenAsync"))
            {
                Assert.True(MetricsAggregator.IsConnectionOpenActivity(activity!));
            }
        }

        [Fact]
        public void IsConnectionOpenActivity_ExecuteQuery_ReturnsFalse()
        {
            using (var activity = DriverSource.StartActivity("ExecuteQuery"))
            {
                Assert.False(MetricsAggregator.IsConnectionOpenActivity(activity!));
            }
        }

        #endregion

        #region ProcessActivity - Error Handling Tests

        [Fact]
        public void ProcessActivity_RootWithError_EmitsProtoWithError()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config, sessionId: "sess-1");

            // Act - Root activity that errors
            using (var activity = DriverSource.StartActivity("ExecuteQuery"))
            {
                activity!.SetTag("statement.id", "stmt-err");
                activity.SetTag("session.id", "sess-1");
                activity.SetTag("error.type", "InvalidSqlException");
                activity.SetStatus(ActivityStatusCode.Error, "SQL syntax error");
                activity.Stop();
                aggregator.ProcessActivity(activity);
            }

            // Assert - Proto emitted with error info
            Assert.Equal(1, _mockClient.EnqueueCallCount);
            var log = _mockClient.LastEnqueuedLog;
            Assert.NotNull(log?.Entry?.SqlDriverLog?.ErrorInfo);
            Assert.Equal("InvalidSqlException", log!.Entry!.SqlDriverLog!.ErrorInfo.ErrorName);
        }

        [Fact]
        public void ProcessActivity_ExceptionSwallowed()
        {
            // Arrange - Use a telemetry client that throws on Enqueue
            var throwingClient = new ThrowingTelemetryClient();
            var aggregator = new MetricsAggregator(throwingClient, _config, sessionId: "sess-1");

            // Act & Assert - No exception should propagate
            var ex = Record.Exception(() =>
            {
                using (var activity = DriverSource.StartActivity("ExecuteQuery"))
                {
                    activity!.SetTag("statement.id", "stmt-throw");
                    activity.SetTag("session.id", "sess-1");
                    activity.SetStatus(ActivityStatusCode.Ok);
                    activity.Stop();
                    aggregator.ProcessActivity(activity);
                }
            });

            Assert.Null(ex);
        }

        #endregion

        #region FlushAsync Tests

        [Fact]
        public async Task FlushAsync_EmitsPendingContexts()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config, sessionId: "sess-1");

            // Simulate two in-flight statements where child activities have been processed
            // but root activities haven't completed yet.
            // We create parent activities that stay open, process children inside them.

            // Statement 1: Root still open, child processed
            var root1 = DriverSource.StartActivity("ExecuteQuery");
            root1!.SetTag("statement.id", "stmt-flush-1");
            root1.SetTag("session.id", "sess-1");

            using (var child1 = DriverSource.StartActivity("DownloadFiles"))
            {
                child1!.SetTag("statement.id", "stmt-flush-1");
                child1.SetTag("session.id", "sess-1");
                child1.SetStatus(ActivityStatusCode.Ok);
                child1.Stop();
                aggregator.ProcessActivity(child1);
            }

            // Statement 2: Root still open, child processed
            var root2 = DriverSource.StartActivity("ExecuteQuery");
            root2!.SetTag("statement.id", "stmt-flush-2");
            root2.SetTag("session.id", "sess-1");

            using (var child2 = DriverSource.StartActivity("PollOperationStatus"))
            {
                child2!.SetTag("statement.id", "stmt-flush-2");
                child2.SetTag("session.id", "sess-1");
                child2.SetTag("poll.count", 3);
                child2.SetStatus(ActivityStatusCode.Ok);
                child2.Stop();
                aggregator.ProcessActivity(child2);
            }

            // Confirm nothing emitted yet (children don't trigger emission)
            Assert.Equal(0, _mockClient.EnqueueCallCount);

            // Act - Flush (simulates connection close)
            await aggregator.FlushAsync();

            // Assert - Both pending contexts should be emitted
            Assert.Equal(2, _mockClient.EnqueueCallCount);

            // Cleanup root activities
            root2!.Dispose();
            root1!.Dispose();
        }

        [Fact]
        public async Task FlushAsync_AfterRootComplete_NoPendingContexts()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config, sessionId: "sess-1");

            // Complete a root activity (this emits the proto)
            using (var activity = DriverSource.StartActivity("ExecuteQuery"))
            {
                activity!.SetTag("statement.id", "stmt-flush-done");
                activity.SetTag("session.id", "sess-1");
                activity.SetStatus(ActivityStatusCode.Ok);
                activity.Stop();
                aggregator.ProcessActivity(activity);
            }

            Assert.Equal(1, _mockClient.EnqueueCallCount);

            // Act - Flush should have nothing to emit
            await aggregator.FlushAsync();

            // Assert - No additional emissions
            Assert.Equal(1, _mockClient.EnqueueCallCount);
        }

        [Fact]
        public async Task FlushAsync_EmptyAggregator_NoException()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config);

            // Act & Assert - Should not throw with no pending contexts
            var ex = await Record.ExceptionAsync(() => aggregator.FlushAsync());
            Assert.Null(ex);
            Assert.Equal(0, _mockClient.EnqueueCallCount);
        }

        [Fact]
        public async Task FlushAsync_AfterDispose_NoException()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config, sessionId: "sess-1");

            // Add a pending context: create child inside parent (parent stays open)
            var parent = DriverSource.StartActivity("ExecuteQuery");
            parent!.SetTag("statement.id", "stmt-disposed");
            parent.SetTag("session.id", "sess-1");

            using (var child = DriverSource.StartActivity("DownloadFiles"))
            {
                child!.SetTag("statement.id", "stmt-disposed");
                child.SetTag("session.id", "sess-1");
                child.SetStatus(ActivityStatusCode.Ok);
                child.Stop();
                aggregator.ProcessActivity(child);
            }

            // Dispose
            aggregator.Dispose();
            parent.Dispose();

            // Act & Assert - Flush after dispose should not throw
            var ex = await Record.ExceptionAsync(() => aggregator.FlushAsync());
            Assert.Null(ex);
        }

        [Fact]
        public async Task FlushAsync_ClientThrows_ExceptionSwallowed()
        {
            // Arrange
            var throwingClient = new ThrowingTelemetryClient();
            var aggregator = new MetricsAggregator(throwingClient, _config, sessionId: "sess-1");

            // Add a pending context: create child inside parent (parent stays open)
            var parent = DriverSource.StartActivity("ExecuteQuery");
            parent!.SetTag("statement.id", "stmt-throw-flush");
            parent.SetTag("session.id", "sess-1");

            using (var child = DriverSource.StartActivity("DownloadFiles"))
            {
                child!.SetTag("statement.id", "stmt-throw-flush");
                child.SetTag("session.id", "sess-1");
                child.SetStatus(ActivityStatusCode.Ok);
                child.Stop();
                aggregator.ProcessActivity(child);
            }

            // Act & Assert - Flush should not throw even if client throws
            var ex = await Record.ExceptionAsync(() => aggregator.FlushAsync());
            Assert.Null(ex);

            parent.Dispose();
        }

        #endregion

        #region Dispose Tests

        [Fact]
        public void Dispose_ClearsPendingState()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config, sessionId: "sess-1");

            // Add a pending context: create a child inside a parent (parent stays open)
            var parentActivity = DriverSource.StartActivity("ExecuteQuery");
            parentActivity!.SetTag("statement.id", "stmt-dispose");
            parentActivity.SetTag("session.id", "sess-1");

            using (var child = DriverSource.StartActivity("DownloadFiles"))
            {
                child!.SetTag("statement.id", "stmt-dispose");
                child.SetTag("session.id", "sess-1");
                child.SetStatus(ActivityStatusCode.Ok);
                child.Stop();
                aggregator.ProcessActivity(child);
            }

            // Confirm child didn't trigger emission
            Assert.Equal(0, _mockClient.EnqueueCallCount);

            // Act
            aggregator.Dispose();

            // Assert - ProcessActivity should be a no-op after dispose
            using (var activity = DriverSource.StartActivity("ExecuteQuery"))
            {
                activity!.SetTag("statement.id", "stmt-after-dispose");
                activity.SetTag("session.id", "sess-1");
                activity.SetStatus(ActivityStatusCode.Ok);
                activity.Stop();
                aggregator.ProcessActivity(activity);
            }

            Assert.Equal(0, _mockClient.EnqueueCallCount);

            parentActivity.Dispose();
        }

        [Fact]
        public void Dispose_MultipleCalls_NoException()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config);

            // Act & Assert - Multiple dispose calls should not throw
            var ex = Record.Exception(() =>
            {
                aggregator.Dispose();
                aggregator.Dispose();
            });
            Assert.Null(ex);
        }

        #endregion

        #region Connection-Level Context Tests

        [Fact]
        public void ProcessActivity_InheritsSessionId()
        {
            // Arrange - Create aggregator with pre-set session ID
            var aggregator = new MetricsAggregator(
                _mockClient, _config,
                sessionId: "pre-set-session");

            // Act - Activity that doesn't have session.id
            using (var activity = DriverSource.StartActivity("ExecuteQuery"))
            {
                activity!.SetTag("statement.id", "stmt-inherit");
                activity.SetStatus(ActivityStatusCode.Ok);
                activity.Stop();
                aggregator.ProcessActivity(activity);
            }

            // Assert - Proto should have the pre-set session ID
            Assert.Equal(1, _mockClient.EnqueueCallCount);
            var log = _mockClient.LastEnqueuedLog;
            Assert.Equal("pre-set-session", log!.Entry!.SqlDriverLog!.SessionId);
        }

        [Fact]
        public void ProcessActivity_InheritsSystemConfig()
        {
            // Arrange - Create aggregator with pre-built system config
            var systemConfig = new DriverSystemConfiguration
            {
                DriverVersion = "2.0.0",
                OsName = "Linux"
            };
            var aggregator = new MetricsAggregator(
                _mockClient, _config,
                sessionId: "sess-1",
                systemConfig: systemConfig);

            // Act
            using (var activity = DriverSource.StartActivity("ExecuteQuery"))
            {
                activity!.SetTag("statement.id", "stmt-config");
                activity.SetStatus(ActivityStatusCode.Ok);
                activity.Stop();
                aggregator.ProcessActivity(activity);
            }

            // Assert - Proto should include pre-built system config
            Assert.Equal(1, _mockClient.EnqueueCallCount);
            var log = _mockClient.LastEnqueuedLog;
            Assert.NotNull(log!.Entry!.SqlDriverLog!.SystemConfiguration);
            Assert.Equal("2.0.0", log.Entry.SqlDriverLog.SystemConfiguration.DriverVersion);
            Assert.Equal("Linux", log.Entry.SqlDriverLog.SystemConfiguration.OsName);
        }

        [Fact]
        public void ProcessActivity_BuildsFrontendLogEnvelope()
        {
            // Arrange
            var aggregator = new MetricsAggregator(
                _mockClient, _config,
                sessionId: "sess-1",
                workspaceId: 777);

            // Act
            using (var activity = DriverSource.StartActivity("ExecuteQuery"))
            {
                activity!.SetTag("statement.id", "stmt-envelope");
                activity.SetStatus(ActivityStatusCode.Ok);
                activity.Stop();
                aggregator.ProcessActivity(activity);
            }

            // Assert - Frontend log envelope is properly built
            var log = _mockClient.LastEnqueuedLog!;
            Assert.Equal(777, log.WorkspaceId);
            Assert.NotNull(log.FrontendLogEventId);
            Assert.NotEmpty(log.FrontendLogEventId);
            // Verify it's a GUID format
            Assert.True(Guid.TryParse(log.FrontendLogEventId, out _));
            Assert.NotNull(log.Context);
            Assert.True(log.Context!.TimestampMillis > 0);
            Assert.NotNull(log.Entry);
            Assert.NotNull(log.Entry!.SqlDriverLog);
        }

        #endregion

        #region Multi-Activity Aggregation Integration Tests

        [Fact]
        public void ProcessActivity_FullStatementLifecycle_AggregatesCorrectly()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config, sessionId: "sess-lifecycle");

            // Simulate full statement lifecycle with proper nesting:
            // Root starts first, children created inside it.
            // Children stop before root (stack unwinding), then root stops.
            using (var rootActivity = DriverSource.StartActivity("ExecuteQuery"))
            {
                rootActivity!.SetTag("statement.id", "stmt-lifecycle");
                rootActivity.SetTag("session.id", "sess-lifecycle");
                rootActivity.SetTag("statement.type", "query");
                rootActivity.SetTag("result.format", "cloudfetch");

                // Step 1: Child - PollOperationStatus stops first
                using (var pollActivity = DriverSource.StartActivity("PollOperationStatus"))
                {
                    pollActivity!.SetTag("statement.id", "stmt-lifecycle");
                    pollActivity.SetTag("session.id", "sess-lifecycle");
                    pollActivity.SetTag("poll.count", 10);
                    pollActivity.SetTag("poll.latency_ms", 500L);
                    pollActivity.SetStatus(ActivityStatusCode.Ok);
                    pollActivity.Stop();
                    aggregator.ProcessActivity(pollActivity);
                }

                // Step 2: Child - DownloadFiles stops second
                using (var downloadActivity = DriverSource.StartActivity("DownloadFiles"))
                {
                    downloadActivity!.SetTag("statement.id", "stmt-lifecycle");
                    downloadActivity.SetTag("session.id", "sess-lifecycle");
                    downloadActivity.SetStatus(ActivityStatusCode.Ok);
                    downloadActivity.Stop();
                    aggregator.ProcessActivity(downloadActivity);
                }

                // No emission yet - only children processed
                Assert.Equal(0, _mockClient.EnqueueCallCount);

                // Step 3: Root - ExecuteQuery stops last
                rootActivity.SetStatus(ActivityStatusCode.Ok);
                rootActivity.Stop();
                aggregator.ProcessActivity(rootActivity);
            }

            // Assert - One emission with all aggregated data
            Assert.Equal(1, _mockClient.EnqueueCallCount);
            var proto = _mockClient.LastEnqueuedLog!.Entry!.SqlDriverLog!;
            Assert.Equal("stmt-lifecycle", proto.SqlStatementId);
            Assert.Equal("sess-lifecycle", proto.SessionId);

            // Verify polling data was merged from child activity
            Assert.Equal(10, proto.SqlOperation.OperationDetail.NOperationStatusCalls);
            Assert.Equal(500, proto.SqlOperation.OperationDetail.OperationStatusLatencyMillis);
        }

        [Fact]
        public void ProcessActivity_MetadataOperations_EmitOnRootComplete()
        {
            // Arrange
            var aggregator = new MetricsAggregator(_mockClient, _config, sessionId: "sess-meta");

            // Act - Metadata operations (GetCatalogs, GetSchemas, etc.)
            string[] metadataOps = { "GetCatalogs", "GetSchemas", "GetTables", "GetColumns", "GetTableTypes" };
            int count = 0;
            foreach (var op in metadataOps)
            {
                count++;
                using (var activity = DriverSource.StartActivity(op))
                {
                    activity!.SetTag("statement.id", $"stmt-meta-{count}");
                    activity.SetTag("session.id", "sess-meta");
                    activity.SetStatus(ActivityStatusCode.Ok);
                    activity.Stop();
                    aggregator.ProcessActivity(activity);
                }
            }

            // Assert - Each metadata operation should emit (they're all root activities)
            Assert.Equal(metadataOps.Length, _mockClient.EnqueueCallCount);
        }

        #endregion

        #region Mock Implementations

        /// <summary>
        /// Manual mock of ITelemetryClient that captures Enqueue calls.
        /// </summary>
        private class MockTelemetryClient : ITelemetryClient
        {
            private int _enqueueCallCount;

            public int EnqueueCallCount => _enqueueCallCount;
            public TelemetryFrontendLog? LastEnqueuedLog { get; private set; }
            public ConcurrentBag<TelemetryFrontendLog> AllEnqueuedLogs { get; } = new ConcurrentBag<TelemetryFrontendLog>();

            public void Enqueue(TelemetryFrontendLog log)
            {
                Interlocked.Increment(ref _enqueueCallCount);
                LastEnqueuedLog = log;
                AllEnqueuedLogs.Add(log);
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
        /// Mock ITelemetryClient that throws on Enqueue to test exception swallowing.
        /// </summary>
        private class ThrowingTelemetryClient : ITelemetryClient
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
