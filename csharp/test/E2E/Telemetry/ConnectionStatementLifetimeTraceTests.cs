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
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E regression test for issue #477: every top-level driver call creates a
    /// fresh root <see cref="Activity"/> with a new <c>TraceId</c>, leaving fleet
    /// trace-tree visualization broken. The MemoryStress run referenced in the
    /// issue shows 12,316 of 12,958 TraceIds (95%) as single-span throwaway
    /// <c>ReadNextRecordBatchAsync</c> roots.
    ///
    /// Fix: introduce a long-lived <c>DatabricksConnection.Lifetime</c> activity
    /// that wraps the entire connection, and a long-lived
    /// <c>DatabricksStatement.Lifetime</c> activity that wraps each statement.
    /// Existing per-call activities (HiveServer2Connection.OpenAsync,
    /// HiveServer2Statement.ExecuteStatementAsync, ReadNextRecordBatchAsync,
    /// DatabricksCompositeReader.Dispose, HiveServer2Connection.DisposeClient)
    /// chain underneath via <c>Activity.Current</c>, so every call within one
    /// connection-statement session shares the same TraceId and can be
    /// visualized as a tree.
    ///
    /// This test captures every activity emitted on
    /// <c>AdbcDrivers.Databricks</c> (and <c>AdbcDrivers.HiveServer2</c>) during
    /// a tiny SELECT-1 workflow and asserts the parent relationships. Before
    /// the fix, ConnectionLifetime / StatementLifetime spans do not exist, so
    /// the test fails with "no DatabricksConnection.Lifetime span found".
    /// </summary>
    public class ConnectionStatementLifetimeTraceTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>, IDisposable
    {
        private readonly List<CapturedActivity> _capturedActivities = new();
        private readonly object _capturedLock = new();
        private readonly ActivityListener _activityListener;
        private bool _disposed;

        public ConnectionStatementLifetimeTraceTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));

            _activityListener = new ActivityListener
            {
                // Listen to every driver-level ActivitySource. Each connection
                // creates its own per-instance source named after the assembly
                // (AdbcDrivers.Databricks / AdbcDrivers.HiveServer2 etc.), so
                // we cannot match by exact name — match by prefix instead.
                ShouldListenTo = source =>
                    source.Name.StartsWith("AdbcDrivers.", StringComparison.Ordinal),
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    lock (_capturedLock)
                    {
                        _capturedActivities.Add(new CapturedActivity(
                            SourceName: activity.Source.Name,
                            OperationName: activity.OperationName,
                            TraceId: activity.TraceId,
                            SpanId: activity.SpanId,
                            ParentSpanId: activity.ParentSpanId));
                    }
                }
            };
            ActivitySource.AddActivityListener(_activityListener);
        }

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _activityListener.Dispose();
                }
                _disposed = true;
            }
            base.Dispose(disposing);
        }

        /// <summary>
        /// Asserts that opening a connection, creating a statement, executing a
        /// trivial query, draining the reader, and disposing the statement and
        /// connection produces a single coherent trace tree rooted at
        /// <c>DatabricksConnection.Lifetime</c>, with
        /// <c>DatabricksStatement.Lifetime</c> as a child and every per-call
        /// activity chained underneath.
        ///
        /// Before the fix (issue #477), no Lifetime spans exist at all, so the
        /// first "ConnectionLifetime span found" assertion fails. After the
        /// fix, every per-call activity that was previously a fresh root will
        /// share the ConnectionLifetime TraceId.
        /// </summary>
        [SkippableFact]
        public async Task ConnectionLifetime_ParentsStatementAndCalls_Issue477()
        {
            lock (_capturedLock) { _capturedActivities.Clear(); }

            var parameters = new Dictionary<string, string>
            {
                [DatabricksParameters.Protocol] = "thrift",
            };

            // Drive a minimal but complete connection -> statement -> read -> dispose
            // workflow so we capture every per-call activity at least once.
            using (var connection = NewConnection(TestConfiguration, parameters))
            {
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS val";
                var result = await statement.ExecuteQueryAsync();

                long rows = 0;
                using (var reader = result.Stream!)
                {
                    RecordBatch? batch;
                    while ((batch = await reader.ReadNextRecordBatchAsync()) != null)
                    {
                        rows += batch.Length;
                    }
                }
                OutputHelper?.WriteLine($"Drained {rows} rows.");
            }

            // Snapshot what the listener captured. This is what fleet dashboards
            // would see today.
            List<CapturedActivity> activities;
            lock (_capturedLock)
            {
                activities = _capturedActivities.ToList();
            }

            OutputHelper?.WriteLine($"Captured {activities.Count} activities:");
            foreach (var a in activities)
            {
                OutputHelper?.WriteLine($"  source={a.SourceName} op={a.OperationName} trace={a.TraceId} span={a.SpanId} parent={a.ParentSpanId}");
            }

            // ---- Assertion 1: ConnectionLifetime span exists and is a root. ----
            var connectionLifetime = activities
                .FirstOrDefault(a => a.OperationName == "DatabricksConnection.Lifetime");
            Assert.True(
                connectionLifetime != null,
                "Issue #477: expected a long-lived span named 'DatabricksConnection.Lifetime' " +
                "to wrap every per-call activity emitted for this connection. " +
                $"Found {activities.Count} activities, none with that name. " +
                "Without this wrapper, every top-level driver call (OpenAsync, " +
                "ExecuteStatementAsync, ReadNextRecordBatchAsync, DisposeClient, ...) " +
                "is a fresh TraceId — MemoryStress shows 95% of TraceIds are " +
                "single-span throwaway ReadNextRecordBatchAsync roots.");
            Assert.Equal(default(ActivitySpanId), connectionLifetime!.ParentSpanId);

            // ---- Assertion 2: StatementLifetime span exists and is a child of ConnectionLifetime. ----
            var statementLifetime = activities
                .FirstOrDefault(a => a.OperationName == "DatabricksStatement.Lifetime");
            Assert.True(
                statementLifetime != null,
                "Issue #477: expected a long-lived span named 'DatabricksStatement.Lifetime' " +
                "to wrap every per-call activity emitted for this statement (Execute, " +
                "ReadNext, Dispose). Found no such span.");
            Assert.Equal(connectionLifetime.SpanId, statementLifetime!.ParentSpanId);
            Assert.Equal(connectionLifetime.TraceId, statementLifetime.TraceId);

            // ---- Assertion 3: at least one per-call activity is a child of ConnectionLifetime. ----
            // ApplyServerSidePropertiesAsync runs synchronously inside Database.Connect after
            // the constructor returns, so it sits directly under the lifetime. If the lifetime
            // isn't on the Activity.Current stack, this never chains.
            var connectionChildren = activities
                .Where(a => a.ParentSpanId == connectionLifetime.SpanId &&
                            a.OperationName != "DatabricksStatement.Lifetime")
                .ToList();
            Assert.True(
                connectionChildren.Count > 0,
                "Issue #477: expected at least one per-call activity (e.g. " +
                "ApplyServerSidePropertiesAsync, CreateStatement, DisposeClient) to be a " +
                "child of DatabricksConnection.Lifetime. None were. This means the " +
                "Lifetime activity is not on the Activity.Current stack when subsequent " +
                "calls fire, defeating the purpose of the wrapper. " +
                $"Captured operations: [{string.Join(", ", activities.Select(a => a.OperationName).Distinct())}].");

            // ---- Assertion 4: at least one per-call activity is a child of StatementLifetime. ----
            // ExecuteStatementAsync or ReadNextRecordBatchAsync are the typical signals — but
            // ReadNext lives on the reader, which runs in a different async context than the
            // statement ctor. ExecuteStatementAsync is the strongest signal that the
            // statement's lifetime activity is properly threading through.
            var statementChildren = activities
                .Where(a => a.ParentSpanId == statementLifetime.SpanId)
                .ToList();
            Assert.True(
                statementChildren.Count > 0,
                "Issue #477: expected at least one per-call activity (Execute, ReadNext, " +
                "GetSchema, ...) to be a child of DatabricksStatement.Lifetime. None were. " +
                $"Captured operations under StatementLifetime: [{string.Join(", ", statementChildren.Select(a => a.OperationName))}]; " +
                $"All captured operations: [{string.Join(", ", activities.Select(a => a.OperationName).Distinct())}].");

            // ---- Assertion 5: every captured activity shares the ConnectionLifetime TraceId. ----
            // This is the headline assertion for the fleet visualization concern in #477.
            // Today, every per-call activity has its own TraceId; after the fix they all
            // share one root TraceId per connection.
            var distinctTraceIds = activities.Select(a => a.TraceId).Distinct().ToList();
            Assert.True(
                distinctTraceIds.Count == 1,
                $"Issue #477: expected a single TraceId across the entire connection " +
                $"lifetime (so trace-tree visualization works), got {distinctTraceIds.Count} " +
                $"distinct TraceIds: [{string.Join(", ", distinctTraceIds)}]. " +
                "MemoryStress baseline shows 12,958 TraceIds across one test run — the " +
                "Lifetime activities collapse that to one TraceId per connection.");
        }

        /// <summary>
        /// Captured snapshot of a single Activity. We snapshot at ActivityStopped time
        /// because the framework recycles Activity state shortly after Stop.
        /// </summary>
        private sealed record CapturedActivity(
            string SourceName,
            string OperationName,
            ActivityTraceId TraceId,
            ActivitySpanId SpanId,
            ActivitySpanId ParentSpanId);
    }
}
