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

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// E2E regression tests for CloseOperation behavior in DatabricksCompositeReader.
    ///
    /// Validates that the driver executes the CloseOperation code path in Dispose across
    /// all three result delivery modes when the reader is disposed without closing the
    /// connection (simulating connection pooling).
    ///
    /// Uses ActivityListener to capture the composite_reader.close_operation trace event
    /// emitted inside DatabricksCompositeReader.Dispose. This event is only present after
    /// the fix; without it, CloudFetch operations are orphaned server-side for ~1 hour,
    /// producing thriftOperationCloseReason=CommandInactivityTimeout.
    ///
    /// Bug root cause: refactor(csharp): make CloudFetch pipeline protocol-agnostic (#14)
    /// removed CloseOperationAsync() from BaseDatabricksReader and changed
    /// DatabricksCompositeReader.Dispose to call _activeReader.Dispose() instead.
    /// CloudFetchReader.Dispose() is protocol-agnostic and never sends CloseOperation.
    /// </summary>
    public class CloseOperationE2ETest : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>, IDisposable
    {
        private readonly List<(string ActivityName, string EventName)> _capturedEvents = new();
        private readonly object _capturedEventsLock = new();
        private readonly ActivityListener _activityListener;
        private bool _disposed;

        public CloseOperationE2ETest(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));

            _activityListener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "AdbcDrivers.Databricks",
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    lock (_capturedEventsLock)
                    {
                        foreach (var evt in activity.Events)
                        {
                            _capturedEvents.Add((activity.OperationName, evt.Name));
                        }
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
        /// Test cases: (description, query, useCloudFetch, enableDirectResults)
        /// </summary>
        public static IEnumerable<object[]> TestCases() =>
        [
            // Scenario 1: Inline + DirectResults enabled.
            // Server closes the operation inline (DirectResults.CloseOperation).
            // DatabricksCompositeReader.Dispose still calls CloseOperationAsync which is a no-op,
            // but the composite_reader.close_operation trace event must be emitted to confirm
            // the correct code path was reached.
            new object[] { "Inline+DirectResults", "SELECT 1 AS val", false, true },

            // Scenario 2: Inline + DirectResults disabled.
            // Server does NOT close inline; driver must send explicit CloseOperation on reader
            // dispose. composite_reader.close_operation event confirms the code path was reached.
            new object[] { "Inline+NoDirectResults", "SELECT * FROM range(1, 100)", false, false },

            // Scenario 3: CloudFetch.
            // CloudFetchReader is protocol-agnostic and never sends CloseOperation.
            // DatabricksCompositeReader.Dispose must own the cleanup.
            // Without the fix, composite_reader.close_operation is never emitted for this path.
            new object[] { "CloudFetch", "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000", true, true },
        ];

        /// <summary>
        /// Validates that DatabricksCompositeReader.Dispose emits the composite_reader.close_operation
        /// trace event for all result delivery modes when the reader is disposed without closing
        /// the underlying connection (simulating connection pooling).
        ///
        /// The composite_reader.close_operation event is only present in the code path introduced
        /// by the fix. Without the fix:
        /// - Inline (DirectResults or not): event missing because _activeReader != null caused
        ///   the old code to delegate to _activeReader.Dispose() instead.
        /// - CloudFetch: same delegation, but CloseOperation is never sent at all, orphaning
        ///   the server operation for ~1 hour.
        ///
        /// For Thrift wire-level assertions, see proxy-based tests in databricks-driver-test:
        /// CLOUDFETCH-013 through CLOUDFETCH-016.
        /// </summary>
        [Theory]
        [MemberData(nameof(TestCases))]
        public async Task DisposeEmitsCloseOperationEvent(string description, string query, bool useCloudFetch, bool enableDirectResults)
        {
            lock (_capturedEventsLock) { _capturedEvents.Clear(); }

            var parameters = new Dictionary<string, string>
            {
                [DatabricksParameters.Protocol] = "thrift",
                [DatabricksParameters.UseCloudFetch] = useCloudFetch.ToString(),
                [DatabricksParameters.EnableDirectResults] = enableDirectResults.ToString(),
            };

            // Keep connection alive without disposing — simulates a connection pool.
            // In a pool CloseSession is never sent, so CloseOperation is the only mechanism
            // that releases the server-side operation promptly.
            var connection = NewConnection(TestConfiguration, parameters);
            try
            {
                var statement = connection.CreateStatement();
                statement.SqlQuery = query;
                var result = await statement.ExecuteQueryAsync();

                long totalRows = 0;
                using (var reader = result.Stream!)
                {
                    RecordBatch? batch;
                    while ((batch = await reader.ReadNextRecordBatchAsync()) != null)
                    {
                        totalRows += batch.Length;
                    }
                }
                // reader.Dispose() called here — DatabricksCompositeReader.Dispose runs.
                statement.Dispose();

                OutputHelper?.WriteLine($"[{description}] Read {totalRows} rows, reader disposed.");

                // Collect the events emitted by DatabricksCompositeReader.Dispose.
                List<string> disposeEvents;
                lock (_capturedEventsLock)
                {
                    disposeEvents = _capturedEvents
                        .Where(e => e.ActivityName == "DatabricksCompositeReader.Dispose")
                        .Select(e => e.EventName)
                        .ToList();
                }

                OutputHelper?.WriteLine($"[{description}] Dispose events: [{string.Join(", ", disposeEvents)}]");

                // The composite_reader.close_operation event is only present after the fix.
                // Without it, the CloudFetch path silently skips CloseOperation entirely.
                Assert.True(disposeEvents.Contains("composite_reader.close_operation"),
                    $"[{description}] composite_reader.close_operation event not found in " +
                    $"DatabricksCompositeReader.Dispose. Without the fix, server operations are " +
                    $"orphaned until SQL Gateway closes them with CommandInactivityTimeout (~1 hour).");
            }
            finally
            {
                connection.Dispose();
            }
        }
    }
}
