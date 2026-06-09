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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using AdbcDrivers.Databricks.Telemetry;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// End-to-end tests for the CloudFetch feature in the Databricks ADBC driver.
    /// Runs under the protocol selected by the connection config / CI matrix
    /// (the suite is exercised against both Thrift and REST as separate jobs).
    /// </summary>
    public class CloudFetchE2ETest : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>, IDisposable
    {
        private readonly ActivityListener? _activityListener;
        private readonly List<(string ActivityName, ActivityEvent Event)> _capturedEvents = new();
        private readonly object _capturedEventsLock = new();
        private bool _disposed;

        // Activity source names for Databricks drivers
        private static readonly string[] s_activitySourceNames = new[]
        {
            "AdbcDrivers.Databricks",
            "AdbcDrivers.Databricks.StatementExecution"
        };

        public CloudFetchE2ETest(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            // Skip the test if the DATABRICKS_TEST_CONFIG_FILE environment variable is not set
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));

            // Set up activity listener to capture and output trace information
            _activityListener = new ActivityListener
            {
                ShouldListenTo = source => s_activitySourceNames.Contains(source.Name),
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStarted = activity =>
                {
                    var msg = $"[TRACE START] {activity.OperationName} | TraceId: {activity.TraceId} | SpanId: {activity.SpanId}";
                    Debug.WriteLine(msg);
                    OutputHelper?.WriteLine(msg);
                    foreach (var tag in activity.Tags)
                    {
                        var tagMsg = $"  Tag: {tag.Key} = {tag.Value}";
                        Debug.WriteLine(tagMsg);
                        OutputHelper?.WriteLine(tagMsg);
                    }
                },
                ActivityStopped = activity =>
                {
                    var duration = activity.Duration.TotalMilliseconds;
                    var msg = $"[TRACE END] {activity.OperationName} | Duration: {duration:F2}ms | Status: {activity.Status}";
                    Debug.WriteLine(msg);
                    OutputHelper?.WriteLine(msg);
                    foreach (var evt in activity.Events)
                    {
                        var evtMsg = $"  Event: {evt.Name} at {evt.Timestamp:O}";
                        Debug.WriteLine(evtMsg);
                        OutputHelper?.WriteLine(evtMsg);
                        foreach (var tag in evt.Tags)
                        {
                            var tagMsg = $"    {tag.Key} = {tag.Value}";
                            Debug.WriteLine(tagMsg);
                            OutputHelper?.WriteLine(tagMsg);
                        }
                        // Snapshot the event into a stable list for in-test assertions.
                        // ActivityEvent's tag list is materialized on the event itself,
                        // so it's safe to retain after the activity has stopped.
                        lock (_capturedEventsLock)
                        {
                            _capturedEvents.Add((activity.OperationName, evt));
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
                    _activityListener?.Dispose();
                }
                _disposed = true;
            }
            base.Dispose(disposing);
        }

        /// <summary>
        /// Test cases for CloudFetch.
        /// Format: (query, expected row count, use cloud fetch, enable direct results)
        ///
        /// The protocol is NOT a test-case dimension: each case runs under the protocol
        /// selected by the connection config / CI matrix (separate rest and thrift jobs),
        /// so it isn't duplicated or hardcoded here.
        /// </summary>
        public static IEnumerable<object[]> TestCases()
        {
            string zeroQuery = "SELECT * FROM range(1000) LIMIT 0";
            string smallQuery = "SELECT * FROM range(1000)";
            string largeQuery = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";

            // LIMIT 0 test cases - edge case for empty result set (PECO-2524)
            yield return new object[] { zeroQuery, 0, true, true };
            yield return new object[] { zeroQuery, 0, false, true };

            // Small query test cases
            yield return new object[] { smallQuery, 1000, true, true };
            yield return new object[] { smallQuery, 1000, false, true };
            yield return new object[] { smallQuery, 1000, true, false };
            yield return new object[] { smallQuery, 1000, false, false };

            // Large query test cases
            yield return new object[] { largeQuery, 1000000, true, true };
            yield return new object[] { largeQuery, 1000000, false, true };
            yield return new object[] { largeQuery, 1000000, true, false };
            yield return new object[] { largeQuery, 1000000, false, false };
        }

        /// <summary>
        /// Integration test for running queries against a real Databricks cluster with different CloudFetch settings.
        /// Runs under the protocol selected by the connection config / CI matrix (rest or thrift).
        /// </summary>
        [Theory]
        [MemberData(nameof(TestCases))]
        public async Task TestCloudFetch(string query, int rowCount, bool useCloudFetch, bool enableDirectResults)
        {
            // Effective protocol comes from config (mirrors DatabricksDatabase's default),
            // not from the test — so the rest-only setup below tracks the configured protocol.
            string protocol = string.IsNullOrEmpty(TestConfiguration.Protocol)
                ? "thrift" : TestConfiguration.Protocol.ToLowerInvariant();

            var parameters = new Dictionary<string, string>
            {
                [DatabricksParameters.UseCloudFetch] = useCloudFetch.ToString(),
                [DatabricksParameters.EnableDirectResults] = enableDirectResults.ToString(),
                [DatabricksParameters.CanDecompressLz4] = "true",
                [DatabricksParameters.MaxBytesPerFile] = "10485760", // 10MB
                [DatabricksParameters.CloudFetchUrlExpirationBufferSeconds] = (15 * 60 - 2).ToString(),
                [TelemetryConfiguration.PropertyKeyEnabled] = "true",
            };

            // For REST API, configure result disposition based on CloudFetch setting
            if (protocol == "rest")
            {
                // Map useCloudFetch to result disposition:
                // - useCloudFetch=true -> EXTERNAL_LINKS (forces CloudFetch)
                // - useCloudFetch=false -> INLINE_OR_EXTERNAL_LINKS (server decides, prefers inline for small results)
                // Note: API expects uppercase values
                parameters[DatabricksParameters.ResultDisposition] = "INLINE_OR_EXTERNAL_LINKS";
                parameters[DatabricksParameters.ResultFormat] = "ARROW_STREAM";
                parameters[DatabricksParameters.ResultCompression] = "LZ4_FRAME";
            }

            var connection = NewConnection(TestConfiguration, parameters);
            var protocolName = protocol == "rest" ? "REST API" : "Thrift";

            await ExecuteAndValidateQuery(connection, query, rowCount, protocolName);
	    connection.Dispose();
        }

        /// <summary>
        /// Executes a query and validates the row count.
        /// Validates exact row count to ensure the driver correctly respects LIMIT N in queries (PECO-2524).
        /// </summary>
        private async Task ExecuteAndValidateQuery(AdbcConnection connection, string query, int expectedRowCount, string protocolName)
        {
            Console.WriteLine($"[TEST] ExecuteAndValidateQuery START - {protocolName}");
            // Execute a query that generates a large result set
            var statement = connection.CreateStatement();
            Console.WriteLine($"[TEST] Statement created");
            statement.SqlQuery = query;

            // Execute the query and get the result
            Console.WriteLine($"[TEST] Executing query...");
            var result = await statement.ExecuteQueryAsync();
            Console.WriteLine($"[TEST] Query executed, RowCount={result.RowCount}");

            if (result.Stream == null)
            {
                throw new InvalidOperationException("Result stream is null");
            }

            // Read all the data and count rows
            long totalRows = 0;
            int batchCount = 0;
            RecordBatch? batch;
            Console.WriteLine($"[TEST] Reading batches...");
            while ((batch = await result.Stream.ReadNextRecordBatchAsync()) != null)
            {
                totalRows += batch.Length;
                batchCount++;
                if (batchCount % 10 == 0)
                {
                    Console.WriteLine($"[TEST] Read {batchCount} batches, {totalRows} rows so far");
                }
            }
            Console.WriteLine($"[TEST] Finished reading {batchCount} batches, {totalRows} total rows");

            // Validate exact row count - driver must respect LIMIT N and trim excess rows (PECO-2524)
            // For Thrift: sum of all batch.RowCount = total expected rows
            // For REST API (SEA): manifest.TotalRowCount = total expected rows
            Assert.Equal(expectedRowCount, totalRows);

            Assert.Null(await result.Stream.ReadNextRecordBatchAsync());
            statement.Dispose();

            // Also log to the test output helper if available
            OutputHelper?.WriteLine($"[{protocolName}] Read exactly {totalRows} rows as expected");
        }

        /// <summary>
        /// Regression test for issue #483: the
        /// <c>cloudfetch.download_slot_acquired</c> event emitted by
        /// <see cref="Reader.CloudFetch.CloudFetchDownloader"/> must carry a
        /// <c>wait_duration_ms</c> tag so operators can tell whether a slot was
        /// acquired immediately or whether the call blocked waiting for another
        /// download to finish.
        ///
        /// Before the fix, the event fired with no timing information at all
        /// (only <c>chunk_index</c> and <c>is_sequential_mode</c>), so slot
        /// contention was invisible — under the CloudFetchStressTests run that
        /// motivated this issue the event fired 252 times with no way to
        /// distinguish "acquired instantly" from "queued behind 3 other
        /// downloads."
        ///
        /// The fix wraps <c>SemaphoreSlim.WaitAsync</c> in a
        /// <see cref="Stopwatch"/> and attaches its elapsed milliseconds (as a
        /// <see cref="long"/>) to the event. Even when contention is zero
        /// (typical for small/local test queries) the tag must still be
        /// present with a non-negative value — the original defect is "tag
        /// missing entirely", so a 0ms value still represents a successful
        /// fix.
        /// </summary>
        [SkippableFact]
        public async Task DownloadSlotAcquired_EmitsWaitDurationMs_Issue483()
        {
            lock (_capturedEventsLock) { _capturedEvents.Clear(); }

            // Force CloudFetch on Thrift so DownloadFilesAsync runs and emits
            // cloudfetch.download_slot_acquired. Mirroring the
            // CloudFetchStressTests configuration that motivated this issue —
            // a large tpcds table with MaxBytesPerFile capped at 10MB forces
            // the server to split results across multiple CloudFetch files,
            // each of which goes through the slot-acquisition path.
            var parameters = new Dictionary<string, string>
            {
                [DatabricksParameters.Protocol] = "thrift",
                [DatabricksParameters.UseCloudFetch] = "true",
                [DatabricksParameters.EnableDirectResults] = "true",
                [DatabricksParameters.CanDecompressLz4] = "true",
                [DatabricksParameters.MaxBytesPerFile] = "10485760", // 10MB — forces multiple files
                [TelemetryConfiguration.PropertyKeyEnabled] = "true",
            };

            const string query = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";

            var connection = NewConnection(TestConfiguration, parameters);
            try
            {
                await ExecuteAndValidateQuery(connection, query, 1000000, "Thrift");
            }
            finally
            {
                connection.Dispose();
            }

            // Locate at least one cloudfetch.download_slot_acquired event
            // across all captured activities. The event lives on whichever
            // activity is current when CloudFetchDownloader.DownloadFilesAsync
            // emits it (the DownloadFilesAsync span itself), but we don't pin
            // the operation name here to keep the assertion narrowly focused
            // on the regression: the event must exist and must carry the
            // wait_duration_ms tag.
            List<(string ActivityName, ActivityEvent Event)> slotAcquiredEvents;
            lock (_capturedEventsLock)
            {
                slotAcquiredEvents = _capturedEvents
                    .Where(e => e.Event.Name == "cloudfetch.download_slot_acquired")
                    .ToList();
            }

            OutputHelper?.WriteLine(
                $"Captured {slotAcquiredEvents.Count} cloudfetch.download_slot_acquired event(s).");
            foreach (var e in slotAcquiredEvents)
            {
                OutputHelper?.WriteLine(
                    $"  [{e.ActivityName}] tags: [" +
                    string.Join(", ", e.Event.Tags.Select(t => $"{t.Key}={t.Value} ({t.Value?.GetType().Name ?? "null"})")) +
                    "]");
            }

            Assert.True(slotAcquiredEvents.Count > 0,
                "Expected at least one 'cloudfetch.download_slot_acquired' event to be " +
                "emitted while reading CloudFetch results. None were captured — either " +
                "CloudFetch did not actually run (check Protocol/UseCloudFetch params) or " +
                "the event has been renamed/removed.");

            // The fix: every cloudfetch.download_slot_acquired event must
            // carry a wait_duration_ms tag with a non-negative integer value.
            // We assert on every captured event (not just one) so the
            // regression cannot reappear for a subset of the call sites.
            foreach (var (activityName, evt) in slotAcquiredEvents)
            {
                var tags = evt.Tags.ToList();
                var waitTag = tags.FirstOrDefault(t => t.Key == "wait_duration_ms");

                Assert.True(waitTag.Key == "wait_duration_ms",
                    $"Expected 'wait_duration_ms' tag on 'cloudfetch.download_slot_acquired' " +
                    $"event under activity '{activityName}'. Without it, slot contention is " +
                    $"invisible in traces (issue #483). Got tags: [" +
                    string.Join(", ", tags.Select(t => $"{t.Key}={t.Value}")) + "]");

                // The fix uses Stopwatch.ElapsedMilliseconds which returns
                // long. Accept any integer width defensively in case the
                // implementation downcasts — but reject negative values
                // (a Stopwatch-derived duration cannot be negative).
                long waitMs;
                switch (waitTag.Value)
                {
                    case long l:
                        waitMs = l;
                        break;
                    case int i:
                        waitMs = i;
                        break;
                    default:
                        Assert.Fail(
                            $"Expected 'wait_duration_ms' tag to be a long/int on " +
                            $"'cloudfetch.download_slot_acquired' under activity " +
                            $"'{activityName}'. Got: {waitTag.Value} " +
                            $"({waitTag.Value?.GetType().Name ?? "null"})");
                        return;
                }

                Assert.True(waitMs >= 0,
                    $"Expected 'wait_duration_ms' >= 0 on 'cloudfetch.download_slot_acquired' " +
                    $"under activity '{activityName}'. Got: {waitMs}");
            }
        }
    }
}
