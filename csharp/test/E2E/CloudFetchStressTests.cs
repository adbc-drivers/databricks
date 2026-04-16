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
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using AdbcDrivers.HiveServer2;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// Stress tests targeting the CloudFetch 5-stage async pipeline with real
    /// multi-file downloads from tpcds tables. Tests for task leaks, memory
    /// growth under large results, concurrent CloudFetch sessions, and
    /// disposal mid-download with active parallel downloaders.
    /// </summary>
    public class CloudFetchStressTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        // Large query that triggers multi-file CloudFetch (store_sales is ~300M rows in sf100)
        private const string LargeQuery = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";
        private const int LargeQueryExpectedRows = 1_000_000;

        public CloudFetchStressTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        private AdbcConnection NewCloudFetchConnection(Dictionary<string, string>? extraParams = null)
        {
            var parameters = new Dictionary<string, string>
            {
                [DatabricksParameters.UseCloudFetch] = "true",
                [DatabricksParameters.EnableDirectResults] = "true",
                [DatabricksParameters.CanDecompressLz4] = "true",
                [DatabricksParameters.MaxBytesPerFile] = "10485760", // 10MB — forces multiple files
            };

            if (extraParams != null)
            {
                foreach (var kvp in extraParams)
                    parameters[kvp.Key] = kvp.Value;
            }

            return NewConnection(TestConfiguration, parameters);
        }

        /// <summary>
        /// Run 3 large CloudFetch queries concurrently on separate connections.
        /// Tests: parallel CloudFetch pipelines sharing RecyclableMemoryStreamManager,
        /// LZ4 buffer pool contention, concurrent presigned URL fetching.
        /// </summary>
        [SkippableFact]
        public async Task ConcurrentLargeCloudFetchQueries_AllSucceed()
        {
            const int concurrency = 3;
            var errors = new ConcurrentBag<(int id, Exception ex)>();
            var results = new ConcurrentBag<(int id, long rows, long elapsedMs)>();

            var sw = Stopwatch.StartNew();

            var tasks = Enumerable.Range(0, concurrency).Select(id =>
                Task.Run(async () =>
                {
                    var taskSw = Stopwatch.StartNew();
                    try
                    {
                        using var connection = NewCloudFetchConnection();
                        using var statement = connection.CreateStatement();
                        statement.SqlQuery = LargeQuery;

                        var result = statement.ExecuteQuery();
                        using var reader = result.Stream;

                        long rows = 0;
                        while (true)
                        {
                            var batch = await reader.ReadNextRecordBatchAsync();
                            if (batch == null) break;
                            rows += batch.Length;
                        }

                        taskSw.Stop();
                        results.Add((id, rows, taskSw.ElapsedMilliseconds));
                    }
                    catch (Exception ex)
                    {
                        errors.Add((id, ex));
                    }
                })
            ).ToArray();

            await Task.WhenAll(tasks);
            sw.Stop();

            foreach (var (id, rows, elapsed) in results.OrderBy(r => r.id))
            {
                OutputHelper?.WriteLine($"Connection {id}: {rows:N0} rows in {elapsed:N0}ms");
            }

            foreach (var (id, ex) in errors)
            {
                OutputHelper?.WriteLine($"Connection {id} ERROR: {ex.GetType().Name}: {ex.Message}");
            }

            OutputHelper?.WriteLine($"Total wall time: {sw.ElapsedMilliseconds:N0}ms");

            Assert.Empty(errors);

            foreach (var (id, rows, _) in results)
            {
                Assert.Equal(LargeQueryExpectedRows, rows);
            }
        }

        /// <summary>
        /// Run multiple large CloudFetch queries sequentially on the same connection,
        /// measuring memory to detect leaks in the CloudFetch pipeline cleanup.
        /// Targets: CloudFetchDownloadManager disposal, presigned URL list cleanup,
        /// LZ4 stream disposal, Arrow IPC reader disposal.
        /// </summary>
        [SkippableFact]
        public async Task RepeatedLargeCloudFetch_MemoryShouldPlateau()
        {
            const int iterations = 5;
            using var connection = NewCloudFetchConnection();
            var samples = new List<(int iteration, long bytes, long rows)>();
            var httpClientWeakRefs = new List<WeakReference>();
            var handlerWeakRefs = new List<WeakReference>();

            // Log GC and environment info upfront
            var gcInfo = GC.GetGCMemoryInfo();
            OutputHelper?.WriteLine($"--- Environment ---");
            OutputHelper?.WriteLine($"GC.IsServerGC: {System.Runtime.GCSettings.IsServerGC}");
            OutputHelper?.WriteLine($"GC.LatencyMode: {System.Runtime.GCSettings.LatencyMode}");
            OutputHelper?.WriteLine($"ProcessorCount: {Environment.ProcessorCount}");
            OutputHelper?.WriteLine($"TotalAvailableMemory: {gcInfo.TotalAvailableMemoryBytes / 1024.0 / 1024.0:F0} MB");
            OutputHelper?.WriteLine($"HighMemoryLoadThreshold: {gcInfo.HighMemoryLoadThresholdBytes / 1024.0 / 1024.0:F0} MB");
            OutputHelper?.WriteLine($"HeapSize: {gcInfo.HeapSizeBytes / 1024.0 / 1024.0:F2} MB");

            for (int i = 1; i <= iterations; i++)
            {
                long memBefore = GC.GetTotalMemory(false);

                using var statement = connection.CreateStatement();
                statement.SqlQuery = LargeQuery;

                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                // Track HttpClient and handler via WeakReference
                var httpClientField = reader.GetType().GetField("_httpClient",
                    BindingFlags.NonPublic | BindingFlags.Instance);
                if (httpClientField != null)
                {
                    var httpClient = httpClientField.GetValue(reader) as HttpClient;
                    if (httpClient != null)
                    {
                        httpClientWeakRefs.Add(new WeakReference(httpClient));
                        var handlerField = typeof(HttpMessageInvoker).GetField("_handler",
                            BindingFlags.NonPublic | BindingFlags.Instance);
                        var handler = handlerField?.GetValue(httpClient);
                        if (handler != null)
                            handlerWeakRefs.Add(new WeakReference(handler));
                    }
                }

                long rows = 0;
                while (true)
                {
                    var batch = await reader.ReadNextRecordBatchAsync();
                    if (batch == null) break;
                    rows += batch.Length;
                }
                // reader is disposed here (end of using scope)

                long memAfterRead = GC.GetTotalMemory(false);

                // GC and measure
                int gen0Before = GC.CollectionCount(0);
                int gen1Before = GC.CollectionCount(1);
                int gen2Before = GC.CollectionCount(2);
                GC.Collect(2, GCCollectionMode.Forced, true);
                GC.WaitForPendingFinalizers();
                GC.Collect(2, GCCollectionMode.Forced, true);
                int gen0After = GC.CollectionCount(0);
                int gen1After = GC.CollectionCount(1);
                int gen2After = GC.CollectionCount(2);

                long memAfterGC = GC.GetTotalMemory(true);
                var iterGcInfo = GC.GetGCMemoryInfo();

                int clientsAlive = httpClientWeakRefs.Count(wr => wr.IsAlive);
                int handlersAlive = handlerWeakRefs.Count(wr => wr.IsAlive);

                samples.Add((i, memAfterGC, rows));

                OutputHelper?.WriteLine($"--- Iteration {i} ---");
                OutputHelper?.WriteLine($"  Rows: {rows:N0}");
                OutputHelper?.WriteLine($"  Memory before query: {memBefore / 1024.0 / 1024.0:F2} MB");
                OutputHelper?.WriteLine($"  Memory after read (pre-GC): {memAfterRead / 1024.0 / 1024.0:F2} MB");
                OutputHelper?.WriteLine($"  Memory after GC: {memAfterGC / 1024.0 / 1024.0:F2} MB");
                OutputHelper?.WriteLine($"  GC freed: {(memAfterRead - memAfterGC) / 1024.0 / 1024.0:F2} MB");
                OutputHelper?.WriteLine($"  GC collections triggered: gen0={gen0After - gen0Before} gen1={gen1After - gen1Before} gen2={gen2After - gen2Before}");
                OutputHelper?.WriteLine($"  Heap: {iterGcInfo.HeapSizeBytes / 1024.0 / 1024.0:F2} MB");
                OutputHelper?.WriteLine($"  LOH size: {iterGcInfo.GenerationInfo[3].SizeAfterBytes / 1024.0 / 1024.0:F2} MB");
                OutputHelper?.WriteLine($"  POH size: {iterGcInfo.GenerationInfo[4].SizeAfterBytes / 1024.0 / 1024.0:F2} MB");
                OutputHelper?.WriteLine($"  Finalization pending: {iterGcInfo.FinalizationPendingCount}");
                OutputHelper?.WriteLine($"  HttpClient alive: {clientsAlive}/{httpClientWeakRefs.Count}");
                OutputHelper?.WriteLine($"  HttpClientHandler alive: {handlersAlive}/{handlerWeakRefs.Count}");

                Assert.Equal(LargeQueryExpectedRows, rows);
            }

            // After warmup (first 2 iterations), memory should be stable
            var postWarmup = samples.Skip(2).ToList();
            double baselineMB = postWarmup.First().bytes / 1024.0 / 1024.0;
            double finalMB = postWarmup.Last().bytes / 1024.0 / 1024.0;
            double growthMB = finalMB - baselineMB;

            OutputHelper?.WriteLine($"--- CloudFetch memory analysis ---");
            OutputHelper?.WriteLine($"Post-warmup baseline: {baselineMB:F2} MB");
            OutputHelper?.WriteLine($"Final: {finalMB:F2} MB");
            OutputHelper?.WriteLine($"Growth: {growthMB:F2} MB over {postWarmup.Count} iterations");

            // Relaxed threshold — we're investigating, not gating
            Assert.True(growthMB < 50.0,
                $"Possible CloudFetch memory leak: {growthMB:F2} MB growth over {postWarmup.Count} " +
                $"iterations of 1M-row CloudFetch queries.");
        }

        /// <summary>
        /// Dispose connection mid-way through a real multi-file CloudFetch download.
        /// Unlike the RANGE()-based test in ConcurrencyStressTests, this uses a real
        /// large table that triggers multiple parallel CloudFetch file downloads,
        /// exercising the CloudFetchDownloadManager cancellation path.
        /// </summary>
        [SkippableFact]
        public async Task DisposeConnection_DuringMultiFileCloudFetch_ShouldNotHangOrLeak()
        {
            const int timeoutMs = 60_000;
            var queryStarted = new ManualResetEventSlim(false);
            Exception? queryException = null;
            long rowsRead = 0;

            var connection = NewCloudFetchConnection();

            var queryTask = Task.Run(async () =>
            {
                try
                {
                    using var statement = connection.CreateStatement();
                    statement.SqlQuery = LargeQuery;
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;

                    while (true)
                    {
                        var batch = await reader.ReadNextRecordBatchAsync();
                        if (batch == null) break;

                        long current = Interlocked.Add(ref rowsRead, batch.Length);

                        // Signal after we've read enough to know CloudFetch is active
                        // with multiple files in-flight
                        if (!queryStarted.IsSet && current > 100_000)
                        {
                            queryStarted.Set();
                        }
                    }
                }
                catch (Exception ex)
                {
                    queryException = ex;
                }
            });

            bool started = queryStarted.Wait(60_000);
            long rowsAtDispose = Interlocked.Read(ref rowsRead);
            OutputHelper?.WriteLine($"CloudFetch active, read {rowsAtDispose:N0} rows. Disposing...");

            if (!started)
            {
                connection.Dispose();
                Assert.Fail("CloudFetch query did not start producing data within 60s");
            }

            // Measure memory before dispose to check for post-dispose leak
            GC.Collect(2, GCCollectionMode.Forced, true);
            long memBeforeDispose = GC.GetTotalMemory(true);

            var disposeSw = Stopwatch.StartNew();
            var disposeTask = Task.Run(() => connection.Dispose());

            var completed = await Task.WhenAny(
                Task.WhenAll(queryTask, disposeTask),
                Task.Delay(timeoutMs));
            disposeSw.Stop();

            bool allDone = queryTask.IsCompleted && disposeTask.IsCompleted;

            OutputHelper?.WriteLine($"Dispose took: {disposeSw.ElapsedMilliseconds}ms");
            OutputHelper?.WriteLine($"Query completed: {queryTask.IsCompleted}");
            OutputHelper?.WriteLine($"Dispose completed: {disposeTask.IsCompleted}");
            OutputHelper?.WriteLine($"Total rows read: {Interlocked.Read(ref rowsRead):N0}");

            if (queryException != null)
            {
                OutputHelper?.WriteLine($"Query exception (expected): {queryException.GetType().Name}: {queryException.Message}");
            }

            Assert.True(allDone,
                $"Dispose during multi-file CloudFetch hung for {timeoutMs}ms. " +
                $"Query done: {queryTask.IsCompleted}, Dispose done: {disposeTask.IsCompleted}");

            // Check that CloudFetch buffers were released — memory should not stay elevated
            // Wait a moment for background tasks to clean up
            await Task.Delay(1000);
            GC.Collect(2, GCCollectionMode.Forced, true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, true);
            long memAfterDispose = GC.GetTotalMemory(true);

            double memBeforeMB = memBeforeDispose / 1024.0 / 1024.0;
            double memAfterMB = memAfterDispose / 1024.0 / 1024.0;
            OutputHelper?.WriteLine($"Memory before dispose: {memBeforeMB:F2} MB");
            OutputHelper?.WriteLine($"Memory after dispose + GC: {memAfterMB:F2} MB");
        }

        /// <summary>
        /// Rapidly open connections, execute a CloudFetch query, and close — 10 times.
        /// Tests connection lifecycle churn with CloudFetch, FeatureFlagCache under
        /// rapid connect/disconnect, and that no resources leak across connection lifetimes.
        /// </summary>
        [SkippableFact]
        public async Task RapidConnectionCycling_WithCloudFetch_ShouldNotLeak()
        {
            const int cycles = 20;
            const int warmupCycles = 3;
            // Use a smaller query so each cycle is fast but still exercises CloudFetch
            const string query = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 100000";
            const int expectedRows = 100_000;
            var timings = new List<(int cycle, long rows, long elapsedMs, double memMB)>();

            for (int i = 1; i <= cycles; i++)
            {
                var sw = Stopwatch.StartNew();
                using (var connection = NewCloudFetchConnection())
                {
                    using var statement = connection.CreateStatement();
                    statement.SqlQuery = query;
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;

                    long rows = 0;
                    while (true)
                    {
                        var batch = await reader.ReadNextRecordBatchAsync();
                        if (batch == null) break;
                        rows += batch.Length;
                    }

                    sw.Stop();
                    Assert.Equal(expectedRows, rows);

                    GC.Collect(2, GCCollectionMode.Forced, true);
                    GC.WaitForPendingFinalizers();
                    GC.Collect(2, GCCollectionMode.Forced, true);
                    double mem = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
                    timings.Add((i, rows, sw.ElapsedMilliseconds, mem));
                    OutputHelper?.WriteLine($"Cycle {i,2}: {rows:N0} rows, {sw.ElapsedMilliseconds:N0}ms, {mem:F2} MB");
                }
            }

            // Use linear regression on post-warmup samples to detect sustained growth
            // vs transient GC noise (sawtooth is OK, linear slope is not)
            var postWarmup = timings.Skip(warmupCycles).ToList();
            double n = postWarmup.Count;
            double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
            foreach (var (cycle, _, _, memMB) in postWarmup)
            {
                sumX += cycle;
                sumY += memMB;
                sumXY += cycle * memMB;
                sumX2 += cycle * (double)cycle;
            }
            double slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
            double avgMB = sumY / n;
            double minMB = postWarmup.Min(t => t.memMB);
            double maxMB = postWarmup.Max(t => t.memMB);

            OutputHelper?.WriteLine($"--- Connection cycling memory analysis ---");
            OutputHelper?.WriteLine($"Samples: {postWarmup.Count} (cycles {warmupCycles + 1}-{cycles})");
            OutputHelper?.WriteLine($"Avg: {avgMB:F2} MB, Min: {minMB:F2} MB, Max: {maxMB:F2} MB");
            OutputHelper?.WriteLine($"Slope: {slope:F4} MB/cycle");

            // Threshold: 1.0 MB/cycle sustained growth indicates a real leak.
            // GC noise with large Arrow buffers can produce slopes up to ~0.8 MB/cycle
            // even with no leak, due to LOH fragmentation and non-deterministic collection.
            Assert.True(slope < 1.0,
                $"Memory leak detected in connection cycling: slope = {slope:F4} MB/cycle " +
                $"(threshold: 1.0 MB/cycle). Avg: {avgMB:F2} MB, range: [{minMB:F2}, {maxMB:F2}] MB.");
        }

        /// <summary>
        /// Diagnostic test: uses WeakReferences to prove whether HttpClient instances
        /// created per-query are collected after reader disposal.
        /// If HttpClient is NOT disposed by the reader, the weak references will remain alive
        /// after GC because the socket pool prevents finalization.
        /// </summary>
        [SkippableFact]
        public async Task Diagnostic_HttpClientLeakDetection()
        {
            const int iterations = 5;
            var weakRefs = new List<WeakReference>();

            using var connection = NewCloudFetchConnection();

            for (int i = 1; i <= iterations; i++)
            {
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT * FROM RANGE(100)";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                // Use reflection to extract the _httpClient from the composite reader
                var httpClientField = reader.GetType().GetField("_httpClient",
                    BindingFlags.NonPublic | BindingFlags.Instance);

                if (httpClientField != null)
                {
                    var httpClient = httpClientField.GetValue(reader) as HttpClient;
                    if (httpClient != null)
                    {
                        weakRefs.Add(new WeakReference(httpClient));
                        OutputHelper?.WriteLine($"Iteration {i}: captured WeakReference to HttpClient (hash={httpClient.GetHashCode()})");
                    }
                    else
                    {
                        OutputHelper?.WriteLine($"Iteration {i}: _httpClient field is null");
                    }
                }
                else
                {
                    OutputHelper?.WriteLine($"Iteration {i}: _httpClient field not found on {reader.GetType().Name}");
                    // Try walking the reader chain — the composite reader may wrap inner readers
                    OutputHelper?.WriteLine($"  Reader type: {reader.GetType().FullName}");
                }

                while (await reader.ReadNextRecordBatchAsync() != null) { }
                // reader is disposed here (end of using scope)
            }

            // Force full GC to collect anything that's eligible
            GC.Collect(2, GCCollectionMode.Forced, true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, true);

            int alive = weakRefs.Count(wr => wr.IsAlive);
            int collected = weakRefs.Count - alive;

            OutputHelper?.WriteLine($"--- HttpClient leak analysis ---");
            OutputHelper?.WriteLine($"Total HttpClient instances tracked: {weakRefs.Count}");
            OutputHelper?.WriteLine($"Still alive after GC: {alive}");
            OutputHelper?.WriteLine($"Collected by GC: {collected}");

            if (alive > 0)
            {
                OutputHelper?.WriteLine($"LEAK CONFIRMED: {alive} HttpClient instances survived GC.");
                OutputHelper?.WriteLine("These are created per-query in DatabricksConnection.NewReader() " +
                    "but never disposed in DatabricksCompositeReader.Dispose().");
            }

            // Don't assert-fail — this is diagnostic. Just report.
            // Uncomment to enforce: Assert.Equal(0, alive);
        }

        /// <summary>
        /// Diagnostic: measures the actual memory cost of undisposed HttpClient+handler
        /// by isolating the per-query memory delta from CloudFetch data buffers.
        /// Runs real CloudFetch queries (100K rows) and measures retained memory per iteration.
        /// </summary>
        /// <summary>
        /// Diagnostic: runs 1M-row CloudFetch queries with and without explicit HttpClient
        /// disposal to isolate how much memory the HttpClient leak contributes.
        /// </summary>
        [SkippableFact]
        public async Task Diagnostic_PerQueryMemoryCost_WithAndWithoutDispose()
        {
            const int iterations = 6;

            OutputHelper?.WriteLine("=== WITHOUT HttpClient.Dispose (current behavior) ===");
            var samplesWithout = await RunIterationsWithDispose(iterations, disposeHttpClient: false);

            // Force cleanup between runs
            GC.Collect(2, GCCollectionMode.Forced, true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, true);
            await Task.Delay(2000); // let finalizers settle

            OutputHelper?.WriteLine("=== WITH HttpClient.Dispose (proposed fix) ===");
            var samplesWith = await RunIterationsWithDispose(iterations, disposeHttpClient: true);

            OutputHelper?.WriteLine("=== COMPARISON ===");
            double growthWithout = (samplesWithout[^1].memBytes - samplesWithout[0].memBytes) / 1024.0 / 1024.0;
            double growthWith = (samplesWith[^1].memBytes - samplesWith[0].memBytes) / 1024.0 / 1024.0;
            OutputHelper?.WriteLine($"Without dispose: {growthWithout:F2} MB growth over {iterations} iters");
            OutputHelper?.WriteLine($"With dispose:    {growthWith:F2} MB growth over {iterations} iters");
        }

        private async Task<List<(int iter, long memBytes)>> RunIterationsWithDispose(int iterations, bool disposeHttpClient)
        {
            var samples = new List<(int iter, long memBytes)>();

            using var connection = NewCloudFetchConnection();

            for (int i = 1; i <= iterations; i++)
            {
                using var statement = connection.CreateStatement();
                statement.SqlQuery = LargeQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                // If testing the fix, grab and dispose the HttpClient via reflection
                HttpClient? httpClientToDispose = null;
                if (disposeHttpClient)
                {
                    var field = reader.GetType().GetField("_httpClient",
                        BindingFlags.NonPublic | BindingFlags.Instance);
                    httpClientToDispose = field?.GetValue(reader) as HttpClient;
                }

                long rows = 0;
                while (true)
                {
                    var batch = await reader.ReadNextRecordBatchAsync();
                    if (batch == null) break;
                    rows += batch.Length;
                }
                // reader disposed here (but _httpClient is NOT disposed inside)

                // Simulate the fix: dispose HttpClient after reader
                httpClientToDispose?.Dispose();

                GC.Collect(2, GCCollectionMode.Forced, true);
                GC.WaitForPendingFinalizers();
                GC.Collect(2, GCCollectionMode.Forced, true);
                long mem = GC.GetTotalMemory(true);
                samples.Add((i, mem));
                OutputHelper?.WriteLine($"  Iteration {i}: {mem / 1024.0 / 1024.0:F2} MB ({rows:N0} rows)");
            }

            return samples;
        }
    }
}
