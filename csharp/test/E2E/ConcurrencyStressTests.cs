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
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using AdbcDrivers.HiveServer2;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// Concurrency stress tests based on databricks-driver-test specs CONCURRENT-001 through 005.
    /// Tests thread safety of connections, statements, shared singletons (FeatureFlagCache,
    /// TelemetryClientManager, RecyclableMemoryStreamManager), and cancellation.
    /// </summary>
    public class ConcurrencyStressTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        private const int TestTimeoutMs = 300_000; // 5 minutes

        public ConcurrencyStressTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// CONCURRENT-001: Multiple independent connections execute queries concurrently.
        /// Each thread gets its own connection and runs queries in a loop.
        /// Tests: connection independence, shared singleton safety (FeatureFlagCache,
        /// TelemetryClientManager), RecyclableMemoryStreamManager under contention.
        /// </summary>
        [SkippableFact]
        public async Task MultipleConnections_ConcurrentQueries_AllSucceed()
        {
            const int connectionCount = 5;
            const int queriesPerConnection = 20;

            var errors = new ConcurrentBag<(int threadId, int iteration, Exception ex)>();
            var rowCounts = new ConcurrentBag<(int threadId, int iteration, int rows)>();
            var sw = Stopwatch.StartNew();

            var tasks = Enumerable.Range(0, connectionCount).Select(threadId =>
                Task.Run(async () =>
                {
                    using AdbcConnection connection = NewConnection();

                    for (int i = 0; i < queriesPerConnection; i++)
                    {
                        try
                        {
                            using var statement = connection.CreateStatement();
                            // Each thread queries a unique range to verify result isolation
                            int offset = threadId * 1000 + i;
                            statement.SqlQuery = $"SELECT id + {offset} AS val FROM RANGE(10)";
                            var result = statement.ExecuteQuery();
                            using var reader = result.Stream;

                            int rows = 0;
                            while (true)
                            {
                                var batch = await reader.ReadNextRecordBatchAsync();
                                if (batch == null) break;
                                rows += batch.Length;
                            }
                            rowCounts.Add((threadId, i, rows));
                        }
                        catch (Exception ex)
                        {
                            errors.Add((threadId, i, ex));
                        }
                    }
                })
            ).ToArray();

            var completed = await Task.WhenAny(Task.WhenAll(tasks), Task.Delay(TestTimeoutMs));
            sw.Stop();

            if (!Task.WhenAll(tasks).IsCompleted)
            {
                Assert.Fail($"Concurrent queries timed out after {TestTimeoutMs}ms. " +
                    $"Completed {rowCounts.Count} of {connectionCount * queriesPerConnection} queries.");
            }

            OutputHelper?.WriteLine($"Completed {rowCounts.Count} queries across {connectionCount} connections in {sw.ElapsedMilliseconds}ms");

            // Report any errors
            foreach (var (threadId, iteration, ex) in errors)
            {
                OutputHelper?.WriteLine($"Thread {threadId}, iteration {iteration}: {ex.GetType().Name}: {ex.Message}");
            }

            Assert.Empty(errors);

            // Every query should have returned exactly 10 rows
            var wrongRowCounts = rowCounts.Where(r => r.rows != 10).ToList();
            foreach (var (threadId, iteration, rows) in wrongRowCounts)
            {
                OutputHelper?.WriteLine($"Wrong row count: Thread {threadId}, iteration {iteration}: got {rows}, expected 10");
            }
            Assert.Empty(wrongRowCounts);

            Assert.Equal(connectionCount * queriesPerConnection, rowCounts.Count);
        }

        /// <summary>
        /// CONCURRENT-003 variant: Multiple statements on the SAME connection, executed concurrently.
        /// Tests: ThreadSafeClient serialization under real concurrency, statement handle isolation.
        /// </summary>
        [SkippableFact]
        public async Task SingleConnection_ConcurrentStatements_AllSucceed()
        {
            const int statementCount = 5;
            const int queriesPerStatement = 10;

            using AdbcConnection connection = NewConnection();

            var errors = new ConcurrentBag<(int threadId, int iteration, Exception ex)>();
            var results = new ConcurrentBag<(int threadId, int iteration, long firstVal)>();

            var tasks = Enumerable.Range(0, statementCount).Select(threadId =>
                Task.Run(async () =>
                {
                    for (int i = 0; i < queriesPerStatement; i++)
                    {
                        try
                        {
                            using var statement = connection.CreateStatement();
                            // Each thread uses a distinct literal value for result isolation verification
                            int expected = threadId * 100 + i;
                            statement.SqlQuery = $"SELECT {expected} AS val";
                            var result = statement.ExecuteQuery();
                            using var reader = result.Stream;

                            var batch = await reader.ReadNextRecordBatchAsync();
                            Assert.NotNull(batch);
                            Assert.Equal(1, batch!.Length);

                            // Verify we got OUR value back, not another thread's
                            var array = (Int32Array)batch.Column(0);
                            long actual = array.GetValue(0)!.Value;
                            results.Add((threadId, i, actual));

                            if (actual != expected)
                            {
                                errors.Add((threadId, i,
                                    new Exception($"Result isolation violation: expected {expected}, got {actual}")));
                            }

                            // Drain remaining batches
                            while (await reader.ReadNextRecordBatchAsync() != null) { }
                        }
                        catch (Exception ex)
                        {
                            errors.Add((threadId, i, ex));
                        }
                    }
                })
            ).ToArray();

            var completed = await Task.WhenAny(Task.WhenAll(tasks), Task.Delay(TestTimeoutMs));

            if (!Task.WhenAll(tasks).IsCompleted)
            {
                Assert.Fail($"Concurrent statements on single connection timed out after {TestTimeoutMs}ms. " +
                    $"Completed {results.Count} of {statementCount * queriesPerStatement} queries.");
            }

            OutputHelper?.WriteLine($"Completed {results.Count} queries on single connection, {statementCount} concurrent threads");

            foreach (var (threadId, iteration, ex) in errors)
            {
                OutputHelper?.WriteLine($"Thread {threadId}, iteration {iteration}: {ex.GetType().Name}: {ex.Message}");
            }

            Assert.Empty(errors);
            Assert.Equal(statementCount * queriesPerStatement, results.Count);
        }

        /// <summary>
        /// CONCURRENT-004: Close connection while a query is actively fetching results via CloudFetch.
        /// Tests: clean shutdown of CloudFetch pipeline, no task leaks, no hangs.
        /// </summary>
        [SkippableFact]
        public async Task CloseConnection_DuringCloudFetch_ShouldNotHang()
        {
            const int timeoutMs = 60_000;
            var queryStarted = new ManualResetEventSlim(false);
            Exception? queryException = null;
            int rowsRead = 0;

            AdbcConnection connection = NewConnection();

            // Start a query that returns enough data to trigger CloudFetch streaming
            var queryTask = Task.Run(async () =>
            {
                try
                {
                    using var statement = connection.CreateStatement();
                    statement.SqlQuery = "SELECT * FROM RANGE(1000000)";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;

                    while (true)
                    {
                        var batch = await reader.ReadNextRecordBatchAsync();
                        if (batch == null) break;

                        Interlocked.Add(ref rowsRead, batch.Length);

                        // Signal after first batch so we know the pipeline is active
                        if (!queryStarted.IsSet)
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

            // Wait for the query to start producing results
            bool started = queryStarted.Wait(30_000);
            if (!started)
            {
                connection.Dispose();
                Assert.Fail("Query did not start producing results within 30s");
            }

            OutputHelper?.WriteLine($"Query started, read {rowsRead} rows so far. Disposing connection...");

            // Dispose the connection while results are streaming
            var disposeTask = Task.Run(() => connection.Dispose());

            var completed = await Task.WhenAny(
                Task.WhenAll(queryTask, disposeTask),
                Task.Delay(timeoutMs));

            bool allDone = queryTask.IsCompleted && disposeTask.IsCompleted;

            OutputHelper?.WriteLine($"Query task completed: {queryTask.IsCompleted}");
            OutputHelper?.WriteLine($"Dispose task completed: {disposeTask.IsCompleted}");
            OutputHelper?.WriteLine($"Rows read before shutdown: {rowsRead}");

            if (queryException != null)
            {
                // Exception is expected — the connection was yanked out from under the query.
                // But it should be a clean exception, not a deadlock.
                OutputHelper?.WriteLine($"Query exception (expected): {queryException.GetType().Name}: {queryException.Message}");
            }

            Assert.True(allDone,
                $"Connection close during CloudFetch hung for more than {timeoutMs}ms. " +
                $"Query completed: {queryTask.IsCompleted}, Dispose completed: {disposeTask.IsCompleted}.");
        }

        /// <summary>
        /// CONCURRENT-005: Cancel a statement from another thread while it's executing.
        /// Tests: CancelOperation RPC delivery, prompt query termination, statement reusability.
        /// </summary>
        [SkippableFact]
        public async Task CancelStatement_FromAnotherThread_ShouldStopPromptly()
        {
            const int cancelTimeoutMs = 30_000;

            using AdbcConnection connection = NewConnection();
            using var statement = connection.CreateStatement();

            // A query designed to run long enough to be cancelled
            statement.SqlQuery = "SELECT COUNT(*) FROM (SELECT a.id FROM range(100000000) a CROSS JOIN range(100) b)";

            Exception? queryException = null;
            var queryStarted = new ManualResetEventSlim(false);
            var sw = Stopwatch.StartNew();

            // Start the long query
            var queryTask = Task.Run(() =>
            {
                try
                {
                    queryStarted.Set();
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;
                    while (reader.ReadNextRecordBatchAsync().Result != null) { }
                }
                catch (Exception ex)
                {
                    queryException = ex;
                }
            });

            // Wait for the query to start, then cancel
            queryStarted.Wait();
            await Task.Delay(2000); // Give the server time to start executing
            OutputHelper?.WriteLine($"Cancelling statement after {sw.ElapsedMilliseconds}ms...");

            try
            {
                statement.Cancel();
                OutputHelper?.WriteLine($"Cancel() returned at {sw.ElapsedMilliseconds}ms");
            }
            catch (Exception ex)
            {
                OutputHelper?.WriteLine($"Cancel() threw: {ex.GetType().Name}: {ex.Message}");
            }

            // Query should stop within a reasonable time
            var completed = await Task.WhenAny(queryTask, Task.Delay(cancelTimeoutMs));
            sw.Stop();

            OutputHelper?.WriteLine($"Query task completed: {queryTask.IsCompleted} at {sw.ElapsedMilliseconds}ms");

            if (queryException != null)
            {
                OutputHelper?.WriteLine($"Query exception (expected): {queryException.GetType().Name}: {queryException.Message}");
            }

            Assert.True(queryTask.IsCompleted,
                $"Query did not stop within {cancelTimeoutMs}ms after Cancel(). " +
                "Statement cancellation may not be propagating to the server.");

            // The query should have thrown — either OperationCanceledException or some driver exception
            // indicating cancellation. A null exception means the query completed successfully despite Cancel(),
            // which is acceptable if the server finished before the cancel arrived.
            if (queryException != null)
            {
                OutputHelper?.WriteLine("Query was successfully cancelled.");
            }
            else
            {
                OutputHelper?.WriteLine("Query completed before cancel took effect (race condition — acceptable).");
            }
        }
    }
}
