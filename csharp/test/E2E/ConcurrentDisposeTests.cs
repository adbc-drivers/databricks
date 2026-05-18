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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using AdbcDrivers.HiveServer2;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// Tests for concurrent connection disposal and thread safety.
    /// Investigates known deadlock when multiple threads call Dispose() simultaneously.
    /// See: databricks-driver-test SESSION-007 (skipped due to deadlock in CI).
    /// </summary>
    public class ConcurrentDisposeTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        private const int DeadlockTimeoutMs = 30_000; // 30 seconds — generous for a Dispose

        public ConcurrentDisposeTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Reproduces the deadlock from databricks-driver-test SESSION-007.
        /// Three threads call Dispose() on the same connection concurrently.
        /// Expected: completes within 30s without deadlock.
        /// Root cause: HiveServer2Connection.DisposeClient() calls
        /// _client.CloseSession().Result (sync-over-async) while ThreadSafeClient
        /// serializes via SemaphoreSlim(1,1). No guard against concurrent entry.
        /// </summary>
        [SkippableFact]
        public async Task ConcurrentDispose_ShouldNotDeadlock()
        {
            // Arrange — open a live connection and execute a query to ensure it's active
            using var cts = new CancellationTokenSource(DeadlockTimeoutMs);
            AdbcConnection connection = NewConnection();

            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = "SELECT 1 AS test_value";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                while (await reader.ReadNextRecordBatchAsync() != null) { }
            }

            OutputHelper?.WriteLine("Connection is active, starting concurrent Dispose");

            // Act — 3 threads race to Dispose, with a timeout to detect deadlock
            var exceptions = new ConcurrentBag<Exception>();
            var sw = Stopwatch.StartNew();

            var disposeTask = Task.Run(async () =>
            {
                var tasks = Enumerable.Range(0, 3).Select(_ =>
                    Task.Run(() =>
                    {
                        try
                        {
                            connection.Dispose();
                        }
                        catch (Exception ex)
                        {
                            exceptions.Add(ex);
                        }
                    })
                ).ToArray();

                await Task.WhenAll(tasks);
            });

            var completed = await Task.WhenAny(disposeTask, Task.Delay(DeadlockTimeoutMs));
            sw.Stop();

            // Assert
            if (completed != disposeTask)
            {
                // The test timed out — this IS the deadlock bug
                OutputHelper?.WriteLine($"DEADLOCK DETECTED: Dispose did not complete within {DeadlockTimeoutMs}ms");
                Assert.Fail(
                    $"Concurrent Dispose() deadlocked. Did not complete within {DeadlockTimeoutMs}ms. " +
                    "Root cause: HiveServer2Connection.DisposeClient() uses .Result on async CloseSession " +
                    "through ThreadSafeClient's SemaphoreSlim(1,1), with no concurrent-entry guard.");
            }

            OutputHelper?.WriteLine($"Dispose completed in {sw.ElapsedMilliseconds}ms");

            // If it didn't deadlock, check that no unexpected exceptions were thrown.
            // ObjectDisposedException is acceptable (second/third Dispose racing).
            var unexpectedExceptions = exceptions
                .Where(ex => ex is not ObjectDisposedException)
                .ToList();

            foreach (var ex in unexpectedExceptions)
            {
                OutputHelper?.WriteLine($"Unexpected exception: {ex.GetType().Name}: {ex.Message}");
            }

            // We don't fail on ObjectDisposedException — that's a valid (if noisy) outcome.
            // But other exceptions indicate a real problem.
            Assert.Empty(unexpectedExceptions);
        }

        /// <summary>
        /// Tests that disposing a connection while a query is executing does not hang.
        /// This simulates a user closing their application while a query is in-flight.
        /// </summary>
        [SkippableFact]
        public async Task DisposeWhileQueryExecuting_ShouldNotHang()
        {
            using var cts = new CancellationTokenSource(DeadlockTimeoutMs);
            AdbcConnection connection = NewConnection();

            OutputHelper?.WriteLine("Starting long query, will dispose connection mid-execution");

            // Start a long-running query in the background
            var queryException = default(Exception);
            var queryTask = Task.Run(() =>
            {
                try
                {
                    using var statement = connection.CreateStatement();
                    // A query that takes a few seconds to execute
                    statement.SqlQuery = "SELECT COUNT(*) FROM (SELECT a.id FROM range(10000000) a CROSS JOIN range(100) b)";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;
                    while (reader.ReadNextRecordBatchAsync().Result != null) { }
                }
                catch (Exception ex)
                {
                    queryException = ex;
                }
            });

            // Give the query time to start executing (polling GetOperationStatus)
            await Task.Delay(500);

            // Dispose from the main thread while query is in-flight
            var disposeTask = Task.Run(() =>
            {
                try
                {
                    connection.Dispose();
                }
                catch (Exception ex)
                {
                    OutputHelper?.WriteLine($"Dispose exception (may be expected): {ex.GetType().Name}: {ex.Message}");
                }
            });

            var completed = await Task.WhenAny(
                Task.WhenAll(queryTask, disposeTask),
                Task.Delay(DeadlockTimeoutMs));

            if (completed is Task delayTask && delayTask == await Task.WhenAny(Task.Delay(0), completed))
            {
                // Check if we actually timed out
                if (!queryTask.IsCompleted || !disposeTask.IsCompleted)
                {
                    OutputHelper?.WriteLine("HANG DETECTED: Dispose or query did not complete within timeout");
                    Assert.Fail(
                        $"Dispose while query executing hung for more than {DeadlockTimeoutMs}ms. " +
                        "The connection should cancel the in-flight operation and clean up.");
                }
            }

            OutputHelper?.WriteLine($"Query exception (expected): {queryException?.GetType().Name}: {queryException?.Message}");
            OutputHelper?.WriteLine("Dispose while query executing completed without hanging");
        }

        /// <summary>
        /// Tests that sequential Dispose calls (not concurrent) don't throw.
        /// Double-dispose should be a no-op per IDisposable contract.
        /// </summary>
        [SkippableFact]
        public void DoubleDispose_ShouldBeNoOp()
        {
            AdbcConnection connection = NewConnection();

            // Execute a query to activate the session
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = "SELECT 1 AS test_value";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
            }

            // First dispose — should close the session
            connection.Dispose();
            OutputHelper?.WriteLine("First Dispose completed");

            // Second dispose — should be a no-op, not throw
            connection.Dispose();
            OutputHelper?.WriteLine("Second Dispose completed (no-op)");
        }
    }
}
