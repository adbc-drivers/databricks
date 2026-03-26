using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Databricks.DriverBehaviorTest
{
    /// <summary>
    /// Simulates Power BI parallel partition import (POST-PR #360).
    /// 10 partitions on warehouse 624bc39683101d30, each fetching 50M rows via CloudFetch.
    ///
    /// Power BI behavior:
    /// - Each partition gets its own connection (independent ADBC connection)
    /// - Power BI uses sync APIs (ExecuteQuery, ReadNextRecordBatch via .Result)
    /// - Partitions run in parallel (configurable batch size, default 10)
    ///
    /// Debug logging:
    /// - ThreadPool stats every 30s (to detect starvation)
    /// - Per-partition batch timing (every 100 batches)
    /// - Stall detection (if no batch in >60s, log warning)
    /// </summary>
    class Program
    {
        static readonly string Host = Environment.GetEnvironmentVariable("DATABRICKS_HOST") ?? "adb-XXXXX.X.azuredatabricks.net";
        static readonly string Token = Environment.GetEnvironmentVariable("DATABRICKS_TOKEN") ?? throw new InvalidOperationException("Set DATABRICKS_TOKEN environment variable");

        static readonly string WarehouseId = Environment.GetEnvironmentVariable("DATABRICKS_WAREHOUSE_ID") ?? throw new InvalidOperationException("Set DATABRICKS_WAREHOUSE_ID environment variable");

        static readonly string[] Partitions = new[]
        {
            "CC-001", "CC-002", "CC-003", "CC-004", "CC-005",
            "CC-006", "CC-007", "CC-008", "CC-009", "CC-010"
        };

        // Track per-partition last activity for stall detection
        static readonly long[] LastBatchTimeTicks = new long[10];
        static readonly long[] PartitionRowCounts = new long[10];
        static readonly long[] PartitionBatchCounts = new long[10];
        static readonly string[] PartitionStates = new string[10];

        static void Main(string[] args)
        {
            Console.WriteLine("=== Power BI Parallel Partition Simulation (POST-PR #360) ===");
            Console.WriteLine($"Table: sales_data.sales_demo.fact_sales (500M rows)");
            Console.WriteLine($"Warehouse: {WarehouseId}");
            Console.WriteLine($"Partitions: {Partitions.Length} parallel (CC-001 to CC-010)");
            Console.WriteLine($"Start: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            Console.WriteLine();

            // Log ThreadPool config
            ThreadPool.GetMinThreads(out int minWorker, out int minIO);
            ThreadPool.GetMaxThreads(out int maxWorker, out int maxIO);
            Console.WriteLine($"[THREADPOOL] Min workers={minWorker}, Min IO={minIO}");
            Console.WriteLine($"[THREADPOOL] Max workers={maxWorker}, Max IO={maxIO}");
            Console.WriteLine($"[THREADPOOL] Processor count={Environment.ProcessorCount}");
            Console.WriteLine();

            // Initialize state tracking
            for (int i = 0; i < 10; i++)
            {
                LastBatchTimeTicks[i] = DateTime.Now.Ticks;
                PartitionStates[i] = "STARTING";
            }

            // Start ThreadPool monitor
            var monitorCts = new CancellationTokenSource();
            var monitorTask = Task.Run(() => ThreadPoolMonitor(monitorCts.Token));

            var overallSw = Stopwatch.StartNew();
            var tasks = new Task<PartitionResult>[Partitions.Length];

            // Launch all 10 in parallel — same as Power BI batch of 10
            for (int i = 0; i < Partitions.Length; i++)
            {
                var idx = i;
                var companyCode = Partitions[i];

                // Power BI uses sync API on separate threads
                tasks[i] = Task.Run(() => FetchPartition(idx, WarehouseId, companyCode));
            }

            Log("MAIN", $"All {Partitions.Length} tasks launched, waiting for completion...");

            // Wait for ALL — DO NOT CANCEL
            try
            {
                Task.WaitAll(tasks);
            }
            catch (AggregateException)
            {
                // Individual errors in PartitionResult
            }

            monitorCts.Cancel();
            overallSw.Stop();

            Console.WriteLine();
            Console.WriteLine("=============================================================");
            Console.WriteLine("=== FINAL RESULTS ===");
            Console.WriteLine("=============================================================");

            long totalRows = 0;
            int succeeded = 0, failed = 0;

            for (int i = 0; i < tasks.Length; i++)
            {
                var r = (tasks[i].Status == TaskStatus.RanToCompletion)
                    ? tasks[i].Result
                    : new PartitionResult
                    {
                        Partition = Partitions[i],
                        WarehouseId = WarehouseId,
                        Success = false,
                        Error = tasks[i].Exception?.InnerException?.Message ?? "Task failed"
                    };

                var status = r.Success ? "OK" : "FAILED";
                Console.WriteLine(
                    $"  [{status}] P{i} {r.Partition} wh={r.WarehouseId}: " +
                    $"{r.RowCount:N0} rows, {r.BatchCount} batches, {r.Duration.TotalMinutes:F1}min" +
                    (r.Success ? "" : $" | ERROR: {r.Error}"));

                if (r.Success) { succeeded++; totalRows += r.RowCount; }
                else { failed++; }
            }

            Console.WriteLine();
            Console.WriteLine($"Total rows: {totalRows:N0}");
            Console.WriteLine($"Succeeded: {succeeded}/{Partitions.Length}");
            Console.WriteLine($"Failed: {failed}/{Partitions.Length}");
            Console.WriteLine($"Wall time: {overallSw.Elapsed.TotalMinutes:F1} min");
            Console.WriteLine($"End: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        }

        static PartitionResult FetchPartition(int idx, string warehouseId, string companyCode)
        {
            var result = new PartitionResult { Partition = companyCode, WarehouseId = warehouseId };
            PartitionStates[idx] = "CONNECTING";

            try
            {
                // Each partition gets its own fresh connection — like Power BI
                var properties = new Dictionary<string, string>
                {
                    ["adbc.spark.type"] = "http",
                    ["adbc.spark.host"] = Host,
                    ["adbc.spark.port"] = "443",
                    ["adbc.spark.path"] = $"/sql/1.0/warehouses/{warehouseId}",
                    ["adbc.connection.autocommit"] = "true",
                    ["adbc.spark.token"] = Token,
                    ["adbc.databricks.cloudfetch.enabled"] = "true",
                    ["adbc.databricks.cloudfetch.parallel_downloads"] = "16",
                    ["adbc.databricks.cloudfetch.prefetch_count"] = "16",
                    ["adbc.databricks.fetch_heartbeat_interval"] = "60",
                };

                var driver = new DatabricksDriver();
                var database = driver.Open(properties);
                var connection = database.Connect(null);
                Log($"P{idx}", $"({companyCode}) connected to {warehouseId}");

                var sql = $"SELECT * FROM sales_data.sales_demo.fact_sales WHERE company_code = '{companyCode}' AND company_code IS NOT NULL";
                var statement = connection.CreateStatement();
                statement.SqlQuery = sql;

                PartitionStates[idx] = "EXECUTING";
                var sw = Stopwatch.StartNew();

                // Power BI calls sync ExecuteQuery()
                var queryResult = statement.ExecuteQuery();
                Log($"P{idx}", $"({companyCode}) query done in {sw.Elapsed.TotalSeconds:F1}s");

                PartitionStates[idx] = "READING";
                var reader = queryResult.Stream;
                long totalRows = 0;
                int batchCount = 0;
                var lastLogTime = Stopwatch.StartNew();
                Interlocked.Exchange(ref LastBatchTimeTicks[idx], DateTime.Now.Ticks);

                while (true)
                {
                    var batchSw = Stopwatch.StartNew();
                    RecordBatch? batch;
                    try
                    {
                        // Power BI uses sync read — .Result blocks the thread
                        batch = reader.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        PartitionStates[idx] = "ERROR";
                        Log($"P{idx}", $"({companyCode}) READ ERROR at batch {batchCount + 1}, " +
                            $"{sw.Elapsed.TotalMinutes:F1}min, rows={totalRows:N0}: " +
                            $"{ex.GetType().Name}: {ex.Message}");
                        if (ex.InnerException != null)
                            Log($"P{idx}", $"  Inner: {ex.InnerException.GetType().Name}: {ex.InnerException.Message}");

                        result.Success = false;
                        result.Error = ex.InnerException?.Message ?? ex.Message;
                        result.RowCount = totalRows;
                        result.BatchCount = batchCount;
                        result.Duration = sw.Elapsed;

                        try { reader.Dispose(); } catch { }
                        try { statement.Dispose(); } catch { }
                        try { connection.Dispose(); } catch { }
                        try { database.Dispose(); } catch { }
                        return result;
                    }

                    if (batch == null)
                    {
                        PartitionStates[idx] = "DONE";
                        break;
                    }

                    batchCount++;
                    totalRows += batch.Length;
                    Interlocked.Exchange(ref LastBatchTimeTicks[idx], DateTime.Now.Ticks);
                    Interlocked.Exchange(ref PartitionRowCounts[idx], totalRows);
                    Interlocked.Exchange(ref PartitionBatchCounts[idx], (long)batchCount);

                    var fetchMs = batchSw.ElapsedMilliseconds;

                    // Log every 100 batches or if fetch was slow (>5s)
                    if (batchCount == 1 || batchCount % 100 == 0 || fetchMs > 5000)
                    {
                        Log($"P{idx}", $"({companyCode}) batch #{batchCount}: {totalRows:N0} rows | " +
                            $"fetch={fetchMs}ms | elapsed={sw.Elapsed.TotalMinutes:F1}min");
                    }
                }

                sw.Stop();
                Log($"P{idx}", $"({companyCode}) ALL DONE: {totalRows:N0} rows, {batchCount} batches, {sw.Elapsed.TotalMinutes:F1}min");

                PartitionStates[idx] = "DISPOSING";
                Log($"P{idx}", $"({companyCode}) disposing reader...");
                reader.Dispose();
                Log($"P{idx}", $"({companyCode}) reader disposed");

                statement.Dispose();
                connection.Dispose();
                database.Dispose();
                PartitionStates[idx] = "COMPLETE";

                result.Success = true;
                result.RowCount = totalRows;
                result.BatchCount = batchCount;
                result.Duration = sw.Elapsed;
            }
            catch (Exception ex)
            {
                PartitionStates[idx] = "EXCEPTION";
                Log($"P{idx}", $"({companyCode}) EXCEPTION: {ex.GetType().Name}: {ex.Message}");
                result.Success = false;
                result.Error = ex.Message;
            }

            return result;
        }

        /// <summary>
        /// Monitors ThreadPool health and per-partition stalls every 30 seconds.
        /// </summary>
        static void ThreadPoolMonitor(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    Thread.Sleep(30000); // 30s interval
                    if (ct.IsCancellationRequested) break;

                    ThreadPool.GetAvailableThreads(out int availWorker, out int availIO);
                    ThreadPool.GetMinThreads(out int minWorker, out int minIO);
                    ThreadPool.GetMaxThreads(out int maxWorker, out int maxIO);
                    int busyWorker = maxWorker - availWorker;
                    int busyIO = maxIO - availIO;

                    var now = DateTime.Now;

                    // Check for stalled partitions
                    var stalled = new List<string>();
                    var active = new List<string>();
                    for (int i = 0; i < 10; i++)
                    {
                        var lastTicks = Interlocked.Read(ref LastBatchTimeTicks[i]);
                        var lastTime = new DateTime(lastTicks);
                        var stalledSec = (now - lastTime).TotalSeconds;
                        var rows = Interlocked.Read(ref PartitionRowCounts[i]);
                        var batches = Interlocked.Read(ref PartitionBatchCounts[i]);
                        var state = PartitionStates[i];

                        if (state == "READING" && stalledSec > 60)
                        {
                            stalled.Add($"P{i}({Partitions[i]}):{rows:N0}rows,stalled {stalledSec:F0}s");
                        }
                        else if (state == "READING")
                        {
                            active.Add($"P{i}:{rows:N0}");
                        }
                    }

                    // Build state summary
                    var states = new Dictionary<string, int>();
                    for (int i = 0; i < 10; i++)
                    {
                        var s = PartitionStates[i];
                        states[s] = (states.ContainsKey(s) ? states[s] : 0) + 1;
                    }
                    var stateSummary = string.Join(", ", states.Select(kv => $"{kv.Key}={kv.Value}"));

                    Log("MONITOR",
                        $"ThreadPool: busy={busyWorker}w/{busyIO}io, avail={availWorker}w/{availIO}io, min={minWorker}w | " +
                        $"States: [{stateSummary}]");

                    if (stalled.Count > 0)
                    {
                        Log("MONITOR", $"*** STALLED ({stalled.Count}): {string.Join("; ", stalled)}");
                    }
                    if (active.Count > 0)
                    {
                        Log("MONITOR", $"Active ({active.Count}): {string.Join(", ", active)}");
                    }
                }
                catch (ThreadInterruptedException) { break; }
                catch (Exception) { /* ignore monitor errors */ }
            }
        }

        static void Log(string tag, string message)
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] [{tag}] {message}");
        }

        class PartitionResult
        {
            public string Partition { get; set; } = "";
            public string WarehouseId { get; set; } = "";
            public bool Success { get; set; }
            public long RowCount { get; set; }
            public int BatchCount { get; set; }
            public TimeSpan Duration { get; set; }
            public string? Error { get; set; }
        }
    }
}
