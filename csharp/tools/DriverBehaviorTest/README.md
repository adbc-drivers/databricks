# CloudFetch Parallel Partition Stress Test

Simulates Power BI parallel partition import to reproduce and validate CloudFetch pipeline issues (ES-1778880).

Runs 10 concurrent connections, each fetching 50M rows via CloudFetch from `sales_data.sales_demo.fact_sales` (500M rows total, partitioned by `company_code` CC-001 through CC-010).

## Prerequisites

- .NET 8 SDK (for net8.0) or Mono (for net472)
- A Databricks SQL Warehouse with access to `sales_data.sales_demo.fact_sales`
- The table must have 500M rows partitioned into 10 company codes (CC-001 to CC-010)

## Configuration

Set these environment variables before running:

```bash
export DATABRICKS_HOST=adb-XXXXX.X.azuredatabricks.net
export DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXX
export DATABRICKS_WAREHOUSE_ID=XXXXXXXXXXXXXXXX
```

## Running on net8.0

```bash
cd csharp/tools/DriverBehaviorTest
dotnet run
```

## Running on net472 (matches Power BI runtime)

Power BI uses .NET Framework 4.7.2. To test with the same runtime:

### macOS (via Mono)

1. Install Mono:
   ```bash
   brew install mono
   export MONO_GAC_PREFIX="/opt/homebrew"
   ```

2. Build for net472:
   ```bash
   cd csharp/tools/DriverBehaviorTest
   dotnet build -p:IsWindows=true --framework net472
   ```

3. Run:
   ```bash
   dotnet run --framework net472 -p:IsWindows=true
   ```

### Windows

1. Build for net472:
   ```cmd
   cd csharp\tools\DriverBehaviorTest
   dotnet build -p:IsWindows=true --framework net472
   ```

2. Run:
   ```cmd
   dotnet run --framework net472 -p:IsWindows=true
   ```

## Output

The test logs:

- Per-partition progress (batch count, row count, fetch time)
- ThreadPool stats every 30 seconds
- Stall detection (partitions with no progress for >60 seconds)
- Final results (succeeded/failed count, total rows, wall time)

Example output:
```
=== Power BI Parallel Partition Simulation (POST-PR #360) ===
Table: sales_data.sales_demo.fact_sales (500M rows)
Warehouse: XXXXXXXXXXXXXXXX
Partitions: 10 parallel (CC-001 to CC-010)

[THREADPOOL] Min workers=16, Min IO=16
[THREADPOOL] Max workers=1600, Max IO=200

[10:45:40] [MAIN] All 10 tasks launched, waiting for completion...
[10:45:42] [P0] (CC-001) connected to XXXXXXXXXXXXXXXX
...
[11:50:08] [P9] (CC-010) ALL DONE: 5,00,00,000 rows, 12227 batches, 67.5min

=== FINAL RESULTS ===
Total rows: 50,00,00,000
Succeeded: 10/10
Failed: 0/10
```

## What it validates

- All 10 partitions complete without permanent stalls
- All partitions return exactly 50,000,000 rows each
- CloudFetch body read timeout detects dead connections and retries succeed
- Heartbeat poller keeps server commands alive throughout the fetch
