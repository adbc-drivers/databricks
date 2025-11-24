<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Databricks CloudFetch E2E Benchmark

Real end-to-end benchmark for measuring memory usage and performance of the Databricks CloudFetch implementation against an actual Databricks cluster.

## Overview

This benchmark tests the complete CloudFetch flow with real queries against a Databricks warehouse:
- Full end-to-end CloudFetch flow (query execution, downloads, LZ4 decompression, batch consumption)
- Real data from Databricks tables
- Memory usage tracking with actual network I/O
- Power BI consumption simulation with batch-proportional delays (5ms per 10K rows)

## Quick Start - Running Locally

### Prerequisites

Create a JSON config file with your Databricks cluster details:

```json
{
  "uri": "https://your-workspace.cloud.databricks.com/sql/1.0/warehouses/xxx",
  "token": "dapi...",
  "query": "select * from main.tpcds_sf1_delta.catalog_sales",
  "type": "databricks"
}
```

Or use OAuth client credentials:

```json
{
  "uri": "https://your-workspace.cloud.databricks.com/sql/1.0/warehouses/xxx",
  "auth_type": "oauth",
  "grant_type": "client_credentials",
  "client_id": "your-client-id",
  "client_secret": "your-client-secret",
  "query": "select * from main.tpcds_sf1_delta.catalog_sales",
  "type": "databricks"
}
```

### Run the Benchmark

```bash
# Set configuration
export DATABRICKS_TEST_CONFIG_FILE=/path/to/databricks-config.json

# Run via CI script
./ci/scripts/csharp_benchmark.sh $(pwd) net8.0

# Or run directly
cd csharp/Benchmarks
dotnet run -c Release --project DatabricksBenchmarks.csproj --framework net8.0 -- --filter "*CloudFetchRealE2E*"
```

### Understanding Local Results

**Console output during execution:**
```
Loaded config from: /path/to/databricks-config.json
Hostname: adb-xxx.azuredatabricks.net
HTTP Path: /sql/1.0/warehouses/xxx
Query: select * from main.tpcds_sf1_delta.catalog_sales
Benchmark will test CloudFetch with 5ms per 10K rows read delay

// Warmup
CloudFetch E2E [Delay=5ms/10K rows] - Peak memory: 272.97 MB
WorkloadWarmup   1: 1 op, 11566591709.00 ns, 11.5666 s/op

// Actual iterations (3 runs)
CloudFetch E2E [Delay=5ms/10K rows] - Peak memory: 249.11 MB
WorkloadResult   1: 1 op, 8752445353.00 ns, 8.7524 s/op
...
```

**Key Metrics:**
- **Peak Memory (MB)**: Maximum working set memory during execution (lower is better)
- **Allocated**: Total managed memory allocated during the operation
- **Gen0/Gen1/Gen2**: Number of garbage collections (lower is better)
  - Gen0: Frequent, low cost (short-lived objects)
  - Gen1/Gen2: Less frequent, higher cost (longer-lived objects)
- **Mean/Median**: Execution time statistics (includes query execution, CloudFetch downloads, LZ4 decompression, batch consumption)

---

## CI/CD Automation

### Overview

Benchmarks run automatically via GitHub Actions on every commit to the `main` branch:

- **Multi-platform**: .NET 8.0 (Ubuntu) and .NET Framework 4.7.2 (Windows)
- **Memory tracking**: Peak Memory, Allocated Memory, and Gen2 Collections
- **Artifact storage**: Detailed results retained for 90 days
- **Trend tracking**: GitHub Pages with interactive performance charts
- **Regression alerts**: Triggers if memory usage increases by 150% (2.5x baseline)

### Viewing CI Results

1. **GitHub Actions**: Go to **Actions → Performance Benchmarks** to see run history
2. **Artifacts**: Download detailed HTML/JSON/CSV reports from each run (90-day retention)
3. **Trend Charts**: View performance trends at `https://<org>.github.io/<repo>/bench/net8/` (once GitHub Pages is enabled)

### Manual Trigger

You can manually trigger a benchmark run:

1. Go to **Actions → Performance Benchmarks → Run workflow**
2. Select the `main` branch
3. Optionally specify a custom SQL query
4. Click **Run workflow**

### Automatic Triggers

Benchmarks run automatically on:
- Every push to the `main` branch (when changes affect `.github/workflows/benchmarks.yml`, `csharp/src/**`, or `csharp/Benchmarks/**`)

---

## CI/CD Setup Guide

### Required GitHub Secrets

Configure these secrets under **Settings → Secrets and variables → Actions → Environment secrets** for the `azure-prod` environment:

- `DATABRICKS_HOST`: Databricks workspace hostname (e.g., `adb-xxx.azuredatabricks.net`)
- `TEST_PECO_WAREHOUSE_HTTP_PATH`: SQL warehouse HTTP path (e.g., `/sql/1.0/warehouses/xxx`)
- `DATABRICKS_TEST_CLIENT_ID`: OAuth client ID for M2M authentication
- `DATABRICKS_TEST_CLIENT_SECRET`: OAuth client secret for M2M authentication

### Enabling Trend Tracking (GitHub Pages)

The workflow uses [`benchmark-action/github-action-benchmark`](https://github.com/benchmark-action/github-action-benchmark) to track performance trends over time.

**Enable GitHub Pages:**

1. Go to **Settings → Pages**
2. Under "Source", select **Deploy from a branch**
3. Select branch: **gh-pages** and folder: **/ (root)**
4. Click **Save**

**Configure GitHub Actions permissions:**

1. Go to **Settings → Actions → General**
2. Under "Workflow permissions", select **Read and write permissions**
3. Check **Allow GitHub Actions to create and approve pull requests**
4. Click **Save**

**First run:**
- The first benchmark run will automatically create the `gh-pages` branch
- Subsequent runs will push benchmark data to this branch
- GitHub Pages will be available at: `https://<organization>.github.io/<repository>/bench/net8/`

### Architecture

**Workflow:** `.github/workflows/benchmarks.yml`

Two parallel jobs run on each commit to main:
1. **benchmark-net8**: Runs on `ubuntu-latest` with .NET 8.0
2. **benchmark-net472**: Runs on `windows-2022` with .NET Framework 4.7.2

Each job:
1. Checks out the repository (with submodules)
2. Sets up .NET SDK
3. Creates Databricks configuration from secrets (OAuth client credentials)
4. Builds the driver
5. Runs benchmarks using `ci/scripts/csharp_benchmark.sh`
6. Uploads results as artifacts (90-day retention)
7. Extracts memory metrics (Peak Memory, Allocated, Gen2 Collections)
8. Stores results for trend tracking (main branch only)

**CI Script:** `ci/scripts/csharp_benchmark.sh`

- Takes two parameters: workspace directory and target framework (net8.0 or net472)
- Validates framework parameter
- Checks for `DATABRICKS_TEST_CONFIG_FILE` environment variable
- Runs BenchmarkDotNet with JSON, HTML, and CSV exporters
- Saves results to `csharp/Benchmarks/BenchmarkDotNet.Artifacts/results/`

---

## Configuration

### Default Benchmark Query

```sql
select * from main.tpcds_sf1_delta.catalog_sales
```

This query processes approximately 1.4M rows and tests CloudFetch with realistic data volumes.

**Using a custom query:**
1. Trigger the workflow manually (Actions → Performance Benchmarks → Run workflow)
2. Enter your custom query in the "Custom SQL query" field
3. The query will be added to the config file automatically

### Performance Alert Threshold

The workflow alerts if **memory metrics increase by 150%** or more (2.5x baseline). Since lower memory is better, the workflow uses `customSmallerIsBetter` mode.

Adjust in `.github/workflows/benchmarks.yml`:

```yaml
- name: Store benchmark results for trend tracking
  uses: benchmark-action/github-action-benchmark@v1
  with:
    tool: 'customSmallerIsBetter'  # Lower memory is better
    alert-threshold: '150%'        # Alert if memory increases to 2.5x baseline
```

### Timeout

Each benchmark job has a 30-minute timeout. Adjust if needed:

```yaml
jobs:
  benchmark-net8:
    timeout-minutes: 30  # Adjust this value
```

---

## Interpreting Results

### Artifacts

After each CI run, detailed results are uploaded as artifacts (90-day retention):

- `benchmark-results-net8`: Results from .NET 8.0 benchmark
- `benchmark-results-net472`: Results from .NET Framework 4.7.2 benchmark

Each artifact contains:
- **HTML report** (`*-report.html`): Human-readable summary
- **JSON report** (`*-report-full-compressed.json`): Detailed metrics
- **CSV export** (`*-report.csv`): Spreadsheet-compatible data

### Key Metrics

**Tracked in GitHub Pages (trend analysis):**

1. **Execution Time (seconds)**: End-to-end execution time including query execution, CloudFetch downloads, LZ4 decompression, and batch consumption
   - **Mean**: Average execution time across iterations
   - **Min**: Best (fastest) execution time - tracked across commits to monitor performance improvements
   - **Max**: Worst (slowest) execution time
   - **Median**: Middle value, less sensitive to outliers
   - Lower is better
   - Source: BenchmarkDotNet timing measurements

2. **Peak Memory (MB)**: Maximum working set memory (private bytes) during execution
   - Lower is better
   - Alert threshold: 150% increase (triggers if memory reaches 2.5x baseline)
   - Source: Custom metrics from `Process.PrivateMemorySize64`

3. **Allocated Memory (MB)**: Total managed memory allocated during execution
   - Lower is better
   - Alert threshold: 150%
   - Source: BenchmarkDotNet's `MemoryDiagnoser`

4. **Gen2 Collections**: Number of full garbage collections
   - Lower is better (indicates less memory pressure)
   - Alert threshold: 150%
   - Source: BenchmarkDotNet's `MemoryDiagnoser`

**Additional metrics in BenchmarkDotNet reports:**

5. **Total Rows/Batches**: Data volume processed

6. **GC Time %**: Percentage of time spent in garbage collection

7. **Gen0/Gen1 Collections**: Minor and partial garbage collections

### Trend Analysis

The GitHub Pages dashboard shows:
- **Execution time trends**: Line chart of Mean/Min/Max execution time across commits
- **Memory usage trends**: Line chart of Peak Memory, Allocated Memory, and Gen2 Collections across commits
- **Regression detection**: Automatic alerts when performance degrades (memory increases by 150%+)
- **Comparison view**: Compare performance between different commits

---

## Troubleshooting

### Workflow Issues

**Issue**: Workflow fails with "Config file not found"
- **Solution**: Verify GitHub secrets are properly configured in the `azure-prod` environment

**Issue**: Benchmark times out (>30 minutes)
- **Solution**: Increase `timeout-minutes` value in the workflow or use a smaller test query

**Issue**: Trend tracking not working
- **Solution**: Ensure GitHub Pages is enabled and workflow has write permissions

**Issue**: Results not showing in GitHub Pages
- **Solution**: Wait a few minutes after the first run for Pages to deploy, then check the `gh-pages` branch

### Local Execution Issues

**Issue**: "DATABRICKS_TEST_CONFIG_FILE environment variable must be set"
- **Solution**: Set the environment variable: `export DATABRICKS_TEST_CONFIG_FILE=/path/to/config.json`

**Issue**: Authentication fails
- **Solution**: Verify your token or OAuth credentials are correct in the config file

---

## Best Practices

1. **Run benchmarks on stable infrastructure**: CI uses GitHub-hosted runners which may have variable performance
2. **Use consistent test data**: The default TPC-DS query provides consistent results across runs
3. **Monitor trends, not absolute values**: Focus on relative changes over time
4. **Review alerts promptly**: Performance regressions should be investigated quickly
5. **Keep benchmarks fast**: Long-running benchmarks slow down the CI pipeline

---

## Security Considerations

- Databricks credentials are stored as GitHub secrets and never exposed in logs
- Config files are created at runtime and contain credentials only in memory
- Benchmark results do not contain sensitive data (only performance metrics)

---

## References

- [BenchmarkDotNet Documentation](https://benchmarkdotnet.org/)
- [github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
