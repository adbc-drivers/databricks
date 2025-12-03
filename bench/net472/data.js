window.BENCHMARK_DATA = {
  "lastUpdate": 1764785912977,
  "repoUrl": "https://github.com/adbc-drivers/databricks",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "e.wang@databricks.com",
            "name": "Eric Wang",
            "username": "eric-wang-1990"
          },
          "committer": {
            "email": "e.wang@databricks.com",
            "name": "Eric Wang",
            "username": "eric-wang-1990"
          },
          "distinct": true,
          "id": "3b5439c120c59480327fb9c6156793bec228c6d6",
          "message": "test(ci): allow benchmark tracking on test branch\n\nTemporarily modify condition to run 'Store benchmark results' step\non the test branch so we can verify the permissions fix works.\n\nThis tests that the workflow can successfully push to gh-pages with\nthe new 'contents: write' permission.\n\nWill be reverted before merge.",
          "timestamp": "2025-11-30T22:56:32-08:00",
          "tree_id": "603c1596d6c027303b42a3990ebd234aa22ba29d",
          "url": "https://github.com/adbc-drivers/databricks/commit/3b5439c120c59480327fb9c6156793bec228c6d6"
        },
        "date": 1764572403639,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Peak Memory (MB)",
            "value": 346.19140625,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 539.65,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 7,
            "unit": "collections"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "e.wang@databricks.com",
            "name": "Eric Wang",
            "username": "eric-wang-1990"
          },
          "committer": {
            "email": "e.wang@databricks.com",
            "name": "Eric Wang",
            "username": "eric-wang-1990"
          },
          "distinct": true,
          "id": "b17c1200e3fff8c0d8eef7cfaf9f9c7ffe5e39f4",
          "message": "feat(ci): add Min Execution Time to benchmark tracking\n\nExtracts and tracks the minimum execution time from BenchmarkDotNet results.\nThis metric appears at the top of the GitHub Pages dashboard.\n\nThe Min time represents the best (fastest) execution time across all\nbenchmark iterations, useful for tracking performance improvements.\n\nTime is converted from nanoseconds to seconds with 3 decimal places.",
          "timestamp": "2025-11-30T23:09:19-08:00",
          "tree_id": "a230f11e784826680a2c85fbe5006f57a2d03ac4",
          "url": "https://github.com/adbc-drivers/databricks/commit/b17c1200e3fff8c0d8eef7cfaf9f9c7ffe5e39f4"
        },
        "date": 1764573179056,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Min Execution Time (s)",
            "value": 4.753,
            "unit": "seconds"
          },
          {
            "name": "Peak Memory (MB)",
            "value": 339.1328125,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 540.02,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 8,
            "unit": "collections"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "e.wang@databricks.com",
            "name": "Eric Wang",
            "username": "eric-wang-1990"
          },
          "committer": {
            "email": "e.wang@databricks.com",
            "name": "Eric Wang",
            "username": "eric-wang-1990"
          },
          "distinct": true,
          "id": "6a065767e83e0934a1daeb5adaadacd3da026e21",
          "message": "feat(ci): enable benchmark workflow on pull requests\n\nAllows benchmarks to run automatically on PRs that modify:\n- Benchmark workflow itself\n- C# driver source code\n- Benchmark code\n\nKey behaviors:\n- Benchmarks run and produce artifacts for review\n- Results are NOT pushed to gh-pages (main branch only)\n- Any contributor can trigger via PR (no special permissions needed)\n- Provides performance feedback before merging\n\nThis helps developers understand performance impact of their changes\nwithout requiring manual workflow triggers or admin access.",
          "timestamp": "2025-11-30T23:12:56-08:00",
          "tree_id": "a0fbcaa21ea5c87ecfd4c8a6cbe193fcde06bb23",
          "url": "https://github.com/adbc-drivers/databricks/commit/6a065767e83e0934a1daeb5adaadacd3da026e21"
        },
        "date": 1764573381601,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Min Execution Time (s)",
            "value": 4.24,
            "unit": "seconds"
          },
          {
            "name": "Peak Memory (MB)",
            "value": 334.23828125,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 537.81,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 7,
            "unit": "collections"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "e.wang@databricks.com",
            "name": "Eric Wang",
            "username": "eric-wang-1990"
          },
          "committer": {
            "email": "e.wang@databricks.com",
            "name": "Eric Wang",
            "username": "eric-wang-1990"
          },
          "distinct": true,
          "id": "60131a9abc2a7acf6ce4d4d37308dcccab696cfd",
          "message": "feat(ci): add label-based PR benchmarks with automatic comparison\n\nImplements two key features for PR performance testing:\n\n1. Label-Based Triggering:\n   - Only runs when 'benchmark' label is added to PR\n   - Opt-in approach saves CI resources\n   - No runs on every commit - developers choose when to benchmark\n\n2. Automatic PR Comparison Comments:\n   - Compares PR results against main branch baseline (gh-pages)\n   - Posts detailed comparison comment on the PR\n   - Alert threshold: 110% (alerts if metrics regress by >10%)\n   - Shows all 4 metrics: Min Time, Peak Memory, Allocated Memory, Gen2 Collections\n   - Visual indicators for improvements/regressions\n\nUsage:\n1. Open a PR that modifies driver/benchmark code\n2. Add 'benchmark' label to the PR\n3. Workflow runs automatically (~30 minutes)\n4. Comment appears with comparison vs baseline\n5. Review performance impact before merging\n\nBenefits:\n- Cost-effective: only runs when explicitly requested\n- Accessible: any contributor with write access can add labels\n- Informative: immediate feedback on performance changes\n- Non-blocking: alerts don't fail the workflow",
          "timestamp": "2025-11-30T23:26:44-08:00",
          "tree_id": "f8a47277bd6bd3cdbcd67ab8bde8a67c3addc5fe",
          "url": "https://github.com/adbc-drivers/databricks/commit/60131a9abc2a7acf6ce4d4d37308dcccab696cfd"
        },
        "date": 1764574216649,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Min Execution Time (s)",
            "value": 4.726,
            "unit": "seconds"
          },
          {
            "name": "Peak Memory (MB)",
            "value": 320.84375,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 539.62,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 9,
            "unit": "collections"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "115501094+eric-wang-1990@users.noreply.github.com",
            "name": "eric-wang-1990",
            "username": "eric-wang-1990"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "59075d157489cea7f9b93f4536a5f23a731bda73",
          "message": "feat(csharp): improvement for benchmark ci/cd (#57)\n\n## Summary\n\nThis PR enhances the benchmark workflow with permissions fixes, new\nmetrics, and PR-based performance testing capabilities.\n\n### 1. Fixed gh-pages Push Permissions ✅\n\n**Problem:** Benchmark workflow was failing with 403 error when trying\nto push results to gh-pages:\n```\nremote: Write access to repository not granted.\nfatal: unable to access 'https://github.com/adbc-drivers/databricks.git/': The requested URL returned error: 403\n```\n\n**Solution:** Added explicit `contents: write` permission to the\nworkflow, following the principle of least privilege.\n\n### 2. Added Min Execution Time Tracking 📊\n\n**New Metric:** Min Execution Time (seconds) - the fastest execution\ntime across benchmark iterations\n\n- Tracks performance improvements over time\n- Displayed at the top of GitHub Pages dashboard\n- Extracted from BenchmarkDotNet's `Statistics.Min`\n- Converted from nanoseconds to seconds (3 decimal precision)\n\n**Tracked Metrics (in order):**\n1. Min Execution Time (s)\n2. Peak Memory (MB)\n3. Allocated Memory (MB)\n4. Gen2 Collections\n\n### 3. Label-Based PR Benchmarking 🏷️\n\n**Feature:** Run benchmarks on PRs by adding the `benchmark` label\n\n**How it works:**\n1. Add `benchmark` label to any PR\n2. Workflow triggers automatically (~30 minutes)\n3. Automatic comparison comment posted on PR\n4. Review performance impact before merging\n\n**Benefits:**\n- ✅ **Opt-in:** Only runs when explicitly requested (cost-effective)\n- ✅ **Accessible:** Any contributor with write access can add labels\n- ✅ **Informative:** Immediate feedback on performance changes\n- ✅ **Non-blocking:** Alerts don't fail the workflow\n- ✅ **Clean:** PR results don't pollute gh-pages history\n\n### 4. Automatic PR Comparison Comments 💬\n\n**Feature:** Automatic performance comparison against main branch\nbaseline\n\n**Example comment:**\n```markdown\n## 🎯 Benchmark Results (.NET 8.0)\n\n| Metric | Baseline (main) | This PR | Change | Status |\n|--------|----------------|---------|--------|--------|\n| Min Execution Time (s) | 3.794 | 3.821 | +0.7% | ✅ |\n| Peak Memory (MB) | 420.98 | 398.12 | -5.4% | 🟢 |\n| Allocated Memory (MB) | 286.19 | 275.43 | -3.8% | 🟢 |\n| Gen2 Collections | 61 | 58 | -4.9% | 🟢 |\n```\n\n**Alert Thresholds:**\n- **PRs:** 110% (alerts if metrics regress >10%)\n- **Main branch:** 150% (alerts if metrics regress >50%)\n\n**Visual Indicators:**\n- 🟢 **Improvement** - Metric improved (lower is better)\n- ✅ **No significant change** - Within acceptable range\n- ⚠️ **Regression** - Metric degraded significantly\n\n### 5. Created `benchmark` Label 🏷️\n\nCreated repository label for easy discovery:\n- **Name:** `benchmark`\n- **Description:** \"Run performance benchmarks on this PR\"\n- **Color:** Green (#0E8A16)\n\nAvailable in dropdown when adding labels to PRs.\n\n### 6. Updated Documentation 📚\n\nComprehensive README updates in `csharp/Benchmarks/README.md`:\n- New section: \"Pull Request Benchmarking (Label-Based)\"\n- Step-by-step instructions with examples\n- Updated overview to include all 4 tracked metrics\n- Documented alert thresholds for PRs vs main\n- Clarified Min Execution Time as primary performance metric\n\n## Testing\n\nTested on this PR branch (`eric/fix-benchmark-permissions`):\n1. ✅ Workflow successfully pushed to gh-pages (no 403 errors)\n2. ✅ Min Execution Time extracted and tracked\n3. ✅ Label-based triggering works correctly\n4. ✅ All 4 metrics tracked and displayed\n5. ✅ GitHub Pages updated with new data structure\n\nView results: https://adbc-drivers.github.io/databricks/bench/net8/\n\n## Usage After Merge\n\n**For Main Branch (Automatic):**\n- Benchmarks run on every merge to main\n- Results published to GitHub Pages\n- Historical trend tracking\n\n**For Pull Requests (Label-Based):**\n1. Add `benchmark` label to PR\n2. Wait ~30 minutes for workflow completion\n3. Review automatic comparison comment\n4. Make informed decisions about performance trade-offs\n\n## Impact\n\n- **Cost-effective:** No benchmark runs on every PR commit (only when\nlabeled)\n- **Developer-friendly:** Easy to test performance impact of changes\n- **Informative:** Clear comparison against baseline with visual\nindicators\n- **Non-disruptive:** PR benchmarks don't affect historical tracking on\ngh-pages\n\n## References\n\n- Original error:\nhttps://github.com/adbc-drivers/databricks/actions/runs/19756218411/job/56731242926\n- Benchmark action:\nhttps://github.com/benchmark-action/github-action-benchmark\n- GitHub Pages dashboard:\nhttps://adbc-drivers.github.io/databricks/bench/net8/\n\n---\n\n🤖 Generated with [Claude Code](https://claude.com/claude-code)\n\n---------\n\nCo-authored-by: Claude <noreply@anthropic.com>",
          "timestamp": "2025-12-01T11:31:07-08:00",
          "tree_id": "62d39073ba709de13ff88475ab31c9b3dce787f3",
          "url": "https://github.com/adbc-drivers/databricks/commit/59075d157489cea7f9b93f4536a5f23a731bda73"
        },
        "date": 1764617697384,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Min Execution Time (s)",
            "value": 5.731,
            "unit": "seconds"
          },
          {
            "name": "Peak Memory (MB)",
            "value": 354.1953125,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 528.63,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 6,
            "unit": "collections"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "115501094+eric-wang-1990@users.noreply.github.com",
            "name": "eric-wang-1990",
            "username": "eric-wang-1990"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "96666162e250716f19514ea50c712b828ef6040d",
          "message": "fix(ci): fetch baseline data from gh-pages for PR comparison (#59)\n\n## Summary\n\nFixes the PR benchmark comparison by adding baseline data fetch from\ngh-pages and adding the required `pull-requests: write` permission.\n\n## Problem 1: Missing Baseline Data\n\nWhen adding the `benchmark` label to PRs, the comparison step was\nshowing a warning:\n```\nWarning: Could not find external JSON file for benchmark data at \nD:\\a\\databricks\\databricks\\cache\\benchmark-data.json. \nUsing empty default: Error: ENOENT: no such file or directory\n```\n\nThis happened because the workflow expected baseline data in\n`./cache/benchmark-data.json` but never fetched it from gh-pages.\n\n## Problem 2: Missing PR Comment Permission\n\nEven after fixing the baseline data fetch, comments were not being\nposted on PRs because the workflow lacked `pull-requests: write`\npermission.\n\n## Solution\n\n### 1. Added baseline data fetch step\n\n**\"Download baseline benchmark data from gh-pages\"**\n\nThis step runs before the comparison and:\n1. ✅ Fetches the `gh-pages` branch (shallow, depth=1 for speed)\n2. ✅ Extracts the `data.js` file from `bench/net8/` or `bench/net472/`\n3. ✅ Parses the JavaScript format: `window.BENCHMARK_DATA = {...}`\n4. ✅ Extracts the last entry's benches array (most recent baseline from\nmain)\n5. ✅ Saves it as JSON to `./cache/benchmark-data.json`\n6. ✅ Handles errors gracefully (uses empty array if no baseline found)\n\nImplemented for both platforms:\n- **Linux (.NET 8.0)**: Bash script with Node.js for JSON parsing\n- **Windows (.NET Framework 4.7.2)**: PowerShell script\n\n### 2. Added pull-requests write permission\n\n```yaml\npermissions:\n  contents: write       # Required to push benchmark results to gh-pages branch\n  pull-requests: write  # Required to post comparison comments on PRs\n```\n\n## Impact\n\n**Before:**\n```\n⚠️ Warning: Could not find external JSON file\nUsing empty default (no comparison data)\n❌ No comments posted on PRs\n```\n\n**After:**\n```\n## 🎯 Benchmark Results (.NET 8.0)\n\n| Metric | Baseline (main) | This PR | Change | Status |\n|--------|----------------|---------|--------|--------|\n| Min Execution Time (s) | 3.794 | 5.41 | +42.6% | ⚠️ |\n| Peak Memory (MB) | 420.98 | 360.30 | -14.4% | 🟢 |\n| Allocated Memory (MB) | 286.19 | 530.57 | +85.3% | ⚠️ |\n| Gen2 Collections | 61 | 7 | -88.5% | 🟢 |\n```\n\nNow PR comparison comments show:\n- ✅ Actual baseline values from main branch (most recent run on\ngh-pages)\n- ✅ PR values from current run\n- ✅ Percentage change for each metric\n- ✅ Visual indicators for improvements (🟢) and regressions (⚠️)\n- ✅ No more warnings about missing baseline data\n- ✅ Comments posted automatically on PRs\n\n## Testing\n\nTo test, add the `benchmark` label to this PR and verify:\n- ✅ Baseline data successfully downloaded from gh-pages\n- ✅ Comparison shows actual baseline vs PR values with percentage\nchanges\n- ✅ Warning eliminated\n- ✅ All 4 metrics tracked and compared correctly\n- ✅ Comments posted by both jobs (.NET 8.0 and .NET 4.7.2)\n\n## Dependencies\n\nBuilds on PR #57 (already merged) which added:\n- Label-based PR benchmarking\n- Min Execution Time tracking\n- PR comparison infrastructure\n\nThis PR completes the feature by:\n- Fixing the baseline data fetch\n- Adding the required permission for PR comments\n\n🤖 Generated with [Claude Code](https://claude.com/claude-code)\n\n---------\n\nCo-authored-by: Claude <noreply@anthropic.com>",
          "timestamp": "2025-12-01T17:50:46-08:00",
          "tree_id": "9f0c7feac8750f08fcec8fbb467b1aa2013be02a",
          "url": "https://github.com/adbc-drivers/databricks/commit/96666162e250716f19514ea50c712b828ef6040d"
        },
        "date": 1764640459463,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Min Execution Time (s)",
            "value": 5.692,
            "unit": "seconds"
          },
          {
            "name": "Peak Memory (MB)",
            "value": 357.078125,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 526.47,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 8,
            "unit": "collections"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "111902719+jadewang-db@users.noreply.github.com",
            "name": "Jade Wang",
            "username": "jadewang-db"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f74d92c24494e1009230b2618758e1d2ab563a3d",
          "message": "feat(csharp): implement Statement Execution REST API support (PECO-2791) (#54)\n\n## 🥞 Stacked PR\nUse this\n[link](https://github.com/adbc-drivers/databricks/pull/54/files) to\nreview incremental changes.\n-\n[**stack/PECO-2791-rest-execution-flow-v2**](https://github.com/adbc-drivers/databricks/pull/54)\n[[Files\nchanged](https://github.com/adbc-drivers/databricks/pull/54/files)]\n-\n[stack/PECO-2857-oauth-auth-statement-execution](https://github.com/adbc-drivers/databricks/pull/62)\n[[Files\nchanged](https://github.com/adbc-drivers/databricks/pull/62/files/e0ccba1d406dea5550fc9b015ee966c9a7d4c235..67476f2a845e6112b664e5ca5fd20b04f3c51ba6)]\n\n---------\n## Summary\n\nThis PR implements the core execution flow for the Databricks Statement\nExecution REST API as an alternative to the Thrift protocol, addressing\nPECO-2791 (Phase 1 MVP).\n\n## What's Changed\n\n### New Components\n\n1. **StatementExecutionConnection**\n(`csharp/src/StatementExecution/StatementExecutionConnection.cs`)\n- Manages session lifecycle (create/delete sessions with warehouse_id)\n   - Handles configuration for REST API parameters\n   - Manages HTTP client and memory pooling for CloudFetch\n\n2. **StatementExecutionStatement**\n(`csharp/src/StatementExecution/StatementExecutionStatement.cs`)\n   - Executes queries via REST API with configurable wait timeout\n   - Implements polling for async queries (default: 1000ms interval)\n   - Supports hybrid result disposition (inline_or_external_links)\n   - Handles truncated results warnings\n   - Proper disposal and resource cleanup\n\n3. **Protocol Selection** (`DatabricksDatabase.cs`)\n   - Routes connections based on `adbc.databricks.protocol` parameter\n- Supports \"thrift\" (default, backward compatible) and \"rest\" protocols\n   - Validates protocol selection and provides clear error messages\n\n4. **Configuration Parameters** (`DatabricksParameters.cs`)\n   - Added `adbc.databricks.warehouse_id` (required for REST API)\n- REST-specific parameters: result_disposition, result_format,\nresult_compression\n- Wait timeout, polling interval, and session management configuration\n\n### Scope\n\n✅ **Implemented in this PR:**\n- Core query execution flow via REST API\n- Protocol selection logic in DatabricksDatabase\n- Session management (create/delete)\n- Polling for async query execution\n- Hybrid result disposition support\n- Truncated results warning handling\n- Configuration infrastructure\n\n❌ **Not in scope (future work):**\n- Metadata operations (GetObjects, GetTableTypes) → PECO-2792\n- CloudFetch integration for external links → PECO-2790  \n- Inline results reader implementation → PECO-2792\n- Authentication handlers (OAuth, token refresh, token exchange) →\nFuture work\n- Unit and integration tests → Follow-up work\n\n### Design\n\nThis implementation follows the design documented in\n`csharp/doc/statement-execution-api-design.md`, specifically\nimplementing Phase 1 (Core Implementation MVP).\n\n**Key design decisions:**\n- Backward compatible: Existing Thrift implementation unchanged\n- Protocol selection at connection creation time\n- Reuses existing HTTP client infrastructure (TODO: add auth handlers)\n- Throws NotImplementedException for unimplemented features (metadata,\nCloudFetch, inline reader)\n\n### Testing\n\n- ✅ Build succeeds for all target frameworks (net472, netstandard2.0,\nnet8.0)\n- ⚠️ Unit and integration tests to be added in follow-up work\n\n### Breaking Changes\n\nNone. This PR is fully backward compatible as it defaults to the Thrift\nprotocol.\n\nCloses #PECO-2791.\n\nCo-authored-by: Jade Wang <jade.wang+data@databricks.com>\nCo-authored-by: Claude <noreply@anthropic.com>",
          "timestamp": "2025-12-02T16:13:30-08:00",
          "tree_id": "055f2974912222ac7155272a425030cc0ed18d33",
          "url": "https://github.com/adbc-drivers/databricks/commit/f74d92c24494e1009230b2618758e1d2ab563a3d"
        },
        "date": 1764721033707,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Min Execution Time (s)",
            "value": 5.583,
            "unit": "seconds"
          },
          {
            "name": "Peak Memory (MB)",
            "value": 317.359375,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 541.64,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 8,
            "unit": "collections"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "e.wang@databricks.com",
            "name": "Eric Wang",
            "username": "eric-wang-1990"
          },
          "committer": {
            "email": "e.wang@databricks.com",
            "name": "Eric Wang",
            "username": "eric-wang-1990"
          },
          "distinct": true,
          "id": "a23274232606f4ef70db6c90abf7bf0f75de38ce",
          "message": "feat(ci): add parallel benchmark suite workflow\n\nAdd new workflow for running multiple benchmark queries in parallel using\nmatrix strategy. Queries are defined in benchmark-queries.json config.\n\nFeatures:\n- Matrix strategy runs queries in parallel (faster than sequential)\n- Weekly schedule (Sunday 2 AM UTC)\n- Manual trigger with query selection\n- Separate GitHub Pages tracking per query (bench/{query_name}/)\n- Summary report showing all query results\n\nIncluded queries:\n- catalog_sales: 1.4M rows, 34 cols (current default)\n- store_sales: 2.8M rows, 23 cols (largest)\n- inventory: 11.7M rows, 5 cols (narrow & huge)\n- web_sales: 720K rows, 34 cols (medium)\n- customer: 100K rows, 18 cols (small)\n\nUsage:\n- Automatic: Runs weekly on schedule\n- Manual: Actions → Benchmark Suite → Run workflow\n  - Select \"all\" or comma-separated query names\n\n🤖 Generated with [Claude Code](https://claude.com/claude-code)\n\nCo-Authored-By: Claude <noreply@anthropic.com>",
          "timestamp": "2025-12-03T10:15:37-08:00",
          "tree_id": "a2f1bdb1f2fdb6a89487cdc85c71832e997ad03e",
          "url": "https://github.com/adbc-drivers/databricks/commit/a23274232606f4ef70db6c90abf7bf0f75de38ce"
        },
        "date": 1764785906766,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Min Execution Time (s)",
            "value": 4.58,
            "unit": "seconds"
          },
          {
            "name": "Peak Memory (MB)",
            "value": 398.8046875,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 541.32,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 6,
            "unit": "collections"
          }
        ]
      }
    ]
  }
}