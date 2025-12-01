window.BENCHMARK_DATA = {
  "lastUpdate": 1764617621738,
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
        "date": 1764572339928,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Peak Memory (MB)",
            "value": 420.984375,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 286.18,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 61,
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
        "date": 1764573107107,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Min Execution Time (s)",
            "value": 4.14,
            "unit": "seconds"
          },
          {
            "name": "Peak Memory (MB)",
            "value": 427.7265625,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 280.07,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 66,
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
        "date": 1764573325569,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Min Execution Time (s)",
            "value": 4.231,
            "unit": "seconds"
          },
          {
            "name": "Peak Memory (MB)",
            "value": 436.41796875,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 284.13,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 60,
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
        "date": 1764574152981,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Min Execution Time (s)",
            "value": 2.452,
            "unit": "seconds"
          },
          {
            "name": "Peak Memory (MB)",
            "value": 461.328125,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 306.92,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 38,
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
        "date": 1764617621003,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Min Execution Time (s)",
            "value": 4.822,
            "unit": "seconds"
          },
          {
            "name": "Peak Memory (MB)",
            "value": 421.390625,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 286.34,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 53,
            "unit": "collections"
          }
        ]
      }
    ]
  }
}