window.BENCHMARK_DATA = {
  "lastUpdate": 1765241743019,
  "repoUrl": "https://github.com/adbc-drivers/databricks",
  "entries": {
    "Peak Memory (.NET 8.0)": [
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
          "id": "582dcdd1a66746ef2e41e1b024a3ba422d4084d9",
          "message": "feat(ci): organize benchmarks by metric for cleaner GitHub Pages\n\nRefactor benchmark publishing to organize by metric type instead of\ncombining everything. This provides a cleaner UX with separate pages\nfor each metric.\n\nNew structure:\n- bench/mean-time/ - All queries' execution times\n- bench/peak-memory/ - All queries' peak memory usage\n- bench/allocated-memory/ - All queries' allocated memory\n- bench/gen2-collections/ - All queries' GC collections\n\nEach page shows 7 queries in the dropdown (vs 28 items before).\nEasier to compare all queries for a specific metric.\n\nImplementation:\n- process-benchmarks.py: Extract metrics from BenchmarkDotNet JSON + CSV\n- organize-by-metric.py: Group queries by metric type\n- 8 benchmark-action calls: 4 metrics × 2 frameworks\n\nPeak Memory data is extracted from CSV since BenchmarkDotNet's custom\ncolumns don't export to JSON.\n\n🤖 Generated with [Claude Code](https://claude.com/claude-code)\n\nCo-Authored-By: Claude <noreply@anthropic.com>",
          "timestamp": "2025-12-08T10:08:38-08:00",
          "tree_id": "f07cf2374acd64db1d012b497b2fa618e17f7789",
          "url": "https://github.com/adbc-drivers/databricks/commit/582dcdd1a66746ef2e41e1b024a3ba422d4084d9"
        },
        "date": 1765218197688,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 424.1,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 325.52,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 487.73,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 397.2,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 412.89,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 415.69,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 511.08,
            "unit": "MB"
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
          "id": "761efc3c00d7592e20e1b5d9e9ab313097c45c7b",
          "message": "feat(ci): add benchmark index page for easy navigation\n\nCreate a landing page at the root of gh-pages with cards linking to\nall benchmark metrics. Provides better UX than remembering 8 URLs.\n\nIndex page features:\n- Visual cards for each metric with icons\n- Links to both .NET 8.0 and .NET Framework 4.7.2 versions\n- List of all benchmark queries with dimensions\n- Usage instructions\n\nEntry URL: https://adbc-drivers.github.io/databricks/\n\n🤖 Generated with [Claude Code](https://claude.com/claude-code)\n\nCo-Authored-By: Claude <noreply@anthropic.com>",
          "timestamp": "2025-12-08T10:31:16-08:00",
          "tree_id": "063baff7b1781a58cca84d6689d1d3e0f9fb5883",
          "url": "https://github.com/adbc-drivers/databricks/commit/761efc3c00d7592e20e1b5d9e9ab313097c45c7b"
        },
        "date": 1765219527460,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 473.48,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 358.27,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 465.28,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 455.59,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 495.8,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 430.04,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 493.99,
            "unit": "MB"
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
          "id": "42afdc11da6b0b9b9bb7f0f764fb82229dc49923",
          "message": "fix(ci): move benchmark index to bench/ subdirectory\n\nThe root URL is used for the repository README. Move the benchmark\nindex page to bench/index.html instead.\n\nEntry URL: https://adbc-drivers.github.io/databricks/bench/\n\n🤖 Generated with [Claude Code](https://claude.com/claude-code)\n\nCo-Authored-By: Claude <noreply@anthropic.com>",
          "timestamp": "2025-12-08T10:34:12-08:00",
          "tree_id": "e2baabd1a3b0abbc9e3ff0cc3af63060e0ba143e",
          "url": "https://github.com/adbc-drivers/databricks/commit/42afdc11da6b0b9b9bb7f0f764fb82229dc49923"
        },
        "date": 1765220409571,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 476.89,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 357.54,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 474.48,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 424.66,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 471.64,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 426.42,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 519.46,
            "unit": "MB"
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
          "id": "517ac8f0e504e5329fcf05a72e7b0ef26cc9c3d7",
          "message": "feat(ci): add GitHub Pages publishing for benchmark trend tracking (#65)\n\n## Summary\n\nAdds automatic GitHub Pages publishing for benchmark results with\nhistorical trend tracking across multiple performance metrics.\n\n## Changes\n\n- Added benchmark processing and publishing to `benchmarks.yml`\n- Created `process-benchmarks.py` to extract metrics from\nBenchmarkDotNet output\n- Created `organize-by-metric.py` to group results by metric type\n- Created `create-index-page.sh` to generate navigation landing page\n- Publishes on push to `main` with 150% alert threshold\n\n## Published Pages\n\n**Entry URL**: https://adbc-drivers.github.io/databricks/bench/\n\n**4 Metrics tracked** (each with .NET 8.0 and .NET 4.7.2 dashboards):\n- ⚡ Mean Execution Time (ms)\n- 💾 Peak Memory (MB) - extracted from CSV\n- 📦 Allocated Memory (MB)\n- 🗑️ Gen2 Collections\n\nEach metric page shows all 7 benchmark queries in a dropdown for easy\ncomparison.\n\n## Features\n\n- Separate pages per metric type for cleaner visualization\n- Automatic inclusion of new queries from `benchmark-queries.json`\n- Interactive charts with historical trends\n- Regression alerts when metrics increase >150%\n\n🤖 Generated with [Claude Code](https://claude.com/claude-code)\n\n---------\n\nCo-authored-by: Claude <noreply@anthropic.com>",
          "timestamp": "2025-12-08T13:14:06-08:00",
          "tree_id": "c3985efcda588f0de6023db6a9ac554f1329410e",
          "url": "https://github.com/adbc-drivers/databricks/commit/517ac8f0e504e5329fcf05a72e7b0ef26cc9c3d7"
        },
        "date": 1765229218903,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 450.18,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 370.88,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 464.52,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 441.25,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 428.59,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 390.4,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 507.35,
            "unit": "MB"
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
          "id": "97015f6f6a4c70c2bc812aff52b84ef91902e5b6",
          "message": "feat(csharp): add comprehensive connection logging to Databricks driver (#36)\n\nPort connection logging improvements from apache/arrow-adbc PR #3577 to\nenhance observability and debugging during connection establishment.\n\nChanges:\n- Add LogConnectionProperties method with sensitive value sanitization\n- Enhanced CreateSessionRequest with detailed driver and configuration\nlogging\n- Comprehensive HandleOpenSessionResponse logging including:\n  - Server protocol version and capabilities\n  - Feature support detection (PK/FK, DESC table extended)\n  - Feature downgrade notifications when protocol constraints apply\n  - Final feature flag states for debugging\n  - Namespace handling with detailed event logging\n  - Error conditions with appropriate error tags\n\nThe logging follows commercial ODBC driver patterns (like Simba)\nproviding comprehensive visibility into the Databricks connection\nestablishment process. All sensitive credentials are properly sanitized\nbefore logging.\n\n🤖 Generated with [Claude Code](https://claude.ai/code)\n\n## What's Changed\n\nPlease fill in a description of the changes here.\n\n**This contains breaking changes.** <!-- Remove this line if there are\nno breaking changes. -->\n\nCloses #NNN.\n\n---------\n\nCo-authored-by: Claude <noreply@anthropic.com>",
          "timestamp": "2025-12-08T15:35:11-08:00",
          "tree_id": "5e3bc59d0271f17b0fcfeb282724b070d2954827",
          "url": "https://github.com/adbc-drivers/databricks/commit/97015f6f6a4c70c2bc812aff52b84ef91902e5b6"
        },
        "date": 1765237703527,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 437.9,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 332,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 498.66,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 402.14,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 442.39,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 456.9,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 508.7,
            "unit": "MB"
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
          "id": "0d495af807c85ba33feb0bc2ccd42d73aa79f1a4",
          "message": "fix(ci): improve benchmark results display and documentation (#66)\n\n## Summary\n\nThis PR fixes benchmark results display issues and adds comprehensive\ndocumentation for the benchmark suite and GitHub Pages dashboard.\n\n### Changes\n\n#### Benchmark Workflow Fixes\n- **Fix parameter extraction**: Handle both string and object formats\nfrom BenchmarkDotNet\n  - String format: `\"benchmarkQuery=catalog_sales&ReadDelayMs=5\"`\n- Object format: `{\"benchmarkQuery\": {\"name\": \"catalog_sales\", ...},\n\"ReadDelayMs\": 5}`\n- **Fix baseline matching**: Ensure baseline comparisons work with both\nparameter formats\n- Resolves missing rows/columns data and baseline values in benchmark\nresults\n\n#### Documentation Updates\n- **benchmark-queries.md**: Add comprehensive \"Viewing Results\" section\n  - GitHub Pages dashboard overview with URL\n- Describe all 4 key metrics (Mean Execution Time, Peak Memory,\nAllocated Memory, Gen2 Collections)\n  - Explain console output vs historical trend visualization\n  \n- **Main README.md**: Modernize Benchmarking section\n  - Reference 7-query benchmark suite\n  - Add GitHub Pages dashboard link\n  - Mention PR label-based benchmarking\n  - Reference detailed documentation files\n  \n- **csharp/Benchmarks/README.md**: Update placeholder URLs\n  - Replace generic `<org>/<repo>` URLs with actual GitHub Pages URL\n  - https://adbc-drivers.github.io/databricks/bench/\n\n### Testing\n- [x] Benchmark workflow parameter extraction tested with actual\nBenchmarkDotNet output\n- [x] Documentation reviewed for accuracy\n\n### Related Issues\nFixes benchmark results display showing missing baseline and\nrows/columns information.\n\n🤖 Generated with [Claude Code](https://claude.com/claude-code)",
          "timestamp": "2025-12-08T15:43:07-08:00",
          "tree_id": "3bc51a82d97ad6ba23a97c92690027c26efb1a46",
          "url": "https://github.com/adbc-drivers/databricks/commit/0d495af807c85ba33feb0bc2ccd42d73aa79f1a4"
        },
        "date": 1765238488108,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 445.81,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 358.19,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 479.93,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 480.04,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 531.6,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 458.75,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 519.98,
            "unit": "MB"
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
          "id": "ebb795dd43b7a194d5bf04bcfb6f0f54c6e6984c",
          "message": "fix(ci): add actions:read permission for baseline artifact downloads\n\n- Fixes HTTP 403 error when downloading artifacts from main branch runs\n- Required for PR comparison to access baseline benchmark results",
          "timestamp": "2025-12-08T16:29:14-08:00",
          "tree_id": "23a9cd3ea2506c1c8a980ce59df3ce0ec981074c",
          "url": "https://github.com/adbc-drivers/databricks/commit/ebb795dd43b7a194d5bf04bcfb6f0f54c6e6984c"
        },
        "date": 1765240955611,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 457.38,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 362.31,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 485.52,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 438.8,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 464.88,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 412.25,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 505.65,
            "unit": "MB"
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
          "id": "b92a91609d6ddfc2f1581a5a233c04be398d2d32",
          "message": "feat(csharp): add CloudFetch support for Statement Execution API (#64)\n\n## 🥞 Stacked PR\nUse this\n[link](https://github.com/adbc-drivers/databricks/pull/64/files) to\nreview incremental changes.\n-\n[**stack/PECO-2790-result-fetcher**](https://github.com/adbc-drivers/databricks/pull/64)\n[[Files\nchanged](https://github.com/adbc-drivers/databricks/pull/64/files)]\n\n---------\n## Summary\n\nAdds CloudFetch support for the Statement Execution REST API, enabling\nhigh-performance result\n  retrieval via pre-signed cloud storage URLs.\n\n  ### Changes\n\n- **StatementExecutionResultFetcher**: New CloudFetch result fetcher\nthat supports:\n    - Manifest-based fetching (all external links available upfront)\n- Incremental chunk fetching (links fetched per-chunk via\n`GetResultChunkAsync`)\n    - URL refresh for expired pre-signed URLs\n- **CloudFetchReaderFactory**: Extended with\n`CreateStatementExecutionReader()` factory method and\nadapter classes (`StatementExecutionStatementAdapter`,\n`StatementExecutionResponseAdapter`)\n- **StatementExecutionStatement**: Added `CreateCloudFetchReader()` for\nexternal link results with\n  Arrow schema extraction from manifest\n- **Tracing**: Added detailed activity events throughout the CloudFetch\npipeline for debugging\n- **Bug fix**: Removed `HttpClient.Timeout` assignment in\n`CloudFetchDownloadManager` that could\n  throw after requests started\n\n  ## Test Plan\n\n  - [x] Unit tests: `StatementExecutionResultFetcherTests` (14 tests)\n  - [x] E2E tests: `StatementExecutionResultFetcherTest` (10 tests)\n- [x] Extended `CloudFetchE2ETest` with Statement Execution API\nscenarios\n\nCo-authored-by: Jade Wang <jade.wang+data@databricks.com>\nCo-authored-by: Claude <noreply@anthropic.com>",
          "timestamp": "2025-12-08T16:34:17-08:00",
          "tree_id": "506b2057697095e6117a05d968826da64fad049f",
          "url": "https://github.com/adbc-drivers/databricks/commit/b92a91609d6ddfc2f1581a5a233c04be398d2d32"
        },
        "date": 1765241742708,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 461.49,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 362.14,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 490.67,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 425.76,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 444.94,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 447.47,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 495.04,
            "unit": "MB"
          }
        ]
      }
    ]
  }
}