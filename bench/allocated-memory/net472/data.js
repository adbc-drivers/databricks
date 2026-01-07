window.BENCHMARK_DATA = {
  "lastUpdate": 1767745131755,
  "repoUrl": "https://github.com/adbc-drivers/databricks",
  "entries": {
    "Allocated Memory (.NET Framework 4.7.2)": [
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
        "date": 1765218207110,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 540.66,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 38.79,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 230.36,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 146.31,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 396.21,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 394.66,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2902.47,
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
        "date": 1765219532701,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 534.84,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 38.79,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 230.32,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 146.18,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 400.18,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 394.6,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2911.02,
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
        "date": 1765220418119,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 540.64,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 38.82,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 226.54,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 146.25,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 400.47,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 394.66,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2913.19,
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
        "date": 1765229225125,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 541.46,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 38.8,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 231.48,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 146.98,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 400.56,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 393.52,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2916.59,
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
        "date": 1765237712704,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 540.55,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 38.82,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 231.13,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 142.75,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 400.55,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 394.91,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2913.44,
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
        "date": 1765238493247,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 541.05,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 42.79,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 227.22,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 146.9,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 400.58,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 394.8,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2922.45,
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
        "date": 1765240961315,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 541.14,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 38.82,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 227.72,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 146.94,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 398.96,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 394.57,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2917.76,
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
        "date": 1765241747328,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 540.79,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 42.83,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 231.21,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 146.39,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 398.93,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 394.62,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2917.21,
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
          "id": "597b78ad90894a6f9f4df912b56884ac7632098d",
          "message": "fix(ci): fix jq syntax error in benchmark comparison\n\n- Wrap conditional expression in parentheses for proper 'as' binding\n- Fixes 'unexpected as' syntax error in GitHub Actions",
          "timestamp": "2025-12-08T17:19:01-08:00",
          "tree_id": "efc7aba3fb0b29596ee06672dfd647624dd35f5c",
          "url": "https://github.com/adbc-drivers/databricks/commit/597b78ad90894a6f9f4df912b56884ac7632098d"
        },
        "date": 1765243959559,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 540.06,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 42.8,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 231.06,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 146.44,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 398.78,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 394.56,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2918.75,
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
          "id": "77c5d1da686d4e9e75b962e351d2d0ec5ded25e5",
          "message": "fix(csharp): implement FIFO memory acquisition to prevent starvation in CloudFetch (#56)\n\nThis is cherry pick of this PR:\nhttps://github.com/apache/arrow-adbc/pull/3756\n\nFixes memory starvation issue in CloudFetch download pipeline where\nnewer downloads could win memory allocation over older waiting\ndownloads, causing indefinite delays.\n\n## Problem\nThe CloudFetch downloader was acquiring memory inside parallel download\ntasks with a polling-based wait (10ms intervals). This non-FIFO behavior\ncaused memory starvation:\n\n1. Multiple download tasks acquire semaphore slots and start polling for\nmemory\n2. All tasks wake up simultaneously every 10ms to check memory\navailability\n3. When memory becomes available, any waiting task could acquire it\n(non-deterministic)\n4. Newer downloads (e.g., file 5, 7, 8) could repeatedly win the race\nover older downloads (e.g., file 4)\n5. File 4 never gets memory despite waiting the longest → **indefinite\nstarvation**\n\nExample scenario with 200MB memory, 50MB files, 3 parallel downloads:\n- Files 1, 2, 3 download (150MB used)\n- File 1 completes and releases 50MB\n- Files 4, 5, 6 are all waiting for memory\n- File 5 wins the race and acquires the 50MB\n- File 2 completes, file 7 wins the race\n- File 3 completes, file 8 wins the race\n- File 4 never gets memory because files 5, 7, 8 keep winning\n\n## Solution\nMove memory acquisition from inside parallel download tasks to the main\nsequential loop. This ensures FIFO ordering:\n\n1. Main loop acquires memory sequentially for each download\n2. Only after memory is acquired does the download task start\n3. Downloads are guaranteed to get memory in the order they were queued\n4. No starvation possible\n\nAdditionally, increased default memory buffer from 100MB to 200MB to\nallow more parallel downloads without hitting memory limits.\n\n## Changes\n- **CloudFetchDownloader.cs**: \n- Moved `AcquireMemoryAsync` from inside `DownloadFileAsync` to main\nloop (line 278-280)\n  - Ensures FIFO ordering before spawning download tasks\n- **CloudFetchDownloadManager.cs**: \n  - Increased `DefaultMemoryBufferSizeMB` from 100 to 200\n  - Better performance for large result sets\n\n## Testing\n- Manually verified on fast VM with Power BI Desktop\n- Existing CloudFetchDownloader tests still pass\n- FIFO ordering prevents the starvation scenario\n\n## TODO\nThe memory buffer design have a flaw that it only captures the\ncompressed size, but does not cover decompressed size. There are a\ncouple of options we can take but I wanna land this first since this one\nactually can get blocked for customer.\n\n---------\n\nCo-authored-by: Claude <noreply@anthropic.com>",
          "timestamp": "2025-12-09T23:46:03-08:00",
          "tree_id": "47fd53c34e9ec9db672ad2f19597faa8b2a55475",
          "url": "https://github.com/adbc-drivers/databricks/commit/77c5d1da686d4e9e75b962e351d2d0ec5ded25e5"
        },
        "date": 1765353534848,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 529.37,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 38.83,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 225.7,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 146.92,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 398.59,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 396.2,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2919.08,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "13318837+davidhcoe@users.noreply.github.com",
            "name": "davidhcoe",
            "username": "davidhcoe"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "9066f08616e269b425cf4f7d458bac3556fe5d02",
          "message": "chore(csharp): aligning namespaces for new repo (#68)\n\n## What's Changed\n\n- Changes the namespaces to match the new repo \n- Restructures the solution\n- Minor code formatting\n\nThis will need https://github.com/apache/arrow-adbc/pull/3788/files to\nland.\n\n---------\n\nCo-authored-by: David Coe <>",
          "timestamp": "2025-12-15T09:48:43-07:00",
          "tree_id": "dcbfa3141b9d4ef606230fd978edb91e64838ca7",
          "url": "https://github.com/adbc-drivers/databricks/commit/9066f08616e269b425cf4f7d458bac3556fe5d02"
        },
        "date": 1765818172675,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 537.9,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 42.82,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 227.65,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 143.96,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 396.19,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 402.07,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2918.45,
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
          "id": "fd648ef183200e1453bd8706f81226e72228edc0",
          "message": "fix(csharp): improve server-side property name validation (#72)\n\n## Summary\n\nImproves server-side property name validation to support standard Spark\nconfiguration naming conventions while maintaining security against SQL\ninjection attacks.\n\n### Changes\n\n- **Updated validation regex**: Changed from `^[a-zA-Z_]+$` to\n`^[a-zA-Z0-9_.]+$`\n- Now supports dots (e.g., `spark.databricks.sql.metricViewV2.enabled `)\n- Now supports numbers (e.g., `spark.databricks.sql.metricViewV2.enabled\n`)\n  - Maintains security by blocking special characters\n\n- **Refactored validation logic**:\n- Moved validation into `GetServerSideProperties()` for centralized\nfiltering\n- Changed `IsValidPropertyName` from `private` to `internal` for\ntestability\n- Removed duplicate validation code from\n`ApplyServerSidePropertiesAsync`\n\n- **Enhanced observability**:\n- Added Activity tracing with `connection.server_side_property.filtered`\nevent\n  - Added Activity tracing for SET query failures\n  - Wrapped `ApplyServerSidePropertiesAsync` in `TraceActivityAsync`\n\n- **Added comprehensive tests**:\n  - Created `DatabricksConnectionUnitTests.cs` with 41 test cases\n  - Tests cover valid patterns, invalid characters, and edge cases\n  - All tests passing ✅\n\n### Test Plan\n\n```bash\ndotnet test --filter \"FullyQualifiedName~DatabricksConnectionUnitTests\"\n```\n\n**Result**: All 41 tests passed\n\n### Examples of Now-Supported Properties\n\n- ✅ `spark.sql.adaptive.enabled`\n- ✅ `spark.executor.instances`\n- ✅ `spark.databricks.delta.optimizeWrite.enabled`\n- ✅ `my_custom_property123`\n\n### Security\n\nStill blocks potentially dangerous input:\n- ❌ Hyphens, spaces, semicolons\n- ❌ Quotes, equals signs\n- ❌ Special characters that could enable SQL injection\n\n🤖 Generated with [Claude Code](https://claude.com/claude-code)\n\nCo-authored-by: Claude Sonnet 4.5 <noreply@anthropic.com>",
          "timestamp": "2025-12-15T12:07:39-08:00",
          "tree_id": "4dc88c9361d377f6865747810efa7092876f9861",
          "url": "https://github.com/adbc-drivers/databricks/commit/fd648ef183200e1453bd8706f81226e72228edc0"
        },
        "date": 1765830034725,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 538.45,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 38.81,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 227.27,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 147.39,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 396.45,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 391.93,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2915.79,
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
          "id": "4ba90df43253c7e6cdf387133e69e81423f3f5b2",
          "message": "feat(ci): add memory and GC metrics to benchmark comparison (#69)\n\n## Summary\n- Enhance benchmark comparison to show memory allocation and GC\ncollection metrics\n- Add comprehensive metric comparison in PR comments\n\n## Changes\n1. **Median Baseline Calculation**\n- Calculate median values for execution time, memory, and GC collections\nfrom last 10 runs\n   - More stable baseline that reduces noise from outliers\n\n2. **Enhanced Comparison Table**\n   - Add Memory (MB) column with baseline and percentage diff\n   - Add Gen0, Gen1, Gen2 columns showing GC collection counts\n   - Color-coded indicators for both time and memory changes\n\n3. **Improved Documentation**\n   - Update footer to explain all metrics\n   - Clarify baseline methodology\n\n## Test Plan\n- [x] Workflow syntax validated\n- [ ] Wait for benchmark workflow to run and verify output format\n- [ ] Confirm memory and GC metrics appear in PR comment\n\n🤖 Generated with [Claude Code](https://claude.com/claude-code)\n\n---------\n\nCo-authored-by: Claude Sonnet 4.5 <noreply@anthropic.com>",
          "timestamp": "2025-12-16T23:44:40-08:00",
          "tree_id": "f3d6385670749d74fd5f72996ffed475f735613c",
          "url": "https://github.com/adbc-drivers/databricks/commit/4ba90df43253c7e6cdf387133e69e81423f3f5b2"
        },
        "date": 1765958342717,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 537.39,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 38.81,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 221.73,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 146.14,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 399.87,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 401.57,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2905.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "petridish@gmail.com",
            "name": "Bryce Mecum",
            "username": "amoeba"
          },
          "committer": {
            "email": "petridish@gmail.com",
            "name": "Bryce Mecum",
            "username": "amoeba"
          },
          "distinct": true,
          "id": "e6224770def90297e4f7d375b646a4761aa0e4a5",
          "message": "chore(go): migrate go driver from ADBC Repo\n\nMigrates the Go Databricks driver from the ADBC repo to the `./go` directory in this repo. This PR includes changes to port the driver to the new driverbase and make it build and test cleanly. There was only one commit to migrate and I did that by running git filter-repo with `--path-rename`:\n\n```sh\ngit filter-repo --force --path go/adbc/driver/databricks --path go/adbc/pkg/databricks/ --path-rename go/adbc/driver/databricks:go --path-rename go/adbc/pkg/databricks:go/pkg\n```\n\nand then cherry-picking the commit on this brach. This gives us a new hash but preserves the original commit including date.\n\nThis PR also makes a number of changes to the existing C# driver, mostly to CI files, to make this all work together better.\n\nCloses #31",
          "timestamp": "2025-12-21T16:10:09-08:00",
          "tree_id": "96f7d4aacf938dd537a3204a97315ff54bbe6c16",
          "url": "https://github.com/adbc-drivers/databricks/commit/e6224770def90297e4f7d375b646a4761aa0e4a5"
        },
        "date": 1766362992079,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 537.3,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 38.8,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 225.63,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 146.62,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 400.29,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 402.67,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2918.12,
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
          "id": "f0afca812c6bc1b54239f8988311ebe963f8dc9f",
          "message": "fix(csharp): apply CloudFetch timeout and remove redundant HttpClient (#112)\n\n## What's Changed\n\nThis PR addresses two issues in the CloudFetch implementation and one\nfix in the proxy test infrastructure:\n\n### 1. Apply CloudFetch Timeout Configuration\n\n**Problem**: The `adbc.databricks.cloudfetch.timeout_minutes`\nconfiguration parameter was being read but never applied to the\nHttpClient. The HttpClient used its default 100-second timeout instead\nof the configured value (default: 5 minutes).\n\n**Solution**: Added timeout configuration in `CloudFetchDownloader`\nconstructor:\n```csharp\n_httpClient.Timeout = TimeSpan.FromMinutes(config.TimeoutMinutes);\n```\n\n**Impact**: The CloudFetch HTTP client now correctly respects the\nuser-configured timeout value, allowing for proper timeout behavior in\nCloudFetch downloads.\n\n**Files Changed**:\n- `csharp/src/Reader/CloudFetch/CloudFetchDownloader.cs`\n\n### 2. Remove Redundant HttpClient from CloudFetchDownloadManager\n\n**Problem**: `CloudFetchDownloadManager` accepted an `HttpClient`\nparameter but never used it for any operations. It only disposed of it,\nwhich is incorrect since it doesn't own the resource. The actual HTTP\noperations are performed by `CloudFetchDownloader`.\n\n**Solution**: Removed the redundant `httpClient` parameter from:\n- `CloudFetchDownloadManager` constructor and field\n- `CloudFetchReaderFactory.CreateThriftReader()` call\n- `CloudFetchReaderFactory.CreateStatementExecutionReader()` call\n\n**Impact**: Cleaner code architecture with proper resource ownership.\nThe HttpClient is now owned solely by CloudFetchDownloader, which\nactually uses it for HTTP operations.\n\n**Files Changed**:\n- `csharp/src/Reader/CloudFetch/CloudFetchDownloadManager.cs`\n- `csharp/src/Reader/CloudFetch/CloudFetchReaderFactory.cs`\n\n### 3. Fix Proxy Timing for Test Reliability\n\n**Problem**: The mitmproxy addon was disabling failure scenarios AFTER\nthe delay completed, causing race conditions. When the client timed out\nand retried, the scenario was still enabled, triggering the delay again.\n\n**Solution**: Modified `mitmproxy_addon.py` to disable the scenario\nimmediately when triggered, before the delay starts:\n```python\nself._disable_scenario(scenario_name)  # Disable BEFORE delay\nawait asyncio.sleep(duration_seconds)\n```\n\n**Impact**: CloudFetch timeout tests now behave correctly - only the\nfirst request is delayed, and retry attempts succeed immediately.\n\n**Files Changed**:\n- `test-infrastructure/proxy-server/mitmproxy_addon.py`\n\n## Testing\n\nAll 5 CloudFetch proxy tests now pass:\n- ✅ `CloudFetchConnectionReset_RetriesWithExponentialBackoff`\n- ✅ `CloudFetchExpiredLink_RefreshesLinkViaFetchResults`\n- ✅ `NormalCloudFetch_SucceedsWithoutFailureScenarios`\n- ✅ `CloudFetchTimeout_RetriesWithExponentialBackoff`\n- ✅ `CloudFetch403_RefreshesLinkViaFetchResults`\n\nCo-authored-by: Claude Sonnet 4.5 <noreply@anthropic.com>",
          "timestamp": "2026-01-06T10:37:01-08:00",
          "tree_id": "43ec7fa0f5f9d0b0d4b68b27f54f661db3c00dd7",
          "url": "https://github.com/adbc-drivers/databricks/commit/f0afca812c6bc1b54239f8988311ebe963f8dc9f"
        },
        "date": 1767725448383,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 536.94,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 38.8,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 223.61,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 146.84,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 400.89,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 401.89,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2920.56,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "13318837+davidhcoe@users.noreply.github.com",
            "name": "davidhcoe",
            "username": "davidhcoe"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0bff54c1e37649d81d61cd442a3fbbaa73714078",
          "message": "feat(csharp): align project structures and output locations to other drivers (#115)\n\n## What's Changed\n\n- aligns the project structure and output location (artifacts) with the\nother ADBC drivers\n- added Directory.Build.props file\n\nCo-authored-by: David Coe <>",
          "timestamp": "2026-01-06T19:05:54-05:00",
          "tree_id": "f396118726ef8d5e2d266b58b5d9e7c9ea0f0332",
          "url": "https://github.com/adbc-drivers/databricks/commit/0bff54c1e37649d81d61cd442a3fbbaa73714078"
        },
        "date": 1767745131133,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 536.9,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 38.98,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 223.43,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 147.31,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 400.92,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 401.83,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 2923.62,
            "unit": "MB"
          }
        ]
      }
    ]
  }
}