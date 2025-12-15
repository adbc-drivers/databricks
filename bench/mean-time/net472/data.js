window.BENCHMARK_DATA = {
  "lastUpdate": 1765818170466,
  "repoUrl": "https://github.com/adbc-drivers/databricks",
  "entries": {
    "Mean Execution Time (.NET Framework 4.7.2)": [
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
        "date": 1765218203309,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 6082.59,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 2048.75,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 46237.39,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 15194.53,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 13821.38,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 4334.07,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 23006.88,
            "unit": "ms"
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
        "date": 1765219530613,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 6081.27,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 1896.43,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 46193.76,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 15090.88,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 14104.43,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 4858.36,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 18984.44,
            "unit": "ms"
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
        "date": 1765220414670,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 6281.58,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 1845.15,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 46310.77,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 14859.53,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 14134.86,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 4599.72,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 20325.98,
            "unit": "ms"
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
        "date": 1765229222702,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 5445.68,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 732.25,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 45221.9,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 14083.26,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 12681.36,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 3305.83,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 15412.82,
            "unit": "ms"
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
        "date": 1765237708918,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 4786.49,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 989.02,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 45353.38,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 13767.76,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 12863.47,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 3588.84,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 15185.09,
            "unit": "ms"
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
        "date": 1765238491193,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 4367.99,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 662.53,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 45110.74,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 13775.33,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 12617.78,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 3218.84,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 14641.47,
            "unit": "ms"
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
        "date": 1765240959027,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 4670.69,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 932.65,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 45412.11,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 14077.1,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 12933.96,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 3600.44,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 25811.68,
            "unit": "ms"
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
        "date": 1765241745460,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 4543.74,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 901.51,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 45281.95,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 14337.8,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 12912.05,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 3504.91,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 15288.55,
            "unit": "ms"
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
        "date": 1765243957334,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 4297.1,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 709.68,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 45083.31,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 14442.25,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 12599.79,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 3326.45,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 14766.16,
            "unit": "ms"
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
        "date": 1765353531378,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 4604.4,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 933.85,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 45267.45,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 14689.94,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 12854.87,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 3490.73,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 15257.14,
            "unit": "ms"
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
        "date": 1765818169919,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 4594.24,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 883.46,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 45216.86,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 30001.07,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 12990.77,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 3384.47,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 15853.47,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}