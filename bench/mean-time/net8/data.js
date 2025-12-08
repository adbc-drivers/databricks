window.BENCHMARK_DATA = {
  "lastUpdate": 1765229218052,
  "repoUrl": "https://github.com/adbc-drivers/databricks",
  "entries": {
    "Mean Execution Time (.NET 8.0)": [
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
        "date": 1765218195410,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 5047,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 1852.96,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 7671.27,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 5256.28,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 5608.63,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 3145.01,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 16789.78,
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
        "date": 1765219526066,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 2566.04,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 662.74,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 6910.39,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 4037.11,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 3133.36,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 1771.57,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 12295.86,
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
        "date": 1765220407316,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 4697.28,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 1604.3,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 7570.06,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 5328.2,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 5722.17,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 3003.85,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 14411.88,
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
        "date": 1765229217150,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 3276.86,
            "unit": "ms"
          },
          {
            "name": "customer",
            "value": 1152.87,
            "unit": "ms"
          },
          {
            "name": "inventory",
            "value": 7101.56,
            "unit": "ms"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 6391.71,
            "unit": "ms"
          },
          {
            "name": "store_sales_numeric",
            "value": 4184.54,
            "unit": "ms"
          },
          {
            "name": "web_sales",
            "value": 2450.17,
            "unit": "ms"
          },
          {
            "name": "wide_sales_analysis",
            "value": 13555.87,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}