window.BENCHMARK_DATA = {
  "lastUpdate": 1765220415539,
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
      }
    ]
  }
}