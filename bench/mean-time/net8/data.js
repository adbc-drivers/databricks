window.BENCHMARK_DATA = {
  "lastUpdate": 1765219526724,
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
      }
    ]
  }
}