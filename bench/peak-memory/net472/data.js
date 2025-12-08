window.BENCHMARK_DATA = {
  "lastUpdate": 1765218206178,
  "repoUrl": "https://github.com/adbc-drivers/databricks",
  "entries": {
    "Peak Memory (.NET Framework 4.7.2)": [
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
        "date": 1765218205234,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 296.75,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 155.17,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 271.24,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 221.99,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 274.38,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 338.32,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 401.3,
            "unit": "MB"
          }
        ]
      }
    ]
  }
}