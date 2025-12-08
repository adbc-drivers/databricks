window.BENCHMARK_DATA = {
  "lastUpdate": 1765218200569,
  "repoUrl": "https://github.com/adbc-drivers/databricks",
  "entries": {
    "Allocated Memory (.NET 8.0)": [
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
        "date": 1765218199603,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "catalog_sales",
            "value": 280.6,
            "unit": "MB"
          },
          {
            "name": "customer",
            "value": 37.21,
            "unit": "MB"
          },
          {
            "name": "inventory",
            "value": 214.35,
            "unit": "MB"
          },
          {
            "name": "sales(...)tamps_[21]",
            "value": 104.71,
            "unit": "MB"
          },
          {
            "name": "store_sales_numeric",
            "value": 335.58,
            "unit": "MB"
          },
          {
            "name": "web_sales",
            "value": 172.13,
            "unit": "MB"
          },
          {
            "name": "wide_sales_analysis",
            "value": 1169.74,
            "unit": "MB"
          }
        ]
      }
    ]
  }
}