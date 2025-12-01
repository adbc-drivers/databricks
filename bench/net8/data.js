window.BENCHMARK_DATA = {
  "lastUpdate": 1764573107844,
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
      }
    ]
  }
}