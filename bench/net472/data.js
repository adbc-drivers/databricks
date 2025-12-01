window.BENCHMARK_DATA = {
  "lastUpdate": 1764572408916,
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
        "date": 1764572403639,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Peak Memory (MB)",
            "value": 346.19140625,
            "unit": "MB"
          },
          {
            "name": "Allocated Memory (MB)",
            "value": 539.65,
            "unit": "MB"
          },
          {
            "name": "Gen2 Collections",
            "value": 7,
            "unit": "collections"
          }
        ]
      }
    ]
  }
}