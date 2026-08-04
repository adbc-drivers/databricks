#!/usr/bin/env python3
# Copyright (c) 2026 ADBC Drivers Contributors
# Licensed under the Apache License, Version 2.0
"""POC end-to-end: small inline result + large CloudFetch result.

Reads DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN from env.
"""
from __future__ import annotations

import os
import sys
import time

import databricks_adbc_pyo3 as dbx


def main() -> int:
    host = os.environ["DATABRICKS_HOST"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]

    conn = dbx.Connection(host, http_path, token)
    print(f"connected: {host}{http_path}")

    # 1) Small inline result
    t0 = time.perf_counter()
    rs = conn.execute("SELECT 1 AS one, 'hello' AS greeting")
    table = rs.fetch_all_arrow()
    print(
        f"[small] {table.num_rows} row x {table.num_columns} cols "
        f"({time.perf_counter() - t0:.2f}s) -> {table.to_pydict()}"
    )
    assert table.num_rows == 1
    assert table.column_names == ["one", "greeting"]
    assert table.column("one").to_pylist() == [1]
    assert table.column("greeting").to_pylist() == ["hello"]

    # 2) Large CloudFetch result
    n = 1_000_000
    sql = f"SELECT id, id * 2 AS doubled FROM range({n})"
    t0 = time.perf_counter()
    rs = conn.execute(sql)
    nrows_pre = rs.num_rows()
    table = rs.fetch_all_arrow()
    elapsed = time.perf_counter() - t0
    print(
        f"[large] {table.num_rows:,} rows x {table.num_columns} cols "
        f"({elapsed:.2f}s, {table.num_rows / elapsed / 1e6:.2f}M rows/s)"
    )
    assert table.num_rows == n
    assert nrows_pre == n
    # spot-check first/last
    first = table.slice(0, 1).to_pydict()
    last = table.slice(table.num_rows - 1, 1).to_pydict()
    print(f"  first row: {first}")
    print(f"  last row:  {last}")

    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
