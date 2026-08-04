#!/usr/bin/env python3
# Copyright (c) 2026 ADBC Drivers Contributors
# Licensed under the Apache License, Version 2.0
"""End-to-end via the public databricks.sql API with use_sea=True.

Routes through AdbcDatabricksClient → databricks_adbc_pyo3 → Rust kernel.
"""
from __future__ import annotations

import os
import sys
import time

from databricks import sql


def main() -> int:
    host = os.environ["DATABRICKS_HOST"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]

    with sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
        use_sea=True,
    ) as conn:
        with conn.cursor() as cur:
            # 1) Small inline result, fetched as rows (DB-API path)
            t0 = time.perf_counter()
            cur.execute("SELECT 1 AS one, 'hello' AS greeting")
            desc = cur.description
            row = cur.fetchone()
            print(
                f"[small/rows] description={desc} row={row} "
                f"({time.perf_counter() - t0:.2f}s)"
            )
            assert row[0] == 1 and row[1] == "hello"
            assert [c[0] for c in desc] == ["one", "greeting"]

            # 2) Small inline result, fetchall_arrow path
            cur.execute("SELECT 1 AS one, 2 AS two, 3 AS three")
            arrow_table = cur.fetchall_arrow()
            print(f"[small/arrow] {arrow_table.to_pydict()}")
            assert arrow_table.num_rows == 1
            assert arrow_table.column_names == ["one", "two", "three"]

            # 3) Large CloudFetch result via fetchall_arrow
            n = 1_000_000
            t0 = time.perf_counter()
            cur.execute(f"SELECT id, id * 2 AS doubled FROM range({n})")
            arrow_table = cur.fetchall_arrow()
            elapsed = time.perf_counter() - t0
            print(
                f"[large/arrow] {arrow_table.num_rows:,} rows x "
                f"{arrow_table.num_columns} cols ({elapsed:.2f}s, "
                f"{arrow_table.num_rows / elapsed / 1e6:.2f}M rows/s)"
            )
            assert arrow_table.num_rows == n

            # 4) fetchmany on a small result
            cur.execute("SELECT * FROM range(10)")
            first = cur.fetchmany(3)
            rest = cur.fetchall()
            print(f"[fetchmany] first={first} rest_len={len(rest)}")
            assert len(first) == 3
            assert len(rest) == 7

    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
