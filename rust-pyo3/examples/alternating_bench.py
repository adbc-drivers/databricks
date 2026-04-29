#!/usr/bin/env python3
# Copyright (c) 2026 ADBC Drivers Contributors
# Licensed under the Apache License, Version 2.0
"""Alternate Rust and Thrift on the SAME query repeatedly to isolate warehouse variance.

Both connect first, both warmup, then we interleave runs. If the gap is real
(client side), Rust is consistently slower. If it's variance, the gap shifts.
"""
from __future__ import annotations
import os, statistics, time
import databricks_adbc_pyo3 as dbx
from databricks import sql

QUERY_NAME, QUERY = "100K", "SELECT id, id*2 AS doubled FROM range(100000)"
RUNS = 20


def main():
    rust = dbx.Connection(
        os.environ["DATABRICKS_HOST"],
        os.environ["DATABRICKS_HTTP_PATH"],
        os.environ["DATABRICKS_TOKEN"],
    )
    thrift = sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
        use_sea=False,
    )
    tcur = thrift.cursor()

    # warmups (3 each)
    for _ in range(3):
        rust.execute(QUERY).fetch_all_arrow()
        tcur.execute(QUERY)
        tcur.fetchall_arrow()

    print(f"=== alternating {RUNS}x {QUERY_NAME} ===")
    print(f"{'iter':>4}  {'rust ms':>9}  {'thrift ms':>11}  {'rust/thrift':>12}")
    rs, ts = [], []
    for i in range(RUNS):
        t0 = time.perf_counter()
        rust.execute(QUERY).fetch_all_arrow()
        r = (time.perf_counter() - t0) * 1000
        rs.append(r)

        t0 = time.perf_counter()
        tcur.execute(QUERY)
        tcur.fetchall_arrow()
        t = (time.perf_counter() - t0) * 1000
        ts.append(t)
        print(f"  {i:>2}  {r:>9.1f}  {t:>11.1f}  {r/t:>11.2f}x")

    print()
    print(f"  Rust  min={min(rs):.1f}  med={statistics.median(rs):.1f}  p90={sorted(rs)[int(len(rs)*.9)]:.1f}")
    print(f"  Thrift min={min(ts):.1f}  med={statistics.median(ts):.1f}  p90={sorted(ts)[int(len(ts)*.9)]:.1f}")
    print(f"  ratio (med): {statistics.median(rs)/statistics.median(ts):.2f}x")
    tcur.close()
    thrift.close()


main()
