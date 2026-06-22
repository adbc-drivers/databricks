#!/usr/bin/env python3
# Copyright (c) 2026 ADBC Drivers Contributors
# Licensed under the Apache License, Version 2.0
"""Stage timings on the CloudFetch path to find where the 1M-row gap is.

For 100K, 1M, 10M rows:
  T_exec   : conn.execute(sql) — POST + completion (no chunks fetched yet)
  T_first  : first fetch_next_batch — should fire as soon as batch 0 is decoded
  T_drain  : remaining batches → finished
  T_total  : sum
  Plus: number of batches, total rows.

Comparison: same query through Thrift backend's cursor, with timings around
.execute, first .fetchmany_arrow(1) call, and .fetchall_arrow.
"""
from __future__ import annotations

import os
import statistics
import time

import databricks_adbc_pyo3 as dbx
from databricks import sql

SIZES = [
    ("100K", "SELECT id, id * 2 AS doubled FROM range(100000)"),
    ("1M", "SELECT id, id * 2 AS doubled FROM range(1000000)"),
    ("10M", "SELECT id, id * 2 AS doubled FROM range(10000000)"),
]
RUNS = 3


def bench_pyo3():
    print("=== ADBC-Rust (PyO3 direct) ===")
    conn = dbx.Connection(
        os.environ["DATABRICKS_HOST"],
        os.environ["DATABRICKS_HTTP_PATH"],
        os.environ["DATABRICKS_TOKEN"],
    )
    # warmup
    conn.execute("SELECT id FROM range(100000)").fetch_all_arrow()

    for name, query in SIZES:
        runs = []
        for _ in range(RUNS):
            t0 = time.perf_counter()
            rs = conn.execute(query)
            t_exec = time.perf_counter() - t0

            t0 = time.perf_counter()
            first = rs.fetch_next_batch()
            t_first = time.perf_counter() - t0
            first_rows = first.num_rows if first is not None else 0

            t0 = time.perf_counter()
            n_batches = 1 if first is not None else 0
            total_rows = first_rows
            while True:
                b = rs.fetch_next_batch()
                if b is None:
                    break
                n_batches += 1
                total_rows += b.num_rows
            t_drain = time.perf_counter() - t0
            runs.append((t_exec, t_first, t_drain, n_batches, total_rows))

        # Pick the fastest run as representative
        best = min(runs, key=lambda r: r[0] + r[1] + r[2])
        t_exec, t_first, t_drain, n_batches, total_rows = best
        total = t_exec + t_first + t_drain
        print(
            f"  {name:>4}  exec={t_exec * 1000:6.0f} ms  "
            f"first_batch={t_first * 1000:6.0f} ms  "
            f"drain={t_drain * 1000:6.0f} ms  "
            f"total={total:6.2f}s  "
            f"batches={n_batches:>3}  rows={total_rows:>11,}"
        )


def bench_thrift():
    print("\n=== Thrift ===")
    conn = sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
        use_sea=False,
    )
    cur = conn.cursor()
    cur.execute("SELECT id FROM range(100000)")
    cur.fetchall_arrow()

    for name, query in SIZES:
        runs = []
        for _ in range(RUNS):
            t0 = time.perf_counter()
            cur.execute(query)
            t_exec = time.perf_counter() - t0

            t0 = time.perf_counter()
            tab = cur.fetchall_arrow()
            t_drain = time.perf_counter() - t0
            runs.append((t_exec, t_drain, tab.num_rows))

        best = min(runs, key=lambda r: r[0] + r[1])
        t_exec, t_drain, total_rows = best
        total = t_exec + t_drain
        print(
            f"  {name:>4}  exec={t_exec * 1000:6.0f} ms  "
            f"fetchall_arrow={t_drain * 1000:6.0f} ms  "
            f"total={total:6.2f}s  rows={total_rows:>11,}"
        )

    cur.close()
    conn.close()


def main():
    bench_pyo3()
    bench_thrift()


if __name__ == "__main__":
    main()
