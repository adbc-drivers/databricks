#!/usr/bin/env python3
# Copyright (c) 2026 ADBC Drivers Contributors
# Licensed under the Apache License, Version 2.0
"""Profile 10K-100K inline path: Rust vs Thrift, fine-grained.

For 10K and 100K (both inline-Arrow path on Rust), record:
  - Rust: T_exec (single-call), T_fetch_first_batch, T_drain
  - Thrift: T_exec, T_fetchall_arrow

10 runs each, ignore the first as warmup. Min, median, p90.
"""
from __future__ import annotations
import os, statistics, time
import databricks_adbc_pyo3 as dbx
from databricks import sql

QUERIES = [
    ("10K", "SELECT id, id*2 AS doubled FROM range(10000)"),
    ("100K", "SELECT id, id*2 AS doubled FROM range(100000)"),
]
RUNS = 10


def percentile(samples, p):
    s = sorted(samples)
    k = max(0, min(len(s) - 1, int(len(s) * p / 100)))
    return s[k]


def fmt(name, samples):
    s = sorted(samples)
    return (
        f"  {name:<24} min={s[0]:6.1f} ms  med={statistics.median(samples):6.1f} ms  "
        f"p90={percentile(samples, 90):6.1f} ms  max={s[-1]:6.1f} ms"
    )


def bench_pyo3():
    print("=== ADBC-Rust ===")
    conn = dbx.Connection(
        os.environ["DATABRICKS_HOST"],
        os.environ["DATABRICKS_HTTP_PATH"],
        os.environ["DATABRICKS_TOKEN"],
    )
    # warmup
    conn.execute("SELECT id FROM range(100000)").fetch_all_arrow()
    conn.execute("SELECT id FROM range(10000)").fetch_all_arrow()

    for name, q in QUERIES:
        execs, firsts, drains = [], [], []
        for _ in range(RUNS):
            t0 = time.perf_counter()
            rs = conn.execute(q)
            execs.append((time.perf_counter() - t0) * 1000)
            t0 = time.perf_counter()
            rs.fetch_next_batch()
            firsts.append((time.perf_counter() - t0) * 1000)
            t0 = time.perf_counter()
            while rs.fetch_next_batch() is not None:
                pass
            drains.append((time.perf_counter() - t0) * 1000)
        print(f"  --- {name} ---")
        print(fmt("execute()", execs))
        print(fmt("first batch", firsts))
        print(fmt("drain rest", drains))


def bench_thrift():
    print("\n=== Thrift ===")
    conn = sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
        use_sea=False,
    )
    cur = conn.cursor()
    cur.execute("SELECT id FROM range(100000)").fetchall_arrow()
    cur.execute("SELECT id FROM range(10000)").fetchall_arrow()

    for name, q in QUERIES:
        execs, drains = [], []
        for _ in range(RUNS):
            t0 = time.perf_counter()
            cur.execute(q)
            execs.append((time.perf_counter() - t0) * 1000)
            t0 = time.perf_counter()
            cur.fetchall_arrow()
            drains.append((time.perf_counter() - t0) * 1000)
        print(f"  --- {name} ---")
        print(fmt("execute()", execs))
        print(fmt("fetchall_arrow", drains))

    cur.close()
    conn.close()


bench_pyo3()
bench_thrift()
