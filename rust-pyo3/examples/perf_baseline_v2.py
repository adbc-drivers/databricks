#!/usr/bin/env python3
# Copyright (c) 2026 ADBC Drivers Contributors
# Licensed under the Apache License, Version 2.0
"""Interleaved perf baseline: Rust and Thrift alternate per query iteration.

Earlier perf_baseline.py ran Rust first, then Thrift. Warehouse warmup state
shifted between the two phases, biasing results. This version interleaves
backends iteration-by-iteration so they see the same warehouse state on
matched runs, then reports per-pair ratios.
"""
from __future__ import annotations

import os
import random
import statistics
import sys
import time

import databricks_adbc_pyo3 as dbx
from databricks import sql

SIZES = [
    ("inline_1", "SELECT 1 AS one"),
    ("10K", "SELECT id, id * 2 AS doubled FROM range(10000)"),
    ("100K", "SELECT id, id * 2 AS doubled FROM range(100000)"),
    ("500K", "SELECT id, id * 2 AS doubled FROM range(500000)"),
    ("1M", "SELECT id, id * 2 AS doubled FROM range(1000000)"),
    ("5M", "SELECT id, id * 2 AS doubled FROM range(5000000)"),
    ("10M", "SELECT id, id * 2 AS doubled FROM range(10000000)"),
]
WARMUPS = 3
RUNS = 20  # interleaved pairs per size


def percentile(samples, p):
    s = sorted(samples)
    k = max(0, min(len(s) - 1, int(len(s) * p / 100)))
    return s[k]


def main() -> int:
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

    # warmups
    for _ in range(WARMUPS):
        rust.execute("SELECT id FROM range(100000)").fetch_all_arrow()
        tcur.execute("SELECT id FROM range(100000)")
        tcur.fetchall_arrow()
    print(f"warmups: {WARMUPS} per backend")

    print(
        f"\n{'size':>10}  {'rust min':>9}  {'thr min':>9}  "
        f"{'rust med':>9}  {'thr med':>9}  "
        f"{'rust p90':>9}  {'thr p90':>9}  "
        f"{'med ratio':>9}"
    )
    rng = random.Random(42)
    for name, query in SIZES:
        rs, ts = [], []
        for _ in range(RUNS):
            # Randomize order each iteration so neither backend is consistently first.
            order = ["rust", "thrift"]
            rng.shuffle(order)
            for backend in order:
                if backend == "rust":
                    t0 = time.perf_counter()
                    rust.execute(query).fetch_all_arrow()
                    rs.append((time.perf_counter() - t0) * 1000)
                else:
                    t0 = time.perf_counter()
                    tcur.execute(query)
                    tcur.fetchall_arrow()
                    ts.append((time.perf_counter() - t0) * 1000)

        rmed = statistics.median(rs)
        tmed = statistics.median(ts)
        ratio = rmed / tmed
        print(
            f"{name:>10}  {min(rs):>7.0f}ms  {min(ts):>7.0f}ms  "
            f"{rmed:>7.0f}ms  {tmed:>7.0f}ms  "
            f"{percentile(rs, 90):>7.0f}ms  {percentile(ts, 90):>7.0f}ms  "
            f"{ratio:>8.2f}x"
        )

    tcur.close()
    thrift.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
