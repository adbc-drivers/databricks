#!/usr/bin/env python3
# Copyright (c) 2026 ADBC Drivers Contributors
# Licensed under the Apache License, Version 2.0
"""POC perf baseline: ADBC-Rust vs Thrift across query sizes.

For each (backend, size) pair, runs the same `SELECT id, id*2 FROM range(N)`
3x and reports min wall time. A warmup query is run first per connection to
cover cold-start. fetchall_arrow path only — DB-API row conversion is a
separate (slower) topic.
"""
from __future__ import annotations

import os
import statistics
import sys
import time
from contextlib import contextmanager

from databricks import sql

SIZES = [
    ("inline_1", "SELECT 1 AS one"),
    ("10K", "SELECT id, id * 2 AS doubled FROM range(10000)"),
    ("100K", "SELECT id, id * 2 AS doubled FROM range(100000)"),
    ("500K", "SELECT id, id * 2 AS doubled FROM range(500000)"),
    ("1M", "SELECT id, id * 2 AS doubled FROM range(1000000)"),
    ("10M", "SELECT id, id * 2 AS doubled FROM range(10000000)"),
]
WARMUPS = 2
RUNS = 10


@contextmanager
def conn(use_sea: bool):
    c = sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
        use_sea=use_sea,
    )
    try:
        yield c
    finally:
        c.close()


def time_one(cursor, sql_query: str) -> tuple[float, int]:
    t0 = time.perf_counter()
    cursor.execute(sql_query)
    table = cursor.fetchall_arrow()
    elapsed = time.perf_counter() - t0
    return elapsed, table.num_rows


def bench(use_sea: bool) -> list[dict]:
    label = "ADBC-Rust" if use_sea else "Thrift"
    print(f"\n=== {label} ===")
    results = []
    with conn(use_sea) as c:
        with c.cursor() as cur:
            # warmups
            for _ in range(WARMUPS):
                t0 = time.perf_counter()
                cur.execute("SELECT id FROM range(100000)")
                cur.fetchall_arrow()
            print(f"  warmups done ({WARMUPS}x100K)")

            for name, query in SIZES:
                samples = []
                rows = 0
                for _ in range(RUNS):
                    elapsed, rows = time_one(cur, query)
                    samples.append(elapsed)
                best = min(samples)
                med = statistics.median(samples)
                stdev = statistics.stdev(samples) if len(samples) > 1 else 0.0
                results.append(
                    {
                        "size": name,
                        "rows": rows,
                        "best_s": best,
                        "median_s": med,
                        "stdev_s": stdev,
                        "all_s": samples,
                    }
                )
                rps = rows / best if best > 0 else 0
                print(
                    f"  {name:>8} ({rows:>10,} rows): "
                    f"best {best:6.3f}s  med {med:6.3f}s  "
                    f"stdev {stdev:5.3f}s  ({rps / 1e6:5.2f}M rows/s)  "
                    f"all={[round(s, 3) for s in samples]}"
                )
    return results


def main() -> int:
    rust = bench(use_sea=True)
    thrift = bench(use_sea=False)

    print("\n=== summary (median wall time, fetchall_arrow) ===")
    print(
        f"{'size':>10}  {'ADBC-Rust med':>14}  {'Thrift med':>12}  "
        f"{'ratio (Rust/Thrift)':>22}"
    )
    for r, t in zip(rust, thrift):
        ratio = r["median_s"] / t["median_s"] if t["median_s"] > 0 else float("inf")
        print(
            f"{r['size']:>10}  {r['median_s']:>12.3f}s  "
            f"{t['median_s']:>10.3f}s  {ratio:>20.2f}x"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
