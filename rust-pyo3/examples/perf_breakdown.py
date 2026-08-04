#!/usr/bin/env python3
# Copyright (c) 2026 ADBC Drivers Contributors
# Licensed under the Apache License, Version 2.0
"""Stage-by-stage timing for small queries via the Rust ADBC kernel.

Splits the 'SELECT 1' wall time into:
  T1: Connection construction (auth resolve + first round-trip if any)
  T2: PyConnection.execute("SELECT 1") returning a streaming PyResultSet
  T3: PyResultSet.arrow_schema()         (cheap if schema already known)
  T4: PyResultSet.fetch_next_batch()     (first/only batch for SELECT 1)
  T5: PyResultSet drop                   (releases server resources)

Plus per-stage timings for the steady-state case (warm connection):
  E1: execute(...)
  E2: fetch_next_batch(...)
  E3: drop ResultSet
"""
from __future__ import annotations

import gc
import os
import statistics
import time

import databricks_adbc_pyo3 as dbx


def main():
    host = os.environ["DATABRICKS_HOST"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]

    # ---------- Cold path: open + first query ----------
    print("=== cold ===")
    t0 = time.perf_counter()
    conn = dbx.Connection(host, http_path, token)
    t1 = time.perf_counter()
    print(f"  T1 Connection():            {(t1 - t0) * 1000:7.1f} ms")

    t0 = time.perf_counter()
    rs = conn.execute("SELECT 1")
    t1 = time.perf_counter()
    print(f"  T2 execute('SELECT 1'):     {(t1 - t0) * 1000:7.1f} ms")

    t0 = time.perf_counter()
    schema = rs.arrow_schema()
    t1 = time.perf_counter()
    print(f"  T3 arrow_schema():          {(t1 - t0) * 1000:7.1f} ms")

    t0 = time.perf_counter()
    batch = rs.fetch_next_batch()
    t1 = time.perf_counter()
    print(f"  T4 fetch_next_batch():      {(t1 - t0) * 1000:7.1f} ms")

    t0 = time.perf_counter()
    rs = None
    gc.collect()
    t1 = time.perf_counter()
    print(f"  T5 drop ResultSet:          {(t1 - t0) * 1000:7.1f} ms")

    # ---------- Warm path: repeated queries on same connection ----------
    print("\n=== warm (same connection, 10 iterations) ===")
    e1, e2, e3 = [], [], []
    for _ in range(10):
        t0 = time.perf_counter()
        rs = conn.execute("SELECT 1")
        t1 = time.perf_counter()
        e1.append((t1 - t0) * 1000)

        t0 = time.perf_counter()
        rs.fetch_next_batch()
        t1 = time.perf_counter()
        e2.append((t1 - t0) * 1000)

        t0 = time.perf_counter()
        rs = None
        gc.collect()
        t1 = time.perf_counter()
        e3.append((t1 - t0) * 1000)

    def fmt(name, samples):
        s = sorted(samples)
        return (
            f"  {name:<30} min={s[0]:6.1f} ms  "
            f"median={statistics.median(samples):6.1f} ms  "
            f"max={s[-1]:6.1f} ms"
        )

    print(fmt("E1 execute()", e1))
    print(fmt("E2 fetch_next_batch()", e2))
    print(fmt("E3 drop ResultSet", e3))
    total_min = min(e1) + min(e2) + min(e3)
    print(f"\n  warm SELECT 1 (sum of mins): {total_min:6.1f} ms")


if __name__ == "__main__":
    main()
