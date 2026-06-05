#!/usr/bin/env python3
# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Merge a parsed nightly-run record into the gh-pages E2E dashboard.

Usage:
    update-e2e-dashboard.py RUN_RECORD.json DASHBOARD_DIR TEMPLATE_INDEX.html

  RUN_RECORD.json   output of parse-trx-to-json.py
  DASHBOARD_DIR     e.g. <gh-pages checkout>/e2e-nightly
  TEMPLATE_INDEX    static dashboard page to (re)publish alongside the data

Layout produced under DASHBOARD_DIR:
    index.html                       the dashboard (copied from template)
    data/runs.json                   lightweight history index (one row/run)
    data/run-<run_id>-<protocol>.json full per-run detail (failure list)

History is append-only and keyed by (run_id, protocol) so a re-run replaces
its prior row rather than duplicating it.
"""
import json
import os
import shutil
import sys

# Keep history bounded so the index stays small and the page stays fast.
MAX_RUNS = 365


def main():
    if len(sys.argv) != 4:
        print("usage: update-e2e-dashboard.py RUN_RECORD.json DASHBOARD_DIR TEMPLATE_INDEX.html",
              file=sys.stderr)
        sys.exit(2)

    record_path, dashboard_dir, template = sys.argv[1], sys.argv[2], sys.argv[3]
    data_dir = os.path.join(dashboard_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    with open(record_path) as f:
        record = json.load(f)

    run_id = record.get("run_id", "0")
    protocol = record.get("protocol", "") or "unknown"
    detail_name = f"run-{run_id}-{protocol}.json"

    # Full detail (incl. failure messages) lives in its own file, fetched
    # lazily by the dashboard only when a run is expanded.
    with open(os.path.join(data_dir, detail_name), "w") as f:
        json.dump(record, f, indent=2)

    # Lightweight summary row for the history index — drop the heavy bits.
    summary = {k: record[k] for k in (
        "run_id", "run_attempt", "run_number", "timestamp", "commit", "branch",
        "protocol", "read_only", "html_url", "total", "passed", "failed",
        "skipped", "pass_rate", "by_category", "by_signature", "by_class",
    ) if k in record}
    summary["detail"] = detail_name

    runs_path = os.path.join(data_dir, "runs.json")
    runs = []
    if os.path.exists(runs_path):
        try:
            with open(runs_path) as f:
                runs = json.load(f)
        except (ValueError, OSError):
            runs = []

    key = (summary.get("run_id"), summary.get("protocol"))
    runs = [r for r in runs if (r.get("run_id"), r.get("protocol")) != key]
    runs.append(summary)
    runs.sort(key=lambda r: (r.get("timestamp", ""), r.get("run_id", "")))
    if len(runs) > MAX_RUNS:
        runs = runs[-MAX_RUNS:]

    with open(runs_path, "w") as f:
        json.dump(runs, f, indent=2)

    shutil.copyfile(template, os.path.join(dashboard_dir, "index.html"))
    print(f"Dashboard updated: {len(runs)} runs in index, detail -> {detail_name}")


if __name__ == "__main__":
    main()
