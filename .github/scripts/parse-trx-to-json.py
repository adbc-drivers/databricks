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
"""Parse one or more VSTest .trx files into a single nightly-run JSON record.

Usage:
    parse-trx-to-json.py OUTPUT.json TRX [TRX ...]

The output record captures the totals, per-test outcomes for everything that
did not pass, and a coarse "failure signature" for each failure so the
dashboard can group 100+ failures into a handful of root causes. Run metadata
(commit, branch, run id, timestamp, …) is read from the environment so the
script stays decoupled from the workflow.
"""
import json
import os
import re
import sys
import xml.etree.ElementTree as ET

# TRX uses a default namespace; strip it so we can query with plain tag names.
_NS = {"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}


def _localname(tag):
    return tag.split("}", 1)[-1] if "}" in tag else tag


def signature_for(message):
    """Collapse a failure message into a stable, human-readable bucket.

    The goal is triage, not precision: hundreds of parameterized failures that
    share one root cause (a 404 warehouse, a read-only rejection, …) should
    land in the same bucket regardless of the per-case query/value text.
    """
    if not message:
        return "Unknown / no message"
    m = message.replace("\r", " ").replace("\n", " ")

    patterns = [
        (r"ENDPOINT_NOT_FOUND", "Warehouse not found (ENDPOINT_NOT_FOUND / HTTP 404)"),
        (r"read[- ]?only|READ_ONLY|cannot be modified|not.*allowed.*read", "Read-only warehouse rejected write/DDL"),
        (r"INSERT|UPDATE|DELETE|MERGE|CREATE TABLE|DROP TABLE|ALTER TABLE", "DML/DDL rejected"),
        (r"PERMISSION_DENIED|not authorized|Forbidden|HTTP 403", "Permission denied (403)"),
        (r"timeout|timed out|TimeoutException", "Timeout"),
        (r"Couldn't connect|connection refused|HttpRequestException", "Connection / transport error"),
        (r"TABLE_OR_VIEW_NOT_FOUND|cannot be found|does not exist", "Object not found"),
        (r"PARSE_SYNTAX_ERROR|SYNTAX_ERROR", "SQL syntax error"),
        (r"Assert\.|Equal\(|Xunit|expected", "Assertion failed (value mismatch)"),
    ]
    for pat, label in patterns:
        if re.search(pat, m, re.IGNORECASE):
            return label
    # Fall back to the exception type at the head of the message, if present.
    exc = re.match(r"\s*([A-Za-z0-9_.]+Exception)", m)
    if exc:
        return exc.group(1)
    return "Other"


def class_of(test_name):
    """Best-effort owning class: strip the parameter list and the method."""
    base = test_name.split("(", 1)[0]
    return base.rsplit(".", 1)[0] if "." in base else base


def parse_trx(path):
    tree = ET.parse(path)
    root = tree.getroot()

    # Map testId -> testName from the <TestDefinitions> section.
    names = {}
    for ut in root.iter():
        if _localname(ut.tag) == "UnitTest":
            tm = None
            for child in ut:
                if _localname(child.tag) == "TestMethod":
                    tm = child
                    break
            tid = ut.get("id")
            if tid is not None and tm is not None:
                cls = tm.get("className", "")
                name = tm.get("name", "")
                names[tid] = (f"{cls}.{name}" if cls else name)

    results = []
    for r in root.iter():
        if _localname(r.tag) != "UnitTestResult":
            continue
        outcome = r.get("outcome", "")
        tid = r.get("testId")
        test_name = names.get(tid, r.get("testName", "unknown"))
        message = ""
        stack = ""
        for out in r:
            if _localname(out.tag) != "Output":
                continue
            for err in out:
                if _localname(err.tag) == "ErrorInfo":
                    for e in err:
                        ln = _localname(e.tag)
                        if ln == "Message":
                            message = (e.text or "").strip()
                        elif ln == "StackTrace":
                            stack = (e.text or "").strip()
        results.append({
            "name": test_name,
            "class": class_of(test_name),
            "outcome": outcome,
            "message": message,
            "stack": stack,
        })
    return results


def main():
    if len(sys.argv) < 3:
        print("usage: parse-trx-to-json.py OUTPUT.json TRX [TRX ...]", file=sys.stderr)
        sys.exit(2)

    out_path = sys.argv[1]
    trx_paths = sys.argv[2:]

    all_results = []
    for p in trx_paths:
        if os.path.isdir(p):
            for name in sorted(os.listdir(p)):
                if name.endswith(".trx"):
                    all_results.extend(parse_trx(os.path.join(p, name)))
        elif os.path.exists(p):
            all_results.extend(parse_trx(p))
        else:
            print(f"WARNING: {p} not found, skipping", file=sys.stderr)

    passed = [r for r in all_results if r["outcome"] == "Passed"]
    failed = [r for r in all_results if r["outcome"] == "Failed"]
    skipped = [r for r in all_results if r["outcome"] in ("NotExecuted", "Inconclusive")]

    # Trim payload: keep full detail for failures only.
    for r in failed:
        r["signature"] = signature_for(r["message"])
        if len(r["message"]) > 2000:
            r["message"] = r["message"][:2000] + " …(truncated)"
        if len(r["stack"]) > 2000:
            r["stack"] = r["stack"][:2000] + " …(truncated)"

    by_signature = {}
    by_class = {}
    for r in failed:
        by_signature[r["signature"]] = by_signature.get(r["signature"], 0) + 1
        by_class[r["class"]] = by_class.get(r["class"], 0) + 1

    total = len(all_results)
    record = {
        "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
        "run_number": os.environ.get("GITHUB_RUN_NUMBER", ""),
        "timestamp": os.environ.get("RUN_TIMESTAMP", ""),
        "commit": os.environ.get("GITHUB_SHA", ""),
        "branch": os.environ.get("GITHUB_REF_NAME", ""),
        "protocol": os.environ.get("RUN_PROTOCOL", ""),
        "read_only": os.environ.get("RUN_READ_ONLY", "") == "true",
        "workflow": os.environ.get("GITHUB_WORKFLOW", ""),
        "html_url": (
            f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/"
            f"{os.environ.get('GITHUB_REPOSITORY', '')}/actions/runs/"
            f"{os.environ.get('GITHUB_RUN_ID', '')}"
        ),
        "total": total,
        "passed": len(passed),
        "failed": len(failed),
        "skipped": len(skipped),
        "pass_rate": round(100.0 * len(passed) / total, 1) if total else 0.0,
        "by_signature": dict(sorted(by_signature.items(), key=lambda kv: -kv[1])),
        "by_class": dict(sorted(by_class.items(), key=lambda kv: -kv[1])),
        "failures": sorted(failed, key=lambda r: (r["signature"], r["name"])),
    }

    with open(out_path, "w") as f:
        json.dump(record, f, indent=2)
    print(f"Parsed {total} results ({record['passed']} passed, "
          f"{record['failed']} failed, {record['skipped']} skipped) -> {out_path}")


if __name__ == "__main__":
    main()
