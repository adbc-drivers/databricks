# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Report OSV findings and apply the driver repositories' severity policy."""

import argparse
import html
import json
import math
import os
import re
from pathlib import Path


def numeric_score(value):
    if isinstance(value, bool):
        return None
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    return score if math.isfinite(score) and 0 <= score <= 10 else None


def collect_findings(report):
    if not isinstance(report, dict) or not isinstance(report.get("results"), list):
        raise TypeError("OSV output must contain a results array")

    findings = []
    for source in report["results"]:
        for package in source["packages"]:
            metadata = package["package"]
            vulns = package.get("vulnerabilities", [])
            groups = package.get("groups", [])
            grouped_ids = set()
            for group in groups:
                ids = group["ids"]
                if (
                    not isinstance(ids, list)
                    or not ids
                    or not all(isinstance(id_, str) and id_ for id_ in ids)
                ):
                    raise ValueError("OSV finding has no valid advisory IDs")
                grouped_ids.update(ids)
                aliases = set(ids) | set(group.get("aliases", []))
                summaries = []
                score = numeric_score(group.get("max_severity"))
                for vuln in vulns:
                    if vuln["id"] in ids:
                        aliases.update(vuln.get("aliases", []))
                        summaries.append(vuln.get("summary") or vuln["id"])
                findings.append(
                    {
                        "pkg": f"{metadata['name']}@{metadata['version']}",
                        "ecosystem": metadata["ecosystem"],
                        "source": source["source"]["path"],
                        "ids": sorted(aliases),
                        "summary": "; ".join(dict.fromkeys(summaries)),
                        "severity": "UNKNOWN" if score is None else str(score),
                    }
                )
            if any(vuln["id"] not in grouped_ids for vuln in vulns):
                raise ValueError("OSV vulnerability is missing its advisory group")

    return sorted(
        findings,
        key=lambda finding: (
            finding["severity"] != "UNKNOWN",
            -(numeric_score(finding["severity"]) or 0),
        ),
    )


def is_blocking(finding):
    score = numeric_score(finding["severity"])
    return score is None or score >= 7


def blocking_findings(findings, event):
    return list(filter(is_blocking, findings)) if event == "pull_request" else findings


def render_summary(findings, *, event="pull_request"):
    threshold = "CVSS >= 7 or unscored" if event == "pull_request" else "all findings"
    lines = [
        "## Security Scan Findings",
        "",
        f"- Total findings: {len(findings)}",
        f"- Blocking findings ({threshold}): {len(blocking_findings(findings, event))}",
    ]
    if findings:
        lines += [
            "",
            "| Severity | Ecosystem | Source | Package | Finding | IDs |",
            "|---|---|---|---|---|---|",
        ]
        for finding in findings:
            package = finding["pkg"]
            summary = finding["summary"]
            if len(summary) > 240:
                summary = summary[:237] + "..."
            cells = [
                finding["severity"],
                finding["ecosystem"],
                finding["source"],
                package,
                summary,
                ", ".join(finding["ids"]),
            ]
            cells = [
                re.sub(
                    r"([\\`*_{}\[\]!|])",
                    r"\\\1",
                    html.escape(
                        cell.replace("\n", " ").replace("\r", " "), quote=False
                    ),
                )
                for cell in cells
            ]
            lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument(
        "--event",
        required=True,
        choices=["pull_request", "schedule", "workflow_dispatch"],
    )
    parser.add_argument("--scan-exit-code", required=True, type=int, choices=[0, 1])
    args = parser.parse_args()

    try:
        findings = collect_findings(json.loads(args.report.read_text()))
        if args.scan_exit_code == 1 and not findings:
            raise ValueError(
                "OSV reported vulnerabilities but the report has no findings"
            )
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        print(f"::error::Invalid OSV report: {error}")
        return 1

    args.report.with_name("all-findings.json").write_text(
        json.dumps(findings, indent=2) + "\n"
    )
    summary = render_summary(findings, event=args.event)
    print(summary)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(summary)

    failed = bool(blocking_findings(findings, args.event))
    if failed:
        print(
            "::error::Unsuppressed vulnerabilities exceed the security scan threshold."
        )
        print(
            "Upgrade affected dependencies or document an expiring exception in osv-scanner.toml."
        )
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
