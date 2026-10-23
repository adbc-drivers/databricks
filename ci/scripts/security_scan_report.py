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

"""Report dependency findings and apply the driver repositories' severity policy."""

import argparse
import json
import math
import os
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

    return sort_findings(findings)


def sort_findings(findings):
    return sorted(
        findings,
        key=lambda finding: (
            finding["severity"] != "UNKNOWN",
            -(
                numeric_score(finding["severity"])
                or {
                    "CRITICAL": 9,
                    "HIGH": 7,
                    "MEDIUM": 4,
                    "LOW": 0.1,
                }.get(finding["severity"], 0)
            ),
        ),
    )


def collect_grype_findings(report, source, sbom_artifacts):
    if not isinstance(report, dict) or not isinstance(report.get("matches"), list):
        raise TypeError("Grype output must contain a matches array")
    findings = {}
    for match in report["matches"]:
        artifact = match["artifact"]
        if artifact["type"] != "conda":
            raise ValueError("Expected only Conda packages in the Grype report")
        vulnerability = match["vulnerability"]
        ids = {vulnerability["id"]}
        related = match.get("relatedVulnerabilities", [])
        ids.update(vuln["id"] for vuln in related)
        if not all(isinstance(id_, str) and id_ for id_ in ids):
            raise ValueError("Grype finding has no valid advisory IDs")
        scores = [
            score
            for vuln in [vulnerability, *related]
            for cvss in vuln.get("cvss", [])
            if (score := numeric_score(cvss.get("metrics", {}).get("baseScore")))
            is not None
        ]
        rating = vulnerability.get("severity", "Unknown").upper()
        severity = str(max(scores)) if scores else rating
        if not scores and rating not in {
            "CRITICAL",
            "HIGH",
            "MEDIUM",
            "LOW",
            "NEGLIGIBLE",
        }:
            severity = "UNKNOWN"
        sbom_artifact = sbom_artifacts[artifact["id"]]
        if any(
            artifact[key] != sbom_artifact[key] for key in ("name", "version", "type")
        ):
            raise ValueError("Grype finding does not match its SBOM package")
        # Grype omits Conda metadata; retain build provenance from the input SBOM.
        metadata = sbom_artifact["metadata"]
        findings[(artifact["id"], vulnerability["id"])] = {
            "pkg": f"{artifact['name']}@{artifact['version']}",
            "ecosystem": "Conda",
            "source": source,
            "ids": sorted(ids),
            "summary": vulnerability.get("description") or vulnerability["id"],
            "severity": severity,
            "build": metadata["build"],
            "platform": metadata["subdir"],
            "artifact_url": metadata["url"],
            "match_details": match.get("matchDetails", []),
        }
    return list(findings.values())


def collect_conda_reports(directory):
    inventories = sorted(directory.glob("*/conda-inventory.json"))
    if not inventories:
        raise ValueError("No Conda inventories found")
    findings, coverage = [], []
    for inventory_path in inventories:
        inventory = json.loads(inventory_path.read_text())
        packages = inventory["packages"]
        expected = {
            package["url"]: (
                package["name"],
                package["version"],
                package["build"],
                package["subdir"],
            )
            for package in packages
        }
        sbom = json.loads(inventory_path.with_name("conda-sbom.json").read_text())
        artifacts = sbom["artifacts"]
        actual = {
            artifact["metadata"]["url"]: (
                artifact["name"],
                artifact["version"],
                artifact["metadata"]["build"],
                artifact["metadata"]["subdir"],
            )
            for artifact in artifacts
        }
        if (
            not expected
            or expected != actual
            or len(artifacts) != len(packages)
            or any(artifact["type"] != "conda" for artifact in artifacts)
        ):
            raise ValueError(f"Incomplete Conda SBOM for {inventory['source']}")
        report = json.loads(inventory_path.with_name("conda-findings.json").read_text())
        conda_findings = collect_grype_findings(
            report,
            inventory["source"],
            {artifact["id"]: artifact for artifact in artifacts},
        )
        if any(finding["artifact_url"] not in expected for finding in conda_findings):
            raise ValueError("Grype reported a package outside the Conda inventory")
        findings.extend(conda_findings)
        coverage.append(
            {
                "source": inventory["source"],
                "artifacts": len(packages),
                "source_dependencies": inventory.get("source_dependencies", []),
            }
        )
    return findings, coverage


def is_blocking(finding):
    score = numeric_score(finding["severity"])
    return (
        finding["severity"] not in {"MEDIUM", "LOW", "NEGLIGIBLE"}
        if score is None
        else score >= 7
    )


def render_summary(findings, conda_coverage=()):
    lines = [
        "## Security Scan Findings",
        "",
        f"- Total findings: {len(findings)}",
        f"- Blocking findings (CVSS >= 7, high/critical, or unscored): {sum(map(is_blocking, findings))}",
    ]
    for inventory in conda_coverage:
        lines.append(
            f"- Conda artifacts inventoried from {inventory['source']}: {inventory['artifacts']}"
        )
        for dependency in inventory.get("source_dependencies", []):
            lines.append(
                f"- Source dependency outside registry advisory coverage: "
                f"{dependency['name']} ({inventory['source']}); see the inventory for its Git URL."
            )
    if conda_coverage:
        lines += [
            "",
            (
                "Conda findings use Grype CPE matching of locked package metadata. "
                "Review applicability and build backports; this does not inspect package contents."
            ),
        ]
    if findings:
        lines += [
            "",
            "| Severity | Ecosystem | Source | Package | Finding | IDs |",
            "|---|---|---|---|---|---|",
        ]
        for finding in findings:
            package = finding["pkg"]
            if finding.get("platform"):
                package += f" ({finding['platform']}, {finding['build']})"
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
            cells = [cell.replace("|", "\\|").replace("\n", " ") for cell in cells]
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
    parser.add_argument("--conda-reports-directory", type=Path)
    args = parser.parse_args()

    try:
        findings = collect_findings(json.loads(args.report.read_text()))
        if args.scan_exit_code == 1 and not findings:
            raise ValueError(
                "OSV reported vulnerabilities but the report has no findings"
            )
        conda_coverage = []
        if args.conda_reports_directory:
            conda_findings, conda_coverage = collect_conda_reports(
                args.conda_reports_directory
            )
            findings = sort_findings([*findings, *conda_findings])
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        print(f"::error::Invalid OSV report or Conda scan report: {error}")
        return 1

    args.report.with_name("all-findings.json").write_text(
        json.dumps(findings, indent=2) + "\n"
    )
    summary = render_summary(findings, conda_coverage)
    print(summary)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(summary)

    failed = (
        any(map(is_blocking, findings))
        if args.event == "pull_request"
        else bool(findings)
    )
    if failed:
        print(
            "::error::Unsuppressed vulnerabilities exceed the security scan threshold."
        )
        print(
            "Upgrade affected dependencies. OSV exceptions require a reason and expiry in osv-scanner.toml."
        )
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
