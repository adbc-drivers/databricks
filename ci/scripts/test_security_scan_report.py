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

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from security_scan_report import (
    collect_conda_reports,
    collect_findings,
    collect_grype_findings,
    is_blocking,
    numeric_score,
)


def report_with_score(score):
    return {
        "results": [
            {
                "source": {"path": "csharp/src/packages.lock.json", "type": "lockfile"},
                "packages": [
                    {
                        "package": {
                            "name": "example",
                            "version": "1.0",
                            "ecosystem": "NuGet",
                        },
                        "vulnerabilities": [{"id": "GHSA-example"}],
                        "groups": [{"ids": ["GHSA-example"], "max_severity": score}],
                    }
                ],
            }
        ]
    }


def conda_artifact():
    return {
        "id": "conda-artifact",
        "name": "openssl",
        "version": "3.6.1",
        "type": "conda",
        "metadata": {
            "build": "h123_1",
            "subdir": "linux-64",
            "url": "https://conda.anaconda.org/conda-forge/linux-64/openssl-3.6.1-h123_1.conda",
        },
    }


def conda_report(score=7.8, severity="High"):
    return {
        "matches": [
            {
                "vulnerability": {
                    "id": "CVE-example",
                    "severity": severity,
                    "description": "Example native library vulnerability",
                    "cvss": []
                    if score is None
                    else [{"metrics": {"baseScore": score}}],
                },
                "relatedVulnerabilities": [{"id": "GHSA-example"}],
                "matchDetails": [{"type": "cpe-match"}],
                "artifact": {
                    key: value
                    for key, value in conda_artifact().items()
                    if key != "metadata"
                },
            }
        ],
    }


def write_conda_reports(root, report):
    directory = root / "pixi" / "rust"
    directory.mkdir(parents=True)
    artifact = conda_artifact()
    package = {
        "name": artifact["name"],
        "version": artifact["version"],
        **artifact["metadata"],
    }
    for filename, data in {
        "conda-inventory.json": {"source": "rust/pixi.lock", "packages": [package]},
        "conda-sbom.json": {"artifacts": [artifact]},
        "conda-findings.json": report,
    }.items():
        (directory / filename).write_text(json.dumps(data))
    return directory.parent


class CondaFindingsTest(unittest.TestCase):
    def test_cpe_evidence_build_platform_and_aliases_are_preserved(self):
        report = conda_report()
        report["matches"].append(report["matches"][0])
        findings = collect_grype_findings(
            report, "rust/pixi.lock", {"conda-artifact": conda_artifact()}
        )
        self.assertEqual(len(findings), 1)
        finding = findings[0]
        self.assertEqual(finding["ecosystem"], "Conda")
        self.assertEqual(finding["ids"], ["CVE-example", "GHSA-example"])
        self.assertEqual(finding["source"], "rust/pixi.lock")
        self.assertEqual(finding["build"], "h123_1")
        self.assertEqual(finding["platform"], "linux-64")
        self.assertEqual(finding["match_details"], [{"type": "cpe-match"}])
        self.assertTrue(is_blocking(finding))

    def test_grype_ratings_apply_when_numeric_cvss_is_unavailable(self):
        for rating, blocking in [
            ("High", True),
            ("Critical", True),
            ("Unknown", True),
            ("Medium", False),
            ("Low", False),
        ]:
            with self.subTest(rating=rating):
                finding = collect_grype_findings(
                    conda_report(None, rating),
                    "go/pixi.lock",
                    {"conda-artifact": conda_artifact()},
                )[0]
                self.assertEqual(is_blocking(finding), blocking)

    def test_empty_grype_report_is_valid_but_missing_matches_fails(self):
        self.assertEqual(
            collect_grype_findings({"matches": []}, "rust/pixi.lock", {}), []
        )
        for report in [{}, None, {"matches": None}]:
            with self.subTest(report=report), self.assertRaises(TypeError):
                collect_grype_findings(report, "rust/pixi.lock", {})

    def test_dropped_or_changed_conda_packages_fail_closed(self):
        for mutation in ("drop", "version", "platform"):
            with (
                self.subTest(mutation=mutation),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = write_conda_reports(Path(directory), conda_report())
                path = root / "rust" / "conda-sbom.json"
                sbom = json.loads(path.read_text())
                if mutation == "drop":
                    sbom["artifacts"] = []
                elif mutation == "version":
                    sbom["artifacts"][0]["version"] = "99.0"
                else:
                    sbom["artifacts"][0]["metadata"]["subdir"] = "win-64"
                path.write_text(json.dumps(sbom))
                with self.assertRaisesRegex(ValueError, "Incomplete Conda SBOM"):
                    collect_conda_reports(root)

    def test_missing_conda_reports_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = write_conda_reports(Path(directory), conda_report())
            (root / "rust" / "conda-findings.json").unlink()
            with self.assertRaises(OSError):
                collect_conda_reports(root)


class FindingsTest(unittest.TestCase):
    def test_severity_threshold(self):
        for score, blocking in [
            ("0", False),
            ("6.9", False),
            ("7", True),
            ("10", True),
        ]:
            with self.subTest(score=score):
                findings = collect_findings(report_with_score(score))
                self.assertEqual(len(findings), 1)
                self.assertEqual(is_blocking(findings[0]), blocking)

    def test_unscored_findings_always_block(self):
        for score in [None, "", "UNKNOWN", "NaN", "inf", "11", True, "CVSS:3.1/AV:N"]:
            with self.subTest(score=score):
                finding = collect_findings(report_with_score(score))[0]
                self.assertEqual(finding["severity"], "UNKNOWN")
                self.assertTrue(is_blocking(finding))

    def test_missing_scanner_score_stays_unscored_with_cvss_vector(self):
        report = report_with_score("")
        report["results"][0]["packages"][0]["vulnerabilities"][0]["severity"] = [
            {
                "type": "CVSS_V3",
                "score": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
            }
        ]
        finding = collect_findings(report)[0]
        self.assertEqual(finding["severity"], "UNKNOWN")
        self.assertTrue(is_blocking(finding))

    def test_unscored_then_highest_severity_findings_appear_first(self):
        report = {"results": []}
        for score in ["0", "6.9", "", "9.1", "7"]:
            report["results"].extend(report_with_score(score)["results"])
        self.assertEqual(
            [finding["severity"] for finding in collect_findings(report)],
            ["UNKNOWN", "9.1", "7.0", "6.9", "0.0"],
        )

    def test_aliases_are_counted_once_per_package(self):
        report = report_with_score("8.1")
        package = report["results"][0]["packages"][0]
        package["vulnerabilities"].append({"id": "CVE-example"})
        package["groups"][0]["ids"].append("CVE-example")
        self.assertEqual(len(collect_findings(report)), 1)

    def test_emsign_cve_aliases_and_summary_are_reported(self):
        report = report_with_score("")
        package = report["results"][0]["packages"][0]
        package["groups"][0]["aliases"] = ["CVE-2026-111943"]
        package["vulnerabilities"][0].update(
            {
                "aliases": ["RUSTSEC-2026-0382", "CVE-2026-111943"],
                "summary": "Certificate verification may accept distrusted emSign intermediates",
            }
        )
        finding = collect_findings(report)[0]
        self.assertIn("CVE-2026-111943", finding["ids"])
        self.assertIn("RUSTSEC-2026-0382", finding["ids"])
        self.assertIn("emSign", finding["summary"])
        self.assertTrue(is_blocking(finding))

    def test_ungrouped_vulnerability_is_an_error(self):
        report = report_with_score("9.8")
        del report["results"][0]["packages"][0]["groups"]
        with self.assertRaises(ValueError):
            collect_findings(report)

    def test_invalid_advisory_ids_are_an_error(self):
        for ids in [[], "GHSA-example", [None], [""]]:
            with self.subTest(ids=ids):
                report = report_with_score("9.8")
                report["results"][0]["packages"][0]["groups"][0]["ids"] = ids
                with self.assertRaises(ValueError):
                    collect_findings(report)

    def test_numeric_score_rejects_invalid_values(self):
        for value in [None, "", "NaN", "-1", "10.1", False]:
            with self.subTest(value=value):
                self.assertIsNone(numeric_score(value))


class ReportingTest(unittest.TestCase):
    def run_report(
        self, report, event="pull_request", scan_exit_code=1, raw=False, conda=None
    ):
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "osv-out.json"
            report_path.write_text(report if raw else json.dumps(report))
            summary_path = Path(directory) / "summary.md"
            extra_args = []
            if conda:
                conda_directory = write_conda_reports(Path(directory), conda)
                extra_args = ["--conda-reports-directory", str(conda_directory)]
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).with_name("security_scan_report.py")),
                    str(report_path),
                    "--event",
                    event,
                    "--scan-exit-code",
                    str(scan_exit_code),
                    *extra_args,
                ],
                env={**os.environ, "GITHUB_STEP_SUMMARY": str(summary_path)},
                check=False,
                text=True,
                capture_output=True,
            )
            findings_path = Path(directory) / "all-findings.json"
            findings = (
                json.loads(findings_path.read_text())
                if findings_path.exists()
                else None
            )
            summary = summary_path.read_text() if summary_path.exists() else ""
            return result, findings, summary

    def test_conda_and_osv_findings_share_the_report_and_policy(self):
        result, findings, summary = self.run_report(
            report_with_score("6.9"), conda=conda_report()
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(len(findings), 2)
        self.assertEqual(findings[0]["ecosystem"], "Conda")
        self.assertIn("Conda artifacts inventoried from rust/pixi.lock: 1", summary)
        self.assertIn("CPE matching", summary)

    def test_conda_medium_uses_the_same_event_thresholds(self):
        for event, returncode in [
            ("pull_request", 0),
            ("schedule", 1),
            ("workflow_dispatch", 1),
        ]:
            with self.subTest(event=event):
                result, findings, _summary = self.run_report(
                    {"results": []},
                    event=event,
                    scan_exit_code=0,
                    conda=conda_report(5.5, "Medium"),
                )
                self.assertEqual(
                    result.returncode, returncode, result.stdout + result.stderr
                )
                self.assertEqual(len(findings), 1)

    def test_clean_report_passes_all_events(self):
        for event in ["pull_request", "schedule", "workflow_dispatch"]:
            with self.subTest(event=event):
                result, findings, summary = self.run_report(
                    {"results": []}, event=event, scan_exit_code=0
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(findings, [])
                self.assertIn("Total findings: 0", summary)

    def test_medium_is_reported_but_passes_pr_gate(self):
        result, findings, summary = self.run_report(report_with_score("6.9"))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(findings), 1)
        self.assertIn("example@1.0", summary)

    def test_medium_fails_scheduled_and_manual_runs(self):
        for event in ["schedule", "workflow_dispatch"]:
            with self.subTest(event=event):
                result, findings, summary = self.run_report(
                    report_with_score("6.9"), event=event
                )
                self.assertEqual(result.returncode, 1)
                self.assertEqual(len(findings), 1)
                self.assertIn("example@1.0", summary)

    def test_blocking_pr_findings_still_write_artifacts(self):
        for score in ["7.0", ""]:
            with self.subTest(score=score):
                result, findings, summary = self.run_report(report_with_score(score))
                self.assertEqual(result.returncode, 1)
                self.assertEqual(len(findings), 1)
                self.assertIn("GHSA-example", summary)

    def test_invalid_or_partial_reports_fail_closed(self):
        for raw in ["", "{", "null", "{}", '{"results": null}', '{"results": [{}]}']:
            with self.subTest(raw=raw):
                result, findings, _summary = self.run_report(
                    raw, scan_exit_code=0, raw=True
                )
                self.assertEqual(result.returncode, 1)
                self.assertIn("::error::Invalid OSV report", result.stdout)
                self.assertIsNone(findings)

    def test_missing_findings_after_scanner_exit_one_fail_closed(self):
        result, findings, _summary = self.run_report({"results": []})
        self.assertEqual(result.returncode, 1)
        self.assertIn("report has no findings", result.stdout)
        self.assertIsNone(findings)


if __name__ == "__main__":
    unittest.main()
