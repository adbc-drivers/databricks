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

from security_scan_report import collect_findings, is_blocking, numeric_score


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

    def test_numeric_advisory_severity_fallback(self):
        report = report_with_score("")
        report["results"][0]["packages"][0]["vulnerabilities"][0]["severity"] = [
            {"type": "CVSS_V3", "score": "8.1"}
        ]
        finding = collect_findings(report)[0]
        self.assertEqual(finding["severity"], "8.1")
        self.assertTrue(is_blocking(finding))

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
    def run_report(self, report, event="pull_request", scan_exit_code=1, raw=False):
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "osv-out.json"
            report_path.write_text(report if raw else json.dumps(report))
            summary_path = Path(directory) / "summary.md"
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).with_name("security_scan_report.py")),
                    str(report_path),
                    "--event",
                    event,
                    "--scan-exit-code",
                    str(scan_exit_code),
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
