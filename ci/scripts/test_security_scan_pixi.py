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
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml
from security_scan_pixi import collect_conda_packages, collect_packages


def pypi_package(name="certifi", version="2026.1.4"):
    return {"pypi": f"https://example.org/{name}.whl", "name": name, "version": version}


def conda_package(platform="linux-64", suffix=".conda"):
    return {
        "conda": f"https://conda.anaconda.org/conda-forge/{platform}/openssl-3.6.1-h123_1{suffix}",
        "sha256": "a" * 64,
        "depends": ["ca-certificates"],
    }


class PixiInventoryTest(unittest.TestCase):
    def test_conda_artifact_identity_and_metadata_are_preserved(self):
        for schema, suffix in [(6, ".conda"), (7, ".tar.bz2")]:
            with self.subTest(schema=schema):
                packages = collect_conda_packages(
                    {"version": schema, "packages": [conda_package(suffix=suffix)]}
                )
                self.assertEqual(len(packages), 1)
                package = packages[0]
                self.assertEqual(package["name"], "openssl")
                self.assertEqual(package["version"], "3.6.1")
                self.assertEqual(package["build"], "h123_1")
                self.assertEqual(package["subdir"], "linux-64")
                self.assertEqual(package["sha256"], "a" * 64)
                self.assertEqual(package["depends"], ["ca-certificates"])

    def test_conda_platforms_are_preserved_and_duplicate_urls_are_deduplicated(self):
        packages = collect_conda_packages(
            {
                "version": 7,
                "packages": [
                    conda_package(),
                    conda_package(),
                    conda_package("win-64"),
                    pypi_package(),
                ],
            }
        )
        self.assertEqual(len(packages), 2)
        self.assertEqual({p["subdir"] for p in packages}, {"linux-64", "win-64"})

    def test_hyphenated_conda_package_names_are_preserved(self):
        package = conda_package()
        package["conda"] = package["conda"].replace("/openssl-", "/ca-certificates-")
        self.assertEqual(
            collect_conda_packages({"version": 6, "packages": [package]})[0]["name"],
            "ca-certificates",
        )

    def test_unsupported_conda_artifacts_fail_instead_of_disappearing(self):
        for value in [
            {"git": "https://example.org/repo"},
            "https://example.org/package.conda",
            "file:///tmp/package-1.0-0.conda",
        ]:
            with self.subTest(value=value), self.assertRaises((ValueError, TypeError)):
                collect_conda_packages({"version": 7, "packages": [{"conda": value}]})

    def test_certifi_is_exported_from_both_supported_schemas(self):
        for schema in (6, 7):
            with self.subTest(schema=schema):
                packages = collect_packages(
                    {"version": schema, "packages": [pypi_package()]}
                )
                self.assertEqual(
                    packages,
                    [
                        {
                            "package": {
                                "name": "certifi",
                                "version": "2026.1.4",
                                "ecosystem": "PyPI",
                            }
                        }
                    ],
                )

    def test_platform_wheels_are_deduplicated(self):
        packages = collect_packages(
            {
                "version": 6,
                "packages": [
                    pypi_package("Example_Package"),
                    pypi_package("example-package"),
                ],
            }
        )
        self.assertEqual(len(packages), 1)
        self.assertEqual(packages[0]["package"]["name"], "example-package")

    def test_distinct_locked_versions_are_preserved(self):
        packages = collect_packages(
            {
                "version": 7,
                "packages": [
                    pypi_package(version="2026.1.4"),
                    pypi_package(version="2025.11.12"),
                ],
            }
        )
        self.assertEqual(len(packages), 2)

    def test_conda_and_git_packages_are_not_mapped_to_pypi(self):
        packages = collect_packages(
            {
                "version": 6,
                "packages": [
                    {"conda": "https://example.org/package.conda"},
                    {
                        "pypi": "git+https://example.org/repo#commit",
                        "name": "example",
                        "version": "1",
                    },
                    pypi_package(),
                ],
            }
        )
        self.assertEqual(len(packages), 1)
        self.assertEqual(packages[0]["package"]["name"], "certifi")

    def test_missing_package_metadata_fails(self):
        for field in ("name", "version"):
            with self.subTest(field=field):
                package = pypi_package()
                del package[field]
                with self.assertRaises(ValueError):
                    collect_packages({"version": 6, "packages": [package]})

    def test_invalid_pypi_url_type_fails(self):
        for url in [None, {}]:
            with self.subTest(url=url), self.assertRaises(TypeError):
                collect_packages(
                    {"version": 7, "packages": [{**pypi_package(), "pypi": url}]}
                )

    def test_invalid_schema_fails(self):
        for lock in (
            None,
            {},
            {"version": 8, "packages": []},
            {"version": 6, "packages": None},
        ):
            with self.subTest(lock=lock), self.assertRaises((ValueError, TypeError)):
                collect_packages(lock)

    def test_empty_inventory_fails(self):
        with self.assertRaises(ValueError):
            collect_packages({"version": 7, "packages": []})

    def test_cli_preserves_each_lockfile_and_certifi_version(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lockfiles = []
            for language, schema, version in [
                ("rust", 6, "2026.1.4"),
                ("go", 7, "2025.11.12"),
            ]:
                lockfile = root / language / "pixi.lock"
                lockfile.parent.mkdir()
                lockfile.write_text(
                    yaml.safe_dump(
                        {
                            "version": schema,
                            "packages": [
                                pypi_package(version=version),
                                conda_package(),
                            ],
                        }
                    )
                )
                lockfiles.append(str(lockfile))
            subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).with_name("security_scan_pixi.py")),
                    *lockfiles,
                    "--output-directory",
                    str(root / "inventories"),
                ],
                check=True,
                capture_output=True,
            )
            for language, version in [("rust", "2026.1.4"), ("go", "2025.11.12")]:
                inventory = json.loads(
                    (root / "inventories" / language / "osv-scanner.json").read_text()
                )
                self.assertEqual(
                    inventory["results"][0]["source"]["path"],
                    str(root / language / "pixi.lock"),
                )
                self.assertEqual(
                    inventory["results"][0]["packages"][0]["package"]["version"],
                    version,
                )
                exported = root / "inventories" / language
                metadata = list((exported / "conda-meta").glob("*.json"))
                self.assertEqual(len(metadata), 1)
                package = json.loads(metadata[0].read_text())
                self.assertEqual(package["name"], "openssl")
                self.assertEqual(package["sha256"], "a" * 64)
                conda_inventory = json.loads(
                    (exported / "conda-inventory.json").read_text()
                )
                self.assertEqual(
                    conda_inventory["source"], str(root / language / "pixi.lock")
                )
                self.assertEqual(conda_inventory["packages"], [package])


if __name__ == "__main__":
    unittest.main()
