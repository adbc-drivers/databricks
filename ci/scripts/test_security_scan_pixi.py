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
from security_scan_pixi import collect_packages


def pypi_package(name="certifi", version="2026.1.4"):
    return {"pypi": f"https://example.org/{name}.whl", "name": name, "version": version}


class PixiInventoryTest(unittest.TestCase):
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
                        {"version": schema, "packages": [pypi_package(version=version)]}
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


if __name__ == "__main__":
    unittest.main()
