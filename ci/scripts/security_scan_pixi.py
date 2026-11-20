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

"""Export locked Pixi PyPI packages to OSV-Scanner's custom inventory format."""

import argparse
import json
import re
from pathlib import Path

import yaml


def collect_packages(lock):
    if not isinstance(lock, dict) or lock.get("version") not in (6, 7):
        raise ValueError("Expected a Pixi v6 or v7 lockfile")
    if not isinstance(lock.get("packages"), list):
        raise TypeError("Pixi lockfile must contain a packages array")

    packages = {}
    for record in lock["packages"]:
        if "pypi" not in record:
            continue
        url = record["pypi"]
        if not isinstance(url, str):
            raise TypeError("Locked PyPI package must have a URL string")
        if url.startswith("git+"):
            continue
        name, version = record.get("name"), record.get("version")
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(version, str)
            or not version
        ):
            raise ValueError("Locked PyPI package is missing its name or version")
        name = re.sub(r"[-_.]+", "-", name).lower()
        # Platform-specific wheels can repeat a package/version in the same lockfile.
        packages[(name, version)] = {
            "package": {"name": name, "version": version, "ecosystem": "PyPI"}
        }
    if not packages:
        raise ValueError("Pixi lockfile contains no scannable PyPI packages")
    return [packages[key] for key in sorted(packages)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lockfiles", nargs="+", type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    args = parser.parse_args()
    for lockfile in args.lockfiles:
        packages = collect_packages(yaml.safe_load(lockfile.read_text()))
        inventory = {
            "results": [
                {
                    "source": {"path": str(lockfile), "type": "lockfile"},
                    "packages": packages,
                }
            ]
        }
        output = args.output_directory / lockfile.parent.name / "osv-scanner.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(inventory, indent=2) + "\n")
        print(f"Exported {len(packages)} PyPI packages from {lockfile} to {output}")


if __name__ == "__main__":
    main()
