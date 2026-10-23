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

"""Export locked Pixi packages for OSV and Conda SBOM generation."""

import argparse
import hashlib
import json
import re
from pathlib import Path
from urllib.parse import urlsplit

import yaml


def locked_records(lock):
    if not isinstance(lock, dict) or lock.get("version") not in (6, 7):
        raise ValueError("Expected a Pixi v6 or v7 lockfile")
    if not isinstance(lock.get("packages"), list):
        raise TypeError("Pixi lockfile must contain a packages array")
    return lock["packages"]


def collect_packages(lock):
    packages = {}
    for record in locked_records(lock):
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


def collect_conda_packages(lock):
    packages = {}
    for record in locked_records(lock):
        if "conda" not in record:
            continue
        url = record["conda"]
        if not isinstance(url, str):
            raise TypeError("Conda source dependencies cannot be inventoried")
        parsed = urlsplit(url)
        filename = parsed.path.rsplit("/", 1)[-1]
        suffix = next(
            (suffix for suffix in (".conda", ".tar.bz2") if filename.endswith(suffix)),
            None,
        )
        if not suffix or parsed.scheme != "https" or not parsed.netloc:
            raise ValueError(f"Unsupported Conda artifact URL: {url}")
        identity = filename[: -len(suffix)].rsplit("-", 2)
        if len(identity) != 3 or not all(identity):
            raise ValueError(f"Invalid Conda artifact filename: {filename}")
        name, version, build = identity
        channel, subdir, _filename = url.rsplit("/", 2)
        package = {
            **{key: value for key, value in record.items() if key != "conda"},
            "name": name,
            "version": version,
            "build": build,
            "channel": channel,
            "subdir": subdir,
            "fn": filename,
            "url": url,
        }
        packages[url] = package
    return [packages[url] for url in sorted(packages)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lockfiles", nargs="+", type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    args = parser.parse_args()
    for lockfile in args.lockfiles:
        lock = yaml.safe_load(lockfile.read_text())
        packages = collect_packages(lock)
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

        conda_packages = collect_conda_packages(lock)
        conda_directory = output.parent / "conda-meta"
        conda_directory.mkdir(exist_ok=True)
        for package in conda_packages:
            identity = hashlib.sha256(package["url"].encode()).hexdigest()
            (conda_directory / f"{identity}.json").write_text(
                json.dumps(package, indent=2) + "\n"
            )
        output.with_name("conda-inventory.json").write_text(
            json.dumps(
                {
                    "source": str(lockfile),
                    "packages": conda_packages,
                    "source_dependencies": [
                        {"name": record.get("name"), "url": record["pypi"]}
                        for record in locked_records(lock)
                        if str(record.get("pypi", "")).startswith("git+")
                    ],
                },
                indent=2,
            )
            + "\n"
        )
        print(f"Exported {len(conda_packages)} Conda artifacts from {lockfile}")


if __name__ == "__main__":
    main()
