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

"""Guard the C# System.Text.RegularExpressions pins.

GHSA-cmhx-cq75-c4mj (CVE-2019-0820) is ignored globally in osv-scanner.toml for
the submodule lockfile this repo cannot change. Because the OSV ignore applies to
the advisory ID across *every* scanned lockfile, a regression back to the
vulnerable 4.3.0 in any generated C# lockfile -- the driver, tests, benchmarks or
tools -- would pass the scan silently. This check re-asserts, independently of
OSV, that every generated C# lockfile keeps the fix, excluding only the exempt
arrow-adbc submodule lockfile where 4.3.0 legitimately remains (the case the OSV
ignore actually covers).
"""

import glob
import json
import os
import sys

PACKAGE = "System.Text.RegularExpressions"
MINIMUM = (4, 3, 1)

# The driver's own lockfile must carry the direct pin; absence here means the pin
# was removed and must be flagged.
DRIVER_LOCKFILE = "csharp/src/packages.lock.json"

# Every generated C# lockfile is scanned by OSV under the same global advisory
# ignore, so the pin must hold in all of them.
LOCKFILE_GLOB = "csharp/**/packages.lock.json"

# The arrow-adbc submodule lockfile is the one this repo cannot change and the one
# the OSV ignore exists for; skip it so its legitimate 4.3.0 is not flagged.
SUBMODULE_PREFIX = "csharp/hiveserver2/"


def parse_version(value):
    """Parse a NuGet version string into a comparable tuple of leading integers."""
    parts = []
    for token in str(value).split("+", 1)[0].split("-", 1)[0].split("."):
        if token.isdigit():
            parts.append(int(token))
        else:
            break
    return tuple(parts)


def check_dependencies(data, package=PACKAGE, minimum=MINIMUM, require_present=True):
    """Return a list of error strings for a parsed packages.lock.json document.

    Flags any framework whose resolved version is below ``minimum``. When
    ``require_present`` is set, also flags the package being absent everywhere --
    used for the driver lockfile, whose direct pin must stay present. Other
    lockfiles may legitimately not resolve the package at all, so absence there
    is not an error.
    """
    errors = []
    found = False
    target = package.lower()
    for framework, packages in (data.get("dependencies") or {}).items():
        for name, meta in (packages or {}).items():
            if name.lower() != target:
                continue
            resolved = (meta or {}).get("resolved")
            if not resolved:
                continue
            found = True
            if parse_version(resolved) < minimum:
                minimum_text = ".".join(str(part) for part in minimum)
                errors.append(
                    f"{framework}: {package} resolves {resolved}; "
                    f"CVE-2019-0820 requires >= {minimum_text}."
                )
    if require_present and not found:
        errors.append(
            f"{package} not found in any framework; the direct pin may have been "
            "removed, re-exposing CVE-2019-0820 via the transitive 4.3.0."
        )
    return errors


def discover_lockfiles(root="."):
    """Return the generated C# lockfiles to check, excluding the submodule one.

    Globs ``csharp/**/packages.lock.json`` relative to ``root`` and drops the
    arrow-adbc submodule lockfile, which this repo cannot change.
    """
    lockfiles = []
    for path in glob.glob(os.path.join(root, LOCKFILE_GLOB), recursive=True):
        normalized = os.path.relpath(path, root).replace(os.sep, "/")
        if normalized.startswith(SUBMODULE_PREFIX):
            continue
        lockfiles.append(normalized)
    return sorted(lockfiles)


def main():
    lockfiles = discover_lockfiles()
    if DRIVER_LOCKFILE not in lockfiles:
        print(
            f"::error::Required lockfile not found: {DRIVER_LOCKFILE}. "
            "Run 'dotnet restore --use-lock-file' before this check."
        )
        return 1

    minimum_text = ".".join(str(part) for part in MINIMUM)
    exit_code = 0
    for lockfile in lockfiles:
        try:
            with open(lockfile, encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, ValueError) as exc:
            print(f"::error::Cannot read {lockfile}: {exc}")
            exit_code = 1
            continue

        errors = check_dependencies(
            data, require_present=(lockfile == DRIVER_LOCKFILE)
        )
        if errors:
            for error in errors:
                print(f"::error::{lockfile}: {error}")
            exit_code = 1
        else:
            print(f"{lockfile}: {PACKAGE} is pinned at or above {minimum_text}.")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
