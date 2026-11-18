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

"""Guard the driver's own System.Text.RegularExpressions pin.

GHSA-cmhx-cq75-c4mj (CVE-2019-0820) is ignored globally in osv-scanner.toml for
the submodule lockfile this repo cannot change. Because the OSV ignore applies to
the advisory ID across every scanned lockfile, a regression of the driver's own
resolved version back to the vulnerable 4.3.0 would pass the scan silently. This
check re-asserts, independently of OSV, that the driver lockfile keeps the fix.
"""

import json
import sys

PACKAGE = "System.Text.RegularExpressions"
MINIMUM = (4, 3, 1)
LOCKFILE = "csharp/src/packages.lock.json"


def parse_version(value):
    """Parse a NuGet version string into a comparable tuple of leading integers."""
    parts = []
    for token in str(value).split("+", 1)[0].split("-", 1)[0].split("."):
        if token.isdigit():
            parts.append(int(token))
        else:
            break
    return tuple(parts)


def check_dependencies(data, package=PACKAGE, minimum=MINIMUM):
    """Return a list of error strings for a parsed packages.lock.json document.

    Flags any framework whose resolved version is below ``minimum`` and, if the
    package is absent everywhere, flags that the pin may have been removed.
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
    if not found:
        errors.append(
            f"{package} not found in any framework; the direct pin may have been "
            "removed, re-exposing CVE-2019-0820 via the transitive 4.3.0."
        )
    return errors


def main():
    try:
        with open(LOCKFILE, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError) as exc:
        print(f"::error::Cannot read {LOCKFILE}: {exc}")
        return 1

    errors = check_dependencies(data)
    if errors:
        for error in errors:
            print(f"::error::{LOCKFILE}: {error}")
        return 1

    print(f"{LOCKFILE}: {PACKAGE} is pinned at or above {'.'.join(map(str, MINIMUM))}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
