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

"""Tests for the driver System.Text.RegularExpressions pin guard."""

import unittest

import check_driver_regex_pin as guard


def _lockfile(*versions):
    return {
        "version": 1,
        "dependencies": {
            framework: {
                "System.Text.RegularExpressions": {
                    "type": "Transitive",
                    "resolved": version,
                }
            }
            for framework, version in versions
        },
    }


class ParseVersionTests(unittest.TestCase):
    def test_strips_prerelease_and_metadata(self):
        self.assertEqual(guard.parse_version("4.3.1"), (4, 3, 1))
        self.assertEqual(guard.parse_version("4.3.1-preview.1"), (4, 3, 1))
        self.assertEqual(guard.parse_version("4.3.1+build5"), (4, 3, 1))

    def test_ordering(self):
        self.assertLess(guard.parse_version("4.3.0"), guard.parse_version("4.3.1"))
        self.assertGreater(guard.parse_version("4.10.0"), guard.parse_version("4.3.1"))


class CheckDependenciesTests(unittest.TestCase):
    def test_fixed_version_passes(self):
        data = _lockfile(("netstandard2.0", "4.3.1"), ("net8.0", "4.3.1"))
        self.assertEqual(guard.check_dependencies(data), [])

    def test_higher_version_passes(self):
        self.assertEqual(
            guard.check_dependencies(_lockfile(("netstandard2.0", "5.0.0"))), []
        )

    def test_vulnerable_version_flagged(self):
        errors = guard.check_dependencies(_lockfile(("netstandard2.0", "4.3.0")))
        self.assertEqual(len(errors), 1)
        self.assertIn("4.3.0", errors[0])

    def test_one_vulnerable_framework_among_fixed(self):
        data = _lockfile(("netstandard2.0", "4.3.1"), ("net472", "4.3.0"))
        errors = guard.check_dependencies(data)
        self.assertEqual(len(errors), 1)
        self.assertIn("net472", errors[0])

    def test_missing_package_flagged(self):
        errors = guard.check_dependencies(
            {"version": 1, "dependencies": {"net8.0": {}}}
        )
        self.assertEqual(len(errors), 1)
        self.assertIn("not found", errors[0])


if __name__ == "__main__":
    unittest.main()
