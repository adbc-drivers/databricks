#!/usr/bin/env bash
#
# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

# Run all tests (both E2E and Unit tests)
source_dir=${1}/csharp/test

pushd ${source_dir}
# Run all tests in the Databricks test project.
# TEST_FILTER (optional) is a VSTest --filter expression; when set, only matching
# tests run. Used by the Reyden nightly to exclude tests that require warehouse
# capabilities Lakehouse//RT does not support (DDL/DML, presigned-URL refresh).
if [ -n "${TEST_FILTER:-}" ]; then
  dotnet test --verbosity normal --filter "${TEST_FILTER}"
else
  dotnet test --verbosity normal
fi
popd
