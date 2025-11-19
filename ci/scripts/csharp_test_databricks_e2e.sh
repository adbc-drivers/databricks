#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -ex

# Run Databricks driver E2E tests
source_dir=${1}/csharp/test

pushd ${source_dir}
# TEMPORARY: Run only CloudFetch tests to investigate timing discrepancy
# Between local (70s for 1M rows) and CI (2-4s for 1M rows)
# See: https://github.com/adbc-drivers/databricks/pull/49#issuecomment
echo "=== Running CloudFetch E2E Tests Only ==="
echo "Purpose: Investigate why CloudFetch tests run 18-36x faster in CI than locally"
echo "Expected: 1M rows should take ~60-70s, not 2-4s"
echo ""
dotnet test --filter "FullyQualifiedName~CloudFetchE2ETest" --verbosity detailed
popd
