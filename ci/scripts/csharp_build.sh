#!/usr/bin/env bash
#
# Copyright (c) 2025 ADBC Drivers Contributors
#
# This file has been modified from its original version, which is
# under the Apache License:
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

source_dir=${1}/csharp/src

pushd ${source_dir}
# NU1902 (CVE-2026-62900 / GHSA-23fw-v26w-5fgq): the .NET SDK's SourceLink build task
# Microsoft.Build.Tasks.Git is flagged by NuGet audit. It's a build-time-only dependency.
# The main project suppresses it via Directory.Build.props, but the referenced hiveserver2
# submodule project has its own Directory.Build.props and can't read that suppression, so
# demote NU1902 to a warning for the whole build here. Remove once the SDK bundles the
# patched Microsoft.Build.Tasks.Git 10.0.303+.
dotnet build AdbcDrivers.Databricks.csproj -p:WarningsNotAsErrors=NU1902
popd
