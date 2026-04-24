/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Xunit;

// E2E tests run against a single shared Databricks workspace and share fixture
// tables under main.adbc_testing. Cross-class parallelism races on these
// tables (e.g. one class DROPs+CREATEs while another reads), causing flaky
// merge-queue failures. Serialize the whole assembly to avoid the races.
// Within-class test order is already sequential by xUnit default.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
