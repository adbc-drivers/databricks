<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Changelog

All notable changes to the C# Databricks ADBC driver are documented in this file.

## [1.1.2] - Unreleased

### Added

- Add straggler download detection and mitigation for CloudFetch (#183)
- Add statement-level query tags support for SEA protocol (#434)

### Fixed

- Align INTERVAL and complex type columns to strings on SEA path (#440)
- Catch object-not-found exceptions in SEA metadata (#388)
- Return -1 for unknown affected rows in SEA `ExecuteUpdate` (#427)
- Make telemetry init resilient and consolidate auth-type derivation (#421)
- Populate poll metrics in connection telemetry (#420)
- Populate `async_poll_interval_millis` in connection telemetry (#419)
- Populate discovery telemetry fields (#438)
- Add transient transport exceptions for quick failure detection (#435)
- Default `use_desc_table_extended` to `true` for SEA/REST protocol (#425)
- Use `ADBCDatabricksDriver` user agent for REST/SEA path (#407)
- Emit upper-case `char_set_encoding` to match other drivers (#415)
- Mark `ApplyServerSidePropertiesAsync` statements as internal calls (#417)

## [1.1.1] - Unreleased

### Fixed

- Set telemetry `is_compressed` and `execution_result` from actual result, not connection capability (#402)
- Populate `process_name` telemetry with entry assembly name (#403)
- Use `next_chunk_index` from last ExternalLink for SEA CloudFetch navigation (#404)
- Report distro `PRETTY_NAME` instead of kernel version in `os_version` telemetry (#399)
- Emit bare hostname for telemetry `host_url` to match JDBC (#401)
- Report OAuth U2M access-token passthrough as `auth_mech=OAUTH` (#396)
- Add `scope=sql` to `RefreshTokenAsync` for AAD service principal tokens (#389)
- Retry telemetry 429/503 via `EnsureSuccessOrThrow` (#393)
- Populate `driver_connection_params.http_path` in telemetry (#392, #391)

### Changed

- Emit individual span per `PollOperationStatus` poll (#390)
- Sync with updated hiveserver2 for assembly version updates (#406)

## [1.1.0] - 2025-04-11

Initial public release of the C# Databricks ADBC driver.
