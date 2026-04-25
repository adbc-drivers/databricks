# Changelog

All notable changes to the C# Databricks ADBC driver are documented in this file.

## [1.1.1] - Unreleased

### Fixed

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
