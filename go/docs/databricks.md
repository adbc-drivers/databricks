---
# Copyright (c) 2025 ADBC Drivers Contributors
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
{}
---

{{ cross_reference|safe }}
# Databricks Driver {{ version }}

{{ heading|safe }}

This driver provides access to [Databricks][databricks], a
cloud-based platform for data analytics.

:::{note}
This is an early version of the driver, and contributors are still
working with Databricks to expand the featureset and improve performance.
:::

## Installation

The Databricks driver can be installed with [dbc](https://docs.columnar.tech/dbc):

```bash
dbc install databricks
```

## Connecting

To connect, edit the `uri` option below to match your environment and run the following:

```python
from adbc_driver_manager import dbapi

conn = dbapi.connect(
  driver="databricks",
  db_kwargs = {
    "uri": "databricks://token:dapi1234abcd5678efgh@dbc-a1b2345c-d6e7.cloud.databricks.com:443/sql/protocolv1/o/1234567890123456/1234-567890-abcdefgh"
  }
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers.

### Connection String Format

Databricks's URI syntax supports three primary forms:

1. Databricks personal access token authentication:

   ```
   databricks://token:<personal-access-token>@<server-hostname>:<port-number>/<http-path>?<param1=value1>&<param2=value2>
   ```

   Components:
   - `scheme`: `databricks://` (required)
   - `<personal-access-token>`: (required) Databricks personal access token.
   - `<server-hostname>`: (required) Server Hostname value.
   - `port-number`: (required) Port value, which is typically 443.
   - `http-path`: (required) HTTP Path value.
   - Query params: Databricks connection attributes. For complete list of optional parameters, see [Databricks Optional Parameters](https://docs.databricks.com/dev-tools/go-sql-driver#optional-parameters)


2. OAuth user-to-machine (U2M) authentication:

   ```
   databricks://<server-hostname>:<port-number>/<http-path>?authType=OauthU2M&<param1=value1>&<param2=value2>
   ```

   Components:
   - `scheme`: `databricks://` (required)
   - `<server-hostname>`: (required) Server Hostname value.
   - `port-number`: (required) Port value, which is typically 443.
   - `http-path`: (required) HTTP Path value.
   - `authType=OauthU2M`: (required) Specifies OAuth user-to-machine authentication.
   - Query params: Additional Databricks connection attributes. For complete list of optional parameters, see [Databricks Optional Parameters](https://docs.databricks.com/dev-tools/go-sql-driver#optional-parameters)

3. OAuth machine-to-machine (M2M) authentication:

   ```
   databricks://<server-hostname>:<port-number>/<http-path>?authType=OAuthM2M&clientID=<client-id>&clientSecret=<client-secret>&<param1=value1>&<param2=value2>
   ```

   Components:
   - `scheme`: `databricks://` (required)
   - `<server-hostname>`: (required) Server Hostname value.
   - `port-number`: (required) Port value, which is typically 443.
   - `http-path`: (required) HTTP Path value.
   - `authType=OAuthM2M`: (required) Specifies OAuth machine-to-machine authentication.
   - `<client-id>`: (required) Service principal's UUID or Application ID value.
   - `<client-secret>`: (required) Secret value for the service principal's OAuth secret.
   - Query params: Additional Databricks connection attributes. For complete list of optional parameters, see [Databricks Optional Parameters](https://docs.databricks.com/dev-tools/go-sql-driver#optional-parameters)

This follows the [Databricks SQL Driver for Go](https://docs.databricks.com/dev-tools/go-sql-driver#connect-with-a-dsn-connection-string) format with the addition of the `databricks://` scheme.

:::{note}
Reserved characters in URI elements must be URI-encoded. For example, `@` becomes `%40`.
:::

Examples:

- `databricks://token:dapi1234abcd5678efgh@dbc-a1b2345c-d6e7.cloud.databricks.com:443/sql/protocolv1/o/1234567890123456/1234-567890-abcdefgh`
- `databricks://myworkspace.cloud.databricks.com:443/sql/1.0/warehouses/abc123def456?authType=OauthU2M`
- `databricks://myworkspace.cloud.databricks.com:443/sql/1.0/warehouses/abc123def456?authType=OAuthM2M&clientID=12345678-1234-1234-1234-123456789012&clientSecret=mysecret123`

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

## Compatibility

{{ compatibility_info|safe }}

## Previous Versions

To see documentation for previous versions of this driver, see the following:

- [v0.1.2](./v0.1.2.md)

{{ footnotes|safe }}

[databricks]: https://www.databricks.com
