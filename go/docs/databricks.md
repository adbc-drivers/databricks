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
    db_kwargs={
        "uri": "databricks://token:dapi1234abcd5678efgh@dbc-a1b2345c-d6e7.cloud.databricks.com:443/sql/protocolv1/o/1234567890123456/1234-567890-abcdefgh"
    },
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

## Options

### Authentication Options

`databricks.auth_type`
: **Values:** `pat`, `oauth-m2m`, `external-browser`, `oidc`, `azure-client-secret`. **Default:** (empty, inferred from the options that are set)

  How to authenticate.

  - `pat`: personal access token. Requires `databricks.access_token`.
  - `oauth-m2m`: OAuth machine-to-machine. Requires `databricks.oauth.client_id` and `databricks.oauth.client_secret`.
  - `external-browser`: OAuth user-to-machine (U2M) login through the system browser. The driver prints the login URL to stderr and tries to open it, then waits for the redirect on `localhost:8020` (or on `databricks.oauth.redirect_url` when set). Optional: `databricks.oauth.client_id` (your own OAuth app; default: the built-in client of the cloud), `databricks.oauth.scopes`, `databricks.oauth.redirect_url` and `databricks.oauth.external_browser.timeout`. It is never inferred: set `databricks.auth_type` explicitly. Logins in one process take turns. A login in another process fails if it needs the same redirect port while that one is in use. The redirect port is fixed, so only one browser login can run at a time on a machine.
  - `oidc`: OIDC token federation. The ID token is exchanged for a Databricks token. It is read from the file named by `databricks.oidc.token_filepath` if that is set, and otherwise from the `DATABRICKS_OIDC_TOKEN` environment variable (or the variable named by `DATABRICKS_OIDC_TOKEN_ENV`). Requires `databricks.oidc.client_id`.
  - `azure-client-secret`: Microsoft Entra ID service principal on Azure Databricks. Requires `databricks.azure.client_id`, `databricks.azure.client_secret` and `databricks.azure.tenant_id`.

  If not set, the method is inferred: `pat` when `databricks.access_token` is set, `oauth-m2m` when a client ID or secret is set. Setting both an access token and OAuth options without `databricks.auth_type` is an error. If set, options that belong to other methods are ignored.

`databricks.oauth.scopes`
: **Values:** space- or comma-separated scope names, such as `sql` or `sql offline_access`. **Default:** (empty, requests `all-apis`)

  The scopes `oauth-m2m` and `external-browser` request. For `oauth-m2m`, set it when the service principal is limited to specific scopes and cannot log in to `all-apis`. For `external-browser`, `offline_access` and the cloud's SQL scope are always added.

`databricks.oauth.redirect_url`
: **Values:** a loopback address with a port, such as `localhost:9000` or `http://localhost:9000/callback`. **Default:** `localhost:8020`

  Where `external-browser` login listens for the redirect. Use it when port 8020 is taken or your OAuth app registers another redirect. The same URL must be registered on the OAuth app.

`databricks.oauth.external_browser.timeout`
: **Values:** a positive Go duration such as `90s` or `2m`. **Default:** `1m`

  How long `external-browser` login waits for the sign-in to finish before it fails.

`databricks.oidc.client_id`
: **Default:** (empty)

  The client ID of the Databricks service principal to log in as. Required for `oidc`.

`databricks.oidc.token_filepath`
: **Default:** (empty, the ID token is read from the environment)

  Path of the file holding the ID token. Used by `oidc`.

`databricks.azure.client_id`
: **Default:** (empty)

  The application (client) ID of the Entra ID service principal. Required for `azure-client-secret`.

`databricks.azure.client_secret`
: **Default:** (empty)

  The client secret of the Entra ID service principal. Required for `azure-client-secret`.

`databricks.azure.tenant_id`
: **Default:** (empty)

  The Entra ID tenant ID. Required for `azure-client-secret`; the driver does not discover it.

### Query Options

`databricks.query.query_tag.<name>`
: **Values:** a string, which may be empty. **Default:** (not set)

  Sets the query tag `<name>`, for example `databricks.query.query_tag.team`. May be set at the database, connection or statement level, and a tag set at a more specific level overrides one of the same name set at a less specific level. Reading a tag that is not set returns `NOT_FOUND`.

  Names and values may be at most 128 characters, counted in UTF-16 code units. Names must not be empty or contain `,` `:` `-` `/` `=` or `.`. At most 20 tags are allowed. A tag that breaks these limits is rejected with `INVALID_ARGUMENT`. Names starting with `@@` are reserved by Databricks and passed through unchanged.

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
