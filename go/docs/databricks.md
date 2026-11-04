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

Several connections can be opened from one database at the same time. The first one starts the connection pool, including any login, and the others wait for it and share the pool. Changing a database option makes the next connection start a new pool. Setting or reading an option, and closing the database, wait for a start-up that is in progress.

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

The auth types, the options each one uses and the options it requires are also listed in machine-readable form in [`go/auth_contract.json`](../auth_contract.json).

`databricks.auth_type`
: **Values:** `pat`, `oauth-m2m`, `external-browser`, `env-oidc`, `file-oidc`, `azure-client-secret`. **Default:** (empty, inferred from the options that are set)

  How to authenticate.

  - `pat`: personal access token. Requires `databricks.access_token`.
  - `oauth-m2m`: OAuth machine-to-machine. Requires `databricks.oauth.client_id` and `databricks.oauth.client_secret`.
  - `external-browser`: OAuth user-to-machine (U2M) login through the system browser. The driver prints the login URL and tries to open it, then waits for the redirect on `localhost:8020`. The token is cached in `~/.cache/databricks/oauth` (`$XDG_CACHE_HOME/databricks/oauth` when set, `%LOCALAPPDATA%\databricks\oauth` on Windows), one private file per workspace (and per custom client ID or scope list), and refreshed with its refresh token, so a new process signs in again only when that fails. Processes that start together share one login: one opens the browser and the others wait (at least as long as the browser timeout) for the cached token. The cache and lease files use the same format as the databricks-sql-go fork, so both can share the directory. Optional: `databricks.oauth.client_id` (your own OAuth app; default: the built-in client of the cloud), `databricks.oauth.scopes`, `databricks.oauth.redirect_url` and `databricks.oauth.external_browser.timeout`. It is never inferred: set `databricks.auth_type` explicitly.
  - `env-oidc`: OIDC token federation. The ID token is read from the `DATABRICKS_OIDC_TOKEN` environment variable and exchanged for a Databricks token. Requires `databricks.oidc.client_id`.
  - `file-oidc`: OIDC token federation. The ID token is read from a file and exchanged for a Databricks token. Requires `databricks.oidc.client_id`.
  - `azure-client-secret`: Microsoft Entra ID service principal on Azure Databricks. Requires `databricks.azure.client_id`, `databricks.azure.client_secret` and `databricks.azure.tenant_id`.

  The methods backed by the Databricks SDK (`env-oidc`, `file-oidc`, `azure-client-secret`) also read `DATABRICKS_*` environment variables that belong to their own method (for example `DATABRICKS_OIDC_TOKEN`), but never `~/.databrickscfg`. Credentials of other methods in the environment, such as `DATABRICKS_TOKEN`, are ignored.

  If not set, the method is inferred: `pat` when `databricks.access_token` is set, `oauth-m2m` when a client ID or secret is set. Setting both an access token and OAuth options without `databricks.auth_type` is an error. If set, options that belong to other methods are ignored.

`databricks.oauth.redirect_url`
: **Values:** a loopback address with a port, such as `localhost:9000` or `http://localhost:9000/callback`. **Default:** `localhost:8020`

  Where `external-browser` login listens for the redirect. Use it when port 8020 is taken or your OAuth app registers another redirect. The same URL must be registered on the OAuth app.

`databricks.oauth.external_browser.timeout`
: **Values:** a positive Go duration such as `90s` or `2m`. **Default:** `1m`

  How long `external-browser` login waits for the sign-in to finish before it fails.

`databricks.azure.client_id`
: **Default:** (empty)

  The application (client) ID of the Entra ID service principal. Required for `azure-client-secret`.

`databricks.azure.client_secret`
: **Default:** (empty)

  The client secret of the Entra ID service principal. Required for `azure-client-secret`.

`databricks.azure.tenant_id`
: **Default:** (empty)

  The Entra ID tenant ID. Required for `azure-client-secret`.

`databricks.oauth.scopes`
: **Default:** (empty; `oauth-m2m` requests `all-apis`)

  The OAuth scopes `oauth-m2m` and `external-browser` request, separated by spaces or commas (for example `sql`). For `oauth-m2m`, set this when the service principal's secret is limited to scopes other than `all-apis`. For `external-browser`, `offline_access` and the cloud's SQL scope are always added, and nothing else by default; dbt-databricks v1 (`credentials.py`) defaults to `all-apis offline_access`, so a client that wants the same scopes must set this option.

`databricks.oidc.client_id`
: **Default:** (empty)

  The client ID of the Databricks service principal to log in as. Required for `env-oidc` and `file-oidc`.

`databricks.oidc.token_filepath`
: **Default:** (empty, the `DATABRICKS_OIDC_TOKEN_FILEPATH` environment variable is used)

  Path of the file holding the ID token. Used by `file-oidc` only.

### Session Parameter Options

`databricks.session_param.<name>`
: **Values:** a string. **Default:** (not set)

  Sets the Databricks SQL session parameter `<name>` (such as `ansi_mode` or `timezone`), for example `databricks.session_param.ansi_mode` set to `false`. The parameters are sent when the session opens, and are not applied when the connection is given as a `uri`. The driver does not check the name or value, and rejects an empty name with `INVALID_ARGUMENT`. `QUERY_TAGS` set this way is replaced by the tags set with the `databricks.query_tag.<name>` options.

### Query Tag Options

`databricks.query_tag.<name>`
: **Values:** a non-empty string. **Default:** (not set)

  Sets the query tag `<name>`, for example `databricks.query_tag.team`. Databricks records query tags in `system.query.history`. Set on the database, a tag is a default for every statement; set on a statement, it overrides the default of the same name for that statement only. Reading a tag that is not set returns `NOT_FOUND`.

  The name must not be empty or contain `,` or `:`. The value must not be empty and may be at most 128 characters after escaping (backslash, comma and colon each count twice), and at most 20 tags are sent with a statement, the limits dbt-databricks enforces. An invalid option is rejected with `INVALID_ARGUMENT` and the tags already set are left unchanged.

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
