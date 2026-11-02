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
: **Values:** `pat`, `oauth-m2m`, `env-oidc`, `file-oidc`, `azure-client-secret`, `azure-msi`, `azure-cli`, `google-credentials`, `google-id`. **Default:** (empty, inferred from the options that are set)

  How to authenticate.

  - `pat`: personal access token. Requires `databricks.access_token`.
  - `oauth-m2m`: OAuth machine-to-machine. Requires `databricks.oauth.client_id` and `databricks.oauth.client_secret`.
  - `env-oidc`: OIDC token federation. The ID token is read from the `DATABRICKS_OIDC_TOKEN` environment variable and exchanged for a Databricks token. Requires `databricks.oidc.client_id`.
  - `file-oidc`: OIDC token federation. The ID token is read from a file and exchanged for a Databricks token. Requires `databricks.oidc.client_id`.
  - `azure-client-secret`: Microsoft Entra ID service principal on Azure Databricks. Requires `databricks.azure.client_id`, `databricks.azure.client_secret` and `databricks.azure.tenant_id`.
  - `azure-msi`: Azure managed identity. `databricks.azure.client_id` is optional and selects a user-assigned identity.
  - `azure-cli`: the current Azure CLI (`az login`) session. Needs no options.
  - `google-credentials`: Google service account key. Requires `databricks.google.credentials`.
  - `google-id`: Google ID token of a service account, from the application default credentials of the environment. Requires `databricks.google.service_account`.

  The methods backed by the Databricks SDK (for example `env-oidc`, `file-oidc`, `azure-client-secret`, `azure-msi`, `azure-cli`, `google-credentials` and `google-id`) also read `DATABRICKS_*` environment variables that belong to their own method (for example `DATABRICKS_OIDC_TOKEN`), but never `~/.databrickscfg`. Credentials of other methods in the environment, such as `DATABRICKS_TOKEN`, are ignored.

  If not set, the method is inferred: `pat` when `databricks.access_token` is set, `oauth-m2m` when a client ID or secret is set. Setting both an access token and OAuth options without `databricks.auth_type` is an error. If set, options that belong to other methods are ignored.

`databricks.azure.client_id`
: **Default:** (empty)

  The application (client) ID of the Entra ID service principal. Required for `azure-client-secret`. For `azure-msi`, the optional client ID of a user-assigned managed identity.

`databricks.azure.client_secret`
: **Default:** (empty)

  The client secret of the Entra ID service principal. Required for `azure-client-secret`.

`databricks.azure.tenant_id`
: **Default:** (empty)

  The Entra ID tenant ID. Required for `azure-client-secret`; the driver does not discover it.

`databricks.google.credentials`
: **Default:** (empty)

  The Google service account key, as JSON or the path of a JSON file. Required for `google-credentials`.

`databricks.google.service_account`
: **Default:** (empty)

  The email of the Google service account to get an ID token for. Required for `google-id`.

`databricks.oauth.scopes`
: **Default:** (empty, requests `all-apis`)

  The OAuth scopes `oauth-m2m` requests, separated by spaces or commas (for example `sql`). Set this when the service principal's secret is limited to scopes other than `all-apis`.

`databricks.oidc.client_id`
: **Default:** (empty)

  The client ID of the Databricks service principal to log in as. Required for `env-oidc` and `file-oidc`.

`databricks.oidc.token_filepath`
: **Default:** (empty, the `DATABRICKS_OIDC_TOKEN_FILEPATH` environment variable is used)

  Path of the file holding the ID token. Used by `file-oidc` only.

### Query Tag Options

Query tags label the statements the driver runs. Databricks records them in `system.query.history` (the `query_tags` column). Tags are set one per option, by appending the tag name to the option prefix.

`databricks.query_tag.<name>`
: **Values:** a non-empty string. **Default:** (not set)

  Sets the query tag `<name>`, for example `databricks.query_tag.team` set to `data-platform`.

  - On the database, the tag is a default. The driver sends the defaults as the session-level `QUERY_TAGS` setting when it connects, and again with each statement.
  - On a statement, the tag adds to the defaults or, when a default has the same name, overrides it in the tags the driver sends with that statement. It applies to that statement only. A tag cannot be removed on a statement. How Databricks combines the session-level and statement-level tags on the server is not asserted here.
  - The tags are sent with statements run through `ExecuteQuery` and `ExecuteUpdate` (including bulk ingest). Calls the driver makes for metadata (such as `GetObjects`) carry only the session-level tags.
  - Reading the option on a statement returns the statement's value, or the connection default when the statement has none. Reading a tag that is not set returns `NOT_FOUND`.

  The name must not be empty or blank and must not contain `,` or `:` (the wire format is comma-separated `name:value` pairs and escapes only backslashes in names). The value must not be empty. A value may be at most 128 characters after escaping (the driver escapes backslash, comma and colon), and at most 20 tags may be sent with a statement. These are the limits dbt-databricks enforces in [`QueryTagsUtils`](https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/utils.py). An invalid option is rejected with `INVALID_ARGUMENT` and the tags already set are left unchanged.

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
