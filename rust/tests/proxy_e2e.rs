// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! End-to-end proxy tests for the Databricks ADBC driver.
//!
//! These tests verify that HTTP requests are correctly routed through a proxy.
//! They require a running Squid proxy and a real Databricks workspace.
//! Proxy routing is verified via Squid access logs in CI (see rust-e2e.yml).
//!
//! # Running Locally
//!
//! 1. Start an unauthenticated Squid proxy on port 3128:
//!    ```bash
//!    docker run -d --name squid -p 3128:3128 ubuntu/squid:latest
//!    ```
//!
//! 2. Start an authenticated Squid proxy on port 3129 (using ci/proxy configs):
//!    ```bash
//!    docker run -d --name squid-auth -p 3129:3128 \
//!      -v $(pwd)/ci/proxy/squid-auth.conf:/etc/squid/squid.conf \
//!      -v $(pwd)/ci/proxy/htpasswd:/etc/squid/htpasswd \
//!      ubuntu/squid:latest
//!    ```
//!
//! 3. Set environment variables (or source ~/.databricks/dogfood-creds):
//!    ```bash
//!    export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
//!    export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
//!    # Either PAT auth:
//!    export DATABRICKS_TOKEN="your-pat-token"
//!    # Or OAuth M2M auth (used in CI):
//!    export DATABRICKS_TEST_CLIENT_ID="your-client-id"
//!    export DATABRICKS_TEST_CLIENT_SECRET="your-client-secret"
//!    ```
//!
//! 4. Run the tests:
//!    ```bash
//!    cargo test --test proxy_e2e -- --ignored --nocapture
//!    ```

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection as _, Driver as _, Optionable, Statement as _};
use databricks_adbc::Driver;

/// Helper function to get required environment variable with clear error message.
fn get_env_var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!(
            "Environment variable {} is required for proxy E2E tests. \
             See test file documentation for setup instructions.",
            name
        )
    })
}

/// Helper to create a configured database with proxy settings.
///
/// Supports both PAT auth (via DATABRICKS_TOKEN) and OAuth M2M auth
/// (via DATABRICKS_TEST_CLIENT_ID + DATABRICKS_TEST_CLIENT_SECRET).
/// OAuth M2M is preferred if both are set (this is what CI uses).
fn create_database_with_proxy(
    proxy_url: &str,
    proxy_username: Option<&str>,
    proxy_password: Option<&str>,
    bypass_hosts: Option<&str>,
) -> databricks_adbc::Database {
    let host = get_env_var("DATABRICKS_HOST");
    // CI secret may not include the scheme — ensure it's a full URL
    let host = if host.starts_with("https://") || host.starts_with("http://") {
        host
    } else {
        format!("https://{}", host)
    };
    let http_path = get_env_var("DATABRICKS_HTTP_PATH");

    let mut driver = Driver::new();
    let mut database = driver.new_database().expect("Failed to create database");

    database
        .set_option(OptionDatabase::Uri, OptionValue::String(host))
        .expect("Failed to set uri");
    database
        .set_option(
            OptionDatabase::Other("databricks.http_path".into()),
            OptionValue::String(http_path),
        )
        .expect("Failed to set http_path");

    // Use OAuth M2M if client credentials are available (CI), otherwise PAT
    if let (Ok(client_id), Ok(client_secret)) = (
        std::env::var("DATABRICKS_TEST_CLIENT_ID"),
        std::env::var("DATABRICKS_TEST_CLIENT_SECRET"),
    ) {
        database
            .set_option(
                OptionDatabase::Other("databricks.auth.type".into()),
                OptionValue::String("oauth_m2m".into()),
            )
            .expect("Failed to set auth type");
        database
            .set_option(
                OptionDatabase::Other("databricks.auth.client_id".into()),
                OptionValue::String(client_id),
            )
            .expect("Failed to set client_id");
        database
            .set_option(
                OptionDatabase::Other("databricks.auth.client_secret".into()),
                OptionValue::String(client_secret),
            )
            .expect("Failed to set client_secret");
    } else {
        let token = get_env_var("DATABRICKS_TOKEN");
        database
            .set_option(
                OptionDatabase::Other("databricks.access_token".into()),
                OptionValue::String(token),
            )
            .expect("Failed to set access_token");
    }

    // Proxy configuration
    database
        .set_option(
            OptionDatabase::Other("databricks.http.proxy.url".into()),
            OptionValue::String(proxy_url.to_string()),
        )
        .expect("Failed to set proxy url");

    if let Some(username) = proxy_username {
        database
            .set_option(
                OptionDatabase::Other("databricks.http.proxy.username".into()),
                OptionValue::String(username.to_string()),
            )
            .expect("Failed to set proxy username");
    }
    if let Some(password) = proxy_password {
        database
            .set_option(
                OptionDatabase::Other("databricks.http.proxy.password".into()),
                OptionValue::String(password.to_string()),
            )
            .expect("Failed to set proxy password");
    }
    if let Some(bypass) = bypass_hosts {
        database
            .set_option(
                OptionDatabase::Other("databricks.http.proxy.bypass_hosts".into()),
                OptionValue::String(bypass.to_string()),
            )
            .expect("Failed to set proxy bypass_hosts");
    }

    database
}

/// Execute a simple query and verify it succeeds.
fn execute_select_1(database: &databricks_adbc::Database) {
    use adbc_core::Database as _;

    let mut connection = database
        .new_connection()
        .expect("Failed to create connection");
    let mut statement = connection
        .new_statement()
        .expect("Failed to create statement");
    statement
        .set_sql_query("SELECT 1 AS test_col")
        .expect("Failed to set query");

    let mut reader = statement.execute().expect("Failed to execute query");

    let batch = reader
        .next()
        .expect("No batch returned")
        .expect("Batch error");
    assert!(batch.num_rows() > 0, "Expected at least one row");
}

/// Test connection through an unauthenticated proxy.
///
/// CI verifies proxy routing via Squid access log (CONNECT entry for Databricks host).
#[test]
#[ignore]
fn test_connection_through_proxy() {
    let database = create_database_with_proxy("http://localhost:3128", None, None, None);
    execute_select_1(&database);
}

/// Test connection through an authenticated proxy (basic auth).
///
/// Uses the test credentials from ci/proxy/htpasswd (testuser:testpass).
/// CI verifies proxy routing via Squid access log.
#[test]
#[ignore]
fn test_connection_through_authenticated_proxy() {
    let database = create_database_with_proxy(
        "http://localhost:3129",
        Some("testuser"),
        Some("testpass"),
        None,
    );
    execute_select_1(&database);
}

/// Test that bypass_hosts causes traffic to skip the proxy.
///
/// Configures a proxy but adds the Databricks host to bypass_hosts.
/// The connection should succeed (going direct) and the Squid access log
/// should NOT contain a CONNECT entry for the Databricks host.
#[test]
#[ignore]
fn test_proxy_bypass_hosts() {
    let host = get_env_var("DATABRICKS_HOST");
    // Extract hostname without scheme for bypass list
    let hostname = host
        .strip_prefix("https://")
        .or_else(|| host.strip_prefix("http://"))
        .unwrap_or(&host);

    let database = create_database_with_proxy("http://localhost:3128", None, None, Some(hostname));
    execute_select_1(&database);
}
