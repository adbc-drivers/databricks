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

//! End-to-end TLS configuration tests for the Databricks ADBC driver.
//!
//! These tests verify that TLS configuration parameters work correctly by
//! routing traffic through mitmproxy (a TLS-intercepting proxy). This exercises:
//!
//! - `databricks.http.tls.trusted_certificate_path` — custom CA cert
//! - `databricks.http.tls.allow_self_signed` — accept self-signed certs
//! - Negative case: connection fails without TLS config when proxy intercepts TLS
//!
//! Traffic routing through mitmproxy is verified via mitmdump logs in CI.
//!
//! # Running Locally
//!
//! 1. Start mitmproxy in regular (intercepting) mode on port 8080:
//!    ```bash
//!    docker run -d --name mitmproxy -p 8080:8080 \
//!      mitmproxy/mitmproxy:latest mitmdump
//!    ```
//!
//! 2. Extract the mitmproxy CA certificate:
//!    ```bash
//!    # Wait a moment for mitmproxy to generate certs on first start
//!    sleep 2
//!    docker cp mitmproxy:/home/mitmproxy/.mitmproxy/mitmproxy-ca-cert.pem /tmp/mitmproxy-ca-cert.pem
//!    export MITMPROXY_CA_CERT=/tmp/mitmproxy-ca-cert.pem
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
//!    cargo test --test tls_e2e -- --ignored --nocapture
//!    ```

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection as _, Driver as _, Optionable, Statement as _};
use databricks_adbc::Driver;

/// Helper function to get required environment variable with clear error message.
fn get_env_var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!(
            "Environment variable {} is required for TLS E2E tests. \
             See test file documentation for setup instructions.",
            name
        )
    })
}

/// Helper to create a configured database routed through mitmproxy.
///
/// All traffic goes through mitmproxy on port 8080 so that TLS interception
/// occurs. TLS-specific options are configured by the individual tests.
fn create_database_with_mitmproxy() -> databricks_adbc::Database {
    let host = get_env_var("DATABRICKS_HOST");
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

    // Route all traffic through mitmproxy
    database
        .set_option(
            OptionDatabase::Other("databricks.http.proxy.url".into()),
            OptionValue::String("http://localhost:8080".into()),
        )
        .expect("Failed to set proxy url");

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

/// Test that connection through mitmproxy fails without TLS configuration.
///
/// mitmproxy intercepts TLS and presents its own certificate. Without trusting
/// mitmproxy's CA cert or disabling validation, the connection should fail.
#[test]
#[ignore]
fn test_tls_fails_without_config() {
    let database = create_database_with_mitmproxy();

    use adbc_core::Database as _;
    let result = database.new_connection();

    assert!(
        result.is_err(),
        "Connection should fail when TLS-intercepting proxy is used without TLS config"
    );
}

/// Test connection with a trusted custom CA certificate.
///
/// Configures `trusted_certificate_path` to mitmproxy's CA cert, allowing the
/// driver to trust the intercepted TLS connection.
/// CI verifies traffic routing via mitmdump logs.
#[test]
#[ignore]
fn test_tls_with_trusted_certificate() {
    let ca_cert_path = get_env_var("MITMPROXY_CA_CERT");

    let mut database = create_database_with_mitmproxy();
    database
        .set_option(
            OptionDatabase::Other("databricks.http.tls.trusted_certificate_path".into()),
            OptionValue::String(ca_cert_path),
        )
        .expect("Failed to set trusted_certificate_path");

    execute_select_1(&database);
}

/// Test connection with `allow_self_signed`.
///
/// Disables certificate validation via `danger_accept_invalid_certs`, allowing
/// connection through mitmproxy without trusting its CA cert.
/// CI verifies traffic routing via mitmdump logs.
#[test]
#[ignore]
fn test_tls_with_allow_self_signed() {
    let mut database = create_database_with_mitmproxy();
    database
        .set_option(
            OptionDatabase::Other("databricks.http.tls.allow_self_signed".into()),
            OptionValue::String("true".into()),
        )
        .expect("Failed to set allow_self_signed");

    execute_select_1(&database);
}
