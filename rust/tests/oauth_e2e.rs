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

//! End-to-end OAuth integration tests for M2M and U2M flows.
//!
//! These tests connect to a real Databricks workspace and verify the complete
//! OAuth implementation lifecycle. They are marked with `#[ignore]` to prevent
//! running in CI by default.
//!
//! # Running the Tests
//!
//! ## M2M (Service Principal) Test
//!
//! Set the following environment variables:
//!
//! ```bash
//! export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
//! export DATABRICKS_CLIENT_ID="your-service-principal-client-id"
//! export DATABRICKS_CLIENT_SECRET="your-service-principal-secret"
//! export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
//! ```
//!
//! Then run:
//!
//! ```bash
//! cargo test --test oauth_e2e test_m2m_end_to_end -- --ignored --nocapture
//! ```
//!
//! ## U2M (Browser-Based) Test
//!
//! Set the following environment variables:
//!
//! ```bash
//! export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
//! export DATABRICKS_CLIENT_ID="your-oauth-app-client-id"  # Optional, defaults to "databricks-cli"
//! export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
//! ```
//!
//! Then run:
//!
//! ```bash
//! cargo test --test oauth_e2e test_u2m_end_to_end -- --ignored --nocapture
//! ```
//!
//! **Note:** The U2M test will launch your default web browser for authentication.
//! You must complete the login flow in the browser window.
//!
//! # What These Tests Verify
//!
//! Both tests verify the complete connection lifecycle:
//! 1. Database configuration with OAuth settings
//! 2. OAuth provider creation
//! 3. Token acquisition (M2M: client credentials, U2M: browser flow)
//! 4. Successful API authentication
//! 5. Query execution via authenticated connection
//! 6. Result validation

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection as _, Database as _, Driver as _, Optionable, Statement as _};
use databricks_adbc::Driver;

/// Helper function to get required environment variable with clear error message.
fn get_env_var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!(
            "Environment variable {} is required for E2E tests. \
             See test file documentation for setup instructions.",
            name
        )
    })
}

/// Helper function to get optional environment variable with default value.
fn get_env_var_with_default(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// End-to-end test for M2M (Machine-to-Machine) OAuth flow.
///
/// This test verifies the complete M2M authentication lifecycle using a real
/// Databricks workspace and service principal credentials.
///
/// # Prerequisites
///
/// - A Databricks workspace
/// - A service principal with access to a SQL warehouse
/// - The following environment variables set:
///   - DATABRICKS_HOST: Workspace URL
///   - DATABRICKS_CLIENT_ID: Service principal client ID
///   - DATABRICKS_CLIENT_SECRET: Service principal secret
///   - DATABRICKS_WAREHOUSE_ID: SQL warehouse ID
///
/// # Test Steps
///
/// 1. Configure Database with OAuth M2M (auth.type=oauth_m2m)
/// 2. Create connection (triggers OIDC discovery and token exchange)
/// 3. Create statement and execute a simple query (SELECT 1)
/// 4. Verify results are correct
///
/// # Expected Results
///
/// - Connection succeeds with valid service principal credentials
/// - Query executes successfully
/// - Results contain expected data (single row with value 1)
#[test]
#[ignore] // Only run when explicitly requested (requires env vars)
fn test_m2m_end_to_end() {
    // Read credentials from environment
    let host = get_env_var("DATABRICKS_HOST");
    let client_id = get_env_var("DATABRICKS_CLIENT_ID");
    let client_secret = get_env_var("DATABRICKS_CLIENT_SECRET");
    let warehouse_id = get_env_var("DATABRICKS_WAREHOUSE_ID");

    println!("=== M2M E2E Test ===");
    println!("Host: {}", host);
    println!("Client ID: {}", client_id);
    println!("Warehouse ID: {}", warehouse_id);

    // Create driver and database
    let mut driver = Driver::new();
    let mut database = driver.new_database().expect("Failed to create database");

    // Configure database with M2M OAuth
    database
        .set_option(OptionDatabase::Uri, OptionValue::String(host))
        .expect("Failed to set URI");

    database
        .set_option(
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String(warehouse_id),
        )
        .expect("Failed to set warehouse_id");

    // Set auth type to oauth_m2m
    database
        .set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("oauth_m2m".into()),
        )
        .expect("Failed to set auth type");

    // Set OAuth credentials
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

    // Optional: Set scopes (defaults to "all-apis" for M2M)
    database
        .set_option(
            OptionDatabase::Other("databricks.auth.scopes".into()),
            OptionValue::String("all-apis".into()),
        )
        .expect("Failed to set scopes");

    println!("\nCreating connection (will trigger OIDC discovery and token exchange)...");

    // Create connection - this triggers:
    // 1. OIDC endpoint discovery
    // 2. Client credentials token exchange
    // 3. Connection establishment
    let mut connection = database
        .new_connection()
        .expect("Failed to create connection with M2M OAuth");

    println!("✓ Connection created successfully");

    // Create statement
    let mut statement = connection
        .new_statement()
        .expect("Failed to create statement");

    println!("\nExecuting test query: SELECT 1 as test_value");

    // Execute a simple query to verify authentication works
    statement
        .set_sql_query("SELECT 1 as test_value")
        .expect("Failed to set query");

    let mut reader = statement.execute().expect("Failed to execute query");

    println!("✓ Query executed successfully");

    // Verify results
    let batch = reader
        .next()
        .expect("No batch returned")
        .expect("Batch error");

    println!("\nVerifying results...");
    assert_eq!(batch.num_columns(), 1, "Expected 1 column");
    assert!(batch.num_rows() > 0, "Expected at least 1 row");

    // Verify the value is 1
    use arrow_array::Int32Array;
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Column is not Int32Array");
    assert_eq!(column.value(0), 1, "Expected value 1");

    println!("✓ Results verified: got expected value 1");
    println!("\n=== M2M E2E Test PASSED ===");
}

/// End-to-end test for U2M (User-to-Machine) OAuth flow.
///
/// This test verifies the complete U2M authentication lifecycle using a real
/// Databricks workspace and interactive browser authentication.
///
/// **⚠️  IMPORTANT:** This test requires manual interaction! It will launch your
/// default web browser and you must complete the login flow.
///
/// # Prerequisites
///
/// - A Databricks workspace
/// - A user account with access to a SQL warehouse
/// - The following environment variables set:
///   - DATABRICKS_HOST: Workspace URL
///   - DATABRICKS_WAREHOUSE_ID: SQL warehouse ID
///   - DATABRICKS_CLIENT_ID: (Optional) OAuth client ID, defaults to "databricks-cli"
///
/// # Test Steps
///
/// 1. Configure Database with OAuth U2M (auth.type=oauth_u2m)
/// 2. Create connection (triggers OIDC discovery and browser launch)
/// 3. **USER ACTION REQUIRED:** Complete login in browser window
/// 4. Create statement and execute a simple query (SELECT 1)
/// 5. Verify results are correct
///
/// # Expected Results
///
/// - Browser launches with Databricks login page
/// - User completes authentication
/// - Connection succeeds after OAuth callback
/// - Query executes successfully
/// - Results contain expected data (single row with value 1)
/// - Token is cached to ~/.config/databricks-adbc/oauth/ for future use
#[test]
#[ignore] // Always ignored - requires manual browser interaction
fn test_u2m_end_to_end() {
    // Read credentials from environment
    let host = get_env_var("DATABRICKS_HOST");
    let warehouse_id = get_env_var("DATABRICKS_WAREHOUSE_ID");
    // U2M uses "databricks-cli" as default client_id if not specified
    let client_id = get_env_var_with_default("DATABRICKS_CLIENT_ID", "databricks-cli");

    println!("=== U2M E2E Test ===");
    println!("Host: {}", host);
    println!("Client ID: {}", client_id);
    println!("Warehouse ID: {}", warehouse_id);
    println!("\n⚠️  MANUAL TEST: This will launch your browser for authentication!");
    println!("    You must complete the login flow in the browser window.\n");

    // Create driver and database
    let mut driver = Driver::new();
    let mut database = driver.new_database().expect("Failed to create database");

    // Configure database with U2M OAuth
    database
        .set_option(OptionDatabase::Uri, OptionValue::String(host))
        .expect("Failed to set URI");

    database
        .set_option(
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String(warehouse_id),
        )
        .expect("Failed to set warehouse_id");

    // Set auth type to oauth_u2m
    database
        .set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("oauth_u2m".into()),
        )
        .expect("Failed to set auth type");

    // Set OAuth client ID (optional, defaults to "databricks-cli")
    database
        .set_option(
            OptionDatabase::Other("databricks.auth.client_id".into()),
            OptionValue::String(client_id),
        )
        .expect("Failed to set client_id");

    // Optional: Set scopes (defaults to "all-apis offline_access" for U2M)
    database
        .set_option(
            OptionDatabase::Other("databricks.auth.scopes".into()),
            OptionValue::String("all-apis offline_access".into()),
        )
        .expect("Failed to set scopes");

    // Optional: Set callback port (use 8021 to avoid conflicts with port 8020)
    database
        .set_option(
            OptionDatabase::Other("databricks.auth.redirect_port".into()),
            OptionValue::String("8021".into()),
        )
        .expect("Failed to set redirect_port");

    println!("\nCreating connection...");
    println!("This will:");
    println!("  1. Discover OIDC endpoints");
    println!("  2. Check for cached token");
    println!("  3. Launch browser if no valid cached token exists");
    println!("  4. Wait for you to complete authentication");
    println!("\nWaiting for connection...\n");

    // Create connection - this triggers:
    // 1. OIDC endpoint discovery
    // 2. Token cache check
    // 3. Browser launch (if no cached token or refresh token expired)
    // 4. Callback server listening on localhost:8020
    // 5. Authorization code exchange after user authenticates
    let mut connection = database
        .new_connection()
        .expect("Failed to create connection with U2M OAuth");

    println!("\n✓ Connection created successfully");

    // Create statement
    let mut statement = connection
        .new_statement()
        .expect("Failed to create statement");

    println!("\nExecuting test query: SELECT 1 as test_value");

    // Execute a simple query to verify authentication works
    statement
        .set_sql_query("SELECT 1 as test_value")
        .expect("Failed to set query");

    let mut reader = statement.execute().expect("Failed to execute query");

    println!("✓ Query executed successfully");

    // Verify results
    let batch = reader
        .next()
        .expect("No batch returned")
        .expect("Batch error");

    println!("\nVerifying results...");
    assert_eq!(batch.num_columns(), 1, "Expected 1 column");
    assert!(batch.num_rows() > 0, "Expected at least 1 row");

    // Verify the value is 1
    use arrow_array::Int32Array;
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Column is not Int32Array");
    assert_eq!(column.value(0), 1, "Expected value 1");

    println!("✓ Results verified: got expected value 1");
    println!("\n💾 Token cached to ~/.config/databricks-adbc/oauth/");
    println!("   Next run will use cached token (no browser needed)");
    println!("\n=== U2M E2E Test PASSED ===");
}

/// Helper test to verify OAuth configuration validation.
///
/// This test verifies that proper error messages are returned when
/// required OAuth configuration is missing or invalid.
#[test]
fn test_oauth_config_validation() {
    let mut driver = Driver::new();

    // Test 1: Missing client_secret for M2M
    {
        let mut database = driver.new_database().expect("Failed to create database");
        database
            .set_option(
                OptionDatabase::Uri,
                OptionValue::String("https://test.databricks.com".into()),
            )
            .expect("Failed to set URI");
        database
            .set_option(
                OptionDatabase::Other("databricks.warehouse_id".into()),
                OptionValue::String("test-warehouse".into()),
            )
            .expect("Failed to set warehouse_id");
        database
            .set_option(
                OptionDatabase::Other("databricks.auth.type".into()),
                OptionValue::String("oauth_m2m".into()),
            )
            .expect("Failed to set auth type");
        database
            .set_option(
                OptionDatabase::Other("databricks.auth.client_id".into()),
                OptionValue::String("test-client-id".into()),
            )
            .expect("Failed to set client_id");
        // Missing client_secret - should fail
        let result = database.new_connection();
        assert!(
            result.is_err(),
            "Should fail when oauth_m2m is set without client_secret"
        );
    }

    // Test 2: Invalid auth type value
    {
        let mut database = driver.new_database().expect("Failed to create database");
        let result = database.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("invalid".into()),
        );
        assert!(
            result.is_err(),
            "Should fail when invalid auth type value is set"
        );
    }

    // Test 3: Missing auth type
    {
        let mut database = driver.new_database().expect("Failed to create database");
        database
            .set_option(
                OptionDatabase::Uri,
                OptionValue::String("https://test.databricks.com".into()),
            )
            .expect("Failed to set URI");
        database
            .set_option(
                OptionDatabase::Other("databricks.warehouse_id".into()),
                OptionValue::String("test-warehouse".into()),
            )
            .expect("Failed to set warehouse_id");
        let result = database.new_connection();
        assert!(result.is_err(), "Should fail when auth type is not set");
    }
}
