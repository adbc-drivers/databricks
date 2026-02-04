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

//! Integration tests for the Databricks ADBC driver.
//!
//! This module contains:
//! - Basic driver/database/connection tests (run without Databricks connection)
//! - End-to-end tests requiring real Databricks connection (marked #[ignore])
//! - Metadata tests in the `integration/metadata_tests` submodule

// Include the metadata tests submodule
mod metadata;

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Database as _;
use adbc_core::Driver as _;
use adbc_core::Optionable;
use databricks_adbc::Driver;

#[test]
fn test_database_configuration() {
    // Create driver
    let mut driver = Driver::new();

    // Create database with configuration
    let mut database = driver.new_database().expect("Failed to create database");
    database
        .set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.databricks.com".into()),
        )
        .expect("Failed to set uri");
    database
        .set_option(
            OptionDatabase::Other("databricks.http_path".into()),
            OptionValue::String("/sql/1.0/warehouses/abc123".into()),
        )
        .expect("Failed to set http_path");
    database
        .set_option(
            OptionDatabase::Other("databricks.access_token".into()),
            OptionValue::String("test-token".into()),
        )
        .expect("Failed to set access_token");

    // Verify options
    assert_eq!(
        database.get_option_string(OptionDatabase::Uri).unwrap(),
        "https://example.databricks.com"
    );
    assert_eq!(
        database
            .get_option_string(OptionDatabase::Other("databricks.warehouse_id".into()))
            .unwrap(),
        "abc123"
    );
}

#[test]
fn test_database_requires_token() {
    // Create driver
    let mut driver = Driver::new();

    // Create database without access token
    let mut database = driver.new_database().expect("Failed to create database");
    database
        .set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.databricks.com".into()),
        )
        .expect("Failed to set uri");
    database
        .set_option(
            OptionDatabase::Other("databricks.http_path".into()),
            OptionValue::String("/sql/1.0/warehouses/abc123".into()),
        )
        .expect("Failed to set http_path");

    // Connection should fail without access token
    let result = database.new_connection();
    assert!(result.is_err());
}

#[test]
fn test_cloudfetch_options() {
    let mut driver = Driver::new();
    let mut database = driver.new_database().expect("Failed to create database");

    // Set CloudFetch options
    database
        .set_option(
            OptionDatabase::Other("databricks.cloudfetch.enabled".into()),
            OptionValue::String("true".into()),
        )
        .expect("Failed to set cloudfetch.enabled");
    database
        .set_option(
            OptionDatabase::Other("databricks.cloudfetch.max_chunks_in_memory".into()),
            OptionValue::String("8".into()),
        )
        .expect("Failed to set max_chunks_in_memory");
    database
        .set_option(
            OptionDatabase::Other("databricks.cloudfetch.speed_threshold_mbps".into()),
            OptionValue::String("0.5".into()),
        )
        .expect("Failed to set speed_threshold_mbps");

    // Verify options
    assert_eq!(
        database
            .get_option_int(OptionDatabase::Other(
                "databricks.cloudfetch.max_chunks_in_memory".into()
            ))
            .unwrap(),
        8
    );
    assert_eq!(
        database
            .get_option_double(OptionDatabase::Other(
                "databricks.cloudfetch.speed_threshold_mbps".into()
            ))
            .unwrap(),
        0.5
    );
}

#[test]
fn test_auth_providers() {
    use databricks_adbc::auth::{AuthProvider, OAuthCredentials, PersonalAccessToken};

    // Test PAT
    let pat = PersonalAccessToken::new("test-token");
    assert_eq!(pat.get_auth_header().unwrap(), "Bearer test-token");

    // Test OAuth (not yet implemented)
    let oauth = OAuthCredentials::new("client-id", "client-secret");
    assert!(oauth.get_auth_header().is_err());
}

#[test]
fn test_adbc_traits_implemented() {
    // Verify that our types implement the correct ADBC traits
    fn assert_driver<T: adbc_core::Driver>() {}
    fn assert_database<T: adbc_core::Database>() {}
    fn assert_connection<T: adbc_core::Connection>() {}
    fn assert_statement<T: adbc_core::Statement>() {}

    assert_driver::<databricks_adbc::Driver>();
    assert_database::<databricks_adbc::Database>();
    assert_connection::<databricks_adbc::Connection>();
    assert_statement::<databricks_adbc::Statement>();
}

// End-to-end tests that require a real Databricks connection
// Run with: cargo test --test integration -- --ignored
#[test]
#[ignore]
fn test_real_connection() {
    use adbc_core::Connection as _;
    use adbc_core::Statement as _;

    // Read credentials from environment
    let host = std::env::var("DATABRICKS_HOST").expect("DATABRICKS_HOST not set");
    let http_path = std::env::var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH not set");
    let token = std::env::var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN not set");

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
    database
        .set_option(
            OptionDatabase::Other("databricks.access_token".into()),
            OptionValue::String(token),
        )
        .expect("Failed to set access_token");

    let mut connection = database.new_connection().expect("Failed to connect");
    let mut statement = connection
        .new_statement()
        .expect("Failed to create statement");
    statement
        .set_sql_query("SELECT 1 as test_value")
        .expect("Failed to set query");

    let mut reader = statement.execute().expect("Failed to execute");
    let batch = reader
        .next()
        .expect("No batch returned")
        .expect("Batch error");

    assert_eq!(batch.num_columns(), 1);
    assert!(batch.num_rows() > 0);
}
