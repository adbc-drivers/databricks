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

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Connection as _;
use adbc_core::Database as _;
use adbc_core::Driver as _;
use adbc_core::Optionable;
use adbc_core::Statement as _;
use databricks_adbc::Driver;

#[test]
fn test_driver_database_connection_flow() {
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

    // Verify options
    assert_eq!(
        database.get_option_string(OptionDatabase::Uri).unwrap(),
        "https://example.databricks.com"
    );

    // Create connection
    let mut connection = database.new_connection().expect("Failed to connect");

    // Get driver info
    {
        let info = connection.get_info(None);
        assert!(info.is_ok());
    }

    // Create statement
    let mut statement = connection
        .new_statement()
        .expect("Failed to create statement");
    statement
        .set_sql_query("SELECT 1")
        .expect("Failed to set query");
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
