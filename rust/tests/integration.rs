// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Integration tests for the Databricks ADBC driver.

use databricks_adbc::Driver;

#[test]
fn test_driver_database_connection_flow() {
    // Create driver
    let driver = Driver::new();

    // Create database with configuration
    let database = driver.new_database().expect("Failed to create database");
    let database = database
        .with_host("https://example.databricks.com")
        .with_http_path("/sql/1.0/warehouses/abc123")
        .with_catalog("main")
        .with_schema("default");

    // Create connection
    let connection = database.connect().expect("Failed to connect");

    // Verify configuration propagated
    assert_eq!(connection.host(), Some("https://example.databricks.com"));
    assert_eq!(connection.http_path(), Some("/sql/1.0/warehouses/abc123"));
    assert_eq!(connection.catalog(), Some("main"));
    assert_eq!(connection.schema(), Some("default"));

    // Create statement
    let mut statement = connection
        .new_statement()
        .expect("Failed to create statement");
    statement.set_sql_query("SELECT 1");
    assert_eq!(statement.sql_query(), Some("SELECT 1"));
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
