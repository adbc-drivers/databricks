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

//! FFI round-trip integration tests.
//!
//! These tests load the driver as a shared library through `dlopen` (the same
//! code path C/C++ consumers use) and validate that options, errors, and the
//! driver lifecycle work correctly across the FFI boundary.
//!
//! Run with: `cargo test --features ffi`

#![cfg(feature = "ffi")]

use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Database, Driver, Optionable};
use adbc_driver_manager::ManagedDriver;

/// Load the driver shared library via dlopen + dlsym.
fn load_driver() -> ManagedDriver {
    ManagedDriver::load_dynamic_from_name(
        "databricks_adbc",
        Some(b"AdbcDatabricksInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load libdatabricks_adbc shared library")
}

#[test]
fn test_driver_loads() {
    let _driver = load_driver();
    // If we get here, the shared library loaded and the entry point resolved.
}

#[test]
fn test_database_set_get_options() {
    let mut driver = load_driver();
    let mut db = driver.new_database().expect("new_database failed");

    // Set string options
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("https://example.databricks.com".into()),
    )
    .expect("set uri failed");

    db.set_option(
        OptionDatabase::Other("databricks.warehouse_id".into()),
        OptionValue::String("abc123".into()),
    )
    .expect("set warehouse_id failed");

    db.set_option(
        OptionDatabase::Other("databricks.access_token".into()),
        OptionValue::String("dapi_test_token".into()),
    )
    .expect("set access_token failed");

    // Read back string options
    let uri = db
        .get_option_string(OptionDatabase::Uri)
        .expect("get uri failed");
    assert_eq!(uri, "https://example.databricks.com");

    let wid = db
        .get_option_string(OptionDatabase::Other("databricks.warehouse_id".into()))
        .expect("get warehouse_id failed");
    assert_eq!(wid, "abc123");

    // Set and read back integer option
    db.set_option(
        OptionDatabase::Other("databricks.cloudfetch.max_chunks_in_memory".into()),
        OptionValue::String("8".into()),
    )
    .expect("set max_chunks_in_memory failed");

    let chunks = db
        .get_option_int(OptionDatabase::Other(
            "databricks.cloudfetch.max_chunks_in_memory".into(),
        ))
        .expect("get max_chunks_in_memory failed");
    assert_eq!(chunks, 8);

    // Set and read back double option
    db.set_option(
        OptionDatabase::Other("databricks.cloudfetch.speed_threshold_mbps".into()),
        OptionValue::String("0.5".into()),
    )
    .expect("set speed_threshold_mbps failed");

    let threshold = db
        .get_option_double(OptionDatabase::Other(
            "databricks.cloudfetch.speed_threshold_mbps".into(),
        ))
        .expect("get speed_threshold_mbps failed");
    assert!((threshold - 0.5).abs() < f64::EPSILON);
}

#[test]
fn test_database_missing_required_options() {
    let mut driver = load_driver();
    let db = driver.new_database().expect("new_database failed");

    // Attempting to create a connection without setting required options
    // should return an error, not panic.
    let result = db.new_connection();
    assert!(
        result.is_err(),
        "Expected error when required options are missing"
    );
}

#[test]
fn test_unknown_option_error() {
    let mut driver = load_driver();
    let mut db = driver.new_database().expect("new_database failed");

    // Setting an unrecognized option should return an error.
    let result = db.set_option(
        OptionDatabase::Other("totally.unknown.option".into()),
        OptionValue::String("value".into()),
    );
    assert!(result.is_err(), "Expected error for unknown option");
}
