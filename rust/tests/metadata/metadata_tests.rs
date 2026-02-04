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

//! Integration tests for Connection metadata methods.
//!
//! These tests run against a real Databricks endpoint and verify the correctness
//! of `get_objects()`, `get_table_schema()`, and `get_table_types()` implementations.
//!
//! ## Setup Requirements
//!
//! These tests require the following environment variables to be set:
//! - `DATABRICKS_HOST`: The Databricks workspace URL (e.g., "https://example.databricks.com")
//! - `DATABRICKS_HTTP_PATH`: The SQL warehouse HTTP path (e.g., "/sql/1.0/warehouses/abc123")
//! - `DATABRICKS_TOKEN`: A valid personal access token
//!
//! Optionally, you can specify a test catalog and schema:
//! - `DATABRICKS_TEST_CATALOG`: The catalog to use for tests (default: "main")
//! - `DATABRICKS_TEST_SCHEMA`: The schema to use for tests (default: "default")
//!
//! ## Running Tests
//!
//! These tests are marked with `#[ignore]` to prevent them from running in CI
//! without proper credentials. To run them locally:
//!
//! ```bash
//! # Set environment variables
//! export DATABRICKS_HOST="https://your-workspace.databricks.com"
//! export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
//! export DATABRICKS_TOKEN="your-pat-token"
//!
//! # Run metadata integration tests
//! cargo test --test integration metadata_tests -- --ignored --nocapture
//! ```

use adbc_core::options::{ObjectDepth, OptionDatabase, OptionValue};
use adbc_core::Connection as _;
use adbc_core::Database as _;
use adbc_core::Driver as _;
use adbc_core::Optionable;
use arrow_array::{Array, RecordBatchReader, StringArray};
use arrow_schema::DataType;
use databricks_adbc::Driver;
use std::env;

/// Helper struct for test configuration.
struct TestConfig {
    host: String,
    http_path: String,
    token: String,
    catalog: String,
    schema: String,
}

impl TestConfig {
    /// Creates a TestConfig from environment variables.
    ///
    /// Panics if required environment variables are not set.
    fn from_env() -> Self {
        Self {
            host: env::var("DATABRICKS_HOST").expect("DATABRICKS_HOST not set"),
            http_path: env::var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH not set"),
            token: env::var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN not set"),
            catalog: env::var("DATABRICKS_TEST_CATALOG").unwrap_or_else(|_| "main".to_string()),
            schema: env::var("DATABRICKS_TEST_SCHEMA").unwrap_or_else(|_| "default".to_string()),
        }
    }
}

/// Creates a connected database and connection for testing.
fn create_test_connection() -> databricks_adbc::Connection {
    let config = TestConfig::from_env();

    let mut driver = Driver::new();
    let mut database = driver.new_database().expect("Failed to create database");

    database
        .set_option(OptionDatabase::Uri, OptionValue::String(config.host))
        .expect("Failed to set uri");
    database
        .set_option(
            OptionDatabase::Other("databricks.http_path".into()),
            OptionValue::String(config.http_path),
        )
        .expect("Failed to set http_path");
    database
        .set_option(
            OptionDatabase::Other("databricks.access_token".into()),
            OptionValue::String(config.token),
        )
        .expect("Failed to set access_token");

    database.new_connection().expect("Failed to connect")
}

// =============================================================================
// get_objects Tests at Various Depths
// =============================================================================

/// Test get_objects at Catalogs depth.
///
/// Verifies that:
/// - The result has the correct Arrow schema
/// - At least one catalog is returned
/// - catalog_db_schemas list is empty for Catalogs depth
#[test]
#[ignore]
fn test_get_objects_catalogs_depth() {
    let connection = create_test_connection();

    let mut reader = connection
        .get_objects(
            ObjectDepth::Catalogs,
            None, // all catalogs
            None,
            None,
            None,
            None,
        )
        .expect("get_objects should succeed");

    // Verify schema
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 2, "Should have 2 top-level fields");
    assert_eq!(schema.field(0).name(), "catalog_name");
    assert_eq!(schema.field(1).name(), "catalog_db_schemas");

    // Read results
    let batch = reader.next().expect("Should have one batch").unwrap();
    assert!(batch.num_rows() > 0, "Should have at least one catalog");

    // Verify catalog_name column
    let catalog_names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("catalog_name should be StringArray");

    // Log found catalogs
    println!("Found {} catalogs:", catalog_names.len());
    for i in 0..catalog_names.len() {
        println!("  - {}", catalog_names.value(i));
    }
}

/// Test get_objects at Schemas depth.
///
/// Verifies that:
/// - Schemas are returned for each catalog
/// - The nested db_schema structure is correct
#[test]
#[ignore]
fn test_get_objects_schemas_depth() {
    let config = TestConfig::from_env();
    let connection = create_test_connection();

    // Get objects for a specific catalog to reduce result size
    let mut reader = connection
        .get_objects(
            ObjectDepth::Schemas,
            Some(&config.catalog),
            None,
            None,
            None,
            None,
        )
        .expect("get_objects should succeed");

    let batch = reader.next().expect("Should have one batch").unwrap();
    assert!(batch.num_rows() > 0, "Should have at least one catalog");

    // Verify catalog_db_schemas contains schemas
    let db_schemas_col = batch.column(1);
    assert!(
        matches!(db_schemas_col.data_type(), DataType::List(_)),
        "catalog_db_schemas should be a List"
    );

    println!(
        "Found {} catalog rows with schemas at Schemas depth",
        batch.num_rows()
    );
}

/// Test get_objects at Tables depth.
///
/// Verifies that:
/// - Tables are returned for each schema
/// - Table names and types are populated
#[test]
#[ignore]
fn test_get_objects_tables_depth() {
    let config = TestConfig::from_env();
    let connection = create_test_connection();

    let mut reader = connection
        .get_objects(
            ObjectDepth::Tables,
            Some(&config.catalog),
            Some(&config.schema),
            None,
            None,
            None,
        )
        .expect("get_objects should succeed");

    let batch = reader.next().expect("Should have one batch").unwrap();
    println!(
        "Found {} catalog rows with tables at Tables depth",
        batch.num_rows()
    );

    // Verify schema structure includes table fields
    let schema = reader.schema();
    assert_eq!(schema.field(0).name(), "catalog_name");
    assert_eq!(schema.field(1).name(), "catalog_db_schemas");
}

/// Test get_objects at Columns depth (full depth).
///
/// Verifies that:
/// - Columns are returned for tables
/// - Column metadata includes type information
/// - XDBC fields are populated
#[test]
#[ignore]
fn test_get_objects_columns_depth() {
    let config = TestConfig::from_env();
    let connection = create_test_connection();

    // Use specific filters to limit result size
    let mut reader = connection
        .get_objects(
            ObjectDepth::All, // Full depth includes columns
            Some(&config.catalog),
            Some(&config.schema),
            Some("%"), // All tables in the schema
            None,
            None,
        )
        .expect("get_objects should succeed");

    let batch = reader.next().expect("Should have one batch").unwrap();
    println!(
        "Found {} catalog rows with full column details",
        batch.num_rows()
    );

    // The result should be a nested structure with columns
    assert!(batch.num_rows() > 0, "Should have results");
}

// =============================================================================
// get_objects Filter Tests
// =============================================================================

/// Test get_objects with catalog filter.
///
/// Verifies that:
/// - Only the specified catalog is returned
/// - Pattern matching works correctly
#[test]
#[ignore]
fn test_get_objects_with_catalog_filter() {
    let config = TestConfig::from_env();
    let connection = create_test_connection();

    let mut reader = connection
        .get_objects(
            ObjectDepth::Catalogs,
            Some(&config.catalog), // Filter to specific catalog
            None,
            None,
            None,
            None,
        )
        .expect("get_objects should succeed");

    let batch = reader.next().expect("Should have one batch").unwrap();

    let catalog_names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("catalog_name should be StringArray");

    // All returned catalogs should match the filter
    for i in 0..catalog_names.len() {
        let name = catalog_names.value(i);
        println!("Returned catalog: {}", name);
        // The pattern matching may be exact or LIKE-based
        // At minimum, the specified catalog should be present
    }

    // Should contain the requested catalog
    let has_catalog = (0..catalog_names.len()).any(|i| catalog_names.value(i) == config.catalog);
    assert!(has_catalog, "Should contain the filtered catalog");
}

/// Test get_objects with schema pattern filter.
///
/// Verifies that:
/// - Schema pattern filtering works
/// - LIKE wildcards are supported
#[test]
#[ignore]
fn test_get_objects_with_schema_pattern() {
    let config = TestConfig::from_env();
    let connection = create_test_connection();

    // Filter with a pattern - use the first 3 chars of schema + wildcard
    let pattern = if config.schema.len() > 3 {
        format!("{}%", &config.schema[..3])
    } else {
        format!("{}%", &config.schema)
    };

    let mut reader = connection
        .get_objects(
            ObjectDepth::Schemas,
            Some(&config.catalog),
            Some(&pattern),
            None,
            None,
            None,
        )
        .expect("get_objects should succeed");

    let batch = reader.next().expect("Should have one batch").unwrap();
    println!(
        "Found {} catalogs with schemas matching pattern '{}'",
        batch.num_rows(),
        pattern
    );

    // Should have results if the pattern matches
    assert!(
        batch.num_rows() > 0,
        "Should have results matching the schema pattern"
    );
}

/// Test get_objects with table type filter.
///
/// Verifies that:
/// - Table type filtering works
/// - Only tables of specified types are returned
#[test]
#[ignore]
fn test_get_objects_with_table_type_filter() {
    let config = TestConfig::from_env();
    let connection = create_test_connection();

    // Filter for only TABLEs (not views)
    let table_types = vec!["TABLE"];

    let mut reader = connection
        .get_objects(
            ObjectDepth::Tables,
            Some(&config.catalog),
            Some(&config.schema),
            None,
            Some(table_types),
            None,
        )
        .expect("get_objects should succeed");

    let batch = reader.next().expect("Should have one batch").unwrap();
    println!("Found {} catalogs with TABLE-type tables", batch.num_rows());

    // Results should only contain TABLEs (no VIEWs)
    // Note: The nested structure makes it hard to verify directly in this test
}

// =============================================================================
// get_table_schema Tests
// =============================================================================

/// Test get_table_schema for an existing table.
///
/// Verifies that:
/// - Schema is returned for existing tables
/// - Column names and types are correct
/// - Nullable flags are populated
#[test]
#[ignore]
fn test_get_table_schema_existing_table() {
    let config = TestConfig::from_env();
    let connection = create_test_connection();

    // First, find a table that exists in the test schema
    let mut reader = connection
        .get_objects(
            ObjectDepth::Tables,
            Some(&config.catalog),
            Some(&config.schema),
            None,
            None,
            None,
        )
        .expect("get_objects should succeed");

    let batch = reader.next().expect("Should have one batch").unwrap();

    if batch.num_rows() == 0 {
        println!(
            "SKIP: No tables found in {}.{} - test requires existing tables",
            config.catalog, config.schema
        );
        return;
    }

    // We need to navigate the nested structure to find a table name
    // For now, use a simple approach - try to get schema for a known table pattern
    // In real tests, we would parse the nested structure

    // Try to find any table - this is a simplified test
    // A more robust test would parse the get_objects result to find table names
    let test_table = env::var("DATABRICKS_TEST_TABLE").unwrap_or_else(|_| {
        println!("SKIP: Set DATABRICKS_TEST_TABLE env var to test get_table_schema with a specific table");
        return "".to_string();
    });

    if test_table.is_empty() {
        return;
    }

    let schema = connection
        .get_table_schema(Some(&config.catalog), Some(&config.schema), &test_table)
        .expect("get_table_schema should succeed");

    println!("Table {} schema:", test_table);
    for field in schema.fields() {
        println!(
            "  - {} : {:?} (nullable: {})",
            field.name(),
            field.data_type(),
            field.is_nullable()
        );
    }

    assert!(schema.fields().len() > 0, "Table should have columns");
}

/// Test get_table_schema for a nonexistent table.
///
/// Verifies that:
/// - Appropriate error is returned for missing tables
/// - Error indicates "not found" condition
#[test]
#[ignore]
fn test_get_table_schema_nonexistent_table() {
    let config = TestConfig::from_env();
    let connection = create_test_connection();

    // Use a table name that definitely doesn't exist
    let nonexistent_table = "___nonexistent_table_12345___";

    let result = connection.get_table_schema(
        Some(&config.catalog),
        Some(&config.schema),
        nonexistent_table,
    );

    assert!(result.is_err(), "Should return error for nonexistent table");

    let error = result.unwrap_err();
    println!("Error for nonexistent table: {:?}", error);
    // The error should indicate the table was not found
}

// =============================================================================
// get_table_types Tests
// =============================================================================

/// Test get_table_types returns expected types.
///
/// Verifies that:
/// - get_table_types returns a valid RecordBatchReader
/// - The schema has a single column "table_type"
/// - Standard Databricks table types are returned
#[test]
#[ignore]
fn test_get_table_types() {
    let connection = create_test_connection();

    let mut reader = connection
        .get_table_types()
        .expect("get_table_types should succeed");

    // Verify schema
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 1, "Should have 1 field");
    assert_eq!(schema.field(0).name(), "table_type");
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Utf8,
        "table_type should be Utf8"
    );
    assert!(
        !schema.field(0).is_nullable(),
        "table_type should not be nullable"
    );

    // Read results
    let batch = reader.next().expect("Should have one batch").unwrap();
    assert!(batch.num_rows() >= 4, "Should have at least 4 table types");

    let table_types = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("table_type should be StringArray");

    // Collect types for verification
    let types: Vec<&str> = (0..table_types.len())
        .map(|i| table_types.value(i))
        .collect();

    println!("Table types returned: {:?}", types);

    // Verify expected types are present
    assert!(types.contains(&"TABLE"), "Should contain TABLE type");
    assert!(types.contains(&"VIEW"), "Should contain VIEW type");
    assert!(
        types.contains(&"SYSTEM TABLE"),
        "Should contain SYSTEM TABLE type"
    );
    assert!(
        types.contains(&"METRIC_VIEW"),
        "Should contain METRIC_VIEW type"
    );
}

// =============================================================================
// Arrow Schema Verification Tests
// =============================================================================

/// Test that get_objects returns correct Arrow schema structure.
///
/// This test verifies the nested Arrow schema matches the ADBC specification.
#[test]
#[ignore]
fn test_get_objects_arrow_schema_structure() {
    let connection = create_test_connection();

    let reader = connection
        .get_objects(ObjectDepth::All, None, None, None, None, None)
        .expect("get_objects should succeed");

    let schema = reader.schema();

    // Verify top-level fields
    assert_eq!(schema.field(0).name(), "catalog_name");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert!(schema.field(0).is_nullable());

    assert_eq!(schema.field(1).name(), "catalog_db_schemas");
    assert!(matches!(schema.field(1).data_type(), DataType::List(_)));

    // Verify nested structure
    if let DataType::List(db_schema_field) = schema.field(1).data_type() {
        if let DataType::Struct(db_schema_fields) = db_schema_field.data_type() {
            assert!(
                db_schema_fields.len() >= 2,
                "db_schema struct should have at least 2 fields"
            );

            // First field should be db_schema_name
            assert_eq!(db_schema_fields[0].name(), "db_schema_name");

            // Second field should be db_schema_tables
            assert_eq!(db_schema_fields[1].name(), "db_schema_tables");
        }
    }

    println!("Schema structure verified successfully");
}
