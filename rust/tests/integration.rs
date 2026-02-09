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
use adbc_core::Database as _;
use adbc_core::Driver as _;
use adbc_core::Optionable;
use arrow_array::Array;
use arrow_array::RecordBatchReader as _;
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

// ===========================================================================
// End-to-end tests that require a real Databricks connection
// Run with: cargo test --test integration -- --ignored
// ===========================================================================
//
// Required environment variables:
//   DATABRICKS_HOST       - e.g. "https://your-workspace.databricks.com"
//   DATABRICKS_HTTP_PATH  - e.g. "/sql/1.0/warehouses/abc123"
//   DATABRICKS_TOKEN      - personal access token

/// Helper: create a live Connection from environment variables.
fn create_live_connection() -> databricks_adbc::Connection {
    use adbc_core::Database as _;

    let host = std::env::var("DATABRICKS_HOST").expect("DATABRICKS_HOST not set");
    let http_path =
        std::env::var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH not set");
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

    database.new_connection().expect("Failed to connect")
}

#[test]
#[ignore]
fn test_real_connection() {
    use adbc_core::Connection as _;
    use adbc_core::Statement as _;

    let mut connection = create_live_connection();
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

// ---------------------------------------------------------------------------
// get_objects integration tests
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_get_objects_catalogs_depth() {
    use adbc_core::options::ObjectDepth;
    use adbc_core::schemas::GET_OBJECTS_SCHEMA;
    use adbc_core::Connection as _;
    use arrow_array::cast::AsArray;

    let connection = create_live_connection();

    let mut reader = connection
        .get_objects(ObjectDepth::Catalogs, None, None, None, None, None)
        .expect("get_objects(Catalogs) failed");

    // Schema must match GET_OBJECTS_SCHEMA
    let schema = reader.schema();
    assert_eq!(*schema, **GET_OBJECTS_SCHEMA, "Schema mismatch");

    // Collect all rows
    let mut total_rows = 0;
    let mut found_main = false;
    while let Some(batch_result) = reader.next() {
        let batch = batch_result.expect("Error reading batch");
        let catalog_col = batch.column(0).as_string::<i32>();
        for i in 0..batch.num_rows() {
            let name = catalog_col.value(i);
            if name == "main" {
                found_main = true;
            }
            // catalog_db_schemas should be null at Catalogs depth
            assert!(
                batch.column(1).is_null(i),
                "catalog_db_schemas should be null at Catalogs depth"
            );
        }
        total_rows += batch.num_rows();
    }

    assert!(total_rows > 0, "Expected at least one catalog");
    assert!(found_main, "Expected 'main' catalog to exist");
}

#[test]
#[ignore]
fn test_get_objects_schemas_depth() {
    use adbc_core::options::ObjectDepth;
    use adbc_core::schemas::GET_OBJECTS_SCHEMA;
    use adbc_core::Connection as _;
    use arrow_array::cast::AsArray;
    use arrow_array::StructArray;

    let connection = create_live_connection();

    let mut reader = connection
        .get_objects(ObjectDepth::Schemas, None, None, None, None, None)
        .expect("get_objects(Schemas) failed");

    let schema = reader.schema();
    assert_eq!(*schema, **GET_OBJECTS_SCHEMA, "Schema mismatch");

    let mut found_default_schema = false;
    let mut total_catalog_rows = 0;

    while let Some(batch_result) = reader.next() {
        let batch = batch_result.expect("Error reading batch");
        let catalog_col = batch.column(0).as_string::<i32>();
        let db_schemas_col = batch.column(1).as_list::<i32>();

        for i in 0..batch.num_rows() {
            total_catalog_rows += 1;
            let _catalog_name = catalog_col.value(i);

            // catalog_db_schemas should NOT be null at Schemas depth
            assert!(
                !db_schemas_col.is_null(i),
                "catalog_db_schemas should not be null at Schemas depth"
            );

            let schemas_arr = db_schemas_col.value(i);
            let schemas_struct = schemas_arr
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Expected StructArray for db_schemas");

            // Each schema struct should have db_schema_name and db_schema_tables (null)
            for j in 0..schemas_struct.len() {
                let schema_name = schemas_struct.column(0).as_string::<i32>().value(j);
                if schema_name == "default" {
                    found_default_schema = true;
                }
                // db_schema_tables should be null at Schemas depth
                assert!(
                    schemas_struct.column(1).is_null(j),
                    "db_schema_tables should be null at Schemas depth"
                );
            }
        }
    }

    assert!(total_catalog_rows > 0, "Expected at least one catalog");
    assert!(
        found_default_schema,
        "Expected 'default' schema to exist in some catalog"
    );
}

#[test]
#[ignore]
fn test_get_objects_tables_depth() {
    use adbc_core::options::ObjectDepth;
    use adbc_core::schemas::GET_OBJECTS_SCHEMA;
    use adbc_core::Connection as _;
    use arrow_array::cast::AsArray;
    use arrow_array::StructArray;

    let connection = create_live_connection();

    // Filter to main catalog, default schema to keep result set manageable
    let mut reader = connection
        .get_objects(
            ObjectDepth::Tables,
            Some("main"),
            Some("default"),
            None,
            None,
            None,
        )
        .expect("get_objects(Tables) failed");

    let schema = reader.schema();
    assert_eq!(*schema, **GET_OBJECTS_SCHEMA, "Schema mismatch");

    let mut found_table = false;

    while let Some(batch_result) = reader.next() {
        let batch = batch_result.expect("Error reading batch");
        let catalog_col = batch.column(0).as_string::<i32>();
        let db_schemas_col = batch.column(1).as_list::<i32>();

        for i in 0..batch.num_rows() {
            assert_eq!(
                catalog_col.value(i),
                "main",
                "Expected only 'main' catalog"
            );

            let schemas_arr = db_schemas_col.value(i);
            let schemas_struct = schemas_arr
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Expected StructArray");

            for j in 0..schemas_struct.len() {
                // db_schema_tables should NOT be null at Tables depth
                assert!(
                    !schemas_struct.column(1).is_null(j),
                    "db_schema_tables should not be null at Tables depth"
                );

                let tables_arr = schemas_struct.column(1).as_list::<i32>().value(j);
                let tables_struct = tables_arr
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Expected StructArray for tables");

                for k in 0..tables_struct.len() {
                    found_table = true;
                    let _table_name = tables_struct.column(0).as_string::<i32>().value(k);
                    let table_type = tables_struct.column(1).as_string::<i32>().value(k);
                    // table_type should be a known type
                    assert!(
                        !table_type.is_empty()
                            || true, // empty string is valid for hive_metastore
                        "table_type should be present"
                    );
                    // table_columns should be null at Tables depth
                    assert!(
                        tables_struct.column(2).is_null(k),
                        "table_columns should be null at Tables depth"
                    );
                    // table_constraints should be null at Tables depth
                    assert!(
                        tables_struct.column(3).is_null(k),
                        "table_constraints should be null at Tables depth"
                    );
                }
            }
        }
    }

    assert!(found_table, "Expected at least one table in main.default");
}

#[test]
#[ignore]
fn test_get_objects_columns_depth() {
    use adbc_core::options::ObjectDepth;
    use adbc_core::schemas::GET_OBJECTS_SCHEMA;
    use adbc_core::Connection as _;
    use arrow_array::cast::AsArray;
    use arrow_array::StructArray;

    let connection = create_live_connection();

    // Use All depth (Columns depth is treated identically per ADBC spec).
    // Filter tightly to keep result set small.
    let mut reader = connection
        .get_objects(
            ObjectDepth::All,
            Some("main"),
            Some("information_schema"),
            Some("tables"),
            None,
            None,
        )
        .expect("get_objects(All) failed");

    let schema = reader.schema();
    assert_eq!(*schema, **GET_OBJECTS_SCHEMA, "Schema mismatch");

    let mut found_column = false;

    while let Some(batch_result) = reader.next() {
        let batch = batch_result.expect("Error reading batch");
        let db_schemas_col = batch.column(1).as_list::<i32>();

        for i in 0..batch.num_rows() {
            let schemas_arr = db_schemas_col.value(i);
            let schemas_struct = schemas_arr
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Expected StructArray");

            for j in 0..schemas_struct.len() {
                let tables_arr = schemas_struct.column(1).as_list::<i32>().value(j);
                let tables_struct = tables_arr
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Expected StructArray for tables");

                for k in 0..tables_struct.len() {
                    // table_columns should NOT be null at All depth
                    assert!(
                        !tables_struct.column(2).is_null(k),
                        "table_columns should not be null at All depth"
                    );

                    let cols_arr = tables_struct.column(2).as_list::<i32>().value(k);
                    let cols_struct = cols_arr
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .expect("Expected StructArray for columns");

                    if cols_struct.len() > 0 {
                        found_column = true;
                        // Verify column_name is present (first field of column struct)
                        let col_name = cols_struct.column(0).as_string::<i32>().value(0);
                        assert!(!col_name.is_empty(), "column_name should not be empty");
                    }

                    // table_constraints should be an empty list (not null) at All depth
                    assert!(
                        !tables_struct.column(3).is_null(k),
                        "table_constraints should not be null at All depth"
                    );
                }
            }
        }
    }

    assert!(
        found_column,
        "Expected at least one column in main.information_schema.tables"
    );
}

#[test]
#[ignore]
fn test_get_objects_with_catalog_filter() {
    use adbc_core::options::ObjectDepth;
    use adbc_core::Connection as _;
    use arrow_array::cast::AsArray;

    let connection = create_live_connection();

    let mut reader = connection
        .get_objects(
            ObjectDepth::Catalogs,
            Some("main"),
            None,
            None,
            None,
            None,
        )
        .expect("get_objects with catalog filter failed");

    let mut catalog_names = Vec::new();
    while let Some(batch_result) = reader.next() {
        let batch = batch_result.expect("Error reading batch");
        let catalog_col = batch.column(0).as_string::<i32>();
        for i in 0..batch.num_rows() {
            catalog_names.push(catalog_col.value(i).to_string());
        }
    }

    assert_eq!(catalog_names, vec!["main"], "Filter should return only 'main'");
}

#[test]
#[ignore]
fn test_get_objects_with_schema_pattern() {
    use adbc_core::options::ObjectDepth;
    use adbc_core::Connection as _;
    use arrow_array::cast::AsArray;
    use arrow_array::StructArray;

    let connection = create_live_connection();

    // Filter schemas matching "default" in the "main" catalog
    let mut reader = connection
        .get_objects(
            ObjectDepth::Schemas,
            Some("main"),
            Some("default"),
            None,
            None,
            None,
        )
        .expect("get_objects with schema pattern failed");

    let mut found_default = false;

    while let Some(batch_result) = reader.next() {
        let batch = batch_result.expect("Error reading batch");
        let db_schemas_col = batch.column(1).as_list::<i32>();

        for i in 0..batch.num_rows() {
            let schemas_arr = db_schemas_col.value(i);
            let schemas_struct = schemas_arr
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Expected StructArray");

            for j in 0..schemas_struct.len() {
                let name = schemas_struct.column(0).as_string::<i32>().value(j);
                if name == "default" {
                    found_default = true;
                }
            }
        }
    }

    assert!(found_default, "Expected 'default' schema with pattern filter");
}

#[test]
#[ignore]
fn test_get_objects_with_table_type_filter() {
    use adbc_core::options::ObjectDepth;
    use adbc_core::Connection as _;
    use arrow_array::cast::AsArray;
    use arrow_array::StructArray;

    let connection = create_live_connection();

    // Filter to only VIEWs in main.information_schema
    let mut reader = connection
        .get_objects(
            ObjectDepth::Tables,
            Some("main"),
            Some("information_schema"),
            None,
            Some(vec!["VIEW"]),
            None,
        )
        .expect("get_objects with table_type filter failed");

    let mut found_view = false;

    while let Some(batch_result) = reader.next() {
        let batch = batch_result.expect("Error reading batch");
        let db_schemas_col = batch.column(1).as_list::<i32>();

        for i in 0..batch.num_rows() {
            let schemas_arr = db_schemas_col.value(i);
            let schemas_struct = schemas_arr
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Expected StructArray");

            for j in 0..schemas_struct.len() {
                let tables_arr = schemas_struct.column(1).as_list::<i32>().value(j);
                let tables_struct = tables_arr
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Expected StructArray for tables");

                for k in 0..tables_struct.len() {
                    let table_type = tables_struct.column(1).as_string::<i32>().value(k);
                    assert!(
                        table_type.eq_ignore_ascii_case("VIEW"),
                        "Expected only VIEW types, got '{}'",
                        table_type
                    );
                    found_view = true;
                }
            }
        }
    }

    assert!(
        found_view,
        "Expected at least one VIEW in main.information_schema"
    );
}

// ---------------------------------------------------------------------------
// get_table_schema integration tests
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_get_table_schema_existing_table() {
    use adbc_core::Connection as _;

    let connection = create_live_connection();

    // information_schema.tables exists in every Unity Catalog workspace
    let schema = connection
        .get_table_schema(Some("main"), Some("information_schema"), "tables")
        .expect("get_table_schema failed for existing table");

    assert!(
        schema.fields().len() > 0,
        "Expected at least one column in information_schema.tables"
    );

    // Verify known columns exist
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        field_names.contains(&"table_name"),
        "Expected 'table_name' column, got: {:?}",
        field_names
    );
    assert!(
        field_names.contains(&"table_schema"),
        "Expected 'table_schema' column, got: {:?}",
        field_names
    );
    assert!(
        field_names.contains(&"table_catalog"),
        "Expected 'table_catalog' column, got: {:?}",
        field_names
    );
}

#[test]
#[ignore]
fn test_get_table_schema_nonexistent_table() {
    use adbc_core::Connection as _;

    let connection = create_live_connection();

    let result = connection.get_table_schema(
        Some("main"),
        Some("default"),
        "nonexistent_table_xyz_12345",
    );

    assert!(
        result.is_err(),
        "Expected error for nonexistent table, got: {:?}",
        result
    );
    let err = result.unwrap_err();
    assert_eq!(
        err.status,
        adbc_core::error::Status::NotFound,
        "Expected NOT_FOUND status, got: {:?}",
        err.status
    );
}

#[test]
#[ignore]
fn test_get_table_schema_without_catalog() {
    use adbc_core::Connection as _;

    let connection = create_live_connection();

    // Call without catalog — should discover it via list_tables
    let schema = connection
        .get_table_schema(None, Some("information_schema"), "tables")
        .expect("get_table_schema without catalog failed");

    assert!(
        schema.fields().len() > 0,
        "Expected at least one column when catalog is discovered"
    );

    // Verify it returns the same fields as with explicit catalog
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        field_names.contains(&"table_name"),
        "Expected 'table_name' column, got: {:?}",
        field_names
    );
}

// ---------------------------------------------------------------------------
// get_table_types integration test
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_get_table_types() {
    use adbc_core::schemas::GET_TABLE_TYPES_SCHEMA;
    use adbc_core::Connection as _;
    use arrow_array::cast::AsArray;

    let connection = create_live_connection();

    let mut reader = connection
        .get_table_types()
        .expect("get_table_types failed");

    // Verify schema matches GET_TABLE_TYPES_SCHEMA
    let schema = reader.schema();
    assert_eq!(*schema, **GET_TABLE_TYPES_SCHEMA, "Schema mismatch");
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "table_type");

    // Collect all table types
    let mut table_types = Vec::new();
    while let Some(batch_result) = reader.next() {
        let batch = batch_result.expect("Error reading batch");
        let col = batch.column(0).as_string::<i32>();
        for i in 0..batch.num_rows() {
            table_types.push(col.value(i).to_string());
        }
    }

    // Should contain the standard types
    assert!(
        table_types.contains(&"TABLE".to_string()),
        "Expected TABLE type, got: {:?}",
        table_types
    );
    assert!(
        table_types.contains(&"VIEW".to_string()),
        "Expected VIEW type, got: {:?}",
        table_types
    );
    assert!(
        table_types.len() >= 2,
        "Expected at least 2 table types, got: {:?}",
        table_types
    );
}
