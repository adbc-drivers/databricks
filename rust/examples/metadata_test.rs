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

//! Connection metadata methods test using `samples.tpch`.
//!
//! This example exercises the ADBC Connection metadata methods against the
//! well-known `samples` catalog which contains TPC-H tables with fixed schemas.
//!
//! - `get_info()` — returns driver/vendor info
//! - `get_table_types()` — returns supported table types
//! - `get_objects()` — returns catalog/schema/table/column hierarchy
//! - `get_table_schema()` — returns Arrow schema for a specific table
//!
//! Run with:
//! ```bash
//! RUST_LOG=databricks_adbc=debug cargo run --example metadata_test
//! ```

use adbc_core::options::{ObjectDepth, OptionDatabase, OptionValue};
use adbc_core::Connection as ConnectionTrait;
use adbc_core::Database as DatabaseTrait;
use adbc_core::Driver as DriverTrait;
use adbc_core::Optionable;
use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatchReader, StructArray};
use databricks_adbc::Driver;
use std::time::Instant;

/// The 8 TPC-H tables in `samples.tpch`, in alphabetical order.
const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

/// The 16 columns of `samples.tpch.lineitem`, in ordinal order.
const LINEITEM_COLUMNS: &[&str] = &[
    "l_orderkey",
    "l_partkey",
    "l_suppkey",
    "l_linenumber",
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
    "l_returnflag",
    "l_linestatus",
    "l_shipdate",
    "l_commitdate",
    "l_receiptdate",
    "l_shipinstruct",
    "l_shipmode",
    "l_comment",
];

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .init();

    let host =
        std::env::var("DATABRICKS_HOST").expect("DATABRICKS_HOST environment variable required");
    let http_path = std::env::var("DATABRICKS_HTTP_PATH")
        .expect("DATABRICKS_HTTP_PATH environment variable required");
    let token =
        std::env::var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN environment variable required");

    println!("=== Connection Metadata Methods Test (samples.tpch) ===\n");
    println!("Host: {}", &host);
    println!("HTTP Path: {}", &http_path);
    println!();

    let mut driver = Driver::new();
    let mut db = driver.new_database().expect("Failed to create database");
    db.set_option(OptionDatabase::Uri, OptionValue::String(host))
        .expect("Failed to set uri");
    db.set_option(
        OptionDatabase::Other("databricks.http_path".into()),
        OptionValue::String(http_path),
    )
    .expect("Failed to set http_path");
    db.set_option(
        OptionDatabase::Other("databricks.access_token".into()),
        OptionValue::String(token),
    )
    .expect("Failed to set access_token");

    let conn = db.new_connection().expect("Failed to create connection");

    println!("--- Test 1: get_info ---");
    test_get_info(&conn);

    println!("\n--- Test 2: get_table_types ---");
    test_get_table_types(&conn);

    println!("\n--- Test 3: get_objects(Catalogs) ---");
    test_get_objects_catalogs(&conn);

    println!("\n--- Test 4: get_objects(Schemas) - catalog='samples' ---");
    test_get_objects_schemas(&conn);

    println!("\n--- Test 5: get_objects(Tables) - samples.tpch ---");
    test_get_objects_tables(&conn);

    println!("\n--- Test 6: get_objects(All) - samples.tpch.lineitem ---");
    test_get_objects_all(&conn);

    println!("\n--- Test 7: get_table_schema - samples.tpch.lineitem ---");
    test_get_table_schema(&conn);

    println!("\n--- Test 8: get_table_schema (auto catalog) - tpch.lineitem ---");
    test_get_table_schema_auto_catalog(&conn);

    println!("\n--- Test 9: get_objects(Schemas) with pattern - catalog='samples', schema='tpc%' ---");
    test_get_objects_schemas_pattern(&conn);

    println!("\n--- Test 10: get_objects(Tables) with pattern - samples.tpch, table='line%' ---");
    test_get_objects_tables_pattern(&conn);

    println!("\n--- Test 11: get_objects(Catalogs) with pattern - catalog='sam%' ---");
    test_get_objects_catalogs_pattern(&conn);

    println!("\n=== All Metadata Tests Complete ===");
}

// ---------------------------------------------------------------------------
// Test 1: get_info
// ---------------------------------------------------------------------------

fn test_get_info(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn.get_info(None).expect("get_info failed");

    let mut found_codes = Vec::new();
    for batch_result in reader.by_ref() {
        let batch = batch_result.expect("Error reading get_info batch");
        let codes = batch
            .column(0)
            .as_primitive::<arrow_array::types::UInt32Type>();
        for i in 0..batch.num_rows() {
            let code = codes.value(i);
            let info_name = match code {
                0 => "VendorName",
                100 => "DriverName",
                101 => "DriverVersion",
                _ => "Other",
            };
            println!("  {} (code={})", info_name, code);
            found_codes.push(code);
        }
    }

    assert!(found_codes.contains(&0), "Must return VendorName (code=0)");
    assert!(
        found_codes.contains(&100),
        "Must return DriverName (code=100)"
    );
    assert!(
        found_codes.contains(&101),
        "Must return DriverVersion (code=101)"
    );
    assert!(found_codes.len() >= 3, "Must return at least 3 entries");

    println!(
        "  Total: {} entries [PASS] ({:.3}s)",
        found_codes.len(),
        start.elapsed().as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// Test 2: get_table_types
// ---------------------------------------------------------------------------

fn test_get_table_types(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn.get_table_types().expect("get_table_types failed");

    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "table_type");

    let mut types = Vec::new();
    for batch_result in reader.by_ref() {
        let batch = batch_result.expect("Error reading get_table_types batch");
        let col = batch.column(0).as_string::<i32>();
        for i in 0..batch.num_rows() {
            println!("  {}", col.value(i));
            types.push(col.value(i).to_string());
        }
    }

    let expected = ["SYSTEM TABLE", "TABLE", "VIEW", "METRIC_VIEW"];
    assert_eq!(types.len(), expected.len());
    for tt in &expected {
        assert!(types.iter().any(|t| t == tt), "Missing type: {}", tt);
    }

    println!(
        "  Total: {} types [PASS] ({:.3}s)",
        types.len(),
        start.elapsed().as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// Test 3: get_objects(Catalogs) — must include "samples"
// ---------------------------------------------------------------------------

fn test_get_objects_catalogs(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn
        .get_objects(ObjectDepth::Catalogs, None, None, None, None, None)
        .expect("get_objects(Catalogs) failed");

    let schema = reader.schema();
    assert_eq!(schema.field(0).name(), "catalog_name");

    let mut names = Vec::new();
    for batch_result in reader.by_ref() {
        let batch = batch_result.expect("Error reading Catalogs batch");
        let col = batch.column(0).as_string::<i32>();
        for i in 0..batch.num_rows() {
            println!("  {}", col.value(i));
            names.push(col.value(i).to_string());
        }
    }

    assert!(!names.is_empty(), "Must return at least 1 catalog");
    assert!(
        names.iter().any(|c| c == "samples"),
        "Must include 'samples' catalog"
    );

    println!(
        "  Total: {} catalogs [PASS] ({:.3}s)",
        names.len(),
        start.elapsed().as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// Test 4: get_objects(Schemas) — catalog='samples', must include 'tpch'
// ---------------------------------------------------------------------------

/// Helper to downcast a ListArray value (Arc<dyn Array>) to StructArray reference.
fn as_struct(array: &dyn Array) -> &StructArray {
    array.as_any().downcast_ref::<StructArray>().unwrap()
}

fn test_get_objects_schemas(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn
        .get_objects(
            ObjectDepth::Schemas,
            Some("samples"),
            None,
            None,
            None,
            None,
        )
        .expect("get_objects(Schemas) failed");

    let mut schema_names = Vec::new();
    let mut all_catalogs_correct = true;
    for batch_result in reader.by_ref() {
        let batch = batch_result.expect("Error reading Schemas batch");
        let catalog_col = batch.column(0).as_string::<i32>();
        let schemas_list = batch.column(1).as_list::<i32>();

        for i in 0..batch.num_rows() {
            let catalog = catalog_col.value(i);
            if catalog != "samples" {
                all_catalogs_correct = false;
            }
            if schemas_list.is_null(i) {
                continue;
            }
            let arr = schemas_list.value(i);
            let structs = as_struct(arr.as_ref());
            let names = structs.column(0).as_string::<i32>();
            for j in 0..structs.len() {
                println!("  samples.{}", names.value(j));
                schema_names.push(names.value(j).to_string());
            }
        }
    }

    assert!(all_catalogs_correct, "All rows must have catalog='samples'");
    assert!(
        schema_names.iter().any(|s| s == "tpch"),
        "Must include 'tpch' schema"
    );
    assert!(
        schema_names.iter().any(|s| s == "information_schema"),
        "Must include 'information_schema'"
    );

    println!(
        "  Total: {} schemas [PASS] ({:.3}s)",
        schema_names.len(),
        start.elapsed().as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// Test 5: get_objects(Tables) — samples.tpch, must have the 8 TPC-H tables
// ---------------------------------------------------------------------------

fn test_get_objects_tables(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn
        .get_objects(
            ObjectDepth::Tables,
            Some("samples"),
            Some("tpch"),
            None,
            None,
            None,
        )
        .expect("get_objects(Tables) failed");

    let mut table_names = Vec::new();
    let mut all_correct_location = true;
    for batch_result in reader.by_ref() {
        let batch = batch_result.expect("Error reading Tables batch");
        let catalog_col = batch.column(0).as_string::<i32>();
        let schemas_list = batch.column(1).as_list::<i32>();

        for i in 0..batch.num_rows() {
            let catalog = catalog_col.value(i);
            if schemas_list.is_null(i) {
                continue;
            }
            let schemas_arr = schemas_list.value(i);
            let schemas_struct = as_struct(schemas_arr.as_ref());
            let schema_names = schemas_struct.column(0).as_string::<i32>();
            let tables_lists = schemas_struct.column(1).as_list::<i32>();

            for j in 0..schemas_struct.len() {
                let schema_name = schema_names.value(j);
                if catalog != "samples" || schema_name != "tpch" {
                    all_correct_location = false;
                }
                if tables_lists.is_null(j) {
                    continue;
                }
                let tables_arr = tables_lists.value(j);
                let tables_struct = as_struct(tables_arr.as_ref());
                let tbl_names = tables_struct.column(0).as_string::<i32>();
                let tbl_types = tables_struct.column(1).as_string::<i32>();

                for k in 0..tables_struct.len() {
                    println!(
                        "  samples.tpch.{} ({})",
                        tbl_names.value(k),
                        tbl_types.value(k)
                    );
                    table_names.push(tbl_names.value(k).to_string());
                }
            }
        }
    }

    assert!(all_correct_location, "All tables must be in samples.tpch");
    assert_eq!(
        table_names.len(),
        TPCH_TABLES.len(),
        "Expected {} TPC-H tables, got {}",
        TPCH_TABLES.len(),
        table_names.len()
    );
    for expected in TPCH_TABLES {
        assert!(
            table_names.iter().any(|t| t == expected),
            "Missing table: {}",
            expected
        );
    }

    println!(
        "  Total: {} tables [PASS] ({:.3}s)",
        table_names.len(),
        start.elapsed().as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// Test 6: get_objects(All) — samples.tpch.lineitem with 16 columns
// ---------------------------------------------------------------------------

fn test_get_objects_all(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn
        .get_objects(
            ObjectDepth::All,
            Some("samples"),
            Some("tpch"),
            Some("lineitem"),
            None,
            None,
        )
        .expect("get_objects(All) failed");

    let mut found_table = false;
    let mut column_names = Vec::new();
    for batch_result in reader.by_ref() {
        let batch = batch_result.expect("Error reading All batch");
        let catalog_col = batch.column(0).as_string::<i32>();
        let schemas_list = batch.column(1).as_list::<i32>();

        for i in 0..batch.num_rows() {
            let catalog = catalog_col.value(i);
            if schemas_list.is_null(i) {
                continue;
            }
            let schemas_arr = schemas_list.value(i);
            let schemas_struct = as_struct(schemas_arr.as_ref());
            let schema_names = schemas_struct.column(0).as_string::<i32>();
            let tables_lists = schemas_struct.column(1).as_list::<i32>();

            for j in 0..schemas_struct.len() {
                let schema_name = schema_names.value(j);
                if tables_lists.is_null(j) {
                    continue;
                }
                let tables_arr = tables_lists.value(j);
                let tables_struct = as_struct(tables_arr.as_ref());
                let tbl_names = tables_struct.column(0).as_string::<i32>();
                let tbl_types = tables_struct.column(1).as_string::<i32>();
                let columns_lists = tables_struct.column(2).as_list::<i32>();

                for k in 0..tables_struct.len() {
                    println!(
                        "  {}.{}.{} ({})",
                        catalog,
                        schema_name,
                        tbl_names.value(k),
                        tbl_types.value(k)
                    );
                    found_table = true;

                    if columns_lists.is_null(k) {
                        println!("    (no columns)");
                        continue;
                    }
                    let cols_arr = columns_lists.value(k);
                    let cols_struct = as_struct(cols_arr.as_ref());
                    let col_names = cols_struct.column(0).as_string::<i32>();
                    let col_ordinals = cols_struct
                        .column(1)
                        .as_primitive::<arrow_array::types::Int32Type>();

                    for c in 0..cols_struct.len() {
                        let ordinal = if col_ordinals.is_null(c) {
                            "?".to_string()
                        } else {
                            col_ordinals.value(c).to_string()
                        };
                        let name = col_names.value(c);
                        println!("    column[{}]: {} (ordinal={})", c, name, ordinal);
                        column_names.push(name.to_string());
                    }
                }
            }
        }
    }

    assert!(found_table, "Must find samples.tpch.lineitem");
    assert_eq!(
        column_names.len(),
        LINEITEM_COLUMNS.len(),
        "lineitem must have {} columns, got {}",
        LINEITEM_COLUMNS.len(),
        column_names.len()
    );
    for (i, expected) in LINEITEM_COLUMNS.iter().enumerate() {
        assert_eq!(
            column_names[i], *expected,
            "Column {} must be '{}', got '{}'",
            i, expected, column_names[i]
        );
    }

    println!(
        "  Total: {} columns [PASS] ({:.3}s)",
        column_names.len(),
        start.elapsed().as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// Test 7: get_table_schema — samples.tpch.lineitem (explicit catalog)
// ---------------------------------------------------------------------------

fn test_get_table_schema(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let schema = conn
        .get_table_schema(Some("samples"), Some("tpch"), "lineitem")
        .expect("get_table_schema failed");

    println!("  Fields ({}):", schema.fields().len());
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    for (i, field) in schema.fields().iter().enumerate() {
        println!(
            "    [{}] {} : {} (nullable={})",
            i,
            field.name(),
            field.data_type(),
            field.is_nullable()
        );
    }

    assert_eq!(
        field_names.len(),
        LINEITEM_COLUMNS.len(),
        "lineitem must have {} fields, got {}",
        LINEITEM_COLUMNS.len(),
        field_names.len()
    );
    for (i, expected) in LINEITEM_COLUMNS.iter().enumerate() {
        assert_eq!(
            field_names[i], *expected,
            "Field {} must be '{}', got '{}'",
            i, expected, field_names[i]
        );
    }

    println!(
        "  Total: {} fields [PASS] ({:.3}s)",
        field_names.len(),
        start.elapsed().as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// Test 8: get_table_schema (auto catalog resolution) — tpch.lineitem
// ---------------------------------------------------------------------------

fn test_get_table_schema_auto_catalog(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let schema = conn
        .get_table_schema(None, Some("tpch"), "lineitem")
        .expect("get_table_schema (auto catalog) failed");

    println!("  Fields ({}):", schema.fields().len());
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    for (i, field) in schema.fields().iter().enumerate() {
        println!(
            "    [{}] {} : {} (nullable={})",
            i,
            field.name(),
            field.data_type(),
            field.is_nullable()
        );
    }

    // Same lineitem columns, resolved without specifying catalog
    assert_eq!(
        field_names.len(),
        LINEITEM_COLUMNS.len(),
        "lineitem must have {} fields, got {}",
        LINEITEM_COLUMNS.len(),
        field_names.len()
    );
    for (i, expected) in LINEITEM_COLUMNS.iter().enumerate() {
        assert_eq!(
            field_names[i], *expected,
            "Field {} must be '{}', got '{}'",
            i, expected, field_names[i]
        );
    }

    println!(
        "  Total: {} fields [PASS] ({:.3}s)",
        field_names.len(),
        start.elapsed().as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// Test 9: get_objects(Schemas) with pattern — schema LIKE 'tpc%'
// ---------------------------------------------------------------------------

fn test_get_objects_schemas_pattern(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn
        .get_objects(
            ObjectDepth::Schemas,
            Some("samples"),
            Some("tpc%"),
            None,
            None,
            None,
        )
        .expect("get_objects(Schemas) with pattern failed");

    let mut schema_names = Vec::new();
    for batch_result in reader.by_ref() {
        let batch = batch_result.expect("Error reading Schemas batch");
        let schemas_list = batch.column(1).as_list::<i32>();

        for i in 0..batch.num_rows() {
            if schemas_list.is_null(i) {
                continue;
            }
            let schemas_arr = schemas_list.value(i);
            let schemas_struct = as_struct(schemas_arr.as_ref());
            let names = schemas_struct.column(0).as_string::<i32>();
            for j in 0..schemas_struct.len() {
                let name = names.value(j);
                println!("  {}", name);
                schema_names.push(name.to_string());
            }
        }
    }

    // 'tpc%' should match tpch, tpcds_sf1, tpcds_sf1000
    assert!(
        schema_names.len() >= 3,
        "Pattern 'tpc%' should match at least 3 schemas, got {}",
        schema_names.len()
    );
    assert!(
        schema_names.iter().any(|s| s == "tpch"),
        "Must include 'tpch'"
    );
    for name in &schema_names {
        assert!(
            name.starts_with("tpc"),
            "All matched schemas must start with 'tpc', got '{}'",
            name
        );
    }

    println!(
        "  Total: {} schemas [PASS] ({:.3}s)",
        schema_names.len(),
        start.elapsed().as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// Test 10: get_objects(Tables) with pattern — table LIKE 'line%'
// ---------------------------------------------------------------------------

fn test_get_objects_tables_pattern(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn
        .get_objects(
            ObjectDepth::Tables,
            Some("samples"),
            Some("tpch"),
            Some("line%"),
            None,
            None,
        )
        .expect("get_objects(Tables) with table pattern failed");

    let mut table_names = Vec::new();
    for batch_result in reader.by_ref() {
        let batch = batch_result.expect("Error reading Tables batch");
        let schemas_list = batch.column(1).as_list::<i32>();

        for i in 0..batch.num_rows() {
            if schemas_list.is_null(i) {
                continue;
            }
            let schemas_arr = schemas_list.value(i);
            let schemas_struct = as_struct(schemas_arr.as_ref());
            let tables_lists = schemas_struct.column(1).as_list::<i32>();

            for j in 0..schemas_struct.len() {
                if tables_lists.is_null(j) {
                    continue;
                }
                let tables_arr = tables_lists.value(j);
                let tables_struct = as_struct(tables_arr.as_ref());
                let tbl_names = tables_struct.column(0).as_string::<i32>();
                for k in 0..tables_struct.len() {
                    let name = tbl_names.value(k);
                    println!("  {}", name);
                    table_names.push(name.to_string());
                }
            }
        }
    }

    // 'line%' in samples.tpch should match 'lineitem'
    assert_eq!(
        table_names.len(),
        1,
        "Pattern 'line%' should match exactly 1 table, got {}",
        table_names.len()
    );
    assert_eq!(table_names[0], "lineitem");

    println!(
        "  Total: {} tables [PASS] ({:.3}s)",
        table_names.len(),
        start.elapsed().as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// Test 11: get_objects(Catalogs) with pattern — catalog LIKE 'sam%'
// ---------------------------------------------------------------------------

fn test_get_objects_catalogs_pattern(conn: &impl ConnectionTrait) {
    let start = Instant::now();
    let mut reader = conn
        .get_objects(
            ObjectDepth::Catalogs,
            Some("sam%"),
            None,
            None,
            None,
            None,
        )
        .expect("get_objects(Catalogs) with pattern failed");

    let mut catalog_names = Vec::new();
    for batch_result in reader.by_ref() {
        let batch = batch_result.expect("Error reading Catalogs batch");
        let catalog_col = batch.column(0).as_string::<i32>();
        for i in 0..batch.num_rows() {
            let name = catalog_col.value(i);
            println!("  {}", name);
            catalog_names.push(name.to_string());
        }
    }

    // 'sam%' should match 'samples' (client-side filtered)
    assert!(
        catalog_names.iter().any(|c| c == "samples"),
        "Must include 'samples'"
    );
    for name in &catalog_names {
        assert!(
            name.starts_with("sam"),
            "All matched catalogs must start with 'sam', got '{}'",
            name
        );
    }

    println!(
        "  Total: {} catalogs [PASS] ({:.3}s)",
        catalog_names.len(),
        start.elapsed().as_secs_f64()
    );
}
