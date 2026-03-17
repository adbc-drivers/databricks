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

//! E2E test for result metadata propagation from SEA manifest to Arrow schema.
//!
//! Verifies that `databricks.*` field-level metadata (type_name, type_text,
//! type_precision, type_scale) is attached to Arrow fields after query execution.
//!
//! Run with:
//! ```bash
//! cargo run --example metadata_propagation_test
//! ```

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Connection as ConnectionTrait;
use adbc_core::Database as DatabaseTrait;
use adbc_core::Driver as DriverTrait;
use adbc_core::Optionable;
use adbc_core::Statement as StatementTrait;
use arrow_array::RecordBatchReader;
use databricks_adbc::Driver;

const DATABRICKS_TYPE_NAME: &str = "databricks.type_name";
const DATABRICKS_TYPE_TEXT: &str = "databricks.type_text";
const DATABRICKS_TYPE_PRECISION: &str = "databricks.type_precision";
const DATABRICKS_TYPE_SCALE: &str = "databricks.type_scale";

fn main() {
    let host =
        std::env::var("DATABRICKS_HOST").expect("DATABRICKS_HOST environment variable required");
    let http_path = std::env::var("DATABRICKS_HTTP_PATH")
        .expect("DATABRICKS_HTTP_PATH environment variable required");
    let token =
        std::env::var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN environment variable required");

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
        OptionDatabase::Other("databricks.auth.type".into()),
        OptionValue::String("access_token".to_string()),
    )
    .expect("Failed to set auth type");
    db.set_option(
        OptionDatabase::Other("databricks.access_token".into()),
        OptionValue::String(token),
    )
    .expect("Failed to set access_token");

    let mut conn = db.new_connection().expect("Failed to create connection");

    let mut all_passed = true;

    // Test 1: Basic types
    println!("=== Test 1: Basic Types ===");
    all_passed &= run_test(
        &mut conn,
        r#"SELECT
            CAST(1 AS INT) as int_col,
            CAST(100 AS BIGINT) as bigint_col,
            'hello' as string_col,
            TRUE as bool_col,
            CAST(3.14 AS DOUBLE) as double_col,
            CAST(1.5 AS FLOAT) as float_col,
            CAST(42 AS SMALLINT) as smallint_col,
            CAST(7 AS TINYINT) as tinyint_col
        "#,
        &[
            ("int_col", "INT", "INT", None, None),
            ("bigint_col", "LONG", "BIGINT", None, None),
            ("string_col", "STRING", "STRING", None, None),
            ("bool_col", "BOOLEAN", "BOOLEAN", None, None),
            ("double_col", "DOUBLE", "DOUBLE", None, None),
            ("float_col", "FLOAT", "FLOAT", None, None),
            ("smallint_col", "SHORT", "SMALLINT", None, None),
            ("tinyint_col", "BYTE", "TINYINT", None, None),
        ],
    );

    // Test 2: DECIMAL with precision and scale
    println!("\n=== Test 2: DECIMAL Types ===");
    all_passed &= run_test(
        &mut conn,
        r#"SELECT
            CAST(1.23 AS DECIMAL(10,2)) as dec_10_2,
            CAST(99999.99999 AS DECIMAL(18,5)) as dec_18_5,
            CAST(0 AS DECIMAL(38,0)) as dec_38_0
        "#,
        &[
            (
                "dec_10_2",
                "DECIMAL",
                "DECIMAL(10,2)",
                Some("10"),
                Some("2"),
            ),
            (
                "dec_18_5",
                "DECIMAL",
                "DECIMAL(18,5)",
                Some("18"),
                Some("5"),
            ),
            (
                "dec_38_0",
                "DECIMAL",
                "DECIMAL(38,0)",
                Some("38"),
                Some("0"),
            ),
        ],
    );

    // Test 3: Date/Time types
    println!("\n=== Test 3: Date/Time Types ===");
    all_passed &= run_test(
        &mut conn,
        r#"SELECT
            CURRENT_DATE() as date_col,
            CURRENT_TIMESTAMP() as timestamp_col
        "#,
        &[
            ("date_col", "DATE", "DATE", None, None),
            ("timestamp_col", "TIMESTAMP", "TIMESTAMP", None, None),
        ],
    );

    // Test 4: Complex types
    println!("\n=== Test 4: Complex Types ===");
    all_passed &= run_test(
        &mut conn,
        r#"SELECT
            ARRAY(1, 2, 3) as array_col,
            MAP('a', 1, 'b', 2) as map_col,
            NAMED_STRUCT('x', 1, 'y', 'hello') as struct_col
        "#,
        &[
            ("array_col", "ARRAY", "ARRAY<INT>", None, None),
            ("map_col", "MAP", "MAP<STRING, INT>", None, None),
            (
                "struct_col",
                "STRUCT",
                "STRUCT<x: INT NOT NULL, y: STRING NOT NULL>",
                None,
                None,
            ),
        ],
    );

    // Test 5: BINARY type
    println!("\n=== Test 5: BINARY Type ===");
    all_passed &= run_test(
        &mut conn,
        r#"SELECT CAST('bytes' AS BINARY) as binary_col"#,
        &[("binary_col", "BINARY", "BINARY", None, None)],
    );

    println!("\n========================================");
    if all_passed {
        println!("ALL TESTS PASSED");
    } else {
        println!("SOME TESTS FAILED");
        std::process::exit(1);
    }
}

/// Expected column metadata: (field_name, type_name, type_text, precision, scale)
type ExpectedColumn<'a> = (&'a str, &'a str, &'a str, Option<&'a str>, Option<&'a str>);

/// Run a query and verify that the Arrow field metadata matches expectations.
fn run_test(conn: &mut impl ConnectionTrait, sql: &str, expected: &[ExpectedColumn<'_>]) -> bool {
    let mut stmt = conn.new_statement().expect("Failed to create statement");
    stmt.set_sql_query(sql).expect("Failed to set query");
    let reader = stmt.execute().expect("Failed to execute query");
    let schema = reader.schema();

    let mut all_ok = true;

    for (i, (name, exp_type_name, exp_type_text, exp_precision, exp_scale)) in
        expected.iter().enumerate()
    {
        let field = schema.field(i);
        let meta = field.metadata();

        let got_name = meta.get(DATABRICKS_TYPE_NAME);
        let got_text = meta.get(DATABRICKS_TYPE_TEXT);
        let got_prec = meta.get(DATABRICKS_TYPE_PRECISION);
        let got_scale = meta.get(DATABRICKS_TYPE_SCALE);

        let name_ok = got_name.map(|s| s.as_str()) == Some(exp_type_name);
        let text_ok = got_text.map(|s| s.as_str()) == Some(exp_type_text);
        let prec_ok = got_prec.map(|s| s.as_str()) == *exp_precision;
        let scale_ok = got_scale.map(|s| s.as_str()) == *exp_scale;

        let ok = name_ok && text_ok && prec_ok && scale_ok;
        let status = if ok { "PASS" } else { "FAIL" };

        println!(
            "  [{status}] {name}: type_name={} type_text={} precision={} scale={}",
            got_name.map(|s| s.as_str()).unwrap_or("<missing>"),
            got_text.map(|s| s.as_str()).unwrap_or("<missing>"),
            got_prec.map(|s| s.as_str()).unwrap_or("<none>"),
            got_scale.map(|s| s.as_str()).unwrap_or("<none>"),
        );

        if !ok {
            all_ok = false;
            if !name_ok {
                println!(
                    "         expected type_name={exp_type_name}, got {:?}",
                    got_name
                );
            }
            if !text_ok {
                println!(
                    "         expected type_text={exp_type_text}, got {:?}",
                    got_text
                );
            }
            if !prec_ok {
                println!(
                    "         expected precision={:?}, got {:?}",
                    exp_precision, got_prec
                );
            }
            if !scale_ok {
                println!(
                    "         expected scale={:?}, got {:?}",
                    exp_scale, got_scale
                );
            }
        }
    }

    all_ok
}
