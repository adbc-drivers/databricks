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

//! E2E tests for prepared statement and parameter binding support.
//!
//! These tests require a real Databricks connection and are marked with `#[ignore]`.
//!
//! # Setup
//!
//! ```bash
//! export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
//! export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
//! export DATABRICKS_TOKEN="your-pat-token"
//! ```
//!
//! # Running
//!
//! ```bash
//! cargo test --test prepared_statement_e2e -- --ignored --nocapture
//! ```

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Connection as _;
use adbc_core::Database as _;
use adbc_core::Driver as _;
use adbc_core::Optionable;
use adbc_core::Statement as _;
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, RecordBatchReader,
    StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use databricks_adbc::Driver;
use std::sync::Arc;

fn get_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!(
            "Environment variable {} is required. See test file docs.",
            name
        )
    })
}

fn create_connection() -> impl adbc_core::Connection {
    let host = get_env("DATABRICKS_HOST");
    let http_path = get_env("DATABRICKS_HTTP_PATH");
    let token = get_env("DATABRICKS_TOKEN");

    let mut driver = Driver::new();
    let mut db = driver.new_database().expect("Failed to create database");

    let uri = if host.starts_with("https://") || host.starts_with("http://") {
        host
    } else {
        format!("https://{host}")
    };
    db.set_option(OptionDatabase::Uri, OptionValue::String(uri))
        .expect("Failed to set uri");
    db.set_option(
        OptionDatabase::Other("databricks.http_path".into()),
        OptionValue::String(http_path),
    )
    .expect("Failed to set http_path");
    db.set_option(
        OptionDatabase::Other("databricks.auth.type".into()),
        OptionValue::String("access_token".into()),
    )
    .expect("Failed to set auth type");
    db.set_option(
        OptionDatabase::Other("databricks.access_token".into()),
        OptionValue::String(token),
    )
    .expect("Failed to set access_token");

    db.new_connection().expect("Failed to create connection")
}

/// Helper: create a single-row RecordBatch from typed arrays.
fn single_row_batch(fields: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
    let schema_fields: Vec<Field> = fields
        .iter()
        .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(schema_fields));
    let arrays: Vec<Arc<dyn Array>> = fields.into_iter().map(|(_, arr)| arr).collect();
    RecordBatch::try_new(schema, arrays).unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_parameterized_select_int() {
    let mut conn = create_connection();
    let mut stmt = conn.new_statement().expect("new_statement");

    stmt.set_sql_query("SELECT ? AS value").expect("set_sql");
    stmt.prepare().expect("prepare");

    let batch = single_row_batch(vec![(
        "p1",
        Arc::new(Int32Array::from(vec![42])) as Arc<dyn Array>,
    )]);
    stmt.bind(batch).expect("bind");

    let mut reader = stmt.execute().expect("execute");
    let result = reader.next().expect("has batch").expect("batch ok");
    assert_eq!(result.num_rows(), 1);

    let col = result.column(0);
    let val = col.as_primitive::<Int32Type>().value(0);
    assert_eq!(val, 42);
}

#[test]
#[ignore]
fn test_parameterized_select_string() {
    let mut conn = create_connection();
    let mut stmt = conn.new_statement().expect("new_statement");

    stmt.set_sql_query("SELECT ? AS value").expect("set_sql");
    stmt.prepare().expect("prepare");

    let batch = single_row_batch(vec![(
        "p1",
        Arc::new(StringArray::from(vec!["hello"])) as Arc<dyn Array>,
    )]);
    stmt.bind(batch).expect("bind");

    let mut reader = stmt.execute().expect("execute");
    let result = reader.next().expect("has batch").expect("batch ok");
    let val = result.column(0).as_string::<i32>().value(0);
    assert_eq!(val, "hello");
}

#[test]
#[ignore]
fn test_parameterized_multiple_params() {
    let mut conn = create_connection();
    let mut stmt = conn.new_statement().expect("new_statement");

    stmt.set_sql_query("SELECT ? AS a, ? AS b, ? AS c")
        .expect("set_sql");
    stmt.prepare().expect("prepare");

    let schema = stmt.get_parameter_schema().expect("get_parameter_schema");
    assert_eq!(schema.fields().len(), 3);

    let batch = single_row_batch(vec![
        ("p1", Arc::new(Int32Array::from(vec![1])) as Arc<dyn Array>),
        (
            "p2",
            Arc::new(StringArray::from(vec!["two"])) as Arc<dyn Array>,
        ),
        (
            "p3",
            Arc::new(Float64Array::from(vec![3.0])) as Arc<dyn Array>,
        ),
    ]);
    stmt.bind(batch).expect("bind");

    let mut reader = stmt.execute().expect("execute");
    let result = reader.next().expect("has batch").expect("batch ok");
    assert_eq!(result.num_columns(), 3);
}

#[test]
#[ignore]
fn test_parameterized_null_value() {
    let mut conn = create_connection();
    let mut stmt = conn.new_statement().expect("new_statement");

    stmt.set_sql_query("SELECT ? AS value").expect("set_sql");
    stmt.prepare().expect("prepare");

    let batch = single_row_batch(vec![(
        "p1",
        Arc::new(StringArray::from(vec![None::<&str>])) as Arc<dyn Array>,
    )]);
    stmt.bind(batch).expect("bind");

    let mut reader = stmt.execute().expect("execute");
    let result = reader.next().expect("has batch").expect("batch ok");
    assert!(result.column(0).is_null(0));
}

#[test]
#[ignore]
fn test_parameterized_boolean() {
    let mut conn = create_connection();
    let mut stmt = conn.new_statement().expect("new_statement");

    stmt.set_sql_query("SELECT ? AS value").expect("set_sql");
    stmt.prepare().expect("prepare");

    let batch = single_row_batch(vec![(
        "p1",
        Arc::new(BooleanArray::from(vec![true])) as Arc<dyn Array>,
    )]);
    stmt.bind(batch).expect("bind");

    let mut reader = stmt.execute().expect("execute");
    let result = reader.next().expect("has batch").expect("batch ok");
    let val = result.column(0).as_boolean().value(0);
    assert!(val);
}

#[test]
#[ignore]
fn test_parameterized_bigint() {
    let mut conn = create_connection();
    let mut stmt = conn.new_statement().expect("new_statement");

    stmt.set_sql_query("SELECT ? AS value").expect("set_sql");

    let batch = single_row_batch(vec![(
        "p1",
        Arc::new(Int64Array::from(vec![9_999_999_999i64])) as Arc<dyn Array>,
    )]);
    stmt.bind(batch).expect("bind");

    let mut reader = stmt.execute().expect("execute");
    let result = reader.next().expect("has batch").expect("batch ok");
    let val = result.column(0).as_primitive::<Int64Type>().value(0);
    assert_eq!(val, 9_999_999_999i64);
}

#[test]
#[ignore]
fn test_bind_stream_multiple_rows() {
    let mut conn = create_connection();
    let mut stmt = conn.new_statement().expect("new_statement");

    stmt.set_sql_query("SELECT ? AS value").expect("set_sql");
    stmt.prepare().expect("prepare");

    // Create a stream with 3 rows → 3 separate executions
    let schema = Arc::new(Schema::new(vec![Field::new("p1", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30])) as Arc<dyn Array>],
    )
    .unwrap();

    let stream: Box<dyn RecordBatchReader + Send> = Box::new(
        arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema),
    );
    stmt.bind_stream(stream).expect("bind_stream");

    let reader = stmt.execute().expect("execute");
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("collect");
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3);
}

#[test]
#[ignore]
fn test_execute_schema_via_describe() {
    let mut conn = create_connection();
    let mut stmt = conn.new_statement().expect("new_statement");

    stmt.set_sql_query("SELECT 1 AS id, 'hello' AS name, 3.14 AS amount")
        .expect("set_sql");

    let schema = stmt.execute_schema().expect("execute_schema");
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(2).name(), "amount");
}

#[test]
#[ignore]
fn test_execute_schema_with_parameter_markers() {
    let mut conn = create_connection();
    let mut stmt = conn.new_statement().expect("new_statement");

    // ? markers should be safely handled by replace_parameter_markers_with_literals
    stmt.set_sql_query("SELECT ? AS a, ? AS b")
        .expect("set_sql");

    let schema = stmt.execute_schema().expect("execute_schema");
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "a");
    assert_eq!(schema.field(1).name(), "b");
}

#[test]
#[ignore]
fn test_rebind_and_reexecute() {
    let mut conn = create_connection();
    let mut stmt = conn.new_statement().expect("new_statement");

    stmt.set_sql_query("SELECT ? AS value").expect("set_sql");
    stmt.prepare().expect("prepare");

    // First execution
    let batch1 = single_row_batch(vec![(
        "p1",
        Arc::new(Int32Array::from(vec![100])) as Arc<dyn Array>,
    )]);
    stmt.bind(batch1).expect("bind");
    let val1 = {
        let mut reader = stmt.execute().expect("execute");
        let result = reader.next().expect("has batch").expect("batch ok");
        result.column(0).as_primitive::<Int32Type>().value(0)
    };
    assert_eq!(val1, 100);

    // Re-bind and re-execute
    let batch2 = single_row_batch(vec![(
        "p1",
        Arc::new(Int32Array::from(vec![200])) as Arc<dyn Array>,
    )]);
    stmt.bind(batch2).expect("rebind");
    let val2 = {
        let mut reader = stmt.execute().expect("re-execute");
        let result = reader.next().expect("has batch").expect("batch ok");
        result.column(0).as_primitive::<Int32Type>().value(0)
    };
    assert_eq!(val2, 200);
}
