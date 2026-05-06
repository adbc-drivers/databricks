<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Prepared Statement & Parameter Binding Design

## Overview

Add prepared statement and parameter binding support to the Rust ADBC driver. This enables parameterized queries via the Databricks SQL Execution API (SEA), which is the foundation for `SQLBindParameter`, `SQLNumParams`, and `SQLDescribeParam` in the ODBC bridge layer.

## Motivation

The ODBC driver (`databricks-odbc`) delegates statement execution to the ADBC Rust driver via the ADBC C API. Today, all prepared statement methods (`prepare`, `bind`, `bind_stream`, `get_parameter_schema`) return `NOT_IMPLEMENTED`. This means:

- Applications cannot use parameterized queries (e.g., `SELECT * FROM t WHERE id = ?`)
- `SQLBindParameter` is unsupported in the ODBC driver
- `SQLNumParams` and `SQLDescribeParam` cannot return parameter metadata

The ODBC bridge already handles the `NOT_IMPLEMENTED` fallback gracefully (stores query, defers execution), but this prevents real parameter binding. The SEA API supports positional parameters natively — we just need to wire it through.

## Current State

### Statement (`statement.rs`)

```rust
pub struct Statement {
    query: Option<String>,
    client: Arc<dyn DatabricksClient>,
    session_id: String,
    runtime_handle: RuntimeHandle,
    current_statement_id: Option<String>,
}
```

All prepared statement methods return `NotImplemented`:
- `prepare()` — line 107
- `get_parameter_schema()` — line 113
- `bind()` — line 119
- `bind_stream()` — line 125

### ExecuteParams (`types/sea.rs`)

```rust
pub struct ExecuteParams {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub wait_timeout: Option<String>,
    pub on_wait_timeout: Option<String>,
    pub row_limit: Option<i64>,
}
```

No fields for parameters.

### ExecuteStatementRequest (`types/sea.rs`)

```rust
pub struct ExecuteStatementRequest {
    pub warehouse_id: String,
    pub statement: String,
    pub session_id: Option<String>,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub disposition: String,
    pub format: String,
    pub wait_timeout: Option<String>,
    pub on_wait_timeout: Option<String>,
    pub row_limit: Option<i64>,
}
```

No `parameters` field.

### SEA API Parameter Support

The SEA API (`POST /api/2.0/sql/statements`) accepts a `parameters` array with positional parameters. The JDBC driver (`~/databricks-jdbc`) uses this in production:

```json
{
  "statement": "SELECT * FROM t WHERE id = ? AND name = ?",
  "parameters": [
    { "ordinal": 1, "type": "INT", "value": "100" },
    { "ordinal": 2, "type": "STRING", "value": "alice" }
  ]
}
```

Key characteristics:
- **Positional parameters** — `?` markers in SQL, identified by ordinal position (1-based)
- **String values** — all values serialized as strings; the `type` field tells the server how to cast
- **No server-side PREPARE** — parameters are sent with each execute request (client-side only)
- **256-parameter limit** — enforced by the backend

### ODBC Bridge → ADBC Flow

The ODBC bridge sends parameters to `AdbcStatementBind()` as an Arrow RecordBatch via the Arrow C Data Interface:
- Each **column** is one parameter
- There is **one row** per execution
- Column types are Arrow types (Int32, Utf8, Float64, etc.)

The ADBC driver must convert this Arrow RecordBatch into SEA `parameters` format on `execute()`.

## Design

### 1. SEA Types — `StatementParameter`

Add a new struct and wire it into the request types.

```rust
// types/sea.rs

/// A positional parameter for a parameterized SEA statement execution.
#[derive(Debug, Clone, Serialize)]
pub struct StatementParameter {
    /// 1-based position of the parameter in the SQL statement.
    pub ordinal: i32,
    /// Databricks SQL type name (e.g., "INT", "STRING", "DOUBLE", "TIMESTAMP").
    #[serde(rename = "type")]
    pub param_type: String,
    /// String representation of the parameter value. None for SQL NULL.
    pub value: Option<String>,
}
```

Add `parameters` to `ExecuteStatementRequest`:

```rust
pub struct ExecuteStatementRequest {
    // ... existing fields ...

    /// Positional parameters for parameterized queries.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub parameters: Vec<StatementParameter>,
}
```

Add `parameters` to `ExecuteParams`:

```rust
pub struct ExecuteParams {
    // ... existing fields ...

    /// Parameters for parameterized query execution.
    pub parameters: Vec<StatementParameter>,
}
```

### 2. SQL Parameter Marker Parsing

Add a function to count `?` markers in SQL, ignoring those inside string literals (`'...'`) and comments (`--` line comments, `/* */` block comments).

```rust
// statement.rs (or a new statement/params.rs module)

/// Count parameter markers (?) in a SQL string, ignoring those inside
/// string literals and comments.
fn count_parameter_markers(sql: &str) -> usize {
    // State machine: Normal, InSingleQuote, InLineComment, InBlockComment
    // In Normal state, count '?' occurrences
    // Handle escape sequences: '' (escaped single quote)
}
```

This is needed for `get_parameter_schema()` to return the correct field count before `bind()` is called.

Reference: JDBC's `DatabricksParameterMetaData.countParameters()` handles the same parsing.

### 3. Arrow → SEA Parameter Conversion

Add a module to convert Arrow RecordBatch columns to `Vec<StatementParameter>`.

```rust
// statement/params.rs (new module)

/// Convert a single-row Arrow RecordBatch into SEA statement parameters.
///
/// Each column in the batch becomes one positional parameter.
/// Arrow types are mapped to Databricks SQL type names:
///
/// | Arrow Type        | Databricks Type |
/// |-------------------|-----------------|
/// | Boolean           | BOOLEAN         |
/// | Int8              | TINYINT         |
/// | Int16             | SMALLINT        |
/// | Int32             | INT             |
/// | Int64             | BIGINT          |
/// | Float32           | FLOAT           |
/// | Float64           | DOUBLE          |
/// | Decimal128(p, s)  | DECIMAL(p,s)    |
/// | Utf8 / LargeUtf8  | STRING          |
/// | Binary / LargeBinary | BINARY       |
/// | Date32            | DATE            |
/// | Timestamp(_, _)   | TIMESTAMP       |
/// | Null              | STRING          |
///
/// Values are serialized to their string representation.
/// Null values are sent with `value: None` (JSON null).
pub fn record_batch_to_sea_parameters(
    batch: &RecordBatch,
) -> Result<Vec<StatementParameter>> {
    if batch.num_rows() != 1 {
        return Err(DatabricksErrorHelper::invalid_argument()
            .message(format!(
                "bind() expects a single-row RecordBatch, got {} rows",
                batch.num_rows()
            )));
    }

    let mut params = Vec::with_capacity(batch.num_columns());
    for (i, column) in batch.columns().iter().enumerate() {
        let param_type = arrow_type_to_databricks_type(column.data_type());
        let value = if column.is_null(0) {
            None
        } else {
            Some(arrow_value_to_string(column, 0)?)
        };

        params.push(StatementParameter {
            ordinal: (i + 1) as i32,
            param_type,
            value,
        });
    }

    Ok(params)
}

/// Map Arrow DataType to Databricks SQL type name.
fn arrow_type_to_databricks_type(dt: &DataType) -> String { ... }

/// Extract a scalar value from an Arrow array at the given index as a string.
fn arrow_value_to_string(array: &ArrayRef, index: usize) -> Result<String> { ... }
```

### 4. Statement Changes

Add new fields and implement the prepared statement methods.

```rust
// statement.rs

pub struct Statement {
    // ... existing fields ...

    /// Whether the statement has been prepared.
    is_prepared: bool,
    /// Number of parameter markers (?) found in the SQL query (set by prepare()).
    parameter_count: Option<usize>,
    /// Bound parameters (single row) from bind().
    bound_params: Option<RecordBatch>,
    /// Bound parameter stream from bind_stream().
    bound_stream: Option<Box<dyn RecordBatchReader + Send>>,
}
```

#### `prepare()`

```rust
fn prepare(&mut self) -> Result<()> {
    let query = self.query.as_ref().ok_or_else(|| {
        DatabricksErrorHelper::invalid_state()
            .message("No query set")
            .to_adbc()
    })?;

    self.parameter_count = Some(count_parameter_markers(query));
    self.is_prepared = true;

    // Clear any previously bound parameters
    self.bound_params = None;
    self.bound_stream = None;

    Ok(())
}
```

No server-side call — the SEA API has no PREPARE endpoint. This is client-side only, consistent with JDBC.

#### `bind()`

```rust
fn bind(&mut self, batch: RecordBatch) -> Result<()> {
    // Validate single row
    if batch.num_rows() != 1 {
        return Err(DatabricksErrorHelper::invalid_argument()
            .message(format!(
                "bind() expects a single-row RecordBatch, got {} rows",
                batch.num_rows()
            ))
            .to_adbc());
    }

    // Validate column count matches parameter markers if prepared
    if let Some(expected) = self.parameter_count {
        if batch.num_columns() != expected {
            return Err(DatabricksErrorHelper::invalid_argument()
                .message(format!(
                    "Expected {} parameters but got {} columns",
                    expected, batch.num_columns()
                ))
                .to_adbc());
        }
    }

    self.bound_params = Some(batch);
    self.bound_stream = None; // bind() clears any prior bind_stream()
    Ok(())
}
```

#### `bind_stream()`

```rust
fn bind_stream(&mut self, stream: Box<dyn RecordBatchReader + Send>) -> Result<()> {
    // Validate column count matches parameter markers if prepared
    if let Some(expected) = self.parameter_count {
        if stream.schema().fields().len() != expected {
            return Err(DatabricksErrorHelper::invalid_argument()
                .message(format!(
                    "Expected {} parameters but got {} columns in stream schema",
                    expected, stream.schema().fields().len()
                ))
                .to_adbc());
        }
    }

    self.bound_stream = Some(stream);
    self.bound_params = None; // bind_stream() clears any prior bind()
    Ok(())
}
```

#### `get_parameter_schema()`

```rust
fn get_parameter_schema(&self) -> Result<Schema> {
    // If parameters are bound, return the bound schema
    if let Some(ref batch) = self.bound_params {
        return Ok((*batch.schema()).clone());
    }
    if let Some(ref stream) = self.bound_stream {
        return Ok((*stream.schema()).clone());
    }

    // If prepared but not bound, return a schema with Null-typed fields
    // (parameter count is known, but types are not until bind)
    if let Some(count) = self.parameter_count {
        let fields: Vec<arrow_schema::Field> = (0..count)
            .map(|i| arrow_schema::Field::new(format!("{}", i), arrow_schema::DataType::Null, true))
            .collect();
        return Ok(Schema::new(fields));
    }

    Err(DatabricksErrorHelper::invalid_state()
        .message("Statement not prepared and no parameters bound")
        .to_adbc())
}
```

#### `execute()` — Updated

```rust
fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
    let query = self.query.as_ref().ok_or_else(|| {
        DatabricksErrorHelper::invalid_state()
            .message("No query set")
            .to_adbc()
    })?;

    // Handle bind_stream: execute once per row in the stream
    if let Some(stream) = self.bound_stream.take() {
        return self.execute_with_stream(query, stream);
    }

    // Convert bound parameters (if any) to SEA format
    let parameters = match self.bound_params.take() {
        Some(batch) => record_batch_to_sea_parameters(&batch)
            .map_err(|e| e.to_adbc())?,
        None => vec![],
    };

    let mut exec_params = ExecuteParams::default();
    exec_params.parameters = parameters;

    let result = self
        .runtime_handle
        .block_on(self.client.execute_statement(
            &self.session_id,
            query,
            &exec_params,
        ))
        .map_err(|e| e.to_adbc())?;

    self.current_statement_id = Some(result.statement_id);

    ResultReaderAdapter::new(result.reader, result.manifest.as_ref())
        .map_err(|e| e.to_adbc())
}
```

#### `execute_with_stream()` — New Private Method

Executes the query once for each row in the bound stream, concatenating results.

```rust
/// Execute a parameterized query once per row in the bound stream.
///
/// Each row in the stream becomes a separate SEA API call with its own
/// set of parameters. Results from all executions are concatenated.
fn execute_with_stream(
    &mut self,
    query: &str,
    stream: Box<dyn RecordBatchReader + Send>,
) -> Result<impl RecordBatchReader + Send> {
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut result_schema: Option<Arc<Schema>> = None;

    for batch_result in stream {
        let batch = batch_result.map_err(|e| {
            DatabricksErrorHelper::io()
                .message(format!("Error reading parameter stream: {}", e))
                .to_adbc()
        })?;

        // Execute once per row in this batch
        for row_idx in 0..batch.num_rows() {
            let row_batch = batch.slice(row_idx, 1);
            let parameters = record_batch_to_sea_parameters(&row_batch)
                .map_err(|e| e.to_adbc())?;

            let mut exec_params = ExecuteParams::default();
            exec_params.parameters = parameters;

            let result = self
                .runtime_handle
                .block_on(self.client.execute_statement(
                    &self.session_id,
                    query,
                    &exec_params,
                ))
                .map_err(|e| e.to_adbc())?;

            self.current_statement_id = Some(result.statement_id.clone());

            // Collect result batches
            let reader = ResultReaderAdapter::new(
                result.reader,
                result.manifest.as_ref(),
            ).map_err(|e| e.to_adbc())?;

            if result_schema.is_none() {
                result_schema = Some(reader.schema());
            }

            for rb in reader {
                let rb = rb.map_err(|e| {
                    DatabricksErrorHelper::io()
                        .message(format!("Error reading results: {}", e))
                        .to_adbc()
                })?;
                all_batches.push(rb);
            }
        }
    }

    // Return a RecordBatchReader over the collected results
    let schema = result_schema.unwrap_or_else(|| Arc::new(Schema::empty()));
    Ok(arrow_array::RecordBatchIterator::new(
        all_batches.into_iter().map(Ok),
        schema,
    ))
}
```

### 5. `execute_schema()` — Fix with `DESCRIBE QUERY`

The current `execute_schema()` implementation is broken — it does a full `execute()` and extracts the schema from the result reader. This violates the ADBC spec which states:

> Get the schema of the result set of a query **without executing it**.

For DML statements (INSERT/UPDATE/DELETE), this causes unintended data mutations. It also wastes resources for SELECT queries.

**Fix**: Use Databricks' `DESCRIBE QUERY` statement to get result metadata without execution.

```rust
fn execute_schema(&mut self) -> Result<Schema> {
    let query = self.query.as_ref().ok_or_else(|| {
        DatabricksErrorHelper::invalid_state()
            .message("No query set")
            .to_adbc()
    })?;

    // Build DESCRIBE QUERY, replacing ? markers with '?' to avoid parse errors
    let safe_query = replace_parameter_markers_with_literals(query);
    let describe_sql = format!("DESCRIBE QUERY {}", safe_query);

    let result = self
        .runtime_handle
        .block_on(self.client.execute_statement(
            &self.session_id,
            &describe_sql,
            &ExecuteParams::default(),
        ))
        .map_err(|e| e.to_adbc())?;

    // DESCRIBE QUERY returns rows with (col_name, data_type) columns.
    // Convert these into an Arrow Schema.
    let reader = ResultReaderAdapter::new(result.reader, result.manifest.as_ref())
        .map_err(|e| e.to_adbc())?;

    describe_result_to_schema(reader)
}
```

#### `replace_parameter_markers_with_literals()`

Reuses the same SQL parser state machine from `count_parameter_markers()`, but instead of counting, replaces each `?` in normal context with `'?'` (a string literal). This prevents the server from rejecting the DESCRIBE QUERY due to unbound parameter markers.

```rust
/// Replace parameter markers (?) with string literals ('?') for DESCRIBE QUERY.
/// Uses the same state machine as count_parameter_markers() to skip
/// markers inside string literals and comments.
fn replace_parameter_markers_with_literals(sql: &str) -> String { ... }
```

Reference: JDBC's `DatabricksPreparedStatement.surroundPlaceholdersWithQuotes()` does the same transformation.

#### `describe_result_to_schema()`

Converts the DESCRIBE QUERY result (rows of `col_name STRING, data_type STRING`) into an Arrow `Schema`:

```rust
/// Convert DESCRIBE QUERY result rows into an Arrow Schema.
///
/// Each row has (col_name, data_type). Map Databricks type names
/// back to Arrow DataTypes (reverse of the arrow_type_to_databricks_type mapping).
fn describe_result_to_schema(
    reader: impl RecordBatchReader,
) -> Result<Schema> { ... }
```

This requires a reverse mapping from Databricks type names to Arrow types:

| Databricks Type | Arrow DataType |
|---|---|
| `BOOLEAN` | `Boolean` |
| `TINYINT` | `Int8` |
| `SMALLINT` | `Int16` |
| `INT` | `Int32` |
| `BIGINT` | `Int64` |
| `FLOAT` | `Float32` |
| `DOUBLE` | `Float64` |
| `DECIMAL(p,s)` | `Decimal128(p, s)` |
| `STRING` | `Utf8` |
| `BINARY` | `Binary` |
| `DATE` | `Date32` |
| `TIMESTAMP` | `Timestamp(Microsecond, None)` |
| `TIMESTAMP_NTZ` | `Timestamp(Microsecond, None)` |
| `ARRAY<...>` | `Utf8` (opaque) |
| `MAP<...>` | `Utf8` (opaque) |
| `STRUCT<...>` | `Utf8` (opaque) |

### 6. SeaClient Changes

The `call_execute_api` method needs to pass parameters through to the request body. This is minimal — just plumb the `parameters` from `ExecuteParams` into `ExecuteStatementRequest`.

```rust
// client/sea.rs — call_execute_api()

let request_body = ExecuteStatementRequest {
    // ... existing fields ...
    parameters: params.parameters.clone(),
};
```

No changes to the `DatabricksClient` trait signature — `ExecuteParams` already flows through.

## SQL Parameter Marker Parsing

The parser is a simple state machine over the SQL string:

```
States: Normal, InSingleQuote, InLineComment, InBlockComment

Transitions:
  Normal:
    '?'        → count++, stay in Normal
    '\''       → InSingleQuote
    '--'       → InLineComment
    '/*'       → InBlockComment

  InSingleQuote:
    '\'\''     → stay (escaped quote)
    '\''       → Normal

  InLineComment:
    '\n'       → Normal

  InBlockComment:
    '*/'       → Normal
```

This matches the JDBC driver's approach. We do NOT handle:
- Double-quoted identifiers (`"column?"`) — `?` is not valid in identifiers
- Dollar-quoted strings — Databricks SQL doesn't use them

## Arrow Type Mapping

| Arrow DataType | Databricks Type | Value Serialization |
|---|---|---|
| `Boolean` | `BOOLEAN` | `"true"` / `"false"` |
| `Int8` | `TINYINT` | `"42"` |
| `Int16` | `SMALLINT` | `"42"` |
| `Int32` | `INT` | `"42"` |
| `Int64` | `BIGINT` | `"42"` |
| `Float16` | `FLOAT` | `"3.14"` |
| `Float32` | `FLOAT` | `"3.14"` |
| `Float64` | `DOUBLE` | `"3.14"` |
| `Decimal128(p, s)` | `DECIMAL(p,s)` | `"123.45"` |
| `Utf8` / `LargeUtf8` | `STRING` | value as-is |
| `Binary` / `LargeBinary` | `BINARY` | hex-encoded |
| `Date32` | `DATE` | `"2024-01-15"` |
| `Timestamp(_, None)` | `TIMESTAMP` | `"2024-01-15T10:30:00"` |
| `Timestamp(_, Some(tz))` | `TIMESTAMP` | `"2024-01-15T10:30:00"` (UTC-normalized) |
| `Null` | `STRING` | always `None` (SQL NULL) |

This mapping follows the JDBC driver's `DatabricksTypeUtil` patterns and the SEA API's type system.

## Lifecycle & State Transitions

```
┌─ Statement::new() ──────────────────────────────────────────────┐
│  query: None, is_prepared: false, bound_params: None            │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─ set_sql_query("SELECT * FROM t WHERE id = ?") ────────────────┐
│  query: Some("..."), is_prepared: false                         │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─ prepare() ─────────────────────────────────────────────────────┐
│  is_prepared: true, parameter_count: Some(1)                    │
│  (client-side only — parses SQL for ? markers, no server call)  │
└─────────────────────────────────────────────────────────────────┘
        │
        ├──── get_parameter_schema() → Schema with 1 Null field
        │
        ▼
┌─ bind(RecordBatch) ────────────────────────────────────────────┐
│  bound_params: Some(batch), validates column count == 1         │
└─────────────────────────────────────────────────────────────────┘
        │
        ├──── get_parameter_schema() → Schema from bound batch
        │
        ▼
┌─ execute() ─────────────────────────────────────────────────────┐
│  1. Converts bound RecordBatch → Vec<StatementParameter>        │
│  2. Sends to SEA API with parameters array                      │
│  3. Clears bound_params (consumed)                              │
│  4. Returns ResultReaderAdapter                                 │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼  (can re-bind and re-execute)
┌─ bind(new_batch) → execute() ──────────────────────────────────┐
│  Same flow — reuses prepared state, new parameters              │
└─────────────────────────────────────────────────────────────────┘
```

### bind_stream() Flow

```
┌─ bind_stream(reader) ──────────────────────────────────────────┐
│  bound_stream: Some(reader)                                     │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─ execute() ─────────────────────────────────────────────────────┐
│  For each row in stream:                                        │
│    1. Slice row from batch                                      │
│    2. Convert to Vec<StatementParameter>                        │
│    3. Execute SEA API call                                      │
│    4. Collect result batches                                    │
│  Return concatenated results                                    │
└─────────────────────────────────────────────────────────────────┘
```

### Unprepared Bind

`bind()` and `bind_stream()` work without `prepare()`. In that case, parameter count validation is skipped — the column count of the bound data determines the parameter count. This is valid per the ADBC spec and useful when the caller knows the parameter count without needing the driver to parse SQL.

## ODBC Bridge Integration (Future — Not in This PR)

For reference, the ODBC bridge will need these changes in a follow-up:

| ODBC Function | ADBC Function | Notes |
|---|---|---|
| `SQLBindParameter()` | `AdbcStatementBind()` | Build Arrow RecordBatch from C-type parameters |
| `SQLNumParams()` | `AdbcStatementGetParameterSchema()` | Return `schema.n_children` |
| `SQLDescribeParam()` | `AdbcStatementGetParameterSchema()` | Return type from `schema.children[i]` |

The bridge already handles `AdbcStatementPrepare()` returning `NOT_IMPLEMENTED` via fallback. Once the ADBC driver implements `prepare()`, the bridge will get schema metadata pre-execution.

## Testing Strategy

### Unit Tests

1. **SQL parser**: `count_parameter_markers()` with various SQL inputs:
   - Simple: `SELECT * FROM t WHERE id = ?` → 1
   - Multiple: `INSERT INTO t VALUES (?, ?, ?)` → 3
   - In string literal: `SELECT '?' FROM t` → 0
   - In line comment: `SELECT -- ? comment\n * FROM t WHERE id = ?` → 1
   - In block comment: `SELECT /* ? */ * FROM t WHERE id = ?` → 1
   - Escaped quote: `SELECT * FROM t WHERE name = '?''s value' AND id = ?` → 1
   - No markers: `SELECT 1` → 0

2. **Arrow → SEA conversion**: `record_batch_to_sea_parameters()`:
   - Int32 column → `{"ordinal":1, "type":"INT", "value":"42"}`
   - Utf8 column → `{"ordinal":1, "type":"STRING", "value":"hello"}`
   - Null value → `{"ordinal":1, "type":"STRING", "value":null}`
   - Decimal128(10,2) → `{"ordinal":1, "type":"DECIMAL(10,2)", "value":"123.45"}`
   - Multi-column batch → correct ordinals (1-based)
   - Multi-row batch → error (must be single row)

3. **Statement state machine**:
   - `prepare()` without `set_sql_query()` → error
   - `bind()` with wrong column count after `prepare()` → error
   - `bind()` without `prepare()` → OK (no count validation)
   - `get_parameter_schema()` after `prepare()` → Null-typed fields
   - `get_parameter_schema()` after `bind()` → bound schema
   - `execute()` consumes bound params (second `execute()` has no params)

4. **Request serialization**: `ExecuteStatementRequest` with parameters serializes correctly to JSON.

5. **`replace_parameter_markers_with_literals()`**:
   - `SELECT * FROM t WHERE id = ?` → `SELECT * FROM t WHERE id = '?'`
   - `SELECT '?' FROM t WHERE id = ?` → `SELECT '?' FROM t WHERE id = '?'` (literal stays, marker replaced)
   - No markers: `SELECT 1` → `SELECT 1` (unchanged)

6. **`describe_result_to_schema()`**: Databricks type strings → Arrow DataTypes:
   - `INT` → `Int32`, `STRING` → `Utf8`, `DECIMAL(10,2)` → `Decimal128(10,2)`, etc.

### E2E Tests (Ignored, Require Databricks Connection)

1. Simple parameterized SELECT: `SELECT ? AS value` with INT parameter
2. Parameterized WHERE clause: `SELECT * FROM t WHERE id = ?`
3. Multiple parameters: `SELECT * FROM t WHERE id = ? AND name = ?`
4. NULL parameter: bind a null value
5. `bind_stream()` with multiple rows → multiple results
6. Type coverage: BOOLEAN, INT, BIGINT, DOUBLE, STRING, DATE, TIMESTAMP, DECIMAL
7. `execute_schema()` returns correct schema via DESCRIBE QUERY (no side effects)
8. `execute_schema()` on a parameterized query returns schema with `?` markers safely handled

## Files Changed

| File | Change |
|---|---|
| `src/types/sea.rs` | Add `StatementParameter`, `parameters` field on `ExecuteStatementRequest` and `ExecuteParams` |
| `src/statement.rs` | Add fields, implement `prepare`/`bind`/`bind_stream`/`get_parameter_schema`, fix `execute_schema`, update `execute` |
| `src/statement/params.rs` (new) | `count_parameter_markers()`, `replace_parameter_markers_with_literals()`, `record_batch_to_sea_parameters()`, `describe_result_to_schema()`, Arrow type mapping (forward + reverse) |
| `src/client/sea.rs` | Pass `parameters` from `ExecuteParams` to `ExecuteStatementRequest` |

## Out of Scope

- **Server-side PREPARE** — SEA API has no PREPARE endpoint
- **Interpolation mode** — inlining parameter values into SQL string (JDBC's `supportManyParameters` workaround for the 256-param limit)
- **Batch INSERT optimization** — combining multiple `bind_stream` rows into a single multi-row INSERT
- **ODBC bridge wiring** — `SQLBindParameter`/`SQLNumParams`/`SQLDescribeParam` in `databricks-odbc`
- **Named parameters** — SEA supports `:paramName` syntax but ADBC uses positional only
