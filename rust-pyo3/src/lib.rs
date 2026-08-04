// Copyright (c) 2026 ADBC Drivers Contributors
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

//! POC PyO3 bindings for the Databricks ADBC Rust kernel.
//!
//! Wraps the kernel's ADBC `Optionable` layer (string-keyed config) and
//! exposes a streaming `Connection` / `ResultSet` surface to Python.
//! `execute()` returns as soon as the schema is available; rows are pulled
//! lazily via `fetch_next_batch()` (or all at once via `fetch_all_arrow()`).

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection as _, Database as _, Driver as _, Optionable, Statement as _};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_pyarrow::ToPyArrow;
use arrow_schema::Schema;
use databricks_adbc::{Connection, Driver, Statement};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;

fn adbc_err_to_py(err: adbc_core::error::Error) -> PyErr {
    PyRuntimeError::new_err(format!("databricks-adbc: {err}"))
}

/// Python-facing connection. Owns the kernel's `Database` + `Connection`.
///
/// `Database` must outlive any `Connection`s it spawned, so we keep both here.
#[pyclass(name = "Connection", unsendable)]
struct PyConnection {
    // Order matters for drop: connection drops before database.
    connection: Connection,
    // Held to keep the Database alive for the connection's lifetime.
    _database: databricks_adbc::Database,
}

#[pymethods]
impl PyConnection {
    /// Open a Databricks connection using a Personal Access Token.
    #[new]
    #[pyo3(signature = (host, http_path, access_token, *, catalog=None, schema=None))]
    fn new(
        host: String,
        http_path: String,
        access_token: String,
        catalog: Option<String>,
        schema: Option<String>,
    ) -> PyResult<Self> {
        let host = if host.starts_with("http://") || host.starts_with("https://") {
            host
        } else {
            format!("https://{host}")
        };

        let mut driver = Driver::new();
        let mut database = driver.new_database().map_err(adbc_err_to_py)?;

        // POC is PAT-only — `databricks.auth.type=access_token` is required.
        let mut opts: Vec<(OptionDatabase, OptionValue)> = vec![
            (OptionDatabase::Uri, OptionValue::String(host)),
            (
                OptionDatabase::Other("databricks.http_path".into()),
                OptionValue::String(http_path),
            ),
            (
                OptionDatabase::Other("databricks.auth.type".into()),
                OptionValue::String("access_token".into()),
            ),
            (
                OptionDatabase::Other("databricks.access_token".into()),
                OptionValue::String(access_token),
            ),
        ];
        if let Some(c) = catalog {
            opts.push((
                OptionDatabase::Other("databricks.catalog".into()),
                OptionValue::String(c),
            ));
        }
        if let Some(s) = schema {
            opts.push((
                OptionDatabase::Other("databricks.schema".into()),
                OptionValue::String(s),
            ));
        }
        for (k, v) in opts {
            database.set_option(k, v).map_err(adbc_err_to_py)?;
        }

        let connection = database.new_connection().map_err(adbc_err_to_py)?;
        Ok(Self {
            connection,
            _database: database,
        })
    }

    /// Execute a SQL query and return a streaming `ResultSet`.
    ///
    /// Returns once the kernel has the schema (typically after the first
    /// network round-trip). Rows are not yet materialized — pull them with
    /// `fetch_next_batch()` or drain in one shot with `fetch_all_arrow()`.
    fn execute(&mut self, sql: &str) -> PyResult<PyResultSet> {
        let mut statement: Statement = self.connection.new_statement().map_err(adbc_err_to_py)?;
        statement.set_sql_query(sql).map_err(adbc_err_to_py)?;

        // execute_owned returns Box<dyn RecordBatchReader + Send + 'static>,
        // so the reader's lifetime is independent of the Statement (which we
        // drop immediately after this call). Releases the GIL while waiting
        // on the first round-trip with the SEA endpoint.
        let reader = Python::attach(|py| {
            py.detach(|| -> Result<_, adbc_core::error::Error> { statement.execute_owned() })
        })
        .map_err(adbc_err_to_py)?;

        let schema = reader.schema();
        Ok(PyResultSet {
            schema,
            reader: Some(reader),
        })
    }
}

/// A streaming result set. Holds the kernel's `RecordBatchReader` and produces
/// `pyarrow.RecordBatch` / `pyarrow.Table` lazily.
#[pyclass(name = "ResultSet", unsendable)]
struct PyResultSet {
    schema: Arc<Schema>,
    reader: Option<Box<dyn RecordBatchReader + Send + 'static>>,
}

impl PyResultSet {
    fn next_batch(&mut self) -> PyResult<Option<RecordBatch>> {
        let reader = match self.reader.as_mut() {
            Some(r) => r,
            None => return Ok(None),
        };
        let next = Python::attach(|py| py.detach(|| reader.next()));
        match next {
            None => {
                // Eagerly drop the reader on exhaustion so the kernel can
                // release server-side resources without waiting for close().
                self.reader = None;
                Ok(None)
            }
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(PyRuntimeError::new_err(format!(
                "databricks-adbc: arrow read error: {e}"
            ))),
        }
    }
}

#[pymethods]
impl PyResultSet {
    fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    fn column_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// Return the result schema as a `pyarrow.Schema`.
    fn arrow_schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.schema.as_ref().to_pyarrow(py)
    }

    /// Pull the next batch as a `pyarrow.RecordBatch`, or `None` when exhausted.
    fn fetch_next_batch<'py>(
        &mut self,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        match self.next_batch()? {
            Some(batch) => Ok(Some(batch.to_pyarrow(py)?)),
            None => Ok(None),
        }
    }

    /// Drain the result set and return everything as a `pyarrow.Table`.
    /// Subsequent fetches return None / an empty table.
    ///
    /// The drain phase runs inside a single `py.detach` block so the GIL is
    /// released for the entire kernel-side fetch (network + Arrow IPC parse +
    /// LZ4 decode), then reacquired once for the pyarrow conversion phase.
    fn fetch_all_arrow<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Drain all remaining batches with the GIL released. One attach/detach
        // pair instead of one per batch.
        let result: Result<Vec<RecordBatch>, arrow_schema::ArrowError> = py.detach(|| {
            let mut all: Vec<RecordBatch> = Vec::new();
            if let Some(reader) = self.reader.as_mut() {
                for item in reader.by_ref() {
                    all.push(item?);
                }
            }
            Ok(all)
        });
        let all = result.map_err(|e| {
            PyRuntimeError::new_err(format!("databricks-adbc: arrow read error: {e}"))
        })?;
        // Reader is exhausted; drop it now to release kernel-side resources.
        self.reader = None;

        // Convert all batches to pyarrow holding the GIL throughout. The
        // pyarrow C Data Interface is zero-copy for the buffers; the per-batch
        // overhead here is just creating the Python wrappers.
        let pa = py.import("pyarrow")?;
        let py_batches: Vec<Bound<'py, PyAny>> = all
            .into_iter()
            .map(|b| b.to_pyarrow(py))
            .collect::<PyResult<_>>()?;
        let py_schema = self.schema.as_ref().to_pyarrow(py)?;
        pa.getattr("Table")?
            .call_method1("from_batches", (py_batches, py_schema))
    }

    /// True once the underlying reader has been exhausted.
    fn is_exhausted(&self) -> bool {
        self.reader.is_none()
    }
}

#[pymodule]
fn databricks_adbc_pyo3(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyConnection>()?;
    m.add_class::<PyResultSet>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
