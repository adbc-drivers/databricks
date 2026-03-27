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

//! `extern "C"` metadata functions exposed via the Arrow C Data Interface.
//!
//! Each function follows this pattern:
//! 1. Clear thread-local error buffer
//! 2. Wrap body in `catch_unwind` (panics must not cross FFI boundary)
//! 3. Validate connection pointer (null check)
//! 4. Convert C strings to Rust `&str`
//! 5. Call client method via `Connection`
//! 6. Export result via `FFI_ArrowArrayStream`
//! 7. Return status code; on error, set thread-local error buffer

use crate::ffi::error::{clear_last_error, set_error_from_result, set_last_error, FfiStatus};
use crate::metadata::sql::SqlCommandBuilder;
use crate::reader::{EmptyReader, ResultReader, ResultReaderAdapter};
use crate::types::sea::{ExecuteParams, ResultManifest};
use crate::Connection;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;
use std::ffi::{c_char, c_void, CStr};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

/// Convert a nullable C string pointer to an `Option<&str>`.
///
/// Returns `None` if the pointer is null.
/// Returns an error status if the string is not valid UTF-8.
///
/// # Safety
///
/// `ptr` must be null or point to a valid null-terminated C string.
/// The returned reference borrows the C string and is only valid as long as
/// the caller keeps the C string alive.
unsafe fn c_str_to_option<'a>(ptr: *const c_char) -> std::result::Result<Option<&'a str>, ()> {
    if ptr.is_null() {
        Ok(None)
    } else {
        match CStr::from_ptr(ptr).to_str() {
            Ok(s) => Ok(Some(s)),
            Err(_) => {
                set_last_error("Invalid UTF-8 in string argument", "HY090", -1);
                Err(())
            }
        }
    }
}

/// Convert a non-nullable C string pointer to `&str`.
///
/// Returns an error status if the pointer is null or the string is not valid UTF-8.
///
/// # Safety
///
/// `ptr` must be null or point to a valid null-terminated C string.
/// The returned reference borrows the C string and is only valid as long as
/// the caller keeps the C string alive.
unsafe fn c_str_to_str<'a>(ptr: *const c_char) -> std::result::Result<&'a str, ()> {
    if ptr.is_null() {
        set_last_error("Required string argument is null", "HY009", -1);
        Err(())
    } else {
        match CStr::from_ptr(ptr).to_str() {
            Ok(s) => Ok(s),
            Err(_) => {
                set_last_error("Invalid UTF-8 in string argument", "HY090", -1);
                Err(())
            }
        }
    }
}

/// Export a `ResultReader` as an `FFI_ArrowArrayStream` via `ResultReaderAdapter`.
///
/// When `manifest` is provided, `databricks.*` field-level metadata from the SEA
/// manifest is attached to the Arrow schema. Pass `None` for paths that construct
/// a reader directly (e.g. `EmptyReader`).
///
/// The caller is responsible for releasing the stream.
fn export_reader(
    reader: Box<dyn ResultReader + Send>,
    manifest: Option<&ResultManifest>,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    if out.is_null() {
        set_last_error("Output stream pointer is null", "HY009", -1);
        return FfiStatus::InvalidHandle;
    }

    let adapter = match ResultReaderAdapter::new(reader, manifest) {
        Ok(a) => a,
        Err(e) => return set_error_from_result(&e),
    };

    let boxed: Box<dyn RecordBatchReader + Send> = Box::new(adapter);
    let stream = FFI_ArrowArrayStream::new(boxed);
    unsafe {
        std::ptr::write(out, stream);
    }
    FfiStatus::Success
}

/// Validate connection pointer and recover `&Connection`, or return error status.
macro_rules! get_connection {
    ($conn:expr) => {
        if $conn.is_null() {
            set_last_error("Null connection pointer", "08003", -1);
            return FfiStatus::InvalidHandle;
        } else {
            unsafe { &*($conn as *const Connection) }
        }
    };
}

/// Handle a panic from `catch_unwind` by setting the error buffer and returning Error.
fn handle_panic(panic: Box<dyn std::any::Any + Send>) -> FfiStatus {
    let msg = if let Some(s) = panic.downcast_ref::<&str>() {
        format!("Internal panic: {}", s)
    } else if let Some(s) = panic.downcast_ref::<String>() {
        format!("Internal panic: {}", s)
    } else {
        "Internal panic (unknown cause)".to_string()
    };
    set_last_error(&msg, "HY000", -1);
    FfiStatus::Error
}

// ─── Exported FFI Functions ───────────────────────────────────────────────────

/// List catalogs.
///
/// # Safety
///
/// - `conn` must be a valid pointer to a `Connection` (e.g. from `adbc_connection_.private_data`)
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_catalogs(
    conn: *const c_void,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let conn = get_connection!(conn);
        match conn
            .runtime_handle()
            .block_on(conn.client().list_catalogs(conn.session_id()))
        {
            Ok(result) => export_reader(result.reader, result.manifest.as_ref(), out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

/// List schemas.
///
/// # Safety
///
/// - `conn` must be a valid pointer to a `Connection`
/// - String arguments may be null (treated as no filter)
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_schemas(
    conn: *const c_void,
    catalog: *const c_char,
    schema_pattern: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let conn = get_connection!(conn);
        let Ok(catalog) = (unsafe { c_str_to_option(catalog) }) else {
            return FfiStatus::Error;
        };
        let Ok(schema_pattern) = (unsafe { c_str_to_option(schema_pattern) }) else {
            return FfiStatus::Error;
        };

        match conn.runtime_handle().block_on(conn.client().list_schemas(
            conn.session_id(),
            catalog,
            schema_pattern,
        )) {
            Ok(result) => export_reader(result.reader, result.manifest.as_ref(), out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

/// List tables matching the given filter criteria.
///
/// # Safety
///
/// - `conn` must be a valid pointer to a `Connection`
/// - String arguments may be null (treated as no filter)
/// - `table_types` is a comma-separated list if non-null
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_tables(
    conn: *const c_void,
    catalog: *const c_char,
    schema_pattern: *const c_char,
    table_pattern: *const c_char,
    table_types: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let conn = get_connection!(conn);
        let Ok(catalog) = (unsafe { c_str_to_option(catalog) }) else {
            return FfiStatus::Error;
        };
        let Ok(schema_pattern) = (unsafe { c_str_to_option(schema_pattern) }) else {
            return FfiStatus::Error;
        };
        let Ok(table_pattern) = (unsafe { c_str_to_option(table_pattern) }) else {
            return FfiStatus::Error;
        };
        let Ok(table_types_str) = (unsafe { c_str_to_option(table_types) }) else {
            return FfiStatus::Error;
        };

        // Parse comma-separated table types
        let types_vec: Option<Vec<&str>> =
            table_types_str.map(|s| s.split(',').map(|t| t.trim()).collect());

        match conn.runtime_handle().block_on(conn.client().list_tables(
            conn.session_id(),
            catalog,
            schema_pattern,
            table_pattern,
            types_vec.as_deref(),
        )) {
            Ok(result) => export_reader(result.reader, result.manifest.as_ref(), out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

/// List columns matching the given filter criteria.
///
/// When catalog is null or empty, returns an empty result set (since
/// `SHOW COLUMNS IN ALL CATALOGS` is not supported by Databricks).
///
/// # Safety
///
/// - `conn` must be a valid pointer to a `Connection`
/// - String arguments may be null (treated as no filter)
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_columns(
    conn: *const c_void,
    catalog: *const c_char,
    schema_pattern: *const c_char,
    table_pattern: *const c_char,
    column_pattern: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let conn = get_connection!(conn);
        let Ok(catalog) = (unsafe { c_str_to_option(catalog) }) else {
            return FfiStatus::Error;
        };
        let Ok(schema_pattern) = (unsafe { c_str_to_option(schema_pattern) }) else {
            return FfiStatus::Error;
        };
        let Ok(table_pattern) = (unsafe { c_str_to_option(table_pattern) }) else {
            return FfiStatus::Error;
        };
        let Ok(column_pattern) = (unsafe { c_str_to_option(column_pattern) }) else {
            return FfiStatus::Error;
        };

        // Catalog is required for SHOW COLUMNS; return empty stream when absent.
        let catalog = match catalog.filter(|c| !c.is_empty()) {
            Some(c) => c,
            None => {
                let reader: Box<dyn ResultReader + Send> =
                    Box::new(EmptyReader::new(Arc::new(Schema::empty())));
                return export_reader(reader, None, out);
            }
        };

        match conn.runtime_handle().block_on(conn.client().list_columns(
            conn.session_id(),
            catalog,
            schema_pattern,
            table_pattern,
            column_pattern,
        )) {
            Ok(result) => export_reader(result.reader, result.manifest.as_ref(), out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

/// List primary key columns for a table.
///
/// # Safety
///
/// - `conn` must be a valid pointer to a `Connection`
/// - `catalog`, `schema`, `table` must be valid non-null C strings
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_primary_keys(
    conn: *const c_void,
    catalog: *const c_char,
    schema: *const c_char,
    table: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let conn = get_connection!(conn);
        let Ok(catalog) = (unsafe { c_str_to_str(catalog) }) else {
            return FfiStatus::Error;
        };
        let Ok(schema) = (unsafe { c_str_to_str(schema) }) else {
            return FfiStatus::Error;
        };
        let Ok(table) = (unsafe { c_str_to_str(table) }) else {
            return FfiStatus::Error;
        };

        let sql = SqlCommandBuilder::build_show_primary_keys(catalog, schema, table);
        match conn
            .runtime_handle()
            .block_on(conn.client().execute_statement(
                conn.session_id(),
                &sql,
                &ExecuteParams::default(),
            )) {
            Ok(result) => export_reader(result.reader, result.manifest.as_ref(), out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

/// List foreign key columns for a table.
///
/// # Safety
///
/// - `conn` must be a valid pointer to a `Connection`
/// - `catalog`, `schema`, `table` must be valid non-null C strings
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_foreign_keys(
    conn: *const c_void,
    catalog: *const c_char,
    schema: *const c_char,
    table: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let conn = get_connection!(conn);
        let Ok(catalog) = (unsafe { c_str_to_str(catalog) }) else {
            return FfiStatus::Error;
        };
        let Ok(schema) = (unsafe { c_str_to_str(schema) }) else {
            return FfiStatus::Error;
        };
        let Ok(table) = (unsafe { c_str_to_str(table) }) else {
            return FfiStatus::Error;
        };

        let sql = SqlCommandBuilder::build_show_foreign_keys(catalog, schema, table);
        match conn
            .runtime_handle()
            .block_on(conn.client().execute_statement(
                conn.session_id(),
                &sql,
                &ExecuteParams::default(),
            )) {
            Ok(result) => export_reader(result.reader, result.manifest.as_ref(), out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

/// List procedures matching the given filter criteria.
///
/// Uses `information_schema.routines` filtered by `routine_type = 'PROCEDURE'`.
/// When catalog is NULL, queries `system.information_schema` (cross-catalog).
/// When catalog is empty string, returns an empty result set.
///
/// # Safety
///
/// - `conn` must be a valid pointer to a `Connection`
/// - String arguments may be null (treated as no filter)
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_procedures(
    conn: *const c_void,
    catalog: *const c_char,
    schema_pattern: *const c_char,
    procedure_pattern: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let conn = get_connection!(conn);
        let Ok(catalog) = (unsafe { c_str_to_option(catalog) }) else {
            return FfiStatus::Error;
        };
        let Ok(schema_pattern) = (unsafe { c_str_to_option(schema_pattern) }) else {
            return FfiStatus::Error;
        };
        let Ok(procedure_pattern) = (unsafe { c_str_to_option(procedure_pattern) }) else {
            return FfiStatus::Error;
        };

        // Empty catalog → return empty result (no server round-trip).
        // ODBC layer constructs proper schema for the empty result set.
        if catalog == Some("") {
            let reader: Box<dyn ResultReader + Send> =
                Box::new(EmptyReader::new(Arc::new(Schema::empty())));
            return export_reader(reader, None, out);
        }

        match conn
            .runtime_handle()
            .block_on(conn.client().list_procedures(
                conn.session_id(),
                catalog,
                schema_pattern,
                procedure_pattern,
            )) {
            Ok(result) => export_reader(result.reader, result.manifest.as_ref(), out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

/// List procedure columns (parameters) matching the given filter criteria.
///
/// Uses `information_schema.parameters` joined with `information_schema.routines`.
/// When catalog is NULL, queries `system.information_schema` (cross-catalog).
/// When catalog is empty string, returns empty result with correct column schema.
///
/// # Safety
///
/// - `conn` must be a valid pointer to a `Connection`
/// - String arguments may be null (treated as no filter)
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_procedure_columns(
    conn: *const c_void,
    catalog: *const c_char,
    schema_pattern: *const c_char,
    procedure_pattern: *const c_char,
    column_pattern: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let conn = get_connection!(conn);
        let Ok(catalog) = (unsafe { c_str_to_option(catalog) }) else {
            return FfiStatus::Error;
        };
        let Ok(schema_pattern) = (unsafe { c_str_to_option(schema_pattern) }) else {
            return FfiStatus::Error;
        };
        let Ok(procedure_pattern) = (unsafe { c_str_to_option(procedure_pattern) }) else {
            return FfiStatus::Error;
        };
        let Ok(column_pattern) = (unsafe { c_str_to_option(column_pattern) }) else {
            return FfiStatus::Error;
        };

        // Empty catalog → return empty result (no server round-trip).
        // ODBC layer constructs proper schema for the empty result set.
        if catalog == Some("") {
            let reader: Box<dyn ResultReader + Send> =
                Box::new(EmptyReader::new(Arc::new(Schema::empty())));
            return export_reader(reader, None, out);
        }

        match conn
            .runtime_handle()
            .block_on(conn.client().list_procedure_columns(
                conn.session_id(),
                catalog,
                schema_pattern,
                procedure_pattern,
                column_pattern,
            )) {
            Ok(result) => export_reader(result.reader, result.manifest.as_ref(), out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::error::FfiError;
    use crate::reader::test_utils::{MockReader, SchemaErrorReader};
    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn test_export_reader_null_output() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap();

        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let status = export_reader(reader, None, std::ptr::null_mut());
        assert_eq!(status, FfiStatus::InvalidHandle);
    }

    #[test]
    fn test_export_reader_success() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap();

        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = export_reader(reader, None, &mut stream);
        assert_eq!(status, FfiStatus::Success);
    }

    #[test]
    fn test_export_reader_empty() {
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![]));
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = export_reader(reader, None, &mut stream);
        assert_eq!(status, FfiStatus::Success);
    }

    #[test]
    fn test_export_reader_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["a"]))])
                .unwrap();
        let batch2 =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["b", "c"]))])
                .unwrap();

        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch1, batch2]));
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = export_reader(reader, None, &mut stream);
        assert_eq!(status, FfiStatus::Success);
    }

    #[test]
    fn test_export_reader_schema_error() {
        let reader: Box<dyn ResultReader + Send> = Box::new(SchemaErrorReader);
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = export_reader(reader, None, &mut stream);
        assert_eq!(status, FfiStatus::Error);
    }

    #[test]
    fn test_handle_panic_with_str() {
        let panic = Box::new("something broke") as Box<dyn std::any::Any + Send>;
        let status = handle_panic(panic);
        assert_eq!(status, FfiStatus::Error);

        let mut err = FfiError::default();
        unsafe { crate::ffi::error::metadata_get_last_error(&mut err) };
        let msg: String = err
            .message
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as u8 as char)
            .collect();
        assert!(msg.contains("something broke"), "got: {}", msg);
    }

    #[test]
    fn test_handle_panic_with_string() {
        let panic = Box::new("owned panic message".to_string()) as Box<dyn std::any::Any + Send>;
        let status = handle_panic(panic);
        assert_eq!(status, FfiStatus::Error);

        let mut err = FfiError::default();
        unsafe { crate::ffi::error::metadata_get_last_error(&mut err) };
        let msg: String = err
            .message
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as u8 as char)
            .collect();
        assert!(msg.contains("owned panic message"), "got: {}", msg);
    }

    #[test]
    fn test_handle_panic_with_unknown_type() {
        let panic = Box::new(42i32) as Box<dyn std::any::Any + Send>;
        let status = handle_panic(panic);
        assert_eq!(status, FfiStatus::Error);

        let mut err = FfiError::default();
        unsafe { crate::ffi::error::metadata_get_last_error(&mut err) };
        let msg: String = err
            .message
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as u8 as char)
            .collect();
        assert!(msg.contains("unknown cause"), "got: {}", msg);
    }

    #[test]
    fn test_null_connection_returns_invalid_handle() {
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = unsafe { metadata_get_catalogs(std::ptr::null(), &mut stream) };
        assert_eq!(status, FfiStatus::InvalidHandle);

        // Error should be set
        let mut err = FfiError::default();
        unsafe { crate::ffi::error::metadata_get_last_error(&mut err) };
        let msg: String = err
            .message
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as u8 as char)
            .collect();
        assert!(
            msg.contains("Null connection pointer"),
            "Expected null pointer error, got: {}",
            msg
        );
    }

    #[test]
    fn test_c_str_to_option_null() {
        let result = unsafe { c_str_to_option(std::ptr::null()) };
        assert_eq!(result, Ok(None));
    }

    #[test]
    fn test_c_str_to_option_valid() {
        let s = std::ffi::CString::new("hello").unwrap();
        let result = unsafe { c_str_to_option(s.as_ptr()) };
        assert_eq!(result, Ok(Some("hello")));
    }

    #[test]
    fn test_c_str_to_str_null() {
        let result = unsafe { c_str_to_str(std::ptr::null()) };
        assert!(result.is_err());
    }

    #[test]
    fn test_c_str_to_str_valid() {
        let s = std::ffi::CString::new("world").unwrap();
        let result = unsafe { c_str_to_str(s.as_ptr()) };
        assert_eq!(result, Ok("world"));
    }

    #[test]
    fn test_clear_error_on_entry() {
        // Set an error, then call a function that clears it at entry
        crate::ffi::error::set_last_error("stale error", "HY000", -1);

        // Call with null connection — clears the error first, then sets a new one
        let mut stream = FFI_ArrowArrayStream::empty();
        let _status = unsafe { metadata_get_catalogs(std::ptr::null(), &mut stream) };

        let mut err = FfiError::default();
        unsafe { crate::ffi::error::metadata_get_last_error(&mut err) };
        let msg: String = err
            .message
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as u8 as char)
            .collect();
        assert!(
            msg.contains("Null connection pointer"),
            "Expected connection error, got: {}",
            msg
        );
    }
}
