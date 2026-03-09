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

//! `extern "C"` catalog metadata functions.
//!
//! Each function follows this pattern:
//! 1. Clear thread-local error buffer
//! 2. Wrap body in `catch_unwind` (panics must not cross FFI boundary)
//! 3. Validate handle (null check)
//! 4. Recover `ConnectionMetadataService` from handle
//! 5. Convert C strings to Rust &str
//! 6. Call service method
//! 7. Export result via Arrow C Data Interface (`FFI_ArrowArrayStream`)
//! 8. Return status code; on error, set thread-local error buffer

use crate::ffi::error::{clear_last_error, set_error_from_result, set_last_error, FfiStatus};
use crate::ffi::handle::{handle_to_service, FfiConnectionHandle};
use crate::reader::{ResultReader, ResultReaderAdapter};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatchReader;
use std::ffi::{c_char, CStr};
use std::panic::AssertUnwindSafe;

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

/// Export a ResultReader as an FFI_ArrowArrayStream via ResultReaderAdapter.
///
/// The caller is responsible for releasing the stream.
fn export_reader(
    reader: Box<dyn ResultReader + Send>,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    if out.is_null() {
        set_last_error("Output stream pointer is null", "HY009", -1);
        return FfiStatus::InvalidHandle;
    }

    let adapter = match ResultReaderAdapter::new(reader) {
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

/// Validate handle and recover service, or return error status.
macro_rules! get_service {
    ($conn:expr) => {
        match unsafe { handle_to_service($conn) } {
            Some(svc) => svc,
            None => {
                set_last_error("Invalid connection handle", "08003", -1);
                return FfiStatus::InvalidHandle;
            }
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
/// - `conn` must be a valid handle from `metadata_connection_from_ref()`
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_catalogs(
    conn: FfiConnectionHandle,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let svc = get_service!(conn);
        match svc.get_catalogs() {
            Ok(reader) => export_reader(reader, out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

/// List schemas.
///
/// # Safety
///
/// - `conn` must be a valid handle from `metadata_connection_from_ref()`
/// - String arguments may be null (treated as no filter)
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_schemas(
    conn: FfiConnectionHandle,
    catalog: *const c_char,
    schema_pattern: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let svc = get_service!(conn);
        let Ok(catalog) = (unsafe { c_str_to_option(catalog) }) else {
            return FfiStatus::Error;
        };
        let Ok(schema_pattern) = (unsafe { c_str_to_option(schema_pattern) }) else {
            return FfiStatus::Error;
        };

        match svc.get_schemas(catalog, schema_pattern) {
            Ok(reader) => export_reader(reader, out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

/// List tables matching the given filter criteria.
///
/// Results are exported via the Arrow C Data Interface.
/// Caller must release the ArrowArrayStream when done.
///
/// # Safety
///
/// - `conn` must be a valid handle from `metadata_connection_from_ref()`
/// - String arguments may be null (treated as no filter)
/// - `table_types` is a comma-separated list if non-null
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_tables(
    conn: FfiConnectionHandle,
    catalog: *const c_char,
    schema_pattern: *const c_char,
    table_pattern: *const c_char,
    table_types: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let svc = get_service!(conn);
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

        match svc.get_tables(catalog, schema_pattern, table_pattern, types_vec.as_deref()) {
            Ok(reader) => export_reader(reader, out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

/// List columns matching the given filter criteria.
///
/// # Safety
///
/// - `conn` must be a valid handle from `metadata_connection_from_ref()`
/// - String arguments may be null (treated as no filter)
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_columns(
    conn: FfiConnectionHandle,
    catalog: *const c_char,
    schema_pattern: *const c_char,
    table_pattern: *const c_char,
    column_pattern: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let svc = get_service!(conn);
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

        match svc.get_columns(catalog, schema_pattern, table_pattern, column_pattern) {
            Ok(reader) => export_reader(reader, out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

/// List primary key columns for a table.
///
/// # Safety
///
/// - `conn` must be a valid handle from `metadata_connection_from_ref()`
/// - `catalog`, `schema`, `table` must be valid non-null C strings
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_primary_keys(
    conn: FfiConnectionHandle,
    catalog: *const c_char,
    schema: *const c_char,
    table: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let svc = get_service!(conn);
        let Ok(catalog) = (unsafe { c_str_to_str(catalog) }) else {
            return FfiStatus::Error;
        };
        let Ok(schema) = (unsafe { c_str_to_str(schema) }) else {
            return FfiStatus::Error;
        };
        let Ok(table) = (unsafe { c_str_to_str(table) }) else {
            return FfiStatus::Error;
        };

        match svc.get_primary_keys(catalog, schema, table) {
            Ok(reader) => export_reader(reader, out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

/// List foreign key columns for a table.
///
/// # Safety
///
/// - `conn` must be a valid handle from `metadata_connection_from_ref()`
/// - `catalog`, `schema`, `table` must be valid non-null C strings
/// - `out` must point to a valid, writable `FFI_ArrowArrayStream`
#[no_mangle]
pub unsafe extern "C" fn metadata_get_foreign_keys(
    conn: FfiConnectionHandle,
    catalog: *const c_char,
    schema: *const c_char,
    table: *const c_char,
    out: *mut FFI_ArrowArrayStream,
) -> FfiStatus {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let svc = get_service!(conn);
        let Ok(catalog) = (unsafe { c_str_to_str(catalog) }) else {
            return FfiStatus::Error;
        };
        let Ok(schema) = (unsafe { c_str_to_str(schema) }) else {
            return FfiStatus::Error;
        };
        let Ok(table) = (unsafe { c_str_to_str(table) }) else {
            return FfiStatus::Error;
        };

        match svc.get_foreign_keys(catalog, schema, table) {
            Ok(reader) => export_reader(reader, out),
            Err(e) => set_error_from_result(&e),
        }
    }))
    .unwrap_or_else(handle_panic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::error::FfiError;
    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use std::sync::Arc;

    /// A simple mock reader that returns predefined batches.
    struct MockReader {
        batches: Vec<RecordBatch>,
        index: usize,
        schema: SchemaRef,
    }

    impl MockReader {
        fn new(batches: Vec<RecordBatch>) -> Self {
            let schema = if batches.is_empty() {
                Arc::new(Schema::empty())
            } else {
                batches[0].schema()
            };
            Self {
                batches,
                index: 0,
                schema,
            }
        }
    }

    impl ResultReader for MockReader {
        fn schema(&self) -> crate::error::Result<SchemaRef> {
            Ok(self.schema.clone())
        }

        fn next_batch(&mut self) -> crate::error::Result<Option<RecordBatch>> {
            if self.index >= self.batches.len() {
                Ok(None)
            } else {
                let batch = self.batches[self.index].clone();
                self.index += 1;
                Ok(Some(batch))
            }
        }
    }

    /// A reader that fails on schema().
    struct SchemaErrorReader;

    impl ResultReader for SchemaErrorReader {
        fn schema(&self) -> crate::error::Result<SchemaRef> {
            Err(crate::error::DatabricksErrorHelper::io().message("schema unavailable"))
        }

        fn next_batch(&mut self) -> crate::error::Result<Option<RecordBatch>> {
            Ok(None)
        }
    }

    #[test]
    fn test_export_reader_null_output() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap();

        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let status = export_reader(reader, std::ptr::null_mut());
        assert_eq!(status, FfiStatus::InvalidHandle);
    }

    #[test]
    fn test_export_reader_success() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap();

        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = export_reader(reader, &mut stream);
        assert_eq!(status, FfiStatus::Success);
    }

    #[test]
    fn test_export_reader_empty() {
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![]));
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = export_reader(reader, &mut stream);
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

        let reader: Box<dyn ResultReader + Send> =
            Box::new(MockReader::new(vec![batch1, batch2]));
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = export_reader(reader, &mut stream);
        assert_eq!(status, FfiStatus::Success);
    }

    #[test]
    fn test_export_reader_schema_error() {
        let reader: Box<dyn ResultReader + Send> = Box::new(SchemaErrorReader);
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = export_reader(reader, &mut stream);
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
        let panic =
            Box::new("owned panic message".to_string()) as Box<dyn std::any::Any + Send>;
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
    fn test_null_handle_returns_invalid_handle() {
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = unsafe { metadata_get_catalogs(std::ptr::null_mut(), &mut stream) };
        assert_eq!(status, FfiStatus::InvalidHandle);

        // Error should be set
        let mut err = FfiError::default();
        unsafe { crate::ffi::error::metadata_get_last_error(&mut err) };
        assert_ne!(err.message[0], 0); // non-empty message
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

        // Call with null handle — this clears the error first, then sets a new one
        let mut stream = FFI_ArrowArrayStream::empty();
        let _status = unsafe { metadata_get_catalogs(std::ptr::null_mut(), &mut stream) };

        let mut err = FfiError::default();
        unsafe { crate::ffi::error::metadata_get_last_error(&mut err) };
        // The error should be about invalid handle, not "stale error"
        let msg: String = err
            .message
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as u8 as char)
            .collect();
        assert!(
            msg.contains("Invalid connection handle"),
            "Expected handle error, got: {}",
            msg
        );
    }
}
