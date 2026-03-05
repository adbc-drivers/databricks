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
//! 1. Validate handle (null check)
//! 2. Recover `ConnectionMetadataService` from handle
//! 3. Convert C strings to Rust &str
//! 4. Call service method
//! 5. Export result via Arrow C Data Interface (`FFI_ArrowArrayStream`)
//! 6. Return status code; on error, set thread-local error buffer

use crate::ffi::error::{set_error_from_result, set_last_error, FfiStatus};
use crate::ffi::handle::{handle_to_service, FfiConnectionHandle};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use std::ffi::{c_char, CStr};

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

/// Export a RecordBatch as an FFI_ArrowArrayStream.
///
/// The caller is responsible for releasing the stream.
fn export_batch(batch: RecordBatch, out: *mut FFI_ArrowArrayStream) -> FfiStatus {
    if out.is_null() {
        set_last_error("Output stream pointer is null", "HY009", -1);
        return FfiStatus::InvalidHandle;
    }

    let schema = batch.schema();
    let reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema,
    ));

    let stream = FFI_ArrowArrayStream::new(reader);
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
    let svc = get_service!(conn);

    match svc.get_catalogs() {
        Ok(batch) => export_batch(batch, out),
        Err(e) => set_error_from_result(&e),
    }
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
    let svc = get_service!(conn);
    let Ok(catalog) = c_str_to_option(catalog) else {
        return FfiStatus::Error;
    };
    let Ok(schema_pattern) = c_str_to_option(schema_pattern) else {
        return FfiStatus::Error;
    };

    match svc.get_schemas(catalog, schema_pattern) {
        Ok(batch) => export_batch(batch, out),
        Err(e) => set_error_from_result(&e),
    }
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
    let svc = get_service!(conn);
    let Ok(catalog) = c_str_to_option(catalog) else {
        return FfiStatus::Error;
    };
    let Ok(schema_pattern) = c_str_to_option(schema_pattern) else {
        return FfiStatus::Error;
    };
    let Ok(table_pattern) = c_str_to_option(table_pattern) else {
        return FfiStatus::Error;
    };
    let Ok(table_types_str) = c_str_to_option(table_types) else {
        return FfiStatus::Error;
    };

    // Parse comma-separated table types
    let types_vec: Option<Vec<&str>> =
        table_types_str.map(|s| s.split(',').map(|t| t.trim()).collect());

    match svc.get_tables(catalog, schema_pattern, table_pattern, types_vec.as_deref()) {
        Ok(batch) => export_batch(batch, out),
        Err(e) => set_error_from_result(&e),
    }
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
    let svc = get_service!(conn);
    let Ok(catalog) = c_str_to_option(catalog) else {
        return FfiStatus::Error;
    };
    let Ok(schema_pattern) = c_str_to_option(schema_pattern) else {
        return FfiStatus::Error;
    };
    let Ok(table_pattern) = c_str_to_option(table_pattern) else {
        return FfiStatus::Error;
    };
    let Ok(column_pattern) = c_str_to_option(column_pattern) else {
        return FfiStatus::Error;
    };

    match svc.get_columns(catalog, schema_pattern, table_pattern, column_pattern) {
        Ok(batch) => export_batch(batch, out),
        Err(e) => set_error_from_result(&e),
    }
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
    let svc = get_service!(conn);
    let Ok(catalog) = c_str_to_str(catalog) else {
        return FfiStatus::Error;
    };
    let Ok(schema) = c_str_to_str(schema) else {
        return FfiStatus::Error;
    };
    let Ok(table) = c_str_to_str(table) else {
        return FfiStatus::Error;
    };

    match svc.get_primary_keys(catalog, schema, table) {
        Ok(batch) => export_batch(batch, out),
        Err(e) => set_error_from_result(&e),
    }
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
    let svc = get_service!(conn);
    let Ok(catalog) = c_str_to_str(catalog) else {
        return FfiStatus::Error;
    };
    let Ok(schema) = c_str_to_str(schema) else {
        return FfiStatus::Error;
    };
    let Ok(table) = c_str_to_str(table) else {
        return FfiStatus::Error;
    };

    match svc.get_foreign_keys(catalog, schema, table) {
        Ok(batch) => export_batch(batch, out),
        Err(e) => set_error_from_result(&e),
    }
}
