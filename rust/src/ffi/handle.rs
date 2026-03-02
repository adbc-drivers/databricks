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

//! Opaque handle management for the ODBC FFI layer.
//!
//! The ODBC wrapper creates connections via the standard ADBC FFI flow and then
//! calls `odbc_connection_from_adbc()` to get a metadata handle. All `odbc_*`
//! metadata functions accept this handle.
//!
//! Internally, the handle wraps a `ConnectionMetadataService` which holds
//! the client, session ID, and runtime handle needed to execute metadata queries.

use crate::ffi::error::set_last_error;
use crate::metadata::service::ConnectionMetadataService;
use std::ffi::c_void;

/// Opaque handle representing a Databricks connection for metadata FFI.
pub type OdbcConnectionHandle = *mut c_void;

/// Create a metadata handle directly from a Connection reference.
///
/// This is the primary way the ODBC wrapper creates handles. It takes a
/// reference to the Rust Connection object and wraps it in a metadata service.
///
/// # Safety
///
/// `conn` must be a valid pointer to a `crate::Connection`. The Connection
/// must outlive this handle. Free the handle with `odbc_connection_free()`.
#[no_mangle]
pub unsafe extern "C" fn odbc_connection_from_ref(
    conn: *const c_void,
) -> OdbcConnectionHandle {
    if conn.is_null() {
        set_last_error("Null connection pointer", "HY009", -1);
        return std::ptr::null_mut();
    }

    let connection = &*(conn as *const crate::Connection);
    let service = ConnectionMetadataService::new(
        connection.client().clone(),
        connection.session_id().to_string(),
        connection.runtime_handle().clone(),
    );

    Box::into_raw(Box::new(service)) as OdbcConnectionHandle
}

/// Free a metadata handle created by `odbc_connection_from_ref()`.
///
/// # Safety
///
/// `handle` must be a valid handle returned by `odbc_connection_from_ref()`,
/// or null (which is a no-op).
#[no_mangle]
pub unsafe extern "C" fn odbc_connection_free(handle: OdbcConnectionHandle) {
    if !handle.is_null() {
        drop(Box::from_raw(
            handle as *mut ConnectionMetadataService,
        ));
    }
}

/// Recover the MetadataService from an opaque handle.
///
/// Returns `None` if the handle is null.
///
/// # Safety
///
/// The handle must be a valid pointer returned by `odbc_connection_from_ref()`
/// and must not have been freed via `odbc_connection_free()`. The returned
/// reference is only valid as long as the handle is alive.
pub(crate) unsafe fn handle_to_service<'a>(
    handle: OdbcConnectionHandle,
) -> Option<&'a ConnectionMetadataService> {
    if handle.is_null() {
        None
    } else {
        Some(&*(handle as *const ConnectionMetadataService))
    }
}
