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

//! Opaque handle management for the metadata FFI layer.
//!
//! Callers create connections via the standard ADBC FFI flow and then
//! call `metadata_connection_from_ref()` to get a metadata handle. All
//! `metadata_*` catalog functions accept this handle.
//!
//! Internally, the handle wraps a `ConnectionMetadataService` which holds
//! the client, session ID, and runtime handle needed to execute metadata queries.

use crate::ffi::error::{clear_last_error, set_last_error};
use crate::metadata::service::ConnectionMetadataService;
use std::ffi::c_void;
use std::panic::AssertUnwindSafe;

/// Opaque handle representing a Databricks connection for metadata FFI.
pub type FfiConnectionHandle = *mut c_void;

/// Create a metadata handle directly from a Connection reference.
///
/// This is the primary way callers create handles. It takes a reference
/// to the Rust Connection object and wraps it in a metadata service.
///
/// # Safety
///
/// `conn` must be a valid pointer to a `crate::Connection`. The Connection
/// must outlive this handle. Free the handle with `metadata_connection_free()`.
#[no_mangle]
pub unsafe extern "C" fn metadata_connection_from_ref(conn: *const c_void) -> FfiConnectionHandle {
    clear_last_error();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        if conn.is_null() {
            set_last_error("Null connection pointer", "HY009", -1);
            return std::ptr::null_mut();
        }

        let connection = unsafe { &*(conn as *const crate::Connection) };
        let service = ConnectionMetadataService::new(
            connection.client().clone(),
            connection.session_id().to_string(),
            connection.runtime_handle().clone(),
        );

        Box::into_raw(Box::new(service)) as FfiConnectionHandle
    }))
    .unwrap_or_else(|_| {
        set_last_error(
            "Internal panic in metadata_connection_from_ref",
            "HY000",
            -1,
        );
        std::ptr::null_mut()
    })
}

/// Free a metadata handle created by `metadata_connection_from_ref()`.
///
/// # Safety
///
/// `handle` must be a valid handle returned by `metadata_connection_from_ref()`,
/// or null (which is a no-op).
#[no_mangle]
pub unsafe extern "C" fn metadata_connection_free(handle: FfiConnectionHandle) {
    // No clear_last_error here — free is not expected to produce errors
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        if !handle.is_null() {
            drop(unsafe { Box::from_raw(handle as *mut ConnectionMetadataService) });
        }
    }));
}

/// Recover the ConnectionMetadataService from an opaque handle.
///
/// Returns `None` if the handle is null.
///
/// # Safety
///
/// The handle must be a valid pointer returned by `metadata_connection_from_ref()`
/// and must not have been freed via `metadata_connection_free()`. The returned
/// reference is only valid as long as the handle is alive.
pub(crate) unsafe fn handle_to_service<'a>(
    handle: FfiConnectionHandle,
) -> Option<&'a ConnectionMetadataService> {
    if handle.is_null() {
        None
    } else {
        Some(&*(handle as *const ConnectionMetadataService))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_connection_returns_null_handle() {
        let handle = unsafe { metadata_connection_from_ref(std::ptr::null()) };
        assert!(handle.is_null());
    }

    #[test]
    fn test_free_null_handle_is_noop() {
        // Should not panic or crash
        unsafe { metadata_connection_free(std::ptr::null_mut()) };
    }

    #[test]
    fn test_handle_to_service_null() {
        let result = unsafe { handle_to_service(std::ptr::null_mut()) };
        assert!(result.is_none());
    }
}
