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

//! C-compatible error types and thread-local error propagation for the ODBC FFI layer.
//!
//! The ODBC pattern: call a function, check the return status, then retrieve
//! error details via `odbc_get_last_error()` if the status indicates failure.

use std::cell::RefCell;
use std::ffi::c_char;

/// FFI status codes matching ODBC conventions.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OdbcFfiStatus {
    Success = 0,
    SuccessWithInfo = 1,
    Error = -1,
    InvalidHandle = -2,
    NoData = 100,
}

/// C-compatible error details buffer.
///
/// Callers retrieve this via `odbc_get_last_error()` after a failed operation.
#[repr(C)]
pub struct OdbcFfiError {
    pub message: [c_char; 1024],
    pub sql_state: [c_char; 6],
    pub native_error: i32,
}

impl Default for OdbcFfiError {
    fn default() -> Self {
        Self {
            message: [0; 1024],
            sql_state: [0; 6],
            native_error: 0,
        }
    }
}

// Thread-local storage for the last error.
thread_local! {
    static LAST_ERROR: RefCell<OdbcFfiError> = RefCell::new(OdbcFfiError::default());
}

/// Set the thread-local error details.
pub(crate) fn set_last_error(message: &str, sql_state: &str, native_error: i32) {
    LAST_ERROR.with(|cell| {
        let mut err = cell.borrow_mut();

        // Copy message (truncate to buffer size - 1 for null terminator)
        let msg_bytes = message.as_bytes();
        let msg_len = msg_bytes.len().min(err.message.len() - 1);
        for (i, &b) in msg_bytes[..msg_len].iter().enumerate() {
            err.message[i] = b as c_char;
        }
        err.message[msg_len] = 0; // null terminate

        // Copy sql_state
        let state_bytes = sql_state.as_bytes();
        let state_len = state_bytes.len().min(err.sql_state.len() - 1);
        for (i, &b) in state_bytes[..state_len].iter().enumerate() {
            err.sql_state[i] = b as c_char;
        }
        err.sql_state[state_len] = 0; // null terminate

        err.native_error = native_error;
    });
}

/// Set error from a Rust Error type and return the appropriate status code.
pub(crate) fn set_error_from_result(err: &crate::error::Error) -> OdbcFfiStatus {
    set_last_error(&format!("{}", err), "HY000", -1);
    OdbcFfiStatus::Error
}

/// Retrieve the last error into the provided buffer.
///
/// # Safety
///
/// `error_out` must point to a valid, writable `OdbcFfiError` struct.
#[no_mangle]
pub unsafe extern "C" fn odbc_get_last_error(error_out: *mut OdbcFfiError) -> OdbcFfiStatus {
    if error_out.is_null() {
        return OdbcFfiStatus::InvalidHandle;
    }

    LAST_ERROR.with(|cell| {
        let err = cell.borrow();
        std::ptr::copy_nonoverlapping(&*err as *const OdbcFfiError, error_out, 1);
    });

    OdbcFfiStatus::Success
}
