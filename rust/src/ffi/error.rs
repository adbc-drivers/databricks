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

//! C-compatible error types and thread-local error propagation for the metadata FFI layer.
//!
//! Call a function, check the return status, then retrieve error details
//! via `metadata_get_last_error()` if the status indicates failure.

use std::cell::RefCell;
use std::ffi::c_char;

/// FFI status codes matching ODBC conventions.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum FfiStatus {
    Success = 0,
    SuccessWithInfo = 1,
    Error = -1,
    InvalidHandle = -2,
    NoData = 100,
}

/// C-compatible error details buffer.
///
/// Callers retrieve this via `metadata_get_last_error()` after a failed operation.
#[repr(C)]
pub struct FfiError {
    pub message: [c_char; 1024],
    pub sql_state: [c_char; 6],
    pub native_error: i32,
}

impl Default for FfiError {
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
    static LAST_ERROR: RefCell<FfiError> = RefCell::new(FfiError::default());
}

/// Clear the thread-local error buffer.
///
/// Called at the start of each FFI function so that callers do not see
/// stale errors from a previous call after a successful operation.
pub(crate) fn clear_last_error() {
    LAST_ERROR.with(|cell| {
        let mut err = cell.borrow_mut();
        err.message[0] = 0;
        err.sql_state[0] = 0;
        err.native_error = 0;
    });
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
pub(crate) fn set_error_from_result(err: &crate::error::Error) -> FfiStatus {
    set_last_error(&format!("{}", err), "HY000", -1);
    FfiStatus::Error
}

/// Retrieve the last error into the provided buffer.
///
/// Unlike other FFI functions, this does **not** clear the error buffer
/// (callers may need to read it multiple times).
///
/// # Safety
///
/// `error_out` must point to a valid, writable `FfiError` struct.
#[no_mangle]
pub unsafe extern "C" fn metadata_get_last_error(error_out: *mut FfiError) -> FfiStatus {
    // No clear_last_error here — reading must not destroy the error.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if error_out.is_null() {
            return FfiStatus::InvalidHandle;
        }

        LAST_ERROR.with(|cell| {
            let err = cell.borrow();
            unsafe {
                std::ptr::copy_nonoverlapping(&*err as *const FfiError, error_out, 1);
            }
        });

        FfiStatus::Success
    }))
    .unwrap_or(FfiStatus::Error)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to read the message from an FfiError as a Rust string.
    fn error_message(err: &FfiError) -> String {
        let bytes: Vec<u8> = err
            .message
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as u8)
            .collect();
        String::from_utf8(bytes).unwrap()
    }

    /// Helper to read the sql_state from an FfiError as a Rust string.
    fn error_sql_state(err: &FfiError) -> String {
        let bytes: Vec<u8> = err
            .sql_state
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as u8)
            .collect();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn test_set_and_get_last_error() {
        set_last_error("something went wrong", "HY001", -42);

        let mut err = FfiError::default();
        let status = unsafe { metadata_get_last_error(&mut err) };
        assert_eq!(status, FfiStatus::Success);
        assert_eq!(error_message(&err), "something went wrong");
        assert_eq!(error_sql_state(&err), "HY001");
        assert_eq!(err.native_error, -42);
    }

    #[test]
    fn test_clear_last_error() {
        set_last_error("old error", "HY000", -1);
        clear_last_error();

        let mut err = FfiError::default();
        let status = unsafe { metadata_get_last_error(&mut err) };
        assert_eq!(status, FfiStatus::Success);
        assert_eq!(error_message(&err), "");
        assert_eq!(error_sql_state(&err), "");
        assert_eq!(err.native_error, 0);
    }

    #[test]
    fn test_get_last_error_null_pointer() {
        let status = unsafe { metadata_get_last_error(std::ptr::null_mut()) };
        assert_eq!(status, FfiStatus::InvalidHandle);
    }

    #[test]
    fn test_set_last_error_truncates_long_message() {
        let long_msg = "x".repeat(2000);
        set_last_error(&long_msg, "HY000", -1);

        let mut err = FfiError::default();
        unsafe { metadata_get_last_error(&mut err) };
        let msg = error_message(&err);
        // Should be truncated to 1023 chars (buffer is 1024 with null terminator)
        assert_eq!(msg.len(), 1023);
    }

    #[test]
    fn test_successful_call_clears_stale_error() {
        // Simulate: first call fails, second succeeds
        set_last_error("first error", "HY000", -1);
        // clear_last_error simulates the start of a successful FFI call
        clear_last_error();

        let mut err = FfiError::default();
        unsafe { metadata_get_last_error(&mut err) };
        assert_eq!(error_message(&err), "");
    }
}
