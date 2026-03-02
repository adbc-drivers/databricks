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

//! FFI layer for ODBC metadata operations.
//!
//! This module provides `extern "C"` functions that expose the driver's
//! metadata capabilities via the Arrow C Data Interface. It is conditionally
//! compiled with the `odbc-ffi` feature flag.
//!
//! ## Usage from C/C++
//!
//! ```c
//! // 1. Create connection via ADBC FFI (AdbcDriverInit, etc.)
//! // 2. Get metadata handle
//! OdbcConnectionHandle handle = odbc_connection_from_ref(adbc_conn_ptr);
//!
//! // 3. Call metadata functions
//! FFI_ArrowArrayStream stream;
//! OdbcFfiStatus status = odbc_get_tables(handle, "main", NULL, "%", NULL, &stream);
//!
//! // 4. Process Arrow stream...
//!
//! // 5. Free handle when done
//! odbc_connection_free(handle);
//! ```

pub mod error;
pub mod handle;
pub mod odbc;

// Re-export key types for convenience
pub use error::{OdbcFfiError, OdbcFfiStatus};
pub use handle::OdbcConnectionHandle;
