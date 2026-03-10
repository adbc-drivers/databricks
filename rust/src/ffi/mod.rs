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

//! FFI layer for catalog metadata operations.
//!
//! This module provides `extern "C"` functions that expose the driver's
//! metadata capabilities via the Arrow C Data Interface. It is conditionally
//! compiled with the `metadata-ffi` feature flag.
//!
//! ## Usage from C/C++
//!
//! ```c
//! // 1. Get Connection* from adbc_connection_.private_data
//! // 2. Call metadata functions directly
//! FFI_ArrowArrayStream stream;
//! FfiStatus status = metadata_get_tables(conn_ptr, "main", NULL, "%", NULL, &stream);
//!
//! // 3. Process Arrow stream...
//! ```

pub mod error;
pub mod metadata;
