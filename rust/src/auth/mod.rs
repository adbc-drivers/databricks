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

//! Authentication mechanisms for the Databricks ADBC driver.

pub mod oauth;
pub mod pat;

pub use oauth::OAuthCredentials;
pub use pat::PersonalAccessToken;

use crate::error::Result;
use std::fmt::Debug;

/// Trait for authentication providers.
pub trait AuthProvider: Send + Sync + Debug {
    /// Returns the authorization header value for HTTP requests.
    fn get_auth_header(&self) -> Result<String>;
}
