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

//! OAuth 2.0 authentication for Databricks.
//!
//! This module provides OAuth 2.0 authentication support including:
//! - Token lifecycle management and refresh logic
//! - M2M (Machine-to-Machine) client credentials flow
//! - U2M (User-to-Machine) authorization code + PKCE flow
//! - OIDC endpoint discovery
//! - File-based token caching

pub mod cache;
pub mod callback;
pub mod m2m;
pub mod oidc;
pub mod token;
pub(crate) mod token_store;
pub mod u2m;

// Re-export the main types
pub use callback::CallbackServer;
pub use m2m::ClientCredentialsProvider;
pub use oidc::OidcEndpoints;
pub use token::OAuthToken;
pub use u2m::AuthorizationCodeProvider;
