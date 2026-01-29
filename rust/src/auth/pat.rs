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

//! Personal Access Token (PAT) authentication.

use super::AuthProvider;
use crate::error::Result;

/// Personal Access Token authentication provider.
///
/// This is the simplest form of authentication for Databricks, using a
/// token generated from the Databricks workspace settings.
#[derive(Debug, Clone)]
pub struct PersonalAccessToken {
    token: String,
}

impl PersonalAccessToken {
    /// Creates a new PAT authentication provider.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

impl AuthProvider for PersonalAccessToken {
    fn get_auth_header(&self) -> Result<String> {
        Ok(format!("Bearer {}", self.token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pat_auth_header() {
        let pat = PersonalAccessToken::new("test-token");
        let header = pat.get_auth_header().unwrap();
        assert_eq!(header, "Bearer test-token");
    }
}
