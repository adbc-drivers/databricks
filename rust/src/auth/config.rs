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

//! Authentication configuration types for the Databricks ADBC driver.
//!
//! This module defines the enums and configuration struct used to configure
//! authentication when creating a new database connection.

use crate::error::DatabricksErrorHelper;
use driverbase::error::ErrorHelper;

/// Authentication type -- single selector for the authentication method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthType {
    /// Personal access token.
    AccessToken,
    /// M2M: client credentials grant for service principals.
    OAuthM2m,
    /// U2M: browser-based authorization code + PKCE.
    OAuthU2m,
}

impl TryFrom<&str> for AuthType {
    type Error = crate::error::Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "access_token" => Ok(AuthType::AccessToken),
            "oauth_m2m" => Ok(AuthType::OAuthM2m),
            "oauth_u2m" => Ok(AuthType::OAuthU2m),
            _ => Err(DatabricksErrorHelper::invalid_argument().message(format!(
                "Invalid auth type: '{}'. Valid values: 'access_token', 'oauth_m2m', 'oauth_u2m'",
                value
            ))),
        }
    }
}

impl std::fmt::Display for AuthType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthType::AccessToken => write!(f, "access_token"),
            AuthType::OAuthM2m => write!(f, "oauth_m2m"),
            AuthType::OAuthU2m => write!(f, "oauth_u2m"),
        }
    }
}

/// Authentication configuration parsed from Database options.
///
/// This struct collects all auth-related options set via `Database::set_option()`.
/// It is used by `Database::new_connection()` to validate the configuration and
/// create the appropriate `AuthProvider`.
#[derive(Debug, Default, Clone)]
pub struct AuthConfig {
    pub auth_type: Option<AuthType>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub scopes: Option<String>,
    pub token_endpoint: Option<String>,
    pub redirect_port: Option<u16>,
}

impl AuthConfig {
    /// Validates the auth configuration and returns the auth type.
    ///
    /// This checks that:
    /// - An auth type is specified
    /// - Required fields are present for the chosen auth type
    pub fn validate(&self, access_token: &Option<String>) -> crate::error::Result<AuthType> {
        let auth_type = self.auth_type.ok_or_else(|| {
            DatabricksErrorHelper::invalid_argument().message(
                "databricks.auth.type is required. Valid values: 'access_token', 'oauth_m2m', 'oauth_u2m'",
            )
        })?;

        match auth_type {
            AuthType::AccessToken => {
                if access_token.is_none() {
                    return Err(DatabricksErrorHelper::invalid_argument().message(
                        "databricks.access_token is required when auth type is 'access_token'",
                    ));
                }
            }
            AuthType::OAuthM2m => {
                if self.client_id.is_none() {
                    return Err(DatabricksErrorHelper::invalid_argument().message(
                        "databricks.auth.client_id is required when auth type is 'oauth_m2m'",
                    ));
                }
                if self.client_secret.is_none() {
                    return Err(DatabricksErrorHelper::invalid_argument().message(
                        "databricks.auth.client_secret is required when auth type is 'oauth_m2m'",
                    ));
                }
            }
            AuthType::OAuthU2m => {
                // U2M flow has no required fields - all parameters have defaults:
                // - client_id defaults to "databricks-cli"
                // - scopes defaults to "all-apis offline_access"
                // - redirect_port defaults to 8020
            }
        }

        Ok(auth_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_type_valid() {
        assert_eq!(
            AuthType::try_from("access_token").unwrap(),
            AuthType::AccessToken
        );
        assert_eq!(AuthType::try_from("oauth_m2m").unwrap(), AuthType::OAuthM2m);
        assert_eq!(AuthType::try_from("oauth_u2m").unwrap(), AuthType::OAuthU2m);
    }

    #[test]
    fn test_auth_type_invalid() {
        assert!(AuthType::try_from("pat").is_err());
        assert!(AuthType::try_from("oauth").is_err());
        assert!(AuthType::try_from("0").is_err());
        assert!(AuthType::try_from("11").is_err());
        assert!(AuthType::try_from("").is_err());
    }

    #[test]
    fn test_auth_type_display() {
        assert_eq!(AuthType::AccessToken.to_string(), "access_token");
        assert_eq!(AuthType::OAuthM2m.to_string(), "oauth_m2m");
        assert_eq!(AuthType::OAuthU2m.to_string(), "oauth_u2m");
    }

    #[test]
    fn test_validate_missing_auth_type() {
        let config = AuthConfig::default();
        let result = config.validate(&None);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("databricks.auth.type is required"));
    }

    #[test]
    fn test_validate_access_token_missing_token() {
        let config = AuthConfig {
            auth_type: Some(AuthType::AccessToken),
            ..Default::default()
        };
        let result = config.validate(&None);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("databricks.access_token is required"));
    }

    #[test]
    fn test_validate_access_token_with_token() {
        let config = AuthConfig {
            auth_type: Some(AuthType::AccessToken),
            ..Default::default()
        };
        let result = config.validate(&Some("token".to_string()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), AuthType::AccessToken);
    }

    #[test]
    fn test_validate_oauth_m2m_missing_client_id() {
        let config = AuthConfig {
            auth_type: Some(AuthType::OAuthM2m),
            ..Default::default()
        };
        let result = config.validate(&None);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("databricks.auth.client_id is required"));
    }

    #[test]
    fn test_validate_oauth_m2m_missing_secret() {
        let config = AuthConfig {
            auth_type: Some(AuthType::OAuthM2m),
            client_id: Some("id".to_string()),
            ..Default::default()
        };
        let result = config.validate(&None);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("databricks.auth.client_secret is required"));
    }

    #[test]
    fn test_validate_oauth_m2m_valid() {
        let config = AuthConfig {
            auth_type: Some(AuthType::OAuthM2m),
            client_id: Some("id".to_string()),
            client_secret: Some("secret".to_string()),
            ..Default::default()
        };
        let result = config.validate(&None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), AuthType::OAuthM2m);
    }

    #[test]
    fn test_validate_oauth_u2m_no_required_fields() {
        let config = AuthConfig {
            auth_type: Some(AuthType::OAuthU2m),
            ..Default::default()
        };
        let result = config.validate(&None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), AuthType::OAuthU2m);
    }
}
