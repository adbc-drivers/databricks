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

//! Logging configuration for the Databricks ADBC driver.
//!
//! Initializes a `tracing-subscriber` with file or stderr output.
//!
//! ## Configuration priority
//!
//! 1. `databricks.log_level` / `databricks.log_file` ADBC options (highest)
//! 2. `RUST_LOG` environment variable
//! 3. Default: `warn`
//!
//! ## Usage
//!
//! ```bash
//! # Via environment variable
//! RUST_LOG=databricks_adbc=debug ./my_adbc_app
//! ```
//!
//! Or programmatically via ADBC options:
//! ```ignore
//! database.set_option("databricks.log_level", "debug")?;
//! database.set_option("databricks.log_file", "/tmp/adbc.log")?;
//! ```

use std::sync::OnceLock;
use tracing_subscriber::{
    fmt::{self, time::SystemTime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

static LOGGING_INITIALIZED: OnceLock<()> = OnceLock::new();

/// Logging configuration passed via ADBC database options.
#[derive(Debug, Clone, Default)]
pub(crate) struct LogConfig {
    /// Log level: "OFF", "ERROR", "WARN", "INFO", "DEBUG", "TRACE".
    pub level: Option<String>,
    /// Log file path. If unset, logs go to stderr.
    pub file: Option<String>,
}

/// Initialize the tracing subscriber.
///
/// Uses `OnceLock` to ensure this is called at most once per process.
/// The first `Database::new_connection()` call configures logging;
/// subsequent calls are no-ops.
pub(crate) fn init_logging(config: &LogConfig) {
    LOGGING_INITIALIZED.get_or_init(|| {
        // Check if the configured level is "OFF" — skip initialization entirely
        if let Some(ref level) = config.level {
            if level.eq_ignore_ascii_case("off") {
                return;
            }
        }

        let filter = if let Some(ref level) = config.level {
            EnvFilter::new(format!("databricks_adbc={}", level.to_lowercase()))
        } else {
            // Fall back to RUST_LOG env var, default to warn
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("databricks_adbc=warn"))
        };

        if let Some(ref path) = config.file {
            let file = match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("databricks-adbc: failed to open log file {}: {}", path, e);
                    return;
                }
            };

            tracing_subscriber::registry()
                .with(filter)
                .with(
                    fmt::layer()
                        .with_writer(file)
                        .with_target(false)
                        .with_ansi(false)
                        .with_timer(SystemTime),
                )
                .try_init()
                .ok();
        } else {
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    fmt::layer()
                        .with_writer(std::io::stderr)
                        .with_target(false)
                        .with_timer(SystemTime),
                )
                .try_init()
                .ok();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_config_default() {
        let config = LogConfig::default();
        assert!(config.level.is_none());
        assert!(config.file.is_none());
    }

    #[test]
    fn test_log_config_with_values() {
        let config = LogConfig {
            level: Some("DEBUG".to_string()),
            file: Some("/tmp/test.log".to_string()),
        };
        assert_eq!(config.level.as_deref(), Some("DEBUG"));
        assert_eq!(config.file.as_deref(), Some("/tmp/test.log"));
    }
}
