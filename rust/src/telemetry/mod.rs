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

//! Telemetry and metrics collection for the Databricks ADBC driver.

use std::time::{Duration, Instant};

/// Configuration for telemetry collection.
///
/// Telemetry is disabled by default and requires explicit opt-in.
/// When enabled, the driver collects anonymous usage metrics such as
/// query execution times and error counts. No query content or
/// personally identifiable information is collected.
#[derive(Debug, Clone, Default)]
pub struct TelemetryConfig {
    /// Whether telemetry collection is enabled (default: false).
    pub enabled: bool,
}

/// Collects and reports driver telemetry data.
#[derive(Debug)]
pub struct TelemetryCollector {
    config: TelemetryConfig,
}

impl TelemetryCollector {
    /// Creates a new telemetry collector with the given configuration.
    pub fn new(config: TelemetryConfig) -> Self {
        Self { config }
    }

    /// Returns whether telemetry is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Records a query execution metric.
    pub fn record_query_execution(&self, _duration: Duration, _rows_fetched: u64) {
        if self.config.enabled {
            // TODO: Implement telemetry reporting
        }
    }

    /// Records an error occurrence.
    pub fn record_error(&self, _error_type: &str) {
        if self.config.enabled {
            // TODO: Implement telemetry reporting
        }
    }
}

impl Default for TelemetryCollector {
    fn default() -> Self {
        Self::new(TelemetryConfig::default())
    }
}

/// A timer for measuring operation durations.
#[derive(Debug)]
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// Starts a new timer.
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Returns the elapsed duration since the timer was started.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_telemetry_config_default_disabled() {
        let config = TelemetryConfig::default();
        assert!(!config.enabled);
    }

    #[test]
    fn test_telemetry_collector_disabled() {
        let collector = TelemetryCollector::new(TelemetryConfig { enabled: false });
        assert!(!collector.is_enabled());
    }

    #[test]
    fn test_timer() {
        let timer = Timer::start();
        sleep(Duration::from_millis(10));
        assert!(timer.elapsed() >= Duration::from_millis(10));
    }
}
