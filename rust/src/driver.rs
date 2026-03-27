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

//! ADBC Driver implementation for Databricks.

use crate::database::Database;
use adbc_core::error::Result;
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Optionable;

/// The main entry point for the Databricks ADBC driver.
///
/// The Driver is responsible for creating Database instances, which in turn
/// create Connections.
#[derive(Debug, Default)]
pub struct Driver {}

impl Driver {
    /// Creates a new Driver instance.
    pub fn new() -> Self {
        Self {}
    }
}

impl adbc_core::Driver for Driver {
    type DatabaseType = Database;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(Database::new())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = Database::new();
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::Driver as _;

    #[test]
    fn test_driver_new_database() {
        let mut driver = Driver::new();
        assert!(driver.new_database().is_ok());
    }

    #[test]
    fn test_driver_new_database_with_opts() {
        let mut driver = Driver::new();
        let opts = [(
            OptionDatabase::Uri,
            OptionValue::String("https://example.databricks.com".into()),
        )];
        let result = driver.new_database_with_opts(opts);
        assert!(result.is_ok());
    }
}
