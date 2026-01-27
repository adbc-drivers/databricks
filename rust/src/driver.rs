// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! ADBC Driver implementation for Databricks.

use crate::database::Database;
use crate::Result;

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

    /// Creates a new Database with the given options.
    pub fn new_database(&self) -> Result<Database> {
        Ok(Database::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_driver_new() {
        let driver = Driver::new();
        assert!(driver.new_database().is_ok());
    }
}
