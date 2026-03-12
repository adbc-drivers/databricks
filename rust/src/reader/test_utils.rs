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

//! Shared mock `ResultReader` implementations for tests.

use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::ResultReader;
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use driverbase::error::ErrorHelper;
use std::sync::Arc;

/// A simple mock reader that returns predefined batches.
pub struct MockReader {
    batches: Vec<RecordBatch>,
    index: usize,
    schema: SchemaRef,
}

impl MockReader {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        let schema = if batches.is_empty() {
            Arc::new(Schema::empty())
        } else {
            batches[0].schema()
        };
        Self {
            batches,
            index: 0,
            schema,
        }
    }
}

impl ResultReader for MockReader {
    fn schema(&self) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.index >= self.batches.len() {
            Ok(None)
        } else {
            let batch = self.batches[self.index].clone();
            self.index += 1;
            Ok(Some(batch))
        }
    }
}

/// A reader that returns an error on `next_batch()`.
pub struct ErrorReader {
    pub schema: SchemaRef,
}

impl ResultReader for ErrorReader {
    fn schema(&self) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Err(DatabricksErrorHelper::io().message("simulated read error"))
    }
}

/// A reader that returns an error on `schema()`.
pub struct SchemaErrorReader;

impl ResultReader for SchemaErrorReader {
    fn schema(&self) -> Result<SchemaRef> {
        Err(DatabricksErrorHelper::io().message("schema unavailable"))
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}
