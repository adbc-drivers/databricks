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

//! Result set handling for query results.

use crate::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

/// Represents a result set from a query execution.
#[derive(Debug)]
pub struct ResultSet {
    schema: Option<SchemaRef>,
    batches: Vec<RecordBatch>,
    current_index: usize,
}

impl ResultSet {
    /// Creates an empty result set.
    pub fn empty() -> Self {
        Self {
            schema: None,
            batches: Vec::new(),
            current_index: 0,
        }
    }

    /// Creates a result set with the given schema and batches.
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema: Some(schema),
            batches,
            current_index: 0,
        }
    }

    /// Returns the schema of the result set.
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    /// Returns the total number of batches.
    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    /// Returns the total number of rows across all batches.
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Fetches the next record batch.
    pub fn next_batch(&mut self) -> Result<Option<&RecordBatch>> {
        if self.current_index >= self.batches.len() {
            return Ok(None);
        }
        let batch = &self.batches[self.current_index];
        self.current_index += 1;
        Ok(Some(batch))
    }

    /// Resets the iterator to the beginning.
    pub fn reset(&mut self) {
        self.current_index = 0;
    }

    /// Closes the result set and releases resources.
    pub fn close(&mut self) -> Result<()> {
        self.batches.clear();
        self.current_index = 0;
        Ok(())
    }
}

impl Default for ResultSet {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_result_set() {
        let mut rs = ResultSet::empty();
        assert!(rs.schema().is_none());
        assert_eq!(rs.batch_count(), 0);
        assert_eq!(rs.row_count(), 0);
        assert!(rs.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_result_set_close() {
        let mut rs = ResultSet::empty();
        assert!(rs.close().is_ok());
    }
}
