// Copyright (c) 2025 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
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

package databricks

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
)

// ipcReaderAdapter uses the new IPC stream interface for Arrow access
type ipcReaderAdapter struct {
	rows          driver.Rows
	ipcIterator   dbsqlrows.ArrowIPCStreamIterator
	currentReader *ipc.Reader
	currentRecord arrow.RecordBatch
	schema        *arrow.Schema
	closed        bool
	refCount      int64
	err           error
}

// newIPCReaderAdapter creates a RecordReader using direct IPC stream access
func newIPCReaderAdapter(ctx context.Context, rows driver.Rows) (array.RecordReader, error) {
	ipcRows, ok := rows.(dbsqlrows.Rows)
	if !ok {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "[db] rows do not support Arrow IPC streams",
		}
	}

	// Get IPC stream iterator
	ipcIterator, err := ipcRows.GetArrowIPCStreams(ctx)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to get IPC streams: %v", err),
		}
	}

	schema_bytes, err := ipcIterator.SchemaBytes()
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to get schema bytes: %v", err),
		}
	}

	// Read schema from bytes
	reader, err := ipc.NewReader(bytes.NewReader(schema_bytes))
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to get schema reader: %v", err),
		}
	}
	defer reader.Release()

	schema := reader.Schema()
	if schema == nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "schema is nil",
		}
	}

	adapter := &ipcReaderAdapter{
		rows:        rows,
		refCount:    1,
		ipcIterator: ipcIterator,
		schema:      schema,
	}

	// Initialize the first reader
	err = adapter.loadNextReader()
	if err != nil && err != io.EOF {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to initialize IPC reader: %v", err),
		}
	}
	return adapter, nil
}

func (r *ipcReaderAdapter) loadNextReader() error {
	if r.currentReader != nil {
		r.currentReader.Release()
		r.currentReader = nil
	}

	// Get next IPC stream
	if !r.ipcIterator.HasNext() {
		return io.EOF
	}

	ipcStream, err := r.ipcIterator.Next()
	if err != nil {
		return err
	}

	// Create IPC reader from stream
	reader, err := ipc.NewReader(ipcStream)
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to create IPC reader: %v", err),
		}
	}

	r.currentReader = reader

	return nil
}

// Implement array.RecordReader interface
func (r *ipcReaderAdapter) Schema() *arrow.Schema {
	return r.schema
}

func (r *ipcReaderAdapter) Next() bool {
	if r.closed || r.err != nil {
		return false
	}

	// Release previous record
	if r.currentRecord != nil {
		r.currentRecord.Release()
		r.currentRecord = nil
	}

	// Try to get next record from current reader
	if r.currentReader != nil && r.currentReader.Next() {
		r.currentRecord = r.currentReader.RecordBatch()
		r.currentRecord.Retain()
		return true
	}

	// Need to load next IPC stream
	err := r.loadNextReader()
	if err == io.EOF {
		return false
	} else if err != nil {
		r.err = err
		return false
	}

	// Try again with new reader
	if r.currentReader != nil && r.currentReader.Next() {
		r.currentRecord = r.currentReader.RecordBatch()
		r.currentRecord.Retain()
		return true
	}

	return false
}

func (r *ipcReaderAdapter) Record() arrow.RecordBatch {
	return r.currentRecord
}

func (r *ipcReaderAdapter) RecordBatch() arrow.RecordBatch {
	return r.currentRecord
}

func (r *ipcReaderAdapter) Release() {
	if atomic.AddInt64(&r.refCount, -1) <= 0 {
		if r.closed {
			panic("Double cleanup on ipc_reader_adapter - was Release() called with a closed reader?")
		}
		r.closed = true

		if r.currentRecord != nil {
			r.currentRecord.Release()
			r.currentRecord = nil
		}

		if r.currentReader != nil {
			r.currentReader.Release()
			r.currentReader = nil
		}

		if r.schema != nil {
			r.schema = nil
		}

		r.ipcIterator.Close()

		if r.rows != nil {
			r.err = errors.Join(r.err, r.rows.Close())
			r.rows = nil
		}
	}
}

func (r *ipcReaderAdapter) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *ipcReaderAdapter) Err() error {
	return r.err
}
