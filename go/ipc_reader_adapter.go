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
	rawSchema     *arrow.Schema // original schema before geoarrow transform
	closed        bool
	refCount      int64
	err           error

	// geoarrow conversion: indices of geometry struct columns to flatten
	geoColumnIndices []int
	geoSchemaBuilt   bool // whether geoarrow schema has been built from first batch
}

// isGeometryStruct checks if a field is a Databricks geometry struct:
// Struct<srid: Int32, wkb: Binary>
func isGeometryStruct(field arrow.Field) bool {
	st, ok := field.Type.(*arrow.StructType)
	if !ok || st.NumFields() != 2 {
		return false
	}
	f0 := st.Field(0)
	f1 := st.Field(1)
	return f0.Name == "srid" && f0.Type.ID() == arrow.INT32 &&
		f1.Name == "wkb" && f1.Type.ID() == arrow.BINARY
}

// detectGeometryColumns finds geometry Struct columns in the schema.
func detectGeometryColumns(schema *arrow.Schema) []int {
	var indices []int
	for i, f := range schema.Fields() {
		if isGeometryStruct(f) {
			indices = append(indices, i)
		}
	}
	return indices
}

// buildGeoArrowSchemaWithoutCRS creates a geoarrow.wkb schema without CRS
// metadata. Used eagerly so the schema is available before the first Next().
func buildGeoArrowSchemaWithoutCRS(schema *arrow.Schema, geoIndices []int) *arrow.Schema {
	fields := schema.Fields()
	newFields := make([]arrow.Field, len(fields))
	copy(newFields, fields)

	for _, idx := range geoIndices {
		f := fields[idx]
		newFields[idx] = arrow.Field{
			Name:     f.Name,
			Type:     arrow.BinaryTypes.Binary,
			Nullable: f.Nullable,
			Metadata: arrow.MetadataFrom(map[string]string{
				"ARROW:extension:name":     "geoarrow.wkb",
				"ARROW:extension:metadata": "",
			}),
		}
	}

	meta := schema.Metadata()
	return arrow.NewSchema(newFields, &meta)
}

// buildGeoArrowSchema creates a new schema with geometry Struct fields replaced
// by Binary fields with geoarrow.wkb extension metadata. The SRID from the
// first record batch is used to populate the CRS in the extension metadata.
func buildGeoArrowSchema(schema *arrow.Schema, geoIndices []int, rec arrow.RecordBatch) *arrow.Schema {
	fields := schema.Fields()
	newFields := make([]arrow.Field, len(fields))
	copy(newFields, fields)

	for _, idx := range geoIndices {
		f := fields[idx]

		// Read SRID from first non-null row of this geometry column
		srid := 0
		structArr := rec.Column(idx).(*array.Struct)
		sridArr := structArr.Field(0)
		for row := 0; row < sridArr.Len(); row++ {
			if !sridArr.IsNull(row) {
				srid = int(sridArr.(*array.Int32).Value(row))
				break
			}
		}

		// Build geoarrow.wkb extension metadata with CRS from SRID
		extMeta := ""
		if srid != 0 {
			extMeta = fmt.Sprintf(`{"crs":{"type":"projjson","properties":{"name":"EPSG:%d"},"id":{"authority":"EPSG","code":%d}}}`, srid, srid)
		}

		newFields[idx] = arrow.Field{
			Name:     f.Name,
			Type:     arrow.BinaryTypes.Binary,
			Nullable: f.Nullable,
			Metadata: arrow.MetadataFrom(map[string]string{
				"ARROW:extension:name":     "geoarrow.wkb",
				"ARROW:extension:metadata": extMeta,
			}),
		}
	}

	meta := schema.Metadata()
	return arrow.NewSchema(newFields, &meta)
}

// transformRecordForGeoArrow extracts the wkb child from geometry struct
// columns and builds a new record with flat Binary columns.
func transformRecordForGeoArrow(rec arrow.RecordBatch, schema *arrow.Schema, geoIndices []int) arrow.RecordBatch {
	if len(geoIndices) == 0 {
		return rec
	}

	geoSet := make(map[int]bool, len(geoIndices))
	for _, idx := range geoIndices {
		geoSet[idx] = true
	}

	cols := make([]arrow.Array, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		if geoSet[i] {
			// Extract the "wkb" field (index 1) from the struct array
			structArr := rec.Column(i).(*array.Struct)
			wkbArr := structArr.Field(1)
			wkbArr.Retain()
			cols[i] = wkbArr
		} else {
			col := rec.Column(i)
			col.Retain()
			cols[i] = col
		}
	}

	newRec := array.NewRecord(schema, cols, rec.NumRows())

	// Release our references to the columns
	for _, col := range cols {
		col.Release()
	}

	return newRec
}

// newIPCReaderAdapter creates a RecordReader using direct IPC stream access
func newIPCReaderAdapter(ctx context.Context, rows driver.Rows, useArrowNativeGeospatial bool) (array.RecordReader, error) {
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

	adapter := &ipcReaderAdapter{
		rows:        rows,
		refCount:    1,
		ipcIterator: ipcIterator,
	}

	// Load the first IPC stream to get the schema.
	// Note: SchemaBytes() may return empty bytes if no direct results were
	// returned with the query response. The schema is populated lazily
	// during the first data fetch in databricks-sql-go. By loading the
	// first reader, we ensure the schema is available.
	err = adapter.loadNextReader()
	if err != nil && err != io.EOF {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to initialize IPC reader: %v", err),
		}
	}

	// Get schema from the first reader, or fall back to SchemaBytes() if
	// the result set is empty (no readers available)
	if adapter.currentReader != nil {
		adapter.schema = adapter.currentReader.Schema()
	} else {
		// Empty result set - try to get schema from SchemaBytes()
		schema_bytes, err := ipcIterator.SchemaBytes()
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("failed to get schema bytes: %v", err),
			}
		}

		if len(schema_bytes) == 0 {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "schema bytes are empty and no data available",
			}
		}

		reader, err := ipc.NewReader(bytes.NewReader(schema_bytes))
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("failed to read schema: %v", err),
			}
		}
		adapter.schema = reader.Schema()
		reader.Release()
	}

	if adapter.schema == nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "schema is nil",
		}
	}

	// When Arrow-native geospatial is enabled, detect geometry Struct columns
	// and build a geoarrow.wkb schema. The schema must be available before
	// the first Next() call since consumers (e.g. adbc_scanner) read it
	// upfront to create table columns. We build the schema eagerly with
	// empty CRS metadata, then enrich it with the SRID from the first
	// record batch when available.
	if useArrowNativeGeospatial {
		adapter.geoColumnIndices = detectGeometryColumns(adapter.schema)
		if len(adapter.geoColumnIndices) > 0 {
			adapter.rawSchema = adapter.schema
			adapter.schema = buildGeoArrowSchemaWithoutCRS(adapter.rawSchema, adapter.geoColumnIndices)
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

// handleGeoRecord enriches the geoarrow schema with CRS from the first batch,
// then transforms the record to flatten geometry struct columns.
func (r *ipcReaderAdapter) handleGeoRecord(rec arrow.RecordBatch) arrow.RecordBatch {
	if len(r.geoColumnIndices) == 0 {
		return rec
	}

	// On the first record batch, rebuild the schema with SRID-based CRS
	// from the actual data. This replaces the initial empty-CRS schema.
	if !r.geoSchemaBuilt {
		r.schema = buildGeoArrowSchema(r.rawSchema, r.geoColumnIndices, rec)
		r.geoSchemaBuilt = true
	}

	return transformRecordForGeoArrow(rec, r.schema, r.geoColumnIndices)
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
		rec := r.currentReader.RecordBatch()
		r.currentRecord = r.handleGeoRecord(rec)
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
		rec := r.currentReader.RecordBatch()
		r.currentRecord = r.handleGeoRecord(rec)
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
