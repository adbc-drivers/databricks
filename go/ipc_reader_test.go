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

// This file tests internal IPC reader implementation.
// It uses package databricks (not databricks_test) to access unexported functions.
// For public API tests, see ipc_public_test.go and other *_test.go files.

package databricks

import (
	"bytes"
	"context"
	"database/sql/driver"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockIPCStreamIterator implements dbsqlrows.ArrowIPCStreamIterator for testing
type mockIPCStreamIterator struct {
	streams [][]byte
	index   int
	schema  []byte
}

func (m *mockIPCStreamIterator) Next() (io.Reader, error) {
	if m.index >= len(m.streams) {
		return nil, io.EOF
	}
	stream := m.streams[m.index]
	m.index++
	return bytes.NewReader(stream), nil
}

func (m *mockIPCStreamIterator) HasNext() bool {
	return m.index < len(m.streams)
}

func (m *mockIPCStreamIterator) Close() {
	// Nothing to close
}

func (m *mockIPCStreamIterator) SchemaBytes() ([]byte, error) {
	return m.schema, nil
}

// mockRows implements the subset of dbsqlrows.Rows needed for testing
type mockRows struct {
	iterator dbsqlrows.ArrowIPCStreamIterator
}

func (m *mockRows) GetArrowIPCStreams(ctx context.Context) (dbsqlrows.ArrowIPCStreamIterator, error) {
	return m.iterator, nil
}

func (m *mockRows) GetArrowBatches(ctx context.Context) (dbsqlrows.ArrowBatchIterator, error) {
	return nil, nil // Not used in our tests
}

func (m *mockRows) Columns() []string {
	panic("not implemented")
}

func (m *mockRows) Next(dest []driver.Value) error {
	panic("not implemented")
}

func (m *mockRows) Close() error {
	return nil
}

// TestIPCReaderAdapter tests the IPC reader adapter with mock data
func TestIPCReaderAdapter(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create a simple schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	// Create test data
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"alice", "bob", "charlie"}, nil)

	record := builder.NewRecordBatch()
	defer record.Release()

	// Serialize to IPC format
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	err := writer.Write(record)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	ipcData := buf.Bytes()

	// Create schema bytes
	var schemaBuf bytes.Buffer
	schemaWriter := ipc.NewWriter(&schemaBuf, ipc.WithSchema(schema))
	err = schemaWriter.Close()
	require.NoError(t, err)
	schemaBytes := schemaBuf.Bytes()

	// Create mock iterator
	mockIterator := &mockIPCStreamIterator{
		streams: [][]byte{ipcData},
		schema:  schemaBytes,
	}

	// Create mock rows
	mockRows := &mockRows{
		iterator: mockIterator,
	}

	// Test the IPC reader adapter
	ctx := context.Background()
	reader, err := newIPCReaderAdapter(ctx, mockRows, false)
	require.NoError(t, err)
	defer reader.Release()

	// Verify schema
	readerSchema := reader.Schema()
	assert.Equal(t, 2, len(readerSchema.Fields()))
	assert.Equal(t, "id", readerSchema.Field(0).Name)
	assert.Equal(t, "name", readerSchema.Field(1).Name)

	// Read data
	hasData := reader.Next()
	assert.True(t, hasData)

	readRecord := reader.RecordBatch()
	assert.Equal(t, int64(3), readRecord.NumRows())

	// Verify values
	idCol := readRecord.Column(0).(*array.Int64)
	nameCol := readRecord.Column(1).(*array.String)

	assert.Equal(t, int64(1), idCol.Value(0))
	assert.Equal(t, int64(2), idCol.Value(1))
	assert.Equal(t, int64(3), idCol.Value(2))

	assert.Equal(t, "alice", nameCol.Value(0))
	assert.Equal(t, "bob", nameCol.Value(1))
	assert.Equal(t, "charlie", nameCol.Value(2))

	// No more data
	hasData = reader.Next()
	assert.False(t, hasData)
}

// TestIPCReaderAdapterMultipleStreams tests handling multiple IPC streams
func TestIPCReaderAdapterMultipleStreams(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "value", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	// Create multiple batches
	var streams [][]byte
	totalRows := 0

	for i := 0; i < 3; i++ {
		builder := array.NewRecordBuilder(mem, schema)

		// Each batch has different data
		start := i * 100
		values := make([]int32, 100)
		for j := 0; j < 100; j++ {
			values[j] = int32(start + j)
		}
		builder.Field(0).(*array.Int32Builder).AppendValues(values, nil)

		record := builder.NewRecordBatch()

		// Serialize to IPC
		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
		err := writer.Write(record)
		require.NoError(t, err)
		err = writer.Close()
		require.NoError(t, err)

		streams = append(streams, buf.Bytes())
		totalRows += 100

		record.Release()
		builder.Release()
	}

	var schemaBuf bytes.Buffer
	schemaWriter := ipc.NewWriter(&schemaBuf, ipc.WithSchema(schema))
	err := schemaWriter.Close()
	require.NoError(t, err)
	schemaBytes := schemaBuf.Bytes()

	// Create mock iterator
	mockIterator := &mockIPCStreamIterator{
		streams: streams,
		schema:  schemaBytes,
	}

	// Create mock rows
	mockRows := &mockRows{
		iterator: mockIterator,
	}

	// Test the adapter
	ctx := context.Background()
	reader, err := newIPCReaderAdapter(ctx, mockRows, false)
	require.NoError(t, err)
	defer reader.Release()

	// Read all batches
	rowCount := 0
	batchCount := 0

	for reader.Next() {
		record := reader.RecordBatch()
		rowCount += int(record.NumRows())
		batchCount++

		// Verify first value of each batch
		valueCol := record.Column(0).(*array.Int32)
		expectedFirst := int32((batchCount - 1) * 100)
		assert.Equal(t, expectedFirst, valueCol.Value(0))
	}

	assert.Equal(t, 3, batchCount)
	assert.Equal(t, 300, rowCount)
}

// TestIPCReaderAdapterGeoArrowCRSEager verifies that when a geometry struct
// column is present and Arrow-native geospatial is enabled, the reader's
// Schema() carries geoarrow.wkb extension metadata with CRS populated from
// the first batch's SRID *before* Next() is called. Consumers like DuckDB's
// adbc_scanner read Schema() upfront — if CRS is only populated on the first
// Next(), it never reaches them.
func TestIPCReaderAdapterGeoArrowCRSEager(t *testing.T) {
	mem := memory.NewGoAllocator()

	geoStructType := arrow.StructOf(
		arrow.Field{Name: "srid", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "wkb", Type: arrow.BinaryTypes.Binary, Nullable: true},
	)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "geom", Type: geoStructType, Nullable: true},
		},
		nil,
	)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)

	geoB := builder.Field(1).(*array.StructBuilder)
	geoB.Append(true)
	geoB.FieldBuilder(0).(*array.Int32Builder).Append(3857)
	geoB.FieldBuilder(1).(*array.BinaryBuilder).Append([]byte{0x01, 0x01, 0x00, 0x00, 0x00})
	geoB.Append(true)
	geoB.FieldBuilder(0).(*array.Int32Builder).Append(3857)
	geoB.FieldBuilder(1).(*array.BinaryBuilder).Append([]byte{0x01, 0x01, 0x00, 0x00, 0x00})

	record := builder.NewRecordBatch()
	defer record.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	ipcData := buf.Bytes()

	var schemaBuf bytes.Buffer
	schemaWriter := ipc.NewWriter(&schemaBuf, ipc.WithSchema(schema))
	require.NoError(t, schemaWriter.Close())

	mockRows := &mockRows{
		iterator: &mockIPCStreamIterator{
			streams: [][]byte{ipcData},
			schema:  schemaBuf.Bytes(),
		},
	}

	ctx := context.Background()
	reader, err := newIPCReaderAdapter(ctx, mockRows, true)
	require.NoError(t, err)
	defer reader.Release()

	// Schema() must already carry geoarrow.wkb + CRS before any Next() call.
	s := reader.Schema()
	require.Equal(t, 2, len(s.Fields()))
	geoField := s.Field(1)

	assert.Equal(t, arrow.BINARY, geoField.Type.ID(), "geometry struct should be flattened to Binary")

	extName, ok := geoField.Metadata.GetValue("ARROW:extension:name")
	require.True(t, ok, "ARROW:extension:name missing")
	assert.Equal(t, "geoarrow.wkb", extName)

	extMeta, ok := geoField.Metadata.GetValue("ARROW:extension:metadata")
	require.True(t, ok, "ARROW:extension:metadata missing")
	assert.Equal(t, `{"crs":"EPSG:3857"}`, extMeta, "CRS must be populated from first batch SRID before Next()")

	// Record still emerges on the first Next() and has matching shape.
	require.True(t, reader.Next())
	rec := reader.RecordBatch()
	require.Equal(t, int64(2), rec.NumRows())
	require.Equal(t, arrow.BINARY, rec.Column(1).DataType().ID())

	require.False(t, reader.Next())
}
