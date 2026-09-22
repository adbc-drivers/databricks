// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package databricks

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/stretchr/testify/require"
)

func TestParameterRowIteratorConvertsNamedValues(t *testing.T) {
	timestampType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	decimal128Type := &arrow.Decimal128Type{Precision: 10, Scale: 2}
	decimal256Type := &arrow.Decimal256Type{Precision: 38, Scale: 3}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "null", Type: arrow.Null, Nullable: true},
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
		{Name: "i16", Type: arrow.PrimitiveTypes.Int16},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
		{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "str", Type: arrow.BinaryTypes.String},
		{Name: "large_str", Type: arrow.BinaryTypes.LargeString},
		{Name: "str_view", Type: arrow.BinaryTypes.StringView},
		{Name: "date32", Type: arrow.FixedWidthTypes.Date32},
		{Name: "date64", Type: arrow.FixedWidthTypes.Date64},
		{Name: "timestamp", Type: timestampType},
		{Name: "decimal128", Type: decimal128Type},
		{Name: "decimal256", Type: decimal256Type},
	}, nil)

	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[
		{
			"null": null,
			"bool": true,
			"i8": -8,
			"i16": -16,
			"i32": -32,
			"i64": -64,
			"u8": 8,
			"u16": 16,
			"u32": 32,
			"u64": 64,
			"f32": 1.25,
			"f64": 2.5,
			"str": "string",
			"large_str": "large",
			"str_view": "view",
			"date32": "2026-09-22",
			"date64": "2026-09-23",
			"timestamp": "2026-09-22T12:34:56.123456Z",
			"decimal128": "123.45",
			"decimal256": "987.654"
		}
	]`), array.WithUseNumber())
	require.NoError(t, err)
	defer record.Release()

	stream, err := array.NewRecordReader(schema, []arrow.RecordBatch{record})
	require.NoError(t, err)
	iterator, err := newParameterRowIterator(stream)
	require.NoError(t, err)
	defer iterator.Release()

	args, ok, err := iterator.Next()
	require.NoError(t, err)
	require.True(t, ok)

	expected := []struct {
		name  string
		type_ dbsql.SqlType
		value any
	}{
		{"null", dbsql.SqlVoid, nil},
		{"bool", dbsql.SqlBoolean, "true"},
		{"i8", dbsql.SqlTinyInt, "-8"},
		{"i16", dbsql.SqlSmallInt, "-16"},
		{"i32", dbsql.SqlInteger, "-32"},
		{"i64", dbsql.SqlBigInt, "-64"},
		{"u8", dbsql.SqlSmallInt, "8"},
		{"u16", dbsql.SqlInteger, "16"},
		{"u32", dbsql.SqlBigInt, "32"},
		{"u64", dbsql.SqlDecimal, "64"},
		{"f32", dbsql.SqlFloat, "1.25"},
		{"f64", dbsql.SqlDouble, "2.5"},
		{"str", dbsql.SqlString, "string"},
		{"large_str", dbsql.SqlString, "large"},
		{"str_view", dbsql.SqlString, "view"},
		{"date32", dbsql.SqlDate, "2026-09-22"},
		{"date64", dbsql.SqlDate, "2026-09-23"},
		{"timestamp", dbsql.SqlTimestamp, "2026-09-22T12:34:56.123456Z"},
		{"decimal128", dbsql.SqlDecimal, "123.45"},
		{"decimal256", dbsql.SqlDecimal, "987.654"},
	}
	require.Len(t, args, len(expected))
	for i, want := range expected {
		require.Equal(t, i+1, args[i].Ordinal)
		require.Equal(t, want.name, args[i].Name)
		parameter := args[i].Value.(dbsql.Parameter)
		require.Equal(t, want.name, parameter.Name)
		require.Equal(t, want.type_, parameter.Type)
		require.Equal(t, want.value, parameter.Value)
	}
}

func TestParameterRowIteratorPositionalMultipleBatches(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Type: arrow.PrimitiveTypes.Int32}}, nil)
	stream := newInt32RecordReader(t, schema, []int32{1, 2}, []int32{3})
	iterator, err := newParameterRowIterator(stream)
	require.NoError(t, err)
	defer iterator.Release()

	var values []string
	for {
		args, ok, err := iterator.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		require.Empty(t, args[0].Name)
		parameter := args[0].Value.(dbsql.Parameter)
		require.Empty(t, parameter.Name)
		values = append(values, parameter.Value.(string))
	}
	require.Equal(t, []string{"1", "2", "3"}, values)
}

func TestParameterRowIteratorRejectsInvalidSchemas(t *testing.T) {
	tests := []struct {
		name   string
		fields []arrow.Field
		status adbc.Status
	}{
		{
			name: "mixed named and positional",
			fields: []arrow.Field{
				{Name: "named", Type: arrow.PrimitiveTypes.Int64},
				{Type: arrow.PrimitiveTypes.Int64},
			},
			status: adbc.StatusInvalidArgument,
		},
		{
			name: "duplicate names",
			fields: []arrow.Field{
				{Name: "duplicate", Type: arrow.PrimitiveTypes.Int64},
				{Name: "duplicate", Type: arrow.PrimitiveTypes.Int64},
			},
			status: adbc.StatusInvalidArgument,
		},
		{
			name:   "binary",
			fields: []arrow.Field{{Name: "binary", Type: arrow.BinaryTypes.Binary}},
			status: adbc.StatusNotImplemented,
		},
		{
			name:   "nested",
			fields: []arrow.Field{{Name: "list", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}},
			status: adbc.StatusNotImplemented,
		},
		{
			name:   "decimal over maximum precision",
			fields: []arrow.Field{{Name: "decimal", Type: &arrow.Decimal256Type{Precision: 39, Scale: 0}}},
			status: adbc.StatusNotImplemented,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := arrow.NewSchema(test.fields, nil)
			stream, err := array.NewRecordReader(schema, nil)
			require.NoError(t, err)
			_, err = newParameterRowIterator(stream)
			requireADBCStatus(t, err, test.status)
		})
	}
}

func TestParameterizedQueryReaderConcatenatesResultsLazily(t *testing.T) {
	parameterSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	iterator, err := newParameterRowIterator(newInt32RecordReader(t, parameterSchema, []int32{1, 2}, []int32{3}))
	require.NoError(t, err)

	resultSchema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.PrimitiveTypes.Int32}}, nil)
	executions := 0
	reader, err := newParameterizedQueryReader(iterator, func(args []driver.NamedValue) (array.RecordReader, error) {
		executions++
		parameter := args[0].Value.(dbsql.Parameter)
		parsed, err := strconv.ParseInt(parameter.Value.(string), 10, 32)
		require.NoError(t, err)
		return newInt32RecordReader(t, resultSchema, []int32{int32(parsed)}), nil
	})
	require.NoError(t, err)
	defer reader.Release()
	require.Equal(t, 1, executions)

	var values []int32
	for reader.Next() {
		values = append(values, reader.RecordBatch().Column(0).(*array.Int32).Value(0))
		require.Equal(t, len(values), executions)
	}
	require.NoError(t, reader.Err())
	require.Equal(t, []int32{1, 2, 3}, values)
}

func TestParameterizedQueryReaderRejectsSchemaChanges(t *testing.T) {
	parameterSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	iterator, err := newParameterRowIterator(newInt32RecordReader(t, parameterSchema, []int32{1, 2}))
	require.NoError(t, err)

	executions := 0
	reader, err := newParameterizedQueryReader(iterator, func(_ []driver.NamedValue) (array.RecordReader, error) {
		executions++
		schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.PrimitiveTypes.Int32}}, nil)
		if executions == 2 {
			schema = arrow.NewSchema([]arrow.Field{{Name: "changed", Type: arrow.PrimitiveTypes.Int32}}, nil)
		}
		return newInt32RecordReader(t, schema, []int32{int32(executions)}), nil
	})
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next())
	require.False(t, reader.Next())
	requireADBCStatus(t, reader.Err(), adbc.StatusInvalidData)
}

func TestParameterizedQueryReaderRejectsEmptyInput(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	stream, err := array.NewRecordReader(schema, nil)
	require.NoError(t, err)
	iterator, err := newParameterRowIterator(stream)
	require.NoError(t, err)

	_, err = newParameterizedQueryReader(iterator, func(_ []driver.NamedValue) (array.RecordReader, error) {
		t.Fatal("query should not execute")
		return nil, nil
	})
	requireADBCStatus(t, err, adbc.StatusInvalidArgument)
}

func TestExecuteUpdateRunsOncePerParameterRow(t *testing.T) {
	capture := &parameterCaptureConn{}
	driverName := fmt.Sprintf("databricks-parameter-test-%d", parameterTestDriverCounter.Add(1))
	sql.Register(driverName, parameterCaptureDriver{conn: capture})
	database, err := sql.Open(driverName, "")
	require.NoError(t, err)
	defer database.Close()
	sqlConn, err := database.Conn(context.Background())
	require.NoError(t, err)
	defer sqlConn.Close()

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	statement := &statementImpl{
		conn:        &connectionImpl{conn: sqlConn},
		query:       "UPDATE target SET value = :value",
		boundStream: newInt32RecordReader(t, schema, []int32{1, 2}, []int32{3}),
	}

	rowsAffected, err := statement.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 3, rowsAffected)
	require.Len(t, capture.calls, 3)
	for i, args := range capture.calls {
		require.Len(t, args, 1)
		parameter := args[0].Value.(dbsql.Parameter)
		require.Equal(t, "value", parameter.Name)
		require.Equal(t, dbsql.SqlInteger, parameter.Type)
		require.Equal(t, strconv.Itoa(i+1), parameter.Value)
	}
	require.Nil(t, statement.boundStream)
}

func TestBindNilUnbinds(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	recordBuilder.Field(0).(*array.Int32Builder).Append(1)
	record := recordBuilder.NewRecordBatch()
	recordBuilder.Release()
	defer record.Release()

	statement := &statementImpl{}
	require.NoError(t, statement.Bind(context.Background(), record))
	require.NotNil(t, statement.boundStream)
	require.NoError(t, statement.Bind(context.Background(), nil))
	require.Nil(t, statement.boundStream)

	stream, err := array.NewRecordReader(schema, nil)
	require.NoError(t, err)
	require.NoError(t, statement.BindStream(context.Background(), stream))
	stream.Release()
	require.NotNil(t, statement.boundStream)
	require.NoError(t, statement.BindStream(context.Background(), nil))
	require.Nil(t, statement.boundStream)
}

func newInt32RecordReader(t *testing.T, schema *arrow.Schema, batches ...[]int32) array.RecordReader {
	t.Helper()
	records := make([]arrow.RecordBatch, 0, len(batches))
	for _, values := range batches {
		builder := array.NewInt32Builder(memory.DefaultAllocator)
		builder.AppendValues(values, nil)
		valuesArray := builder.NewInt32Array()
		builder.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{valuesArray}, int64(len(values)))
		valuesArray.Release()
		records = append(records, record)
	}
	reader, err := array.NewRecordReader(schema, records)
	require.NoError(t, err)
	for _, record := range records {
		record.Release()
	}
	return reader
}

func requireADBCStatus(t *testing.T, err error, status adbc.Status) {
	t.Helper()
	require.Error(t, err)
	var adbcError adbc.Error
	require.ErrorAs(t, err, &adbcError)
	require.Equal(t, status, adbcError.Code)
}

var parameterTestDriverCounter atomic.Uint64

type parameterCaptureDriver struct {
	conn *parameterCaptureConn
}

func (d parameterCaptureDriver) Open(string) (driver.Conn, error) {
	return d.conn, nil
}

type parameterCaptureConn struct {
	calls [][]driver.NamedValue
}

func (c *parameterCaptureConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (c *parameterCaptureConn) Close() error {
	return nil
}

func (c *parameterCaptureConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c *parameterCaptureConn) CheckNamedValue(*driver.NamedValue) error {
	return nil
}

func (c *parameterCaptureConn) ExecContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
	clonedArgs := append([]driver.NamedValue(nil), args...)
	c.calls = append(c.calls, clonedArgs)
	return driver.RowsAffected(1), nil
}
