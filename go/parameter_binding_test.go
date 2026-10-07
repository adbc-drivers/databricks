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

func TestParameterBindingModeForQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected parameterBindingMode
	}{
		{name: "positional", query: "SELECT ?", expected: positionalParameterBinding},
		{name: "named", query: "SELECT :value", expected: namedParameterBinding},
		{name: "single quoted", query: "SELECT '?', :value", expected: namedParameterBinding},
		{name: "doubled single quote", query: "SELECT 'isn''t ?'", expected: namedParameterBinding},
		{name: "double quoted", query: `SELECT "?"`, expected: namedParameterBinding},
		{name: "backtick quoted", query: "SELECT `?`", expected: namedParameterBinding},
		{name: "line comment", query: "SELECT 1 -- ?\n, :value", expected: namedParameterBinding},
		{name: "block comment", query: "SELECT /* ? */ :value", expected: namedParameterBinding},
		{name: "nested block comment", query: "SELECT /* outer /* ? */ */ :value", expected: namedParameterBinding},
		{name: "marker after quoted text", query: "SELECT '?', ?", expected: positionalParameterBinding},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, parameterBindingModeForQuery(test.query))
		})
	}
}

func TestParameterRowIteratorConvertsNamedValues(t *testing.T) {
	timestampType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
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
		{Name: "f16", Type: arrow.FixedWidthTypes.Float16},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "str", Type: arrow.BinaryTypes.String},
		{Name: "large_str", Type: arrow.BinaryTypes.LargeString},
		{Name: "str_view", Type: arrow.BinaryTypes.StringView},
		{Name: "binary", Type: arrow.BinaryTypes.Binary},
		{Name: "large_binary", Type: arrow.BinaryTypes.LargeBinary},
		{Name: "binary_view", Type: arrow.BinaryTypes.BinaryView},
		{Name: "fixed_binary", Type: &arrow.FixedSizeBinaryType{ByteWidth: 3}},
		{Name: "decimal128", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}},
		{Name: "decimal256", Type: &arrow.Decimal256Type{Precision: 38, Scale: 3}},
		{Name: "date32", Type: arrow.FixedWidthTypes.Date32},
		{Name: "date64", Type: arrow.FixedWidthTypes.Date64},
		{Name: "timestamp", Type: timestampType},
		{Name: "timestamp_ntz", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
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
			"f16": 1.25,
			"f32": 1.25,
			"f64": 2.5,
			"str": "string",
			"large_str": "large",
			"str_view": "view",
			"binary": "AP9B",
			"large_binary": "",
			"binary_view": "AP9B",
			"fixed_binary": "AP9B",
			"decimal128": "-123.45",
			"decimal256": "12345678901234567890123456789012345.678",
			"date32": "2026-09-22",
			"date64": "2026-09-23",
			"timestamp": "2026-09-22T12:34:56.123456Z",
			"timestamp_ntz": "2026-09-22T12:34:56.123456"
		}
	]`))
	require.NoError(t, err)
	defer record.Release()

	stream, err := array.NewRecordReader(schema, []arrow.RecordBatch{record})
	require.NoError(t, err)
	iterator, err := newParameterRowIterator(stream, namedParameterBinding)
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
		{"f16", dbsql.SqlFloat, "1.25"},
		{"f32", dbsql.SqlFloat, "1.25"},
		{"f64", dbsql.SqlDouble, "2.5"},
		{"str", dbsql.SqlString, "string"},
		{"large_str", dbsql.SqlString, "large"},
		{"str_view", dbsql.SqlString, "view"},
		{"binary", dbsql.SqlString, "00ff41"},
		{"large_binary", dbsql.SqlString, ""},
		{"binary_view", dbsql.SqlString, "00ff41"},
		{"fixed_binary", dbsql.SqlString, "00ff41"},
		{"decimal128", dbsql.SqlString, "-123.45"},
		{"decimal256", dbsql.SqlString, "12345678901234567890123456789012345.678"},
		{"date32", dbsql.SqlDate, "2026-09-22"},
		{"date64", dbsql.SqlDate, "2026-09-23"},
		{"timestamp", dbsql.SqlTimestamp, "2026-09-22T12:34:56.123456Z"},
		{"timestamp_ntz", dbsql.SqlString, "2026-09-22 12:34:56.123456"},
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

func TestParameterRowIteratorPreservesTimestampInstants(t *testing.T) {
	for _, timezone := range []string{"UTC", "Asia/Tokyo", "Europe/Amsterdam"} {
		t.Run(timezone, func(t *testing.T) {
			timestampType := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: timezone}
			schema := arrow.NewSchema([]arrow.Field{{Name: "timestamp", Type: timestampType}}, nil)
			record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[
				{"timestamp": -9223372036854775808},
				{"timestamp": 0},
				{"timestamp": 9223372036854775807}
			]`))
			require.NoError(t, err)
			defer record.Release()

			stream, err := array.NewRecordReader(schema, []arrow.RecordBatch{record})
			require.NoError(t, err)
			iterator, err := newParameterRowIterator(stream, namedParameterBinding)
			require.NoError(t, err)
			defer iterator.Release()

			for _, expected := range []string{
				"1677-09-21T00:12:43.145224192Z",
				"1970-01-01T00:00:00Z",
				"2262-04-11T23:47:16.854775807Z",
			} {
				args, ok, err := iterator.Next()
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, dbsql.Parameter{
					Name: "timestamp", Type: dbsql.SqlTimestamp, Value: expected,
				}, args[0].Value)
			}
		})
	}
}

func TestParameterRowIteratorPositionalMultipleBatches(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "ignored", Type: arrow.PrimitiveTypes.Int32}}, nil)
	stream := newInt32RecordReader(t, schema, []int32{1, 2}, []int32{3})
	iterator, err := newParameterRowIterator(stream, parameterBindingModeForQuery("SELECT ?"))
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
			name: "unnamed field in named mode",
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
			name:   "time",
			fields: []arrow.Field{{Name: "time", Type: arrow.FixedWidthTypes.Time32s}},
			status: adbc.StatusNotImplemented,
		},
		{
			name:   "nested",
			fields: []arrow.Field{{Name: "list", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}},
			status: adbc.StatusNotImplemented,
		},
		{
			name:   "uint64",
			fields: []arrow.Field{{Name: "uint64", Type: arrow.PrimitiveTypes.Uint64}},
			status: adbc.StatusNotImplemented,
		},
		{
			name:   "decimal precision above 38",
			fields: []arrow.Field{{Name: "decimal", Type: &arrow.Decimal256Type{Precision: 39, Scale: 2}}},
			status: adbc.StatusNotImplemented,
		},
		{
			name:   "negative decimal scale",
			fields: []arrow.Field{{Name: "decimal", Type: &arrow.Decimal128Type{Precision: 10, Scale: -2}}},
			status: adbc.StatusNotImplemented,
		},
		{
			name:   "decimal scale above precision",
			fields: []arrow.Field{{Name: "decimal", Type: &arrow.Decimal128Type{Precision: 10, Scale: 11}}},
			status: adbc.StatusNotImplemented,
		},
		{
			name: "timestamp with invalid timezone",
			fields: []arrow.Field{{
				Name: "timestamp",
				Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "invalid/timezone"},
			}},
			status: adbc.StatusInvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := arrow.NewSchema(test.fields, nil)
			stream, err := array.NewRecordReader(schema, nil)
			require.NoError(t, err)
			_, err = newParameterRowIterator(stream, namedParameterBinding)
			requireADBCStatus(t, err, test.status)
		})
	}
}

func TestParameterRowIteratorConvertsTypedNulls(t *testing.T) {
	types := []arrow.DataType{
		arrow.FixedWidthTypes.Boolean,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.Binary,
		arrow.FixedWidthTypes.Date32,
		&arrow.TimestampType{Unit: arrow.Microsecond},
		&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
		&arrow.Decimal128Type{Precision: 10, Scale: 2},
	}
	fields := make([]arrow.Field, len(types))
	for i, dataType := range types {
		fields[i] = arrow.Field{Name: fmt.Sprintf("value%d", i), Type: dataType, Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	for _, field := range builder.Fields() {
		field.AppendNull()
	}
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()

	stream, err := array.NewRecordReader(schema, []arrow.RecordBatch{record})
	require.NoError(t, err)
	iterator, err := newParameterRowIterator(stream, namedParameterBinding)
	require.NoError(t, err)
	defer iterator.Release()

	args, ok, err := iterator.Next()
	require.NoError(t, err)
	require.True(t, ok)
	for i, arg := range args {
		require.Equal(t, dbsql.Parameter{Name: fields[i].Name, Type: dbsql.SqlVoid}, arg.Value)
	}
}

func TestBindQuery(t *testing.T) {
	fields := []arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int32},
		{Name: "decimal", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}},
		{Name: "binary", Type: arrow.BinaryTypes.Binary},
		{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
	}
	tests := []struct {
		name, query, expected string
		fields                []arrow.Field
	}{
		{
			name: "positional", fields: fields,
			query:    "SELECT ?, ?, ?, ?",
			expected: "SELECT CAST(? AS INT), CAST(? AS DECIMAL(10,2)), unhex(?), CAST(? AS TIMESTAMP_NTZ)",
		},
		{
			name: "named", fields: fields,
			query:    "SELECT :timestamp, :value, :binary, :decimal, :value",
			expected: "SELECT CAST(:timestamp AS TIMESTAMP_NTZ), CAST(:value AS INT), unhex(:binary), CAST(:decimal AS DECIMAL(10,2)), CAST(:value AS INT)",
		},
		{
			name: "quoted and commented markers", fields: fields[:1],
			query:    "SELECT ':value ?', \"?\", `:value`, :value -- :value ?\n/* outer /* :value ? */ */",
			expected: "SELECT ':value ?', \"?\", `:value`, CAST(:value AS INT) -- :value ?\n/* outer /* :value ? */ */",
		},
		{
			name: "variant path and cast operator", fields: fields[:1],
			query:    "SELECT v:value, v::STRING, :value",
			expected: "SELECT v:value, v::STRING, CAST(:value AS INT)",
		},
		{
			name: "name prefix", fields: append(fields[:1:1], arrow.Field{Name: "value2", Type: arrow.PrimitiveTypes.Int64}),
			query:    "SELECT :value2, :value",
			expected: "SELECT CAST(:value2 AS BIGINT), CAST(:value AS INT)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stream, err := array.NewRecordReader(arrow.NewSchema(test.fields, nil), nil)
			require.NoError(t, err)
			iterator, err := newParameterRowIterator(stream, parameterBindingModeForQuery(test.query))
			require.NoError(t, err)
			defer iterator.Release()
			query, err := iterator.bindQuery(test.query)
			require.NoError(t, err)
			require.Equal(t, test.expected, query)
		})
	}
}

func TestBindQueryRejectsInvalidParameters(t *testing.T) {
	for _, query := range []string{"SELECT ?, ?", "SELECT 1", "SELECT :missing", "SELECT ?, :value"} {
		t.Run(query, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
			stream, err := array.NewRecordReader(schema, nil)
			require.NoError(t, err)
			iterator, err := newParameterRowIterator(stream, parameterBindingModeForQuery(query))
			require.NoError(t, err)
			defer iterator.Release()
			_, err = iterator.bindQuery(query)
			requireADBCStatus(t, err, adbc.StatusInvalidArgument)
		})
	}
}

func TestParameterizedQueryReaderReleasesBuffers(t *testing.T) {
	for _, consume := range []bool{false, true} {
		t.Run(fmt.Sprintf("consume=%t", consume), func(t *testing.T) {
			allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer allocator.AssertSize(t, 0)
			schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
			newReader := func() array.RecordReader {
				record, _, err := array.RecordFromJSON(allocator, schema, strings.NewReader(`[{"value":1},{"value":2}]`))
				require.NoError(t, err)
				defer record.Release()
				reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{record})
				require.NoError(t, err)
				return reader
			}
			iterator, err := newParameterRowIterator(newReader(), namedParameterBinding)
			require.NoError(t, err)
			reader, err := newParameterizedQueryReader(iterator, func(_ []driver.NamedValue) (array.RecordReader, error) {
				return newReader(), nil
			})
			require.NoError(t, err)
			defer reader.Release()
			require.True(t, reader.Next())
			if consume {
				for reader.Next() {
				}
				require.NoError(t, reader.Err())
			}
		})
	}
}

func TestParameterizedQueryReaderConcatenatesResultsLazily(t *testing.T) {
	parameterSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	iterator, err := newParameterRowIterator(
		newInt32RecordReader(t, parameterSchema, []int32{1, 2}, []int32{3}), namedParameterBinding)
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
	require.False(t, reader.Next())
}

func TestParameterizedQueryReaderRejectsSchemaChanges(t *testing.T) {
	parameterSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	iterator, err := newParameterRowIterator(
		newInt32RecordReader(t, parameterSchema, []int32{1, 2}), namedParameterBinding)
	require.NoError(t, err)

	executions := 0
	reader, err := newParameterizedQueryReader(iterator, func(_ []driver.NamedValue) (array.RecordReader, error) {
		executions++
		schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.PrimitiveTypes.Int32}}, nil)
		if executions == 2 {
			schema = arrow.NewSchema([]arrow.Field{{Name: "changed", Type: arrow.PrimitiveTypes.Int64}}, nil)
			record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"changed":2}]`))
			require.NoError(t, err)
			defer record.Release()
			return array.NewRecordReader(schema, []arrow.RecordBatch{record})
		}
		return newInt32RecordReader(t, schema, []int32{int32(executions)}), nil
	})
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next())
	require.False(t, reader.Next())
	requireADBCStatus(t, reader.Err(), adbc.StatusInvalidData)
}

func TestParameterizedQueryReaderNormalizesResultSchemas(t *testing.T) {
	parameterSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	iterator, err := newParameterRowIterator(
		newInt32RecordReader(t, parameterSchema, []int32{1, 2}), namedParameterBinding)
	require.NoError(t, err)
	executions := 0
	reader, err := newParameterizedQueryReader(iterator, func(_ []driver.NamedValue) (array.RecordReader, error) {
		executions++
		schema := arrow.NewSchema([]arrow.Field{{
			Name: fmt.Sprintf("(1 + %d)", executions), Type: arrow.PrimitiveTypes.Int32,
			Nullable: executions == 2,
		}}, nil)
		return newInt32RecordReader(t, schema, []int32{int32(executions)}), nil
	})
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Schema().Field(0).Nullable)
	require.Equal(t, "(1 + 1)", reader.Schema().Field(0).Name)

	var retained []arrow.RecordBatch
	for reader.Next() {
		record := reader.RecordBatch()
		require.True(t, record.Schema().Equal(reader.Schema()))
		record.Retain()
		retained = append(retained, record)
	}
	require.NoError(t, reader.Err())
	require.Len(t, retained, 2)
	for i, record := range retained {
		require.EqualValues(t, i+1, record.Column(0).(*array.Int32).Value(0))
		record.Release()
	}
	require.Nil(t, reader.RecordBatch())
}

func TestParameterResultSchemasRejectExtensionChanges(t *testing.T) {
	expected := arrow.NewSchema([]arrow.Field{{
		Name: "value", Type: arrow.BinaryTypes.Binary,
		Metadata: arrow.NewMetadata([]string{"ARROW:extension:name"}, []string{"first"}),
	}}, nil)
	actual := arrow.NewSchema([]arrow.Field{{
		Name: "value", Type: arrow.BinaryTypes.Binary,
		Metadata: arrow.NewMetadata([]string{"ARROW:extension:name"}, []string{"second"}),
	}}, nil)
	require.False(t, parameterResultSchemasCompatible(expected, actual))
}

func TestParameterizedQueryReaderRejectsEmptyInput(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	stream, err := array.NewRecordReader(schema, nil)
	require.NoError(t, err)
	iterator, err := newParameterRowIterator(stream, namedParameterBinding)
	require.NoError(t, err)

	_, err = newParameterizedQueryReader(iterator, func(_ []driver.NamedValue) (array.RecordReader, error) {
		t.Fatal("query should not execute")
		return nil, nil
	})
	requireADBCStatus(t, err, adbc.StatusInvalidArgument)
}

func TestExecuteUpdateRunsOncePerParameterRow(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		parameterName string
		prepared      bool
	}{
		{name: "named", query: "UPDATE target SET value = :value", parameterName: "value"},
		{name: "positional", query: "UPDATE target SET value = ?", parameterName: ""},
		{name: "prepared named", query: "UPDATE target SET value = :value", parameterName: "value", prepared: true},
		{name: "prepared positional", query: "UPDATE target SET value = ?", parameterName: "", prepared: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			capture := &parameterCaptureConn{}
			driverName := fmt.Sprintf("databricks-parameter-test-%d", parameterTestDriverCounter.Add(1))
			sql.Register(driverName, parameterCaptureDriver{conn: capture})
			database, err := sql.Open(driverName, "")
			require.NoError(t, err)
			defer func() { require.NoError(t, database.Close()) }()
			sqlConn, err := database.Conn(context.Background())
			require.NoError(t, err)
			defer func() { require.NoError(t, sqlConn.Close()) }()

			schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
			statement := &statementImpl{
				conn:        &connectionImpl{conn: sqlConn},
				query:       test.query,
				boundStream: newInt32RecordReader(t, schema, []int32{1, 2}, []int32{3}),
			}
			defer func() { require.NoError(t, statement.Close()) }()
			if test.prepared {
				require.NoError(t, statement.Prepare(context.Background()))
			}

			rowsAffected, err := statement.ExecuteUpdate(context.Background())
			require.NoError(t, err)
			require.EqualValues(t, 3, rowsAffected)
			require.Len(t, capture.calls, 3)
			expectedQuery := "UPDATE target SET value = CAST(? AS INT)"
			if test.parameterName != "" {
				expectedQuery = "UPDATE target SET value = CAST(:value AS INT)"
			}
			require.Equal(t, []string{expectedQuery, expectedQuery, expectedQuery}, capture.queries)
			for i, args := range capture.calls {
				require.Len(t, args, 1)
				parameter := args[0].Value.(dbsql.Parameter)
				require.Equal(t, test.parameterName, parameter.Name)
				require.Equal(t, dbsql.SqlInteger, parameter.Type)
				require.Equal(t, strconv.Itoa(i+1), parameter.Value)
			}
			require.Nil(t, statement.boundStream)
		})
	}
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
	calls   [][]driver.NamedValue
	queries []string
}

func (c *parameterCaptureConn) Prepare(query string) (driver.Stmt, error) {
	return parameterCaptureStmt{conn: c, query: query}, nil
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

func (c *parameterCaptureConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.queries = append(c.queries, query)
	clonedArgs := append([]driver.NamedValue(nil), args...)
	c.calls = append(c.calls, clonedArgs)
	return driver.RowsAffected(1), nil
}

type parameterCaptureStmt struct {
	conn  *parameterCaptureConn
	query string
}

func (s parameterCaptureStmt) Close() error  { return nil }
func (s parameterCaptureStmt) NumInput() int { return -1 }

func (s parameterCaptureStmt) Exec(values []driver.Value) (driver.Result, error) {
	args := make([]driver.NamedValue, len(values))
	for i, value := range values {
		args[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
	}
	return s.conn.ExecContext(context.Background(), s.query, args)
}

func (s parameterCaptureStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("not implemented")
}
