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
	"database/sql/driver"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	dbsql "github.com/databricks/databricks-sql-go"
)

type parameterRowIterator struct {
	stream array.RecordReader
	batch  arrow.RecordBatch
	row    int
	named  bool
}

// newParameterRowIterator takes ownership of stream.
func newParameterRowIterator(stream array.RecordReader) (*parameterRowIterator, error) {
	if stream == nil {
		return nil, adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "parameter stream is nil"}
	}

	it := &parameterRowIterator{stream: stream}
	schema := stream.Schema()
	if schema == nil {
		it.Release()
		return nil, adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "parameter stream has no schema"}
	}

	fields := schema.Fields()
	if len(fields) > 0 {
		it.named = fields[0].Name != ""
	}

	seenNames := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if (field.Name != "") != it.named {
			it.Release()
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "parameter fields must be either all named or all unnamed",
			}
		}
		if it.named {
			if _, ok := seenNames[field.Name]; ok {
				it.Release()
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("duplicate parameter name %q", field.Name),
				}
			}
			seenNames[field.Name] = struct{}{}
		}
		if err := validateParameterType(field.Type); err != nil {
			it.Release()
			return nil, err
		}
	}

	return it, nil
}

func validateParameterType(dataType arrow.DataType) error {
	if dataType == nil {
		return adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "parameter field has no Arrow type"}
	}

	switch dataType.ID() {
	case arrow.NULL,
		arrow.BOOL,
		arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64,
		arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW,
		arrow.DATE32, arrow.DATE64,
		arrow.TIMESTAMP:
		return nil
	case arrow.DECIMAL128:
		if dataType.(*arrow.Decimal128Type).Precision <= 38 {
			return nil
		}
	case arrow.DECIMAL256:
		if dataType.(*arrow.Decimal256Type).Precision <= 38 {
			return nil
		}
	}

	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("parameter type %s is not supported", dataType),
	}
}

func (it *parameterRowIterator) Next() ([]driver.NamedValue, bool, error) {
	for {
		if it.stream == nil {
			return nil, false, nil
		}

		if it.batch != nil && it.row < int(it.batch.NumRows()) {
			row := it.row
			it.row++

			args := make([]driver.NamedValue, int(it.batch.NumCols()))
			for col := range int(it.batch.NumCols()) {
				name := ""
				if it.named {
					name = it.batch.Schema().Field(col).Name
				}
				parameter, err := arrowValueToParameter(it.batch.Column(col), row, name)
				if err != nil {
					it.Release()
					return nil, false, err
				}
				args[col] = driver.NamedValue{
					Name:    name,
					Ordinal: col + 1,
					Value:   parameter,
				}
			}
			return args, true, nil
		}

		if !it.stream.Next() {
			err := it.stream.Err()
			it.Release()
			if err != nil {
				return nil, false, adbc.Error{
					Code: adbc.StatusInternal,
					Msg:  fmt.Sprintf("failed to read parameter stream: %v", err),
				}
			}
			return nil, false, nil
		}

		it.batch = it.stream.RecordBatch()
		it.row = 0
	}
}

func (it *parameterRowIterator) Release() {
	if it.stream != nil {
		it.stream.Release()
		it.stream = nil
		it.batch = nil
	}
}

func arrowValueToParameter(values arrow.Array, row int, name string) (dbsql.Parameter, error) {
	parameter := dbsql.Parameter{Name: name}
	if values.IsNull(row) {
		parameter.Type = dbsql.SqlVoid
		return parameter, nil
	}

	switch values.DataType().ID() {
	case arrow.NULL:
		parameter.Type = dbsql.SqlVoid
	case arrow.BOOL:
		parameter.Type = dbsql.SqlBoolean
		parameter.Value = strconv.FormatBool(values.(*array.Boolean).Value(row))
	case arrow.INT8:
		parameter.Type = dbsql.SqlTinyInt
		parameter.Value = strconv.FormatInt(int64(values.(*array.Int8).Value(row)), 10)
	case arrow.INT16:
		parameter.Type = dbsql.SqlSmallInt
		parameter.Value = strconv.FormatInt(int64(values.(*array.Int16).Value(row)), 10)
	case arrow.INT32:
		parameter.Type = dbsql.SqlInteger
		parameter.Value = strconv.FormatInt(int64(values.(*array.Int32).Value(row)), 10)
	case arrow.INT64:
		parameter.Type = dbsql.SqlBigInt
		parameter.Value = strconv.FormatInt(values.(*array.Int64).Value(row), 10)
	case arrow.UINT8:
		parameter.Type = dbsql.SqlSmallInt
		parameter.Value = strconv.FormatUint(uint64(values.(*array.Uint8).Value(row)), 10)
	case arrow.UINT16:
		parameter.Type = dbsql.SqlInteger
		parameter.Value = strconv.FormatUint(uint64(values.(*array.Uint16).Value(row)), 10)
	case arrow.UINT32:
		parameter.Type = dbsql.SqlBigInt
		parameter.Value = strconv.FormatUint(uint64(values.(*array.Uint32).Value(row)), 10)
	case arrow.UINT64:
		parameter.Type = dbsql.SqlDecimal
		parameter.Value = strconv.FormatUint(values.(*array.Uint64).Value(row), 10)
	case arrow.FLOAT32:
		parameter.Type = dbsql.SqlFloat
		parameter.Value = strconv.FormatFloat(float64(values.(*array.Float32).Value(row)), 'g', -1, 32)
	case arrow.FLOAT64:
		parameter.Type = dbsql.SqlDouble
		parameter.Value = strconv.FormatFloat(values.(*array.Float64).Value(row), 'g', -1, 64)
	case arrow.STRING:
		parameter.Type = dbsql.SqlString
		parameter.Value = values.(*array.String).Value(row)
	case arrow.LARGE_STRING:
		parameter.Type = dbsql.SqlString
		parameter.Value = values.(*array.LargeString).Value(row)
	case arrow.STRING_VIEW:
		parameter.Type = dbsql.SqlString
		parameter.Value = values.(*array.StringView).Value(row)
	case arrow.DATE32:
		parameter.Type = dbsql.SqlDate
		parameter.Value = values.(*array.Date32).Value(row).ToTime().Format(time.DateOnly)
	case arrow.DATE64:
		parameter.Type = dbsql.SqlDate
		parameter.Value = values.(*array.Date64).Value(row).ToTime().Format(time.DateOnly)
	case arrow.TIMESTAMP:
		dataType := values.DataType().(*arrow.TimestampType)
		toTime, err := dataType.GetToTimeFunc()
		if err != nil {
			return parameter, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("invalid timestamp parameter type %s: %v", dataType, err),
			}
		}
		parameter.Type = dbsql.SqlTimestamp
		parameter.Value = toTime(values.(*array.Timestamp).Value(row)).Format(time.RFC3339Nano)
	case arrow.DECIMAL128:
		dataType := values.DataType().(*arrow.Decimal128Type)
		parameter.Type = dbsql.SqlDecimal
		parameter.Value = values.(*array.Decimal128).Value(row).ToString(dataType.Scale)
	case arrow.DECIMAL256:
		dataType := values.DataType().(*arrow.Decimal256Type)
		parameter.Type = dbsql.SqlDecimal
		parameter.Value = values.(*array.Decimal256).Value(row).ToString(dataType.Scale)
	default:
		return parameter, adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("parameter type %s is not supported", values.DataType()),
		}
	}

	return parameter, nil
}

type parameterQueryExecutor func([]driver.NamedValue) (array.RecordReader, error)

type parameterizedQueryReader struct {
	refCount atomic.Int64
	iterator *parameterRowIterator
	execute  parameterQueryExecutor
	current  array.RecordReader
	schema   *arrow.Schema
	err      error
	closed   bool
}

func newParameterizedQueryReader(iterator *parameterRowIterator, execute parameterQueryExecutor) (array.RecordReader, error) {
	args, ok, err := iterator.Next()
	if err != nil {
		iterator.Release()
		return nil, err
	}
	if !ok {
		iterator.Release()
		return nil, adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "parameter stream contains no rows"}
	}

	reader, err := execute(args)
	if err != nil {
		iterator.Release()
		return nil, err
	}
	if reader == nil || reader.Schema() == nil {
		if reader != nil {
			reader.Release()
		}
		iterator.Release()
		return nil, adbc.Error{Code: adbc.StatusInternal, Msg: "parameterized query returned no schema"}
	}

	result := &parameterizedQueryReader{
		iterator: iterator,
		execute:  execute,
		current:  reader,
		schema:   reader.Schema(),
	}
	result.refCount.Store(1)
	return result, nil
}

func (r *parameterizedQueryReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *parameterizedQueryReader) Next() bool {
	if r.closed || r.err != nil || r.iterator == nil {
		return false
	}

	for {
		if r.current != nil {
			if r.current.Next() {
				return true
			}
			if err := r.current.Err(); err != nil {
				r.fail(err)
				return false
			}
			r.current.Release()
			r.current = nil
		}

		args, ok, err := r.iterator.Next()
		if err != nil {
			r.fail(err)
			return false
		}
		if !ok {
			r.iterator.Release()
			r.iterator = nil
			return false
		}

		reader, err := r.execute(args)
		if err != nil {
			r.fail(err)
			return false
		}
		if reader == nil || reader.Schema() == nil {
			if reader != nil {
				reader.Release()
			}
			r.fail(adbc.Error{Code: adbc.StatusInternal, Msg: "parameterized query returned no schema"})
			return false
		}
		if !reader.Schema().Equal(r.schema) {
			reader.Release()
			r.fail(adbc.Error{
				Code: adbc.StatusInvalidData,
				Msg:  "parameterized query returned inconsistent result schemas",
			})
			return false
		}
		r.current = reader
	}
}

func (r *parameterizedQueryReader) RecordBatch() arrow.RecordBatch {
	if r.current == nil {
		return nil
	}
	return r.current.RecordBatch()
}

func (r *parameterizedQueryReader) Record() arrow.RecordBatch {
	return r.RecordBatch()
}

func (r *parameterizedQueryReader) Err() error {
	return r.err
}

func (r *parameterizedQueryReader) Retain() {
	r.refCount.Add(1)
}

func (r *parameterizedQueryReader) Release() {
	if r.refCount.Add(-1) == 0 {
		r.closed = true
		if r.current != nil {
			r.current.Release()
			r.current = nil
		}
		if r.iterator != nil {
			r.iterator.Release()
			r.iterator = nil
		}
	}
}

func (r *parameterizedQueryReader) fail(err error) {
	r.err = err
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.iterator != nil {
		r.iterator.Release()
		r.iterator = nil
	}
}
