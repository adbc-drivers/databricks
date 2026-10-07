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
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	dbsql "github.com/databricks/databricks-sql-go"
)

type parameterRowIterator struct {
	stream              array.RecordReader
	batch               arrow.RecordBatch
	row                 int
	named               bool
	timestampConverters []func(arrow.Timestamp) time.Time
}

type parameterBindingMode uint8

const (
	positionalParameterBinding parameterBindingMode = iota
	namedParameterBinding
)

// newParameterRowIterator takes ownership of stream.
func newParameterRowIterator(stream array.RecordReader, mode parameterBindingMode) (*parameterRowIterator, error) {
	if stream == nil {
		return nil, adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "parameter stream is nil"}
	}

	it := &parameterRowIterator{stream: stream, named: mode == namedParameterBinding}
	schema := stream.Schema()
	if schema == nil {
		it.Release()
		return nil, adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "parameter stream has no schema"}
	}

	fields := schema.Fields()
	it.timestampConverters = make([]func(arrow.Timestamp) time.Time, len(fields))

	seenNames := make(map[string]struct{}, len(fields))
	for i, field := range fields {
		if it.named && field.Name == "" {
			it.Release()
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "named parameter fields must have names",
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
		if field.Type.ID() == arrow.TIMESTAMP {
			converter, err := field.Type.(*arrow.TimestampType).GetToTimeFunc()
			if err != nil {
				it.Release()
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid timestamp parameter type %s: %v", field.Type, err),
				}
			}
			it.timestampConverters[i] = converter
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
		arrow.UINT8, arrow.UINT16, arrow.UINT32,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64,
		arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW,
		arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY,
		arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP:
		return nil
	case arrow.DECIMAL128, arrow.DECIMAL256:
		decimalType := dataType.(arrow.DecimalType)
		if decimalType.GetPrecision() > 0 && decimalType.GetPrecision() <= 38 &&
			decimalType.GetScale() >= 0 && decimalType.GetScale() <= decimalType.GetPrecision() {
			return nil
		}
	}

	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("parameter type %s is not supported", dataType),
	}
}

type parameterMarker struct {
	start, end int
	name       string
}

// parameterMarkers ignores markers in quoted text and SQL comments.
func parameterMarkers(query string) []parameterMarker {
	const (
		queryText = iota
		singleQuoted
		doubleQuoted
		backtickQuoted
		lineComment
		blockComment
	)

	state := queryText
	blockCommentDepth := 0
	var markers []parameterMarker
	for i := 0; i < len(query); i++ {
		ch := query[i]
		next := byte(0)
		if i+1 < len(query) {
			next = query[i+1]
		}

		switch state {
		case queryText:
			switch {
			case ch == '?':
				markers = append(markers, parameterMarker{start: i, end: i + 1})
			case ch == ':' && next != ':' && (i == 0 || query[i-1] != ':'):
				if i > 0 {
					prev, _ := utf8.DecodeLastRuneInString(query[:i])
					if isParameterNameRune(prev, false) || strings.ContainsRune(")]`", prev) {
						continue
					}
				}
				end := i + 1
				for end < len(query) {
					r, size := utf8.DecodeRuneInString(query[end:])
					if !isParameterNameRune(r, end == i+1) {
						break
					}
					end += size
				}
				if end > i+1 {
					markers = append(markers, parameterMarker{start: i, end: end, name: query[i+1 : end]})
					i = end - 1
				}
			case ch == '\'':
				state = singleQuoted
			case ch == '"':
				state = doubleQuoted
			case ch == '`':
				state = backtickQuoted
			case ch == '-' && next == '-':
				state = lineComment
				i++
			case ch == '/' && next == '*':
				state = blockComment
				blockCommentDepth = 1
				i++
			}
		case singleQuoted, doubleQuoted, backtickQuoted:
			quote := byte('\'')
			switch state {
			case doubleQuoted:
				quote = '"'
			case backtickQuoted:
				quote = '`'
			}
			if ch == '\\' && next != 0 {
				i++
			} else if ch == quote {
				if next == quote {
					i++
				} else {
					state = queryText
				}
			}
		case lineComment:
			if ch == '\n' || ch == '\r' {
				state = queryText
			}
		case blockComment:
			if ch == '/' && next == '*' {
				blockCommentDepth++
				i++
			} else if ch == '*' && next == '/' {
				blockCommentDepth--
				i++
				if blockCommentDepth == 0 {
					state = queryText
				}
			}
		}
	}

	return markers
}

func isParameterNameRune(r rune, first bool) bool {
	return r == '_' || unicode.IsLetter(r) || (!first && unicode.IsDigit(r))
}

func parameterBindingModeForQuery(query string) parameterBindingMode {
	for _, marker := range parameterMarkers(query) {
		if marker.name == "" {
			return positionalParameterBinding
		}
	}
	return namedParameterBinding
}

func (it *parameterRowIterator) bindQuery(query string) (string, error) {
	fields := it.stream.Schema().Fields()
	byName := make(map[string]int, len(fields))
	for i, field := range fields {
		byName[field.Name] = i
	}

	var result strings.Builder
	last, position := 0, 0
	markers := parameterMarkers(query)
	if len(markers) == 0 && len(fields) != 0 {
		return "", adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "query has no parameter markers"}
	}
	for _, marker := range markers {
		if (marker.name != "") != it.named {
			return "", adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "named and positional parameters cannot be mixed"}
		}
		index := position
		if it.named {
			var ok bool
			index, ok = byName[marker.name]
			if !ok {
				return "", adbc.Error{Code: adbc.StatusInvalidArgument, Msg: fmt.Sprintf("no bound parameter named %q", marker.name)}
			}
		} else {
			position++
			if index >= len(fields) {
				return "", adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "parameter count does not match bound columns"}
			}
		}

		result.WriteString(query[last:marker.start])
		markerText := query[marker.start:marker.end]
		dataType := fields[index].Type
		switch dataType.ID() {
		case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
			fmt.Fprintf(&result, "unhex(%s)", markerText)
		default:
			// The base driver encodes NULL as VOID; casts preserve the Arrow type.
			fmt.Fprintf(&result, "CAST(%s AS %s)", markerText, parameterSQLType(dataType))
		}
		last = marker.end
	}
	if !it.named && position != len(fields) {
		return "", adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "parameter count does not match bound columns"}
	}
	result.WriteString(query[last:])
	return result.String(), nil
}

func parameterSQLType(dataType arrow.DataType) string {
	switch dataType.ID() {
	case arrow.NULL:
		return "VOID"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT16, arrow.UINT8:
		return "SMALLINT"
	case arrow.INT32, arrow.UINT16:
		return "INT"
	case arrow.INT64, arrow.UINT32:
		return "BIGINT"
	case arrow.FLOAT16, arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.DATE32, arrow.DATE64:
		return "DATE"
	case arrow.TIMESTAMP:
		if dataType.(*arrow.TimestampType).TimeZone == "" {
			return "TIMESTAMP_NTZ"
		}
		return "TIMESTAMP"
	case arrow.DECIMAL128, arrow.DECIMAL256:
		decimalType := dataType.(arrow.DecimalType)
		return fmt.Sprintf("DECIMAL(%d,%d)", decimalType.GetPrecision(), decimalType.GetScale())
	default:
		return "STRING"
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
				parameter, err := arrowValueToParameter(
					it.batch.Column(col), row, name, it.timestampConverters[col])
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

func arrowValueToParameter(
	values arrow.Array,
	row int,
	name string,
	timestampConverter func(arrow.Timestamp) time.Time,
) (dbsql.Parameter, error) {
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
	case arrow.FLOAT16:
		parameter.Type = dbsql.SqlFloat
		parameter.Value = values.(*array.Float16).Value(row).String()
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
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
		parameter.Type = dbsql.SqlString
		var value []byte
		switch values := values.(type) {
		case *array.Binary:
			value = values.Value(row)
		case *array.LargeBinary:
			value = values.Value(row)
		case *array.BinaryView:
			value = values.Value(row)
		case *array.FixedSizeBinary:
			value = values.Value(row)
		}
		parameter.Value = hex.EncodeToString(value)
	case arrow.DECIMAL128:
		parameter.Type = dbsql.SqlString
		parameter.Value = values.(*array.Decimal128).Value(row).ToString(values.DataType().(arrow.DecimalType).GetScale())
	case arrow.DECIMAL256:
		parameter.Type = dbsql.SqlString
		parameter.Value = values.(*array.Decimal256).Value(row).ToString(values.DataType().(arrow.DecimalType).GetScale())
	case arrow.DATE32:
		parameter.Type = dbsql.SqlDate
		parameter.Value = values.(*array.Date32).Value(row).ToTime().Format(time.DateOnly)
	case arrow.DATE64:
		parameter.Type = dbsql.SqlDate
		parameter.Value = values.(*array.Date64).Value(row).ToTime().Format(time.DateOnly)
	case arrow.TIMESTAMP:
		value := timestampConverter(values.(*array.Timestamp).Value(row))
		if values.DataType().(*arrow.TimestampType).TimeZone == "" {
			parameter.Type = dbsql.SqlString
			parameter.Value = value.Format("2006-01-02 15:04:05.999999999")
		} else {
			parameter.Type = dbsql.SqlTimestamp
			// Historical timezone offsets can contain seconds, which RFC3339 cannot encode.
			parameter.Value = value.UTC().Format(time.RFC3339Nano)
		}
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
	record   arrow.RecordBatch
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

	fields := reader.Schema().Fields()
	for i := range fields {
		fields[i].Nullable = true
	}
	metadata := reader.Schema().Metadata()
	result := &parameterizedQueryReader{
		iterator: iterator,
		execute:  execute,
		current:  reader,
		schema:   arrow.NewSchemaWithEndian(fields, &metadata, reader.Schema().Endianness()),
	}
	result.refCount.Store(1)
	return result, nil
}

func (r *parameterizedQueryReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *parameterizedQueryReader) Next() bool {
	if r.record != nil {
		r.record.Release()
		r.record = nil
	}
	if r.closed || r.err != nil || r.iterator == nil {
		return false
	}

	for {
		if r.current != nil {
			if r.current.Next() {
				record := r.current.RecordBatch()
				r.record = array.NewRecordBatch(r.schema, record.Columns(), record.NumRows())
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
		if !parameterResultSchemasCompatible(r.schema, reader.Schema()) {
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
	return r.record
}

func parameterResultSchemasCompatible(expected, actual *arrow.Schema) bool {
	if expected.NumFields() != actual.NumFields() || expected.Endianness() != actual.Endianness() {
		return false
	}
	for i, field := range expected.Fields() {
		other := actual.Field(i)
		if !arrow.TypeEqual(field.Type, other.Type, arrow.CheckMetadata()) {
			return false
		}
		// Extension metadata affects interpretation even when storage types match.
		for _, key := range []string{"ARROW:extension:name", "ARROW:extension:metadata"} {
			value, ok := field.Metadata.GetValue(key)
			otherValue, otherOK := other.Metadata.GetValue(key)
			if ok != otherOK || value != otherValue {
				return false
			}
		}
	}
	return true
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
		if r.record != nil {
			r.record.Release()
			r.record = nil
		}
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
