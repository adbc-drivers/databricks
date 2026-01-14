// Copyright (c) 2025 ADBC Drivers Contributors
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
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// executeIngest performs bulk insert using parameterized INSERT statements
func (s *statementImpl) executeIngest(ctx context.Context) (int64, error) {
	if s.boundStream == nil {
		return -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no data bound for ingestion")
	}

	opts := &s.bulkIngestOptions

	tableName := buildTableName(opts.CatalogName, opts.SchemaName, opts.TableName)

	if err := s.createTableIfNeeded(ctx, tableName, s.boundStream.Schema(), opts); err != nil {
		return -1, err
	}

	insertSQL, err := buildInsertSQL(tableName, s.boundStream.Schema())
	if err != nil {
		return -1, err
	}

	totalRows := int64(0)
	params := make([]driver.NamedValue, s.boundStream.Schema().NumFields())

	for s.boundStream.Next() {
		recordBatch := s.boundStream.RecordBatch()

		for rowIdx := range int(recordBatch.NumRows()) {
			// Extract Go values from Arrow columns
			for colIdx := range int(recordBatch.NumCols()) {
				arr := recordBatch.Column(colIdx)
				val, err := extractGoValue(arr, rowIdx)
				if err != nil {
					return totalRows, s.ErrorHelper.Errorf(adbc.StatusInternal, "failed to extract go value: %v", err)
				}
				params[colIdx].Value = val
			}

			// Use ExecContext directly instead of PrepareContext because Databricks doesn't do server-side statement preparation
			result, err := s.conn.conn.ExecContext(ctx, insertSQL, valuesToInterfaces(params)...)
			if err != nil {
				return totalRows, s.ErrorHelper.Errorf(adbc.StatusInternal, "failed to execute the query: %v", err)
			}

			rows, _ := result.RowsAffected()
			totalRows += rows
		}
	}

	if err := s.boundStream.Err(); err != nil {
		return totalRows, s.ErrorHelper.Errorf(adbc.StatusInternal, "stream error: %v", err)
	}

	s.boundStream = nil
	return totalRows, nil
}

// createTableIfNeeded creates/drops table based on ingest mode
func (s *statementImpl) createTableIfNeeded(ctx context.Context, tableName string, schema *arrow.Schema, opts *driverbase.BulkIngestOptions) error {
	switch opts.Mode {
	case adbc.OptionValueIngestModeCreate:
		return s.createTable(ctx, tableName, schema, false)

	case adbc.OptionValueIngestModeCreateAppend:
		return s.createTable(ctx, tableName, schema, true)

	case adbc.OptionValueIngestModeReplace:
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
		if _, err := s.conn.conn.ExecContext(ctx, dropSQL); err != nil {
			return s.ErrorHelper.Errorf(adbc.StatusInternal, "failed to drop the table: %v", err)
		}
		return s.createTable(ctx, tableName, schema, false)

	case adbc.OptionValueIngestModeAppend:
		return nil

	default:
		return s.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "invalid ingest mode: %s", opts.Mode)
	}
}

// createTable generates and executes CREATE TABLE DDL
func (s *statementImpl) createTable(ctx context.Context, tableName string, schema *arrow.Schema, ifNotExists bool) error {
	var sql strings.Builder
	sql.WriteString("CREATE TABLE ")
	if ifNotExists {
		sql.WriteString("IF NOT EXISTS ")
	}
	sql.WriteString(tableName)
	sql.WriteString(" (")

	for i, field := range schema.Fields() {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(quoteIdentifier(field.Name))
		sql.WriteString(" ")
		sql.WriteString(arrowTypeToDatabricksType(field.Type))
		if !field.Nullable {
			sql.WriteString(" NOT NULL")
		}
	}
	sql.WriteString(")")

	_, err := s.conn.conn.ExecContext(ctx, sql.String())
	if err != nil {
		return s.ErrorHelper.Errorf(adbc.StatusInternal, "failed to create table: %v", err)
	}
	return nil
}

// buildInsertSQL generates parameterized INSERT statement
func buildInsertSQL(tableName string, schema *arrow.Schema) (string, error) {
	var sql strings.Builder

	sql.WriteString("INSERT INTO ")
	sql.WriteString(tableName)
	sql.WriteString(" (")

	for i, field := range schema.Fields() {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(quoteIdentifier(field.Name))
	}

	sql.WriteString(") VALUES (")

	for i, field := range schema.Fields() {
		if i > 0 {
			sql.WriteString(", ")
		}

		if field.Type.ID() == arrow.FIXED_SIZE_BINARY {
			// Use UNHEX() to convert hex string to binary
			sql.WriteString("UNHEX(?)")
		} else {
			sql.WriteString("?")
		}
	}

	sql.WriteString(")")
	return sql.String(), nil
}

// buildTableName constructs catalog.schema.table name
func buildTableName(catalog, schema, table string) string {
	parts := []string{}
	if catalog != "" {
		parts = append(parts, quoteIdentifier(catalog))
	}
	if schema != "" {
		parts = append(parts, quoteIdentifier(schema))
	}
	parts = append(parts, quoteIdentifier(table))
	return strings.Join(parts, ".")
}

// quoteIdentifier quotes a Databricks identifier with backticks
func quoteIdentifier(id string) string {
	escaped := strings.ReplaceAll(id, "`", "``")
	return fmt.Sprintf("`%s`", escaped)
}

// valuesToInterfaces converts driver.NamedValue slice to []any for ExecContext
func valuesToInterfaces(params []driver.NamedValue) []any {
	result := make([]any, len(params))
	for i, p := range params {
		result[i] = p.Value
	}
	return result
}

// extractGoValue extracts a Go value from an Arrow array at the given index
func extractGoValue(arr arrow.Array, idx int) (any, error) {
	if arr.IsNull(idx) {
		return nil, nil
	}

	switch arr.DataType().ID() {
	case arrow.BOOL:
		return arr.(*array.Boolean).Value(idx), nil

	case arrow.INT8:
		return int64(arr.(*array.Int8).Value(idx)), nil
	case arrow.INT16:
		return int64(arr.(*array.Int16).Value(idx)), nil
	case arrow.INT32:
		return int64(arr.(*array.Int32).Value(idx)), nil
	case arrow.INT64:
		// https://github.com/databricks/databricks-sql-go/issues/315
		// databricks-sql-go incorrectly maps int64 to SqlInteger (INT) instead of SqlBigInt (BIGINT)
		// Pass as string to preserve full range
		return fmt.Sprintf("%d", arr.(*array.Int64).Value(idx)), nil

	case arrow.UINT8:
		return int64(arr.(*array.Uint8).Value(idx)), nil
	case arrow.UINT16:
		return int64(arr.(*array.Uint16).Value(idx)), nil
	case arrow.UINT32:
		// https://github.com/databricks/databricks-sql-go/issues/315
		// databricks-sql-go incorrectly maps int64 to SqlInteger (INT)
		// Pass as string to preserve full uint32 range
		return fmt.Sprintf("%d", arr.(*array.Uint32).Value(idx)), nil
	case arrow.UINT64:
		// Pass as string to preserve full uint64 range (may still overflow if > int64 max)
		return fmt.Sprintf("%d", arr.(*array.Uint64).Value(idx)), nil

	case arrow.FLOAT32:
		return float64(arr.(*array.Float32).Value(idx)), nil
	case arrow.FLOAT64:
		// https://github.com/databricks/databricks-sql-go/issues/314
		// databricks-sql-go has a bug where float64 is treated as SqlFloat instead of SqlDouble
		// causing precision loss. Pass as string to preserve full precision.
		val := arr.(*array.Float64).Value(idx)
		return fmt.Sprintf("%.17g", val), nil

	case arrow.STRING:
		return arr.(*array.String).Value(idx), nil
	case arrow.LARGE_STRING:
		return arr.(*array.LargeString).Value(idx), nil
	case arrow.STRING_VIEW:
		return arr.(*array.StringView).Value(idx), nil

	case arrow.BINARY:
		return arr.(*array.Binary).Value(idx), nil
	case arrow.LARGE_BINARY:
		return arr.(*array.LargeBinary).Value(idx), nil
	case arrow.BINARY_VIEW:
		return arr.(*array.BinaryView).Value(idx), nil
	case arrow.FIXED_SIZE_BINARY:
		// Convert to hex string for use with UNHEX() SQL function
		return fmt.Sprintf("%x", arr.(*array.FixedSizeBinary).Value(idx)), nil

	case arrow.DATE32:
		return arr.(*array.Date32).Value(idx).ToTime(), nil
	case arrow.DATE64:
		return arr.(*array.Date64).Value(idx).ToTime(), nil

	case arrow.TIMESTAMP:
		ts := arr.DataType().(*arrow.TimestampType)
		return arr.(*array.Timestamp).Value(idx).ToTime(ts.Unit), nil

	case arrow.DECIMAL128:
		dec := arr.(*array.Decimal128)
		// Return as string, databricks-sql-go will infer DECIMAL type
		return dec.ValueStr(idx), nil

	default:
		return nil, fmt.Errorf("unsupported Arrow type: %s", arr.DataType())
	}
}

// arrowTypeToDatabricksType maps Arrow types to Databricks DDL types for CREATE TABLE
func arrowTypeToDatabricksType(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INT"
	case arrow.INT64:
		return "BIGINT"
	case arrow.UINT8:
		return "SMALLINT"
	case arrow.UINT16:
		return "INT"
	case arrow.UINT32:
		return "BIGINT"
	case arrow.UINT64:
		return "BIGINT"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "STRING"
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
		return "BINARY"
	case arrow.DATE32, arrow.DATE64:
		return "DATE"
	case arrow.TIMESTAMP:
		ts := dt.(*arrow.TimestampType)
		if ts.TimeZone != "" {
			return "TIMESTAMP"
		}
		return "TIMESTAMP_NTZ"
	case arrow.DECIMAL128:
		dec := dt.(*arrow.Decimal128Type)
		return fmt.Sprintf("DECIMAL(%d, %d)", dec.Precision, dec.Scale)
	default:
		return "STRING" // Fallback
	}
}
