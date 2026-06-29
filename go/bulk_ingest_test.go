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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func TestBuildTableName(t *testing.T) {
	tests := []struct {
		name    string
		catalog string
		schema  string
		table   string
		want    string
	}{
		{
			name:  "table only",
			table: "my_table",
			want:  "`my_table`",
		},
		{
			name:   "schema and table",
			schema: "my_schema",
			table:  "my_table",
			want:   "`my_schema`.`my_table`",
		},
		{
			name:    "fully qualified",
			catalog: "my_catalog",
			schema:  "my_schema",
			table:   "my_table",
			want:    "`my_catalog`.`my_schema`.`my_table`",
		},
		{
			name:    "with backticks in name",
			catalog: "my`catalog",
			schema:  "my`schema",
			table:   "my`table",
			want:    "`my``catalog`.`my``schema`.`my``table`",
		},
		{
			name:    "catalog without schema",
			catalog: "my_catalog",
			table:   "my_table",
			want:    "`my_catalog`.`my_table`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildTableName(tt.catalog, tt.schema, tt.table)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "`simple`"},
		{"with space", "`with space`"},
		{"with`backtick", "`with``backtick`"},
		{"multiple``backticks", "`multiple````backticks`"},
		{"", "``"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, quoteIdentifier(tt.input))
		})
	}
}

func TestArrowTypeToDatabricksType(t *testing.T) {
	tests := []struct {
		name     string
		dataType arrow.DataType
		want     string
	}{
		{"bool", arrow.FixedWidthTypes.Boolean, "BOOLEAN"},
		{"int8", arrow.PrimitiveTypes.Int8, "TINYINT"},
		{"int16", arrow.PrimitiveTypes.Int16, "SMALLINT"},
		{"int32", arrow.PrimitiveTypes.Int32, "INT"},
		{"int64", arrow.PrimitiveTypes.Int64, "BIGINT"},
		{"uint8", arrow.PrimitiveTypes.Uint8, "SMALLINT"},
		{"uint16", arrow.PrimitiveTypes.Uint16, "INT"},
		{"uint32", arrow.PrimitiveTypes.Uint32, "BIGINT"},
		{"uint64", arrow.PrimitiveTypes.Uint64, "BIGINT"},
		{"float32", arrow.PrimitiveTypes.Float32, "FLOAT"},
		{"float64", arrow.PrimitiveTypes.Float64, "DOUBLE"},
		{"string", arrow.BinaryTypes.String, "STRING"},
		{"large_string", arrow.BinaryTypes.LargeString, "STRING"},
		{"binary", arrow.BinaryTypes.Binary, "BINARY"},
		{"large_binary", arrow.BinaryTypes.LargeBinary, "BINARY"},
		{"date32", arrow.PrimitiveTypes.Date32, "DATE"},
		{"date64", arrow.PrimitiveTypes.Date64, "DATE"},
		{"timestamp_with_tz", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, "TIMESTAMP"},
		{"timestamp_without_tz", &arrow.TimestampType{Unit: arrow.Microsecond}, "TIMESTAMP_NTZ"},
		{"decimal128", &arrow.Decimal128Type{Precision: 10, Scale: 2}, "DECIMAL(10, 2)"},
		{"decimal128_large", &arrow.Decimal128Type{Precision: 38, Scale: 18}, "DECIMAL(38, 18)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, arrowTypeToDatabricksType(tt.dataType))
		})
	}
}

func TestPendingCopy(t *testing.T) {
	p := &pendingCopy{
		path: "Volumes/cat/sch/vol/staging/abc123.parquet",
		rows: 42,
	}

	assert.Equal(t, "Volumes/cat/sch/vol/staging/abc123.parquet", p.String())
	assert.Equal(t, int64(42), p.Rows())
}
