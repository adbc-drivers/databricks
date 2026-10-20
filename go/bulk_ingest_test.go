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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestIngestDictionary(t *testing.T) {
	tests := []struct {
		name           string
		valueType      arrow.DataType
		dictionaryJSON string
		sqlType        string
		placeholder    string
		want           []any
	}{
		{
			name: "string", valueType: arrow.BinaryTypes.String,
			dictionaryJSON: `["hello", "", null]`, sqlType: "STRING", placeholder: "?",
			want: []any{"", "hello", "", nil, nil},
		},
		{
			name: "large_string", valueType: arrow.BinaryTypes.LargeString,
			dictionaryJSON: `["hello", "", null]`, sqlType: "STRING", placeholder: "?",
			want: []any{"", "hello", "", nil, nil},
		},
		{
			name: "binary", valueType: arrow.BinaryTypes.Binary,
			dictionaryJSON: `["AP9B", "", null]`, sqlType: "BINARY", placeholder: "UNHEX(?)",
			want: []any{"", "00ff41", "", nil, nil},
		},
		{
			name: "large_binary", valueType: arrow.BinaryTypes.LargeBinary,
			dictionaryJSON: `["AP9B", "", null]`, sqlType: "BINARY", placeholder: "UNHEX(?)",
			want: []any{"", "00ff41", "", nil, nil},
		},
		{
			name: "binary_view", valueType: arrow.BinaryTypes.BinaryView,
			dictionaryJSON: `["AP9B", "", null]`, sqlType: "BINARY", placeholder: "UNHEX(?)",
			want: []any{"", "00ff41", "", nil, nil},
		},
		{
			name: "fixed_size_binary", valueType: &arrow.FixedSizeBinaryType{ByteWidth: 3},
			dictionaryJSON: `["AP9B", "AAEC", null]`, sqlType: "BINARY", placeholder: "UNHEX(?)",
			want: []any{"000102", "00ff41", "000102", nil, nil},
		},
		{
			name: "int64", valueType: arrow.PrimitiveTypes.Int64,
			dictionaryJSON: `[9223372036854775807, -5, null]`, sqlType: "BIGINT", placeholder: "?",
			want: []any{"-5", "9223372036854775807", "-5", nil, nil},
		},
		{
			name: "timestamp", valueType: &arrow.TimestampType{Unit: arrow.Microsecond},
			dictionaryJSON: `[1234567, 0, null]`, sqlType: "TIMESTAMP_NTZ", placeholder: "?",
			want: []any{time.UnixMicro(0).UTC(), time.UnixMicro(1234567).UTC(), time.UnixMicro(0).UTC(), nil, nil},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			values, _, err := array.FromJSON(mem, test.valueType, strings.NewReader(test.dictionaryJSON))
			require.NoError(t, err)
			defer values.Release()
			// Exercise repeated/out-of-order indices, a null index, and a null dictionary value.
			indices, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[1, 0, 1, null, 2]`))
			require.NoError(t, err)
			defer indices.Release()
			dictionaryType := &arrow.DictionaryType{IndexType: indices.DataType(), ValueType: test.valueType}
			dictionary, err := array.NewValidatedDictionaryArray(dictionaryType, indices, values)
			require.NoError(t, err)
			defer dictionary.Release()
			schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: dictionaryType, Nullable: true}}, nil)
			record := array.NewRecordBatch(schema, []arrow.Array{dictionary}, int64(dictionary.Len()))
			defer record.Release()

			capture := &parameterCaptureConn{}
			driverName := fmt.Sprintf("databricks-ingest-test-%d", parameterTestDriverCounter.Add(1))
			sql.Register(driverName, parameterCaptureDriver{conn: capture})
			database, err := sql.Open(driverName, "")
			require.NoError(t, err)
			defer func() { require.NoError(t, database.Close()) }()
			sqlConn, err := database.Conn(context.Background())
			require.NoError(t, err)
			defer func() { require.NoError(t, sqlConn.Close()) }()

			statement := &statementImpl{conn: &connectionImpl{conn: sqlConn}}
			defer func() { require.NoError(t, statement.Close(context.Background())) }()
			require.NoError(t, statement.SetOption(context.Background(), adbc.OptionKeyIngestTargetTable, "dictionary_ingest"))
			require.NoError(t, statement.SetOption(context.Background(), adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate))
			require.NoError(t, statement.Bind(context.Background(), record))
			rows, err := statement.ExecuteUpdate(context.Background())
			require.NoError(t, err)
			require.EqualValues(t, len(test.want), rows)
			require.Nil(t, statement.boundStream)

			require.Len(t, capture.queries, 1+len(test.want))
			require.Equal(t, "CREATE TABLE `dictionary_ingest` (`value` "+test.sqlType+")", capture.queries[0])
			require.Empty(t, capture.calls[0])
			for row, want := range test.want {
				require.Equal(t, "INSERT INTO `dictionary_ingest` (`value`) VALUES ("+test.placeholder+")", capture.queries[row+1])
				require.Len(t, capture.calls[row+1], 1)
				require.Equal(t, want, capture.calls[row+1][0].Value)
			}
		})
	}
}
