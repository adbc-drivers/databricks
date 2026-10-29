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
	"fmt"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/stretchr/testify/require"
)

// fakeExecutionError implements only the parts of DBExecutionError the inspector uses.
type fakeExecutionError struct {
	dbsqlerr.DBExecutionError
	sqlState string
	cause    error
}

func (e fakeExecutionError) Error() string    { return "fake execution error" }
func (e fakeExecutionError) Unwrap() error    { return e.cause }
func (e fakeExecutionError) SqlState() string { return e.sqlState }

func TestInspectErrorStatus(t *testing.T) {
	tests := []struct {
		name     string
		inspect  DatabricksErrorInspector
		sqlState string
		expected adbc.Status
	}{
		{"table or view not found", DatabricksErrorInspector{}, "42P01", adbc.StatusNotFound},
		{"schema or catalog not found", DatabricksErrorInspector{}, "42704", adbc.StatusNotFound},
		{"table already exists", DatabricksErrorInspector{}, "42P07", adbc.StatusAlreadyExists},
		{"unresolved column", DatabricksErrorInspector{}, "42703", adbc.StatusInvalidArgument},
		{"unresolved column on ingest", DatabricksErrorInspector{ingest: true}, "42703", adbc.StatusAlreadyExists},
		{"unresolved routine", DatabricksErrorInspector{}, "42883", adbc.StatusNotFound},
		{"unknown SQLSTATE keeps default", DatabricksErrorInspector{}, "22012", adbc.StatusInternal},
		{"no SQLSTATE keeps default", DatabricksErrorInspector{}, "", adbc.StatusInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.inspect.InspectError(fakeExecutionError{sqlState: tt.sqlState}, adbc.StatusInternal)
			require.Equal(t, tt.expected, got.Code)
			require.Equal(t, tt.sqlState, string(got.SqlState[:len(tt.sqlState)]))
			require.Equal(t, "fake execution error", got.Msg)
		})
	}
}

func TestInspectErrorWrappedSQLState(t *testing.T) {
	wrapped := fmt.Errorf("outer: %w", fakeExecutionError{cause: fakeExecutionError{sqlState: "42P01"}})
	got := DatabricksErrorInspector{}.InspectError(wrapped, adbc.StatusInternal)
	require.Equal(t, adbc.StatusNotFound, got.Code)
}
