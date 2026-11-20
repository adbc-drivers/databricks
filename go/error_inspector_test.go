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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// testExecutionError implements dbsqlerr.DBExecutionError. The real error type
// is private in databricks-sql-go, so it cannot be constructed here.
type testExecutionError struct {
	sqlState string
	cause    error
}

var _ dbsqlerr.DBExecutionError = testExecutionError{}

func (e testExecutionError) Error() string                    { return "test execution error" }
func (e testExecutionError) CorrelationId() string            { return "" }
func (e testExecutionError) ConnectionId() string             { return "" }
func (e testExecutionError) StackTrace() pkgerrors.StackTrace { return nil }
func (e testExecutionError) Cause() error                     { return e.cause }
func (e testExecutionError) Unwrap() error                    { return e.cause }
func (e testExecutionError) IsRetryable() bool                { return false }
func (e testExecutionError) RetryAfter() time.Duration        { return 0 }
func (e testExecutionError) QueryId() string                  { return "" }
func (e testExecutionError) SqlState() string                 { return e.sqlState }

func TestInspectErrorMappedSQLState(t *testing.T) {
	got := DatabricksErrorInspector{}.InspectError(testExecutionError{sqlState: "42P01"}, adbc.StatusInternal)
	require.Equal(t, adbc.StatusNotFound, got.Code)
	require.Equal(t, [5]byte{'4', '2', 'P', '0', '1'}, got.SqlState)
	require.Equal(t, "test execution error", got.Msg)
}

func TestInspectErrorUnresolvedColumn(t *testing.T) {
	err := testExecutionError{sqlState: "42703"}
	require.Equal(t, adbc.StatusInvalidArgument,
		DatabricksErrorInspector{}.InspectError(err, adbc.StatusInternal).Code)
	require.Equal(t, adbc.StatusAlreadyExists,
		DatabricksErrorInspector{ingest: true}.InspectError(err, adbc.StatusInternal).Code)
}

func TestInspectErrorUnmappedSQLStateKeepsDefault(t *testing.T) {
	got := DatabricksErrorInspector{}.InspectError(testExecutionError{sqlState: "22012"}, adbc.StatusInternal)
	require.Equal(t, adbc.StatusInternal, got.Code)
	require.Equal(t, [5]byte{'2', '2', '0', '1', '2'}, got.SqlState)
}

func TestInspectErrorNoSQLStateKeepsDefault(t *testing.T) {
	got := DatabricksErrorInspector{}.InspectError(testExecutionError{}, adbc.StatusInternal)
	require.Equal(t, adbc.StatusInternal, got.Code)
	require.Equal(t, [5]byte{}, got.SqlState)

	got = DatabricksErrorInspector{}.InspectError(errors.New("plain"), adbc.StatusInternal)
	require.Equal(t, adbc.StatusInternal, got.Code)
}

func TestInspectErrorWrapperWithoutSQLState(t *testing.T) {
	wrapped := fmt.Errorf("outer: %w", testExecutionError{cause: testExecutionError{sqlState: "42P01"}})
	got := DatabricksErrorInspector{}.InspectError(wrapped, adbc.StatusInternal)
	require.Equal(t, adbc.StatusNotFound, got.Code)
	require.Equal(t, [5]byte{'4', '2', 'P', '0', '1'}, got.SqlState)
}
