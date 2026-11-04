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

package databricks_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/adbc-drivers/databricks/go"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// TestErrorStatusFromServer runs failing queries against a live warehouse so the
// inspector sees the databricks-sql-go error type the server really produces.
func TestErrorStatusFromServer(t *testing.T) {
	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTPPATH")
	token := os.Getenv("DATABRICKS_TOKEN")
	if host == "" || httpPath == "" || token == "" {
		t.Skip("DATABRICKS_HOST, DATABRICKS_HTTPPATH or DATABRICKS_TOKEN not defined, skipping Databricks error status test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	db, err := databricks.NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{
		databricks.OptionServerHostname: host,
		databricks.OptionHTTPPath:       httpPath,
		databricks.OptionAccessToken:    token,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close(ctx)) }()
	cnxn, err := db.Open(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, cnxn.Close(ctx)) }()

	tests := []struct {
		name     string
		query    string
		status   adbc.Status
		sqlState string
	}{
		{"missing table", "SELECT * FROM adbc_no_such_catalog.no_schema.no_table", adbc.StatusNotFound, "42P01"},
		{"unresolved column", "SELECT nonexistent_col FROM (SELECT 1 AS a)", adbc.StatusInvalidArgument, "42703"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := cnxn.NewStatement(ctx)
			require.NoError(t, err)
			defer func() { require.NoError(t, stmt.Close(ctx)) }()
			require.NoError(t, stmt.SetSqlQuery(ctx, tt.query))

			reader, _, err := stmt.ExecuteQuery(ctx)
			if reader != nil {
				reader.Release()
			}
			require.Error(t, err)
			var adbcErr adbc.Error
			require.True(t, errors.As(err, &adbcErr), "not an adbc.Error: %v", err)
			require.Equal(t, tt.status, adbcErr.Code)
			require.Equal(t, tt.sqlState, string(adbcErr.SqlState[:]))
		})
	}
}
