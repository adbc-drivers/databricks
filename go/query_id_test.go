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
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	dbsqlctx "github.com/databricks/databricks-sql-go/driverctx"
	"github.com/stretchr/testify/require"
)

// queryIDConn is a driver connection whose queries report a query ID the way
// databricks-sql-go does (through the context callback) and return one result.
type queryIDConn struct {
	queryID string
	rows    *mockRows
}

func (c *queryIDConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *queryIDConn) Close() error                        { return nil }
func (c *queryIDConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c *queryIDConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	if c.queryID != "" {
		// NewContextWithQueryId calls the callback the driver put on the context,
		// which is how databricks-sql-go reports the ID; the returned context is not needed.
		dbsqlctx.NewContextWithQueryId(ctx, c.queryID)
	}
	return c.rows, nil
}

type queryIDConnector struct{ conn *queryIDConn }

func (c queryIDConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c queryIDConnector) Driver() driver.Driver                        { return stubDriver{} }

func schemaOnlyRows(t *testing.T, schema *arrow.Schema) *mockRows {
	t.Helper()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	require.NoError(t, w.Close())
	return &mockRows{iterator: &mockIPCStreamIterator{schema: buf.Bytes()}}
}

func TestExecuteQueryReportsQueryID(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}},
		&arrow.Metadata{})
	helper := driverbase.ErrorHelper{DriverName: "databricks"}

	run := func(t *testing.T, queryID string) array.RecordReader {
		conn := &queryIDConn{queryID: queryID, rows: schemaOnlyRows(t, schema)}
		db := sql.OpenDB(queryIDConnector{conn: conn})
		t.Cleanup(func() { _ = db.Close() })
		c, err := db.Conn(context.Background())
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })
		reader, err := executeQuery(context.Background(), c, "select 1", nil, helper)
		require.NoError(t, err)
		t.Cleanup(reader.Release)
		return reader
	}

	t.Run("query id is in the result schema metadata", func(t *testing.T) {
		reader := run(t, "01f0-aaaa-bbbb")
		got, ok := reader.Schema().Metadata().GetValue(MetadataKeyDatabricksQueryID)
		require.True(t, ok)
		require.Equal(t, "01f0-aaaa-bbbb", got)
		require.Equal(t, "id", reader.Schema().Field(0).Name)
	})

	t.Run("no query id reported leaves the schema alone", func(t *testing.T) {
		reader := run(t, "")
		_, ok := reader.Schema().Metadata().GetValue(MetadataKeyDatabricksQueryID)
		require.False(t, ok)
	})
}

func TestSchemaWithQueryIDKeepsOtherMetadata(t *testing.T) {
	meta := arrow.NewMetadata([]string{"k"}, []string{"v"})
	in := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32}}, &meta)

	out := schemaWithQueryID(in, "qid")

	require.Equal(t, map[string]string{"k": "v", MetadataKeyDatabricksQueryID: "qid"}, out.Metadata().ToMap())
	require.Equal(t, map[string]string{"k": "v"}, in.Metadata().ToMap(), "input schema is not modified")
}
