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
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	dbsqlctx "github.com/databricks/databricks-sql-go/driverctx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireAdbcStatus(t *testing.T, err error, code adbc.Status) {
	t.Helper()
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, code, adbcErr.Code)
}

func TestDatabaseQueryTagOptions(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}

	require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"team", "data-platform"))
	require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"env", "prod"))
	assert.Equal(t, map[string]string{"team": "data-platform", "env": "prod"}, db.queryTags)

	v, err := db.GetOption(ctx, OptionQueryTagPrefix+"team")
	require.NoError(t, err)
	assert.Equal(t, "data-platform", v)

	_, err = db.GetOption(ctx, OptionQueryTagPrefix+"missing")
	requireAdbcStatus(t, err, adbc.StatusNotFound)

	// Setting a tag again replaces its value
	require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"env", "dev"))
	assert.Equal(t, "dev", db.queryTags["env"])
}

// The database and the statement share the rules: an invalid tag is rejected
// with INVALID_ARGUMENT and the tags already set stay as they were.
func TestQueryTagValidation(t *testing.T) {
	ctx := context.Background()
	invalid := map[string]struct{ name, value string }{
		"empty name":      {"", "v"},
		"blank name":      {" ", "v"},
		"comma in name":   {"a,b", "v"},
		"colon in name":   {"a:b", "v"},
		"empty value":     {"team", ""},
		"value too long":  {"team", strings.Repeat("x", maxQueryTagValueLength+1)},
		"escaping counts": {"team", strings.Repeat(",", 65)}, // 65 commas are 130 characters on the wire
	}

	t.Run("database", func(t *testing.T) {
		db := &databaseImpl{}
		require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"team", "data-platform"))
		for name, c := range invalid {
			t.Run(name, func(t *testing.T) {
				requireAdbcStatus(t, db.SetOption(ctx, OptionQueryTagPrefix+c.name, c.value), adbc.StatusInvalidArgument)
			})
		}
		assert.Equal(t, map[string]string{"team": "data-platform"}, db.queryTags)
	})

	t.Run("statement", func(t *testing.T) {
		stmt := &statementImpl{conn: &connectionImpl{}}
		require.NoError(t, stmt.SetOption(ctx, OptionQueryTagPrefix+"team", "data-platform"))
		for name, c := range invalid {
			t.Run(name, func(t *testing.T) {
				requireAdbcStatus(t, stmt.SetOption(ctx, OptionQueryTagPrefix+c.name, c.value), adbc.StatusInvalidArgument)
			})
		}
		assert.Equal(t, map[string]string{"team": "data-platform"}, stmt.queryTags)
	})
}

// The 128 limit counts characters, not bytes.
func TestQueryTagValueLengthCountsCharacters(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"team", strings.Repeat("é", maxQueryTagValueLength)))
	requireAdbcStatus(t, db.SetOption(ctx, OptionQueryTagPrefix+"team", strings.Repeat("é", maxQueryTagValueLength+1)), adbc.StatusInvalidArgument)
}

func TestDatabaseQueryTagLimit(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	for i := range maxQueryTags {
		require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+string(rune('a'+i)), "v"))
	}

	err := db.SetOption(ctx, OptionQueryTagPrefix+"one_too_many", "v")
	requireAdbcStatus(t, err, adbc.StatusInvalidArgument)
	assert.Len(t, db.queryTags, maxQueryTags)

	// Replacing an existing tag is still allowed at the limit
	require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"a", "changed"))
	assert.Equal(t, "changed", db.queryTags["a"])
}

func TestResolveConnectionOptionsQueryTags(t *testing.T) {
	ctx := context.Background()
	newDB := func() *databaseImpl {
		return &databaseImpl{
			serverHostname: "example.cloud.databricks.com",
			httpPath:       "/sql/1.0/warehouses/abc",
			accessToken:    "token",
		}
	}

	without, err := newDB().resolveConnectionOptions()
	require.NoError(t, err)

	db := newDB()
	require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"team", "data-platform"))
	with, err := db.resolveConnectionOptions()
	require.NoError(t, err)

	// One extra option: the session-level QUERY_TAGS.
	assert.Len(t, with, len(without)+1)
}

func TestStatementQueryTagOptions(t *testing.T) {
	ctx := context.Background()
	stmt := &statementImpl{
		conn: &connectionImpl{queryTags: map[string]string{"team": "data-platform"}},
	}

	require.NoError(t, stmt.SetOption(ctx, OptionQueryTagPrefix+"run_id", "42"))

	v, err := stmt.GetOption(ctx, OptionQueryTagPrefix+"run_id")
	require.NoError(t, err)
	assert.Equal(t, "42", v)

	// Falls back to the connection default
	v, err = stmt.GetOption(ctx, OptionQueryTagPrefix+"team")
	require.NoError(t, err)
	assert.Equal(t, "data-platform", v)

	_, err = stmt.GetOption(ctx, OptionQueryTagPrefix+"missing")
	requireAdbcStatus(t, err, adbc.StatusNotFound)
}

func TestStatementQueryTagLimitCountsConnectionDefaults(t *testing.T) {
	ctx := context.Background()
	defaults := map[string]string{}
	for i := range maxQueryTags - 1 {
		defaults[string(rune('a'+i))] = "v"
	}
	stmt := &statementImpl{conn: &connectionImpl{queryTags: defaults}}

	require.NoError(t, stmt.SetOption(ctx, OptionQueryTagPrefix+"last", "v"))
	err := stmt.SetOption(ctx, OptionQueryTagPrefix+"one_too_many", "v")
	requireAdbcStatus(t, err, adbc.StatusInvalidArgument)
	assert.Equal(t, map[string]string{"last": "v"}, stmt.queryTags)

	// Overriding a default does not add a tag
	require.NoError(t, stmt.SetOption(ctx, OptionQueryTagPrefix+"a", "changed"))
}

func TestEffectiveQueryTagsDoesNotMutateInputs(t *testing.T) {
	conn := &connectionImpl{queryTags: map[string]string{"team": "data-platform"}}
	stmt := &statementImpl{conn: conn, queryTags: map[string]string{"run_id": "42"}}

	tags := stmt.effectiveQueryTags()
	tags["team"] = "mutated"
	delete(tags, "run_id")

	assert.Equal(t, map[string]string{"team": "data-platform"}, conn.queryTags)
	assert.Equal(t, map[string]string{"run_id": "42"}, stmt.queryTags)
}

// tagCaptureConn stands in for a database/sql driver connection, because the
// real driver needs a warehouse: it records the query tags that reach the
// driver on the context of every call. Its driver is registered under a name
// made from parameterTestDriverCounter, shared with the parameter binding tests.
type tagCaptureConn struct {
	calls []tagCall
}

type tagCall struct {
	method string
	query  string
	tags   map[string]string
}

func (c *tagCaptureConn) record(method string, ctx context.Context, query string) {
	c.calls = append(c.calls, tagCall{method, query, dbsqlctx.QueryTagsFromContext(ctx)})
}

func (c *tagCaptureConn) Prepare(query string) (driver.Stmt, error) {
	return &tagCaptureStmt{conn: c, query: query}, nil
}
func (c *tagCaptureConn) Close() error              { return nil }
func (c *tagCaptureConn) Begin() (driver.Tx, error) { return nil, errors.New("not implemented") }
func (c *tagCaptureConn) CheckNamedValue(*driver.NamedValue) error {
	return nil
}

func (c *tagCaptureConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.record("exec", ctx, query)
	return driver.RowsAffected(1), nil
}

// QueryContext records the call, then fails: the capture is all these tests need.
func (c *tagCaptureConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.record("query", ctx, query)
	return nil, errors.New("capture only")
}

type tagCaptureStmt struct {
	conn  *tagCaptureConn
	query string
}

func (s *tagCaptureStmt) Close() error  { return nil }
func (s *tagCaptureStmt) NumInput() int { return -1 }
func (s *tagCaptureStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errors.New("ExecContext expected")
}
func (s *tagCaptureStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("not implemented")
}
func (s *tagCaptureStmt) ExecContext(ctx context.Context, _ []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, nil)
}

type tagCaptureDriver struct{ conn *tagCaptureConn }

func (d tagCaptureDriver) Open(string) (driver.Conn, error) { return d.conn, nil }

func newTagCaptureStatement(t *testing.T, defaults map[string]string) (*statementImpl, *tagCaptureConn) {
	t.Helper()
	capture := &tagCaptureConn{}
	name := fmt.Sprintf("databricks-query-tag-test-%d", parameterTestDriverCounter.Add(1))
	sql.Register(name, tagCaptureDriver{conn: capture})
	database, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = database.Close() })
	sqlConn, err := database.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlConn.Close() })
	return &statementImpl{conn: &connectionImpl{conn: sqlConn, queryTags: defaults}}, capture
}

func TestQueryTagsReachTheDriverContext(t *testing.T) {
	ctx := context.Background()
	defaults := map[string]string{"team": "data-platform", "component": "adbc"}
	expected := map[string]string{"team": "adhoc", "component": "adbc", "run_id": "42"}

	setTags := func(t *testing.T, stmt *statementImpl) {
		require.NoError(t, stmt.SetOption(ctx, OptionQueryTagPrefix+"team", "adhoc"))
		require.NoError(t, stmt.SetOption(ctx, OptionQueryTagPrefix+"run_id", "42"))
	}

	t.Run("ExecuteQuery", func(t *testing.T) {
		stmt, capture := newTagCaptureStatement(t, defaults)
		setTags(t, stmt)
		require.NoError(t, stmt.SetSqlQuery(ctx, "SELECT 1"))
		_, _, err := stmt.ExecuteQuery(ctx)
		require.Error(t, err) // the fake driver returns no rows
		require.Equal(t, []tagCall{{"query", "SELECT 1", expected}}, capture.calls)
	})

	t.Run("ExecuteUpdate", func(t *testing.T) {
		stmt, capture := newTagCaptureStatement(t, defaults)
		setTags(t, stmt)
		require.NoError(t, stmt.SetSqlQuery(ctx, "DELETE FROM t"))
		_, err := stmt.ExecuteUpdate(ctx)
		require.NoError(t, err)
		require.Equal(t, []tagCall{{"exec", "DELETE FROM t", expected}}, capture.calls)
	})

	t.Run("ExecuteUpdate prepared", func(t *testing.T) {
		stmt, capture := newTagCaptureStatement(t, defaults)
		setTags(t, stmt)
		require.NoError(t, stmt.SetSqlQuery(ctx, "DELETE FROM t"))
		require.NoError(t, stmt.Prepare(ctx))
		_, err := stmt.ExecuteUpdate(ctx)
		require.NoError(t, err)
		require.Equal(t, []tagCall{{"exec", "DELETE FROM t", expected}}, capture.calls)
	})

	t.Run("no tags leaves the context untouched", func(t *testing.T) {
		stmt, capture := newTagCaptureStatement(t, nil)
		require.NoError(t, stmt.SetSqlQuery(ctx, "DELETE FROM t"))
		_, err := stmt.ExecuteUpdate(ctx)
		require.NoError(t, err)
		require.Equal(t, []tagCall{{"exec", "DELETE FROM t", nil}}, capture.calls)
	})

	t.Run("bulk ingest", func(t *testing.T) {
		stmt, capture := newTagCaptureStatement(t, defaults)
		setTags(t, stmt)
		require.NoError(t, stmt.SetOption(ctx, adbc.OptionKeyIngestTargetTable, "target"))
		require.NoError(t, stmt.SetOption(ctx, adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend))
		schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
		stmt.boundStream = newInt32RecordReader(t, schema, []int32{1})
		_, err := stmt.ExecuteUpdate(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, capture.calls)
		for _, call := range capture.calls {
			assert.Equal(t, expected, call.tags, call.query)
		}
	})
}
