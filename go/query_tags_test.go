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

func TestParseQueryTags(t *testing.T) {
	eh := &databaseImpl{}
	cases := map[string]struct {
		raw  string
		want map[string]string
	}{
		"pairs":               {"team:data,env:prod", map[string]string{"team": "data", "env": "prod"}},
		"whitespace trimmed":  {" team : data , env:prod ", map[string]string{"team": "data", "env": "prod"}},
		"empty string":        {"", map[string]string{}},
		"empty pairs skipped": {",team:data,,", map[string]string{"team": "data"}},
		"bare key":            {"team", map[string]string{"team": ""}},
		"empty value":         {"team:", map[string]string{"team": ""}},
		"first colon splits":  {"url:http://x", map[string]string{"url": "http://x"}},
		"escaped comma":       {`note:a\,b,env:prod`, map[string]string{"note": "a,b", "env": "prod"}},
		"escaped colon":       {`note:a\:b`, map[string]string{"note": "a:b"}},
		"escaped backslash":   {`note:a\\b`, map[string]string{"note": `a\b`}},
		"trailing backslash":  {`note:a\`, map[string]string{"note": `a\`}},
		"repeated key last":   {"env:dev,env:prod", map[string]string{"env": "prod"}},
		"unicode value":       {"team:données", map[string]string{"team": "données"}},
		"dbt style reserved":  {"@@dbt_core_version:2.0", map[string]string{"@@dbt_core_version": "2.0"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := parseQueryTags(&eh.ErrorHelper, tc.raw)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestQueryTagsRoundTrip(t *testing.T) {
	eh := &databaseImpl{}
	tags := map[string]string{"b": `x\y`, "a": "1,2:3", "c": ""}
	raw := formatQueryTags(tags)
	assert.Equal(t, `a:1\,2\:3,b:x\\y,c:`, raw)
	got, err := parseQueryTags(&eh.ErrorHelper, raw)
	require.NoError(t, err)
	assert.Equal(t, tags, got)
}

// The database and the statement share the rules: an invalid string is
// rejected with INVALID_ARGUMENT and the tags already set stay as they were.
func TestQueryTagValidation(t *testing.T) {
	ctx := context.Background()
	long := strings.Repeat("a", maxQueryTagLength+1)
	invalid := map[string]string{
		"value without key": ":v",
		"key with dash":     "my-key:v",
		"key with dot":      "my.key:v",
		"key with slash":    "my/key:v",
		"key with equals":   "my=key:v",
		"key too long":      long + ":v",
		"value too long":    "k:" + long,
	}
	setters := map[string]func() (func(string) error, func() map[string]string){
		"database": func() (func(string) error, func() map[string]string) {
			db := &databaseImpl{}
			require.NoError(t, db.SetOption(ctx, OptionQueryTags, "team:data"))
			return func(v string) error { return db.SetOption(ctx, OptionQueryTags, v) },
				func() map[string]string { return db.queryTags }
		},
		"statement": func() (func(string) error, func() map[string]string) {
			stmt := &statementImpl{}
			require.NoError(t, stmt.SetOption(ctx, OptionQueryTags, "team:data"))
			return func(v string) error { return stmt.SetOption(ctx, OptionQueryTags, v) },
				func() map[string]string { return stmt.queryTags }
		},
	}
	for level, setup := range setters {
		for name, raw := range invalid {
			t.Run(level+"/"+name, func(t *testing.T) {
				set, current := setup()
				requireAdbcStatus(t, set(raw), adbc.StatusInvalidArgument)
				assert.Equal(t, map[string]string{"team": "data"}, current())
			})
		}
	}
}

func TestQueryTagLengthCountsCharacters(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	// 128 two-byte characters, in the key and in the value.
	exact := strings.Repeat("é", maxQueryTagLength)
	require.NoError(t, db.SetOption(ctx, OptionQueryTags, exact+":"+exact))
	// Backslash, comma and colon in a value count once, as typed.
	require.NoError(t, db.SetOption(ctx, OptionQueryTags, "k:"+strings.Repeat(`\,`, maxQueryTagLength/2)))
}

func TestDatabaseQueryTagLimit(t *testing.T) {
	ctx := context.Background()
	tags := func(n int) string {
		pairs := make([]string, n)
		for i := range pairs {
			pairs[i] = fmt.Sprintf("k%d:v", i)
		}
		return strings.Join(pairs, ",")
	}
	db := &databaseImpl{}
	require.NoError(t, db.SetOption(ctx, OptionQueryTags, tags(maxQueryTags)))
	requireAdbcStatus(t, db.SetOption(ctx, OptionQueryTags, tags(maxQueryTags+1)), adbc.StatusInvalidArgument)
	assert.Len(t, db.queryTags, maxQueryTags)
}

// The string replaces the tags set before it at the same level.
func TestDatabaseQueryTagOptions(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}

	_, err := db.GetOption(ctx, OptionQueryTags)
	requireAdbcStatus(t, err, adbc.StatusNotFound)

	require.NoError(t, db.SetOption(ctx, OptionQueryTags, "team:data-platform,env:prod"))
	assert.Equal(t, map[string]string{"team": "data-platform", "env": "prod"}, db.queryTags)
	v, err := db.GetOption(ctx, OptionQueryTags)
	require.NoError(t, err)
	assert.Equal(t, "env:prod,team:data-platform", v)

	require.NoError(t, db.SetOption(ctx, OptionQueryTags, "env:dev"))
	assert.Equal(t, map[string]string{"env": "dev"}, db.queryTags)

	require.NoError(t, db.SetOption(ctx, OptionQueryTags, ""))
	_, err = db.GetOption(ctx, OptionQueryTags)
	requireAdbcStatus(t, err, adbc.StatusNotFound)
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
	require.NoError(t, db.SetOption(ctx, OptionQueryTags, "team:data-platform"))
	with, err := db.resolveConnectionOptions()
	require.NoError(t, err)

	// One extra option: the session-level QUERY_TAGS.
	assert.Len(t, with, len(without)+1)
}

// A statement's tags are merged with the connection defaults by key: the
// statement wins, the other defaults stay, and the statement's own string
// replaces what it set before.
func TestStatementQueryTagsMergeWithDefaults(t *testing.T) {
	ctx := context.Background()
	defaults := map[string]string{"team": "data", "env": "prod"}
	stmt := &statementImpl{conn: &connectionImpl{queryTags: defaults}}

	_, err := (&statementImpl{}).GetOption(ctx, OptionQueryTags)
	requireAdbcStatus(t, err, adbc.StatusNotFound)

	require.NoError(t, stmt.SetOption(ctx, OptionQueryTags, "env:dev,model:orders"))
	assert.Equal(t, map[string]string{"team": "data", "env": "dev", "model": "orders"}, stmt.effectiveQueryTags())
	v, err := stmt.GetOption(ctx, OptionQueryTags)
	require.NoError(t, err)
	assert.Equal(t, "env:dev,model:orders,team:data", v)

	require.NoError(t, stmt.SetOption(ctx, OptionQueryTags, "model:customers"))
	assert.Equal(t, map[string]string{"team": "data", "env": "prod", "model": "customers"}, stmt.effectiveQueryTags())

	// The inputs are never changed.
	assert.Equal(t, map[string]string{"team": "data", "env": "prod"}, defaults)
}

func TestStatementQueryTagLimitCountsConnectionDefaults(t *testing.T) {
	ctx := context.Background()
	defaults := make(map[string]string)
	for i := range maxQueryTags - 1 {
		defaults[fmt.Sprintf("d%d", i)] = "v"
	}
	stmt := &statementImpl{conn: &connectionImpl{queryTags: defaults}}

	require.NoError(t, stmt.SetOption(ctx, OptionQueryTags, "d0:override,extra:v"))
	requireAdbcStatus(t, stmt.SetOption(ctx, OptionQueryTags, "extra1:v,extra2:v"), adbc.StatusInvalidArgument)
	assert.Equal(t, map[string]string{"d0": "override", "extra": "v"}, stmt.queryTags)
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
		require.NoError(t, stmt.SetOption(ctx, OptionQueryTags, "team:adhoc,run_id:42"))
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
