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
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// connectorSessionParam reads one session parameter from the connector built
// from opts.
func connectorSessionParam(t *testing.T, opts []dbsql.ConnOption, name string) (string, bool) {
	t.Helper()
	params := connectorConfigField(t, opts, "SessionParams")
	v := params.MapIndex(reflect.ValueOf(name))
	if !v.IsValid() {
		return "", false
	}
	return v.String(), true
}

// The tags set on the database reach the session as QUERY_TAGS and win over a
// raw QUERY_TAGS session parameter; the other session parameters are kept and
// the database's own maps are not changed.
func TestQueryTagsLayerOverSessionParams(t *testing.T) {
	ctx := context.Background()
	db := newSessionParamDatabase()
	require.NoError(t, db.SetOption(ctx, OptionSessionParamPrefix+"QUERY_TAGS", "raw:1"))
	require.NoError(t, db.SetOption(ctx, OptionSessionParamPrefix+"ansi_mode", "false"))

	opts, err := db.resolveConnectionOptions()
	require.NoError(t, err)
	got, ok := connectorSessionParam(t, opts, "QUERY_TAGS")
	require.True(t, ok)
	require.Equal(t, "raw:1", got, "without query tag options the raw session parameter is sent")

	require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"team", "data-platform"))
	require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"@@dbt_core_version", "1.2.3"))
	opts, err = db.resolveConnectionOptions()
	require.NoError(t, err)
	got, ok = connectorSessionParam(t, opts, "QUERY_TAGS")
	require.True(t, ok)
	// The order of the tags in the serialised value is not fixed.
	require.ElementsMatch(t, []string{"team:data-platform", "@@dbt_core_version:1.2.3"}, strings.Split(got, ","))
	got, ok = connectorSessionParam(t, opts, "ansi_mode")
	require.True(t, ok)
	require.Equal(t, "false", got)

	require.Equal(t, map[string]string{"QUERY_TAGS": "raw:1", "ansi_mode": "false"}, db.sessionParams)

	// A raw query_tags in another case is dropped too, so only one
	// QUERY_TAGS-like parameter is sent. The stored parameters are not changed.
	require.NoError(t, db.SetOption(ctx, OptionSessionParamPrefix+"query_tags", "lower:1"))
	opts, err = db.resolveConnectionOptions()
	require.NoError(t, err)
	_, ok = connectorSessionParam(t, opts, "query_tags")
	require.False(t, ok, "query_tags must not be sent next to the query tag options")
	got, ok = connectorSessionParam(t, opts, "QUERY_TAGS")
	require.True(t, ok)
	require.ElementsMatch(t, []string{"team:data-platform", "@@dbt_core_version:1.2.3"}, strings.Split(got, ","))
	require.Equal(t, "lower:1", db.sessionParams["query_tags"])

	// Without query tag options the raw parameter is sent as given.
	savedTags := db.queryTags
	db.queryTags = nil
	opts, err = db.resolveConnectionOptions()
	require.NoError(t, err)
	got, ok = connectorSessionParam(t, opts, "query_tags")
	require.True(t, ok)
	require.Equal(t, "lower:1", got)
	db.queryTags = savedTags
	require.Equal(t, map[string]string{"team": "data-platform", "@@dbt_core_version": "1.2.3"}, db.queryTags)
}

// openTestConnection opens a connection on a database whose pool is the fake
// driver of the query tag tests.
func openTestConnection(t *testing.T, db *databaseImpl) (adbc.ConnectionWithContext, *tagCaptureConn) {
	t.Helper()
	capture := &tagCaptureConn{}
	name := fmt.Sprintf("databricks-contract-test-%d", parameterTestDriverCounter.Add(1))
	sql.Register(name, tagCaptureDriver{conn: capture})
	db.newPool = func(context.Context) (*sql.DB, error) { return sql.Open(name, "") }
	db.needsRefresh = true
	conn, err := db.Open(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn, capture
}

// Query tags are layered database default, connection, statement. The
// connection takes a copy of the database defaults when it opens.
func TestQueryTagLayersAcrossDatabaseConnectionAndStatement(t *testing.T) {
	ctx := context.Background()
	db := newSessionParamDatabase()
	require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"team", "data-platform"))
	require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"env", "prod"))

	conn, capture := openTestConnection(t, db)

	// A default set on the database after the connection opened does not reach it.
	require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"env", "dev"))
	require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"late", "x"))

	stmt, err := conn.NewStatement(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stmt.Close(ctx) })
	require.NoError(t, stmt.SetSqlQuery(ctx, "DELETE FROM t"))

	_, err = stmt.ExecuteUpdate(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"team": "data-platform", "env": "prod"}, capture.calls[0].tags)

	// A statement tag overrides a default for that statement only.
	require.NoError(t, stmt.(adbc.GetSetOptionsWithContext).SetOption(ctx, OptionQueryTagPrefix+"env", "staging"))
	_, err = stmt.ExecuteUpdate(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"team": "data-platform", "env": "staging"}, capture.calls[1].tags)

	other, err := conn.NewStatement(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Close(ctx) })
	require.NoError(t, other.SetSqlQuery(ctx, "DELETE FROM t"))
	_, err = other.ExecuteUpdate(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"team": "data-platform", "env": "prod"}, capture.calls[2].tags)
}

// An invalid tag on a statement is rejected and leaves both the statement's
// and the connection's tags, and what is sent, unchanged.
func TestInvalidStatementQueryTagLeavesLayersUnchanged(t *testing.T) {
	ctx := context.Background()
	db := newSessionParamDatabase()
	require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"team", "data-platform"))
	conn, capture := openTestConnection(t, db)

	stmt, err := conn.NewStatement(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stmt.Close(ctx) })
	setter := stmt.(adbc.GetSetOptionsWithContext)
	require.NoError(t, setter.SetOption(ctx, OptionQueryTagPrefix+"run_id", "42"))

	for _, tc := range []struct{ key, value string }{
		{OptionQueryTagPrefix, "v"},
		{OptionQueryTagPrefix + "a,b", "v"},
		{OptionQueryTagPrefix + "run_id", ""},
		{OptionQueryTagPrefix + "team", ""},
	} {
		requireAdbcStatus(t, setter.SetOption(ctx, tc.key, tc.value), adbc.StatusInvalidArgument)
	}

	require.NoError(t, stmt.SetSqlQuery(ctx, "DELETE FROM t"))
	_, err = stmt.ExecuteUpdate(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "data-platform", "run_id": "42"}, capture.calls[0].tags)
}

// The transaction contract: Databricks statements are committed as they run, so
// the driver supports autocommit only. Turning autocommit off is refused with
// NOT_IMPLEMENTED. Commit and Rollback on an open connection are refused with
// INVALID_STATE: the connection is in autocommit mode, which the driver base
// checks before it reaches the driver's own (NOT_IMPLEMENTED) methods.
func TestTransactionContract(t *testing.T) {
	ctx := context.Background()
	conn, _ := openTestConnection(t, newSessionParamDatabase())

	tx, ok := conn.(adbc.GetSetOptionsWithContext)
	require.True(t, ok)

	require.NoError(t, tx.SetOption(ctx, adbc.OptionKeyAutoCommit, adbc.OptionValueEnabled))
	requireAdbcStatus(t, tx.SetOption(ctx, adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled), adbc.StatusNotImplemented)
	requireAdbcStatus(t, conn.Commit(ctx), adbc.StatusInvalidState)
	requireAdbcStatus(t, conn.Rollback(ctx), adbc.StatusInvalidState)

	// The refused request left autocommit on.
	got, err := tx.GetOption(ctx, adbc.OptionKeyAutoCommit)
	require.NoError(t, err)
	require.Equal(t, adbc.OptionValueEnabled, got)
}
