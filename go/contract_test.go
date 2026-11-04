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
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	dbsql "github.com/databricks/databricks-sql-go"
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
// raw QUERY_TAGS session parameter, whatever its case; the other session
// parameters are kept and the database's own maps are not changed.
func TestQueryTagsLayerOverSessionParams(t *testing.T) {
	ctx := context.Background()
	db := newSessionParamDatabase()
	require.NoError(t, db.SetOption(ctx, OptionSessionParamPrefix+sessionParamQueryTags, "raw:1"))
	require.NoError(t, db.SetOption(ctx, OptionSessionParamPrefix+"query_tags", "lower:1"))
	require.NoError(t, db.SetOption(ctx, OptionSessionParamPrefix+"ansi_mode", "false"))

	t.Run("without query tag options the raw parameters are sent", func(t *testing.T) {
		opts, err := db.resolveConnectionOptions()
		require.NoError(t, err)
		got, ok := connectorSessionParam(t, opts, sessionParamQueryTags)
		require.True(t, ok)
		require.Equal(t, "raw:1", got)
	})

	t.Run("with query tag options they replace the raw ones", func(t *testing.T) {
		require.NoError(t, db.SetOption(ctx, OptionQueryTagPrefix+"team", "data-platform"))
		opts, err := db.resolveConnectionOptions()
		require.NoError(t, err)
		got, ok := connectorSessionParam(t, opts, sessionParamQueryTags)
		require.True(t, ok)
		require.Equal(t, "team:data-platform", got)
		_, ok = connectorSessionParam(t, opts, "query_tags")
		require.False(t, ok, "a raw query_tags must not be sent next to the query tag options")
		got, ok = connectorSessionParam(t, opts, "ansi_mode")
		require.True(t, ok)
		require.Equal(t, "false", got)
		require.Equal(t, map[string]string{sessionParamQueryTags: "raw:1", "query_tags": "lower:1", "ansi_mode": "false"}, db.sessionParams)
	})
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
