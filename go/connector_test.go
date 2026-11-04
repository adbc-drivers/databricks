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
	"reflect"
	"testing"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/stretchr/testify/require"
)

// connectorConfig builds the databricks-sql-go connector from opts and returns
// its configuration. The configuration is private to databricks-sql-go, so it
// is read with reflection; this fails with a clear message when a
// databricks-sql-go upgrade renames the field.
func connectorConfig(t *testing.T, opts []dbsql.ConnOption) reflect.Value {
	t.Helper()
	conn, err := dbsql.NewConnector(opts...)
	require.NoError(t, err)
	v := reflect.ValueOf(conn)
	require.Equal(t, reflect.Ptr, v.Kind(), "databricks-sql-go connector is no longer a pointer: update connectorConfig")
	cfg := v.Elem().FieldByName("cfg")
	require.True(t, cfg.IsValid() && cfg.Kind() == reflect.Ptr && !cfg.IsNil(),
		"databricks-sql-go connector has no cfg field: update connectorConfig in connector_test.go")
	return cfg.Elem()
}

func connectorConfigField(t *testing.T, opts []dbsql.ConnOption, name string) reflect.Value {
	t.Helper()
	f := connectorConfig(t, opts).FieldByName(name)
	require.True(t, f.IsValid(), "databricks-sql-go config has no %s field: update connector_test.go", name)
	return f
}

func TestUserAgentOptionReachesConnector(t *testing.T) {
	ctx := context.Background()
	db := newSessionParamDatabase()
	opts, err := db.resolveConnectionOptions()
	require.NoError(t, err)
	require.Equal(t, "", connectorConfigField(t, opts, "UserAgentEntry").String())

	require.NoError(t, db.SetOption(ctx, OptionUserAgent, "dbt"))
	got, err := db.GetOption(ctx, OptionUserAgent)
	require.NoError(t, err)
	require.Equal(t, "dbt", got)
	opts, err = db.resolveConnectionOptions()
	require.NoError(t, err)
	require.Equal(t, "dbt", connectorConfigField(t, opts, "UserAgentEntry").String())

	require.NoError(t, db.SetOption(ctx, OptionUserAgent, ""))
	opts, err = db.resolveConnectionOptions()
	require.NoError(t, err)
	require.Equal(t, "", connectorConfigField(t, opts, "UserAgentEntry").String())
}
