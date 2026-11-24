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
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
)

func TestBrowserBuiltInClient(t *testing.T) {
	id, err := resolveBrowserClientID("x.cloud.databricks.com", "")
	require.NoError(t, err)
	require.Equal(t, awsGCPBrowserClientID, id)

	id, err = resolveBrowserClientID("adb-1.2.azuredatabricks.net", "")
	require.NoError(t, err)
	require.Equal(t, azureBrowserClientID, id)

	_, err = resolveBrowserClientID("localhost", "")
	require.ErrorContains(t, err, "unhandled cloud type")

	id, err = resolveBrowserClientID("localhost", "mine")
	require.NoError(t, err)
	require.Equal(t, "mine", id)
}

func TestExternalBrowserLoginMethod(t *testing.T) {
	db := databaseImpl{authType: "external-browser"}
	name, _, err := db.resolveLoginMethod()
	require.NoError(t, err)
	require.Equal(t, "external-browser", name)

	// Browser login is never inferred, so existing configurations keep their method.
	db = databaseImpl{oauthClientID: "id"}
	_, _, err = db.resolveLoginMethod()
	require.ErrorContains(t, err, "client secret is required when using auth type 'oauth-m2m'")
}

func TestExternalBrowserTimeoutOption(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	require.NoError(t, db.SetOption(ctx, OptionExternalBrowserTimeout, "2m"))
	got, err := db.GetOption(ctx, OptionExternalBrowserTimeout)
	require.NoError(t, err)
	require.Equal(t, "2m0s", got)

	for _, bad := range []string{"soon", "-1m", "0s"} {
		err := db.SetOption(ctx, OptionExternalBrowserTimeout, bad)
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr, bad)
		require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, bad)
		got, _ = db.GetOption(ctx, OptionExternalBrowserTimeout)
		require.Equal(t, "2m0s", got, "a bad value must not change the setting")
	}

	require.NoError(t, db.SetOption(ctx, OptionExternalBrowserTimeout, ""))
	got, _ = db.GetOption(ctx, OptionExternalBrowserTimeout)
	require.Equal(t, "", got)
}

// isolateHome keeps a test away from the real home and cache directories.
func isolateHome(t *testing.T) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("LOCALAPPDATA", t.TempDir())
}

func TestExternalBrowserBuildsConnectorOption(t *testing.T) {
	isolateHome(t)
	db := &databaseImpl{
		serverHostname: "x.cloud.databricks.com",
		httpPath:       "/sql/1.0/warehouses/abc",
		authType:       "external-browser",
	}
	opts, err := db.resolveConnectionOptions()
	require.NoError(t, err)
	require.NotEmpty(t, opts)
}
