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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
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

func TestOAuthHostDropsSchemeAndPath(t *testing.T) {
	for host, want := range map[string]string{
		"x.cloud.databricks.com":                  "x.cloud.databricks.com",
		"https://x.cloud.databricks.com":          "x.cloud.databricks.com",
		"https://x.cloud.databricks.com:443/path": "x.cloud.databricks.com:443",
	} {
		db := &databaseImpl{serverHostname: host}
		require.Equal(t, want, db.oauthHost(), host)
	}
}

// The pool is rebuilt when an option changes, but the login has to outlive
// it, or every Open would open the browser again.
func TestBrowserLoginIsKeptWhileOptionsAreUnchanged(t *testing.T) {
	isolateHome(t)
	ctx := context.Background()
	db := &databaseImpl{serverHostname: "x.cloud.databricks.com"}
	first := db.newBrowserAuthenticator()
	require.Same(t, first, db.newBrowserAuthenticator())

	// An option the login does not use leaves it alone.
	require.NoError(t, db.SetOption(ctx, OptionCatalog, "main"))
	require.Same(t, first, db.newBrowserAuthenticator())

	// An option it does use starts a new login.
	require.NoError(t, db.SetOption(ctx, OptionExternalBrowserTimeout, "2m"))
	require.NotSame(t, first, db.newBrowserAuthenticator())
}

// Connections opened on several goroutines share one login.
func TestBrowserLoginIsSharedAcrossGoroutines(t *testing.T) {
	isolateHome(t)
	db := &databaseImpl{serverHostname: "x.cloud.databricks.com"}
	const n = 16
	got := make([]interface{}, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got[i] = db.newBrowserAuthenticator()
		}()
	}
	wg.Wait()
	for _, a := range got {
		require.Same(t, got[0], a)
	}
}

func TestOnlyOneBrowserFlowRunsAtATime(t *testing.T) {
	release, err := acquireBrowserFlow(context.Background())
	require.NoError(t, err)

	// A second login waits, and gives up when its context ends.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = acquireBrowserFlow(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	release()
	release, err = acquireBrowserFlow(context.Background())
	require.NoError(t, err)
	release()
}

func TestIsRejectedGrant(t *testing.T) {
	require.True(t, isRejectedGrant(&oauth2.RetrieveError{ErrorCode: "invalid_grant"}))
	require.False(t, isRejectedGrant(&oauth2.RetrieveError{ErrorCode: "server_error"}))
	require.False(t, isRejectedGrant(errors.New("connection reset")))
	require.False(t, isRejectedGrant(nil))
	// An expired token with no refresh token can only be replaced by a new login.
	require.True(t, isRejectedGrant(errors.New("oauth2: token expired and refresh token is not set")))
}

// The OAuth requests go to the port the SQL client uses.
func TestOAuthHostPortFollowsTheServerPort(t *testing.T) {
	for _, tt := range []struct {
		host string
		port int64
		want string
	}{
		{"x.cloud.databricks.com", 0, "x.cloud.databricks.com"},
		{"x.cloud.databricks.com", 443, "x.cloud.databricks.com"},
		{"x.cloud.databricks.com", 8443, "x.cloud.databricks.com:8443"},
		{"https://x.cloud.databricks.com", 8443, "x.cloud.databricks.com:8443"},
		{"x.cloud.databricks.com:9443", 8443, "x.cloud.databricks.com:9443"},
	} {
		db := &databaseImpl{serverHostname: tt.host, port: int(tt.port)}
		require.Equal(t, tt.want, db.oauthHostPort(), "%s:%d", tt.host, tt.port)
	}
}
