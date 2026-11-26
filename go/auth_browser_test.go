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
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
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

	// Setting the timeout to its default is no change.
	require.NoError(t, db.SetOption(ctx, OptionExternalBrowserTimeout, DefaultBrowserLoginTimeout.String()))
	require.Same(t, first, db.newBrowserAuthenticator())

	// An option it does use starts a new login.
	require.NoError(t, db.SetOption(ctx, OptionExternalBrowserTimeout, "2m"))
	require.NotSame(t, first, db.newBrowserAuthenticator())

	// So does the client ID.
	second := db.newBrowserAuthenticator()
	require.NoError(t, db.SetOption(ctx, OptionOAuthClientID, "my-app"))
	require.NotSame(t, second, db.newBrowserAuthenticator())
}

// The client ID and scopes options reach the login configuration.
func TestBrowserOptionsReachTheLoginConfig(t *testing.T) {
	db := &databaseImpl{oauthClientID: "my-app", oauthScopes: []string{"sql", "offline_access"}}
	a := db.newBrowserAuthenticator().(*browserAuthenticator)
	require.Equal(t, "my-app", a.cfg.clientID)
	require.Equal(t, []string{"sql", "offline_access"}, a.cfg.scopes)
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

// A browser launcher that never returns does not hold up the login: it still ends
// when the caller gives up.
func TestBrowserLauncherThatNeverReturnsDoesNotBlockLogin(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	block := make(chan struct{})
	defer close(block)
	a := newBrowserAuthenticator(browserLoginConfig{
		host:        "x.cloud.databricks.com",
		timeout:     time.Minute,
		redirectURL: addr,
		out:         io.Discard,
		openBrowser: func(string) error { <-block; return nil },
	})
	conf := oauth2.Config{
		ClientID:    "c",
		RedirectURL: "http://" + addr,
		Endpoint:    oauth2.Endpoint{AuthURL: "https://x.cloud.databricks.com/authorize", TokenURL: "https://x.cloud.databricks.com/token"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { _, err := a.runBrowserFlow(ctx, conf); done <- err }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("the login waited for the browser launcher")
	}
}

// The login prompt goes to stderr by default, so it cannot mix with query
// results written to stdout.
func TestLoginPromptDefaultsToStderr(t *testing.T) {
	a := newBrowserAuthenticator(browserLoginConfig{host: "x.cloud.databricks.com"})
	require.Same(t, os.Stderr, a.cfg.out)
}

// freeLoopbackAddr returns a loopback address that is free right now.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// A request waiting behind another request's sign-in stops waiting when its own
// context ends.
func TestWaitingBehindALoginStopsWhenTheRequestIsCancelled(t *testing.T) {
	addr := freeLoopbackAddr(t)
	signIn := make(chan struct{}) // never closed: nobody signs in
	a := newBrowserAuthenticator(browserLoginConfig{
		host:    "x.cloud.databricks.com",
		timeout: time.Minute,
		out:     io.Discard,
		resolveConfig: func(context.Context, browserLoginConfig) (oauth2.Config, error) {
			return oauth2.Config{ClientID: "c", RedirectURL: "http://" + addr, Endpoint: oauth2.Endpoint{AuthURL: "https://x.cloud.databricks.com/authorize", TokenURL: "https://x.cloud.databricks.com/token"}}, nil
		},
		openBrowser: func(string) error { <-signIn; return nil },
	})

	// The first request starts a sign-in that waits for the user.
	firstCtx, stopFirst := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(firstCtx, http.MethodGet, "http://example.invalid", nil)
		first <- a.Authenticate(req)
	}()
	require.Eventually(t, func() bool { return len(a.lock) == 1 }, 5*time.Second, 10*time.Millisecond)

	// The second request gives up after 100ms, not after the login timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)
	start := time.Now()
	err = a.Authenticate(req)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 5*time.Second)

	stopFirst()
	require.Error(t, <-first)
}

// A callback is handled while the browser launcher is still running. The
// callback here carries an OAuth error, so no token endpoint is needed: the login
// ends with that error.
func TestCallbackIsHandledWhileTheLauncherIsHeldOpen(t *testing.T) {
	addr := freeLoopbackAddr(t)
	hold := make(chan struct{})
	defer close(hold)
	a := newBrowserAuthenticator(browserLoginConfig{
		host:    "x.cloud.databricks.com",
		timeout: time.Minute,
		out:     io.Discard,
		openBrowser: func(loginURL string) error {
			u, err := url.Parse(loginURL)
			if err != nil {
				return err
			}
			// What the browser does after the user signs in: a request to the redirect.
			go func() {
				resp, err := http.Get("http://" + addr + "/?error=access_denied&error_description=no&state=" + url.QueryEscape(u.Query().Get("state")))
				if err == nil {
					_ = resp.Body.Close()
				}
			}()
			<-hold // the launcher itself never returns
			return nil
		},
	})
	conf := oauth2.Config{ClientID: "c", RedirectURL: "http://" + addr, Endpoint: oauth2.Endpoint{AuthURL: "https://x.cloud.databricks.com/authorize", TokenURL: "https://x.cloud.databricks.com/token"}}

	done := make(chan error, 1)
	go func() { _, err := a.runBrowserFlow(context.Background(), conf); done <- err }()
	select {
	case err := <-done:
		require.ErrorContains(t, err, "access_denied")
	case <-time.After(10 * time.Second):
		t.Fatal("the login did not see the callback while the launcher was running")
	}
}

// The client ID and scopes reach the authorization request, with the scopes the
// cloud requires added. An Azure host needs no network lookup: its endpoints
// follow from the host name.
func TestDefaultResolveConfigUsesTheClientAndScopes(t *testing.T) {
	cfg := browserLoginConfig{host: "adb-1.2.azuredatabricks.net", clientID: "my-app", scopes: []string{"custom-scope"}}
	conf, err := defaultResolveConfig(context.Background(), cfg)
	require.NoError(t, err)
	require.Equal(t, "my-app", conf.ClientID)
	require.Equal(t, "http://localhost:8020", conf.RedirectURL)

	u, err := url.Parse(conf.AuthCodeURL("state"))
	require.NoError(t, err)
	require.Equal(t, "my-app", u.Query().Get("client_id"))
	scope := u.Query().Get("scope")
	require.Contains(t, scope, "custom-scope")
	require.Contains(t, scope, "offline_access")
	require.Contains(t, scope, "/user_impersonation")
}

func TestOAuthRedirectURLOption(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	for _, ok := range []string{"localhost:9000", "http://localhost:9000/callback", "127.0.0.1:8020", "http://[::1]:8020"} {
		require.NoError(t, db.SetOption(ctx, OptionOAuthRedirectURL, ok), ok)
		got, err := db.GetOption(ctx, OptionOAuthRedirectURL)
		require.NoError(t, err)
		require.Equal(t, ok, got)
	}

	require.NoError(t, db.SetOption(ctx, OptionOAuthRedirectURL, "localhost:9000"))
	for _, bad := range []string{"https://localhost:9000", "example.com:9000", "localhost", "localhost:0", "localhost:70000", "http://localhost:abc", "::1:8020", "http://localhost:9000/{", "http://localhost:9000/%7B", "http://localhost:9000/a%20b", "http://localhost:9000/a%09b", "http://localhost:9000/cb?state=x", "http://localhost:9000/cb?", "http://localhost:9000/cb#done", "http://localhost:9000/cb#", "http://localhost:9000/%7B$%7D", "http://localhost:9000/{name}", "http://localhost:9000/{$}", "http://user:pass@localhost:9000/cb", "http://localhost:9000/a%2Fb"} {
		err := db.SetOption(ctx, OptionOAuthRedirectURL, bad)
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr, bad)
		require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, bad)
		got, _ := db.GetOption(ctx, OptionOAuthRedirectURL)
		require.Equal(t, "localhost:9000", got, "a bad value must not change the setting")
	}

	// An empty value restores the default.
	require.NoError(t, db.SetOption(ctx, OptionOAuthRedirectURL, ""))
	a := db.newBrowserAuthenticator().(*browserAuthenticator)
	require.Equal(t, "", a.cfg.redirectURL)
}

func TestRedirectURLReachesTheLoginConfig(t *testing.T) {
	db := &databaseImpl{oauthRedirectURL: "localhost:9000"}
	a := db.newBrowserAuthenticator().(*browserAuthenticator)
	require.Equal(t, "localhost:9000", a.cfg.redirectURL)
}

// A rejected redirect URL is not repeated in the error: it can carry credentials.
func TestRedirectURLErrorDoesNotEchoTheValue(t *testing.T) {
	err := validateBrowserRedirectURL("http://user:secret-password@localhost:9000/cb")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret-password")
}

// The redirect URL reaches the authorization request as it was given, as a bare
// address or as a full URL with a path. An Azure host needs no network lookup.
func TestRedirectURLReachesTheAuthorizationRequest(t *testing.T) {
	for given, want := range map[string]string{
		"localhost:9000":                 "http://localhost:9000",
		"http://localhost:9000/callback": "http://localhost:9000/callback",
		"":                               "http://localhost:8020",
	} {
		conf, err := defaultResolveConfig(context.Background(), browserLoginConfig{host: "adb-1.2.azuredatabricks.net", redirectURL: given})
		require.NoError(t, err, given)
		require.Equal(t, want, conf.RedirectURL, given)
	}
}
