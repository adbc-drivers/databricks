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
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

// fakeIdP is an offline OAuth identity provider: OIDC discovery, an authorize
// endpoint that redirects back to the caller, and a token endpoint for the
// authorization code and refresh token grants. It checks PKCE.
type fakeIdP struct {
	srv *httptest.Server

	mu                sync.Mutex
	expiresIn         int
	rejectRefresh     bool
	failRefreshStatus int               // when set, refresh requests fail with this HTTP status
	codes             map[string]string // code -> PKCE challenge
	authRequests      []url.Values
	grants            []string // grant_type of each token request
	issued            int
	// authorizeError, when set, is returned to the callback instead of a code.
	authorizeError string
	// stateOverride, when set, replaces the state sent back to the callback.
	stateOverride string
}

func newFakeIdP(t *testing.T) *fakeIdP {
	t.Helper()
	f := &fakeIdP{expiresIn: 3600, codes: map[string]string{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/oauth-authorization-server", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"authorization_endpoint": f.srv.URL + "/authorize",
			"token_endpoint":         f.srv.URL + "/token",
		})
	})
	mux.HandleFunc("/authorize", f.authorize)
	mux.HandleFunc("/token", f.token)
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeIdP) authorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.authRequests = append(f.authRequests, q)
	redirect, err := url.Parse(q.Get("redirect_uri"))
	if err != nil || q.Get("response_type") != "code" || q.Get("code_challenge_method") != "S256" {
		http.Error(w, "bad authorize request", http.StatusBadRequest)
		return
	}
	state := q.Get("state")
	if f.stateOverride != "" {
		state = f.stateOverride
	}
	rq := redirect.Query()
	rq.Set("state", state)
	if f.authorizeError != "" {
		rq.Set("error", f.authorizeError)
		rq.Set("error_description", "denied by user")
	} else {
		code := fmt.Sprintf("code-%d", len(f.authRequests))
		f.codes[code] = q.Get("code_challenge")
		rq.Set("code", code)
	}
	redirect.RawQuery = rq.Encode()
	http.Redirect(w, r, redirect.String(), http.StatusFound)
}

func (f *fakeIdP) token(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	grant := r.PostForm.Get("grant_type")
	f.grants = append(f.grants, grant)
	fail := func(code string) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
	}
	switch grant {
	case "authorization_code":
		challenge, ok := f.codes[r.PostForm.Get("code")]
		sum := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
		if !ok || base64.RawURLEncoding.EncodeToString(sum[:]) != challenge {
			fail("invalid_grant")
			return
		}
		delete(f.codes, r.PostForm.Get("code"))
	case "refresh_token":
		if f.failRefreshStatus != 0 {
			w.WriteHeader(f.failRefreshStatus)
			return
		}
		if f.rejectRefresh || !strings.HasPrefix(r.PostForm.Get("refresh_token"), "refresh-") {
			fail("invalid_grant")
			return
		}
	default:
		fail("unsupported_grant_type")
		return
	}
	f.issued++
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token":  fmt.Sprintf("access-%d", f.issued),
		"refresh_token": fmt.Sprintf("refresh-%d", f.issued),
		"token_type":    "Bearer",
		"expires_in":    f.expiresIn,
	})
}

func (f *fakeIdP) snapshot() (grants []string, auth []url.Values) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.grants...), append([]url.Values(nil), f.authRequests...)
}

// discover resolves the OAuth endpoints from the fake provider's discovery
// document, standing in for the OIDC lookup against a real workspace.
func (f *fakeIdP) discover(ctx context.Context, c browserLoginConfig) (oauth2.Config, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.srv.URL+"/.well-known/oauth-authorization-server", nil)
	if err != nil {
		return oauth2.Config{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return oauth2.Config{}, err
	}
	defer resp.Body.Close() //nolint:errcheck
	var doc struct {
		Auth  string `json:"authorization_endpoint"`
		Token string `json:"token_endpoint"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return oauth2.Config{}, err
	}
	redirect := c.redirectURL
	if redirect == "" {
		redirect = defaultBrowserRedirectURL
	}
	if !strings.HasPrefix(redirect, "http") {
		redirect = "http://" + redirect
	}
	clientID := c.clientID
	if clientID == "" {
		clientID = "built-in-client"
	}
	return oauth2.Config{
		ClientID:    clientID,
		Endpoint:    oauth2.Endpoint{AuthURL: doc.Auth, TokenURL: doc.Token, AuthStyle: oauth2.AuthStyleInParams},
		RedirectURL: redirect,
		Scopes:      append([]string{"offline_access", "sql"}, c.scopes...),
	}, nil
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close() //nolint:errcheck
	return l.Addr().String()
}

// browserHarness wires a browserLoginConfig to a fake IdP. The "browser" is a
// function that follows the login URL, which ends at the local callback.
type browserHarness struct {
	idp      *fakeIdP
	cfg      browserLoginConfig
	out      *strings.Builder
	mu       sync.Mutex
	opened   []string
	callback []int // status of the callback response per login
}

func newBrowserHarness(t *testing.T) *browserHarness {
	t.Helper()
	h := &browserHarness{idp: newFakeIdP(t), out: &strings.Builder{}}
	h.cfg = browserLoginConfig{
		host:          "fake.cloud.databricks.com",
		redirectURL:   freeAddr(t),
		timeout:       10 * time.Second,
		resolveConfig: h.idp.discover,
		out:           h.out,
	}
	h.cfg.openBrowser = func(loginURL string) error {
		h.mu.Lock()
		h.opened = append(h.opened, loginURL)
		h.mu.Unlock()
		go func() {
			resp, err := http.Get(loginURL) //nolint:gosec,noctx // test browser
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			h.mu.Lock()
			h.callback = append(h.callback, resp.StatusCode)
			h.mu.Unlock()
		}()
		return nil
	}
	return h
}

func (h *browserHarness) openCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.opened)
}

func authHeader(t *testing.T, a *browserAuthenticator) (string, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)
	err = a.Authenticate(req)
	return req.Header.Get("Authorization"), err
}

func TestBrowserLoginCompletesAndReusesToken(t *testing.T) {
	h := newBrowserHarness(t)
	a := newBrowserAuthenticator(h.cfg)

	hdr, err := authHeader(t, a)
	require.NoError(t, err)
	require.Equal(t, "Bearer access-1", hdr)

	// A second request reuses the token: no new browser, no new token request.
	hdr, err = authHeader(t, a)
	require.NoError(t, err)
	require.Equal(t, "Bearer access-1", hdr)
	require.Equal(t, 1, h.openCount())
	grants, auth := h.idp.snapshot()
	require.Equal(t, []string{"authorization_code"}, grants)

	require.Len(t, auth, 1)
	require.Equal(t, "built-in-client", auth[0].Get("client_id"))
	require.Equal(t, "S256", auth[0].Get("code_challenge_method"))
	require.Contains(t, h.out.String(), "Open URL in Browser to Continue")
	require.Equal(t, "http://"+h.cfg.redirectURL, auth[0].Get("redirect_uri"))
}

func TestBrowserLoginRefreshesExpiredToken(t *testing.T) {
	h := newBrowserHarness(t)
	h.idp.expiresIn = 1 // inside the oauth2 expiry margin, so it is always stale
	a := newBrowserAuthenticator(h.cfg)

	hdr, err := authHeader(t, a)
	require.NoError(t, err)
	require.Equal(t, "Bearer access-1", hdr)

	hdr, err = authHeader(t, a)
	require.NoError(t, err)
	require.Equal(t, "Bearer access-2", hdr)
	require.Equal(t, 1, h.openCount(), "refresh must not open the browser")
	grants, _ := h.idp.snapshot()
	require.Equal(t, []string{"authorization_code", "refresh_token"}, grants)
}

func TestBrowserLoginSignsInAgainWhenRefreshIsRejected(t *testing.T) {
	h := newBrowserHarness(t)
	h.idp.expiresIn = 1
	a := newBrowserAuthenticator(h.cfg)

	_, err := authHeader(t, a)
	require.NoError(t, err)
	h.idp.mu.Lock()
	h.idp.rejectRefresh = true
	h.idp.mu.Unlock()

	hdr, err := authHeader(t, a)
	require.NoError(t, err)
	require.Equal(t, "Bearer access-2", hdr)
	require.Equal(t, 2, h.openCount())
	grants, _ := h.idp.snapshot()
	require.Equal(t, []string{"authorization_code", "refresh_token", "authorization_code"}, grants)
}

func TestBrowserLoginCustomClientScopesAndRedirect(t *testing.T) {
	h := newBrowserHarness(t)
	h.cfg.clientID = "dbt-databricks"
	h.cfg.scopes = []string{"all-apis"}
	a := newBrowserAuthenticator(h.cfg)

	_, err := authHeader(t, a)
	require.NoError(t, err)
	_, auth := h.idp.snapshot()
	require.Len(t, auth, 1)
	require.Equal(t, "dbt-databricks", auth[0].Get("client_id"))
	require.Contains(t, strings.Fields(auth[0].Get("scope")), "all-apis")
	require.Equal(t, "http://"+h.cfg.redirectURL, auth[0].Get("redirect_uri"))
}

func TestBrowserLoginFailures(t *testing.T) {
	t.Run("identity provider error", func(t *testing.T) {
		h := newBrowserHarness(t)
		h.idp.authorizeError = "access_denied"
		_, err := authHeader(t, newBrowserAuthenticator(h.cfg))
		require.ErrorContains(t, err, "identity provider error: access_denied: denied by user")
	})
	t.Run("state mismatch is ignored until timeout", func(t *testing.T) {
		h := newBrowserHarness(t)
		h.idp.stateOverride = "someone-elses-state"
		h.cfg.timeout = 500 * time.Millisecond
		_, err := authHeader(t, newBrowserAuthenticator(h.cfg))
		require.ErrorContains(t, err, "timed out waiting for response from provider")
		require.Eventually(t, func() bool {
			h.mu.Lock()
			defer h.mu.Unlock()
			return len(h.callback) == 1 && h.callback[0] == http.StatusBadRequest
		}, 2*time.Second, 10*time.Millisecond)
	})
	t.Run("no callback", func(t *testing.T) {
		h := newBrowserHarness(t)
		h.cfg.openBrowser = func(string) error { return fmt.Errorf("no browser") }
		h.cfg.timeout = 200 * time.Millisecond
		_, err := authHeader(t, newBrowserAuthenticator(h.cfg))
		require.ErrorContains(t, err, "timed out")
		require.Contains(t, h.out.String(), "Please open manually")
	})
	t.Run("redirect port in use", func(t *testing.T) {
		h := newBrowserHarness(t)
		l, err := net.Listen("tcp", h.cfg.redirectURL)
		require.NoError(t, err)
		defer l.Close() //nolint:errcheck
		_, err = authHeader(t, newBrowserAuthenticator(h.cfg))
		require.ErrorContains(t, err, "unable to listen for the login callback")
	})
	t.Run("canceled request", func(t *testing.T) {
		h := newBrowserHarness(t)
		h.cfg.openBrowser = func(string) error { return nil }
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.invalid", nil)
		require.NoError(t, err)
		err = newBrowserAuthenticator(h.cfg).Authenticate(req)
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("unknown cloud has no built-in client", func(t *testing.T) {
		_, err := resolveBrowserClientID("localhost", "")
		require.ErrorContains(t, err, "unhandled cloud type")
		id, err := resolveBrowserClientID("localhost", "mine")
		require.NoError(t, err)
		require.Equal(t, "mine", id)
	})
}

func TestBrowserBuiltInClientPerCloud(t *testing.T) {
	for host, want := range map[string]string{
		"x.cloud.databricks.com":      "databricks-sql-connector",
		"x.gcp.databricks.com":        "databricks-sql-connector",
		"adb-1.2.azuredatabricks.net": "96eecda7-19ea-49cc-abb5-240097d554f5",
	} {
		got, err := resolveBrowserClientID(host, "")
		require.NoError(t, err, host)
		require.Equal(t, want, got, host)
	}
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

func TestBrowserLoginRefreshFailureThatIsNotARejection(t *testing.T) {
	h := newBrowserHarness(t)
	h.idp.expiresIn = 1
	a := newBrowserAuthenticator(h.cfg)
	_, err := authHeader(t, a)
	require.NoError(t, err)

	h.idp.mu.Lock()
	h.idp.failRefreshStatus = http.StatusServiceUnavailable
	h.idp.mu.Unlock()
	_, err = authHeader(t, a)
	require.ErrorContains(t, err, "unable to refresh the login token")
	require.Equal(t, 1, h.openCount(), "an unavailable token endpoint must not open the browser")

	// Once the endpoint recovers the same refresh token still works.
	h.idp.mu.Lock()
	h.idp.failRefreshStatus = 0
	h.idp.mu.Unlock()
	hdr, err := authHeader(t, a)
	require.NoError(t, err)
	require.Equal(t, "Bearer access-2", hdr)
	require.Equal(t, 1, h.openCount())
}

func TestBrowserLoginRefreshOutlivesFirstRequestContext(t *testing.T) {
	h := newBrowserHarness(t)
	h.idp.expiresIn = 1
	a := newBrowserAuthenticator(h.cfg)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)
	require.NoError(t, a.Authenticate(req))
	cancel() // the request that triggered the login is over

	hdr, err := authHeader(t, a)
	require.NoError(t, err)
	require.Equal(t, "Bearer access-2", hdr)
	require.Equal(t, 1, h.openCount())
}
