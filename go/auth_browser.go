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
	"cmp"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/databricks/databricks-sql-go/auth"
	"github.com/databricks/databricks-sql-go/auth/oauth"
	"github.com/databricks/databricks-sql-go/auth/oauth/u2m"
	"github.com/pkg/browser"
	"golang.org/x/oauth2"

	dbsql "github.com/databricks/databricks-sql-go"
)

const (
	// OptionValueAuthTypeExternalBrowser selects browser (user-to-machine
	// OAuth) login for databricks.auth_type.
	OptionValueAuthTypeExternalBrowser = "external-browser"

	// OptionOAuthRedirectURL is the local address external-browser login
	// listens on for the redirect, such as "localhost:9000" or
	// "http://localhost:9000/callback". It must be registered on the OAuth app.
	// The default is "localhost:8020".
	OptionOAuthRedirectURL = "databricks.oauth.redirect_url"

	// OptionTokenCache selects where the token of external-browser login is
	// kept: "keyring" (the default) also keeps it in the OS keyring, so that
	// later processes reuse it, and "memory" keeps it in the process only.
	OptionTokenCache = "databricks.oauth.token_cache"

	// OptionExternalBrowserTimeout is how long browser login waits for the
	// sign-in to finish, as a Go duration such as "2m".
	OptionExternalBrowserTimeout = "databricks.oauth.external_browser.timeout"

	// DefaultBrowserLoginTimeout is how long a browser login waits for the
	// identity provider to call back.
	DefaultBrowserLoginTimeout = 1 * time.Minute

	// tokenRequestTimeout bounds each request to the token endpoint.
	tokenRequestTimeout = 30 * time.Second

	// callbackReadHeaderTimeout and callbackWriteTimeout bound the loopback
	// server that receives the redirect.
	callbackReadHeaderTimeout = 3 * time.Second
	callbackShutdownTimeout   = 2 * time.Second
	callbackWriteTimeout      = 30 * time.Second

	// stateBytes is the size of the random state value that ties the redirect to
	// the login that started it.
	stateBytes = 16

	// defaultBrowserRedirectURL is where the identity provider sends the
	// browser back to. It matches the redirect registered for the built-in
	// clients.
	defaultBrowserRedirectURL = "localhost:8020"

	// Built-in public OAuth clients, one per cloud. Copied from the
	// unexported constants in
	// https://github.com/databricks/databricks-sql-go/blob/v1.19.0/auth/oauth/u2m/authenticator.go
	// so that a profile that sets no client ID signs in as before.
	awsGCPBrowserClientID = "databricks-sql-connector"
	azureBrowserClientID  = "96eecda7-19ea-49cc-abb5-240097d554f5"
)

// externalBrowserMethod logs in as a user through the system browser
// (OAuth authorization code with PKCE). It has no required options: the
// built-in client and redirect are used unless they are overridden.
var externalBrowserMethod = loginMethod{
	build: func(d *databaseImpl) dbsql.ConnOption {
		return dbsql.WithAuthenticator(d.newBrowserAuthenticator())
	},
}

// browserLoginMu guards databaseImpl.browserLogin and its key. It is shared by
// all databases, which is cheap because only creating the login takes it.
var browserLoginMu sync.Mutex

// newBrowserAuthenticator builds the authenticator for external-browser login
// from the database options. Nothing is contacted until the first request.
func (d *databaseImpl) newBrowserAuthenticator() auth.Authenticator {
	// Assign only a real transport: a nil *http.Transport in the interface
	// would not be nil.
	var transport http.RoundTripper
	if t := d.customTransport(); t != nil {
		transport = t
	}
	cfg := browserLoginConfig{
		host:        d.oauthHostPort(),
		clientID:    d.oauthClientID,
		scopes:      d.oauthScopes,
		redirectURL: d.oauthRedirectURL,
		tokenCache:  cmp.Or(d.tokenCache, tokenCacheKeyring),
		timeout:     d.oauthU2MTimeout,
		transport:   transport,
	}
	// Open can run on several goroutines: they must share one login, or they
	// would fight over the redirect port.
	browserLoginMu.Lock()
	defer browserLoginMu.Unlock()
	// Reuse the login while the options it depends on are unchanged.
	timeout := cfg.timeout
	if timeout <= 0 {
		timeout = DefaultBrowserLoginTimeout
	}
	key := fmt.Sprintf("%q|%v|%q|%v|%q|%q|%q|%s", cfg.host, timeout, d.sslRootCert, d.sslInsecure, cfg.clientID, cfg.scopes, cfg.redirectURL, cfg.tokenCache)
	if d.browserLogin == nil || d.browserLoginKey != key {
		d.browserLogin = newBrowserAuthenticator(cfg)
		d.browserLoginKey = key
	}
	return d.browserLogin
}

// browserLoginConfig is everything a browser login needs. Zero values mean
// the default.
type browserLoginConfig struct {
	host        string
	clientID    string
	scopes      []string
	redirectURL string
	timeout     time.Duration
	// transport carries the database's TLS settings; nil means the default.
	transport http.RoundTripper

	// tokenCache is where the token is kept: tokenCacheKeyring (the default
	// when empty) or tokenCacheMemory.
	tokenCache string

	// Seams for tests; nil means the real implementation.
	resolveConfig func(ctx context.Context, c browserLoginConfig) (oauth2.Config, error)
	openBrowser   func(loginURL string) error
	out           io.Writer
	keyring       keyringBackend
}

// resolveBrowserClientID returns the configured client ID, or the built-in
// client for the cloud the host belongs to.
func resolveBrowserClientID(host, configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	switch cloud := oauth.InferCloudFromHost(host); cloud {
	case oauth.AWS, oauth.GCP:
		return awsGCPBrowserClientID, nil
	case oauth.Azure:
		return azureBrowserClientID, nil
	default:
		return "", fmt.Errorf("unhandled cloud type for host '%s': %s", host, cloud.String())
	}
}

// defaultResolveConfig looks up the identity provider's endpoints with the
// databricks-sql-go resolver, which also adds the scopes the cloud requires.
func defaultResolveConfig(ctx context.Context, c browserLoginConfig) (oauth2.Config, error) {
	clientID, err := resolveBrowserClientID(c.host, c.clientID)
	if err != nil {
		return oauth2.Config{}, err
	}
	redirect := c.redirectURL
	if redirect == "" {
		redirect = defaultBrowserRedirectURL
	}
	return u2m.GetConfig(ctx, c.host, clientID, "", redirect, c.scopes)
}

type browserAuthenticator struct {
	cfg browserLoginConfig

	// lock lets one request at a time use the login. It is a channel, not a
	// mutex, so a request waiting behind another's sign-in stops when its own
	// context ends.
	lock        chan struct{}
	oauthConfig *oauth2.Config
	// token is the token this authenticator last used. It is checked on every
	// request; once it expires, login looks at the shared cache first.
	token *oauth2.Token

	// cache keeps the token in memory, shared by every database with the same
	// workspace, client and scopes, and in the keyring unless that is off.
	cache *tokenCache
}

func newBrowserAuthenticator(cfg browserLoginConfig) *browserAuthenticator {
	if cfg.timeout <= 0 {
		cfg.timeout = DefaultBrowserLoginTimeout
	}
	if cfg.resolveConfig == nil {
		cfg.resolveConfig = defaultResolveConfig
	}
	if cfg.openBrowser == nil {
		cfg.openBrowser = browser.OpenURL
	}
	if cfg.out == nil {
		cfg.out = os.Stderr // stdout may carry query results
	}
	return &browserAuthenticator{cfg: cfg, cache: tokenCacheFor(cfg), lock: make(chan struct{}, 1)}
}

// Authenticate adds a bearer token to the request. The first call runs the
// browser login; later calls reuse the token and refresh it with the refresh
// token when it expires. If the refresh is rejected, the login runs again.
func (a *browserAuthenticator) Authenticate(r *http.Request) error {
	ctx := r.Context()
	select {
	case a.lock <- struct{}{}:
		defer func() { <-a.lock }()
	case <-ctx.Done():
		return fmt.Errorf("interrupted while waiting for another login: %w", ctx.Err())
	}

	// A token that is still valid needs nothing. Otherwise login looks at the
	// cache that every database of the workspace shares (another may already
	// have refreshed), and refreshes or signs in on its turn. A refresh is never
	// made outside that turn, so sibling databases do not use the same refresh
	// token at once.
	if a.token == nil || !a.token.Valid() {
		token, err := a.login(ctx)
		if err != nil {
			return fmt.Errorf("browser login failed: %w", err)
		}
		a.token = token
	}
	a.token.SetAuthHeader(r)
	return nil
}

// errNoRefreshToken is the text of the error the oauth2 package returns for an
// expired token that came without a refresh token.
const errNoRefreshToken = "refresh token is not set"

// isRejectedGrant reports whether the token can no longer be refreshed, so a new
// login is needed: the identity provider refused the refresh token (OAuth error
// invalid_grant), or it never gave one. A network or server failure is neither.
func isRejectedGrant(err error) bool {
	var re *oauth2.RetrieveError
	if errors.As(err, &re) {
		return re.ErrorCode == "invalid_grant"
	}
	return err != nil && strings.Contains(err.Error(), errNoRefreshToken)
}

// browserFlowSlot lets one browser login run at a time in a process: the
// redirect port is fixed, so two at once would fail with "address in use".
var browserFlowSlot = make(chan struct{}, 1)

// acquireBrowserFlow waits for the slot, or for ctx to end.
func acquireBrowserFlow(ctx context.Context) (release func(), err error) {
	select {
	case browserFlowSlot <- struct{}{}:
		return func() { <-browserFlowSlot }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("interrupted while waiting for another login: %w", ctx.Err())
	}
}

// requestContext carries the HTTP client for token requests (the database's
// TLS settings and a timeout) and keeps the cancellation of ctx.
func (a *browserAuthenticator) requestContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, oauth2.HTTPClient, &http.Client{
		Transport: a.cfg.transport,
		Timeout:   tokenRequestTimeout,
	})
}

// login returns a token for the first request or after a rejected refresh. It
// prefers, in order: a cached token that is still valid, the cached token
// refreshed, and then a browser login. Refreshes and logins in a process take
// turns, so when several databases need a token at once, one logs in and the
// others then find its token in the cache.
func (a *browserAuthenticator) login(ctx context.Context) (*oauth2.Token, error) {
	// A cached token that is still valid needs no refresh, no wait and no
	// network, not even the endpoint lookup.
	token, err := a.fromCache(ctx, false)
	if err != nil || token != nil {
		return token, err
	}
	if a.oauthConfig == nil {
		// Discovery stops when the request does, and never takes longer than
		// a token request.
		discoveryCtx, cancel := context.WithTimeout(ctx, tokenRequestTimeout)
		conf, err := a.cfg.resolveConfig(discoveryCtx, a.cfg)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("unable to generate oauth2.Config: %w", err)
		}
		a.oauthConfig = &conf
	}
	release, err := acquireBrowserFlow(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	// Another database or process may have refreshed or logged in while we
	// waited: read the cache again, and refresh it if it has expired.
	token, err = a.fromCache(ctx, true)
	if err != nil || token != nil {
		return token, err
	}
	token, err = a.runBrowserFlow(ctx, *a.oauthConfig)
	if err != nil {
		return nil, err
	}
	a.cache.Set(token)
	return token, nil
}

// fromCache returns a usable token from the cache. With refresh false it only
// returns a token that is still valid, and never makes a request. With refresh
// true, which callers set after they take their turn, the keyring is read again
// (another process may have rotated the refresh token) and an expired access
// token is refreshed. It returns nil, nil when there is nothing usable,
// including when the identity provider rejects the refresh token, which is then
// removed from the cache unless another process has already replaced it. Any
// other refresh failure (network, server error) is returned, so it never opens
// a browser.
func (a *browserAuthenticator) fromCache(ctx context.Context, refresh bool) (*oauth2.Token, error) {
	if !refresh {
		if cached := a.cache.Token(); cached != nil && cached.Valid() {
			return cached, nil
		}
		return nil, nil
	}
	cached := a.cache.Reload()
	if cached == nil {
		return nil, nil
	}
	if cached.Valid() {
		return cached, nil // another process refreshed it
	}
	token, err := a.oauthConfig.TokenSource(a.requestContext(ctx), cached).Token()
	if err != nil {
		if isRejectedGrant(err) {
			if !a.cache.ClearIfSame(cached.RefreshToken) {
				// Another process stored a newer token while we tried.
				if newer := a.cache.Token(); newer != nil && newer.Valid() {
					return newer, nil
				}
			}
			return nil, nil
		}
		return nil, fmt.Errorf("unable to refresh the cached login token: %w", err)
	}
	a.cache.Set(token)
	return token, nil
}

type callbackResult struct {
	code, state, errCode, errDesc string
}

// runBrowserFlow runs the authorization code flow with PKCE: it listens on
// the redirect address, sends the user to the login page, and exchanges the
// code the identity provider calls back with.
func (a *browserAuthenticator) runBrowserFlow(ctx context.Context, conf oauth2.Config) (*oauth2.Token, error) {
	redirect, err := url.Parse(conf.RedirectURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redirect URL '%s': %w", conf.RedirectURL, err)
	}
	if redirect.Path == "" {
		redirect.Path = "/"
	}

	state, err := randomState(stateBytes)
	if err != nil {
		return nil, err
	}
	challenge, method, verifier, err := u2m.GetAuthCodeOptions()
	if err != nil {
		return nil, err
	}
	loginURL := conf.AuthCodeURL(state, challenge, method)

	listener, err := net.Listen("tcp", redirect.Host)
	if err != nil {
		return nil, fmt.Errorf("unable to listen for the login callback on %s: %w", redirect.Host, err)
	}
	defer listener.Close() //nolint:errcheck

	results := make(chan callbackResult, 1)
	mux := http.NewServeMux()
	mux.HandleFunc(redirect.Path, func(w http.ResponseWriter, req *http.Request) {
		handleBrowserCallback(w, req, state, results)
	})
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: callbackReadHeaderTimeout,
		WriteTimeout:      callbackWriteTimeout,
	}
	go func() { _ = srv.Serve(listener) }()
	defer func() {
		// Let the page reach the browser before the listener goes away.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), callbackShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	_, _ = fmt.Fprintf(a.cfg.out, "\nOpen URL in Browser to Continue: %s\n\n", loginURL)
	// The launcher can stay attached to the browser (on Linux it is waited for),
	// so it runs on its own: the wait for the callback, the timeout and
	// cancellation start now, not when the launcher returns.
	go func() {
		if err := a.cfg.openBrowser(loginURL); err != nil {
			_, _ = fmt.Fprintf(a.cfg.out, "Unable to open browser automatically. Please open manually: %s\n", loginURL)
		}
	}()

	timer := time.NewTimer(a.cfg.timeout)
	defer timer.Stop()
	select {
	case res := <-results:
		if res.errCode != "" {
			return nil, fmt.Errorf("identity provider error: %s: %s", res.errCode, res.errDesc)
		}
		token, err := conf.Exchange(a.requestContext(ctx), res.code, verifier)
		if err != nil {
			return nil, fmt.Errorf("failed to exchange token: %w", err)
		}
		return token, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("interrupted while waiting for auth callback: %w", ctx.Err())
	case <-timer.C:
		return nil, errors.New("timed out waiting for response from provider")
	}
}

// handleBrowserCallback receives the redirect from the identity provider.
// Requests that are not the callback (favicon, empty probes) and callbacks
// with another state (an old browser tab) are rejected without ending the
// login.
func handleBrowserCallback(w http.ResponseWriter, req *http.Request, state string, results chan<- callbackResult) {
	q := req.URL.Query()
	res := callbackResult{
		code:    q.Get("code"),
		state:   q.Get("state"),
		errCode: q.Get("error"),
		errDesc: q.Get("error_description"),
	}
	if res.code == "" && res.state == "" && res.errCode == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if res.state != state {
		w.WriteHeader(http.StatusBadRequest)
		writeBrowserPage(w, "Login failed", "Authentication state mismatch. Close this window and use the latest browser tab.")
		return
	}
	if res.errCode != "" {
		w.WriteHeader(http.StatusBadRequest)
		writeBrowserPage(w, "Login failed", "The identity provider returned an error: "+res.errCode)
	} else {
		writeBrowserPage(w, "Sign-in received", "You may close this window and go back to the terminal.")
	}
	select {
	case results <- res:
	default: // already received a result
	}
}

func writeBrowserPage(w http.ResponseWriter, title, msg string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w, "<!doctype html><html><head><title>%s</title></head><body><h2>%s</h2><p>%s</p></body></html>",
		html.EscapeString(title), html.EscapeString(title), html.EscapeString(msg))
}

// muxAcceptsPath reports whether path can be registered on a ServeMux, which
// panics on a pattern it cannot parse (braces, a space and so on). The login
// registers the callback on a mux, so a path that panics there is rejected here.
func muxAcceptsPath(path string) (ok bool) {
	if path == "" {
		path = "/" // the login uses "/" when the redirect has no path
	}
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	http.NewServeMux().HandleFunc(path, func(http.ResponseWriter, *http.Request) {})
	return true
}

// validateBrowserRedirectURL checks that the redirect address can be listened
// on: a loopback host with an explicit port. An empty value means the default.
func validateBrowserRedirectURL(value string) error {
	if value == "" {
		return nil
	}
	invalid := func(why string) error {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[db] invalid OAuth redirect URL: " + why,
		}
	}
	// A fragment is not valid in a redirect URI (even an empty one), and an
	// escape would make the path the browser asks for differ from the one the
	// callback is registered under.
	if strings.ContainsAny(value, "#%") {
		return invalid("it must not contain '#' or '%'")
	}
	raw := value
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return invalid("not a valid URL")
	}
	if u.Scheme != "http" {
		return invalid("the scheme must be http")
	}
	if u.User != nil {
		return invalid("it must not contain a user name or password")
	}
	// The identity provider appends its own query to the redirect, and a
	// fragment is not valid in a redirect URI.
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return invalid("it must not contain a query or a fragment")
	}
	// Go splits an unbracketed IPv6 address on its last colon, so it would pass
	// here and fail later when the listener is opened.
	if strings.Count(u.Host, ":") > 1 && !strings.HasPrefix(u.Host, "[") {
		return invalid("an IPv6 address must be in brackets, such as [::1]:8020")
	}
	switch u.Hostname() {
	case "localhost", "127.0.0.1", "::1":
	default:
		return invalid("the host must be localhost, 127.0.0.1 or ::1")
	}
	// "{name}" and "{$}" are patterns to the mux, not part of a path.
	if strings.ContainsAny(u.Path, "{}") || !muxAcceptsPath(u.Path) {
		return invalid("the path cannot be used as a callback path")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return invalid("a port between 1 and 65535 is required")
	}
	return nil
}

func randomState(n int) (string, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return "", fmt.Errorf("unable to generate state: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
