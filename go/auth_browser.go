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

	// OptionExternalBrowserTimeout is how long browser login waits for the
	// sign-in to finish, as a Go duration such as "2m".
	OptionExternalBrowserTimeout = "databricks.oauth.external_browser.timeout"

	// DefaultBrowserLoginTimeout is how long a browser login waits for the
	// identity provider to call back.
	DefaultBrowserLoginTimeout = 1 * time.Minute

	// tokenRequestTimeout bounds each request to the token endpoint.
	tokenRequestTimeout = 30 * time.Second

	// defaultBrowserRedirectURL is where the identity provider sends the
	// browser back to. It matches the redirect registered for the built-in
	// clients.
	defaultBrowserRedirectURL = "localhost:8020"

	// Built-in public OAuth clients, one per cloud. Copied from the
	// unexported constants in
	// https://github.com/databricks/databricks-sql-go/blob/v1.17.0/auth/oauth/u2m/authenticator.go
	// so that a profile that sets no client ID signs in as before.
	awsGCPBrowserClientID = "databricks-sql-connector"
	azureBrowserClientID  = "96eecda7-19ea-49cc-abb5-240097d554f5"
)

// externalBrowserMethod logs in as a user through the system browser
// (OAuth authorization code with PKCE). It has no required options: the
// built-in client and redirect are used unless they are overridden.
var externalBrowserMethod = loginMethod{
	optional: []string{OptionOAuthClientID, OptionOAuthScopes, OptionOAuthRedirectURL, OptionExternalBrowserTimeout},
	build: func(d *databaseImpl) dbsql.ConnOption {
		return dbsql.WithAuthenticator(d.newBrowserAuthenticator())
	},
}

// newBrowserAuthenticator builds the authenticator for external-browser login
// from the database options. Nothing is contacted until the first request.
func (d *databaseImpl) newBrowserAuthenticator() auth.Authenticator {
	return newBrowserAuthenticator(browserLoginConfig{
		host:        d.serverHostname,
		clientID:    d.oauthClientID,
		scopes:      d.oauthScopes,
		redirectURL: d.oauthRedirectURL,
		timeout:     d.oauthU2MTimeout,
	})
}

// browserLoginConfig is everything a browser login needs. Zero values mean
// the default.
type browserLoginConfig struct {
	host        string
	clientID    string
	scopes      []string
	redirectURL string
	timeout     time.Duration

	// tokenCacheDir holds the on-disk token cache and login lease. Empty means
	// the default directory (see defaultTokenCacheDir).
	tokenCacheDir string

	// Seams for tests; nil means the real implementation.
	resolveConfig func(ctx context.Context, c browserLoginConfig) (oauth2.Config, error)
	openBrowser   func(loginURL string) error
	out           io.Writer
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

	mu          sync.Mutex
	oauthConfig *oauth2.Config
	tokenSource oauth2.TokenSource

	cache        *tokenCache // nil when no cache directory is available
	lastSaved    string      // access token last written to the cache
	cacheWarning bool

	// refreshToken is the refresh token of the token source in use, and
	// rejected the last one the identity provider refused; a rejected token is
	// not tried again from the cache.
	refreshToken string
	rejected     string
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
		cfg.out = os.Stdout
	}
	a := &browserAuthenticator{cfg: cfg}
	if cache, err := newTokenCache(cfg.tokenCacheDir); err == nil {
		a.cache = cache
	}
	return a
}

// Authenticate adds a bearer token to the request. The first call runs the
// browser login; later calls reuse the token and refresh it with the refresh
// token when it expires. If the refresh is rejected, the login runs again.
func (a *browserAuthenticator) Authenticate(r *http.Request) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	ctx := r.Context()
	if a.tokenSource != nil {
		token, err := a.tokenSource.Token()
		if err == nil {
			a.refreshToken = token.RefreshToken
			a.save(token) // keeps the cache current after a refresh
			token.SetAuthHeader(r)
			return nil
		}
		if !isRejectedGrant(err) {
			// A network or server failure says nothing about the refresh
			// token: report it instead of opening a browser.
			return fmt.Errorf("unable to refresh the login token: %w", err)
		}
		// The refresh token no longer works: sign in again.
		a.rejected = a.refreshToken
		a.tokenSource = nil
	}

	token, err := a.login(ctx)
	if err != nil {
		return fmt.Errorf("browser login failed: %w", err)
	}
	token.SetAuthHeader(r)
	return nil
}

// isRejectedGrant reports whether the identity provider refused the refresh
// token (OAuth error invalid_grant), as opposed to being unreachable or failing.
func isRejectedGrant(err error) bool {
	var re *oauth2.RetrieveError
	return errors.As(err, &re) && re.ErrorCode == "invalid_grant"
}

// tokenContext is the context the token source keeps for later refreshes. It
// must outlive the request that triggered the login, so it drops that
// request's cancellation, and it bounds each token request with a timeout.
func tokenContext(ctx context.Context) context.Context {
	return context.WithValue(context.WithoutCancel(ctx), oauth2.HTTPClient, &http.Client{Timeout: tokenRequestTimeout})
}

// login returns a token for the first request or after a rejected refresh. It
// prefers, in order: the cached token (refreshed when it is stale), then a
// browser login. Logins are serialised across processes by the lease, so that
// one process opens the browser and the others pick up the token it caches.
func (a *browserAuthenticator) login(ctx context.Context) (*oauth2.Token, error) {
	if a.oauthConfig == nil {
		conf, err := a.cfg.resolveConfig(context.WithoutCancel(ctx), a.cfg)
		if err != nil {
			return nil, fmt.Errorf("unable to generate oauth2.Config: %w", err)
		}
		a.oauthConfig = &conf
	}
	token, err := a.fromCache(ctx)
	if err != nil || token != nil {
		return token, err
	}
	var lost chan struct{} // closed when the login lease is lost
	if a.cache != nil {
		lease, err := a.cache.acquireLease(ctx, a.cfg.timeout)
		if err != nil {
			return nil, err
		}
		defer lease.release()
		// If the lease is lost (another process took it over), stop waiting
		// for the browser rather than race that login.
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		lost = make(chan struct{})
		defer lease.keepAlive(leaseTTL, func() { close(lost); cancel() })()
		// Another process may have finished its login while we waited.
		token, err := a.fromCache(ctx)
		if err != nil || token != nil {
			return token, err
		}
	}
	token, err = a.runBrowserFlow(ctx, *a.oauthConfig)
	if err != nil {
		select {
		case <-lost:
			return nil, fmt.Errorf("lost the login lease to another process: %w", err)
		default:
		}
		return nil, err
	}
	a.tokenSource = a.oauthConfig.TokenSource(tokenContext(ctx), token)
	a.refreshToken = token.RefreshToken
	a.save(token)
	return token, nil
}

// fromCache returns a usable token from the cache, refreshing it with the
// stored refresh token when the access token has expired. It returns nil, nil
// when there is nothing usable, including when the identity provider rejects
// the refresh token. Any other refresh failure (network, server error) is
// returned, so it never opens a browser.
func (a *browserAuthenticator) fromCache(ctx context.Context) (*oauth2.Token, error) {
	if a.cache == nil {
		return nil, nil
	}
	cached := a.cache.read(a.cfg.host, a.cfg.clientID, a.cfg.scopes)
	if cached == nil || (cached.RefreshToken != "" && cached.RefreshToken == a.rejected && !cached.Valid()) {
		return nil, nil
	}
	source := a.oauthConfig.TokenSource(tokenContext(ctx), cached)
	token, err := source.Token()
	if err != nil {
		if isRejectedGrant(err) {
			a.rejected = cached.RefreshToken
			return nil, nil
		}
		return nil, fmt.Errorf("unable to refresh the cached login token: %w", err)
	}
	a.tokenSource = source
	a.refreshToken = token.RefreshToken
	a.save(token)
	return token, nil
}

// save writes the token to the cache when it changed. A failure does not stop
// the login; it is reported once.
func (a *browserAuthenticator) save(token *oauth2.Token) {
	if a.cache == nil || token.AccessToken == a.lastSaved {
		return
	}
	if err := a.cache.write(a.cfg.host, a.cfg.clientID, a.cfg.scopes, token); err != nil {
		if !a.cacheWarning {
			a.cacheWarning = true
			_, _ = fmt.Fprintf(a.cfg.out, "Warning: could not cache the login token: %v\n", err)
		}
		return
	}
	a.lastSaved = token.AccessToken
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

	state, err := randomState(16)
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
		ReadHeaderTimeout: 3 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	go func() { _ = srv.Serve(listener) }()
	defer srv.Close() //nolint:errcheck

	_, _ = fmt.Fprintf(a.cfg.out, "\nOpen URL in Browser to Continue: %s\n\n", loginURL)
	if err := a.cfg.openBrowser(loginURL); err != nil {
		_, _ = fmt.Fprintf(a.cfg.out, "Unable to open browser automatically. Please open manually: %s\n", loginURL)
	}

	timer := time.NewTimer(a.cfg.timeout)
	defer timer.Stop()
	select {
	case res := <-results:
		if res.errCode != "" {
			return nil, fmt.Errorf("identity provider error: %s: %s", res.errCode, res.errDesc)
		}
		token, err := conf.Exchange(tokenContext(ctx), res.code, verifier)
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
		writeBrowserPage(w, "Login successful", "You may close this window and go back to the terminal.")
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

// validateBrowserRedirectURL checks that the redirect address can be listened
// on: a loopback host with an explicit port. An empty value means the default.
func validateBrowserRedirectURL(value string) error {
	if value == "" {
		return nil
	}
	invalid := func(why string) error {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[db] invalid OAuth redirect URL '%s': %s", value, why),
		}
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
	switch u.Hostname() {
	case "localhost", "127.0.0.1", "::1":
	default:
		return invalid("the host must be localhost, 127.0.0.1 or ::1")
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
