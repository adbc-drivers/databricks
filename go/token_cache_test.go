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
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zalando/go-keyring"
	"golang.org/x/oauth2"
)

// fakeKeyring is an in-memory keyring.
type fakeKeyring struct {
	mu      sync.Mutex
	entries map[string]string
}

func newFakeKeyring() *fakeKeyring { return &fakeKeyring{entries: map[string]string{}} }

func (f *fakeKeyring) Set(service, user, password string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[service+"/"+user] = password
	return nil
}

func (f *fakeKeyring) Get(service, user string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.entries[service+"/"+user]
	if !ok {
		return "", keyring.ErrNotFound
	}
	return v, nil
}

func (f *fakeKeyring) Delete(service, user string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.entries[service+"/"+user]; !ok {
		return keyring.ErrNotFound
	}
	delete(f.entries, service+"/"+user)
	return nil
}

func (f *fakeKeyring) size() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.entries)
}

func testCacheConfig(kr keyringBackend, mode string) browserLoginConfig {
	return browserLoginConfig{host: "x.cloud.databricks.com", tokenCache: mode, keyring: kr, out: io.Discard}
}

func TestTokenCacheRoundTrip(t *testing.T) {
	kr := newFakeKeyring()
	cache := tokenCacheFor(testCacheConfig(kr, tokenCacheKeyring))
	require.Nil(t, cache.Token())

	want := &oauth2.Token{AccessToken: "a", RefreshToken: "r", TokenType: "Bearer", Expiry: time.Now().Add(time.Hour).Truncate(time.Second)}
	cache.Set(want)
	require.Equal(t, want.AccessToken, cache.Token().AccessToken)

	// Another process (a new cache on the same keyring) reads it.
	got := tokenCacheFor(testCacheConfig(kr, tokenCacheKeyring)).Token()
	require.NotNil(t, got)
	require.Equal(t, want.AccessToken, got.AccessToken)
	require.Equal(t, want.RefreshToken, got.RefreshToken)
	require.True(t, want.Expiry.Equal(got.Expiry))

	cache.Clear()
	require.Nil(t, cache.Token())
	require.Equal(t, 0, kr.size())
	require.Nil(t, tokenCacheFor(testCacheConfig(kr, tokenCacheKeyring)).Token())
}

// A token too big for one keyring entry is stored in chunks and read back whole.
func TestTokenCacheChunksBigTokens(t *testing.T) {
	kr := newFakeKeyring()
	cache := tokenCacheFor(testCacheConfig(kr, tokenCacheKeyring))
	big := &oauth2.Token{AccessToken: strings.Repeat("a", 3*keyringChunkSize), RefreshToken: strings.Repeat("r", 500), Expiry: time.Now().Add(time.Hour)}
	cache.Set(big)
	require.Greater(t, kr.size(), 2)

	got := tokenCacheFor(testCacheConfig(kr, tokenCacheKeyring)).Token()
	require.NotNil(t, got)
	require.Equal(t, big.AccessToken, got.AccessToken)
	require.Equal(t, big.RefreshToken, got.RefreshToken)

	// A smaller token afterwards leaves no chunks behind.
	cache.Set(&oauth2.Token{AccessToken: "small", RefreshToken: "r", Expiry: time.Now().Add(time.Hour)})
	require.Equal(t, 1, kr.size())
}

func TestTokenCacheIgnoresAnUnreadableValue(t *testing.T) {
	kr := newFakeKeyring()
	cfg := testCacheConfig(kr, tokenCacheKeyring)
	require.NoError(t, kr.Set(keyringService, tokenCacheKey(cfg.host, "", nil), "not json"))
	require.Nil(t, tokenCacheFor(cfg).Token())
}

// With "memory" nothing reaches the keyring.
func TestTokenCacheMemoryModeLeavesTheKeyringAlone(t *testing.T) {
	kr := newFakeKeyring()
	cache := tokenCacheFor(testCacheConfig(kr, tokenCacheMemory))
	cache.Set(&oauth2.Token{AccessToken: "a", RefreshToken: "r", Expiry: time.Now().Add(time.Hour)})
	require.Equal(t, "a", cache.Token().AccessToken)
	require.Equal(t, 0, kr.size())
}

// Databases with the same workspace, client and scopes share one cache in the
// process, so when one logs in the others reuse its token.
func TestTokenCachesAreSharedInTheProcess(t *testing.T) {
	cfg := browserLoginConfig{host: "shared.cloud.databricks.com", tokenCache: tokenCacheMemory, out: io.Discard}
	first := tokenCacheFor(cfg)
	require.Same(t, first, tokenCacheFor(cfg))

	other := cfg
	other.clientID = "another-app"
	require.NotSame(t, first, tokenCacheFor(other))

	cfg.scopes = []string{"b", "a"}
	same := cfg
	same.scopes = []string{"a", "b"}
	require.Same(t, tokenCacheFor(cfg), tokenCacheFor(same), "the order of the scopes does not matter")
}

// A valid cached token is used with no request, and an expired one is not
// refreshed until the caller has its turn (refresh is true).
func TestCachedTokenIsRefreshedOnlyWhenAsked(t *testing.T) {
	a := newBrowserAuthenticator(testCacheConfig(newFakeKeyring(), tokenCacheKeyring))
	// The token endpoint is unreachable: any request would fail the test.
	a.oauthConfig = &oauth2.Config{Endpoint: oauth2.Endpoint{TokenURL: "http://127.0.0.1:1/token"}}
	ctx := context.Background()

	a.cache.Set(&oauth2.Token{AccessToken: "fresh", RefreshToken: "r", Expiry: time.Now().Add(time.Hour)})
	token, err := a.fromCache(ctx, false)
	require.NoError(t, err)
	require.Equal(t, "fresh", token.AccessToken)

	a.cache.Set(&oauth2.Token{AccessToken: "old", RefreshToken: "r", Expiry: time.Now().Add(-time.Hour)})
	token, err = a.fromCache(ctx, false)
	require.NoError(t, err)
	require.Nil(t, token, "an expired token is refreshed only on the caller's turn")

	_, err = a.fromCache(ctx, true)
	require.ErrorContains(t, err, "unable to refresh the cached login token")
}

// An expired token with no refresh token can only be replaced by a new login,
// and is removed from the cache.
func TestExpiredTokenWithoutRefreshTokenIsDropped(t *testing.T) {
	kr := newFakeKeyring()
	a := newBrowserAuthenticator(testCacheConfig(kr, tokenCacheKeyring))
	a.oauthConfig = &oauth2.Config{Endpoint: oauth2.Endpoint{TokenURL: "http://127.0.0.1:1/token"}}
	a.cache.Set(&oauth2.Token{AccessToken: "old", Expiry: time.Now().Add(-time.Hour)})

	token, err := a.fromCache(context.Background(), true)
	require.NoError(t, err)
	require.Nil(t, token)
	require.Nil(t, a.cache.Token())
	require.Equal(t, 0, kr.size())
}

// A database whose token has expired uses the token another database saved,
// instead of refreshing the same refresh token again.
func TestAuthenticateUsesTheTokenAnotherDatabaseSaved(t *testing.T) {
	a := newBrowserAuthenticator(testCacheConfig(newFakeKeyring(), tokenCacheKeyring))
	// The token endpoint is unreachable: any request would fail the test.
	a.oauthConfig = &oauth2.Config{Endpoint: oauth2.Endpoint{TokenURL: "http://127.0.0.1:1/token"}}
	a.token = &oauth2.Token{AccessToken: "expired", RefreshToken: "r", Expiry: time.Now().Add(-time.Hour)}
	a.cache.Set(&oauth2.Token{AccessToken: "saved-by-sibling", RefreshToken: "r2", Expiry: time.Now().Add(time.Hour)})

	req, err := http.NewRequest(http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)
	require.NoError(t, a.Authenticate(req))
	require.Equal(t, "Bearer saved-by-sibling", req.Header.Get("Authorization"))
}

// failingDeleteKeyring fails to delete the entries that match fail.
type failingDeleteKeyring struct {
	*fakeKeyring
	fail func(user string) bool
}

func (f failingDeleteKeyring) Delete(service, user string) error {
	if f.fail(user) {
		return errors.New("keyring is locked")
	}
	return f.fakeKeyring.Delete(service, user)
}

// A chunk that cannot be removed is reported, not ignored.
func TestTokenCacheReportsAChunkItCannotRemove(t *testing.T) {
	inner := newFakeKeyring()
	var out strings.Builder
	cfg := testCacheConfig(failingDeleteKeyring{fakeKeyring: inner, fail: func(user string) bool { return strings.HasPrefix(user, "chunk#") && strings.Contains(user, "#1#") }}, tokenCacheKeyring)
	cfg.out = &out
	cache := tokenCacheFor(cfg)
	cache.Set(&oauth2.Token{AccessToken: strings.Repeat("a", 3*keyringChunkSize), RefreshToken: "r", Expiry: time.Now().Add(time.Hour)})

	cache.Clear()
	require.Contains(t, out.String(), "could not remove the login token from the keyring")
}

// A token saved in place of a bigger one removes the old chunks.
func TestTokenCacheRemovesOldChunksWhenTheTokenShrinks(t *testing.T) {
	kr := newFakeKeyring()
	cache := tokenCacheFor(testCacheConfig(kr, tokenCacheKeyring))
	cache.Set(&oauth2.Token{AccessToken: strings.Repeat("a", 5*keyringChunkSize), RefreshToken: "r", Expiry: time.Now().Add(time.Hour)})
	before := kr.size()
	cache.Set(&oauth2.Token{AccessToken: strings.Repeat("b", 2*keyringChunkSize), RefreshToken: "r", Expiry: time.Now().Add(time.Hour)})
	require.Less(t, kr.size(), before)
	got := tokenCacheFor(testCacheConfig(kr, tokenCacheKeyring)).Token()
	require.NotNil(t, got)
	require.Equal(t, strings.Repeat("b", 2*keyringChunkSize), got.AccessToken)
}

// A process that holds an old refresh token reads the keyring again before it
// refreshes, and never removes a newer token another process stored.
func TestStaleRefreshTokenDoesNotWipeANewerOne(t *testing.T) {
	kr := newFakeKeyring()
	mine := tokenCacheFor(testCacheConfig(kr, tokenCacheKeyring))
	mine.Set(&oauth2.Token{AccessToken: "a1", RefreshToken: "r1", Expiry: time.Now().Add(-time.Hour)})

	// Another process refreshes and stores a rotated refresh token.
	other := tokenCacheFor(testCacheConfig(kr, tokenCacheKeyring))
	other.Set(&oauth2.Token{AccessToken: "a2", RefreshToken: "r2", Expiry: time.Now().Add(time.Hour)})

	require.Equal(t, "r1", mine.Token().RefreshToken, "the process still has its own copy")
	require.Equal(t, "r2", mine.Reload().RefreshToken, "reloading finds the newer token")

	// The identity provider rejected r1: the newer token stays, in memory and in
	// the keyring.
	require.False(t, mine.ClearIfSame("r1"))
	require.Equal(t, "r2", mine.Token().RefreshToken)
	require.Equal(t, "r2", tokenCacheFor(testCacheConfig(kr, tokenCacheKeyring)).Token().RefreshToken)

	// A rejected token that is still the stored one is removed.
	require.True(t, mine.ClearIfSame("r2"))
	require.Nil(t, tokenCacheFor(testCacheConfig(kr, tokenCacheKeyring)).Token())
}

// failingSetKeyring fails to write the entries that match fail, once armed.
type failingSetKeyring struct {
	*fakeKeyring
	armed *bool
	fail  func(user string) bool
}

func (f failingSetKeyring) Set(service, user, password string) error {
	if *f.armed && f.fail(user) {
		return errors.New("keyring is full")
	}
	return f.fakeKeyring.Set(service, user, password)
}

// A save that fails half way leaves the previous token readable and no chunks
// of the new one behind.
func TestFailedSaveKeepsThePreviousToken(t *testing.T) {
	inner := newFakeKeyring()
	armed := false
	kr := failingSetKeyring{fakeKeyring: inner, armed: &armed, fail: func(user string) bool { return strings.Contains(user, "#1#") }}
	cache := tokenCacheFor(testCacheConfig(kr, tokenCacheKeyring))
	old := &oauth2.Token{AccessToken: strings.Repeat("o", 3*keyringChunkSize), RefreshToken: "old-refresh", Expiry: time.Now().Add(time.Hour)}
	cache.Set(old)
	entries := inner.size()

	armed = true
	cache.Set(&oauth2.Token{AccessToken: strings.Repeat("n", 3*keyringChunkSize), RefreshToken: "new-refresh", Expiry: time.Now().Add(time.Hour)})

	require.Equal(t, entries, inner.size(), "no fragments of the failed save are left")
	got := tokenCacheFor(testCacheConfig(inner, tokenCacheKeyring)).Token()
	require.NotNil(t, got)
	require.Equal(t, "old-refresh", got.RefreshToken)
	require.Equal(t, old.AccessToken, got.AccessToken)
}

// A token that could not be saved is kept in memory, and reading the keyring
// again does not replace it with the older one on disk (or with nothing).
func TestUnsavedTokenSurvivesAReload(t *testing.T) {
	inner := newFakeKeyring()
	armed := false
	kr := failingSetKeyring{fakeKeyring: inner, armed: &armed, fail: func(string) bool { return true }}
	var out strings.Builder
	cfg := testCacheConfig(kr, tokenCacheKeyring)
	cfg.out = &out
	cache := tokenCacheFor(cfg)

	older := &oauth2.Token{AccessToken: "a1", RefreshToken: "r1", Expiry: time.Now().Add(-time.Hour)}
	cache.Set(older) // saved: the keyring works
	require.Equal(t, 1, inner.size())

	armed = true // now the keyring refuses writes
	newer := &oauth2.Token{AccessToken: "a2", RefreshToken: "r2", Expiry: time.Now().Add(time.Hour)}
	cache.Set(newer)
	require.Contains(t, out.String(), "could not save the login token")

	got := cache.Reload()
	require.NotNil(t, got)
	require.Equal(t, "r2", got.RefreshToken, "the newer token is not replaced by the older one on disk")

	// Once the keyring works again, the next save persists it and reloading is normal.
	armed = false
	cache.Set(&oauth2.Token{AccessToken: "a3", RefreshToken: "r3", Expiry: time.Now().Add(time.Hour)})
	require.Equal(t, "r3", tokenCacheFor(testCacheConfig(inner, tokenCacheKeyring)).Token().RefreshToken)
	require.Equal(t, "r3", cache.Reload().RefreshToken)
}
