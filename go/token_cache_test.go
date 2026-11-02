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
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestTokenCacheRoundTrip(t *testing.T) {
	c, err := newTokenCache(filepath.Join(t.TempDir(), "nested", "oauth"))
	require.NoError(t, err)
	require.Nil(t, c.read("h", "", nil))

	tok := &oauth2.Token{AccessToken: "a", RefreshToken: "r", TokenType: "Bearer", Expiry: time.Now().Add(time.Hour).Truncate(time.Second)}
	require.NoError(t, c.write("h", "", nil, tok))
	got := c.read("h", "", nil)
	require.NotNil(t, got)
	require.Equal(t, "a", got.AccessToken)
	require.Equal(t, "r", got.RefreshToken)
	require.True(t, tok.Expiry.Equal(got.Expiry))

	if runtime.GOOS != "windows" {
		info, err := os.Stat(c.path("h", "", nil))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	// The built-in client keeps the fork's file name; a custom client gets its own.
	require.Equal(t, "h_databricks_oauth_token_cache.json", filepath.Base(c.path("h", "", nil)))
	require.NotEqual(t, c.path("h", "", nil), c.path("h", "mine", nil))
	require.Nil(t, c.read("h", "mine", nil))
	require.Nil(t, c.read("other-host", "", nil))
}

func TestTokenCacheIgnoresUnreadableFiles(t *testing.T) {
	c, err := newTokenCache(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(c.path("h", "", nil), []byte("{not json"), 0o600))
	require.Nil(t, c.read("h", "", nil))
	require.NoError(t, os.WriteFile(c.path("h", "", nil), []byte("{}"), 0o600))
	require.Nil(t, c.read("h", "", nil))
}

func TestDefaultTokenCacheDirs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix layout")
	}
	t.Setenv("XDG_CACHE_HOME", "/xdg")
	t.Setenv("HOME", "/home/u")
	require.Equal(t, []string{"/xdg/databricks/oauth", "/home/u/.cache/databricks/oauth"}, defaultTokenCacheDirs())
	t.Setenv("XDG_CACHE_HOME", "")
	require.Equal(t, []string{"/home/u/.cache/databricks/oauth"}, defaultTokenCacheDirs())
	t.Setenv("HOME", "")
	require.Empty(t, defaultTokenCacheDirs())
}

// When the XDG directory cannot be created the cache falls back to ~/.cache.
func TestTokenCacheFallsBackToHome(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix layout")
	}
	blocker := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))
	home := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", blocker)
	t.Setenv("HOME", home)
	c, err := newTokenCache("")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(home, ".cache", "databricks", "oauth"), c.dir)
}

func TestTokenCachePathIsSafe(t *testing.T) {
	c, err := newTokenCache(t.TempDir())
	require.NoError(t, err)
	for _, host := range []string{"../../etc/passwd", "a/b", `a\b`, "..", "", "h\x00"} {
		p := c.path(host, "", nil)
		require.Equal(t, c.dir, filepath.Dir(p), host)
	}
	require.Equal(t, filepath.Join(c.dir, "my-ws.cloud.databricks.com_databricks_oauth_token_cache.json"), c.path("my-ws.cloud.databricks.com", "", nil))
}

func TestTokenCachePathDependsOnScopes(t *testing.T) {
	c, err := newTokenCache(t.TempDir())
	require.NoError(t, err)
	plain := c.path("h", "", nil)
	scoped := c.path("h", "", []string{"all-apis"})
	require.NotEqual(t, plain, scoped)
	require.Equal(t, scoped, c.path("h", "", []string{"all-apis"}))
	require.Equal(t, c.path("h", "x", []string{"a", "b"}), c.path("h", "x", []string{"b", "a"}))
}

func TestLeaseIsExclusiveUntilReleased(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.lease")
	ctx := context.Background()
	first, err := acquireLease(ctx, path, time.Minute, time.Second)
	require.NoError(t, err)

	_, err = acquireLease(ctx, path, time.Minute, 300*time.Millisecond)
	require.ErrorContains(t, err, "timed out")

	acquired := make(chan *fileLease, 1)
	go func() {
		l, err := acquireLease(ctx, path, time.Minute, 5*time.Second)
		if err == nil {
			acquired <- l
		}
	}()
	time.Sleep(200 * time.Millisecond)
	select {
	case <-acquired:
		t.Fatal("second lease acquired while the first is held")
	default:
	}
	first.release()
	select {
	case l := <-acquired:
		l.release()
	case <-time.After(5 * time.Second):
		t.Fatal("second lease not acquired after release")
	}
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr))
}

func TestLeaseExpiryCorruptionAndRenewal(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "x.lease")

	// An expired lease (crashed holder) is taken over, and the old holder knows.
	old, err := acquireLease(ctx, path, 50*time.Millisecond, time.Second)
	require.NoError(t, err)
	time.Sleep(150 * time.Millisecond)
	next, err := acquireLease(ctx, path, time.Minute, time.Second)
	require.NoError(t, err)
	require.Error(t, old.renew(time.Minute))
	old.release() // must not free the new holder's lease
	owner, _ := readLease(path)
	require.Equal(t, next.id, owner)

	// Renewal pushes the expiry out.
	_, before := readLease(path)
	require.NoError(t, next.renew(time.Hour))
	_, after := readLease(path)
	require.True(t, after.After(before))
	next.release()

	// A corrupt lease file counts as no lease.
	require.NoError(t, os.WriteFile(path, []byte("garbage"), 0o600))
	l, err := acquireLease(ctx, path, time.Minute, time.Second)
	require.NoError(t, err)
	l.release()
}

func TestLeaseWaitStopsWhenContextEnds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.lease")
	held, err := acquireLease(context.Background(), path, time.Minute, time.Second)
	require.NoError(t, err)
	defer held.release()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = acquireLease(ctx, path, time.Minute, time.Minute)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestLeaseKeepAliveRenews(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.lease")
	l, err := acquireLease(context.Background(), path, 150*time.Millisecond, time.Second)
	require.NoError(t, err)
	defer l.release()
	_, before := readLease(path)
	stop := l.keepAlive(150*time.Millisecond, func() {})
	time.Sleep(400 * time.Millisecond)
	stop()
	_, after := readLease(path)
	require.True(t, after.After(before))
}

// A second process finds the first one's token in the cache instead of
// opening the browser.
func TestBrowserLoginUsesCacheAcrossProcesses(t *testing.T) {
	h := newBrowserHarness(t)
	_, err := authHeader(t, newBrowserAuthenticator(h.cfg))
	require.NoError(t, err)

	hdr, err := authHeader(t, newBrowserAuthenticator(h.cfg))
	require.NoError(t, err)
	require.Equal(t, "Bearer access-1", hdr)
	require.Equal(t, 1, h.openCount())
	grants, _ := h.idp.snapshot()
	require.Equal(t, []string{"authorization_code"}, grants)
}

func TestBrowserLoginRefreshesStaleCachedToken(t *testing.T) {
	h := newBrowserHarness(t)
	h.idp.expiresIn = 1
	_, err := authHeader(t, newBrowserAuthenticator(h.cfg))
	require.NoError(t, err)

	hdr, err := authHeader(t, newBrowserAuthenticator(h.cfg))
	require.NoError(t, err)
	require.Equal(t, "Bearer access-2", hdr)
	require.Equal(t, 1, h.openCount())
	grants, _ := h.idp.snapshot()
	require.Equal(t, []string{"authorization_code", "refresh_token"}, grants)

	// The refreshed token was written back for the next process.
	cache, err := newTokenCache(h.cfg.tokenCacheDir)
	require.NoError(t, err)
	require.Equal(t, "access-2", cache.read(h.cfg.host, "", nil).AccessToken)
}

func TestBrowserLoginSignsInWhenCachedRefreshIsRejected(t *testing.T) {
	h := newBrowserHarness(t)
	h.idp.expiresIn = 1
	_, err := authHeader(t, newBrowserAuthenticator(h.cfg))
	require.NoError(t, err)
	h.idp.mu.Lock()
	h.idp.rejectRefresh = true
	h.idp.mu.Unlock()

	hdr, err := authHeader(t, newBrowserAuthenticator(h.cfg))
	require.NoError(t, err)
	require.Equal(t, "Bearer access-2", hdr)
	require.Equal(t, 2, h.openCount())
}

func TestBrowserLoginCacheIsPerClientID(t *testing.T) {
	h := newBrowserHarness(t)
	_, err := authHeader(t, newBrowserAuthenticator(h.cfg))
	require.NoError(t, err)

	other := h.cfg
	other.clientID = "dbt-databricks"
	_, err = authHeader(t, newBrowserAuthenticator(other))
	require.NoError(t, err)
	require.Equal(t, 2, h.openCount(), "a custom client must not reuse the built-in client's token")
}

// Concurrent processes share one login: the lease lets one open the browser
// and the rest take the token it cached.
func TestBrowserLoginLeaseSerialisesConcurrentLogins(t *testing.T) {
	h := newBrowserHarness(t)
	const n = 4
	headers := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := newTestRequest()
			errs[i] = newBrowserAuthenticator(h.cfg).Authenticate(req)
			headers[i] = req.Header.Get("Authorization")
		}()
	}
	wg.Wait()
	for i := range n {
		require.NoError(t, errs[i])
		require.Equal(t, "Bearer access-1", headers[i])
	}
	require.Equal(t, 1, h.openCount())
}

func TestBrowserLoginWithoutCacheDirStillWorks(t *testing.T) {
	h := newBrowserHarness(t)
	blocker := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))
	h.cfg.tokenCacheDir = filepath.Join(blocker, "sub") // cannot be created
	a := newBrowserAuthenticator(h.cfg)
	require.Nil(t, a.cache)
	hdr, err := authHeader(t, a)
	require.NoError(t, err)
	require.Equal(t, "Bearer access-1", hdr)
}

// A lease file as the databricks-sql-go fork writes it (v1.14.2 lease.go:
// id, ctime and expiry in Unix milliseconds, each ending in CRLF).
func TestLeaseReadsForkFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oauth_flow.lease")
	expiry := time.Now().Add(time.Minute).UnixMilli()
	fixture := fmt.Sprintf("fork-lease-id\r\n%d\r\n%d\r\n", time.Now().UnixMilli(), expiry)
	require.NoError(t, os.WriteFile(path, []byte(fixture), 0o600))

	id, got := readLease(path)
	require.Equal(t, "fork-lease-id", id)
	require.Equal(t, expiry, got.UnixMilli())

	// A live fork lease blocks us.
	_, err := acquireLease(context.Background(), path, time.Minute, 300*time.Millisecond)
	require.ErrorContains(t, err, "timed out")

	// An expired one is taken over.
	old := fmt.Sprintf("fork-lease-id\r\n%d\r\n%d\r\n", time.Now().Add(-time.Hour).UnixMilli(), time.Now().Add(-time.Minute).UnixMilli())
	require.NoError(t, os.WriteFile(path, []byte(old), 0o600))
	l, err := acquireLease(context.Background(), path, time.Minute, time.Second)
	require.NoError(t, err)

	// What we write has the fork's shape, so the fork can read it.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	parts := strings.Split(string(data), "\r\n")
	require.Len(t, parts, 4)
	require.Equal(t, l.id, parts[0])
	require.Equal(t, "", parts[3])
	l.release()
}

func TestLeaseRenewIsAtomicAndReportsLoss(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.lease")
	l, err := acquireLease(context.Background(), path, time.Minute, time.Second)
	require.NoError(t, err)
	require.NoError(t, l.renew(time.Minute))
	entries, _ := os.ReadDir(filepath.Dir(path))
	require.Len(t, entries, 1, "no temp files left behind")

	// Another process takes over: renew fails and keepAlive reports the loss.
	require.NoError(t, writeLease(path, "someone-else", time.Now().Add(time.Minute)))
	require.Error(t, l.renew(time.Minute))
	lost := make(chan struct{})
	stop := l.keepAlive(90*time.Millisecond, func() { close(lost) })
	defer stop()
	select {
	case <-lost:
	case <-time.After(2 * time.Second):
		t.Fatal("lost lease not reported")
	}
}

func TestBrowserLoginCachedRefreshFailureDoesNotOpenBrowser(t *testing.T) {
	h := newBrowserHarness(t)
	h.idp.expiresIn = 1
	_, err := authHeader(t, newBrowserAuthenticator(h.cfg))
	require.NoError(t, err)
	h.idp.mu.Lock()
	h.idp.failRefreshStatus = http.StatusServiceUnavailable
	h.idp.mu.Unlock()

	_, err = authHeader(t, newBrowserAuthenticator(h.cfg))
	require.ErrorContains(t, err, "unable to refresh the cached login token")
	require.Equal(t, 1, h.openCount())
}

func TestBrowserLoginCacheIsPerScopes(t *testing.T) {
	h := newBrowserHarness(t)
	_, err := authHeader(t, newBrowserAuthenticator(h.cfg))
	require.NoError(t, err)
	other := h.cfg
	other.scopes = []string{"all-apis"}
	_, err = authHeader(t, newBrowserAuthenticator(other))
	require.NoError(t, err)
	require.Equal(t, 2, h.openCount())
}
