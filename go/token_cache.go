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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"golang.org/x/oauth2"
)

const (
	tokenCacheFileSuffix = "databricks_oauth_token_cache.json"
	tokenLeaseFileName   = "oauth_flow.lease"
)

// tokenCacheEntry is the on-disk form of a token. It matches the file the
// databricks-sql-go fork writes, so both read each other's cache.
type tokenCacheEntry struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	TokenType    string    `json:"token_type"`
	Expiry       time.Time `json:"expiry"`
}

// tokenCache keeps one browser-login token per workspace (and per custom
// client ID) in a directory, and a lease file that serialises logins across
// processes. Files are private to the user (0600, directory 0700).
type tokenCache struct {
	dir string
}

// defaultTokenCacheDirs lists where the cache may live, in order of
// preference: $XDG_CACHE_HOME/databricks/oauth, then ~/.cache/databricks/oauth
// (%LOCALAPPDATA%\databricks\oauth on Windows). The first one that can be
// created is used, as in the databricks-sql-go fork.
func defaultTokenCacheDirs() []string {
	var dirs []string
	if runtime.GOOS == "windows" {
		if dir := os.Getenv("LOCALAPPDATA"); dir != "" {
			dirs = append(dirs, filepath.Join(dir, "databricks", "oauth"))
		}
		return dirs
	}
	if dir := os.Getenv("XDG_CACHE_HOME"); dir != "" {
		dirs = append(dirs, filepath.Join(dir, "databricks", "oauth"))
	}
	if home := os.Getenv("HOME"); home != "" {
		dirs = append(dirs, filepath.Join(home, ".cache", "databricks", "oauth"))
	}
	return dirs
}

// newTokenCache opens the cache in dir, or in the first default directory that
// can be created when dir is empty.
func newTokenCache(dir string) (*tokenCache, error) {
	dirs := []string{dir}
	if dir == "" {
		dirs = defaultTokenCacheDirs()
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o700); err == nil {
			return &tokenCache{dir: d}, nil
		}
	}
	return nil, errors.New("unable to create the token cache directory")
}

// path is the cache file of one workspace. With the built-in client and the
// default scopes the file is named after the host alone, as in the fork. A
// custom client ID or scopes get their own file, because a refresh token only
// works with the client that obtained it and holds only the scopes it was
// issued. A host that is not a plain host name (path separators, "..") is
// hashed, so it cannot name a file outside the cache directory.
func (c *tokenCache) path(host, customClientID string, scopes []string) string {
	name := host
	if !safeHostName(host) {
		sum := sha256.Sum256([]byte(host))
		name = "host-" + hex.EncodeToString(sum[:8])
	}
	if customClientID != "" || len(scopes) > 0 {
		sorted := append([]string(nil), scopes...)
		sort.Strings(sorted)
		sum := sha256.Sum256([]byte(customClientID + "\n" + strings.Join(sorted, " ")))
		name += "_" + hex.EncodeToString(sum[:4])
	}
	return filepath.Join(c.dir, name+"_"+tokenCacheFileSuffix)
}

func safeHostName(host string) bool {
	if host == "" || strings.Contains(host, "..") {
		return false
	}
	for _, r := range host {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_', r == ':':
		default:
			return false
		}
	}
	return true
}

func (c *tokenCache) leasePath() string { return filepath.Join(c.dir, tokenLeaseFileName) }

// read returns the cached token, or nil when there is none or it is unreadable.
// The access token may be expired: the refresh token is still useful.
func (c *tokenCache) read(host, customClientID string, scopes []string) *oauth2.Token {
	data, err := os.ReadFile(c.path(host, customClientID, scopes))
	if err != nil {
		return nil
	}
	var e tokenCacheEntry
	if json.Unmarshal(data, &e) != nil || (e.AccessToken == "" && e.RefreshToken == "") {
		return nil
	}
	return &oauth2.Token{AccessToken: e.AccessToken, RefreshToken: e.RefreshToken, TokenType: e.TokenType, Expiry: e.Expiry}
}

// write stores the token atomically, so a reader never sees half a file.
func (c *tokenCache) write(host, customClientID string, scopes []string, t *oauth2.Token) error {
	data, err := json.MarshalIndent(tokenCacheEntry{
		AccessToken: t.AccessToken, RefreshToken: t.RefreshToken, TokenType: t.TokenType, Expiry: t.Expiry,
	}, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(c.dir, "*.tmp") // created 0600
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp) //nolint:errcheck
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, c.path(host, customClientID, scopes))
}

// acquireLease waits for the login lease for at least the browser timeout (the
// time another process may need to finish its login), and never less than
// leaseWaitTimeout.
func (c *tokenCache) acquireLease(ctx context.Context, browserTimeout time.Duration) (*fileLease, error) {
	l, err := acquireLease(ctx, c.leasePath(), leaseTTL, max(leaseWaitTimeout, browserTimeout+leaseTTL))
	if err != nil {
		return nil, fmt.Errorf("token cache: %w", err)
	}
	return l, nil
}
