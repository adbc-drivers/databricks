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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zalando/go-keyring"
	"golang.org/x/oauth2"
)

const (
	// tokenCacheKeyring also keeps the login token in the OS keyring, so that
	// later processes reuse it. This is the default.
	tokenCacheKeyring = "keyring"
	// tokenCacheMemory keeps the token in memory only.
	tokenCacheMemory = "memory"

	keyringService = "adbc-driver-databricks"
	// keyringChunkSize keeps each keyring entry under the Windows Credential
	// Manager limit of 2560 bytes and the macOS limit of about 3000 bytes.
	keyringChunkSize    = 1000
	keyringChunksPrefix = "adbc-databricks-chunks:"
)

// keyringBackend is the subset of the go-keyring API the token cache uses.
type keyringBackend interface {
	Set(service, user, password string) error
	Get(service, user string) (string, error)
	Delete(service, user string) error
}

type osKeyring struct{}

func (osKeyring) Set(service, user, password string) error {
	return keyring.Set(service, user, password)
}
func (osKeyring) Get(service, user string) (string, error) { return keyring.Get(service, user) }
func (osKeyring) Delete(service, user string) error        { return keyring.Delete(service, user) }

// tokenCacheEntry is the stored form of a login token.
type tokenCacheEntry struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	TokenType    string    `json:"token_type"`
	Expiry       time.Time `json:"expiry"`
}

// tokenCache keeps the login token of one workspace (and client and scope
// list). The token is kept in memory, and written through to the keyring unless
// that is nil. Every database in the process with the same key shares one
// cache, so when one of them logs in the others reuse its token.
type tokenCache struct {
	key     string
	keyring keyringBackend
	out     io.Writer

	mu     sync.Mutex
	token  *oauth2.Token
	loaded bool
	warned bool
	// unsaved is set when the newest token could not be written to the keyring.
	// Until it is saved, reading the keyring again must not replace it: the copy
	// on disk is older, or missing.
	unsaved bool
}

// sharedTokenCaches holds a cache per mode and key. The memory cache is kept
// apart from the keyring one, so it never writes to the keyring.
var sharedTokenCaches sync.Map

// tokenCacheKey identifies the workspace, client and scopes a token belongs to.
func tokenCacheKey(host, clientID string, scopes []string) string {
	sorted := append([]string(nil), scopes...)
	sort.Strings(sorted)
	return host + "|" + clientID + "|" + strings.Join(sorted, " ")
}

// tokenCacheFor returns the cache of cfg. A keyring in cfg is for tests and
// gives a cache of its own.
func tokenCacheFor(cfg browserLoginConfig) *tokenCache {
	mode := cfg.tokenCache
	if mode == "" {
		mode = tokenCacheKeyring
	}
	key := tokenCacheKey(cfg.host, cfg.clientID, cfg.scopes)
	var backend keyringBackend
	if mode == tokenCacheKeyring {
		backend = cfg.keyring
		if backend == nil {
			backend = osKeyring{}
		}
	}
	if cfg.keyring != nil {
		return &tokenCache{key: key, keyring: backend, out: cfg.out}
	}
	shared, _ := sharedTokenCaches.LoadOrStore(mode+"|"+key, &tokenCache{key: key, keyring: backend, out: cfg.out})
	return shared.(*tokenCache)
}

// Token returns the cached token, or nil. The access token may be expired: the
// refresh token is still useful.
func (c *tokenCache) Token() *oauth2.Token {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded {
		c.loaded = true
		if c.keyring != nil {
			token, err := c.load()
			if err != nil && !errors.Is(err, keyring.ErrNotFound) {
				c.warn("could not read the login token from the keyring: %v", err)
			}
			c.token = token
		}
	}
	if c.token == nil {
		return nil
	}
	token := *c.token
	return &token
}

// Reload re-reads the keyring, so a token that another process refreshed (and
// whose refresh token it rotated) is used instead of this process's old copy.
// If the keyring cannot be read, the token this process has is kept.
func (c *tokenCache) Reload() *oauth2.Token {
	c.mu.Lock()
	c.reloadLocked()
	c.mu.Unlock()
	return c.Token()
}

func (c *tokenCache) reloadLocked() {
	c.loaded = true
	if c.keyring == nil || (c.unsaved && c.token != nil) {
		return
	}
	token, err := c.load()
	switch {
	case err == nil:
		c.token = token
	case errors.Is(err, keyring.ErrNotFound):
		c.token = nil
	default:
		c.warn("could not read the login token from the keyring: %v", err)
	}
}

// Set replaces the cached token. It is not written again when it has not
// changed.
func (c *tokenCache) Set(token *oauth2.Token) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != nil && c.token.AccessToken == token.AccessToken && c.token.RefreshToken == token.RefreshToken &&
		c.token.TokenType == token.TokenType && c.token.Expiry.Equal(token.Expiry) {
		return
	}
	stored := *token
	c.token = &stored
	c.loaded = true
	if c.keyring == nil {
		return
	}
	if err := c.store(token); err != nil {
		c.unsaved = true
		c.warn("could not save the login token to the keyring; it will only be reused within this process: %v", err)
		return
	}
	c.unsaved = false
}

// Clear removes the token, for example when the identity provider rejects it.
func (c *tokenCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clearLocked()
}

// ClearIfSame removes the token only if its refresh token is still refreshToken,
// the one the identity provider rejected. If another process has stored a newer
// token, that one is kept and false is returned.
func (c *tokenCache) ClearIfSame(refreshToken string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reloadLocked()
	if c.token != nil && c.token.RefreshToken != refreshToken {
		return false
	}
	c.clearLocked()
	return true
}

func (c *tokenCache) clearLocked() {
	c.token = nil
	c.loaded = true
	c.unsaved = false
	if c.keyring == nil {
		return
	}
	if err := c.delete(); err != nil {
		c.warn("could not remove the login token from the keyring: %v", err)
	}
}

// warn writes a message once per cache; a keyring that does not work would
// otherwise repeat it on every request.
func (c *tokenCache) warn(format string, args ...any) {
	if c.warned || c.out == nil {
		return
	}
	c.warned = true
	_, _ = fmt.Fprintf(c.out, "Warning: "+format+"\n", args...)
}

// store saves the token under the key. A token too big for one keyring entry
// is saved in chunks named after a new generation, and the key then holds the
// generation and the number of chunks. Because the chunks are new and the key is
// written last, another process reads either the old token or the new one,
// never a mix. Big values are not tried in one entry first: on macOS, go-keyring
// leaves the security process it started running when it rejects them. The
// chunks of the token that was replaced are removed afterwards.
func (c *tokenCache) store(token *oauth2.Token) error {
	data, err := json.Marshal(tokenCacheEntry{
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
		TokenType:    token.TokenType,
		Expiry:       token.Expiry,
	})
	if err != nil {
		return err
	}
	old, hadOld := c.manifest()
	value := string(data)
	if len(value) <= keyringChunkSize {
		if err := c.keyring.Set(keyringService, c.key, value); err != nil {
			return err
		}
		return c.removeChunks(old, hadOld)
	}
	gen, err := newGeneration()
	if err != nil {
		return err
	}
	n := (len(value) + keyringChunkSize - 1) / keyringChunkSize
	fresh := manifest{gen: gen, n: n}
	for i := range n {
		chunk := value[i*keyringChunkSize : min((i+1)*keyringChunkSize, len(value))]
		if err := c.keyring.Set(keyringService, chunkKey(c.key, gen, i), chunk); err != nil {
			_ = c.removeChunks(fresh, true) // do not leave fragments behind
			return err
		}
	}
	if err := c.keyring.Set(keyringService, c.key, fresh.String()); err != nil {
		_ = c.removeChunks(fresh, true)
		return err
	}
	return c.removeChunks(old, hadOld)
}

func (c *tokenCache) load() (*oauth2.Token, error) {
	value, err := c.keyring.Get(keyringService, c.key)
	if err != nil {
		return nil, err
	}
	m, chunked, err := parseManifest(value)
	if err != nil {
		return nil, err
	}
	if chunked {
		var b strings.Builder
		for i := range m.n {
			chunk, err := c.keyring.Get(keyringService, chunkKey(c.key, m.gen, i))
			if err != nil {
				// Not wrapped: a missing chunk is a corrupt token, which is
				// reported, rather than no token.
				return nil, fmt.Errorf("reading token chunk %d of %d: %v", i+1, m.n, err)
			}
			b.WriteString(chunk)
		}
		value = b.String()
	}
	var entry tokenCacheEntry
	if err := json.Unmarshal([]byte(value), &entry); err != nil {
		return nil, fmt.Errorf("the stored login token is not valid: %v", err)
	}
	if entry.AccessToken == "" && entry.RefreshToken == "" {
		return nil, nil
	}
	return &oauth2.Token{
		AccessToken:  entry.AccessToken,
		RefreshToken: entry.RefreshToken,
		TokenType:    entry.TokenType,
		Expiry:       entry.Expiry,
	}, nil
}

// delete removes the token and its chunks.
func (c *tokenCache) delete() error {
	old, hadOld := c.manifest()
	chunkErr := c.removeChunks(old, hadOld)
	if err := c.keyring.Delete(keyringService, c.key); err != nil && !errors.Is(err, keyring.ErrNotFound) {
		return err
	}
	return chunkErr
}

// manifest is what the key holds for a token saved in chunks.
type manifest struct {
	gen string
	n   int
}

func (m manifest) String() string {
	return keyringChunksPrefix + m.gen + ":" + strconv.Itoa(m.n)
}

// manifest reads the chunk manifest under the key, if there is one.
func (c *tokenCache) manifest() (manifest, bool) {
	value, err := c.keyring.Get(keyringService, c.key)
	if err != nil {
		return manifest{}, false
	}
	m, chunked, err := parseManifest(value)
	return m, err == nil && chunked
}

// removeChunks removes every chunk of m, and carries on past one it cannot
// remove so that the others are not left behind. It returns the first error
// other than a missing chunk.
func (c *tokenCache) removeChunks(m manifest, ok bool) error {
	if !ok {
		return nil
	}
	var first error
	for i := range m.n {
		err := c.keyring.Delete(keyringService, chunkKey(c.key, m.gen, i))
		if err != nil && !errors.Is(err, keyring.ErrNotFound) && first == nil {
			first = err
		}
	}
	return first
}

// chunkKey is the keyring entry of chunk i of a generation. It has its own
// prefix, so it can never be the key of another cache.
func chunkKey(key, gen string, i int) string {
	return "chunk#" + gen + "#" + strconv.Itoa(i) + "#" + key
}

func newGeneration() (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// parseManifest reads a key's value as a chunk manifest. chunked is false when
// the value is the token itself.
func parseManifest(value string) (m manifest, chunked bool, err error) {
	rest, ok := strings.CutPrefix(value, keyringChunksPrefix)
	if !ok {
		return manifest{}, false, nil
	}
	gen, count, found := strings.Cut(rest, ":")
	n, convErr := strconv.Atoi(count)
	if !found || gen == "" || convErr != nil || n < 1 {
		return manifest{}, true, fmt.Errorf("invalid token chunk manifest %q", rest)
	}
	return manifest{gen: gen, n: n}, true, nil
}
