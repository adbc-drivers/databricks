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
	"maps"
	"strings"
	"unicode/utf16"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	dbsqlctx "github.com/databricks/databricks-sql-go/driverctx"
)

const (
	// The limits Databricks documents for query tags: at most 20 tags per
	// statement, and keys and values of at most 128 characters. Databricks drops
	// a tag that breaks them and marks the statement with a tag_invalid or
	// tag_truncated tag, so the driver rejects them up front instead.
	maxQueryTags           = 20
	maxQueryTagLength      = 128
	invalidQueryTagKeyChar = ",:-/=."
)

// queryTagName returns the tag name when key is a "databricks.query.query_tag.<name>"
// option, and whether key has that prefix at all.
func queryTagName(key string) (name string, isQueryTag bool) {
	return strings.CutPrefix(key, OptionQueryTagPrefix)
}

// queryTagLength is the length checked against maxQueryTagLength: UTF-16 code units.
func queryTagLength(s string) int {
	n := 0
	for _, r := range s {
		n += utf16.RuneLen(r)
	}
	return n
}

func requireQueryTagName(eh *driverbase.ErrorHelper, name string) error {
	if strings.TrimSpace(name) == "" {
		return eh.Errorf(adbc.StatusInvalidArgument, "query tag name is required, e.g. '%steam'", OptionQueryTagPrefix)
	}
	return nil
}

// validateQueryTag checks one tag before it is stored, so that an invalid tag
// leaves the tags that are already set untouched. total is the number of tags
// that would be sent if the tag were accepted. An empty value is allowed:
// The docs show key-only tags, so an empty value is sent as one.
func validateQueryTag(eh *driverbase.ErrorHelper, name, value string, total int) error {
	if err := requireQueryTagName(eh, name); err != nil {
		return err
	}
	if i := strings.IndexAny(name, invalidQueryTagKeyChar); i >= 0 {
		return eh.Errorf(adbc.StatusInvalidArgument,
			"query tag name %q must not contain any of %q", name, invalidQueryTagKeyChar)
	}
	if n := queryTagLength(name); n > maxQueryTagLength {
		return eh.Errorf(adbc.StatusInvalidArgument,
			"query tag name %q is %d characters, the maximum is %d", name, n, maxQueryTagLength)
	}
	if n := queryTagLength(value); n > maxQueryTagLength {
		return eh.Errorf(adbc.StatusInvalidArgument,
			"query tag %q value is %d characters, the maximum is %d", name, n, maxQueryTagLength)
	}
	if total > maxQueryTags {
		return eh.Errorf(adbc.StatusInvalidArgument,
			"cannot set query tag %q: at most %d query tags are allowed", name, maxQueryTags)
	}
	return nil
}

// queryTagNotSet is the error for reading a query tag that is not set.
func queryTagNotSet(eh *driverbase.ErrorHelper, name string) error {
	return eh.Errorf(adbc.StatusNotFound, "query tag not set: %s", name)
}

// mergeQueryTags layers overrides on top of defaults: an override with the same
// name replaces the default. It returns nil when there are no tags and never
// aliases its inputs.
func mergeQueryTags(defaults, overrides map[string]string) map[string]string {
	if len(defaults)+len(overrides) == 0 {
		return nil
	}
	merged := make(map[string]string, len(defaults)+len(overrides))
	maps.Copy(merged, defaults)
	maps.Copy(merged, overrides)
	return merged
}

// setQueryTag validates and stores one database-level default tag.
func (d *databaseImpl) setQueryTag(name, value string) error {
	total := len(d.queryTags)
	if _, exists := d.queryTags[name]; !exists {
		total++
	}
	if err := validateQueryTag(&d.ErrorHelper, name, value, total); err != nil {
		return err
	}
	if d.queryTags == nil {
		d.queryTags = make(map[string]string)
	}
	d.queryTags[name] = value
	return nil
}

// setQueryTag validates and stores one connection-level tag. Statements read the
// tags under the same lock when they run.
func (c *connectionImpl) setQueryTag(name, value string) error {
	c.queryTagsMu.Lock()
	defer c.queryTagsMu.Unlock()
	total := len(c.queryTags)
	if _, exists := c.queryTags[name]; !exists {
		total++
	}
	if err := validateQueryTag(&c.ErrorHelper, name, value, total); err != nil {
		return err
	}
	if c.queryTags == nil {
		c.queryTags = make(map[string]string)
	}
	c.queryTags[name] = value
	return nil
}

// setQueryTag validates and stores one per-statement override. The limits apply
// to the tags the statement would send. A statement is not shared between
// goroutines, so it needs no lock.
func (s *statementImpl) setQueryTag(name, value string) error {
	if err := validateQueryTag(&s.ErrorHelper, name, value, s.conn.tagCount(s.queryTags, name)); err != nil {
		return err
	}
	if s.queryTags == nil {
		s.queryTags = make(map[string]string)
	}
	s.queryTags[name] = value
	return nil
}

// tag returns the connection's tag for name.
func (c *connectionImpl) tag(name string) (string, bool) {
	c.queryTagsMu.RLock()
	defer c.queryTagsMu.RUnlock()
	v, ok := c.queryTags[name]
	return v, ok
}

// tagCount is the number of tags a statement would send if it also set name:
// the connection's tags and the statement's overrides, counted by name.
func (c *connectionImpl) tagCount(overrides map[string]string, name string) int {
	c.queryTagsMu.RLock()
	defer c.queryTagsMu.RUnlock()
	n := len(c.queryTags)
	for k := range overrides {
		if _, ok := c.queryTags[k]; !ok {
			n++
		}
	}
	if _, known := c.queryTags[name]; !known {
		if _, overridden := overrides[name]; !overridden {
			n++
		}
	}
	return n
}

// mergedTags is the connection's tags with overrides on top, as a new map, or nil
// when there are none.
func (c *connectionImpl) mergedTags(overrides map[string]string) map[string]string {
	c.queryTagsMu.RLock()
	defer c.queryTagsMu.RUnlock()
	return mergeQueryTags(c.queryTags, overrides)
}

func (c *connectionImpl) getQueryTag(name string) (string, error) {
	if v, ok := c.tag(name); ok {
		return v, nil
	}
	return "", queryTagNotSet(&c.ErrorHelper, name)
}

// getQueryTag returns the statement override for name, falling back to the
// connection's tag.
func (s *statementImpl) getQueryTag(name string) (string, bool) {
	if v, ok := s.queryTags[name]; ok {
		return v, true
	}
	return s.conn.tag(name)
}

// effectiveQueryTags is what this statement sends: the connection's tags with the
// statement's overrides applied.
func (s *statementImpl) effectiveQueryTags() map[string]string {
	return s.conn.mergedTags(s.queryTags)
}

func withQueryTags(ctx context.Context, tags map[string]string) context.Context {
	if len(tags) == 0 {
		return ctx
	}
	return dbsqlctx.NewContextWithQueryTags(ctx, tags)
}
