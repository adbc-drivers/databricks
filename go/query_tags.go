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
	"slices"
	"strings"
	"unicode/utf8"

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

// parseQueryTags parses "key1:value1,key2:value2". Pairs are separated by
// commas and a key is separated from its value by the first colon. A backslash
// makes the next character literal, so values can contain "\,", "\:" and "\\".
// A pair without a colon is a key with an empty value. Empty pairs are skipped
// and a repeated key keeps its last value.
func parseQueryTags(eh *driverbase.ErrorHelper, raw string) (map[string]string, error) {
	tags := make(map[string]string)
	var key, value strings.Builder
	inKey := true
	finish := func() error {
		k, v := strings.TrimSpace(key.String()), strings.TrimSpace(value.String())
		key.Reset()
		value.Reset()
		inKey = true
		if k == "" && v == "" {
			return nil
		}
		if err := validateQueryTag(eh, k, v); err != nil {
			return err
		}
		tags[k] = v
		return nil
	}

	runes := []rune(raw)
	for i := 0; i < len(runes); i++ {
		cur := &value
		if inKey {
			cur = &key
		}
		switch r := runes[i]; {
		case r == '\\' && i+1 < len(runes):
			i++
			cur.WriteRune(runes[i])
		case r == ',':
			if err := finish(); err != nil {
				return nil, err
			}
		case r == ':' && inKey:
			inKey = false
		default:
			cur.WriteRune(r)
		}
	}
	if err := finish(); err != nil {
		return nil, err
	}
	return tags, nil
}

// validateQueryTag checks one tag against the limits Databricks documents.
func validateQueryTag(eh *driverbase.ErrorHelper, key, value string) error {
	if key == "" {
		return eh.Errorf(adbc.StatusInvalidArgument, "query tag with value %q has no key", value)
	}
	if strings.ContainsAny(key, invalidQueryTagKeyChar) {
		return eh.Errorf(adbc.StatusInvalidArgument,
			"query tag key %q must not contain any of %q", key, invalidQueryTagKeyChar)
	}
	if n := utf8.RuneCountInString(key); n > maxQueryTagLength {
		return eh.Errorf(adbc.StatusInvalidArgument,
			"query tag key %q is %d characters, the maximum is %d", key, n, maxQueryTagLength)
	}
	if n := utf8.RuneCountInString(value); n > maxQueryTagLength {
		return eh.Errorf(adbc.StatusInvalidArgument,
			"query tag %q value is %d characters, the maximum is %d", key, n, maxQueryTagLength)
	}
	return nil
}

// checkQueryTagCount rejects more than the maximum number of tags that would
// be sent with one statement.
func checkQueryTagCount(eh *driverbase.ErrorHelper, total int) error {
	if total > maxQueryTags {
		return eh.Errorf(adbc.StatusInvalidArgument,
			"%d query tags would be sent, at most %d are allowed", total, maxQueryTags)
	}
	return nil
}

// formatQueryTags is the inverse of parseQueryTags, with the tags sorted by key.
func formatQueryTags(tags map[string]string) string {
	escape := strings.NewReplacer(`\`, `\\`, ",", `\,`, ":", `\:`)
	pairs := make([]string, 0, len(tags))
	for _, k := range slices.Sorted(maps.Keys(tags)) {
		pairs = append(pairs, k+":"+escape.Replace(tags[k]))
	}
	return strings.Join(pairs, ",")
}

// queryTagsNotSet is the error for reading query tags when none are set.
func queryTagsNotSet(eh *driverbase.ErrorHelper) error {
	return eh.Errorf(adbc.StatusNotFound, "query tags not set: %s", OptionQueryTags)
}

// mergeQueryTags layers overrides on top of defaults: an override with the same
// key replaces the default. It returns nil when there are no tags and never
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

// setQueryTags parses raw and replaces the database's default tags with it. On
// an error the tags already set are left unchanged.
func (d *databaseImpl) setQueryTags(raw string) error {
	tags, err := parseQueryTags(&d.ErrorHelper, raw)
	if err != nil {
		return err
	}
	if err := checkQueryTagCount(&d.ErrorHelper, len(tags)); err != nil {
		return err
	}
	d.queryTags = tags
	return nil
}

// getQueryTags returns the default tags in the option format.
func (d *databaseImpl) getQueryTags() (string, error) {
	if len(d.queryTags) == 0 {
		return "", queryTagsNotSet(&d.ErrorHelper)
	}
	return formatQueryTags(d.queryTags), nil
}

// setQueryTags parses raw and replaces this statement's overrides with it. The
// limit applies to the tags the statement would send, i.e. the connection
// defaults plus the overrides. On an error the overrides are left unchanged.
func (s *statementImpl) setQueryTags(raw string) error {
	tags, err := parseQueryTags(&s.ErrorHelper, raw)
	if err != nil {
		return err
	}
	if err := checkQueryTagCount(&s.ErrorHelper, len(mergeQueryTags(s.connQueryTags(), tags))); err != nil {
		return err
	}
	s.queryTags = tags
	return nil
}

func (s *statementImpl) connQueryTags() map[string]string {
	if s.conn == nil {
		return nil
	}
	return s.conn.queryTags
}

// getQueryTags returns the tags this statement sends in the option format.
func (s *statementImpl) getQueryTags() (string, error) {
	tags := s.effectiveQueryTags()
	if len(tags) == 0 {
		return "", queryTagsNotSet(&s.ErrorHelper)
	}
	return formatQueryTags(tags), nil
}

// effectiveQueryTags is what this statement sends: the connection defaults with
// the statement's overrides applied.
func (s *statementImpl) effectiveQueryTags() map[string]string {
	return mergeQueryTags(s.connQueryTags(), s.queryTags)
}

func withQueryTags(ctx context.Context, tags map[string]string) context.Context {
	if len(tags) == 0 {
		return ctx
	}
	return dbsqlctx.NewContextWithQueryTags(ctx, tags)
}
