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
	"unicode/utf8"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	dbsqlctx "github.com/databricks/databricks-sql-go/driverctx"
)

// sessionParamQueryTags is the session parameter the query tags are sent as.
const sessionParamQueryTags = "QUERY_TAGS"

const (
	// The limits dbt-databricks enforces in QueryTagsUtils (MAX_TAGS and the
	// 128-character check on escaped values in dbt/adapters/databricks/utils.py).
	maxQueryTags           = 20
	maxQueryTagValueLength = 128
)

// queryTagName returns the tag name when key is a "databricks.query_tag.<name>"
// option, and whether key has that prefix at all.
func queryTagName(key string) (name string, isQueryTag bool) {
	return strings.CutPrefix(key, OptionQueryTagPrefix)
}

// escapedQueryTagValueLength is the length in characters of a tag value on the
// wire, where backslash, comma and colon are each escaped with a backslash.
func escapedQueryTagValueLength(value string) int {
	return utf8.RuneCountInString(value) + strings.Count(value, `\`) + strings.Count(value, ",") + strings.Count(value, ":")
}

// validateQueryTag checks one tag before it is stored, so that an invalid tag
// leaves the tags that are already set untouched. total is the number of tags
// that would be sent if the tag were accepted.
//
// The wire format is comma-separated "key:value" pairs. databricks-sql-go
// escapes backslash, colon and comma in values but only backslash in names, so a
// name containing a comma or colon cannot be represented and is rejected.
// Empty values are rejected too: databricks-sql-go would send them as a bare
// key, which this driver does not support.
func validateQueryTag(eh *driverbase.ErrorHelper, name, value string, total int) error {
	if strings.TrimSpace(name) == "" {
		return eh.Errorf(adbc.StatusInvalidArgument, "query tag name is required, e.g. '%steam'", OptionQueryTagPrefix)
	}
	if strings.ContainsAny(name, ",:") {
		return eh.Errorf(adbc.StatusInvalidArgument, "query tag name %q must not contain ',' or ':'", name)
	}
	if value == "" {
		return eh.Errorf(adbc.StatusInvalidArgument, "query tag %q must have a non-empty value", name)
	}
	if n := escapedQueryTagValueLength(value); n > maxQueryTagValueLength {
		return eh.Errorf(adbc.StatusInvalidArgument,
			"query tag %q value is %d characters after escaping, the maximum is %d", name, n, maxQueryTagValueLength)
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

// setQueryTag validates and stores one per-statement override. The limits apply
// to the tags the statement would send, i.e. connection defaults plus overrides.
func (s *statementImpl) setQueryTag(name, value string) error {
	candidate := maps.Clone(s.queryTags)
	if candidate == nil {
		candidate = make(map[string]string)
	}
	candidate[name] = value
	if err := validateQueryTag(&s.ErrorHelper, name, value, len(mergeQueryTags(s.connQueryTags(), candidate))); err != nil {
		return err
	}
	s.queryTags = candidate
	return nil
}

func (s *statementImpl) connQueryTags() map[string]string {
	if s.conn == nil {
		return nil
	}
	return s.conn.queryTags
}

// getQueryTag returns the statement override for name, falling back to the
// connection default.
func (s *statementImpl) getQueryTag(name string) (string, bool) {
	if v, ok := s.queryTags[name]; ok {
		return v, true
	}
	v, ok := s.connQueryTags()[name]
	return v, ok
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
