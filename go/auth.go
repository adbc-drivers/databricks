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
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	dbsql "github.com/databricks/databricks-sql-go"
)

// authField is an option a login method needs, read from the database.
type authField struct {
	option string
	label  string
	value  func(d *databaseImpl) string
}

// loginMethod is one way to authenticate: the options it requires and how it
// configures the SQL driver. Adding a method means adding one entry to
// loginMethods, keyed by its auth_type value.
type loginMethod struct {
	required []authField
	build    func(d *databaseImpl) dbsql.ConnOption
}

var loginMethods = map[string]loginMethod{
	OptionValueAuthTypePAT: {
		required: []authField{
			{OptionAccessToken, "access token", func(d *databaseImpl) string { return d.accessToken }},
		},
		build: func(d *databaseImpl) dbsql.ConnOption {
			return dbsql.WithAccessToken(d.accessToken)
		},
	},
	OptionValueAuthTypeOAuthM2M: {
		required: []authField{
			{OptionOAuthClientID, "client ID", func(d *databaseImpl) string { return d.oauthClientID }},
			{OptionOAuthClientSecret, "client secret", func(d *databaseImpl) string { return d.oauthClientSecret }},
		},
		build: func(d *databaseImpl) dbsql.ConnOption {
			return dbsql.WithClientCredentials(d.oauthClientID, d.oauthClientSecret)
		},
	},
}

// resolveLoginMethod picks the login method for the configured options and
// checks that everything it requires is set. It returns the method's auth_type
// name along with the method.
//
// When databricks.auth_type is set, it names the method. When it is not, the
// method is inferred from the options that are set, as it was before
// auth_type existed: an access token means "pat", a client ID or secret means
// "oauth-m2m".
func (d *databaseImpl) resolveLoginMethod() (string, loginMethod, error) {
	name := d.authType
	if name == "" {
		hasOAuth := d.oauthClientID != "" || d.oauthClientSecret != ""
		switch {
		case d.accessToken == "" && !hasOAuth:
			return "", loginMethod{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "[db] access token or OAuth config is required",
			}
		case d.accessToken != "" && hasOAuth:
			return "", loginMethod{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "[db] cannot specify both access token and OAuth config",
			}
		case d.accessToken != "":
			name = OptionValueAuthTypePAT
		default:
			name = OptionValueAuthTypeOAuthM2M
		}
	}

	method, ok := loginMethods[name]
	if !ok {
		return "", loginMethod{}, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[db] unknown auth type '%s'", name),
		}
	}
	for _, f := range method.required {
		if f.value(d) == "" {
			return "", loginMethod{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[db] %s is required when using auth type '%s'. Set this via '%s'.", f.label, name, f.option),
			}
		}
	}
	return name, method, nil
}
