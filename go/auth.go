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
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/databricks/databricks-sdk-go/common/environment"
	sdkconfig "github.com/databricks/databricks-sdk-go/config"
	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/databricks/databricks-sql-go/auth"
	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
)

// authField is an option a login method needs, read from the database.
type authField struct {
	option string
	label  string
	value  func(d *databaseImpl) string
}

// loginMethod is one way to authenticate: the options it requires and may
// take, and how it configures the SQL driver. Adding a method means adding one
// entry to loginMethods, keyed by its auth_type value. A method either builds
// its own connection option (build) or is a credential of the Databricks SDK
// (sdk).
type loginMethod struct {
	required []authField
	optional []string
	build    func(d *databaseImpl) dbsql.ConnOption
	sdk      *sdkLogin
}

// sdkLogin describes a login done by the Databricks SDK.
type sdkLogin struct {
	// cloud is set when the auth type implies one; otherwise the SDK guesses
	// it from the host name.
	cloud       environment.Cloud
	credentials sdkconfig.CredentialsStrategy
	// apply copies the method's options into the SDK config.
	apply func(d *databaseImpl, cfg *sdkconfig.Config)
}

var oidcClientIDField = authField{OptionOIDCClientID, "client ID", func(d *databaseImpl) string { return d.oidcClientID }}

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
		optional: []string{OptionOAuthScopes},
		build: func(d *databaseImpl) dbsql.ConnOption {
			return dbsql.WithAuthenticator(d.newM2MAuthenticator())
		},
	},
	// OIDC token federation, spelled as in dbt-databricks. The SDK exchanges
	// the ID token for a Databricks token.
	OptionValueAuthTypeEnvOIDC: {
		required: []authField{oidcClientIDField},
		sdk: &sdkLogin{
			credentials: sdkconfig.EnvOIDCCredentials{},
			apply:       func(d *databaseImpl, cfg *sdkconfig.Config) { cfg.ClientID = d.oidcClientID },
		},
	},
	OptionValueAuthTypeFileOIDC: {
		required: []authField{oidcClientIDField},
		optional: []string{OptionOIDCTokenFilepath},
		sdk: &sdkLogin{
			credentials: sdkconfig.FileOIDCCredentials{},
			apply: func(d *databaseImpl, cfg *sdkconfig.Config) {
				cfg.ClientID = d.oidcClientID
				// When unset, the SDK falls back to DATABRICKS_OIDC_TOKEN_FILEPATH.
				cfg.OIDCTokenFilepath = d.oidcTokenFilepath
			},
		},
	},
	// Microsoft Entra ID service principal on Azure Databricks. The SDK only
	// activates this credential when the tenant is set, so it is required.
	OptionValueAuthTypeAzureClientSecret: {
		required: []authField{
			{OptionAzureClientID, "azure client ID", func(d *databaseImpl) string { return d.azureClientID }},
			{OptionAzureClientSecret, "azure client secret", func(d *databaseImpl) string { return d.azureClientSecret }},
			{OptionAzureTenantID, "azure tenant ID", func(d *databaseImpl) string { return d.azureTenantID }},
		},
		sdk: &sdkLogin{
			cloud:       environment.CloudAzure,
			credentials: sdkconfig.AzureClientSecretCredentials{},
			apply: func(d *databaseImpl, cfg *sdkconfig.Config) {
				cfg.AzureClientID = d.azureClientID
				cfg.AzureClientSecret = d.azureClientSecret
				cfg.AzureTenantID = d.azureTenantID
			},
		},
	},
	// Azure managed identity. The client ID is optional and selects a
	// user-assigned identity.
	OptionValueAuthTypeAzureMSI: {
		optional: []string{OptionAzureClientID},
		sdk: &sdkLogin{
			cloud:       environment.CloudAzure,
			credentials: sdkconfig.AzureMsiCredentials{},
			apply: func(d *databaseImpl, cfg *sdkconfig.Config) {
				cfg.AzureUseMSI = true
				cfg.AzureClientID = d.azureClientID
			},
		},
	},
	// Azure CLI: uses the current `az login` session.
	OptionValueAuthTypeAzureCLI: {
		sdk: &sdkLogin{cloud: environment.CloudAzure, credentials: sdkconfig.AzureCliCredentials{}},
	},
	// Google service account key, as JSON or the path of a JSON file.
	OptionValueAuthTypeGoogleCredentials: {
		required: []authField{
			{OptionGoogleCredentials, "google credentials", func(d *databaseImpl) string { return d.googleCredentials }},
		},
		sdk: &sdkLogin{
			cloud:       environment.CloudGCP,
			credentials: sdkconfig.GoogleCredentials{},
			apply:       func(d *databaseImpl, cfg *sdkconfig.Config) { cfg.GoogleCredentials = d.googleCredentials },
		},
	},
	// Google ID token of a service account, from the application default
	// credentials of the environment.
	OptionValueAuthTypeGoogleID: {
		required: []authField{
			{OptionGoogleServiceAccount, "google service account", func(d *databaseImpl) string { return d.googleServiceAccount }},
		},
		sdk: &sdkLogin{
			cloud:       environment.CloudGCP,
			credentials: sdkconfig.GoogleDefaultCredentials{},
			apply:       func(d *databaseImpl, cfg *sdkconfig.Config) { cfg.GoogleServiceAccount = d.googleServiceAccount },
		},
	},
}

// secretAuthOptions are the auth options that hold a secret. The contract file
// marks the same options.
var secretAuthOptions = map[string]bool{
	OptionAccessToken:       true,
	OptionOAuthClientSecret: true,
	OptionAzureClientSecret: true,
	OptionGoogleCredentials: true,
}

// splitHost splits the server host name into its URL scheme and bare host.
// The scheme defaults to https, as in the SQL client.
func splitHost(host string) (scheme, name string) {
	if s, rest, ok := strings.Cut(host, "://"); ok && (strings.EqualFold(s, "https") || strings.EqualFold(s, "http")) {
		return strings.ToLower(s), rest
	}
	return "https", host
}

// sdkConfig returns the SDK config of login method name, to be used as the SQL
// client's authenticator. The config resolves itself on the first request, so
// a bad value surfaces when the connection is opened. Only environment
// variables are read, never ~/.databrickscfg, so a profile on disk cannot
// change the login method. Because auth_type is always set, credentials in
// other DATABRICKS_* variables (such as DATABRICKS_TOKEN) are ignored.
func (d *databaseImpl) sdkConfig(name string, login *sdkLogin) *sdkconfig.Config {
	scheme, host := splitHost(d.serverHostname)
	cfg := &sdkconfig.Config{
		Host:        scheme + "://" + host,
		Loaders:     []sdkconfig.Loader{sdkconfig.ConfigAttributes},
		AuthType:    name,
		Cloud:       login.cloud,
		Credentials: login.credentials,
	}
	if login.apply != nil {
		login.apply(d, cfg)
	}
	return cfg
}

// connOption returns the SQL client option that logs in with this method.
func (m loginMethod) connOption(name string, d *databaseImpl) dbsql.ConnOption {
	if m.sdk != nil {
		return dbsql.WithAuthenticator(d.sdkConfig(name, m.sdk))
	}
	return m.build(d)
}

// newM2MAuthenticator requests the configured scopes, or "all-apis" when none
// are set, so a secret limited to other scopes (for example "sql") can log in.
func (d *databaseImpl) newM2MAuthenticator() auth.Authenticator {
	_, host := splitHost(d.serverHostname)
	return m2m.NewAuthenticatorWithScopes(d.oauthClientID, d.oauthClientSecret, host, d.oauthScopes)
}

// parseScopes splits a comma or whitespace separated list of OAuth scopes.
// Each scope must be a valid RFC 6749 scope token.
func parseScopes(value string) ([]string, error) {
	scopes := strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' })
	if len(scopes) == 0 {
		return nil, nil
	}
	for _, scope := range scopes {
		for _, r := range scope {
			if r < 0x21 || r > 0x7e || r == '"' || r == '\\' {
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("[db] invalid OAuth scope '%s' for '%s'", scope, OptionOAuthScopes),
				}
			}
		}
	}
	return scopes, nil
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
