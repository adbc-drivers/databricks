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
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
)

func TestResolveLoginMethod(t *testing.T) {
	tests := []struct {
		name    string
		db      databaseImpl
		want    string
		wantErr string
	}{
		{name: "token is inferred as pat", db: databaseImpl{accessToken: "tok"}, want: "pat"},
		{name: "client credentials are inferred as oauth-m2m", db: databaseImpl{oauthClientID: "id", oauthClientSecret: "secret"}, want: "oauth-m2m"},
		{name: "nothing set", db: databaseImpl{}, wantErr: "access token or OAuth config is required"},
		{name: "token and client credentials", db: databaseImpl{accessToken: "tok", oauthClientID: "id", oauthClientSecret: "secret"}, wantErr: "cannot specify both access token and OAuth config"},
		{name: "inferred oauth-m2m without secret", db: databaseImpl{oauthClientID: "id"}, wantErr: "client secret is required when using auth type 'oauth-m2m'. Set this via 'databricks.oauth.client_secret'"},
		{name: "explicit pat", db: databaseImpl{authType: "pat", accessToken: "tok"}, want: "pat"},
		{name: "explicit pat without token", db: databaseImpl{authType: "pat"}, wantErr: "access token is required when using auth type 'pat'. Set this via 'databricks.access_token'"},
		{name: "explicit pat ignores unrelated oauth options", db: databaseImpl{authType: "pat", accessToken: "tok", oauthClientID: "id"}, want: "pat"},
		{name: "explicit oauth-m2m", db: databaseImpl{authType: "oauth-m2m", oauthClientID: "id", oauthClientSecret: "secret"}, want: "oauth-m2m"},
		{name: "explicit oauth-m2m without client id", db: databaseImpl{authType: "oauth-m2m", oauthClientSecret: "secret"}, wantErr: "client ID is required when using auth type 'oauth-m2m'. Set this via 'databricks.oauth.client_id'"},
		{name: "explicit auth type with nothing else", db: databaseImpl{authType: "oauth-m2m"}, wantErr: "client ID is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, _, err := tt.db.resolveLoginMethod()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				var adbcErr adbc.Error
				require.ErrorAs(t, err, &adbcErr)
				require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, name)
		})
	}
}

func TestResolveConnectionOptionsRequiresLogin(t *testing.T) {
	db := &databaseImpl{serverHostname: "host", httpPath: "/sql/1.0/warehouses/w"}
	_, err := db.resolveConnectionOptions()
	require.ErrorContains(t, err, "access token or OAuth config is required")

	db.accessToken = "tok"
	opts, err := db.resolveConnectionOptions()
	require.NoError(t, err)
	require.NotEmpty(t, opts)
}

func TestAuthTypeOption(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}

	got, err := db.GetOption(ctx, OptionAuthType)
	require.NoError(t, err)
	require.Equal(t, "", got)

	require.NoError(t, db.SetOption(ctx, OptionAuthType, "oauth-m2m"))
	got, err = db.GetOption(ctx, OptionAuthType)
	require.NoError(t, err)
	require.Equal(t, "oauth-m2m", got)

	// A bad value is rejected and leaves the existing setting alone.
	err = db.SetOption(ctx, OptionAuthType, "kerberos")
	require.ErrorContains(t, err, "unknown auth type 'kerberos'")
	got, err = db.GetOption(ctx, OptionAuthType)
	require.NoError(t, err)
	require.Equal(t, "oauth-m2m", got)

	// An empty value goes back to inferring the method.
	require.NoError(t, db.SetOption(ctx, OptionAuthType, ""))
	got, err = db.GetOption(ctx, OptionAuthType)
	require.NoError(t, err)
	require.Equal(t, "", got)
}

func TestOAuthScopesOption(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}

	got, err := db.GetOption(ctx, OptionOAuthScopes)
	require.NoError(t, err)
	require.Equal(t, "", got)

	// Commas and any whitespace, including a CRLF line ending, separate scopes.
	require.NoError(t, db.SetOption(ctx, OptionOAuthScopes, "sql, offline_access\r\n"))
	require.Equal(t, []string{"sql", "offline_access"}, db.oauthScopes)
	got, err = db.GetOption(ctx, OptionOAuthScopes)
	require.NoError(t, err)
	require.Equal(t, "sql offline_access", got)

	require.NoError(t, db.SetOption(ctx, OptionOAuthScopes, ""))
	require.Empty(t, db.oauthScopes)
	require.NoError(t, db.SetOption(ctx, OptionOAuthScopes, "  "))
	require.Empty(t, db.oauthScopes)

	// A bad value is rejected and leaves the existing setting alone.
	require.NoError(t, db.SetOption(ctx, OptionOAuthScopes, "sql"))
	err = db.SetOption(ctx, OptionOAuthScopes, `sql "quoted"`)
	require.ErrorContains(t, err, "invalid OAuth scope")
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
	// Only separators is rejected too, and leaves the setting alone.
	err = db.SetOption(ctx, OptionOAuthScopes, ",,,")
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
	got, err = db.GetOption(ctx, OptionOAuthScopes)
	require.NoError(t, err)
	require.Equal(t, "sql", got)
}

func TestResolveLoginMethodOIDC(t *testing.T) {
	t.Run("requires a client ID", func(t *testing.T) {
		db := databaseImpl{authType: OptionValueAuthTypeOIDC}
		_, _, err := db.resolveLoginMethod()
		require.ErrorContains(t, err, "client ID is required when using auth type 'oidc'. Set this via 'databricks.oidc.client_id'")
	})

	t.Run("with a client ID", func(t *testing.T) {
		db := databaseImpl{authType: OptionValueAuthTypeOIDC, oidcClientID: "sp-client"}
		got, _, err := db.resolveLoginMethod()
		require.NoError(t, err)
		require.Equal(t, OptionValueAuthTypeOIDC, got)
	})

	t.Run("OIDC options alone do not select a method", func(t *testing.T) {
		db := databaseImpl{oidcClientID: "sp-client"}
		_, _, err := db.resolveLoginMethod()
		require.ErrorContains(t, err, "access token or OAuth config is required")
	})
}

func TestOIDCOptions(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	for key, value := range map[string]string{
		OptionAuthType:          OptionValueAuthTypeOIDC,
		OptionOIDCClientID:      "sp-client",
		OptionOIDCTokenFilepath: "/var/run/secrets/token",
	} {
		require.NoError(t, db.SetOption(ctx, key, value))
		got, err := db.GetOption(ctx, key)
		require.NoError(t, err)
		require.Equal(t, value, got)
	}

	// A bad auth type is rejected and leaves the OIDC settings alone.
	require.ErrorContains(t, db.SetOption(ctx, OptionAuthType, "env-oidc"), "unknown auth type 'env-oidc'")
	got, err := db.GetOption(ctx, OptionAuthType)
	require.NoError(t, err)
	require.Equal(t, "oidc", got)
}

// The ID token comes from the file option when it is set, and from the
// environment otherwise.
func TestOIDCCredentialChoice(t *testing.T) {
	login := loginMethods[OptionValueAuthTypeOIDC].sdk

	db := &databaseImpl{serverHostname: "dbc-1.cloud.databricks.com", oidcClientID: "sp-client"}
	cfg := db.sdkConfig(login)
	require.Equal(t, "env-oidc", cfg.AuthType)
	require.Equal(t, "sp-client", cfg.ClientID)

	db.oidcTokenFilepath = "/var/run/secrets/token"
	cfg = db.sdkConfig(login)
	require.Equal(t, "file-oidc", cfg.AuthType)
	require.Equal(t, "/var/run/secrets/token", cfg.OIDCTokenFilepath)
}

// The SDK login must reach the workspace the way the SQL client does.
func TestSDKConfigHostAndTLS(t *testing.T) {
	login := loginMethods[OptionValueAuthTypeOIDC].sdk

	for _, tt := range []struct {
		name string
		host string
		port int
		want string
	}{
		{"bare host", "dbc.example.com", 0, "https://dbc.example.com"},
		{"default port is left out", "dbc.example.com", 443, "https://dbc.example.com"},
		{"port option", "dbc.example.com", 8443, "https://dbc.example.com:8443"},
		{"port in the host name wins", "dbc.example.com:8443", 9000, "https://dbc.example.com:8443"},
		{"scheme is kept", "HTTP://localhost", 8080, "http://localhost:8080"},
		{"IPv6", "[::1]", 8443, "https://[::1]:8443"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := &databaseImpl{serverHostname: tt.host, port: tt.port, oidcClientID: "sp-client"}
			require.Equal(t, tt.want, db.sdkConfig(login).Host)
		})
	}

	t.Run("TLS settings", func(t *testing.T) {
		db := &databaseImpl{serverHostname: "dbc.example.com", oidcClientID: "sp-client"}
		require.Nil(t, db.sdkConfig(login).HTTPTransport)
		db.sslInsecure = true
		require.NotNil(t, db.sdkConfig(login).HTTPTransport)
	})
}

// The SDK-backed methods build their login option through resolveConnectionOptions,
// the path Open takes; a method with neither a builder nor an SDK login would panic here.
func TestOIDCBuildsConnectionOptions(t *testing.T) {
	db := &databaseImpl{authType: OptionValueAuthTypeOIDC, serverHostname: "dbc-1.cloud.databricks.com", httpPath: "/sql/1.0/warehouses/abc", oidcClientID: "sp-client"}
	opts, err := db.resolveConnectionOptions()
	require.NoError(t, err)
	require.NotEmpty(t, opts)
}
