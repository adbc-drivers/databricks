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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
)

func TestResolveLoginMethod(t *testing.T) {
	tests := []struct {
		name    string
		db      *databaseImpl
		want    string
		wantErr string
	}{
		{name: "token is inferred as pat", db: &databaseImpl{accessToken: "tok"}, want: "pat"},
		{name: "client credentials are inferred as oauth-m2m", db: &databaseImpl{oauthClientID: "id", oauthClientSecret: "secret"}, want: "oauth-m2m"},
		{name: "nothing set", db: &databaseImpl{}, wantErr: "access token or OAuth config is required"},
		{name: "token and client credentials", db: &databaseImpl{accessToken: "tok", oauthClientID: "id", oauthClientSecret: "secret"}, wantErr: "cannot specify both access token and OAuth config"},
		{name: "inferred oauth-m2m without secret", db: &databaseImpl{oauthClientID: "id"}, wantErr: "client secret is required when using auth type 'oauth-m2m'. Set this via 'databricks.oauth.client_secret'"},
		{name: "explicit pat", db: &databaseImpl{authType: "pat", accessToken: "tok"}, want: "pat"},
		{name: "explicit pat without token", db: &databaseImpl{authType: "pat"}, wantErr: "access token is required when using auth type 'pat'. Set this via 'databricks.access_token'"},
		{name: "explicit pat ignores unrelated oauth options", db: &databaseImpl{authType: "pat", accessToken: "tok", oauthClientID: "id"}, want: "pat"},
		{name: "explicit oauth-m2m", db: &databaseImpl{authType: "oauth-m2m", oauthClientID: "id", oauthClientSecret: "secret"}, want: "oauth-m2m"},
		{name: "explicit oauth-m2m without client id", db: &databaseImpl{authType: "oauth-m2m", oauthClientSecret: "secret"}, wantErr: "client ID is required when using auth type 'oauth-m2m'. Set this via 'databricks.oauth.client_id'"},
		{name: "explicit auth type with nothing else", db: &databaseImpl{authType: "oauth-m2m"}, wantErr: "client ID is required"},
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

// fakeWorkspace serves the OIDC discovery and token endpoints the Databricks
// SDK uses to exchange an ID token. It records the token requests.
type fakeWorkspace struct {
	*httptest.Server
	mu    sync.Mutex
	forms []map[string]string
}

func newFakeWorkspace(t *testing.T) *fakeWorkspace {
	t.Helper()
	w := &fakeWorkspace{}
	mux := http.NewServeMux()
	mux.HandleFunc("/oidc/.well-known/oauth-authorization-server", func(rw http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(rw).Encode(map[string]string{
			"authorization_endpoint": w.URL + "/oidc/v1/authorize",
			"token_endpoint":         w.URL + "/oidc/v1/token",
		})
	})
	mux.HandleFunc("/oidc/v1/token", func(rw http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
		form := map[string]string{}
		for k := range r.PostForm {
			form[k] = r.PostForm.Get(k)
		}
		w.mu.Lock()
		w.forms = append(w.forms, form)
		w.mu.Unlock()
		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(map[string]any{"access_token": "exchanged-token", "token_type": "Bearer", "expires_in": 3600})
	})
	w.Server = httptest.NewServer(mux)
	t.Cleanup(w.Close)
	return w
}

// exchanges returns the token requests received so far.
func (w *fakeWorkspace) exchanges() []map[string]string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]map[string]string(nil), w.forms...)
}

func authorizationHeader(t *testing.T, a interface{ Authenticate(*http.Request) error }) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid/", nil)
	require.NoError(t, err)
	require.NoError(t, a.Authenticate(req))
	return req.Header.Get("Authorization")
}

func TestOIDCFederation(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DATABRICKS_OIDC_TOKEN_FILEPATH", "")

	t.Run("env-oidc exchanges the ID token from the environment", func(t *testing.T) {
		ws := newFakeWorkspace(t)
		t.Setenv("DATABRICKS_OIDC_TOKEN", "id-token-from-env")
		db := &databaseImpl{authType: OptionValueAuthTypeEnvOIDC, serverHostname: ws.URL, oidcClientID: "sp-client"}

		require.Equal(t, "Bearer exchanged-token", authorizationHeader(t, db.sdkConfig(OptionValueAuthTypeEnvOIDC, loginMethods[OptionValueAuthTypeEnvOIDC].sdk)))
		forms := ws.exchanges()
		require.Len(t, forms, 1)
		require.Equal(t, "id-token-from-env", forms[0]["subject_token"])
		require.Equal(t, "sp-client", forms[0]["client_id"])
	})

	t.Run("file-oidc exchanges the ID token from the file option", func(t *testing.T) {
		ws := newFakeWorkspace(t)
		path := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(path, []byte("id-token-from-file"), 0o600))
		db := &databaseImpl{authType: OptionValueAuthTypeFileOIDC, serverHostname: ws.URL, oidcClientID: "sp-client", oidcTokenFilepath: path}

		require.Equal(t, "Bearer exchanged-token", authorizationHeader(t, db.sdkConfig(OptionValueAuthTypeFileOIDC, loginMethods[OptionValueAuthTypeFileOIDC].sdk)))
		forms := ws.exchanges()
		require.Len(t, forms, 1)
		require.Equal(t, "id-token-from-file", forms[0]["subject_token"])
	})

	t.Run("file-oidc falls back to DATABRICKS_OIDC_TOKEN_FILEPATH", func(t *testing.T) {
		ws := newFakeWorkspace(t)
		path := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(path, []byte("id-token-from-env-path"), 0o600))
		t.Setenv("DATABRICKS_OIDC_TOKEN_FILEPATH", path)
		db := &databaseImpl{authType: OptionValueAuthTypeFileOIDC, serverHostname: ws.URL, oidcClientID: "sp-client"}

		require.Equal(t, "Bearer exchanged-token", authorizationHeader(t, db.sdkConfig(OptionValueAuthTypeFileOIDC, loginMethods[OptionValueAuthTypeFileOIDC].sdk)))
		require.Equal(t, "id-token-from-env-path", ws.exchanges()[0]["subject_token"])
	})

	// The docs promise that credentials of other methods in the environment are ignored.
	t.Run("env-oidc ignores other credentials in the environment", func(t *testing.T) {
		ws := newFakeWorkspace(t)
		t.Setenv("DATABRICKS_OIDC_TOKEN", "id-token-from-env")
		t.Setenv("DATABRICKS_TOKEN", "pat-from-env")
		t.Setenv("DATABRICKS_ACCOUNT_ID", "account")
		db := &databaseImpl{authType: OptionValueAuthTypeEnvOIDC, serverHostname: ws.URL, oidcClientID: "sp-client"}

		require.Equal(t, "Bearer exchanged-token", authorizationHeader(t, db.sdkConfig(OptionValueAuthTypeEnvOIDC, loginMethods[OptionValueAuthTypeEnvOIDC].sdk)))
	})
}

// The SDK-backed methods build their login option through resolveConnectionOptions,
// the path Open takes; a method with neither a builder nor an SDK login would panic here.
func TestOIDCMethodsBuildConnectionOptions(t *testing.T) {
	for _, name := range []string{OptionValueAuthTypeEnvOIDC, OptionValueAuthTypeFileOIDC} {
		db := &databaseImpl{authType: name, serverHostname: "dbc-1.cloud.databricks.com", httpPath: "/sql/1.0/warehouses/abc", oidcClientID: "sp-client"}
		opts, err := db.resolveConnectionOptions()
		require.NoError(t, err, name)
		require.NotEmpty(t, opts, name)
	}
}

func TestResolveLoginMethodOIDC(t *testing.T) {
	for _, name := range []string{OptionValueAuthTypeEnvOIDC, OptionValueAuthTypeFileOIDC} {
		t.Run(name+" requires a client ID", func(t *testing.T) {
			db := databaseImpl{authType: name}
			_, _, err := db.resolveLoginMethod()
			require.ErrorContains(t, err, "client ID is required when using auth type '"+name+"'. Set this via 'databricks.oidc.client_id'")
		})
		t.Run(name+" with a client ID", func(t *testing.T) {
			db := databaseImpl{authType: name, oidcClientID: "sp-client"}
			got, _, err := db.resolveLoginMethod()
			require.NoError(t, err)
			require.Equal(t, name, got)
		})
	}

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
		OptionAuthType:          OptionValueAuthTypeFileOIDC,
		OptionOIDCClientID:      "sp-client",
		OptionOIDCTokenFilepath: "/var/run/secrets/token",
	} {
		require.NoError(t, db.SetOption(ctx, key, value))
		got, err := db.GetOption(ctx, key)
		require.NoError(t, err)
		require.Equal(t, value, got)
	}

	// A bad auth type is rejected and leaves the OIDC settings alone.
	require.ErrorContains(t, db.SetOption(ctx, OptionAuthType, "oidc"), "unknown auth type 'oidc'")
	got, err := db.GetOption(ctx, OptionAuthType)
	require.NoError(t, err)
	require.Equal(t, "file-oidc", got)
}

func TestSplitHost(t *testing.T) {
	for host, want := range map[string][2]string{
		"dbc-1.cloud.databricks.com": {"https", "dbc-1.cloud.databricks.com"},
		"HTTP://127.0.0.1:8080":      {"http", "127.0.0.1:8080"},
	} {
		scheme, name := splitHost(host)
		require.Equal(t, want, [2]string{scheme, name}, host)
	}
}

func TestOAuthScopesOption(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}

	got, err := db.GetOption(ctx, OptionOAuthScopes)
	require.NoError(t, err)
	require.Equal(t, "", got)

	// Commas and any whitespace, including a CRLF line ending, separate scopes, and the
	// value is read back as it was set.
	value := "sql, offline_access\r\n"
	require.NoError(t, db.SetOption(ctx, OptionOAuthScopes, value))
	require.Equal(t, []string{"sql", "offline_access"}, db.oauthScopes)
	got, err = db.GetOption(ctx, OptionOAuthScopes)
	require.NoError(t, err)
	require.Equal(t, value, got)

	require.NoError(t, db.SetOption(ctx, OptionOAuthScopes, ""))
	require.Empty(t, db.oauthScopes)

	// A bad value is rejected and leaves the existing setting alone.
	require.NoError(t, db.SetOption(ctx, OptionOAuthScopes, "sql"))
	err = db.SetOption(ctx, OptionOAuthScopes, `sql "quoted"`)
	require.ErrorContains(t, err, "invalid OAuth scope")
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
	got, err = db.GetOption(ctx, OptionOAuthScopes)
	require.NoError(t, err)
	require.Equal(t, "sql", got)
}

func TestM2MAuthenticatorScopes(t *testing.T) {
	// sql-go's authenticator type is private, so the scopes it will request are
	// read through its M2MScopes method.
	scopesOf := func(db *databaseImpl) []string {
		a := db.newM2MAuthenticator()
		p, ok := a.(interface{ M2MScopes() []string })
		require.True(t, ok)
		return p.M2MScopes()
	}

	// The request used to be made with dbsql.WithClientCredentials, which asks for
	// "all-apis"; keep that when no scopes are set.
	db := &databaseImpl{serverHostname: "https://host.example.com", oauthClientID: "id", oauthClientSecret: "secret"}
	require.Equal(t, []string{"all-apis"}, scopesOf(db), "no scopes keeps the default")

	db.oauthScopes = []string{"sql"}
	require.Equal(t, []string{"sql"}, scopesOf(db))
}

func TestAzureClientSecret(t *testing.T) {
	ctx := context.Background()
	newFull := func() *databaseImpl {
		return &databaseImpl{authType: OptionValueAuthTypeAzureClientSecret, azureClientID: "app", azureClientSecret: "secret", azureTenantID: "tenant"}
	}

	t.Run("requires its options", func(t *testing.T) {
		name, _, err := newFull().resolveLoginMethod()
		require.NoError(t, err)
		require.Equal(t, OptionValueAuthTypeAzureClientSecret, name)

		db := newFull()
		db.azureTenantID = ""
		_, _, err = db.resolveLoginMethod()
		require.ErrorContains(t, err, "when using auth type 'azure-client-secret'. Set this via '"+OptionAzureTenantID+"'")
	})

	t.Run("builds an SDK config for Azure", func(t *testing.T) {
		db := newFull()
		db.serverHostname = "adb-123.4.azuredatabricks.net"
		cfg := db.sdkConfig(OptionValueAuthTypeAzureClientSecret, loginMethods[OptionValueAuthTypeAzureClientSecret].sdk)
		require.Equal(t, "https://adb-123.4.azuredatabricks.net", cfg.Host)
		require.Equal(t, OptionValueAuthTypeAzureClientSecret, cfg.Credentials.Name())
		// The driver's own mapping of its options onto the SDK config.
		require.Equal(t, []string{"app", "secret", "tenant"}, []string{cfg.AzureClientID, cfg.AzureClientSecret, cfg.AzureTenantID})

		// A custom host name is still treated as Azure.
		db.serverHostname = "databricks.internal.example.com"
		require.True(t, db.sdkConfig(OptionValueAuthTypeAzureClientSecret, loginMethods[OptionValueAuthTypeAzureClientSecret].sdk).IsAzure())
	})

	t.Run("is configured through the options", func(t *testing.T) {
		db := &databaseImpl{}
		for key, value := range map[string]string{
			OptionAuthType:          OptionValueAuthTypeAzureClientSecret,
			OptionAzureClientID:     "app",
			OptionAzureClientSecret: "secret",
			OptionAzureTenantID:     "tenant",
		} {
			require.NoError(t, db.SetOption(ctx, key, value))
		}
		name, _, err := db.resolveLoginMethod()
		require.NoError(t, err)
		require.Equal(t, OptionValueAuthTypeAzureClientSecret, name)
	})
}
