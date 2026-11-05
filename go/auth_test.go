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
