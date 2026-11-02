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
	"errors"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

// The offline login matrix: every auth type, which options it needs, which it
// ignores, and what the error says when something is missing. No network.
func TestLoginMatrix(t *testing.T) {
	isolateHome(t)
	base := databaseImpl{serverHostname: "x.cloud.databricks.com", httpPath: "/sql/1.0/warehouses/abc"}
	with := func(f func(d *databaseImpl)) databaseImpl {
		d := base
		f(&d)
		return d
	}
	tests := []struct {
		name    string
		db      databaseImpl
		wantErr string // empty: the options build
	}{
		{"pat", with(func(d *databaseImpl) { d.authType, d.accessToken = "pat", "t" }), ""},
		{"pat missing token", with(func(d *databaseImpl) { d.authType = "pat" }), "access token is required when using auth type 'pat'"},
		{"oauth-m2m", with(func(d *databaseImpl) { d.authType, d.oauthClientID, d.oauthClientSecret = "oauth-m2m", "i", "s" }), ""},
		{"oauth-m2m missing secret", with(func(d *databaseImpl) { d.authType, d.oauthClientID = "oauth-m2m", "i" }), "client secret is required when using auth type 'oauth-m2m'"},
		{"external-browser needs nothing", with(func(d *databaseImpl) { d.authType = "external-browser" }), ""},
		{"external-browser ignores a token", with(func(d *databaseImpl) { d.authType, d.accessToken = "external-browser", "t" }), ""},
		{"external-browser with client and scopes", with(func(d *databaseImpl) {
			d.authType, d.oauthClientID, d.oauthScopes, d.oauthRedirectURL = "external-browser", "mine", []string{"all-apis"}, "localhost:9000"
		}), ""},
		{"nothing set and no auth type", base, "access token or OAuth config is required"},
		{"unknown auth type", with(func(d *databaseImpl) { d.authType = "kerberos" }), "unknown auth type 'kerberos'"},
		{"token and client credentials without auth type", with(func(d *databaseImpl) { d.accessToken, d.oauthClientID, d.oauthClientSecret = "t", "i", "s" }), "cannot specify both"},
		{"client ID alone is never browser login", with(func(d *databaseImpl) { d.oauthClientID = "i" }), "client secret is required when using auth type 'oauth-m2m'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := tt.db.resolveConnectionOptions()
			if tt.wantErr == "" {
				require.NoError(t, err)
				require.NotEmpty(t, opts)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			var adbcErr adbc.Error
			require.ErrorAs(t, err, &adbcErr)
			require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
		})
	}
}

func TestAuthTypeOptionValidation(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	for _, good := range []string{"pat", "oauth-m2m", "external-browser", ""} {
		require.NoError(t, db.SetOption(ctx, OptionAuthType, good), good)
	}
	require.NoError(t, db.SetOption(ctx, OptionAuthType, "external-browser"))
	err := db.SetOption(ctx, OptionAuthType, "kerberos")
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
	got, _ := db.GetOption(ctx, OptionAuthType)
	require.Equal(t, "external-browser", got, "a bad value must not change the setting")
}

func TestBrowserDiscoveryFailureIsReportedAndRetried(t *testing.T) {
	h := newBrowserHarness(t)
	fail := true
	h.cfg.resolveConfig = func(ctx context.Context, c browserLoginConfig) (oauth2.Config, error) {
		if fail {
			return oauth2.Config{}, errors.New("could not lookup provider details")
		}
		return h.idp.discover(ctx, c)
	}
	a := newBrowserAuthenticator(h.cfg)
	_, err := authHeader(t, a)
	require.ErrorContains(t, err, "unable to generate oauth2.Config: could not lookup provider details")
	require.Equal(t, 0, h.openCount())

	fail = false
	hdr, err := authHeader(t, a)
	require.NoError(t, err)
	require.Equal(t, "Bearer access-1", hdr)
}
