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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
)

// writeTestCertificate writes a self-signed certificate in PEM form.
func writeTestCertificate(t *testing.T, name string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	return path
}

type optionValue struct {
	set string // value passed to SetOption
	get string // value GetOption returns afterwards
}

// validatedOptionCases lists the options whose value is validated, with values
// the driver accepts and values it rejects. Options with their own tests
// (auth type, OAuth scopes, redirect URL, external browser timeout, session
// parameters, query tags) are covered where they are defined.
func validatedOptionCases(t *testing.T) map[string]struct {
	good  []optionValue
	bad   []string
	state func(*databaseImpl) any
} {
	cert := writeTestCertificate(t, "ca.pem")
	notCert := filepath.Join(t.TempDir(), "not-a-cert.pem")
	require.NoError(t, os.WriteFile(notCert, []byte("not a certificate"), 0o600))

	return map[string]struct {
		good  []optionValue
		bad   []string
		state func(*databaseImpl) any
	}{
		OptionPort: {
			good:  []optionValue{{"443", "443"}, {"1", "1"}, {"65535", "65535"}},
			bad:   []string{"0", "65536", "-1", "abc", "", "44.3"},
			state: func(d *databaseImpl) any { return d.port },
		},
		OptionQueryTimeout: {
			good:  []optionValue{{"30s", "30s"}, {"2m", "2m0s"}, {"1h30m", "1h30m0s"}},
			bad:   []string{"soon", "30", "1 minute"},
			state: func(d *databaseImpl) any { return d.queryTimeout },
		},
		OptionMaxRows: {
			good:  []optionValue{{"1000", "1000"}, {"1", "1"}},
			bad:   []string{"many", "1.5", "10k"},
			state: func(d *databaseImpl) any { return d.maxRows },
		},
		OptionQueryRetryCount: {
			good:  []optionValue{{"3", "3"}, {"1", "1"}},
			bad:   []string{"often", "1.5"},
			state: func(d *databaseImpl) any { return d.queryRetryCount },
		},
		OptionDownloadThreadCount: {
			good:  []optionValue{{"4", "4"}, {"1", "1"}},
			bad:   []string{"many", "2.5"},
			state: func(d *databaseImpl) any { return d.downloadThreadCount },
		},
		OptionSSLMode: {
			good:  []optionValue{{"require", "require"}, {"insecure", "insecure"}, {"REQUIRE", "require"}},
			bad:   []string{"verify-full", "disable", "true"},
			state: func(d *databaseImpl) any { return [2]any{d.sslMode, d.sslInsecure} },
		},
		OptionSSLRootCert: {
			good:  []optionValue{{cert, cert}},
			bad:   []string{filepath.Join(t.TempDir(), "missing.pem"), notCert},
			state: func(d *databaseImpl) any { return [2]any{d.sslRootCert, d.sslCertPool} },
		},
	}
}

func TestValidatedOptions(t *testing.T) {
	ctx := context.Background()
	for key, tc := range validatedOptionCases(t) {
		t.Run(key, func(t *testing.T) {
			for _, good := range tc.good {
				db := &databaseImpl{}
				require.NoError(t, db.SetOption(ctx, key, good.set), "set %q", good.set)
				got, err := db.GetOption(ctx, key)
				require.NoError(t, err)
				require.Equal(t, good.get, got, "get after set %q", good.set)
			}

			// A bad value is rejected with INVALID_ARGUMENT and leaves the
			// setting that was already there unchanged.
			for _, existing := range tc.good {
				db := &databaseImpl{}
				require.NoError(t, db.SetOption(ctx, key, existing.set))
				before := tc.state(db)
				for _, bad := range tc.bad {
					err := db.SetOption(ctx, key, bad)
					var adbcErr adbc.Error
					require.ErrorAs(t, err, &adbcErr, "bad value %q", bad)
					require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, "bad value %q", bad)
					got, err := db.GetOption(ctx, key)
					require.NoError(t, err)
					require.Equal(t, existing.get, got, "bad value %q must not change the setting", bad)
					require.Equal(t, before, tc.state(db), "bad value %q must not change the stored state", bad)
				}
			}
		})
	}
}

// The options that take any string: the value is stored as given and an empty
// value clears it. (databricks.user_agent is covered at the connector level in
// connector_test.go.)
func TestFreeTextOptions(t *testing.T) {
	ctx := context.Background()
	for _, key := range []string{
		OptionServerHostname, OptionHTTPPath, OptionAccessToken, OptionCatalog, OptionSchema,
		OptionOAuthClientID, OptionOAuthClientSecret, OptionOAuthRefreshToken,
		OptionAzureClientID, OptionAzureClientSecret, OptionAzureTenantID,
		OptionOIDCClientID, OptionOIDCTokenFilepath,
	} {
		t.Run(key, func(t *testing.T) {
			db := &databaseImpl{}
			got, err := db.GetOption(ctx, key)
			require.NoError(t, err)
			require.Equal(t, "", got, "default")

			require.NoError(t, db.SetOption(ctx, key, "some value"))
			got, err = db.GetOption(ctx, key)
			require.NoError(t, err)
			require.Equal(t, "some value", got)

			require.NoError(t, db.SetOption(ctx, key, ""))
			got, err = db.GetOption(ctx, key)
			require.NoError(t, err)
			require.Equal(t, "", got, "empty clears")
		})
	}
}

func TestURIOption(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	require.NoError(t, db.SetOption(ctx, adbc.OptionKeyURI, "databricks://host.example:443/sql/1.0/warehouses/w"))
	got, err := db.GetOption(ctx, adbc.OptionKeyURI)
	require.NoError(t, err)
	require.Equal(t, "host.example:443/sql/1.0/warehouses/w", got)

	// An empty URI is rejected on purpose: a URI must carry the databricks:// scheme.
	for _, bad := range []string{"postgres://host", "host:443/path", ""} {
		err := db.SetOption(ctx, adbc.OptionKeyURI, bad)
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr, bad)
		require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, bad)
		got, _ := db.GetOption(ctx, adbc.OptionKeyURI)
		require.Equal(t, "host.example:443/sql/1.0/warehouses/w", got, "a bad value must not change the setting")
	}
}

func TestUnknownOptionIsRejected(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	err := db.SetOption(ctx, "databricks.no_such_option", "x")
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusNotImplemented, adbcErr.Code)
	_, err = db.GetOption(ctx, "databricks.no_such_option")
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusNotFound, adbcErr.Code)
}

// SetOptions rejects a URI combined with individual options without changing
// any setting.
func TestSetOptionsURIExcludesOtherOptions(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	err := db.SetOptions(ctx, map[string]string{
		adbc.OptionKeyURI: "databricks://host.example/path",
		OptionCatalog:     "main",
	})
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
	require.Equal(t, "", db.uri)
	require.Equal(t, "", db.catalog)
}

// An empty value for an integer or duration option leaves the setting alone.
func TestEmptyValueKeepsNumericOptions(t *testing.T) {
	ctx := context.Background()
	for key, value := range map[string]string{
		OptionQueryTimeout:        "30s",
		OptionMaxRows:             "1000",
		OptionQueryRetryCount:     "3",
		OptionDownloadThreadCount: "4",
	} {
		t.Run(key, func(t *testing.T) {
			tc := validatedOptionCases(t)[key]
			db := &databaseImpl{}
			require.NoError(t, db.SetOption(ctx, key, value))
			before := tc.state(db)
			require.NoError(t, db.SetOption(ctx, key, ""))
			got, err := db.GetOption(ctx, key)
			require.NoError(t, err)
			require.NotEqual(t, "", got)
			require.Equal(t, before, tc.state(db))
		})
	}
}

// An empty value resets the SSL options.
func TestEmptyValueResetsSSLOptions(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	require.NoError(t, db.SetOption(ctx, OptionSSLMode, "insecure"))
	require.True(t, db.sslInsecure)
	require.NoError(t, db.SetOption(ctx, OptionSSLMode, ""))
	require.Equal(t, "", db.sslMode)
	require.False(t, db.sslInsecure)

	cert := writeTestCertificate(t, "ca.pem")
	require.NoError(t, db.SetOption(ctx, OptionSSLRootCert, cert))
	require.NotNil(t, db.sslCertPool)
	require.NoError(t, db.SetOption(ctx, OptionSSLRootCert, ""))
	require.Equal(t, "", db.sslRootCert)
	require.Nil(t, db.sslCertPool)
}

// Setting an option makes the next Open build a new pool; Open without a change
// reuses the pool.
func TestSetOptionMakesOpenBuildANewPool(t *testing.T) {
	ctx := context.Background()
	name := fmt.Sprintf("databricks-options-test-%d", parameterTestDriverCounter.Add(1))
	sql.Register(name, tagCaptureDriver{conn: &tagCaptureConn{}})
	var pools int
	db := newSessionParamDatabase()
	db.needsRefresh = true
	db.newPool = func(context.Context) (*sql.DB, error) {
		pools++
		return sql.Open(name, "")
	}
	open := func() {
		conn, err := db.Open(ctx)
		require.NoError(t, err)
		require.NoError(t, conn.Close(ctx))
	}

	open()
	open()
	require.Equal(t, 1, pools, "no option changed, so the pool is reused")

	require.NoError(t, db.SetOption(ctx, OptionCatalog, "main"))
	open()
	require.Equal(t, 2, pools, "an option changed, so Open builds a new pool")
	open()
	require.Equal(t, 2, pools)
}
