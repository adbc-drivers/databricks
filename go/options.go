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
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
)

// optionDef reads and writes one database option. set validates the value and
// stores it only when it is valid; get returns the stored value as a string.
type optionDef struct {
	set func(d *databaseImpl, value string) error
	get func(d *databaseImpl) string
}

// stringOption is an option that stores any string as given.
func stringOption(field func(d *databaseImpl) *string) optionDef {
	return optionDef{
		set: func(d *databaseImpl, value string) error { *field(d) = value; return nil },
		get: func(d *databaseImpl) string { return *field(d) },
	}
}

// intOption is an option holding an integer, where "" leaves the setting alone
// and a value that is not positive reads back as "".
func intOption(label string, field func(d *databaseImpl) *int) optionDef {
	return optionDef{
		set: func(d *databaseImpl, value string) error {
			if value == "" {
				return nil
			}
			n, err := strconv.Atoi(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid %s: %v", label, err),
				}
			}
			*field(d) = n
			return nil
		},
		get: func(d *databaseImpl) string {
			if n := *field(d); n > 0 {
				return strconv.Itoa(n)
			}
			return ""
		},
	}
}

// databaseOptions holds every database option that has a fixed key. Options
// with a key prefix (session parameters and query tags) are handled by
// SetOption and GetOption.
var databaseOptions = map[string]optionDef{
	adbc.OptionKeyURI: {
		set: func(d *databaseImpl, value string) error {
			// Strip the databricks:// scheme since databricks-sql-go expects raw DSN format
			after, ok := strings.CutPrefix(value, "databricks://")
			if !ok {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid URI scheme: expected 'databricks://', got '%s'", value),
				}
			}
			d.uri = after
			return nil
		},
		get: func(d *databaseImpl) string { return d.uri },
	},
	OptionServerHostname: stringOption(func(d *databaseImpl) *string { return &d.serverHostname }),
	OptionHTTPPath:       stringOption(func(d *databaseImpl) *string { return &d.httpPath }),
	OptionAccessToken:    stringOption(func(d *databaseImpl) *string { return &d.accessToken }),
	OptionAuthType: {
		set: func(d *databaseImpl, value string) error {
			if _, ok := loginMethods[value]; !ok && value != "" {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("[db] unknown auth type '%s'", value),
				}
			}
			d.authType = value
			return nil
		},
		get: func(d *databaseImpl) string { return d.authType },
	},
	OptionUserAgent: stringOption(func(d *databaseImpl) *string { return &d.userAgent }),
	OptionPort: {
		set: func(d *databaseImpl, value string) error {
			port, err := strconv.Atoi(value)
			if err != nil || port < 1 || port > 65535 {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "invalid port number",
				}
			}
			d.port = port
			return nil
		},
		get: func(d *databaseImpl) string { return strconv.Itoa(d.port) },
	},
	OptionCatalog: stringOption(func(d *databaseImpl) *string { return &d.catalog }),
	OptionSchema:  stringOption(func(d *databaseImpl) *string { return &d.schema }),
	OptionQueryTimeout: {
		set: func(d *databaseImpl, value string) error {
			if value == "" {
				return nil
			}
			timeout, err := time.ParseDuration(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid query timeout: %v", err),
				}
			}
			d.queryTimeout = timeout
			return nil
		},
		get: func(d *databaseImpl) string {
			if d.queryTimeout > 0 {
				return d.queryTimeout.String()
			}
			return ""
		},
	},
	OptionMaxRows:             intOption("max rows", func(d *databaseImpl) *int { return &d.maxRows }),
	OptionQueryRetryCount:     intOption("query retry count", func(d *databaseImpl) *int { return &d.queryRetryCount }),
	OptionDownloadThreadCount: intOption("download thread count", func(d *databaseImpl) *int { return &d.downloadThreadCount }),
	OptionSSLMode: {
		set: func(d *databaseImpl, value string) error {
			if value == "" {
				d.sslMode = value
				d.sslInsecure = false
				return nil
			}
			lowerValue := strings.ToLower(value)
			switch lowerValue {
			case "insecure":
				d.sslMode = lowerValue
				d.sslInsecure = true
			case "require":
				d.sslMode = lowerValue
				d.sslInsecure = false
			default:
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid SSL mode: %s (supported: 'require', 'insecure')", value),
				}
			}
			return nil
		},
		get: func(d *databaseImpl) string { return d.sslMode },
	},
	OptionSSLRootCert: {
		set: func(d *databaseImpl, value string) error {
			if value == "" {
				d.sslRootCert = value
				d.sslCertPool = nil
				return nil
			}
			// Validate that the certificate file exists and can be read.
			// Then, store valid cert.
			caCert, err := os.ReadFile(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("failed to read SSL root certificate: %v", err),
				}
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "failed to parse SSL root certificate",
				}
			}

			d.sslRootCert = value
			d.sslCertPool = caCertPool
			return nil
		},
		get: func(d *databaseImpl) string { return d.sslRootCert },
	},
	OptionOAuthClientID:        stringOption(func(d *databaseImpl) *string { return &d.oauthClientID }),
	OptionOAuthClientSecret:    stringOption(func(d *databaseImpl) *string { return &d.oauthClientSecret }),
	OptionOAuthRefreshToken:    stringOption(func(d *databaseImpl) *string { return &d.oauthRefreshToken }),
	OptionAzureClientID:        stringOption(func(d *databaseImpl) *string { return &d.azureClientID }),
	OptionAzureClientSecret:    stringOption(func(d *databaseImpl) *string { return &d.azureClientSecret }),
	OptionAzureTenantID:        stringOption(func(d *databaseImpl) *string { return &d.azureTenantID }),
	OptionGoogleCredentials:    stringOption(func(d *databaseImpl) *string { return &d.googleCredentials }),
	OptionGoogleServiceAccount: stringOption(func(d *databaseImpl) *string { return &d.googleServiceAccount }),
	OptionOIDCClientID:         stringOption(func(d *databaseImpl) *string { return &d.oidcClientID }),
	OptionOIDCTokenFilepath:    stringOption(func(d *databaseImpl) *string { return &d.oidcTokenFilepath }),
	OptionOAuthRedirectURL: {
		set: func(d *databaseImpl, value string) error {
			if err := validateBrowserRedirectURL(value); err != nil {
				return err
			}
			d.oauthRedirectURL = value
			return nil
		},
		get: func(d *databaseImpl) string { return d.oauthRedirectURL },
	},
	OptionOAuthScopes: {
		set: func(d *databaseImpl, value string) error {
			scopes, err := parseScopes(value)
			if err != nil {
				return err
			}
			d.oauthScopes = scopes
			return nil
		},
		get: func(d *databaseImpl) string { return strings.Join(d.oauthScopes, " ") },
	},
	OptionExternalBrowserTimeout: {
		set: func(d *databaseImpl, value string) error {
			if value == "" {
				d.oauthU2MTimeout = 0
				return nil
			}
			timeout, err := time.ParseDuration(value)
			if err != nil || timeout <= 0 {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("[db] invalid external browser timeout '%s': must be a positive duration such as '2m'", value),
				}
			}
			d.oauthU2MTimeout = timeout
			return nil
		},
		get: func(d *databaseImpl) string {
			if d.oauthU2MTimeout > 0 {
				return d.oauthU2MTimeout.String()
			}
			return ""
		},
	},
}
