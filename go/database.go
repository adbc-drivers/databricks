// Copyright (c) 2025 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package databricks

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"crypto/tls"
	"crypto/x509"
	"net/http"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	dbsql "github.com/databricks/databricks-sql-go"
)

const (
	DEFAULT_PORT           = 443
	DEFAULT_RETRY_WAIT_MIN = 1 * time.Second
	DEFAULT_RETRY_WAIT_MAX = 30 * time.Second
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	// Connection Pool. mu serialises pool start-up and Close so that concurrent
	// Open calls cannot each run a login (two browser logins would race for the
	// same local callback port).
	mu           sync.Mutex
	db           *sql.DB
	needsRefresh bool // Whether we need to re-initialize

	// newPool creates the connection pool; nil means initializeConnectionPool.
	// Tests replace it to observe start-up without a workspace.
	newPool func(ctx context.Context) (*sql.DB, error)

	// Connection parameters
	uri            string
	serverHostname string
	httpPath       string
	accessToken    string
	authType       string
	userAgent      string
	port           int
	catalog        string
	schema         string

	// Query options
	queryTimeout        time.Duration
	maxRows             int
	queryRetryCount     int
	downloadThreadCount int

	// TLS/SSL options
	sslMode     string
	sslRootCert string
	sslCertPool *x509.CertPool
	sslInsecure bool

	// OAuth options
	oauthClientID     string
	oauthClientSecret string
	oauthRefreshToken string
	// oauthScopes is the parsed list and oauthScopesValue the value as set, which
	// GetOption returns.
	oauthScopes      []string
	oauthScopesValue string
	oauthU2MTimeout  time.Duration

	// Default query tags, keyed by tag name
	queryTags map[string]string

	// Session parameters sent when the session opens (databricks.session_param.<name>).
	sessionParams map[string]string

	// Azure service principal options
	azureClientID     string
	azureClientSecret string
	azureTenantID     string

	// OIDC token federation options
	oidcClientID      string
	oidcTokenFilepath string
	oauthRedirectURL  string
}

func (d *databaseImpl) resolveConnectionOptions() ([]dbsql.ConnOption, error) {
	if d.serverHostname == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "server hostname is required",
		}
	}

	if d.httpPath == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "HTTP path is required",
		}
	}

	loginName, login, err := d.resolveLoginMethod()
	if err != nil {
		return nil, err
	}

	opts := []dbsql.ConnOption{
		dbsql.WithServerHostname(d.serverHostname),
		dbsql.WithHTTPPath(d.httpPath),
		dbsql.WithArrowNativeDecimal(true),
		login.connOption(loginName, d),
	}

	// An empty entry adds nothing to the User-Agent, so it is passed as is.
	opts = append(opts, dbsql.WithUserAgentEntry(d.userAgent))

	// Validate and set custom port
	// Defaults to 443
	if d.port != 0 {
		opts = append(opts, dbsql.WithPort(d.port))
	} else {
		opts = append(opts, dbsql.WithPort(DEFAULT_PORT))
	}

	// Default namespace for queries (catalog/schema)
	if d.catalog != "" || d.schema != "" {
		opts = append(opts, dbsql.WithInitialNamespace(d.catalog, d.schema))
	}

	if d.queryTimeout > 0 {
		opts = append(opts, dbsql.WithTimeout(d.queryTimeout))
	}

	if d.maxRows > 0 {
		opts = append(opts, dbsql.WithMaxRows(int(d.maxRows)))
	}
	if d.queryRetryCount >= 0 {
		opts = append(opts, dbsql.WithRetries(d.queryRetryCount, DEFAULT_RETRY_WAIT_MIN, DEFAULT_RETRY_WAIT_MAX))
	}
	if d.downloadThreadCount > 0 {
		opts = append(opts, dbsql.WithMaxDownloadThreads(d.downloadThreadCount))
	}

	// TLS/SSL handling
	// Configure a custom transport with proper timeout settings when custom
	// TLS config is needed. These settings match the defaults from
	// databricks-sql-go's PooledTransport to ensure reliable connections
	// for large result set downloads.
	if d.sslCertPool != nil || d.sslInsecure {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		if d.sslCertPool != nil {
			tlsConfig.RootCAs = d.sslCertPool
		}

		if d.sslInsecure {
			tlsConfig.InsecureSkipVerify = true
		}

		transport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSClientConfig:       tlsConfig,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       180 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			MaxIdleConnsPerHost:   10,
			MaxConnsPerHost:       100,
		}
		opts = append(opts, dbsql.WithTransport(transport))
	}

	// databricks-sql-go keeps the map it is given and WithQueryTags adds QUERY_TAGS to
	// it, so pass a copy. Applied before the query tags, so that the query tag options
	// win over a raw "databricks.session_param.QUERY_TAGS".
	if len(d.sessionParams) > 0 {
		params := maps.Clone(d.sessionParams)
		if len(d.queryTags) > 0 {
			// databricks-sql-go sends the keys as given, so a raw "query_tags"
			// spelled in another case would be sent next to the QUERY_TAGS built
			// from the query tag options; drop it whatever its case.
			for name := range params {
				if strings.EqualFold(name, sessionParamQueryTags) {
					delete(params, name)
				}
			}
		}
		opts = append(opts, dbsql.WithSessionParams(params))
	}

	if len(d.queryTags) > 0 {
		opts = append(opts, dbsql.WithQueryTags(d.queryTags))
	}

	return opts, nil
}

func (d *databaseImpl) resolveConnectionDSN() (string, error) {
	dsn := d.uri
	if !strings.HasPrefix(dsn, "https://") && !strings.HasPrefix(dsn, "http://") {
		dsn = "https://" + dsn
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "invalid connection URI"}
	}
	parameters, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return "", adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "invalid connection URI query parameters"}
	}
	if !parameters.Has("useArrowNativeDecimal") {
		parameters.Set("useArrowNativeDecimal", "true")
	}
	parsed.RawQuery = parameters.Encode()
	return parsed.String(), nil
}

func (d *databaseImpl) initializeConnectionPool(ctx context.Context) (*sql.DB, error) {
	var db *sql.DB

	// Use URI if provided
	if d.uri != "" {
		dsn, err := d.resolveConnectionDSN()
		if err != nil {
			return nil, err
		}
		db, err = sql.Open("databricks", dsn)
		if err != nil {
			return nil, err
		}
	} else {
		opts, err := d.resolveConnectionOptions()
		if err != nil {
			return nil, err
		}

		connector, err := dbsql.NewConnector(opts...)
		if err != nil {
			return nil, err
		}

		db = sql.OpenDB(connector)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		err = errors.Join(err, db.Close())
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to ping database: %v", err),
		}
	}

	return db, nil
}

// getOrCreatePool returns the connection pool, creating it first when it does
// not exist yet or an option changed. Callers share one start-up: the lock is
// held while the pool is created, so concurrent callers wait for it and reuse it.
func (d *databaseImpl) getOrCreatePool(ctx context.Context) (*sql.DB, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.db != nil && !d.needsRefresh {
		return d.db, nil
	}

	newPool := d.initializeConnectionPool
	if d.newPool != nil {
		newPool = d.newPool
	}
	db, err := newPool(ctx)
	if err != nil {
		return nil, err
	}

	// Close the existing connection pool
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	d.db = db
	d.needsRefresh = false
	return d.db, nil
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.ConnectionWithContext, error) {
	db, err := d.getOrCreatePool(ctx)
	if err != nil {
		return nil, err
	}

	c, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	// The connection starts from the options as they are now; read them under the
	// lock that SetOption writes them under.
	d.mu.Lock()
	catalog, schema, queryTags := d.catalog, d.schema, maps.Clone(d.queryTags)
	d.mu.Unlock()

	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		catalog:            catalog,
		dbSchema:           schema,
		queryTags:          queryTags,
		conn:               c,
	}

	return driverbase.NewConnectionBuilder(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		WithDbObjectsEnumerator(conn).
		WithDriverInfoPreparer(conn).
		Connection(), nil
}

func (d *databaseImpl) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	defer func() {
		d.needsRefresh = true
		d.db = nil
	}()
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *databaseImpl) GetOption(ctx context.Context, key string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.getOption(ctx, key)
}

func (d *databaseImpl) getOption(ctx context.Context, key string) (string, error) {
	if def, ok := databaseOptions[key]; ok {
		return def.get(d), nil
	}
	if name, ok := sessionParamName(key); ok {
		if v, ok := d.sessionParams[name]; ok {
			return v, nil
		}
		return "", d.ErrorHelper.Errorf(adbc.StatusNotFound, "session parameter not set: %s", name)
	}
	if name, ok := queryTagName(key); ok {
		if v, ok := d.queryTags[name]; ok {
			return v, nil
		}
		return "", queryTagNotSet(&d.ErrorHelper, name)
	}
	return d.DatabaseImplBase.GetOption(ctx, key)
}

func (d *databaseImpl) SetOptions(ctx context.Context, options map[string]string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// We need to re-initialize the db/connection pool if options change
	d.needsRefresh = true

	hasURI := false
	hasOtherOptions := false

	if _, ok := options[adbc.OptionKeyURI]; ok {
		hasURI = true
	}

	if len(options) > 1 || (len(options) == 1 && !hasURI) {
		hasOtherOptions = true
	}

	if hasURI && hasOtherOptions {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "cannot specify both URI and individual connection options",
		}
	}

	for k, v := range options {
		err := d.setOption(ctx, k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

// SetOption and GetOption share d.mu with pool start-up, so they wait for a
// login that is in progress and never race with it.
func (d *databaseImpl) SetOption(ctx context.Context, key, value string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.setOption(ctx, key, value)
}

func (d *databaseImpl) setOption(ctx context.Context, key, value string) error {
	// We need to re-initialize the db/connection pool if options change
	d.needsRefresh = true
	if def, ok := databaseOptions[key]; ok {
		return def.set(d, value)
	}
	if name, ok := sessionParamName(key); ok {
		return d.setSessionParam(name, value)
	}
	if name, ok := queryTagName(key); ok {
		return d.setQueryTag(name, value)
	}
	return d.DatabaseImplBase.SetOption(ctx, key, value)
}
