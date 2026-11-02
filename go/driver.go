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

// Package databricks is an ADBC Driver Implementation for Databricks
// SQL using databricks-sql-go as the underlying SQL driver.
//
// It can be used to register a driver for database/sql by importing
// github.com/apache/arrow-adbc/go/adbc/sqldriver and running:
//
//	sql.Register("databricks", sqldriver.Driver{databricks.Driver{}})
//
// You can then open a databricks connection with the database/sql
// standard package by using:
//
//	db, err := sql.Open("databricks", "token=<token>&hostname=<hostname>&port=<port>&httpPath=<path>")
package databricks

import (
	"context"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

const (
	// Connection options
	OptionServerHostname = "databricks.server_hostname"
	OptionHTTPPath       = "databricks.http_path"
	OptionAccessToken    = "databricks.access_token"
	OptionPort           = "databricks.port"
	OptionCatalog        = "databricks.catalog"
	OptionSchema         = "databricks.schema"

	// Query options
	OptionQueryTimeout        = "databricks.query.timeout"
	OptionMaxRows             = "databricks.query.max_rows"
	OptionQueryRetryCount     = "databricks.query.retry_count"
	OptionDownloadThreadCount = "databricks.download_thread_count"

	// TLS/SSL options
	OptionSSLMode     = "databricks.ssl_mode"
	OptionSSLRootCert = "databricks.ssl_root_cert"

	// Authentication. When auth_type is not set, the method is inferred from the
	// other options that are set.
	OptionAuthType              = "databricks.auth_type"
	OptionValueAuthTypePAT      = "pat"
	OptionValueAuthTypeOAuthM2M = "oauth-m2m"
	OptionValueAuthTypeEnvOIDC  = "env-oidc"
	OptionValueAuthTypeFileOIDC = "file-oidc"

	OptionValueAuthTypeAzureClientSecret = "azure-client-secret"
	OptionValueAuthTypeAzureMSI          = "azure-msi"
	OptionValueAuthTypeAzureCLI          = "azure-cli"
	OptionValueAuthTypeGoogleCredentials = "google-credentials"
	OptionValueAuthTypeGoogleID          = "google-id"

	// OAuth options
	OptionOAuthClientID     = "databricks.oauth.client_id"
	OptionOAuthClientSecret = "databricks.oauth.client_secret"
	OptionOAuthRefreshToken = "databricks.oauth.refresh_token"
	OptionOAuthScopes       = "databricks.oauth.scopes"

	// Query tags attached to executed statements, e.g. "databricks.query_tag.team".
	// Settable on the database (defaults) and on a statement (overrides the default per tag).
	OptionQueryTagPrefix = "databricks.query_tag."

	// Azure service principal (Microsoft Entra ID) options, auth type azure-client-secret
	OptionAzureClientID     = "databricks.azure.client_id"
	OptionAzureClientSecret = "databricks.azure.client_secret"
	OptionAzureTenantID     = "databricks.azure.tenant_id"

	// Google options, auth types google-credentials and google-id
	OptionGoogleCredentials    = "databricks.google.credentials"
	OptionGoogleServiceAccount = "databricks.google.service_account"

	// OIDC token federation options (auth types env-oidc and file-oidc)
	OptionOIDCClientID      = "databricks.oidc.client_id"
	OptionOIDCTokenFilepath = "databricks.oidc.token_filepath"

	// Default values
	DefaultPort    = 443
	DefaultSSLMode = "require"
)

func init() {
	// databricks-go sends logs to zerolog; disable them
	zerolog.SetGlobalLevel(zerolog.Disabled)
}

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new Databricks driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) driverbase.DriverWithContext {
	info := driverbase.DefaultDriverInfo("Databricks")

	if err := info.RegisterInfoCode(adbc.InfoDriverName, "ADBC Driver Foundry Driver for Databricks"); err != nil {
		panic(err)
	}

	base := driverbase.NewDriverImplBase(info, alloc)
	base.ErrorHelper.ErrorInspector = DatabricksErrorInspector{}
	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: base,
	})
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.DatabaseWithContext, error) {
	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase, driverbase.TracingOptions{})
	if err != nil {
		return nil, err
	}

	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		port:             DefaultPort,
		sslMode:          DefaultSSLMode,
	}

	if err := db.SetOptions(ctx, opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
