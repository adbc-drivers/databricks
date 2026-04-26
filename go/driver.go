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

	// OAuth options (for future expansion)
	OptionOAuthClientID     = "databricks.oauth.client_id"
	OptionOAuthClientSecret = "databricks.oauth.client_secret"
	OptionOAuthRefreshToken = "databricks.oauth.refresh_token"

	// Arrow serialization options
	OptionArrowNativeGeospatial = "databricks.arrow.native_geospatial"

	// Bulk ingest options.  When OptionBulkVolumePath is set, executeIngest
	// streams Arrow batches as Parquet files to a Databricks Volume and
	// applies COPY INTO (one COPY per Arrow batch) instead of issuing a
	// per-row INSERT.  The path must be a writable Volume directory the
	// connection's principal has access to, e.g.
	//   /Volumes/<catalog>/<schema>/<volume>/<subdir>
	OptionBulkVolumePath = "databricks.bulk.volume_path"

	// OptionBulkGeometryColumns is a comma-separated list of
	// `column:srid` pairs that tells the bulk-ingest path which Arrow
	// BINARY columns are WKB-encoded geometries and which SRID to apply.
	// The destination columns will be created as GEOMETRY(srid) and the
	// COPY INTO transform projects each through ST_GEOMFROMWKB so the
	// staged BINARY rebuilds as a typed geometry on insert.
	//
	// Example: "geom:4326" or "boundary:3857,centroid:4326".
	//
	// Required only when the source emits geometry as plain BINARY with
	// no geoarrow.wkb extension metadata.  When the source already
	// annotates the column (e.g., DuckDB's adbc_scanner emits PROJJSON
	// CRS), the driver auto-detects and the hint is unnecessary.
	OptionBulkGeometryColumns = "databricks.bulk.geometry_columns"

	// OptionBulkBatchRows is the target number of rows to accumulate per
	// staged Parquet file before issuing COPY INTO.  The bulk path holds
	// a single Parquet writer open across multiple Arrow record batches
	// so per-batch fixed cost (HTTP upload + COPY INTO planning) is
	// amortized over many rows.  Default: 20000.  Tuning above ~50000
	// rarely helps further; tuning below ~5000 sacrifices throughput
	// without saving meaningful memory.
	OptionBulkBatchRows = "databricks.bulk.batch_rows"

	// DefaultBulkBatchRows is used when OptionBulkBatchRows is unset.
	DefaultBulkBatchRows = 20000

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
func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("Databricks")

	if err := info.RegisterInfoCode(adbc.InfoDriverName, "ADBC Driver Foundry Driver for Databricks"); err != nil {
		panic(err)
	}

	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: driverbase.NewDriverImplBase(info, alloc),
	})
}

func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase)
	if err != nil {
		return nil, err
	}

	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		port:             DefaultPort,
		sslMode:          DefaultSSLMode,
	}

	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
