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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	_ "github.com/databricks/databricks-sql-go"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	// Connection settings
	catalog  string
	dbSchema string

	// Database connection
	conn *sql.Conn

	// Staging client for bulk ingest file operations
	stagingClient *stagingClient
}

func (c *connectionImpl) Close() error {
	if c.conn == nil {
		return adbc.Error{Code: adbc.StatusInvalidState}
	}
	defer func() {
		c.conn = nil
	}()
	return c.conn.Close()
}

func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statementImpl{
		StatementImplBase: driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper),
		conn:              c,
		bulkIngestOptions: driverbase.NewBulkIngestOptions(),
	}, nil
}

func (c *connectionImpl) SetAutocommit(autocommit bool) error {
	// Databricks SQL doesn't support explicit transaction control in the same way
	// as traditional databases. Most operations are implicitly committed.
	if !autocommit {
		return adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  "disabling autocommit is not supported",
		}
	}
	return nil
}

// CurrentNamespacer interface implementation
func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	if c.catalog != "" {
		return c.catalog, nil
	}

	if c.conn == nil {
		return "", adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "connection is nil",
		}
	}

	var catalog string
	err := c.conn.QueryRowContext(context.Background(), "SELECT current_catalog()").Scan(&catalog)
	if err != nil {
		return "", adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to get current catalog: %v", err),
		}
	}

	return catalog, nil
}

func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	if c.dbSchema != "" {
		return c.dbSchema, nil
	}

	if c.conn == nil {
		return "", adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "connection is nil",
		}
	}

	var schema string
	err := c.conn.QueryRowContext(context.Background(), "SELECT current_schema()").Scan(&schema)
	if err != nil {
		return "", adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to get current schema: %v", err),
		}
	}

	return schema, nil
}

func (c *connectionImpl) SetCurrentCatalog(catalog string) error {
	if catalog == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "catalog cannot be empty",
		}
	}
	if c.conn == nil {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "failed to set catalog: connection is nil",
		}
	}
	escapedCatalog := strings.ReplaceAll(catalog, "`", "``")
	_, err := c.conn.ExecContext(context.Background(), fmt.Sprintf("USE CATALOG `%s`", escapedCatalog))
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to set catalog: %v", err),
		}
	}
	c.catalog = catalog
	return nil
}

func (c *connectionImpl) SetCurrentDbSchema(schema string) error {
	if schema == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "schema cannot be empty",
		}
	}
	if c.conn == nil {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "failed to set db schema: connection is nil",
		}
	}
	escapedSchema := strings.ReplaceAll(schema, "`", "``")
	_, err := c.conn.ExecContext(context.Background(), fmt.Sprintf("USE SCHEMA `%s`", escapedSchema))
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to set schema: %v", err),
		}
	}
	c.dbSchema = schema
	return nil
}

// TableTypeLister interface implementation
func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	// Databricks supports these table types
	return []string{"TABLE", "VIEW", "EXTERNAL_TABLE", "MANAGED_TABLE", "STREAMING_TABLE", "MATERIALIZED_VIEW"}, nil
}

// Transaction methods (Databricks has limited transaction support)
func (c *connectionImpl) Commit(ctx context.Context) error {
	// Most operations are auto-committed.
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Commit is not supported",
	}
}

func (c *connectionImpl) Rollback(ctx context.Context) error {
	// Databricks SQL doesn't support explicit transactions in the traditional sense.
	// Most operations are auto-committed.
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "rollback is not supported",
	}
}

// DbObjectsEnumerator interface implementation
func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) (catalogs []string, err error) {
	catalogs = []string{}
	query := "SHOW CATALOGS"
	if catalogFilter != nil {
		escapedFilter := strings.ReplaceAll(*catalogFilter, "'", "''")
		query += fmt.Sprintf(" LIKE '%s'", escapedFilter)
	}
	var rows *sql.Rows
	rows, err = c.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to query catalogs: %v", err),
		}
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	for rows.Next() {
		var catalog string
		if err := rows.Scan(&catalog); err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("failed to scan catalog: %v", err),
			}
		}
		catalogs = append(catalogs, catalog)
	}

	return catalogs, errors.Join(err, rows.Err())
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) (schemas []string, err error) {
	schemas = []string{}
	escapedCatalog := strings.ReplaceAll(catalog, "`", "``")
	query := fmt.Sprintf("SHOW SCHEMAS IN `%s`", escapedCatalog)
	if schemaFilter != nil {
		escapedFilter := strings.ReplaceAll(*schemaFilter, "'", "''")
		query += fmt.Sprintf(" LIKE '%s'", escapedFilter)
	}

	var rows *sql.Rows
	rows, err = c.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to query schemas: %v", err),
		}
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("failed to scan schema: %v", err),
			}
		}
		schemas = append(schemas, schema)
	}

	err = errors.Join(err, rows.Err())
	return schemas, err
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) (tables []driverbase.TableInfo, err error) {
	if includeColumns {
		return c.getTablesWithColumns(ctx, catalog, schema, tableFilter, columnFilter)
	}

	tables = []driverbase.TableInfo{}
	escapedCatalog := strings.ReplaceAll(catalog, "`", "``")
	escapedSchema := strings.ReplaceAll(schema, "`", "``")
	query := fmt.Sprintf("SHOW TABLES IN `%s`.`%s`", escapedCatalog, escapedSchema)
	if tableFilter != nil {
		escapedFilter := strings.ReplaceAll(*tableFilter, "'", "''")
		query += fmt.Sprintf(" LIKE '%s'", escapedFilter)
	}

	var rows *sql.Rows
	rows, err = c.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to query tables: %v", err),
		}
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	for rows.Next() {
		var database, tableName, isTemporary string
		if err := rows.Scan(&database, &tableName, &isTemporary); err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("failed to scan table: %v", err),
			}
		}

		tableInfo := driverbase.TableInfo{
			TableName:        tableName,
			TableType:        "TABLE", // Default to TABLE, could be improved with more detailed queries
			TableColumns:     []driverbase.ColumnInfo{},
			TableConstraints: []driverbase.ConstraintInfo{},
		}

		tables = append(tables, tableInfo)
	}

	return tables, errors.Join(err, rows.Err())
}

// getTablesWithColumns retrieves complete table and column information using INFORMATION_SCHEMA
func (c *connectionImpl) getTablesWithColumns(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string) (tables []driverbase.TableInfo, err error) {
	tables = []driverbase.TableInfo{}

	lowerCatalog := strings.ToLower(catalog)

	// Skip internal catalogs that do not support metadata queries
	if lowerCatalog == "__databricks_internal" {
		return tables, nil
	}

	var queryBuilder strings.Builder
	queryBuilder.WriteString("SELECT DISTINCT c.TABLE_NAME, c.ordinal_position, c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE FROM ")
	if lowerCatalog == "hive_metastore" || lowerCatalog == "system" {
		// Hive Metastore and system catalog metadata are only available via the system-level information_schema
		queryBuilder.WriteString("system.information_schema.COLUMNS c ")
		queryBuilder.WriteString("WHERE c.table_catalog = ")
		queryBuilder.WriteString(quoteString(catalog))
		queryBuilder.WriteString(" AND c.TABLE_SCHEMA = ")
		queryBuilder.WriteString(quoteString(schema))
	} else {
		// Unity Catalog catalogs have their own information_schema
		queryBuilder.WriteString(quoteIdentifier(catalog))
		queryBuilder.WriteString(".information_schema.COLUMNS c WHERE c.TABLE_SCHEMA = ")
		queryBuilder.WriteString(quoteString(schema))
	}

	if tableFilter != nil {
		queryBuilder.WriteString(" AND c.TABLE_NAME LIKE ")
		queryBuilder.WriteString(quoteString(*tableFilter))
	}
	if columnFilter != nil {
		queryBuilder.WriteString(" AND c.COLUMN_NAME LIKE ")
		queryBuilder.WriteString(quoteString(*columnFilter))
	}

	queryBuilder.WriteString(" ORDER BY c.TABLE_NAME, c.ordinal_position")

	rows, err := c.conn.QueryContext(ctx, queryBuilder.String())
	if err != nil {
		// If we don't have permissions on the catalog, this will
		// error. Catch that and simply return no tables instead of
		// blowing up.
		var dbExecutionErr dbsqlerr.DBExecutionError
		if errors.As(err, &dbExecutionErr) && dbExecutionErr.SqlState() == "42501" {
			return tables, nil
		}
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to query tables with columns: %v", err),
		}
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	var currentTable *driverbase.TableInfo

	for rows.Next() {
		var tableName, columnName, dataType, isNullable string
		var ordinalPosition sql.NullInt32

		if err := rows.Scan(
			&tableName,
			&ordinalPosition, &columnName,
			&dataType, &isNullable,
		); err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("failed to scan table with columns: %v", err),
			}
		}

		// Check if we need to create a new table entry
		if currentTable == nil || currentTable.TableName != tableName {
			tables = append(tables, driverbase.TableInfo{
				TableName:        tableName,
				TableType:        "TABLE",
				TableColumns:     []driverbase.ColumnInfo{},
				TableConstraints: []driverbase.ConstraintInfo{},
			})
			currentTable = &tables[len(tables)-1]
		}

		var nullable *int16
		var isNullablePtr *string
		switch isNullable {
		case "YES":
			n := int16(driverbase.XdbcColumnNullable)
			nullable = &n
			isNullablePtr = &isNullable
		case "NO":
			n := int16(driverbase.XdbcColumnNoNulls)
			nullable = &n
			isNullablePtr = &isNullable
		}

		columnInfo := driverbase.ColumnInfo{
			ColumnName:     columnName,
			XdbcTypeName:   &dataType,
			XdbcNullable:   nullable,
			XdbcIsNullable: isNullablePtr,
		}

		if ordinalPosition.Valid {
			// Databricks uses 0-based indexing
			pos := ordinalPosition.Int32 + 1
			columnInfo.OrdinalPosition = &pos
		}

		currentTable.TableColumns = append(currentTable.TableColumns, columnInfo)
	}

	return tables, errors.Join(err, rows.Err())
}

// PrepareDriverInfo implements driverbase.DriverInfoPreparer.
func (c *connectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	var versionJSON string
	err := c.conn.QueryRowContext(ctx, "SELECT current_version()").Scan(&versionJSON)
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to get vendor version: %v", err),
		}
	}

	var versionData map[string]any
	if err := json.Unmarshal([]byte(versionJSON), &versionData); err != nil {
		return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, "unknown")
	}

	version := "unknown"
	if dbsqlVersion, ok := versionData["dbsql_version"].(string); ok && dbsqlVersion != "" {
		version = dbsqlVersion
	} else if dbrVersion, ok := versionData["dbr_version"].(string); ok {
		version = dbrVersion
	}

	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, version)
}

// quoteString escapes string literals using single quotes
func quoteString(value string) string {
	return fmt.Sprintf("'%s'", strings.ReplaceAll(value, "'", "''"))
}
