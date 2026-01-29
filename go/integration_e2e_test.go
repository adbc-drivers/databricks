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

package databricks_test

import (
	"context"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/adbc-drivers/databricks/go"
	"github.com/adbc-drivers/driverbase-go/validation"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/databricks/databricks-sql-go"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type E2ETests struct {
	BaseTests
}

func TestE2E(t *testing.T) {
	withQuirks(t, func(q *DatabricksQuirks) {
		suite.Run(t, &E2ETests{BaseTests{Quirks: q}})
	})
}

func (suite *E2ETests) TestSimpleQuery() {
	ctx := context.Background()
	suite.Require().NoError(suite.stmt.SetSqlQuery("SELECT 1 as test_column"))
	reader, rowsAffected, err := suite.stmt.ExecuteQuery(ctx)
	suite.Require().NoError(err)
	defer reader.Release()

	suite.T().Logf("✅ Simple query executed successfully, rows affected: %d", rowsAffected)

	// Verify we got a result
	suite.Require().True(reader.Next(), "Expected at least one record")
	record := reader.RecordBatch()
	suite.Require().Equal(int64(1), record.NumCols(), "Expected 1 column")
	suite.Require().Equal(int64(1), record.NumRows(), "Expected 1 row")

	suite.T().Logf("✅ Query result: %d columns, %d rows", record.NumCols(), record.NumRows())
}

// TestE2E_MetadataOperations tests metadata retrieval operations
func (suite *E2ETests) TestMetadataOperations() {
	catalog := suite.Quirks.catalogName
	schema := suite.Quirks.schemaName

	suite.T().Run("GetTableTypes", func(t *testing.T) {
		ctx := context.Background()
		reader, err := suite.cnxn.GetTableTypes(ctx)
		require.NoError(t, err)
		defer reader.Release()

		count := 0
		for reader.Next() {
			record := reader.RecordBatch()
			count += int(record.NumRows())
			suite.T().Logf("Table types record: %d rows", record.NumRows())
		}
		suite.Require().Greater(count, 0, "Expected some table types")
		suite.T().Logf("✅ Found %d table types", count)
	})

	// Test basic metadata queries
	suite.T().Run("ShowCatalogs", func(t *testing.T) {
		ctx := context.Background()
		stmt, err := suite.cnxn.NewStatement()
		require.NoError(t, err)
		defer validation.CheckedClose(suite.T(), stmt)

		err = stmt.SetSqlQuery("SHOW CATALOGS")
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		catalogCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			catalogCount += int(record.NumRows())
			suite.T().Logf("Catalogs record: %d rows", record.NumRows())
		}
		suite.Require().Greater(catalogCount, 0, "Expected some catalogs")
		suite.T().Logf("✅ Found %d catalogs", catalogCount)
	})

	suite.T().Run("ShowSchemas", func(t *testing.T) {
		ctx := context.Background()
		stmt, err := suite.cnxn.NewStatement()
		require.NoError(t, err)
		defer validation.CheckedClose(suite.T(), stmt)

		query := fmt.Sprintf("SHOW SCHEMAS IN %s", catalog)
		err = stmt.SetSqlQuery(query)
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		schemaCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			schemaCount += int(record.NumRows())
			suite.T().Logf("Schemas record: %d rows", record.NumRows())
		}
		suite.Require().Greater(schemaCount, 0, "Expected some schemas")
		suite.T().Logf("✅ Found %d schemas in catalog %s", schemaCount, catalog)
	})

	suite.T().Run("ShowTables", func(t *testing.T) {
		ctx := context.Background()
		stmt, err := suite.cnxn.NewStatement()
		require.NoError(t, err)
		defer validation.CheckedClose(suite.T(), stmt)

		query := fmt.Sprintf("SHOW TABLES IN %s.%s", catalog, schema)
		err = stmt.SetSqlQuery(query)
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		tableCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			tableCount += int(record.NumRows())
			suite.T().Logf("Tables record: %d rows", record.NumRows())
		}
		// Note: schema might be empty, so we don't assert on table count
		suite.T().Logf("✅ Found %d tables in schema %s.%s", tableCount, catalog, schema)
	})
}

func (suite *E2ETests) TestConnectionOptions() {
	tests := []struct {
		name    string
		options map[string]string
	}{
		{
			name: "with_timeout",
			options: map[string]string{
				databricks.OptionQueryTimeout: "30s",
			},
		},
		{
			name: "with_max_rows",
			options: map[string]string{
				databricks.OptionMaxRows: "100",
			},
		},
	}

	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			driver := databricks.NewDriver(memory.DefaultAllocator)

			opts := map[string]string{}
			maps.Copy(opts, suite.Quirks.DatabaseOptions())
			maps.Copy(opts, tt.options)

			db, err := driver.NewDatabase(opts)
			require.NoError(t, err)
			defer validation.CheckedClose(suite.T(), db)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			conn, err := db.Open(ctx)
			require.NoError(t, err)
			defer validation.CheckedClose(suite.T(), conn)

			// Execute a simple query to verify the connection works
			stmt, err := conn.NewStatement()
			require.NoError(t, err)
			defer validation.CheckedClose(suite.T(), stmt)

			err = stmt.SetSqlQuery("SELECT 1")
			require.NoError(t, err)

			reader, _, err := stmt.ExecuteQuery(ctx)
			require.NoError(t, err)
			defer reader.Release()

			suite.Require().True(reader.Next(), "Expected at least one record")
			suite.T().Logf("✅ Connection with %s options successful", tt.name)
		})
	}
}
