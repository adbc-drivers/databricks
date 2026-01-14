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
	"database/sql/driver"
	"errors"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
)

type statementImpl struct {
	driverbase.StatementImplBase
	conn              *connectionImpl
	query             string
	prepared          *sql.Stmt
	boundStream       array.RecordReader
	bulkIngestOptions driverbase.BulkIngestOptions
}

func (s *statementImpl) Close() error {
	if s.conn == nil {
		return s.ErrorHelper.Errorf(adbc.StatusInvalidState, "statement already closed")
	}
	if s.boundStream != nil {
		s.boundStream.Release()
		s.boundStream = nil
	}
	if s.prepared != nil {
		if err := s.prepared.Close(); err != nil {
			return err
		}
		s.prepared = nil
	}
	s.conn = nil
	return nil
}

func (s *statementImpl) SetOption(key, val string) error {
	if handled, err := s.bulkIngestOptions.SetOption(&s.ErrorHelper, key, val); err != nil {
		return err
	} else if handled {
		return nil
	}

	return s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "unsupported statement option: %s=%s", key, val)
}

func (s *statementImpl) SetSqlQuery(query string) error {
	s.query = query
	// Reset prepared statement if query changes
	if s.prepared != nil {
		if err := s.prepared.Close(); err != nil {
			return s.ErrorHelper.Errorf(adbc.StatusInvalidState, "failed to close previous prepared statement: %v", err)
		}
		s.prepared = nil
	}
	return nil
}

func (s *statementImpl) Prepare(ctx context.Context) error {
	if s.query == "" {
		return s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}

	stmt, err := s.conn.conn.PrepareContext(ctx, s.query)
	if err != nil {
		return s.ErrorHelper.Errorf(adbc.StatusInvalidState, "failed to prepare statement: %v", err)
	}

	s.prepared = stmt
	return nil
}

func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.boundStream != nil {
		return nil, -1, s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "parameterized queries not yet implemented")
	}

	if s.query == "" {
		return nil, -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}

	// Execute query using raw driver interface to get Arrow batches
	// This works for both prepared and unprepared statements since
	// databricks-sql-go doesn't do server-side preparation
	var driverRows driver.Rows
	var err error
	err = s.conn.conn.Raw(func(driverConn interface{}) error {
		// Use raw driver interface for direct Arrow access
		queryerCtx := driverConn.(driver.QueryerContext)
		var driverArgs []driver.NamedValue
		driverRows, err = queryerCtx.QueryContext(ctx, s.query, driverArgs)
		return err
	})

	if err != nil {
		return nil, -1, s.ErrorHelper.Errorf(adbc.StatusInternal, "failed to execute query: %v", err)
	}

	defer func() {
		if closeErr := driverRows.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	// Convert to databricks rows interface to get Arrow batches
	databricksRows, ok := driverRows.(dbsqlrows.Rows)
	if !ok {
		return nil, -1, s.ErrorHelper.Errorf(adbc.StatusInternal, "driver rows do not support Arrow batches")
	}

	// Use the IPC stream interface (zero-copy)
	reader, err := newIPCReaderAdapter(ctx, databricksRows)
	if err != nil {
		return nil, -1, s.ErrorHelper.Errorf(adbc.StatusInternal, "failed to create IPC reader adapter: %v", err)
	}

	// Return -1 for rowsAffected (unknown) since we can't count without consuming
	// The ADBC spec allows -1 to indicate "unknown number of rows affected"
	return reader, -1, nil
}

func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	if s.bulkIngestOptions.IsSet() {
		return s.executeIngest(ctx)
	}

	if s.boundStream != nil {
		return -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "bound data provided but no ingest target set")
	}

	var result sql.Result
	var err error

	if s.prepared != nil {
		result, err = s.prepared.ExecContext(ctx)
	} else if s.query != "" {
		result, err = s.conn.conn.ExecContext(ctx, s.query)
	} else {
		return -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}

	if err != nil {
		return -1, s.ErrorHelper.Errorf(adbc.StatusInternal, "failed to execute update: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return -1, s.ErrorHelper.Errorf(adbc.StatusInternal, "failed to get rows affected: %v", err)
	}

	return rowsAffected, nil
}

func (s *statementImpl) Bind(ctx context.Context, values arrow.RecordBatch) error {
	if s.boundStream != nil {
		s.boundStream.Release()
	}
	stream, err := array.NewRecordReader(values.Schema(), []arrow.RecordBatch{values})
	if err != nil {
		return s.ErrorHelper.Errorf(adbc.StatusInternal, "failed to create record reader")
	}
	s.boundStream = stream
	return nil
}

func (s *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	if s.boundStream != nil {
		s.boundStream.Release()
	}
	stream.Retain()
	s.boundStream = stream
	return nil
}

func (s *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	// This would require parsing the SQL query to determine parameter types
	// For now, return nil to indicate unknown schema
	return nil, s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "parameter schema detection not implemented")
}

func (s *statementImpl) SetSubstraitPlan(plan []byte) error {
	// Databricks SQL doesn't support Substrait plans
	return s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Substrait plans not supported")
}

func (s *statementImpl) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	// Databricks SQL doesn't support partitioned result sets
	return nil, adbc.Partitions{}, -1, s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "partitioned result sets not supported")
}
