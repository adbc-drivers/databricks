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
	if s.query == "" {
		return nil, -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}
	if s.boundStream != nil {
		stream := s.boundStream
		s.boundStream = nil
		conn := s.conn.conn
		query := s.query
		errorHelper := s.ErrorHelper
		iterator, err := newParameterRowIterator(stream, parameterBindingModeForQuery(query))
		if err != nil {
			return nil, -1, err
		}
		reader, err := newParameterizedQueryReader(iterator, func(args []driver.NamedValue) (array.RecordReader, error) {
			return executeQuery(ctx, conn, query, args, errorHelper)
		})
		if err != nil {
			return nil, -1, err
		}
		return reader, -1, nil
	}

	reader, err := s.executeQuery(ctx, nil)
	return reader, -1, err
}

func (s *statementImpl) executeQuery(ctx context.Context, args []driver.NamedValue) (array.RecordReader, error) {
	return executeQuery(ctx, s.conn.conn, s.query, args, s.ErrorHelper)
}

func executeQuery(ctx context.Context, conn *sql.Conn, query string, args []driver.NamedValue, errorHelper driverbase.ErrorHelper) (array.RecordReader, error) {
	// Execute query using raw driver interface to get Arrow batches
	// This works for both prepared and unprepared statements since
	// databricks-sql-go doesn't do server-side preparation
	var driverRows driver.Rows
	var err error
	err = conn.Raw(func(driverConn interface{}) error {
		// Use raw driver interface for direct Arrow access
		queryerCtx, ok := driverConn.(driver.QueryerContext)
		if !ok {
			return errors.New("driver does not support context-aware queries")
		}
		driverRows, err = queryerCtx.QueryContext(ctx, query, args)
		return err
	})

	if err != nil {
		return nil, errorHelper.Errorf(adbc.StatusInternal, "failed to execute query: %v", err)
	}

	defer func() {
		if driverRows == nil {
			return
		}
		if closeErr := driverRows.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	// Use the IPC stream interface (zero-copy)
	reader, err := newIPCReaderAdapter(ctx, driverRows)
	if err != nil {
		return nil, errorHelper.Errorf(adbc.StatusInternal, "failed to create IPC reader adapter: %v", err)
	}
	driverRows = nil // Prevent double close in defer

	// Return -1 for rowsAffected (unknown) since we can't count without consuming
	// The ADBC spec allows -1 to indicate "unknown number of rows affected"
	return reader, nil
}

func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	if s.bulkIngestOptions.IsSet() {
		return s.executeIngest(ctx)
	}

	if s.query == "" {
		return -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}
	if s.boundStream == nil {
		return s.executeUpdate(ctx, nil)
	}

	stream := s.boundStream
	s.boundStream = nil
	iterator, err := newParameterRowIterator(stream, parameterBindingModeForQuery(s.query))
	if err != nil {
		return -1, err
	}
	defer iterator.Release()

	var totalRows int64
	executed := false
	unknown := false
	for {
		args, ok, err := iterator.Next()
		if err != nil {
			return -1, err
		}
		if !ok {
			break
		}
		executed = true
		rows, err := s.executeUpdate(ctx, args)
		if err != nil {
			return -1, err
		}
		if rows < 0 {
			unknown = true
		} else if !unknown {
			totalRows += rows
		}
	}
	if !executed {
		return -1, s.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "parameter stream contains no rows")
	}
	if unknown {
		return -1, nil
	}
	return totalRows, nil
}

func (s *statementImpl) executeUpdate(ctx context.Context, args []driver.NamedValue) (int64, error) {
	values := make([]any, len(args))
	for i := range args {
		values[i] = args[i].Value
	}

	var result sql.Result
	var err error
	if s.prepared != nil {
		result, err = s.prepared.ExecContext(ctx, values...)
	} else {
		result, err = s.conn.conn.ExecContext(ctx, s.query, values...)
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
	if values == nil {
		if s.boundStream != nil {
			s.boundStream.Release()
			s.boundStream = nil
		}
		return nil
	}
	stream, err := array.NewRecordReader(values.Schema(), []arrow.RecordBatch{values})
	if err != nil {
		return s.ErrorHelper.Errorf(adbc.StatusInternal, "failed to create record reader")
	}
	if s.boundStream != nil {
		s.boundStream.Release()
	}
	s.boundStream = stream
	return nil
}

func (s *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	if stream != nil {
		stream.Retain()
	}
	if s.boundStream != nil {
		s.boundStream.Release()
	}
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
