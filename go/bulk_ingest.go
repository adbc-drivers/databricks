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
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
)

// databricksBulkIngest implements driverbase.BulkIngestImpl for the
// Databricks Staging + COPY INTO pattern. It uploads Parquet files to a
// Unity Catalog Volume via the Databricks Files API, then uses COPY INTO
// to load them into the target table.
type databricksBulkIngest struct {
	conn          *connectionImpl
	stagingClient *stagingClient
	errorHelper   *driverbase.ErrorHelper
	options       *driverbase.BulkIngestOptions
}

// pendingCopy tracks a file uploaded to staging that is ready for COPY INTO.
// It implements driverbase.BulkIngestPendingCopy.
type pendingCopy struct {
	path string
	rows int64
}

func (p *pendingCopy) String() string { return p.path }
func (p *pendingCopy) Rows() int64    { return p.rows }

// CreateSink returns an in-memory buffer for Parquet data to be written to.
func (bi *databricksBulkIngest) CreateSink(ctx context.Context, options *driverbase.BulkIngestOptions) (driverbase.BulkIngestSink, error) {
	return &driverbase.BufferBulkIngestSink{}, nil
}

// CreateTable creates or drops/recreates the target table based on the
// specified table existence and missing behaviors.
func (bi *databricksBulkIngest) CreateTable(ctx context.Context, schema *arrow.Schema, ifTableExists driverbase.BulkIngestTableExistsBehavior, ifTableMissing driverbase.BulkIngestTableMissingBehavior) error {
	tableName := buildTableName(bi.options.CatalogName, bi.options.SchemaName, bi.options.TableName)

	if ifTableExists == driverbase.BulkIngestTableExistsDrop {
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
		if _, err := bi.conn.conn.ExecContext(ctx, dropSQL); err != nil {
			return bi.errorHelper.Errorf(adbc.StatusInternal, "failed to drop table: %v", err)
		}
	}

	if ifTableMissing == driverbase.BulkIngestTableMissingCreate {
		ifNotExists := ifTableExists == driverbase.BulkIngestTableExistsIgnore
		return bi.createTableDDL(ctx, tableName, schema, ifNotExists)
	}

	return nil
}

// createTableDDL generates and executes CREATE TABLE DDL from an Arrow schema.
func (bi *databricksBulkIngest) createTableDDL(ctx context.Context, tableName string, schema *arrow.Schema, ifNotExists bool) error {
	var sql strings.Builder
	sql.WriteString("CREATE TABLE ")
	if ifNotExists {
		sql.WriteString("IF NOT EXISTS ")
	}
	sql.WriteString(tableName)
	sql.WriteString(" (")

	for i, field := range schema.Fields() {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(quoteIdentifier(field.Name))
		sql.WriteString(" ")
		sql.WriteString(arrowTypeToDatabricksType(field.Type))
		if !field.Nullable {
			sql.WriteString(" NOT NULL")
		}
	}
	sql.WriteString(")")

	_, err := bi.conn.conn.ExecContext(ctx, sql.String())
	if err != nil {
		return bi.errorHelper.Errorf(adbc.StatusInternal, "failed to create table: %v", err)
	}
	return nil
}

// Upload uploads the Parquet data from a buffer to the staging volume.
func (bi *databricksBulkIngest) Upload(ctx context.Context, chunk driverbase.BulkIngestPendingUpload) (driverbase.BulkIngestPendingCopy, error) {
	buf, ok := chunk.Data.(*driverbase.BufferBulkIngestSink)
	if !ok {
		return nil, bi.errorHelper.Errorf(adbc.StatusInternal, "unexpected sink type: %T", chunk.Data)
	}

	path, err := bi.stagingClient.generateFileName()
	if err != nil {
		return nil, bi.errorHelper.Errorf(adbc.StatusInternal, "failed to generate staging file name: %v", err)
	}

	if err := bi.stagingClient.Upload(ctx, path, bytes.NewReader(buf.Bytes())); err != nil {
		return nil, bi.errorHelper.Errorf(adbc.StatusIO, "failed to upload staging file: %v", err)
	}

	return &pendingCopy{path: path, rows: chunk.Rows}, nil
}

// Copy executes COPY INTO to load a staged Parquet file into the target table.
func (bi *databricksBulkIngest) Copy(ctx context.Context, chunk driverbase.BulkIngestPendingCopy) error {
	tableName := buildTableName(bi.options.CatalogName, bi.options.SchemaName, bi.options.TableName)

	// The path from pendingCopy has the form "Volumes/catalog/schema/volume/prefix/file.parquet".
	// COPY INTO expects the path with a leading slash: '/Volumes/...'
	copySQL := fmt.Sprintf(
		"COPY INTO %s FROM '/%s' FILEFORMAT = PARQUET",
		tableName, chunk.String(),
	)

	_, err := bi.conn.conn.ExecContext(ctx, copySQL)
	if err != nil {
		return bi.errorHelper.Errorf(adbc.StatusInternal, "COPY INTO failed: %v", err)
	}
	return nil
}

// Delete removes the staging file after it has been copied into the target table.
func (bi *databricksBulkIngest) Delete(ctx context.Context, chunk driverbase.BulkIngestPendingCopy) error {
	if err := bi.stagingClient.Delete(ctx, chunk.String()); err != nil {
		return bi.errorHelper.Errorf(adbc.StatusIO, "failed to delete staging file: %v", err)
	}
	return nil
}

// buildTableName constructs a fully qualified catalog.schema.table name.
func buildTableName(catalog, schema, table string) string {
	parts := []string{}
	if catalog != "" {
		parts = append(parts, quoteIdentifier(catalog))
	}
	if schema != "" {
		parts = append(parts, quoteIdentifier(schema))
	}
	parts = append(parts, quoteIdentifier(table))
	return strings.Join(parts, ".")
}

// quoteIdentifier quotes a Databricks identifier with backticks.
func quoteIdentifier(id string) string {
	escaped := strings.ReplaceAll(id, "`", "``")
	return fmt.Sprintf("`%s`", escaped)
}

// arrowTypeToDatabricksType maps Arrow types to Databricks DDL types for CREATE TABLE.
func arrowTypeToDatabricksType(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INT"
	case arrow.INT64:
		return "BIGINT"
	case arrow.UINT8:
		return "SMALLINT"
	case arrow.UINT16:
		return "INT"
	case arrow.UINT32:
		return "BIGINT"
	case arrow.UINT64:
		return "BIGINT"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "STRING"
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
		return "BINARY"
	case arrow.DATE32, arrow.DATE64:
		return "DATE"
	case arrow.TIMESTAMP:
		ts := dt.(*arrow.TimestampType)
		if ts.TimeZone != "" {
			return "TIMESTAMP"
		}
		return "TIMESTAMP_NTZ"
	case arrow.DECIMAL128:
		dec := dt.(*arrow.Decimal128Type)
		return fmt.Sprintf("DECIMAL(%d, %d)", dec.Precision, dec.Scale)
	default:
		return "STRING" // Fallback
	}
}
