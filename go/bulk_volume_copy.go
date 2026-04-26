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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	extNameKey     = "ARROW:extension:name"
	extMetadataKey = "ARROW:extension:metadata"
	geoarrowWKBExt = "geoarrow.wkb"
)

// geoColumnInfo captures everything needed to bridge a geoarrow.wkb column on
// the Arrow side to a typed GEOMETRY(srid) column on the Databricks side.
type geoColumnInfo struct {
	index int
	srid  int // 0 means no CRS specified; column will land as GEOMETRY (untyped)
}

// detectGeoColumns walks the schema and returns one entry per geoarrow.wkb
// field, parsing the SRID from the {"crs":"EPSG:N"} extension metadata.
// Falls back to a user-provided hint (parsed from the
// databricks.bulk.geometry_columns option, e.g. "geom:4326,boundary:3857")
// because some sources — notably DuckDB's adbc_scanner — emit geometry
// columns as plain BINARY without the geoarrow.wkb extension metadata.
func detectGeoColumns(schema *arrow.Schema, hintRaw string) []geoColumnInfo {
	var out []geoColumnInfo
	hint := parseGeometryColumnsHint(hintRaw)
	for i, f := range schema.Fields() {
		extName, hasExtName := f.Metadata.GetValue(extNameKey)
		extMeta, _ := f.Metadata.GetValue(extMetadataKey)
		// 1. Schema metadata wins (geoarrow.wkb extension).  If the CRS
		// metadata didn't yield an EPSG code, fall back to the user hint
		// for this column so the destination still gets a typed
		// GEOMETRY(srid).
		if hasExtName && extName == geoarrowWKBExt {
			info := geoColumnInfo{index: i}
			if extMeta != "" {
				info.srid = parseEPSGFromExtensionMetadata(extMeta)
			}
			if info.srid == 0 {
				if srid, ok := hint[f.Name]; ok {
					info.srid = srid
				}
			}
			out = append(out, info)
			continue
		}
		// 2. Otherwise, accept user hints — but only for BINARY-ish columns
		// since ST_GEOMFROMWKB requires a binary input.
		if srid, ok := hint[f.Name]; ok && isBinaryArrowType(f.Type) {
			out = append(out, geoColumnInfo{index: i, srid: srid})
		}
	}
	return out
}

// parseGeometryColumnsHint parses "col:srid,col:srid" into a name->srid map.
// Empty / malformed entries are skipped silently rather than failing the
// bulk insert; users can verify by inspecting the destination table type.
func parseGeometryColumnsHint(raw string) map[string]int {
	out := map[string]int{}
	if raw == "" {
		return out
	}
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		colon := strings.LastIndex(entry, ":")
		if colon <= 0 || colon == len(entry)-1 {
			continue
		}
		col := strings.TrimSpace(entry[:colon])
		srid, err := strconv.Atoi(strings.TrimSpace(entry[colon+1:]))
		if err != nil || srid <= 0 || col == "" {
			continue
		}
		out[col] = srid
	}
	return out
}

// isBinaryArrowType reports whether t is one of the binary-ish Arrow types
// that ST_GEOMFROMWKB(<binary>, <srid>) accepts on Databricks.
func isBinaryArrowType(t arrow.DataType) bool {
	switch t.ID() {
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
		return true
	}
	return false
}

// parseEPSGFromExtensionMetadata extracts NNNN from a geoarrow.wkb metadata
// payload.  Two shapes are common in the wild:
//
//   1. AUTH:CODE string:  {"crs":"EPSG:4326"}
//   2. PROJJSON object:   {"crs_type":"projjson","crs":{... "id":{"authority":"EPSG","code":4326}}}
//
// DuckDB's adbc_scanner emits the PROJJSON form by default for spatial
// outputs.  Returns 0 if neither shape yields an EPSG code.
func parseEPSGFromExtensionMetadata(extMeta string) int {
	// Form 1: crs as a plain string.
	var asString struct {
		CRS string `json:"crs"`
	}
	if err := json.Unmarshal([]byte(extMeta), &asString); err == nil {
		const prefix = "EPSG:"
		if strings.HasPrefix(asString.CRS, prefix) {
			if srid, err := strconv.Atoi(strings.TrimPrefix(asString.CRS, prefix)); err == nil && srid > 0 {
				return srid
			}
		}
	}
	// Form 2: crs as a nested PROJJSON object with id.{authority,code}.
	var asObject struct {
		CRS struct {
			ID struct {
				Authority string `json:"authority"`
				Code      int    `json:"code"`
			} `json:"id"`
		} `json:"crs"`
	}
	if err := json.Unmarshal([]byte(extMeta), &asObject); err == nil {
		if strings.EqualFold(asObject.CRS.ID.Authority, "EPSG") && asObject.CRS.ID.Code > 0 {
			return asObject.CRS.ID.Code
		}
	}
	return 0
}

// databricksTypeForField maps an Arrow field to a Databricks DDL type, with
// special handling for geoarrow.wkb extension fields so the destination
// column lands as GEOMETRY(srid) rather than the default BINARY fallback.
func databricksTypeForField(f arrow.Field) string {
	if extName, ok := f.Metadata.GetValue(extNameKey); ok && extName == geoarrowWKBExt {
		srid := 0
		if extMeta, ok := f.Metadata.GetValue(extMetadataKey); ok && extMeta != "" {
			srid = parseEPSGFromExtensionMetadata(extMeta)
		}
		if srid > 0 {
			return fmt.Sprintf("GEOMETRY(%d)", srid)
		}
		// Fall through to BINARY for untyped geometry; Databricks rejects
		// bare GEOMETRY without an SRID modifier on most runtimes.
	}
	return arrowTypeToDatabricksType(f.Type)
}

// stripExtensionTypes returns a schema where any geoarrow.wkb extension
// metadata has been removed from each field.  pqarrow writes the storage
// type unchanged, but cleaning the metadata avoids surprises on the read
// side and keeps the staged parquet schema strictly BINARY for COPY INTO.
func stripExtensionTypes(schema *arrow.Schema, geoIndices map[int]struct{}) *arrow.Schema {
	if len(geoIndices) == 0 {
		return schema
	}
	fields := make([]arrow.Field, len(schema.Fields()))
	for i, f := range schema.Fields() {
		if _, isGeo := geoIndices[i]; isGeo {
			fields[i] = arrow.Field{
				Name:     f.Name,
				Type:     arrow.BinaryTypes.Binary,
				Nullable: f.Nullable,
			}
		} else {
			fields[i] = f
		}
	}
	meta := schema.Metadata()
	return arrow.NewSchema(fields, &meta)
}

// buildCopyTransform returns the SELECT projection used inside COPY INTO so
// that geometry columns get reconstructed from binary WKB into typed
// GEOMETRY(srid) values.  Non-geo columns pass through.
func buildCopyTransform(schema *arrow.Schema, geoCols []geoColumnInfo) string {
	geoBySrid := make(map[int]int, len(geoCols))
	for _, g := range geoCols {
		geoBySrid[g.index] = g.srid
	}
	parts := make([]string, len(schema.Fields()))
	for i, f := range schema.Fields() {
		col := quoteIdentifier(f.Name)
		if srid, ok := geoBySrid[i]; ok && srid > 0 {
			parts[i] = fmt.Sprintf("ST_GEOMFROMWKB(%s, %d) AS %s", col, srid, col)
		} else {
			parts[i] = col
		}
	}
	return strings.Join(parts, ", ")
}

// stagedParquetWriter holds an open Parquet writer and the local file it
// targets.  Multiple Arrow record batches accumulate into one file before
// it is closed and uploaded.
type stagedParquetWriter struct {
	writer        *pqarrow.FileWriter
	file          *os.File
	localPath     string
	stagingSchema *arrow.Schema
	rows          int64
}

// newStagedParquetWriter opens a fresh Parquet file at localPath using a
// schema where any geoarrow.wkb extension metadata has been stripped (so the
// staged file is plain BINARY for the COPY INTO transform to rebuild).
func newStagedParquetWriter(localPath string, stagingSchema *arrow.Schema) (*stagedParquetWriter, error) {
	f, err := os.Create(localPath)
	if err != nil {
		return nil, fmt.Errorf("create parquet file: %w", err)
	}
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrProps := pqarrow.DefaultWriterProps()
	w, err := pqarrow.NewFileWriter(stagingSchema, f, props, arrProps)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(localPath)
		return nil, fmt.Errorf("new parquet writer: %w", err)
	}
	return &stagedParquetWriter{
		writer:        w,
		file:          f,
		localPath:     localPath,
		stagingSchema: stagingSchema,
	}, nil
}

// appendRecord re-wraps rec under the staging schema (so pqarrow doesn't
// emit the source extension metadata) and writes it as one row group.
func (sw *stagedParquetWriter) appendRecord(rec arrow.RecordBatch) error {
	stagingRec := array.NewRecord(sw.stagingSchema, rec.Columns(), rec.NumRows())
	defer stagingRec.Release()
	if err := sw.writer.Write(stagingRec); err != nil {
		return fmt.Errorf("write record to parquet: %w", err)
	}
	sw.rows += rec.NumRows()
	return nil
}

// close finalizes the Parquet file.  pqarrow.FileWriter.Close already
// closes the underlying *os.File, so we must NOT close it again here —
// double-Close on os.File returns an error and would mask any real
// upload/COPY-INTO failure.
func (sw *stagedParquetWriter) close() error {
	if err := sw.writer.Close(); err != nil {
		return fmt.Errorf("close parquet writer: %w", err)
	}
	return nil
}

// volumePathToFilesAPI converts a Volume path of the form
// /Volumes/<cat>/<schema>/<vol>/<rest> into the matching Files API URL path
// segment expected by `PUT /api/2.0/fs/files{path}`.
func volumePathToFilesAPI(volumePath string) string {
	// The Files API accepts a Volume-qualified path verbatim under the prefix.
	// Encode each path segment so spaces / unicode survive.
	parts := strings.Split(strings.TrimPrefix(volumePath, "/"), "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return "/" + strings.Join(parts, "/")
}

// uploadToVolume PUTs a local file to a Databricks Volume via the Files API.
// `remotePath` must be the full Volume path (e.g. /Volumes/c/s/v/.../x.parquet).
func (s *statementImpl) uploadToVolume(ctx context.Context, localPath, remotePath string) error {
	if s.conn.serverHostname == "" || s.conn.accessToken == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[databricks] Volume upload requires server hostname and access token on the connection",
		}
	}
	body, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open staged parquet: %w", err)
	}
	defer body.Close()

	apiURL := fmt.Sprintf("https://%s/api/2.0/fs/files%s?overwrite=true",
		s.conn.serverHostname, volumePathToFilesAPI(remotePath))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, apiURL, body)
	if err != nil {
		return fmt.Errorf("build files-api request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.conn.accessToken)
	req.Header.Set("Content-Type", "application/octet-stream")
	if stat, err := os.Stat(localPath); err == nil {
		req.ContentLength = stat.Size()
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("files-api PUT: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("files-api PUT %s -> %d: %s", remotePath, resp.StatusCode, bytes.TrimSpace(errBody))
	}
	return nil
}

// deleteFromVolume removes a staged file.  Best-effort: logs nothing, returns
// no error visibility — staging files are also cleaned up by the COPY INTO
// dedupe logic on Databricks if missed here.
func (s *statementImpl) deleteFromVolume(ctx context.Context, remotePath string) {
	if s.conn.serverHostname == "" || s.conn.accessToken == "" {
		return
	}
	apiURL := fmt.Sprintf("https://%s/api/2.0/fs/files%s",
		s.conn.serverHostname, volumePathToFilesAPI(remotePath))
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, apiURL, nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+s.conn.accessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// executeIngestViaVolumeCopy is the streaming bulk path: accumulate Arrow
// record batches into a single Parquet file in the configured Volume up to
// targetRows rows, then issue COPY INTO with a transform that rebuilds
// GEOMETRY values via ST_GEOMFROMWKB.  Repeats until the stream ends.
//
// Compared with the per-row INSERT path in executeIngest:
//   - Throughput scales with the staged file size instead of being capped
//     by per-row statement-execute round trips against the SQL warehouse.
//   - RSS stays bounded to one in-flight Parquet writer's state plus one
//     HTTP request body, regardless of total source size.
//
// The accumulator is important because DuckDB's adbc_scanner emits Arrow
// batches at its 2048-row vector size by default; staging one Parquet
// per Arrow batch turns into many small COPY INTOs and their per-call
// overhead dominates throughput.  Accumulating to OptionBulkBatchRows
// (default 20000) amortizes that overhead.
func (s *statementImpl) executeIngestViaVolumeCopy(ctx context.Context, runID string) (int64, error) {
	opts := &s.bulkIngestOptions
	tableName := buildTableName(opts.CatalogName, opts.SchemaName, opts.TableName)
	schema := s.boundStream.Schema()

	geoCols := detectGeoColumns(schema, s.conn.bulkGeometryColumns)
	geoIdx := make(map[int]struct{}, len(geoCols))
	for _, g := range geoCols {
		geoIdx[g.index] = struct{}{}
	}

	if err := s.createTableIfNeededViaVolumeCopy(ctx, tableName, schema, opts, geoCols); err != nil {
		return 0, err
	}

	stageDir := strings.TrimRight(s.conn.bulkVolumePath, "/") + "/__adbc_ingest_" + runID
	transform := buildCopyTransform(schema, geoCols)
	stagingSchema := stripExtensionTypes(schema, geoIdx)

	targetRows := int64(s.conn.bulkBatchRows)
	if targetRows <= 0 {
		targetRows = DefaultBulkBatchRows
	}

	totalRows := int64(0)
	batchIdx := 0
	var staged *stagedParquetWriter

	// flush closes the current Parquet, uploads it to the Volume, runs
	// COPY INTO, and resets the accumulator state.  Safe to call when
	// there is no open writer.
	flush := func() error {
		if staged == nil {
			return nil
		}
		// Always tear down local + remote artifacts even on error.
		localPath := staged.localPath
		defer func() {
			_ = os.Remove(localPath)
		}()

		if err := staged.close(); err != nil {
			staged = nil
			return s.ErrorHelper.Errorf(adbc.StatusInternal, "stage parquet: %v", err)
		}
		remotePath := fmt.Sprintf("%s/batch_%d.parquet", stageDir, batchIdx)
		if err := s.uploadToVolume(ctx, localPath, remotePath); err != nil {
			staged = nil
			return s.ErrorHelper.Errorf(adbc.StatusInternal, "upload to volume: %v", err)
		}
		copySQL := fmt.Sprintf(
			"COPY INTO %s FROM (SELECT %s FROM '%s') FILEFORMAT = PARQUET",
			tableName, transform, remotePath,
		)
		result, copyErr := s.conn.conn.ExecContext(ctx, copySQL)
		s.deleteFromVolume(ctx, remotePath)
		staged = nil
		batchIdx++
		if copyErr != nil {
			return s.ErrorHelper.Errorf(adbc.StatusInternal, "COPY INTO: %v", copyErr)
		}
		rows, _ := result.RowsAffected()
		totalRows += rows
		return nil
	}

	for s.boundStream.Next() {
		rec := s.boundStream.RecordBatch()
		if staged == nil {
			localPath := path.Join(os.TempDir(), fmt.Sprintf("adbc_dbx_%s_%d.parquet", runID, batchIdx))
			w, err := newStagedParquetWriter(localPath, stagingSchema)
			if err != nil {
				return totalRows, s.ErrorHelper.Errorf(adbc.StatusInternal, "stage parquet: %v", err)
			}
			staged = w
		}
		if err := staged.appendRecord(rec); err != nil {
			return totalRows, s.ErrorHelper.Errorf(adbc.StatusInternal, "%v", err)
		}
		if staged.rows >= targetRows {
			if err := flush(); err != nil {
				return totalRows, err
			}
		}
	}
	if err := s.boundStream.Err(); err != nil {
		// Best-effort cleanup of any in-flight staged file.
		if staged != nil {
			_ = staged.close()
			_ = os.Remove(staged.localPath)
			staged = nil
		}
		return totalRows, s.ErrorHelper.Errorf(adbc.StatusInternal, "stream error: %v", err)
	}
	if err := flush(); err != nil {
		return totalRows, err
	}
	return totalRows, nil
}

// createTableIfNeededViaVolumeCopy mirrors createTableIfNeeded but routes
// CREATE TABLE through databricksTypeForField so geoarrow.wkb extension
// columns become GEOMETRY(srid) on the Databricks side rather than landing
// as plain BINARY (which would force callers to ST_GEOMFROMWKB on every read).
func (s *statementImpl) createTableIfNeededViaVolumeCopy(ctx context.Context, tableName string, schema *arrow.Schema, opts *driverbase.BulkIngestOptions, geoCols []geoColumnInfo) error {
	switch opts.Mode {
	case adbc.OptionValueIngestModeCreate:
		return s.createGeoAwareTable(ctx, tableName, schema, false, geoCols)
	case adbc.OptionValueIngestModeCreateAppend:
		return s.createGeoAwareTable(ctx, tableName, schema, true, geoCols)
	case adbc.OptionValueIngestModeReplace:
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
		if _, err := s.conn.conn.ExecContext(ctx, dropSQL); err != nil {
			return s.ErrorHelper.Errorf(adbc.StatusInternal, "failed to drop the table: %v", err)
		}
		return s.createGeoAwareTable(ctx, tableName, schema, false, geoCols)
	case adbc.OptionValueIngestModeAppend:
		return nil
	default:
		return s.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "invalid ingest mode: %s", opts.Mode)
	}
}

// createGeoAwareTable emits CREATE TABLE DDL where columns flagged as
// geometries (either via geoarrow.wkb extension or the user's hint) are
// declared GEOMETRY(srid) rather than BINARY.
func (s *statementImpl) createGeoAwareTable(ctx context.Context, tableName string, schema *arrow.Schema, ifNotExists bool, geoCols []geoColumnInfo) error {
	geoBySrid := make(map[int]int, len(geoCols))
	for _, g := range geoCols {
		geoBySrid[g.index] = g.srid
	}
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
		if srid, ok := geoBySrid[i]; ok && srid > 0 {
			sql.WriteString(fmt.Sprintf("GEOMETRY(%d)", srid))
		} else {
			sql.WriteString(databricksTypeForField(field))
		}
		if !field.Nullable {
			sql.WriteString(" NOT NULL")
		}
	}
	sql.WriteString(")")
	if _, err := s.conn.conn.ExecContext(ctx, sql.String()); err != nil {
		return s.ErrorHelper.Errorf(adbc.StatusInternal, "failed to create table: %v", err)
	}
	return nil
}
