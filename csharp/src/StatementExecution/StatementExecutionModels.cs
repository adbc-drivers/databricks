/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace AdbcDrivers.Databricks.StatementExecution
{
    // ============================================================================
    // Session Management Models
    // ============================================================================

    /// <summary>
    /// Request to create a new SQL session.
    /// </summary>
    public class CreateSessionRequest
    {
        /// <summary>
        /// The warehouse ID to use for this session.
        /// </summary>
        [JsonPropertyName("warehouse_id")]
        public string WarehouseId { get; set; } = string.Empty;

        /// <summary>
        /// The catalog to use for this session.
        /// </summary>
        [JsonPropertyName("catalog")]
        public string? Catalog { get; set; }

        /// <summary>
        /// The schema to use for this session.
        /// </summary>
        [JsonPropertyName("schema")]
        public string? Schema { get; set; }

        /// <summary>
        /// Session configuration parameters.
        /// </summary>
        [JsonPropertyName("session_confs")]
        public Dictionary<string, string>? SessionConfigs { get; set; }
    }

    /// <summary>
    /// Response from creating a SQL session.
    /// </summary>
    public class CreateSessionResponse
    {
        /// <summary>
        /// The unique identifier for the created session.
        /// </summary>
        [JsonPropertyName("session_id")]
        public string SessionId { get; set; } = string.Empty;
    }

    // ============================================================================
    // Statement Execution Models
    // ============================================================================

    /// <summary>
    /// Parameter for parameterized SQL queries.
    /// </summary>
    public class StatementParameter
    {
        /// <summary>
        /// The parameter name (without leading colon).
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// The parameter value as a string.
        /// </summary>
        [JsonPropertyName("value")]
        public string Value { get; set; } = string.Empty;

        /// <summary>
        /// The parameter type (e.g., "STRING", "INT", "DOUBLE").
        /// </summary>
        [JsonPropertyName("type")]
        public string? Type { get; set; }
    }

    /// <summary>
    /// Request to execute a SQL statement.
    /// </summary>
    public class ExecuteStatementRequest
    {
        /// <summary>
        /// The warehouse ID to use (required if session_id is not provided).
        /// </summary>
        [JsonPropertyName("warehouse_id")]
        public string? WarehouseId { get; set; }

        /// <summary>
        /// The session ID to use (if session management is enabled).
        /// </summary>
        [JsonPropertyName("session_id")]
        public string? SessionId { get; set; }

        /// <summary>
        /// The SQL statement to execute.
        /// </summary>
        [JsonPropertyName("statement")]
        public string Statement { get; set; } = string.Empty;

        /// <summary>
        /// The catalog to use for this statement.
        /// </summary>
        [JsonPropertyName("catalog")]
        public string? Catalog { get; set; }

        /// <summary>
        /// The schema to use for this statement.
        /// </summary>
        [JsonPropertyName("schema")]
        public string? Schema { get; set; }

        /// <summary>
        /// Parameters for parameterized queries.
        /// </summary>
        [JsonPropertyName("parameters")]
        public List<StatementParameter>? Parameters { get; set; }

        /// <summary>
        /// Result disposition: "INLINE", "EXTERNAL_LINKS", or "INLINE_OR_EXTERNAL_LINKS".
        /// INLINE_OR_EXTERNAL_LINKS returns small results inline and larger results via CloudFetch.
        /// EXTERNAL_LINKS always returns results via CloudFetch download links.
        /// </summary>
        [JsonPropertyName("disposition")]
        public string Disposition { get; set; } = "INLINE_OR_EXTERNAL_LINKS";

        /// <summary>
        /// Result format: "ARROW_STREAM", "JSON_ARRAY", or "CSV".
        /// </summary>
        [JsonPropertyName("format")]
        public string Format { get; set; } = "ARROW_STREAM";

        /// <summary>
        /// Result compression: "lz4", "gzip", or "none".
        /// </summary>
        [JsonPropertyName("result_compression")]
        public string? ResultCompression { get; set; }

        /// <summary>
        /// Wait timeout (e.g., "10s"). Omit for direct results mode.
        /// </summary>
        [JsonPropertyName("wait_timeout")]
        public string? WaitTimeout { get; set; }

        /// <summary>
        /// Action on wait timeout: "CONTINUE" or "CANCEL".
        /// </summary>
        [JsonPropertyName("on_wait_timeout")]
        public string? OnWaitTimeout { get; set; }

        /// <summary>
        /// Maximum number of rows to return.
        /// </summary>
        [JsonPropertyName("row_limit")]
        public long? RowLimit { get; set; }

        /// <summary>
        /// Maximum number of bytes to return.
        /// </summary>
        [JsonPropertyName("byte_limit")]
        public long? ByteLimit { get; set; }
    }

    /// <summary>
    /// Response from executing a SQL statement.
    /// </summary>
    public class ExecuteStatementResponse
    {
        /// <summary>
        /// The unique identifier for the executed statement.
        /// </summary>
        [JsonPropertyName("statement_id")]
        public string StatementId { get; set; } = string.Empty;

        /// <summary>
        /// The current status of the statement.
        /// </summary>
        [JsonPropertyName("status")]
        public StatementStatus? Status { get; set; }

        /// <summary>
        /// The result manifest (for EXTERNAL_LINKS disposition).
        /// </summary>
        [JsonPropertyName("manifest")]
        public ResultManifest? Manifest { get; set; }

        /// <summary>
        /// The result data (for INLINE disposition).
        /// </summary>
        [JsonPropertyName("result")]
        public ResultData? Result { get; set; }
    }

    /// <summary>
    /// Response from getting a statement's status and results.
    /// </summary>
    public class GetStatementResponse
    {
        /// <summary>
        /// The statement identifier.
        /// </summary>
        [JsonPropertyName("statement_id")]
        public string StatementId { get; set; } = string.Empty;

        /// <summary>
        /// The current status of the statement.
        /// </summary>
        [JsonPropertyName("status")]
        public StatementStatus? Status { get; set; }

        /// <summary>
        /// The result manifest (for EXTERNAL_LINKS disposition).
        /// </summary>
        [JsonPropertyName("manifest")]
        public ResultManifest? Manifest { get; set; }

        /// <summary>
        /// The result data (for INLINE disposition).
        /// </summary>
        [JsonPropertyName("result")]
        public ResultData? Result { get; set; }
    }

    // ============================================================================
    // Status and Error Models
    // ============================================================================

    /// <summary>
    /// Status of a SQL statement execution.
    /// </summary>
    public class StatementStatus
    {
        /// <summary>
        /// The execution state: "PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELED", "CLOSED".
        /// </summary>
        [JsonPropertyName("state")]
        public string State { get; set; } = string.Empty;

        /// <summary>
        /// Error information if the statement failed.
        /// </summary>
        [JsonPropertyName("error")]
        public StatementError? Error { get; set; }
    }

    /// <summary>
    /// Error information for a failed statement.
    /// </summary>
    public class StatementError
    {
        /// <summary>
        /// The error code.
        /// </summary>
        [JsonPropertyName("error_code")]
        public string? ErrorCode { get; set; }

        /// <summary>
        /// The error message.
        /// </summary>
        [JsonPropertyName("message")]
        public string? Message { get; set; }

        /// <summary>
        /// SQL state code.
        /// </summary>
        [JsonPropertyName("sql_state")]
        public string? SqlState { get; set; }
    }

    /// <summary>
    /// Service error from the API.
    /// </summary>
    public class ServiceError
    {
        /// <summary>
        /// The error code.
        /// </summary>
        [JsonPropertyName("error_code")]
        public string? ErrorCode { get; set; }

        /// <summary>
        /// The error message.
        /// </summary>
        [JsonPropertyName("message")]
        public string? Message { get; set; }
    }

    // ============================================================================
    // Result Models
    // ============================================================================

    /// <summary>
    /// Manifest describing the structure and location of query results.
    /// </summary>
    public class ResultManifest
    {
        /// <summary>
        /// The format of the results: "arrow_stream", "json_array", or "csv".
        /// </summary>
        [JsonPropertyName("format")]
        public string Format { get; set; } = string.Empty;

        /// <summary>
        /// The schema of the result set.
        /// </summary>
        [JsonPropertyName("schema")]
        public ResultSchema? Schema { get; set; }

        /// <summary>
        /// Total number of result chunks.
        /// </summary>
        [JsonPropertyName("total_chunk_count")]
        public int TotalChunkCount { get; set; }

        /// <summary>
        /// The result chunks (may be incomplete for large result sets).
        /// </summary>
        [JsonPropertyName("chunks")]
        public List<ResultChunk>? Chunks { get; set; }

        /// <summary>
        /// Total number of rows in the result set.
        /// </summary>
        [JsonPropertyName("total_row_count")]
        public long TotalRowCount { get; set; }

        /// <summary>
        /// Total size in bytes of the result set.
        /// </summary>
        [JsonPropertyName("total_byte_count")]
        public long TotalByteCount { get; set; }

        /// <summary>
        /// Result compression: "lz4", "gzip", or "none".
        /// </summary>
        [JsonPropertyName("result_compression")]
        public string? ResultCompression { get; set; }

        /// <summary>
        /// True if results were truncated due to row_limit or byte_limit.
        /// </summary>
        [JsonPropertyName("truncated")]
        public bool? Truncated { get; set; }

        /// <summary>
        /// True for Unity Catalog Volume operations.
        /// </summary>
        [JsonPropertyName("is_volume_operation")]
        public bool? IsVolumeOperation { get; set; }
    }

    /// <summary>
    /// A chunk of result data.
    /// </summary>
    public class ResultChunk
    {
        /// <summary>
        /// The zero-based index of this chunk.
        /// </summary>
        [JsonPropertyName("chunk_index")]
        public int ChunkIndex { get; set; }

        /// <summary>
        /// Number of rows in this chunk.
        /// </summary>
        [JsonPropertyName("row_count")]
        public long RowCount { get; set; }

        /// <summary>
        /// Starting row offset of this chunk in the overall result set.
        /// </summary>
        [JsonPropertyName("row_offset")]
        public long RowOffset { get; set; }

        /// <summary>
        /// Size in bytes of this chunk.
        /// </summary>
        [JsonPropertyName("byte_count")]
        public long ByteCount { get; set; }

        /// <summary>
        /// External links for downloading this chunk (EXTERNAL_LINKS disposition).
        /// </summary>
        [JsonPropertyName("external_links")]
        public List<ExternalLink>? ExternalLinks { get; set; }

        /// <summary>
        /// Inline data for this chunk (INLINE disposition).
        /// </summary>
        [JsonPropertyName("data_array")]
        public List<List<object>>? DataArray { get; set; }

        /// <summary>
        /// Binary attachment for special result types.
        /// </summary>
        [JsonPropertyName("attachment")]
        public byte[]? Attachment { get; set; }

        /// <summary>
        /// Index of the next chunk (for incremental fetching).
        /// </summary>
        [JsonPropertyName("next_chunk_index")]
        public long? NextChunkIndex { get; set; }

        /// <summary>
        /// Internal link to the next chunk.
        /// </summary>
        [JsonPropertyName("next_chunk_internal_link")]
        public string? NextChunkInternalLink { get; set; }
    }

    /// <summary>
    /// External link for downloading result data from cloud storage.
    /// </summary>
    public class ExternalLink
    {
        /// <summary>
        /// The pre-signed URL for downloading the result file.
        /// </summary>
        [JsonPropertyName("external_link")]
        public string ExternalLinkUrl { get; set; } = string.Empty;

        /// <summary>
        /// Expiration timestamp (ISO 8601 format).
        /// </summary>
        [JsonPropertyName("expiration")]
        public string? Expiration { get; set; }

        /// <summary>
        /// The chunk index this link belongs to.
        /// </summary>
        [JsonPropertyName("chunk_index")]
        public long ChunkIndex { get; set; }

        /// <summary>
        /// Number of rows in this external link.
        /// </summary>
        [JsonPropertyName("row_count")]
        public long RowCount { get; set; }

        /// <summary>
        /// Starting row offset in the overall result set.
        /// </summary>
        [JsonPropertyName("row_offset")]
        public long RowOffset { get; set; }

        /// <summary>
        /// Size in bytes of this external link.
        /// </summary>
        [JsonPropertyName("byte_count")]
        public long ByteCount { get; set; }

        /// <summary>
        /// HTTP headers required for downloading (e.g., for cloud storage auth).
        /// </summary>
        [JsonPropertyName("http_headers")]
        public Dictionary<string, string>? HttpHeaders { get; set; }

        /// <summary>
        /// Index of the next chunk (for incremental fetching).
        /// </summary>
        [JsonPropertyName("next_chunk_index")]
        public long? NextChunkIndex { get; set; }

        /// <summary>
        /// Internal link to the next chunk.
        /// </summary>
        [JsonPropertyName("next_chunk_internal_link")]
        public string? NextChunkInternalLink { get; set; }
    }

    /// <summary>
    /// Result data returned directly in the response (INLINE disposition).
    /// </summary>
    public class ResultData
    {
        /// <summary>
        /// Size in bytes of this result data.
        /// </summary>
        [JsonPropertyName("byte_count")]
        public long? ByteCount { get; set; }

        /// <summary>
        /// The chunk index.
        /// </summary>
        [JsonPropertyName("chunk_index")]
        public long? ChunkIndex { get; set; }

        /// <summary>
        /// Inline data as a 2D array.
        /// </summary>
        [JsonPropertyName("data_array")]
        public List<List<string>>? DataArray { get; set; }

        /// <summary>
        /// External links (for hybrid disposition).
        /// </summary>
        [JsonPropertyName("external_links")]
        public List<ExternalLink>? ExternalLinks { get; set; }

        /// <summary>
        /// Index of the next chunk.
        /// </summary>
        [JsonPropertyName("next_chunk_index")]
        public long? NextChunkIndex { get; set; }

        /// <summary>
        /// Internal link to the next chunk.
        /// </summary>
        [JsonPropertyName("next_chunk_internal_link")]
        public string? NextChunkInternalLink { get; set; }

        /// <summary>
        /// Number of rows in this result data.
        /// </summary>
        [JsonPropertyName("row_count")]
        public long? RowCount { get; set; }

        /// <summary>
        /// Starting row offset.
        /// </summary>
        [JsonPropertyName("row_offset")]
        public long? RowOffset { get; set; }

        /// <summary>
        /// Binary attachment.
        /// </summary>
        [JsonPropertyName("attachment")]
        public byte[]? Attachment { get; set; }
    }

    /// <summary>
    /// Schema description of the result set.
    /// </summary>
    public class ResultSchema
    {
        /// <summary>
        /// Total number of columns.
        /// </summary>
        [JsonPropertyName("column_count")]
        public long? ColumnCount { get; set; }

        /// <summary>
        /// List of column information.
        /// </summary>
        [JsonPropertyName("columns")]
        public List<ColumnInfo>? Columns { get; set; }
    }

    /// <summary>
    /// Information about a result set column.
    /// </summary>
    public class ColumnInfo
    {
        /// <summary>
        /// The column name.
        /// </summary>
        [JsonPropertyName("name")]
        public string? Name { get; set; }

        /// <summary>
        /// The column position (zero-based).
        /// </summary>
        [JsonPropertyName("position")]
        public long? Position { get; set; }

        /// <summary>
        /// The type name (e.g., "INT", "STRING", "TIMESTAMP").
        /// </summary>
        [JsonPropertyName("type_name")]
        public string? TypeName { get; set; }

        /// <summary>
        /// The full type text.
        /// </summary>
        [JsonPropertyName("type_text")]
        public string? TypeText { get; set; }

        /// <summary>
        /// Type precision (for numeric types).
        /// </summary>
        [JsonPropertyName("type_precision")]
        public long? TypePrecision { get; set; }

        /// <summary>
        /// Type scale (for numeric types).
        /// </summary>
        [JsonPropertyName("type_scale")]
        public long? TypeScale { get; set; }

        /// <summary>
        /// Interval type (for interval types).
        /// </summary>
        [JsonPropertyName("type_interval_type")]
        public string? TypeIntervalType { get; set; }
    }
}
