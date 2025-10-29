/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    // ========================================
    // Session Management Models
    // ========================================

    /// <summary>
    /// Request to create a new session in the Statement Execution API.
    /// </summary>
    public class CreateSessionRequest
    {
        /// <summary>
        /// The ID of the SQL warehouse to use for the session.
        /// </summary>
        [JsonPropertyName("warehouse_id")]
        public string WarehouseId { get; set; } = string.Empty;

        /// <summary>
        /// The catalog to use for the session.
        /// </summary>
        [JsonPropertyName("catalog")]
        public string? Catalog { get; set; }

        /// <summary>
        /// The schema to use for the session.
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
    /// Response from creating a session.
    /// </summary>
    public class CreateSessionResponse
    {
        /// <summary>
        /// The ID of the created session.
        /// </summary>
        [JsonPropertyName("session_id")]
        public string SessionId { get; set; } = string.Empty;
    }

    // ========================================
    // Statement Execution Models
    // ========================================

    /// <summary>
    /// Request to execute a SQL statement.
    /// </summary>
    public class ExecuteStatementRequest
    {
        /// <summary>
        /// The ID of the SQL warehouse to use (required if session_id is not provided).
        /// </summary>
        [JsonPropertyName("warehouse_id")]
        public string? WarehouseId { get; set; }

        /// <summary>
        /// The ID of the session to use (required if warehouse_id is not provided).
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
        /// Statement parameters for parameterized queries.
        /// </summary>
        [JsonPropertyName("parameters")]
        public List<StatementParameter>? Parameters { get; set; }

        /// <summary>
        /// Result disposition: "inline", "external_links", or "inline_or_external_links".
        /// </summary>
        [JsonPropertyName("disposition")]
        public string Disposition { get; set; } = "inline_or_external_links";

        /// <summary>
        /// Result format: "arrow_stream", "json_array", or "csv".
        /// </summary>
        [JsonPropertyName("format")]
        public string Format { get; set; } = "arrow_stream";

        /// <summary>
        /// Result compression codec: "lz4", "gzip", or "none".
        /// </summary>
        [JsonPropertyName("result_compression")]
        public string? ResultCompression { get; set; }

        /// <summary>
        /// Wait timeout in seconds (e.g., "10s"). Omit for direct results mode.
        /// </summary>
        [JsonPropertyName("wait_timeout")]
        public string? WaitTimeout { get; set; }

        /// <summary>
        /// Action to take on wait timeout: "CONTINUE" or "CANCEL".
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
    /// Response from executing a statement.
    /// </summary>
    public class ExecuteStatementResponse
    {
        /// <summary>
        /// The ID of the executed statement.
        /// </summary>
        [JsonPropertyName("statement_id")]
        public string StatementId { get; set; } = string.Empty;

        /// <summary>
        /// The status of the statement execution.
        /// </summary>
        [JsonPropertyName("status")]
        public StatementStatus Status { get; set; } = new StatementStatus();

        /// <summary>
        /// The result manifest (metadata about results).
        /// </summary>
        [JsonPropertyName("manifest")]
        public ResultManifest? Manifest { get; set; }

        /// <summary>
        /// The result data (if inline disposition).
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
        /// The ID of the statement.
        /// </summary>
        [JsonPropertyName("statement_id")]
        public string StatementId { get; set; } = string.Empty;

        /// <summary>
        /// The status of the statement execution.
        /// </summary>
        [JsonPropertyName("status")]
        public StatementStatus Status { get; set; } = new StatementStatus();

        /// <summary>
        /// The result manifest (metadata about results).
        /// </summary>
        [JsonPropertyName("manifest")]
        public ResultManifest? Manifest { get; set; }

        /// <summary>
        /// The result data (if inline disposition).
        /// </summary>
        [JsonPropertyName("result")]
        public ResultData? Result { get; set; }
    }

    // ========================================
    // Status and Error Models
    // ========================================

    /// <summary>
    /// Status of a statement execution.
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
        public ServiceError? Error { get; set; }

        /// <summary>
        /// SQL state code if available.
        /// </summary>
        [JsonPropertyName("sql_state")]
        public string? SqlState { get; set; }
    }

    /// <summary>
    /// Error information for failed statements.
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

    // ========================================
    // Result Models
    // ========================================

    /// <summary>
    /// Manifest containing metadata about query results.
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
        public ResultSchema Schema { get; set; } = new ResultSchema();

        /// <summary>
        /// Total number of chunks in the result set.
        /// </summary>
        [JsonPropertyName("total_chunk_count")]
        public int TotalChunkCount { get; set; }

        /// <summary>
        /// List of result chunks (for external_links disposition).
        /// </summary>
        [JsonPropertyName("chunks")]
        public List<ResultChunk>? Chunks { get; set; }

        /// <summary>
        /// Total number of rows in the result set.
        /// </summary>
        [JsonPropertyName("total_row_count")]
        public long TotalRowCount { get; set; }

        /// <summary>
        /// Total number of bytes in the result set.
        /// </summary>
        [JsonPropertyName("total_byte_count")]
        public long TotalByteCount { get; set; }

        /// <summary>
        /// Result compression codec: "lz4", "gzip", or "none".
        /// </summary>
        [JsonPropertyName("result_compression")]
        public string? ResultCompression { get; set; }

        /// <summary>
        /// True if results were truncated by row_limit or byte_limit.
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
        /// The index of this chunk.
        /// </summary>
        [JsonPropertyName("chunk_index")]
        public int ChunkIndex { get; set; }

        /// <summary>
        /// Number of rows in this chunk.
        /// </summary>
        [JsonPropertyName("row_count")]
        public long RowCount { get; set; }

        /// <summary>
        /// Starting row offset of this chunk.
        /// </summary>
        [JsonPropertyName("row_offset")]
        public long RowOffset { get; set; }

        /// <summary>
        /// Number of bytes in this chunk.
        /// </summary>
        [JsonPropertyName("byte_count")]
        public long ByteCount { get; set; }

        /// <summary>
        /// External links for downloading this chunk (external_links disposition).
        /// </summary>
        [JsonPropertyName("external_links")]
        public List<ExternalLink>? ExternalLinks { get; set; }

        /// <summary>
        /// Inline data array (inline disposition).
        /// </summary>
        [JsonPropertyName("data_array")]
        public List<List<object>>? DataArray { get; set; }

        /// <summary>
        /// Binary attachment for special result types.
        /// </summary>
        [JsonPropertyName("attachment")]
        public byte[]? Attachment { get; set; }

        /// <summary>
        /// Index of the next chunk (for pagination).
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
        /// The presigned URL for downloading the data.
        /// </summary>
        [JsonPropertyName("external_link")]
        public string ExternalLinkUrl { get; set; } = string.Empty;

        /// <summary>
        /// Expiration time of the presigned URL (ISO 8601 timestamp).
        /// </summary>
        [JsonPropertyName("expiration")]
        public string Expiration { get; set; } = string.Empty;

        /// <summary>
        /// The chunk index this link corresponds to.
        /// </summary>
        [JsonPropertyName("chunk_index")]
        public long ChunkIndex { get; set; }

        /// <summary>
        /// Number of rows in this chunk.
        /// </summary>
        [JsonPropertyName("row_count")]
        public long RowCount { get; set; }

        /// <summary>
        /// Starting row offset of this chunk.
        /// </summary>
        [JsonPropertyName("row_offset")]
        public long RowOffset { get; set; }

        /// <summary>
        /// Number of bytes in this chunk.
        /// </summary>
        [JsonPropertyName("byte_count")]
        public long ByteCount { get; set; }

        /// <summary>
        /// HTTP headers required for downloading (for cloud storage auth).
        /// </summary>
        [JsonPropertyName("http_headers")]
        public Dictionary<string, string>? HttpHeaders { get; set; }

        /// <summary>
        /// Index of the next chunk (for pagination).
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
    /// Result data returned for inline disposition.
    /// </summary>
    public class ResultData
    {
        /// <summary>
        /// Number of bytes in this result.
        /// </summary>
        [JsonPropertyName("byte_count")]
        public long? ByteCount { get; set; }

        /// <summary>
        /// The chunk index.
        /// </summary>
        [JsonPropertyName("chunk_index")]
        public long? ChunkIndex { get; set; }

        /// <summary>
        /// Inline data as array of rows.
        /// </summary>
        [JsonPropertyName("data_array")]
        public List<List<string>>? DataArray { get; set; }

        /// <summary>
        /// External links for downloading data.
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
        /// Number of rows in this result.
        /// </summary>
        [JsonPropertyName("row_count")]
        public long? RowCount { get; set; }

        /// <summary>
        /// Starting row offset.
        /// </summary>
        [JsonPropertyName("row_offset")]
        public long? RowOffset { get; set; }

        /// <summary>
        /// Binary attachment for special result types.
        /// </summary>
        [JsonPropertyName("attachment")]
        public byte[]? Attachment { get; set; }
    }

    // ========================================
    // Schema Models
    // ========================================

    /// <summary>
    /// Schema of the result set.
    /// </summary>
    public class ResultSchema
    {
        /// <summary>
        /// Total number of columns.
        /// </summary>
        [JsonPropertyName("column_count")]
        public long? ColumnCount { get; set; }

        /// <summary>
        /// List of column metadata.
        /// </summary>
        [JsonPropertyName("columns")]
        public List<ColumnInfo>? Columns { get; set; }
    }

    /// <summary>
    /// Metadata about a column in the result set.
    /// </summary>
    public class ColumnInfo
    {
        /// <summary>
        /// The column name.
        /// </summary>
        [JsonPropertyName("name")]
        public string? Name { get; set; }

        /// <summary>
        /// The column position (0-indexed).
        /// </summary>
        [JsonPropertyName("position")]
        public long? Position { get; set; }

        /// <summary>
        /// The interval type for INTERVAL columns.
        /// </summary>
        [JsonPropertyName("type_interval_type")]
        public string? TypeIntervalType { get; set; }

        /// <summary>
        /// The type name (e.g., "BIGINT", "STRING", "DECIMAL").
        /// </summary>
        [JsonPropertyName("type_name")]
        public string? TypeName { get; set; }

        /// <summary>
        /// The precision for numeric types.
        /// </summary>
        [JsonPropertyName("type_precision")]
        public long? TypePrecision { get; set; }

        /// <summary>
        /// The scale for decimal types.
        /// </summary>
        [JsonPropertyName("type_scale")]
        public long? TypeScale { get; set; }

        /// <summary>
        /// The full SQL type text (e.g., "DECIMAL(18,2)", "VARCHAR(255)").
        /// </summary>
        [JsonPropertyName("type_text")]
        public string? TypeText { get; set; }
    }

    // ========================================
    // Parameter Models
    // ========================================

    /// <summary>
    /// A parameter for parameterized SQL queries.
    /// </summary>
    public class StatementParameter
    {
        /// <summary>
        /// The parameter name.
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// The parameter type (e.g., "STRING", "INT", "DATE").
        /// </summary>
        [JsonPropertyName("type")]
        public string? Type { get; set; }

        /// <summary>
        /// The parameter value.
        /// </summary>
        [JsonPropertyName("value")]
        public object? Value { get; set; }
    }
}
