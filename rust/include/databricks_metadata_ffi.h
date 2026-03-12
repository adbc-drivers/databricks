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

/**
 * @file databricks_metadata_ffi.h
 * @brief C declarations for the Databricks ADBC driver's metadata FFI layer.
 *
 * These functions expose catalog metadata operations (catalogs, schemas,
 * tables, columns, keys) via the Arrow C Data Interface. Results are
 * returned as `ArrowArrayStream` structs that the caller owns and must
 * release.
 *
 * ## Usage
 *
 *   // conn_ptr comes from adbc_connection_.private_data (cast to void*)
 *   ArrowArrayStream stream;
 *   FfiStatus status = metadata_get_tables(conn_ptr, "main", NULL, "%", NULL, &stream);
 *   if (status != FfiStatus_Success) {
 *       FfiError err;
 *       metadata_get_last_error(&err);
 *       // handle err.message, err.sql_state
 *   }
 *   // consume stream...
 *
 * ## Error handling
 *
 * All functions return FfiStatus. On failure, call metadata_get_last_error()
 * to retrieve a thread-local error with message, SQLSTATE, and native code.
 * Errors are cleared at the start of each metadata_* call.
 */

#ifndef DATABRICKS_METADATA_FFI_H
#define DATABRICKS_METADATA_FFI_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ─── Status codes (matches ODBC conventions) ─────────────────────────────── */

typedef int32_t FfiStatus;

#define FfiStatus_Success       0
#define FfiStatus_SuccessWithInfo 1
#define FfiStatus_Error         (-1)
#define FfiStatus_InvalidHandle (-2)
#define FfiStatus_NoData        100

/* ─── Error details ───────────────────────────────────────────────────────── */

/**
 * C-compatible error details buffer.
 * Retrieved via metadata_get_last_error() after a failed operation.
 */
typedef struct {
    char message[1024];
    char sql_state[6];
    int32_t native_error;
} FfiError;

/**
 * Retrieve the last error from the current thread's error buffer.
 *
 * @param[out] out  Pointer to an FfiError struct to populate.
 * @return FfiStatus_Success on success, FfiStatus_InvalidHandle if out is NULL.
 */
FfiStatus metadata_get_last_error(FfiError* out);

/* ─── Forward declaration for Arrow C Data Interface ──────────────────────── */

struct ArrowArrayStream;

/* ─── Metadata functions ──────────────────────────────────────────────────── */

/**
 * List all catalogs.
 *
 * @param conn  Pointer to the Rust Connection (from adbc_connection_.private_data).
 * @param[out] out  Arrow stream to populate with catalog results.
 * @return FfiStatus code.
 */
FfiStatus metadata_get_catalogs(
    const void* conn,
    struct ArrowArrayStream* out
);

/**
 * List schemas, optionally filtered by catalog and pattern.
 *
 * @param conn            Pointer to the Rust Connection.
 * @param catalog         Catalog name filter (NULL = all catalogs).
 * @param schema_pattern  Schema name pattern filter (NULL = no filter).
 * @param[out] out        Arrow stream to populate.
 * @return FfiStatus code.
 */
FfiStatus metadata_get_schemas(
    const void* conn,
    const char* catalog,
    const char* schema_pattern,
    struct ArrowArrayStream* out
);

/**
 * List tables matching the given filter criteria.
 *
 * @param conn            Pointer to the Rust Connection.
 * @param catalog         Catalog name filter (NULL = all catalogs).
 * @param schema_pattern  Schema name pattern (NULL = no filter).
 * @param table_pattern   Table name pattern (NULL = no filter).
 * @param table_types     Comma-separated table types (NULL = all types).
 * @param[out] out        Arrow stream to populate.
 * @return FfiStatus code.
 */
FfiStatus metadata_get_tables(
    const void* conn,
    const char* catalog,
    const char* schema_pattern,
    const char* table_pattern,
    const char* table_types,
    struct ArrowArrayStream* out
);

/**
 * List columns matching the given filter criteria.
 *
 * When catalog is NULL or empty, returns an empty result set (since
 * SHOW COLUMNS IN ALL CATALOGS is not supported by Databricks).
 *
 * @param conn             Pointer to the Rust Connection.
 * @param catalog          Catalog name (NULL/empty = empty result).
 * @param schema_pattern   Schema name pattern (NULL = no filter).
 * @param table_pattern    Table name pattern (NULL = no filter).
 * @param column_pattern   Column name pattern (NULL = no filter).
 * @param[out] out         Arrow stream to populate.
 * @return FfiStatus code.
 */
FfiStatus metadata_get_columns(
    const void* conn,
    const char* catalog,
    const char* schema_pattern,
    const char* table_pattern,
    const char* column_pattern,
    struct ArrowArrayStream* out
);

/**
 * List primary key columns for a specific table.
 *
 * @param conn     Pointer to the Rust Connection.
 * @param catalog  Catalog name (required, must not be NULL).
 * @param schema   Schema name (required, must not be NULL).
 * @param table    Table name (required, must not be NULL).
 * @param[out] out Arrow stream to populate.
 * @return FfiStatus code.
 */
FfiStatus metadata_get_primary_keys(
    const void* conn,
    const char* catalog,
    const char* schema,
    const char* table,
    struct ArrowArrayStream* out
);

/**
 * List foreign key columns for a specific table.
 *
 * @param conn     Pointer to the Rust Connection.
 * @param catalog  Catalog name (required, must not be NULL).
 * @param schema   Schema name (required, must not be NULL).
 * @param table    Table name (required, must not be NULL).
 * @param[out] out Arrow stream to populate.
 * @return FfiStatus code.
 */
FfiStatus metadata_get_foreign_keys(
    const void* conn,
    const char* catalog,
    const char* schema,
    const char* table,
    struct ArrowArrayStream* out
);

#ifdef __cplusplus
}
#endif

#endif /* DATABRICKS_METADATA_FFI_H */
