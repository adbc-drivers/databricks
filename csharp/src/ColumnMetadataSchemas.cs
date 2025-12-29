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

using Apache.Arrow;
using Apache.Arrow.Types;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Shared helper class for standard column metadata schemas.
    /// Used by both Thrift and Statement Execution API protocols to ensure consistent metadata structure.
    /// Provides standard 24-column format for GetColumns metadata queries.
    /// </summary>
    internal static class ColumnMetadataSchemas
    {
        /// <summary>
        /// Creates the standard GetColumns schema (24 columns).
        /// This schema follows the standard flat table format for column metadata.
        /// </summary>
        /// <returns>Schema with 24 column definitions</returns>
        public static Schema CreateColumnMetadataSchema()
        {
            var fields = new[]
            {
                new Field("TABLE_CAT", StringType.Default, true),
                new Field("TABLE_SCHEM", StringType.Default, true),
                new Field("TABLE_NAME", StringType.Default, true),
                new Field("COLUMN_NAME", StringType.Default, true),
                new Field("DATA_TYPE", Int32Type.Default, true),
                new Field("TYPE_NAME", StringType.Default, true),
                new Field("COLUMN_SIZE", Int32Type.Default, true),
                new Field("BUFFER_LENGTH", Int8Type.Default, true),
                new Field("DECIMAL_DIGITS", Int32Type.Default, true),
                new Field("NUM_PREC_RADIX", Int32Type.Default, true),
                new Field("NULLABLE", Int32Type.Default, true),
                new Field("REMARKS", StringType.Default, true),
                new Field("COLUMN_DEF", StringType.Default, true),
                new Field("SQL_DATA_TYPE", Int32Type.Default, true),
                new Field("SQL_DATETIME_SUB", Int32Type.Default, true),
                new Field("CHAR_OCTET_LENGTH", Int32Type.Default, true),
                new Field("ORDINAL_POSITION", Int32Type.Default, true),
                new Field("IS_NULLABLE", StringType.Default, true),
                new Field("SCOPE_CATALOG", StringType.Default, true),
                new Field("SCOPE_SCHEMA", StringType.Default, true),
                new Field("SCOPE_TABLE", StringType.Default, true),
                new Field("SOURCE_DATA_TYPE", Int16Type.Default, true),
                new Field("IS_AUTO_INCREMENT", StringType.Default, true),
                new Field("BASE_TYPE_NAME", StringType.Default, true)
            };
            return new Schema(fields, null);
        }

        /// <summary>
        /// Creates empty Arrow arrays for each column in the column metadata schema.
        /// Useful for returning empty results from metadata queries.
        /// </summary>
        /// <returns>Array of empty Arrow arrays (24 columns)</returns>
        public static IArrowArray[] CreateColumnMetadataEmptyArray()
        {
            return
            [
                new StringArray.Builder().Build(), // TABLE_CAT
                new StringArray.Builder().Build(), // TABLE_SCHEM
                new StringArray.Builder().Build(), // TABLE_NAME
                new StringArray.Builder().Build(), // COLUMN_NAME
                new Int32Array.Builder().Build(),  // DATA_TYPE
                new StringArray.Builder().Build(), // TYPE_NAME
                new Int32Array.Builder().Build(),  // COLUMN_SIZE
                new Int8Array.Builder().Build(),   // BUFFER_LENGTH
                new Int32Array.Builder().Build(),  // DECIMAL_DIGITS
                new Int32Array.Builder().Build(),  // NUM_PREC_RADIX
                new Int32Array.Builder().Build(),  // NULLABLE
                new StringArray.Builder().Build(), // REMARKS
                new StringArray.Builder().Build(), // COLUMN_DEF
                new Int32Array.Builder().Build(),  // SQL_DATA_TYPE
                new Int32Array.Builder().Build(),  // SQL_DATETIME_SUB
                new Int32Array.Builder().Build(),  // CHAR_OCTET_LENGTH
                new Int32Array.Builder().Build(),  // ORDINAL_POSITION
                new StringArray.Builder().Build(), // IS_NULLABLE
                new StringArray.Builder().Build(), // SCOPE_CATALOG
                new StringArray.Builder().Build(), // SCOPE_SCHEMA
                new StringArray.Builder().Build(), // SCOPE_TABLE
                new Int16Array.Builder().Build(),  // SOURCE_DATA_TYPE
                new StringArray.Builder().Build(), // IS_AUTO_INCREMENT
                new StringArray.Builder().Build()  // BASE_TYPE_NAME
            ];
        }
    }
}
