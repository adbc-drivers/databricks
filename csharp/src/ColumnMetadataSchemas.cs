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
    /// Shared helper class for standard metadata schemas.
    /// Used by both Thrift and Statement Execution API protocols to ensure consistent metadata structure.
    /// Provides schemas for GetCatalogs, GetSchemas, GetTables, GetColumns, GetPrimaryKeys, GetCrossReference.
    /// </summary>
    internal static class ColumnMetadataSchemas
    {
        #region GetColumns Schema (24 columns)

        /// <summary>
        /// Creates the standard GetColumns schema.
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
        /// <returns>Array of empty Arrow arrays</returns>
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

        #endregion

        #region GetCatalogs Schema (1 column)

        /// <summary>
        /// Creates the standard GetCatalogs schema (1 column: TABLE_CAT).
        /// Follows JDBC DatabaseMetaData.getCatalogs() convention.
        /// </summary>
        /// <returns>Schema with TABLE_CAT column</returns>
        public static Schema CreateCatalogsSchema()
        {
            var fields = new[]
            {
                new Field("TABLE_CAT", StringType.Default, true)
            };
            return new Schema(fields, null);
        }

        /// <summary>
        /// Creates empty Arrow arrays for GetCatalogs schema.
        /// </summary>
        /// <returns>Array of empty Arrow arrays (1 column)</returns>
        public static IArrowArray[] CreateCatalogsEmptyArray()
        {
            return
            [
                new StringArray.Builder().Build() // TABLE_CAT
            ];
        }

        #endregion

        #region GetSchemas Schema (2 columns)

        /// <summary>
        /// Creates the standard GetSchemas schema (2 columns: TABLE_SCHEM, TABLE_CATALOG).
        /// Follows JDBC DatabaseMetaData.getSchemas() convention.
        /// </summary>
        /// <returns>Schema with TABLE_SCHEM and TABLE_CATALOG columns</returns>
        public static Schema CreateSchemasSchema()
        {
            var fields = new[]
            {
                new Field("TABLE_SCHEM", StringType.Default, true),
                new Field("TABLE_CATALOG", StringType.Default, true)
            };
            return new Schema(fields, null);
        }

        /// <summary>
        /// Creates empty Arrow arrays for GetSchemas schema.
        /// </summary>
        /// <returns>Array of empty Arrow arrays (2 columns)</returns>
        public static IArrowArray[] CreateSchemasEmptyArray()
        {
            return
            [
                new StringArray.Builder().Build(), // TABLE_SCHEM
                new StringArray.Builder().Build()  // TABLE_CATALOG
            ];
        }

        #endregion

        #region GetTables Schema (10 columns)

        /// <summary>
        /// Creates the standard GetTables schema (10 columns).
        /// Follows JDBC DatabaseMetaData.getTables() convention.
        /// </summary>
        /// <returns>Schema with 10 table metadata columns</returns>
        public static Schema CreateTablesSchema()
        {
            var fields = new[]
            {
                new Field("TABLE_CAT", StringType.Default, true),
                new Field("TABLE_SCHEM", StringType.Default, true),
                new Field("TABLE_NAME", StringType.Default, true),
                new Field("TABLE_TYPE", StringType.Default, true),
                new Field("REMARKS", StringType.Default, true),
                new Field("TYPE_CAT", StringType.Default, true),
                new Field("TYPE_SCHEM", StringType.Default, true),
                new Field("TYPE_NAME", StringType.Default, true),
                new Field("SELF_REFERENCING_COL_NAME", StringType.Default, true),
                new Field("REF_GENERATION", StringType.Default, true)
            };
            return new Schema(fields, null);
        }

        /// <summary>
        /// Creates empty Arrow arrays for GetTables schema.
        /// </summary>
        /// <returns>Array of empty Arrow arrays (10 columns)</returns>
        public static IArrowArray[] CreateTablesEmptyArray()
        {
            return
            [
                new StringArray.Builder().Build(), // TABLE_CAT
                new StringArray.Builder().Build(), // TABLE_SCHEM
                new StringArray.Builder().Build(), // TABLE_NAME
                new StringArray.Builder().Build(), // TABLE_TYPE
                new StringArray.Builder().Build(), // REMARKS
                new StringArray.Builder().Build(), // TYPE_CAT
                new StringArray.Builder().Build(), // TYPE_SCHEM
                new StringArray.Builder().Build(), // TYPE_NAME
                new StringArray.Builder().Build(), // SELF_REFERENCING_COL_NAME
                new StringArray.Builder().Build()  // REF_GENERATION
            ];
        }

        #endregion

        #region GetPrimaryKeys Schema (6 columns)

        /// <summary>
        /// Creates the standard GetPrimaryKeys schema (6 columns).
        /// </summary>
        /// <returns>Schema with 6 primary key metadata columns</returns>
        public static Schema CreatePrimaryKeySchema()
        {
            var fields = new[]
            {
                new Field("TABLE_CAT", StringType.Default, true),
                new Field("TABLE_SCHEM", StringType.Default, true),
                new Field("TABLE_NAME", StringType.Default, true),
                new Field("COLUMN_NAME", StringType.Default, true),
                new Field("KEQ_SEQ", Int16Type.Default, true),
                new Field("PK_NAME", StringType.Default, true)
            };
            return new Schema(fields, null);
        }

        /// <summary>
        /// Creates empty Arrow arrays for GetPrimaryKeys schema.
        /// </summary>
        /// <returns>Array of empty Arrow arrays (6 columns)</returns>
        public static IArrowArray[] CreatePrimaryKeyEmptyArray()
        {
            return
            [
                new StringArray.Builder().Build(), // TABLE_CAT
                new StringArray.Builder().Build(), // TABLE_SCHEM
                new StringArray.Builder().Build(), // TABLE_NAME
                new StringArray.Builder().Build(), // COLUMN_NAME
                new Int16Array.Builder().Build(),  // KEQ_SEQ
                new StringArray.Builder().Build()  // PK_NAME
            ];
        }

        #endregion

        #region GetCrossReference (Foreign Keys) Schema (14 columns)

        /// <summary>
        /// Creates the standard GetCrossReference schema (14 columns).
        /// </summary>
        /// <returns>Schema with 14 foreign key metadata columns</returns>
        public static Schema CreateForeignKeySchema()
        {
            var fields = new[]
            {
                new Field("PKTABLE_CAT", StringType.Default, true),
                new Field("PKTABLE_SCHEM", StringType.Default, true),
                new Field("PKTABLE_NAME", StringType.Default, true),
                new Field("PKCOLUMN_NAME", StringType.Default, true),
                new Field("FKTABLE_CAT", StringType.Default, true),
                new Field("FKTABLE_SCHEM", StringType.Default, true),
                new Field("FKTABLE_NAME", StringType.Default, true),
                new Field("FKCOLUMN_NAME", StringType.Default, true),
                new Field("KEQ_SEQ", Int16Type.Default, true),
                new Field("UPDATE_RULE", Int16Type.Default, true),
                new Field("DELETE_RULE", Int16Type.Default, true),
                new Field("FK_NAME", StringType.Default, true),
                new Field("PK_NAME", StringType.Default, true),
                new Field("DEFERRABILITY", Int16Type.Default, true)
            };
            return new Schema(fields, null);
        }

        /// <summary>
        /// Creates empty Arrow arrays for GetCrossReference schema.
        /// </summary>
        /// <returns>Array of empty Arrow arrays (14 columns)</returns>
        public static IArrowArray[] CreateForeignKeyEmptyArray()
        {
            return
            [
                new StringArray.Builder().Build(), // PKTABLE_CAT
                new StringArray.Builder().Build(), // PKTABLE_SCHEM
                new StringArray.Builder().Build(), // PKTABLE_NAME
                new StringArray.Builder().Build(), // PKCOLUMN_NAME
                new StringArray.Builder().Build(), // FKTABLE_CAT
                new StringArray.Builder().Build(), // FKTABLE_SCHEM
                new StringArray.Builder().Build(), // FKTABLE_NAME
                new StringArray.Builder().Build(), // FKCOLUMN_NAME
                new Int16Array.Builder().Build(),  // KEQ_SEQ
                new Int16Array.Builder().Build(),  // UPDATE_RULE
                new Int16Array.Builder().Build(),  // DELETE_RULE
                new StringArray.Builder().Build(), // FK_NAME
                new StringArray.Builder().Build(), // PK_NAME
                new Int16Array.Builder().Build()   // DEFERRABILITY
            ];
        }

        #endregion
    }
}
