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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution
{
    /// <summary>
    /// Connection implementation for Databricks Statement Execution REST API.
    /// Manages SQL sessions and statement creation using the REST protocol.
    /// </summary>
    internal class StatementExecutionConnection : AdbcConnection
    {
        private readonly IStatementExecutionClient _client;
        private readonly IReadOnlyDictionary<string, string> _properties;
        private readonly string _warehouseId;
        private readonly string? _catalog;
        private readonly string? _schema;
        private readonly bool _enableSessionManagement;
        private readonly HttpClient _httpClient;
        private string? _sessionId;
        private bool _disposed;

        /// <summary>
        /// Gets the session ID if session management is enabled and a session has been created.
        /// </summary>
        public string? SessionId => _sessionId;

        /// <summary>
        /// Gets the warehouse ID extracted from the http_path parameter.
        /// </summary>
        public string WarehouseId => _warehouseId;

        /// <summary>
        /// Initializes a new instance of the StatementExecutionConnection class.
        /// </summary>
        /// <param name="client">The Statement Execution API client.</param>
        /// <param name="properties">Connection properties.</param>
        /// <param name="httpClient">HTTP client for CloudFetch downloads.</param>
        /// <exception cref="ArgumentNullException">Thrown if client or properties is null.</exception>
        /// <exception cref="ArgumentException">Thrown if required properties are missing or invalid.</exception>
        public StatementExecutionConnection(
            IStatementExecutionClient client,
            IReadOnlyDictionary<string, string> properties,
            HttpClient httpClient)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _properties = properties ?? throw new ArgumentNullException(nameof(properties));
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));

            // Extract warehouse ID from http_path
            _warehouseId = ExtractWarehouseId(properties);

            // Extract optional catalog and schema (using standard ADBC parameters)
            properties.TryGetValue(AdbcOptions.Connection.CurrentCatalog, out _catalog);
            properties.TryGetValue(AdbcOptions.Connection.CurrentDbSchema, out _schema);

            // Check if session management is enabled (default: true)
            _enableSessionManagement = true;
            if (properties.TryGetValue(DatabricksParameters.EnableSessionManagement, out var enableSessionMgmt))
            {
                if (bool.TryParse(enableSessionMgmt, out var enabled))
                {
                    _enableSessionManagement = enabled;
                }
            }
        }

        /// <summary>
        /// Opens the connection and creates a session if session management is enabled.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task OpenAsync(CancellationToken cancellationToken = default)
        {
            if (_enableSessionManagement && _sessionId == null)
            {
                var request = new CreateSessionRequest
                {
                    WarehouseId = _warehouseId,
                    Catalog = _catalog,
                    Schema = _schema
                };

                var response = await _client.CreateSessionAsync(request, cancellationToken).ConfigureAwait(false);
                _sessionId = response.SessionId;
            }
        }

        /// <summary>
        /// Creates a new statement for executing queries.
        /// </summary>
        /// <returns>A new statement instance.</returns>
        public override AdbcStatement CreateStatement()
        {
            return new StatementExecutionStatement(_client, _warehouseId, _sessionId, _properties, _httpClient);
        }

        /// <summary>
        /// Closes the connection and deletes the session if one was created.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task CloseAsync(CancellationToken cancellationToken = default)
        {
            if (_enableSessionManagement && _sessionId != null)
            {
                try
                {
                    await _client.DeleteSessionAsync(_sessionId, _warehouseId, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    // Swallow exceptions during session deletion to avoid masking other errors
                    // TODO: Consider logging this error
                }
                finally
                {
                    _sessionId = null;
                }
            }
        }

        /// <summary>
        /// Get a hierarchical view of all catalogs, database schemas, tables, and columns.
        /// </summary>
        /// <remarks>
        /// Implementation uses SQL queries (SHOW CATALOGS, SHOW SCHEMAS, SHOW TABLES, DESCRIBE TABLE)
        /// to retrieve metadata from Databricks.
        /// </remarks>
        public override IArrowArrayStream GetObjects(
            GetObjectsDepth depth,
            string? catalogPattern,
            string? dbSchemaPattern,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern)
        {
            return GetObjectsAsync(depth, catalogPattern, dbSchemaPattern, tableNamePattern, tableTypes, columnNamePattern)
                .GetAwaiter().GetResult();
        }

        private async Task<IArrowArrayStream> GetObjectsAsync(
            GetObjectsDepth depth,
            string? catalogPattern,
            string? dbSchemaPattern,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern)
        {
            var catalogBuilder = new List<(string catalogName, List<(string schemaName, List<(string tableName, string tableType, List<ColumnInfo> columns)> tables)> schemas)>();

            // Step 1: Get catalogs
            if (depth >= GetObjectsDepth.Catalogs)
            {
                var catalogs = await GetCatalogsAsync(catalogPattern).ConfigureAwait(false);

                foreach (var catalog in catalogs)
                {
                    var schemasList = new List<(string schemaName, List<(string tableName, string tableType, List<ColumnInfo> columns)> tables)>();

                    // Step 2: Get schemas for this catalog
                    if (depth >= GetObjectsDepth.DbSchemas)
                    {
                        var schemas = await GetSchemasAsync(catalog, dbSchemaPattern).ConfigureAwait(false);

                        foreach (var schema in schemas)
                        {
                            var tablesList = new List<(string tableName, string tableType, List<ColumnInfo> columns)>();

                            // Step 3: Get tables for this schema
                            if (depth >= GetObjectsDepth.Tables)
                            {
                                var tables = await GetTablesAsync(catalog, schema, tableNamePattern, tableTypes).ConfigureAwait(false);

                                foreach (var table in tables)
                                {
                                    var columnsList = new List<ColumnInfo>();

                                    // Step 4: Get columns for this table
                                    if (depth == GetObjectsDepth.All && !string.IsNullOrEmpty(columnNamePattern))
                                    {
                                        columnsList = await GetColumnsAsync(catalog, schema, table.tableName, columnNamePattern).ConfigureAwait(false);
                                    }
                                    else if (depth == GetObjectsDepth.All)
                                    {
                                        columnsList = await GetColumnsAsync(catalog, schema, table.tableName, null).ConfigureAwait(false);
                                    }

                                    tablesList.Add((table.tableName, table.tableType, columnsList));
                                }
                            }

                            schemasList.Add((schema, tablesList));
                        }
                    }

                    catalogBuilder.Add((catalog, schemasList));
                }
            }

            // Build Arrow RecordBatch
            return BuildGetObjectsResult(catalogBuilder);
        }

        /// <summary>
        /// Get the Arrow schema of a database table.
        /// </summary>
        /// <remarks>
        /// Implementation uses DESCRIBE TABLE to retrieve column metadata.
        /// </remarks>
        public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
        {
            return GetTableSchemaAsync(catalog, dbSchema, tableName).GetAwaiter().GetResult();
        }

        private async Task<Schema> GetTableSchemaAsync(string? catalog, string? dbSchema, string tableName)
        {
            var columns = await GetColumnsAsync(catalog, dbSchema, tableName, null).ConfigureAwait(false);

            var fields = new List<Field>();
            foreach (var column in columns)
            {
                var arrowType = ConvertDatabricksTypeToArrow(column.TypeName);
                fields.Add(new Field(column.Name, arrowType, column.Nullable));
            }

            return new Schema(fields, null);
        }

        /// <summary>
        /// Get a list of table types supported by the database.
        /// </summary>
        /// <remarks>
        /// Returns the standard table types: TABLE, VIEW, SYSTEM TABLE, GLOBAL TEMPORARY, LOCAL TEMPORARY, ALIAS, SYNONYM.
        /// </remarks>
        public override IArrowArrayStream GetTableTypes()
        {
            // Return standard Databricks table types
            var tableTypes = new[] { "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM" };

            var builder = new StringArray.Builder();
            foreach (var tableType in tableTypes)
            {
                builder.Append(tableType);
            }

            var batch = new RecordBatch(
                StandardSchemas.TableTypesSchema,
                new[] { builder.Build() },
                tableTypes.Length);

            return new SingleBatchArrowArrayStream(batch);
        }

        /// <summary>
        /// Extracts the warehouse ID from the http_path property (SparkParameters.Path).
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <returns>The warehouse ID.</returns>
        /// <exception cref="ArgumentException">Thrown if http_path is missing or invalid.</exception>
        private static string ExtractWarehouseId(IReadOnlyDictionary<string, string> properties)
        {
            // Use the standard SparkParameters.Path (adbc.spark.path) for http_path
            if (!properties.TryGetValue(SparkParameters.Path, out var httpPath))
            {
                throw new ArgumentException(
                    $"Missing required property: {SparkParameters.Path}");
            }

            if (string.IsNullOrWhiteSpace(httpPath))
            {
                throw new ArgumentException(
                    $"Property {SparkParameters.Path} cannot be null or empty");
            }

            // Expected format: /sql/1.0/warehouses/{warehouse_id}
            // Also support: /sql/1.0/warehouses/{warehouse_id}/
            var parts = httpPath.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);

            // Look for "warehouses" segment followed by the warehouse ID
            for (int i = 0; i < parts.Length - 1; i++)
            {
                if (parts[i].Equals("warehouses", StringComparison.OrdinalIgnoreCase))
                {
                    var warehouseId = parts[i + 1];
                    if (!string.IsNullOrWhiteSpace(warehouseId))
                    {
                        return warehouseId;
                    }
                }
            }

            throw new ArgumentException(
                $"Invalid http_path format: '{httpPath}'. Expected format: /sql/1.0/warehouses/{{warehouse_id}}");
        }

        /// <summary>
        /// Disposes the connection and releases resources.
        /// </summary>
        public override void Dispose()
        {
            if (!_disposed)
            {
                // Synchronously close the connection
                // In a real implementation, we should consider async disposal
                if (_enableSessionManagement && _sessionId != null)
                {
                    try
                    {
                        CloseAsync(CancellationToken.None).GetAwaiter().GetResult();
                    }
                    catch
                    {
                        // Swallow exceptions during disposal
                    }
                }

                base.Dispose();
                _disposed = true;
            }
        }

        // ============================================================================
        // Helper Methods for Metadata Operations
        // ============================================================================

        /// <summary>
        /// Executes a SQL query and returns the results.
        /// </summary>
        private async Task<List<RecordBatch>> ExecuteSqlQueryAsync(string sql)
        {
            using var statement = CreateStatement();
            statement.SqlQuery = sql;

            var result = await statement.ExecuteQueryAsync().ConfigureAwait(false);
            var batches = new List<RecordBatch>();

            while (true)
            {
                var batch = await result.Stream.ReadNextRecordBatchAsync().ConfigureAwait(false);
                if (batch == null)
                    break;
                batches.Add(batch);
            }

            return batches;
        }

        /// <summary>
        /// Gets list of catalogs matching the pattern.
        /// </summary>
        private async Task<List<string>> GetCatalogsAsync(string? catalogPattern)
        {
            var sql = "SHOW CATALOGS";
            if (!string.IsNullOrEmpty(catalogPattern))
            {
                sql += $" LIKE '{EscapeSqlPattern(catalogPattern)}'";
            }

            var batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
            var catalogs = new List<string>();

            foreach (var batch in batches)
            {
                // SHOW CATALOGS returns a single column 'catalog' or 'namespace'
                var catalogColumn = batch.Column(0) as StringArray;
                if (catalogColumn != null)
                {
                    for (int i = 0; i < catalogColumn.Length; i++)
                    {
                        if (!catalogColumn.IsNull(i))
                        {
                            catalogs.Add(catalogColumn.GetString(i));
                        }
                    }
                }
            }

            return catalogs;
        }

        /// <summary>
        /// Gets list of schemas in a catalog matching the pattern.
        /// </summary>
        private async Task<List<string>> GetSchemasAsync(string catalog, string? schemaPattern)
        {
            var sql = $"SHOW SCHEMAS IN {QuoteIdentifier(catalog)}";
            if (!string.IsNullOrEmpty(schemaPattern))
            {
                sql += $" LIKE '{EscapeSqlPattern(schemaPattern)}'";
            }

            var batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
            var schemas = new List<string>();

            foreach (var batch in batches)
            {
                // SHOW SCHEMAS returns 'databaseName' column
                var schemaColumn = batch.Column(0) as StringArray;
                if (schemaColumn != null)
                {
                    for (int i = 0; i < schemaColumn.Length; i++)
                    {
                        if (!schemaColumn.IsNull(i))
                        {
                            schemas.Add(schemaColumn.GetString(i));
                        }
                    }
                }
            }

            return schemas;
        }

        /// <summary>
        /// Gets list of tables in a schema matching the pattern and table types.
        /// </summary>
        private async Task<List<(string tableName, string tableType)>> GetTablesAsync(
            string catalog,
            string schema,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes)
        {
            var sql = $"SHOW TABLES IN {QuoteIdentifier(catalog)}.{QuoteIdentifier(schema)}";
            if (!string.IsNullOrEmpty(tableNamePattern))
            {
                sql += $" LIKE '{EscapeSqlPattern(tableNamePattern)}'";
            }

            var batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
            var tables = new List<(string tableName, string tableType)>();

            foreach (var batch in batches)
            {
                // SHOW TABLES returns columns: database, tableName, isTemporary
                // We need to find the tableName column (usually column 1)
                StringArray? tableNameColumn = null;
                BooleanArray? isTemporaryColumn = null;

                // Find columns by iterating through schema
                for (int colIndex = 0; colIndex < batch.Schema.FieldsList.Count; colIndex++)
                {
                    var fieldName = batch.Schema.GetFieldByIndex(colIndex).Name;
                    if (fieldName.Equals("tableName", StringComparison.OrdinalIgnoreCase))
                    {
                        tableNameColumn = batch.Column(colIndex) as StringArray;
                    }
                    else if (fieldName.Equals("isTemporary", StringComparison.OrdinalIgnoreCase))
                    {
                        isTemporaryColumn = batch.Column(colIndex) as BooleanArray;
                    }
                }

                if (tableNameColumn != null)
                {
                    for (int i = 0; i < tableNameColumn.Length; i++)
                    {
                        if (!tableNameColumn.IsNull(i))
                        {
                            var tableName = tableNameColumn.GetString(i);
                            // Determine table type
                            var tableType = "TABLE";
                            if (isTemporaryColumn != null && !isTemporaryColumn.IsNull(i) && isTemporaryColumn.GetValue(i) == true)
                            {
                                tableType = "LOCAL TEMPORARY";
                            }

                            // Filter by table types if specified
                            if (tableTypes == null || tableTypes.Count == 0 || tableTypes.Contains(tableType))
                            {
                                tables.Add((tableName, tableType));
                            }
                        }
                    }
                }
            }

            return tables;
        }

        /// <summary>
        /// Gets list of columns for a table.
        /// </summary>
        private async Task<List<ColumnInfo>> GetColumnsAsync(
            string? catalog,
            string? schema,
            string tableName,
            string? columnNamePattern)
        {
            // Build fully qualified table name
            var qualifiedTableName = BuildQualifiedTableName(catalog, schema, tableName);

            var sql = $"DESCRIBE TABLE {qualifiedTableName}";

            var batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
            var columns = new List<ColumnInfo>();
            int position = 1;

            foreach (var batch in batches)
            {
                // DESCRIBE TABLE returns: col_name, data_type, comment
                StringArray? colNameColumn = null;
                StringArray? dataTypeColumn = null;
                StringArray? commentColumn = null;

                for (int colIndex = 0; colIndex < batch.Schema.FieldsList.Count; colIndex++)
                {
                    var fieldName = batch.Schema.GetFieldByIndex(colIndex).Name;
                    if (fieldName.Equals("col_name", StringComparison.OrdinalIgnoreCase))
                    {
                        colNameColumn = batch.Column(colIndex) as StringArray;
                    }
                    else if (fieldName.Equals("data_type", StringComparison.OrdinalIgnoreCase))
                    {
                        dataTypeColumn = batch.Column(colIndex) as StringArray;
                    }
                    else if (fieldName.Equals("comment", StringComparison.OrdinalIgnoreCase))
                    {
                        commentColumn = batch.Column(colIndex) as StringArray;
                    }
                }

                if (colNameColumn != null && dataTypeColumn != null)
                {
                    for (int i = 0; i < colNameColumn.Length; i++)
                    {
                        if (!colNameColumn.IsNull(i))
                        {
                            var colName = colNameColumn.GetString(i);

                            // Skip partition information and metadata rows
                            if (colName.StartsWith("#") || string.IsNullOrWhiteSpace(colName))
                            {
                                continue;
                            }

                            // Match column pattern if specified
                            if (!string.IsNullOrEmpty(columnNamePattern) && !PatternMatches(colName, columnNamePattern))
                            {
                                continue;
                            }

                            var dataType = !dataTypeColumn.IsNull(i) ? dataTypeColumn.GetString(i) : "string";
                            var comment = (commentColumn != null && !commentColumn.IsNull(i)) ? commentColumn.GetString(i) : null;

                            columns.Add(new ColumnInfo
                            {
                                Name = colName,
                                TypeName = dataType,
                                Position = position++,
                                Nullable = true, // Assume nullable unless we can determine otherwise
                                Comment = comment
                            });
                        }
                    }
                }
            }

            return columns;
        }

        /// <summary>
        /// Builds the GetObjects result as an Arrow array stream.
        /// </summary>
        private IArrowArrayStream BuildGetObjectsResult(
            List<(string catalogName, List<(string schemaName, List<(string tableName, string tableType, List<ColumnInfo> columns)> tables)> schemas)> catalogData)
        {
            var catalogNameBuilder = new StringArray.Builder();
            var catalogDbSchemasValues = new List<IArrowArray?>();

            foreach (var (catalogName, schemas) in catalogData)
            {
                catalogNameBuilder.Append(catalogName);

                // Build schemas structure for this catalog
                if (schemas.Count == 0)
                {
                    catalogDbSchemasValues.Add(null);
                }
                else
                {
                    catalogDbSchemasValues.Add(BuildDbSchemasStruct(schemas));
                }
            }

            Schema schema = StandardSchemas.GetObjectsSchema;
            var dbSchemasListArray = BuildListArray(catalogDbSchemasValues, new StructType(StandardSchemas.DbSchemaSchema));

            var batch = new RecordBatch(
                schema,
                new IArrowArray[] { catalogNameBuilder.Build(), dbSchemasListArray },
                catalogData.Count);

            return new SingleBatchArrowArrayStream(batch);
        }

        /// <summary>
        /// Builds a StructArray for database schemas.
        /// </summary>
        private static StructArray BuildDbSchemasStruct(
            List<(string schemaName, List<(string tableName, string tableType, List<ColumnInfo> columns)> tables)> schemas)
        {
            var dbSchemaNameBuilder = new StringArray.Builder();
            var dbSchemaTablesValues = new List<IArrowArray?>();
            var nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            foreach (var (schemaName, tables) in schemas)
            {
                dbSchemaNameBuilder.Append(schemaName);
                length++;
                nullBitmapBuffer.Append(true);

                if (tables.Count == 0)
                {
                    dbSchemaTablesValues.Add(null);
                }
                else
                {
                    dbSchemaTablesValues.Add(BuildTablesStruct(tables));
                }
            }

            IReadOnlyList<Field> schemaFields = StandardSchemas.DbSchemaSchema;
            var tablesListArray = BuildListArray(dbSchemaTablesValues, new StructType(StandardSchemas.TableSchema));

            return new StructArray(
                new StructType(schemaFields),
                length,
                new IArrowArray[] { dbSchemaNameBuilder.Build(), tablesListArray },
                nullBitmapBuffer.Build());
        }

        /// <summary>
        /// Builds a StructArray for tables.
        /// </summary>
        private static StructArray BuildTablesStruct(
            List<(string tableName, string tableType, List<ColumnInfo> columns)> tables)
        {
            var tableNameBuilder = new StringArray.Builder();
            var tableTypeBuilder = new StringArray.Builder();
            var tableColumnsValues = new List<IArrowArray?>();
            var tableConstraintsValues = new List<IArrowArray?>();
            var nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            foreach (var (tableName, tableType, columns) in tables)
            {
                tableNameBuilder.Append(tableName);
                tableTypeBuilder.Append(tableType);
                nullBitmapBuffer.Append(true);
                length++;

                // Constraints not supported
                tableConstraintsValues.Add(null);

                if (columns.Count == 0)
                {
                    tableColumnsValues.Add(null);
                }
                else
                {
                    tableColumnsValues.Add(BuildColumnsStruct(columns));
                }
            }

            IReadOnlyList<Field> schemaFields = StandardSchemas.TableSchema;
            var columnsListArray = BuildListArray(tableColumnsValues, new StructType(StandardSchemas.ColumnSchema));
            var constraintsListArray = BuildListArray(tableConstraintsValues, new StructType(StandardSchemas.ConstraintSchema));

            return new StructArray(
                new StructType(schemaFields),
                length,
                new IArrowArray[] {
                    tableNameBuilder.Build(),
                    tableTypeBuilder.Build(),
                    columnsListArray,
                    constraintsListArray
                },
                nullBitmapBuffer.Build());
        }

        /// <summary>
        /// Builds a StructArray for columns.
        /// </summary>
        private static StructArray BuildColumnsStruct(List<ColumnInfo> columns)
        {
            var columnNameBuilder = new StringArray.Builder();
            var ordinalPositionBuilder = new Int32Array.Builder();
            var remarksBuilder = new StringArray.Builder();
            var xdbcDataTypeBuilder = new Int16Array.Builder();
            var xdbcTypeNameBuilder = new StringArray.Builder();
            var xdbcColumnSizeBuilder = new Int32Array.Builder();
            var xdbcDecimalDigitsBuilder = new Int16Array.Builder();
            var xdbcNumPrecRadixBuilder = new Int16Array.Builder();
            var xdbcNullableBuilder = new Int16Array.Builder();
            var xdbcColumnDefBuilder = new StringArray.Builder();
            var xdbcSqlDataTypeBuilder = new Int16Array.Builder();
            var xdbcDatetimeSubBuilder = new Int16Array.Builder();
            var xdbcCharOctetLengthBuilder = new Int32Array.Builder();
            var xdbcIsNullableBuilder = new StringArray.Builder();
            var xdbcScopeCatalogBuilder = new StringArray.Builder();
            var xdbcScopeSchemaBuilder = new StringArray.Builder();
            var xdbcScopeTableBuilder = new StringArray.Builder();
            var xdbcIsAutoincrementBuilder = new BooleanArray.Builder();
            var xdbcIsGeneratedcolumnBuilder = new BooleanArray.Builder();
            var nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();

            foreach (var column in columns)
            {
                columnNameBuilder.Append(column.Name);
                ordinalPositionBuilder.Append(column.Position);
                remarksBuilder.Append(column.Comment ?? string.Empty);

                // For now, use defaults for XDBC fields
                xdbcDataTypeBuilder.AppendNull();
                xdbcTypeNameBuilder.Append(column.TypeName ?? string.Empty);
                xdbcColumnSizeBuilder.AppendNull();
                xdbcDecimalDigitsBuilder.AppendNull();
                xdbcNumPrecRadixBuilder.AppendNull();
                xdbcNullableBuilder.AppendNull();
                xdbcColumnDefBuilder.AppendNull();
                xdbcSqlDataTypeBuilder.AppendNull();
                xdbcDatetimeSubBuilder.AppendNull();
                xdbcCharOctetLengthBuilder.AppendNull();
                xdbcIsNullableBuilder.Append(column.Nullable ? "YES" : "NO");
                xdbcScopeCatalogBuilder.AppendNull();
                xdbcScopeSchemaBuilder.AppendNull();
                xdbcScopeTableBuilder.AppendNull();
                xdbcIsAutoincrementBuilder.Append(false);
                xdbcIsGeneratedcolumnBuilder.Append(false);

                nullBitmapBuffer.Append(true);
            }

            IReadOnlyList<Field> schemaFields = StandardSchemas.ColumnSchema;

            return new StructArray(
                new StructType(schemaFields),
                columns.Count,
                new IArrowArray[] {
                    columnNameBuilder.Build(),
                    ordinalPositionBuilder.Build(),
                    remarksBuilder.Build(),
                    xdbcDataTypeBuilder.Build(),
                    xdbcTypeNameBuilder.Build(),
                    xdbcColumnSizeBuilder.Build(),
                    xdbcDecimalDigitsBuilder.Build(),
                    xdbcNumPrecRadixBuilder.Build(),
                    xdbcNullableBuilder.Build(),
                    xdbcColumnDefBuilder.Build(),
                    xdbcSqlDataTypeBuilder.Build(),
                    xdbcDatetimeSubBuilder.Build(),
                    xdbcCharOctetLengthBuilder.Build(),
                    xdbcIsNullableBuilder.Build(),
                    xdbcScopeCatalogBuilder.Build(),
                    xdbcScopeSchemaBuilder.Build(),
                    xdbcScopeTableBuilder.Build(),
                    xdbcIsAutoincrementBuilder.Build(),
                    xdbcIsGeneratedcolumnBuilder.Build()
                },
                nullBitmapBuffer.Build());
        }

        /// <summary>
        /// Builds a ListArray from a list of Arrow arrays.
        /// Simplified version of BuildListArrayForType extension method.
        /// </summary>
        private static ListArray BuildListArray(List<IArrowArray?> list, IArrowType dataType)
        {
            var valueOffsetsBuilder = new ArrowBuffer.Builder<int>();
            var validityBufferBuilder = new ArrowBuffer.BitmapBuilder();
            int length = 0;
            int nullCount = 0;
            var arrayDataList = new List<ArrayData>();

            foreach (var array in list)
            {
                if (array == null)
                {
                    valueOffsetsBuilder.Append(length);
                    validityBufferBuilder.Append(false);
                    nullCount++;
                }
                else
                {
                    valueOffsetsBuilder.Append(length);
                    validityBufferBuilder.Append(true);
                    arrayDataList.Add(array.Data);
                    length += array.Length;
                }
            }

            ArrowBuffer validityBuffer = nullCount > 0
                ? validityBufferBuilder.Build()
                : ArrowBuffer.Empty;

            // Concatenate all array data
            IArrowArray valueArray;
            if (arrayDataList.Count > 0)
            {
                var concatenated = ArrayDataConcatenator.Concatenate(arrayDataList);
                valueArray = ArrowArrayFactory.BuildArray(concatenated!);
            }
            else
            {
                // Create empty array of the appropriate type
                valueArray = CreateEmptyArray(dataType);
            }

            valueOffsetsBuilder.Append(length);

            return new ListArray(
                new ListType(dataType),
                list.Count,
                valueOffsetsBuilder.Build(),
                valueArray,
                validityBuffer,
                nullCount,
                0);
        }

        /// <summary>
        /// Creates an empty array for a given type.
        /// </summary>
        private static IArrowArray CreateEmptyArray(IArrowType type)
        {
            if (type is StructType structType)
            {
                var children = new ArrayData[structType.Fields.Count];
                for (int i = 0; i < structType.Fields.Count; i++)
                {
                    children[i] = CreateEmptyArray(structType.Fields[i].DataType).Data;
                }
                var arrayData = new ArrayData(structType, 0, 0, 0, new[] { ArrowBuffer.Empty }, children);
                return ArrowArrayFactory.BuildArray(arrayData);
            }
            else if (type is StringType)
            {
                return new StringArray.Builder().Build();
            }
            else if (type is Int32Type)
            {
                return new Int32Array.Builder().Build();
            }
            else if (type is Int16Type)
            {
                return new Int16Array.Builder().Build();
            }
            else if (type is BooleanType)
            {
                return new BooleanArray.Builder().Build();
            }
            else
            {
                // Fallback for unknown types
                return new StringArray.Builder().Build();
            }
        }

        /// <summary>
        /// Converts Databricks type string to Arrow type.
        /// </summary>
        private IArrowType ConvertDatabricksTypeToArrow(string databricksType)
        {
            var lowerType = databricksType.ToLowerInvariant();

            if (lowerType.Contains("int")) return Int64Type.Default;
            if (lowerType.Contains("long")) return Int64Type.Default;
            if (lowerType.Contains("double")) return DoubleType.Default;
            if (lowerType.Contains("float")) return FloatType.Default;
            if (lowerType.Contains("bool")) return BooleanType.Default;
            if (lowerType.Contains("string")) return StringType.Default;
            if (lowerType.Contains("binary")) return BinaryType.Default;
            if (lowerType.Contains("date")) return Date64Type.Default;
            if (lowerType.Contains("timestamp")) return new TimestampType(TimeUnit.Microsecond, timezone: (string?)null);
            if (lowerType.Contains("decimal")) return new Decimal128Type(38, 0); // Default precision/scale

            // Default to string for unknown types
            return StringType.Default;
        }

        /// <summary>
        /// Builds a fully qualified table name.
        /// </summary>
        private string BuildQualifiedTableName(string? catalog, string? schema, string tableName)
        {
            var parts = new List<string>();

            if (!string.IsNullOrEmpty(catalog))
                parts.Add(QuoteIdentifier(catalog));

            if (!string.IsNullOrEmpty(schema))
                parts.Add(QuoteIdentifier(schema));

            parts.Add(QuoteIdentifier(tableName));

            return string.Join(".", parts);
        }

        /// <summary>
        /// Quotes an identifier for use in SQL.
        /// </summary>
        private string QuoteIdentifier(string identifier)
        {
            // Use backticks for Databricks
            return $"`{identifier.Replace("`", "``")}`";
        }

        /// <summary>
        /// Escapes a SQL LIKE pattern.
        /// </summary>
        private string EscapeSqlPattern(string pattern)
        {
            return pattern.Replace("'", "''");
        }

        /// <summary>
        /// Checks if a value matches a SQL LIKE pattern.
        /// </summary>
        private bool PatternMatches(string value, string pattern)
        {
            // Simple pattern matching - % for any characters, _ for single character
            var regexPattern = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
                .Replace("%", ".*")
                .Replace("_", ".") + "$";

            return System.Text.RegularExpressions.Regex.IsMatch(value, regexPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// Column information structure.
        /// </summary>
        private struct ColumnInfo
        {
            public string Name { get; set; }
            public string TypeName { get; set; }
            public int Position { get; set; }
            public bool Nullable { get; set; }
            public string? Comment { get; set; }
        }

        /// <summary>
        /// Simple Arrow array stream that returns a single batch.
        /// </summary>
        private class SingleBatchArrowArrayStream : IArrowArrayStream
        {
            private readonly RecordBatch _batch;
            private bool _read;

            public SingleBatchArrowArrayStream(RecordBatch batch)
            {
                _batch = batch ?? throw new ArgumentNullException(nameof(batch));
            }

            public Schema Schema => _batch.Schema;

            public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_read)
                    return null;

                _read = true;
                return await Task.FromResult(_batch).ConfigureAwait(false);
            }

            public void Dispose()
            {
                _batch?.Dispose();
            }
        }
    }
}
