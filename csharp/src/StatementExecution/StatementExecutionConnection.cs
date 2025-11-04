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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Ipc;

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
        /// <exception cref="ArgumentNullException">Thrown if client or properties is null.</exception>
        /// <exception cref="ArgumentException">Thrown if required properties are missing or invalid.</exception>
        public StatementExecutionConnection(IStatementExecutionClient client, IReadOnlyDictionary<string, string> properties)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _properties = properties ?? throw new ArgumentNullException(nameof(properties));

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
        /// <remarks>
        /// Returns a StatementExecutionStatement instance. Full query execution
        /// implementation will be completed in PECO-2791-B.
        /// </remarks>
        public override AdbcStatement CreateStatement()
        {
            return new StatementExecutionStatement(_client, _warehouseId, _sessionId);
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
        /// Metadata operations via Statement Execution API will be implemented in a future release.
        /// </remarks>
        public override IArrowArrayStream GetObjects(
            GetObjectsDepth depth,
            string? catalogPattern,
            string? dbSchemaPattern,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern)
        {
            throw AdbcException.NotImplemented(
                "GetObjects not yet implemented for Statement Execution API. " +
                "Metadata operations will be added in a future release.");
        }

        /// <summary>
        /// Get the Arrow schema of a database table.
        /// </summary>
        /// <remarks>
        /// Metadata operations via Statement Execution API will be implemented in a future release.
        /// </remarks>
        public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
        {
            throw AdbcException.NotImplemented(
                "GetTableSchema not yet implemented for Statement Execution API. " +
                "Metadata operations will be added in a future release.");
        }

        /// <summary>
        /// Get a list of table types supported by the database.
        /// </summary>
        /// <remarks>
        /// Metadata operations via Statement Execution API will be implemented in a future release.
        /// </remarks>
        public override IArrowArrayStream GetTableTypes()
        {
            throw AdbcException.NotImplemented(
                "GetTableTypes not yet implemented for Statement Execution API. " +
                "Metadata operations will be added in a future release.");
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
    }
}
