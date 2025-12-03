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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution
{
    /// <summary>
    /// Statement implementation using the Databricks Statement Execution REST API.
    /// Handles query execution, polling, and result retrieval.
    /// </summary>
    internal class StatementExecutionStatement : AdbcStatement
    {
        private readonly IStatementExecutionClient _client;
        private readonly string? _sessionId;
        private readonly string _warehouseId;
        private readonly string? _catalog;
        private readonly string? _schema;

        // Result configuration
        private readonly string _resultDisposition;
        private readonly string _resultFormat;
        private readonly string? _resultCompression;
        private readonly int _waitTimeoutSeconds;
        private readonly int _pollingIntervalMs;

        // Connection properties for CloudFetch configuration
        private readonly IReadOnlyDictionary<string, string> _properties;

        // Memory pooling
        private readonly Microsoft.IO.RecyclableMemoryStreamManager _recyclableMemoryStreamManager;
        private readonly System.Buffers.ArrayPool<byte> _lz4BufferPool;

        // Statement state
        private string? _currentStatementId;
        private string? _sqlQuery;

        public StatementExecutionStatement(
            IStatementExecutionClient client,
            string? sessionId,
            string warehouseId,
            string? catalog,
            string? schema,
            string resultDisposition,
            string resultFormat,
            string? resultCompression,
            int waitTimeoutSeconds,
            int pollingIntervalMs,
            IReadOnlyDictionary<string, string> properties,
            Microsoft.IO.RecyclableMemoryStreamManager recyclableMemoryStreamManager,
            System.Buffers.ArrayPool<byte> lz4BufferPool)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _sessionId = sessionId;
            _warehouseId = warehouseId ?? throw new ArgumentNullException(nameof(warehouseId));
            _catalog = catalog;
            _schema = schema;
            _resultDisposition = resultDisposition ?? throw new ArgumentNullException(nameof(resultDisposition));
            _resultFormat = resultFormat ?? throw new ArgumentNullException(nameof(resultFormat));
            _resultCompression = resultCompression;
            _waitTimeoutSeconds = waitTimeoutSeconds;
            _pollingIntervalMs = pollingIntervalMs;
            _properties = properties ?? throw new ArgumentNullException(nameof(properties));
            _recyclableMemoryStreamManager = recyclableMemoryStreamManager ?? throw new ArgumentNullException(nameof(recyclableMemoryStreamManager));
            _lz4BufferPool = lz4BufferPool ?? throw new ArgumentNullException(nameof(lz4BufferPool));
        }

        /// <summary>
        /// Gets or sets the SQL query to execute.
        /// </summary>
        public override string? SqlQuery
        {
            get => _sqlQuery;
            set => _sqlQuery = value;
        }

        /// <summary>
        /// Executes the query and returns a result set.
        /// </summary>
        public override QueryResult ExecuteQuery()
        {
            return ExecuteQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes the query asynchronously and returns a result set.
        /// </summary>
        public async Task<QueryResult> ExecuteQueryAsync(CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(_sqlQuery))
            {
                throw new InvalidOperationException("SQL query is required");
            }

            // Build the execute statement request
            // Note: warehouse_id is always required by the Databricks Statement Execution API
            var request = new ExecuteStatementRequest
            {
                Statement = _sqlQuery,
                WarehouseId = _warehouseId,
                SessionId = _sessionId,
                Catalog = _catalog,
                Schema = _schema,
                Disposition = _resultDisposition,
                Format = _resultFormat,
                ResultCompression = _resultCompression,
                WaitTimeout = $"{_waitTimeoutSeconds}s",
                OnWaitTimeout = "CONTINUE"
            };

            // Execute the statement
            var response = await _client.ExecuteStatementAsync(request, cancellationToken).ConfigureAwait(false);
            _currentStatementId = response.StatementId;

            // Handle query status according to Databricks API documentation:
            // PENDING: waiting for warehouse - continue polling
            // RUNNING: running - continue polling
            // SUCCEEDED: execution was successful, result data available for fetch
            // FAILED: execution failed; reason for failure described in accompanying error message
            // CANCELED: user canceled; can come from explicit cancel call, or timeout with on_wait_timeout=CANCEL
            // CLOSED: execution successful, and statement closed; result no longer available for fetch
            var state = response.Status?.State;
            if (state == "PENDING" || state == "RUNNING")
            {
                response = await PollUntilCompleteAsync(response.StatementId, cancellationToken).ConfigureAwait(false);
                state = response.Status?.State;
            }

            // Check for terminal error states
            if (state == "FAILED")
            {
                var error = response.Status?.Error;
                throw new AdbcException($"Statement execution failed: {error?.Message ?? "Unknown error"} (Error Code: {error?.ErrorCode})");
            }
            if (state == "CANCELED")
            {
                throw new AdbcException("Statement execution was canceled");
            }
            if (state == "CLOSED")
            {
                throw new AdbcException("Statement was closed before results could be retrieved");
            }

            // Check for truncated results warning
            if (response.Manifest?.Truncated == true)
            {
                Activity.Current?.AddEvent(new ActivityEvent("statement.results_truncated",
                    tags: new ActivityTagsCollection
                    {
                        { "total_row_count", response.Manifest.TotalRowCount },
                        { "total_byte_count", response.Manifest.TotalByteCount }
                    }));
            }

            // Create appropriate reader based on result disposition
            IArrowArrayStream reader = CreateReader(response, cancellationToken);

            // Get schema from reader
            var schema = reader.Schema;

            // Return query result - use -1 if row count is not available
            long rowCount = response.Manifest?.TotalRowCount ?? -1;
            return new QueryResult(rowCount, reader);
        }

        /// <summary>
        /// Polls the statement until it reaches a terminal state.
        /// Terminal states: SUCCEEDED, FAILED, CANCELED, CLOSED
        /// Non-terminal states: PENDING, RUNNING
        /// </summary>
        private async Task<ExecuteStatementResponse> PollUntilCompleteAsync(string statementId, CancellationToken cancellationToken)
        {
            while (true)
            {
                // Check for cancellation before each polling iteration
                cancellationToken.ThrowIfCancellationRequested();

                // Wait for polling interval
                await Task.Delay(_pollingIntervalMs, cancellationToken).ConfigureAwait(false);

                // Check for cancellation after delay
                cancellationToken.ThrowIfCancellationRequested();

                // Get statement status
                var response = await _client.GetStatementAsync(statementId, cancellationToken).ConfigureAwait(false);

                // Convert GetStatementResponse to ExecuteStatementResponse
                var executeResponse = new ExecuteStatementResponse
                {
                    StatementId = response.StatementId,
                    Status = response.Status,
                    Manifest = response.Manifest,
                    Result = response.Result
                };

                // Check if reached a terminal state
                var state = response.Status?.State;
                if (state == "SUCCEEDED" ||
                    state == "FAILED" ||
                    state == "CANCELED" ||
                    state == "CLOSED")
                {
                    return executeResponse;
                }

                // Continue polling for PENDING and RUNNING states
            }
        }

        /// <summary>
        /// Creates an appropriate reader based on the result disposition.
        /// </summary>
        private IArrowArrayStream CreateReader(ExecuteStatementResponse response, CancellationToken cancellationToken)
        {
            if (response.Manifest == null)
            {
                // No results - return empty reader
                return new EmptyArrowArrayStream();
            }

            // Check for external links in manifest chunks or result
            bool hasExternalLinksInChunks = response.Manifest.Chunks != null &&
                                  response.Manifest.Chunks.Count > 0 &&
                                  response.Manifest.Chunks[0].ExternalLinks != null &&
                                  response.Manifest.Chunks[0].ExternalLinks.Count > 0;
            bool hasExternalLinksInResult = response.Result != null &&
                                  response.Result.ExternalLinks != null &&
                                  response.Result.ExternalLinks.Count > 0;
            bool hasExternalLinks = hasExternalLinksInChunks || hasExternalLinksInResult;

            if (hasExternalLinks)
            {
                // TODO: Implement CloudFetch for external links
                throw new NotImplementedException("CloudFetch support for Statement Execution API is not yet implemented");
            }
            else if (response.Result != null && response.Result.Attachment != null && response.Result.Attachment.Length > 0)
            {
                // Inline results with Arrow stream format
                return new InlineArrowStreamReader(response.Result.Attachment);
            }
            else
            {
                // No inline data - return empty reader
                return new EmptyArrowArrayStream();
            }
        }

        /// <summary>
        /// Executes an update query and returns the number of affected rows.
        /// </summary>
        public override UpdateResult ExecuteUpdate()
        {
            return ExecuteUpdateAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes an update query asynchronously and returns the number of affected rows.
        /// </summary>
        public async Task<UpdateResult> ExecuteUpdateAsync(CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(_sqlQuery))
            {
                throw new InvalidOperationException("SQL query is required");
            }

            // Build the execute statement request
            var request = new ExecuteStatementRequest
            {
                Statement = _sqlQuery,
                WarehouseId = _warehouseId,
                SessionId = _sessionId,
                Catalog = _catalog,
                Schema = _schema,
                Disposition = _resultDisposition,
                Format = _resultFormat,
                ResultCompression = _resultCompression,
                WaitTimeout = $"{_waitTimeoutSeconds}s",
                OnWaitTimeout = "CONTINUE"
            };

            // Execute the statement
            var response = await _client.ExecuteStatementAsync(request, cancellationToken).ConfigureAwait(false);
            _currentStatementId = response.StatementId;

            // Handle query status - poll until complete
            var state = response.Status?.State;
            if (state == "PENDING" || state == "RUNNING")
            {
                response = await PollUntilCompleteAsync(response.StatementId, cancellationToken).ConfigureAwait(false);
                state = response.Status?.State;
            }

            // Check for terminal error states
            if (state == "FAILED")
            {
                var error = response.Status?.Error;
                throw new AdbcException($"Statement execution failed: {error?.Message ?? "Unknown error"} (Error Code: {error?.ErrorCode})");
            }
            if (state == "CANCELED")
            {
                throw new AdbcException("Statement execution was canceled");
            }
            if (state == "CLOSED")
            {
                throw new AdbcException("Statement was closed before results could be retrieved");
            }

            // For updates, we don't need to read the results - just return the row count
            long rowCount = response.Manifest?.TotalRowCount ?? 0;
            return new UpdateResult(rowCount);
        }

        /// <summary>
        /// Disposes the statement and cancels/closes any active statement.
        /// </summary>
        public override void Dispose()
        {
            if (_currentStatementId != null)
            {
                try
                {
                    // Close statement synchronously during dispose
                    Activity.Current?.AddEvent(new ActivityEvent("statement.dispose",
                        tags: new ActivityTagsCollection
                        {
                            { "statement_id", _currentStatementId }
                        }));
                    _client.CloseStatementAsync(_currentStatementId, CancellationToken.None).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    // Best effort - ignore errors during dispose
                    Activity.Current?.AddEvent(new ActivityEvent("statement.dispose.error",
                        tags: new ActivityTagsCollection
                        {
                            { "error", ex.Message }
                        }));
                }
                finally
                {
                    _currentStatementId = null;
                }
            }
        }

        /// <summary>
        /// Empty Arrow array stream for queries with no results.
        /// </summary>
        private class EmptyArrowArrayStream : IArrowArrayStream
        {
            public Schema Schema => new Schema.Builder().Build();

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                return new ValueTask<RecordBatch?>((RecordBatch?)null);
            }

            public void Dispose()
            {
                // Nothing to dispose
            }
        }

        /// <summary>
        /// Arrow array stream for inline results in Arrow IPC stream format.
        /// </summary>
        private class InlineArrowStreamReader : IArrowArrayStream
        {
            private readonly ArrowStreamReader _streamReader;
            private readonly System.IO.MemoryStream _memoryStream;
            private bool _disposed;

            public InlineArrowStreamReader(byte[] arrowData)
            {
                if (arrowData == null || arrowData.Length == 0)
                {
                    throw new ArgumentException("Arrow data cannot be null or empty", nameof(arrowData));
                }

                _memoryStream = new System.IO.MemoryStream(arrowData);
                _streamReader = new ArrowStreamReader(_memoryStream);
            }

            public Schema Schema => _streamReader.Schema;

            public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(InlineArrowStreamReader));
                }

                return await _streamReader.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
            }

            public void Dispose()
            {
                if (!_disposed)
                {
                    _streamReader?.Dispose();
                    _memoryStream?.Dispose();
                    _disposed = true;
                }
            }
        }
    }
}
