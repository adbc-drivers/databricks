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

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace AdbcDrivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Reader for CloudFetch results.
    /// Protocol-agnostic - works with both Thrift and REST implementations.
    /// Handles downloading and processing URL-based result sets.
    ///
    /// Note: This reader receives an ITracingStatement for tracing support.
    /// Works with both Thrift (IHiveServer2Statement) and REST (StatementExecutionStatement) protocols.
    /// All CloudFetch logic is handled through the downloadManager.
    /// </summary>
    internal sealed class CloudFetchReader : BaseDatabricksReader
    {
        private ICloudFetchDownloadManager? downloadManager;
        private ArrowStreamReader? currentReader;
        private IDownloadResult? currentDownloadResult;

        // Row count limiting: tracks the total expected rows and cumulative rows read.
        // For REST (SEA): manifest.TotalRowCount is known upfront - use global limiting
        // For Thrift: rowCount is known per-chunk - use chunk-level limiting
        // When trimArrowBatchesToLimit=false (server default), the server may return more data
        // than the limit in the last batch but reports adjusted rowCount in metadata.
        private readonly long _totalExpectedRows;
        private long _rowsRead;
        private long _currentChunkExpectedRows;
        private long _currentChunkRowsRead;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchReader"/> class.
        /// Protocol-agnostic constructor.
        /// Works with both Thrift (IHiveServer2Statement) and REST (StatementExecutionStatement) protocols.
        /// </summary>
        /// <param name="statement">The tracing statement (both protocols implement ITracingStatement).</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="response">The query response (nullable for REST API, which doesn't use IResponse).</param>
        /// <param name="downloadManager">The download manager (already initialized and started).</param>
        /// <param name="totalExpectedRows">The total expected rows from metadata (0 to disable limiting).</param>
        public CloudFetchReader(
            ITracingStatement statement,
            Schema schema,
            IResponse? response,
            ICloudFetchDownloadManager downloadManager,
            long totalExpectedRows = 0)
            : base(statement, schema, response, isLz4Compressed: false) // isLz4Compressed handled by download manager
        {
            this.downloadManager = downloadManager ?? throw new ArgumentNullException(nameof(downloadManager));
            _totalExpectedRows = totalExpectedRows;
        }

        /// <summary>
        /// Reads the next record batch from the result set.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The next record batch, or null if there are no more batches.</returns>
        public override async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            return await this.TraceActivityAsync(async _ =>
            {
                ThrowIfDisposed();

                while (true)
                {
                    // Check if we've already read all expected rows (global row count limiting for REST API)
                    if (_totalExpectedRows > 0 && _rowsRead >= _totalExpectedRows)
                    {
                        Activity.Current?.AddEvent("cloudfetch.row_limit_reached", [
                            new("total_expected_rows", _totalExpectedRows),
                            new("rows_read", _rowsRead)
                        ]);
                        return null;
                    }

                    // Check if we've reached the chunk limit (per-chunk limiting for Thrift)
                    if (_totalExpectedRows == 0 && _currentChunkExpectedRows > 0 && _currentChunkRowsRead >= _currentChunkExpectedRows)
                    {
                        Activity.Current?.AddEvent("cloudfetch.chunk_row_limit_reached", [
                            new("chunk_expected_rows", _currentChunkExpectedRows),
                            new("chunk_rows_read", _currentChunkRowsRead)
                        ]);
                        // Move to next chunk
                        if (this.currentReader != null)
                        {
                            this.currentReader.Dispose();
                            this.currentReader = null;
                        }
                        if (this.currentDownloadResult != null)
                        {
                            this.currentDownloadResult.Dispose();
                            this.currentDownloadResult = null;
                        }
                        _currentChunkExpectedRows = 0;
                        _currentChunkRowsRead = 0;
                    }

                    // If we have a current reader, try to read the next batch
                    if (this.currentReader != null)
                    {
                        RecordBatch? next = await this.currentReader.ReadNextRecordBatchAsync(cancellationToken);
                        if (next != null)
                        {
                            // Apply row count limiting: trim the batch if it would exceed expected rows
                            next = ApplyRowCountLimit(next);
                            if (next != null)
                            {
                                return next;
                            }
                            // If next is null after limiting, we've reached the limit
                            continue;
                        }
                        else
                        {
                            // Clean up the current reader and download result
                            this.currentReader.Dispose();
                            this.currentReader = null;

                            if (this.currentDownloadResult != null)
                            {
                                this.currentDownloadResult.Dispose();
                                this.currentDownloadResult = null;
                            }

                            // Reset chunk-level tracking
                            _currentChunkExpectedRows = 0;
                            _currentChunkRowsRead = 0;
                        }
                    }

                    // If we don't have a current reader, get the next downloaded file
                    if (this.downloadManager != null)
                    {
                        try
                        {
                            // Get the next downloaded file
                            this.currentDownloadResult = await this.downloadManager.GetNextDownloadedFileAsync(cancellationToken);
                            if (this.currentDownloadResult == null)
                            {
                                Activity.Current?.AddEvent("cloudfetch.reader_no_more_files");
                                this.downloadManager.Dispose();
                                this.downloadManager = null;
                                // No more files
                                return null;
                            }

                            // Set up chunk-level row count tracking
                            _currentChunkExpectedRows = this.currentDownloadResult.RowCount;
                            _currentChunkRowsRead = 0;

                            Activity.Current?.AddEvent("cloudfetch.reader_waiting_for_download", [
                                new("chunk_index", this.currentDownloadResult.ChunkIndex),
                                new("chunk_row_count", this.currentDownloadResult.RowCount),
                                new("total_expected_rows", _totalExpectedRows)
                            ]);

                            await this.currentDownloadResult.DownloadCompletedTask;

                            Activity.Current?.AddEvent("cloudfetch.reader_download_ready", [
                                new("chunk_index", this.currentDownloadResult.ChunkIndex)
                            ]);

                            // Create a new reader for the downloaded file
                            try
                            {
                                this.currentReader = new ArrowStreamReader(this.currentDownloadResult.DataStream);
                                continue;
                            }
                            catch (Exception ex)
                            {
                                Activity.Current?.AddEvent("cloudfetch.arrow_reader_creation_error", [
                                    new("error_message", ex.Message),
                                    new("error_type", ex.GetType().Name)
                                ]);
                                this.currentDownloadResult.Dispose();
                                this.currentDownloadResult = null;
                                throw;
                            }
                        }
                        catch (Exception ex)
                        {
                            Activity.Current?.AddEvent("cloudfetch.get_next_file_error", [
                                new("error_message", ex.Message),
                                new("error_type", ex.GetType().Name)
                            ]);
                            throw;
                        }
                    }

                    // If we get here, there are no more files
                    return null;
                }
            });
        }

        /// <summary>
        /// Applies row count limiting to a record batch.
        /// Uses global limiting (totalExpectedRows) for REST API where the total is known upfront.
        /// Uses chunk-level limiting (currentChunkExpectedRows) for Thrift where we don't know total upfront.
        /// </summary>
        private RecordBatch? ApplyRowCountLimit(RecordBatch batch)
        {
            // Determine which limit to use: global (REST) or chunk-level (Thrift)
            long expectedRows = _totalExpectedRows > 0 ? _totalExpectedRows : _currentChunkExpectedRows;
            long rowsReadRef = _totalExpectedRows > 0 ? _rowsRead : _currentChunkRowsRead;

            // If no row limit tracking
            if (expectedRows <= 0)
            {
                _rowsRead += batch.Length;
                _currentChunkRowsRead += batch.Length;
                return batch;
            }

            long remainingRows = expectedRows - rowsReadRef;

            // If we can return the full batch without exceeding the limit
            if (batch.Length <= remainingRows)
            {
                _rowsRead += batch.Length;
                _currentChunkRowsRead += batch.Length;
                return batch;
            }

            // We need to trim the batch - it contains more rows than we should return
            if (remainingRows <= 0)
            {
                // We've already read all expected rows
                batch.Dispose();
                return null;
            }

            Activity.Current?.AddEvent("cloudfetch.trimming_batch", [
                new("original_length", batch.Length),
                new("trimmed_length", remainingRows),
                new("total_expected_rows", _totalExpectedRows),
                new("chunk_expected_rows", _currentChunkExpectedRows),
                new("rows_read_before", _rowsRead)
            ]);

            // Slice the batch to return only the remaining expected rows
            var trimmedBatch = batch.Slice(0, (int)remainingRows);
            _rowsRead += trimmedBatch.Length;
            _currentChunkRowsRead += trimmedBatch.Length;

            // Dispose the original batch after slicing
            batch.Dispose();

            return trimmedBatch;
        }

        protected override void Dispose(bool disposing)
        {
            if (this.currentReader != null)
            {
                this.currentReader.Dispose();
                this.currentReader = null;
            }

            if (this.currentDownloadResult != null)
            {
                this.currentDownloadResult.Dispose();
                this.currentDownloadResult = null;
            }

            if (this.downloadManager != null)
            {
                this.downloadManager.Dispose();
                this.downloadManager = null;
            }
            base.Dispose(disposing);
        }
    }
}
