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
using AdbcDrivers.Databricks.Telemetry.TagDefinitions;
using Apache.Arrow;
using System.Collections.Generic;
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

        // JDBC/Rust-style chunk map: indexed by chunk number, no queue serialization.
        // Used on .NET Framework for true parallel downloads.
        private JdbcStyleChunkMap? _chunkMap;
        private long _currentChunkIndex;

        // Pre-parsed batch iteration (JDBC parity): when download threads pre-parse
        // Arrow IPC, the reader iterates these directly instead of parsing on-the-fly.
        private IReadOnlyList<RecordBatch>? _preParsedBatches;
        private int _preParsedBatchIndex;

        // Row count limiting supports two modes:
        // 1. Global limiting (SEA/REST): Uses manifest.TotalRowCount for total expected rows
        // 2. Per-chunk limiting (Thrift): Uses TSparkArrowResultLink.RowCount per chunk
        // When trimArrowBatchesToLimit=false (server default), the server may return more data
        // than the limit in the last batch but reports adjusted rowCount in metadata.
        private readonly long _totalExpectedRows;
        private long _rowsRead;
        private long _currentChunkExpectedRows;
        private long _currentChunkRowsRead;

        // Telemetry tracking
        private long _totalBytesDownloaded = 0;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchReader"/> class.
        /// Protocol-agnostic constructor.
        /// Works with both Thrift (IHiveServer2Statement) and REST (StatementExecutionStatement) protocols.
        /// </summary>
        /// <param name="statement">The tracing statement (both protocols implement ITracingStatement).</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="response">The query response (nullable for REST API, which doesn't use IResponse).</param>
        /// <param name="downloadManager">The download manager (already initialized and started).</param>
        /// <param name="totalExpectedRows">Total expected rows for global limiting (SEA). Pass 0 to use per-chunk limiting (Thrift).</param>
        public CloudFetchReader(
            ITracingStatement statement,
            Schema schema,
            IResponse? response,
            ICloudFetchDownloadManager downloadManager,
            long totalExpectedRows = 0)
            : base(statement, schema, response, isLz4Compressed: false) // isLz4Compressed handled by download manager
        {
            this.downloadManager = downloadManager ?? throw new ArgumentNullException(nameof(downloadManager));
            if (totalExpectedRows < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(totalExpectedRows), totalExpectedRows, "Total expected rows cannot be negative.");
            }
            _totalExpectedRows = totalExpectedRows;
        }

        /// <summary>
        /// Sets the JDBC/Rust-style chunk map for indexed parallel downloads.
        /// When set, the reader uses WaitForChunk(index) instead of the queue-based download manager.
        /// </summary>
        internal void SetChunkMap(JdbcStyleChunkMap chunkMap)
        {
            _chunkMap = chunkMap ?? throw new ArgumentNullException(nameof(chunkMap));
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
                    // Check global row limit first (used by SEA with manifest.TotalRowCount)
                    if (_totalExpectedRows > 0 && _rowsRead >= _totalExpectedRows)
                    {
                        Activity.Current?.AddEvent("cloudfetch.global_row_limit_reached", [
                            new("total_expected_rows", _totalExpectedRows),
                            new("rows_read", _rowsRead)
                        ]);
                        CleanupCurrentReaderAndDownloadResult();
                        return null;
                    }

                    // Check per-chunk row limit (used by Thrift with TSparkArrowResultLink.RowCount)
                    if (_totalExpectedRows <= 0 && _currentChunkExpectedRows > 0 && _currentChunkRowsRead >= _currentChunkExpectedRows)
                    {
                        Activity.Current?.AddEvent("cloudfetch.chunk_row_limit_reached", [
                            new("chunk_expected_rows", _currentChunkExpectedRows),
                            new("chunk_rows_read", _currentChunkRowsRead)
                        ]);
                        // Move to next chunk
                        CleanupCurrentReaderAndDownloadResult();
                    }

                    // If we have pre-parsed batches (Arrow parsing done on download thread),
                    // iterate them directly — pure memory access, no IPC parsing on reader thread.
                    if (_preParsedBatches != null)
                    {
                        if (_preParsedBatchIndex < _preParsedBatches.Count)
                        {
                            RecordBatch? next = _preParsedBatches[_preParsedBatchIndex++];
                            next = ApplyRowCountLimit(next);
                            if (next != null)
                            {
                                return next;
                            }
                            continue;
                        }
                        else
                        {
                            // All pre-parsed batches consumed — clean up
                            _preParsedBatches = null;
                            _preParsedBatchIndex = 0;
                            CleanupCurrentReaderAndDownloadResult();
                        }
                    }

                    // Fallback: if we have a streaming reader, parse on-the-fly
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
                            CleanupCurrentReaderAndDownloadResult();
                        }
                    }

                    // JDBC/Rust-style: get next chunk by INDEX from the concurrent map.
                    // No queue serialization — chunks complete in any order, consumer reads in order.
                    if (_chunkMap != null)
                    {
                        var batches = _chunkMap.WaitForChunk(_currentChunkIndex, cancellationToken);
                        if (batches == null || batches.Count == 0)
                        {
                            _chunkMap.Dispose();
                            _chunkMap = null;
                            return null;
                        }
                        _preParsedBatches = batches;
                        _preParsedBatchIndex = 0;
                        _currentChunkIndex++;
                        continue;
                    }

                    // Queue-based path (used on .NET 8 with async download manager)
                    if (this.downloadManager != null)
                    {
                        try
                        {
                            // Get the next downloaded file
                            this.currentDownloadResult = await this.downloadManager.GetNextDownloadedFileAsync(cancellationToken);
                            if (this.currentDownloadResult == null)
                            {
                                Activity.Current?.AddEvent("cloudfetch.reader_no_more_files", [
                                    new("rows_read_so_far", _rowsRead),
                                    new("total_expected_rows", _totalExpectedRows),
                                    new("current_chunk_rows_read", _currentChunkRowsRead)
                                ]);
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
                                new("chunk_row_count", this.currentDownloadResult.RowCount)
                            ]);

                            await this.currentDownloadResult.DownloadCompletedTask;

                            // Track bytes downloaded for telemetry
                            _totalBytesDownloaded += this.currentDownloadResult.Size;

                            Activity.Current?.AddEvent("cloudfetch.reader_download_ready", [
                                new("chunk_index", this.currentDownloadResult.ChunkIndex),
                                new("chunk_bytes", this.currentDownloadResult.Size)
                            ]);

                            // Use pre-parsed batches if available (Arrow parsing done on download thread).
                            // This matches JDBC's architecture where download threads pre-parse all
                            // Arrow batches, so the reader just iterates — pure memory access.
                            try
                            {
                                var concreteResult = this.currentDownloadResult as DownloadResult;
                                if (concreteResult?.PreParsedBatches != null)
                                {
                                    this._preParsedBatches = concreteResult.PreParsedBatches;
                                    this._preParsedBatchIndex = 0;
                                    continue;
                                }

                                // Fallback: parse on reader thread (for non-DownloadResult implementations)
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
        /// Cleans up the current reader and download result, resetting chunk-level tracking.
        /// </summary>
        private void CleanupCurrentReaderAndDownloadResult()
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
            _currentChunkExpectedRows = 0;
            _currentChunkRowsRead = 0;
        }

        /// <summary>
        /// Applies row count limiting to a record batch.
        /// Supports two modes:
        /// - Global limiting (SEA): Uses _totalExpectedRows from manifest.TotalRowCount
        /// - Per-chunk limiting (Thrift): Uses _currentChunkExpectedRows from TSparkArrowResultLink.RowCount
        /// </summary>
        private RecordBatch? ApplyRowCountLimit(RecordBatch batch)
        {
            // Mode 1: Global row limiting (SEA with manifest.TotalRowCount)
            if (_totalExpectedRows > 0)
            {
                long remainingRows = _totalExpectedRows - _rowsRead;

                if (batch.Length <= remainingRows)
                {
                    _rowsRead += batch.Length;
                    return batch;
                }

                if (remainingRows <= 0)
                {
                    return null;
                }

                Activity.Current?.AddEvent("cloudfetch.trimming_batch_global", [
                    new("original_length", batch.Length),
                    new("trimmed_length", remainingRows),
                    new("total_expected_rows", _totalExpectedRows),
                    new("rows_read_before", _rowsRead)
                ]);

                // Slice uses reference counting - dispose original to release its reference
                var globalTrimmedBatch = batch.Slice(0, (int)remainingRows);
                batch.Dispose();
                _rowsRead += globalTrimmedBatch.Length;
                return globalTrimmedBatch;
            }

            // Mode 2: Per-chunk row limiting (Thrift with TSparkArrowResultLink.RowCount)
            // If no row limit tracking for this chunk (0 means no limit set, negative is invalid/defensive)
            if (_currentChunkExpectedRows <= 0)
            {
                _currentChunkRowsRead += batch.Length;
                return batch;
            }

            long chunkRemainingRows = _currentChunkExpectedRows - _currentChunkRowsRead;

            // If we can return the full batch without exceeding the limit
            if (batch.Length <= chunkRemainingRows)
            {
                _currentChunkRowsRead += batch.Length;
                return batch;
            }

            // We need to trim the batch - it contains more rows than we should return
            if (chunkRemainingRows <= 0)
            {
                // We've already read all expected rows for this chunk
                return null;
            }

            Activity.Current?.AddEvent("cloudfetch.trimming_batch_chunk", [
                new("original_length", batch.Length),
                new("trimmed_length", chunkRemainingRows),
                new("chunk_expected_rows", _currentChunkExpectedRows),
                new("chunk_rows_read_before", _currentChunkRowsRead)
            ]);

            // Slice uses reference counting - dispose original to release its reference
            var chunkTrimmedBatch = batch.Slice(0, (int)chunkRemainingRows);
            batch.Dispose();
            _currentChunkRowsRead += chunkTrimmedBatch.Length;

            return chunkTrimmedBatch;
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

            if (_chunkMap != null)
            {
                _chunkMap.Stop();
                _chunkMap.Dispose();
                _chunkMap = null;
            }

            if (this.downloadManager != null)
            {
                this.downloadManager.Dispose();
                this.downloadManager = null;
            }

            // Add telemetry tags when reader completes
            Activity.Current?.SetTag(StatementExecutionEvent.ResultBytesDownloaded, _totalBytesDownloaded);

            base.Dispose(disposing);
        }
    }
}
