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

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchReader"/> class.
        /// Protocol-agnostic constructor.
        /// Works with both Thrift (IHiveServer2Statement) and REST (StatementExecutionStatement) protocols.
        /// </summary>
        /// <param name="statement">The tracing statement (both protocols implement ITracingStatement).</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="response">The query response (nullable for REST API, which doesn't use IResponse).</param>
        /// <param name="downloadManager">The download manager (already initialized and started).</param>
        public CloudFetchReader(
            ITracingStatement statement,
            Schema schema,
            IResponse? response,
            ICloudFetchDownloadManager downloadManager)
            : base(statement, schema, response, isLz4Compressed: false) // isLz4Compressed handled by download manager
        {
            this.downloadManager = downloadManager ?? throw new ArgumentNullException(nameof(downloadManager));
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
                    // If we have a current reader, try to read the next batch
                    if (this.currentReader != null)
                    {
                        RecordBatch? next = await this.currentReader.ReadNextRecordBatchAsync(cancellationToken);
                        if (next != null)
                        {
                            return next;
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

                            Activity.Current?.AddEvent("cloudfetch.reader_waiting_for_download", [
                                new("chunk_index", this.currentDownloadResult.ChunkIndex)
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
