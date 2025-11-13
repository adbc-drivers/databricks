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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tracing;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Base class for result fetchers that extract common pipeline management logic.
    /// Subclasses implement protocol-specific fetching logic (Thrift, REST, etc.).
    /// </summary>
    internal abstract class BaseResultFetcher : ICloudFetchResultFetcher
    {
        protected BlockingCollection<IDownloadResult>? _downloadQueue;
        protected ICloudFetchMemoryBufferManager? _memoryManager;
        protected volatile bool _hasMoreResults;
        protected volatile bool _isCompleted;
        protected Exception? _error;
        private Task? _fetchTask;
        private CancellationTokenSource? _cancellationTokenSource;

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseResultFetcher"/> class.
        /// </summary>
        /// <param name="memoryManager">The memory buffer manager (can be null, will be initialized later).</param>
        /// <param name="downloadQueue">The queue to add download tasks to (can be null, will be initialized later).</param>
        protected BaseResultFetcher(
            ICloudFetchMemoryBufferManager? memoryManager,
            BlockingCollection<IDownloadResult>? downloadQueue)
        {
            _memoryManager = memoryManager;
            _downloadQueue = downloadQueue;
            _hasMoreResults = true;
            _isCompleted = false;
        }

        /// <inheritdoc />
        public virtual void Initialize(
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue)
        {
            _memoryManager = memoryManager ?? throw new ArgumentNullException(nameof(memoryManager));
            _downloadQueue = downloadQueue ?? throw new ArgumentNullException(nameof(downloadQueue));
        }

        /// <inheritdoc />
        public bool HasMoreResults => _hasMoreResults;

        /// <inheritdoc />
        public bool IsCompleted => _isCompleted;

        /// <inheritdoc />
        public bool HasError => _error != null;

        /// <inheritdoc />
        public Exception? Error => _error;

        /// <inheritdoc />
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (_fetchTask != null)
            {
                throw new InvalidOperationException("Fetcher is already running.");
            }

            // Reset state
            _hasMoreResults = true;
            _isCompleted = false;
            _error = null;
            ResetState();

            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _fetchTask = FetchResultsWrapperAsync(_cancellationTokenSource.Token);

            await Task.Yield();
        }

        /// <inheritdoc />
        public async Task StopAsync()
        {
            if (_fetchTask == null)
            {
                return;
            }

            _cancellationTokenSource?.Cancel();

            try
            {
                await _fetchTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation is requested
            }
            catch (Exception ex)
            {
                Activity.Current?.AddEvent("cloudfetch.fetcher_stop_error", [
                    new("error_message", ex.Message)
                ]);
            }
            finally
            {
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
                _fetchTask = null;
            }
        }

        /// <summary>
        /// Gets a download result for the specified offset, fetching or refreshing as needed.
        /// </summary>
        /// <param name="offset">The row offset for which to get a download result.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The download result for the specified offset, or null if not available.</returns>
        public abstract Task<IDownloadResult?> GetDownloadResultAsync(long offset, CancellationToken cancellationToken);

        /// <summary>
        /// Re-fetches URLs for chunks in the specified range.
        /// Used when URLs expire before download completes.
        /// </summary>
        /// <param name="startRowOffset">The starting row offset to fetch from (for Thrift protocol).</param>
        /// <param name="startChunkIndex">The starting chunk index (inclusive, for REST protocol).</param>
        /// <param name="endChunkIndex">The ending chunk index (inclusive, for REST protocol).</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A collection of download results with refreshed URLs.</returns>
        public abstract Task<IEnumerable<IDownloadResult>> RefreshUrlsAsync(long startRowOffset, long startChunkIndex, long endChunkIndex, CancellationToken cancellationToken);

        /// <summary>
        /// Resets the fetcher state. Called at the beginning of StartAsync.
        /// Subclasses can override to reset protocol-specific state.
        /// </summary>
        protected virtual void ResetState()
        {
            // Base implementation does nothing. Subclasses can override.
        }

        /// <summary>
        /// Protocol-specific logic to fetch all results and populate the download queue.
        /// This method must add IDownloadResult objects to _downloadQueue using AddDownloadResult().
        /// It should also set _hasMoreResults appropriately and throw exceptions on error.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        protected abstract Task FetchAllResultsAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Helper method for subclasses to add download results to the queue.
        /// </summary>
        /// <param name="result">The download result to add.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        protected void AddDownloadResult(IDownloadResult result, CancellationToken cancellationToken)
        {
            if (_downloadQueue == null)
                throw new InvalidOperationException("Fetcher not initialized. Call Initialize() first.");

            _downloadQueue.Add(result, cancellationToken);
        }

        private async Task FetchResultsWrapperAsync(CancellationToken cancellationToken)
        {
            try
            {
                await FetchAllResultsAsync(cancellationToken).ConfigureAwait(false);

                // Add the end of results guard to the queue
                if (_downloadQueue == null)
                    throw new InvalidOperationException("Fetcher not initialized. Call Initialize() first.");

                _downloadQueue.Add(EndOfResultsGuard.Instance, cancellationToken);
                _isCompleted = true;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected when cancellation is requested
                _isCompleted = true;

                // Add the end of results guard to the queue
                try
                {
                    _downloadQueue.TryAdd(EndOfResultsGuard.Instance, 0);
                }
                catch (Exception)
                {
                    // Ignore any errors when adding the guard
                }
            }
            catch (Exception ex)
            {
                Activity.Current?.AddEvent("cloudfetch.fetcher_unhandled_error", [
                    new("error_message", ex.Message),
                    new("error_type", ex.GetType().Name)
                ]);
                _error = ex;
                _hasMoreResults = false;
                _isCompleted = true;

                // Add the end of results guard to the queue even in case of error
                try
                {
                    _downloadQueue.TryAdd(EndOfResultsGuard.Instance, 0);
                }
                catch (Exception)
                {
                    // Ignore any errors when adding the guard in case of error
                }
            }
        }
    }
}
