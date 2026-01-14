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

namespace AdbcDrivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Base class for result fetchers that manages pipeline and fetching logic.
    /// Protocol-specific implementations should inherit from this class.
    /// </summary>
    internal abstract class CloudFetchResultFetcher : ICloudFetchResultFetcher
    {
        protected readonly ICloudFetchMemoryBufferManager _memoryManager;
        protected readonly BlockingCollection<IDownloadResult> _downloadQueue;
        protected volatile bool _hasMoreResults;
        protected volatile bool _isCompleted;
        private Task? _fetchTask;
        private CancellationTokenSource? _cancellationTokenSource;
        private Exception? _error;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchResultFetcher"/> class.
        /// </summary>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <param name="downloadQueue">The queue to add download tasks to.</param>
        protected CloudFetchResultFetcher(
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue)
        {
            _memoryManager = memoryManager ?? throw new ArgumentNullException(nameof(memoryManager));
            _downloadQueue = downloadQueue ?? throw new ArgumentNullException(nameof(downloadQueue));
            _hasMoreResults = true;
            _isCompleted = false;
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
            _fetchTask = FetchResultsAsync(_cancellationTokenSource.Token);

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
        /// Re-fetches URLs for chunks starting from the specified row offset.
        /// Used when URLs expire before download completes.
        /// </summary>
        /// <param name="startRowOffset">The starting row offset to fetch from.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A collection of download results with refreshed URLs.</returns>
        public abstract Task<IEnumerable<IDownloadResult>> RefreshUrlsAsync(long startRowOffset, CancellationToken cancellationToken);

        /// <inheritdoc />
        public abstract long GetTotalExpectedRows();

        /// <summary>
        /// Resets the fetcher state. Called at the beginning of StartAsync.
        /// Subclasses should override to reset protocol-specific state.
        /// </summary>
        protected abstract void ResetState();

        /// <summary>
        /// Checks if initial results are available from the protocol.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>True if initial results are available, false otherwise.</returns>
        protected abstract Task<bool> HasInitialResultsAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Processes initial results from the protocol.
        /// This method should add IDownloadResult objects to _downloadQueue.
        /// It should also update _hasMoreResults based on whether there are more results available.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        protected abstract void ProcessInitialResultsAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Fetches the next batch of results from the protocol.
        /// This method should add IDownloadResult objects to _downloadQueue.
        /// It should also update _hasMoreResults based on whether there are more results available.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        protected abstract Task FetchNextBatchAsync(CancellationToken cancellationToken);

        private async Task FetchResultsAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Process direct results first, if available
                if (await HasInitialResultsAsync(cancellationToken))
                {
                    // Yield execution so the download queue doesn't get blocked before downloader is started
                    await Task.Yield();
                    ProcessInitialResultsAsync(cancellationToken);
                }

                // Continue fetching as needed
                while (_hasMoreResults && !cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Fetch more results from the server
                        await FetchNextBatchAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        // Expected when cancellation is requested
                        break;
                    }
                    catch (Exception ex)
                    {
                        Activity.Current?.AddEvent("cloudfetch.fetch_results_error", [
                            new("error_message", ex.Message),
                            new("error_type", ex.GetType().Name)
                        ]);
                        _error = ex;
                        _hasMoreResults = false;
                        throw;
                    }
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
            }
            finally
            {
                // Always add the end of results guard to signal completion to the downloader.
                // Use Add with cancellation token to exit promptly when cancelled.
                try
                {
                    _downloadQueue.Add(EndOfResultsGuard.Instance, cancellationToken);
                    Activity.Current?.AddEvent("cloudfetch.end_of_results_guard_added");
                }
                catch (OperationCanceledException)
                {
                    Activity.Current?.AddEvent("cloudfetch.end_of_results_guard_cancelled");
                }
                catch (Exception ex)
                {
                    Activity.Current?.AddEvent("cloudfetch.end_of_results_guard_error", [
                        new("error_message", ex.Message),
                        new("error_type", ex.GetType().Name)
                    ]);
                }
                _isCompleted = true;
            }
        }
    }
}
