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
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch.StragglerDownload;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tracing;
using Microsoft.IO;

namespace AdbcDrivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Downloads files from URLs.
    /// Uses dependency injection to receive IActivityTracer for tracing support.
    /// </summary>
    internal sealed class CloudFetchDownloader : ICloudFetchDownloader
    {
        private readonly IActivityTracer _activityTracer;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly BlockingCollection<IDownloadResult> _resultQueue;
        private readonly ICloudFetchMemoryBufferManager _memoryManager;
        private readonly HttpClient _httpClient;
        private readonly ICloudFetchResultFetcher _resultFetcher;
        private readonly int _maxParallelDownloads;
        private readonly bool _isLz4Compressed;
        private readonly int _maxRetries;
        private readonly int _retryDelayMs;
        private readonly int _maxUrlRefreshAttempts;
        private readonly int _urlExpirationBufferSeconds;
        private readonly SemaphoreSlim _downloadSemaphore;
        private readonly RecyclableMemoryStreamManager? _memoryStreamManager;
        private readonly ArrayPool<byte>? _lz4BufferPool;
        private Task? _downloadTask;
        private CancellationTokenSource? _cancellationTokenSource;
        private bool _isCompleted;
        private Exception? _error;
        private readonly object _errorLock = new object();
        private readonly StragglerDownloadDetector? _stragglerDetector;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchDownloader"/> class.
        /// </summary>
        /// <param name="activityTracer">The activity tracer for tracing support (dependency injection).</param>
        /// <param name="downloadQueue">The queue of downloads to process.</param>
        /// <param name="resultQueue">The queue to add completed downloads to.</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <param name="httpClient">The HTTP client to use for downloads.</param>
        /// <param name="resultFetcher">The result fetcher that manages URLs.</param>
        /// <param name="config">The CloudFetch configuration.</param>
        public CloudFetchDownloader(
            IActivityTracer activityTracer,
            BlockingCollection<IDownloadResult> downloadQueue,
            BlockingCollection<IDownloadResult> resultQueue,
            ICloudFetchMemoryBufferManager memoryManager,
            HttpClient httpClient,
            ICloudFetchResultFetcher resultFetcher,
            CloudFetchConfiguration config)
        {
            _activityTracer = activityTracer ?? throw new ArgumentNullException(nameof(activityTracer));
            _downloadQueue = downloadQueue ?? throw new ArgumentNullException(nameof(downloadQueue));
            _resultQueue = resultQueue ?? throw new ArgumentNullException(nameof(resultQueue));
            _memoryManager = memoryManager ?? throw new ArgumentNullException(nameof(memoryManager));
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _resultFetcher = resultFetcher ?? throw new ArgumentNullException(nameof(resultFetcher));

            if (config == null) throw new ArgumentNullException(nameof(config));

            // Apply CloudFetch timeout configuration to HttpClient
            _httpClient.Timeout = TimeSpan.FromMinutes(config.TimeoutMinutes);

            _maxParallelDownloads = config.ParallelDownloads;
            _isLz4Compressed = config.IsLz4Compressed;
            _maxRetries = config.MaxRetries;
            _retryDelayMs = config.RetryDelayMs;
            _maxUrlRefreshAttempts = config.MaxUrlRefreshAttempts;
            _urlExpirationBufferSeconds = config.UrlExpirationBufferSeconds;
            _memoryStreamManager = config.MemoryStreamManager;
            _lz4BufferPool = config.Lz4BufferPool;
            _downloadSemaphore = new SemaphoreSlim(_maxParallelDownloads, _maxParallelDownloads);
            _isCompleted = false;

            // Initialize straggler mitigation from config object
            var stragglerConfig = config.StragglerMitigationConfig ?? CloudFetchStragglerMitigationConfig.Disabled;
            if (stragglerConfig.Enabled)
            {
                _stragglerDetector = new StragglerDownloadDetector(stragglerConfig, _activityTracer);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchDownloader"/> class for testing.
        /// </summary>
        /// <param name="activityTracer">The activity tracer for tracing support (dependency injection).</param>
        /// <param name="downloadQueue">The queue of downloads to process.</param>
        /// <param name="resultQueue">The queue to add completed downloads to.</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <param name="httpClient">The HTTP client to use for downloads.</param>
        /// <param name="resultFetcher">The result fetcher that manages URLs.</param>
        /// <param name="maxParallelDownloads">Maximum parallel downloads.</param>
        /// <param name="isLz4Compressed">Whether results are LZ4 compressed.</param>
        /// <param name="maxRetries">Maximum retry attempts (optional, default 3).</param>
        /// <param name="retryDelayMs">Delay between retries in ms (optional, default 1000).</param>
        /// <param name="stragglerConfig">Optional configuration for straggler mitigation (null = disabled).</param>
        internal CloudFetchDownloader(
            IActivityTracer activityTracer,
            BlockingCollection<IDownloadResult> downloadQueue,
            BlockingCollection<IDownloadResult> resultQueue,
            ICloudFetchMemoryBufferManager memoryManager,
            HttpClient httpClient,
            ICloudFetchResultFetcher resultFetcher,
            int maxParallelDownloads,
            bool isLz4Compressed,
            int maxRetries = 3,
            int retryDelayMs = 1000,
            CloudFetchStragglerMitigationConfig? stragglerConfig = null)
        {
            _activityTracer = activityTracer ?? throw new ArgumentNullException(nameof(activityTracer));
            _downloadQueue = downloadQueue ?? throw new ArgumentNullException(nameof(downloadQueue));
            _resultQueue = resultQueue ?? throw new ArgumentNullException(nameof(resultQueue));
            _memoryManager = memoryManager ?? throw new ArgumentNullException(nameof(memoryManager));
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _resultFetcher = resultFetcher ?? throw new ArgumentNullException(nameof(resultFetcher));

            _maxParallelDownloads = maxParallelDownloads;
            _isLz4Compressed = isLz4Compressed;
            _maxRetries = maxRetries;
            _retryDelayMs = retryDelayMs;
            _maxUrlRefreshAttempts = CloudFetchConfiguration.DefaultMaxUrlRefreshAttempts;
            _urlExpirationBufferSeconds = CloudFetchConfiguration.DefaultUrlExpirationBufferSeconds;
            _memoryStreamManager = null;
            _lz4BufferPool = null;
            _downloadSemaphore = new SemaphoreSlim(_maxParallelDownloads, _maxParallelDownloads);
            _isCompleted = false;

            // Initialize straggler mitigation from config object
            var config = stragglerConfig ?? CloudFetchStragglerMitigationConfig.Disabled;
            if (config.Enabled)
            {
                _stragglerDetector = new StragglerDownloadDetector(config, _activityTracer);
            }
        }

        /// <inheritdoc />
        public bool IsCompleted => _isCompleted;

        /// <inheritdoc />
        public bool HasError => _error != null;

        /// <inheritdoc />
        public Exception? Error => _error;

        /// <inheritdoc />
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (_downloadTask != null)
            {
                throw new InvalidOperationException("Downloader is already running.");
            }

            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _downloadTask = DownloadFilesAsync(_cancellationTokenSource.Token);
            _stragglerDetector?.StartMonitoring(cancellationToken);

            await Task.Yield();
        }

        /// <inheritdoc />
        public async Task StopAsync()
        {
            if (_downloadTask == null)
            {
                return;
            }

            _cancellationTokenSource?.Cancel();

            if (_stragglerDetector != null)
            {
                await _stragglerDetector.StopMonitoring().ConfigureAwait(false);
            }

            try
            {
                await _downloadTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation is requested
            }
            catch (Exception ex)
            {
                Activity.Current?.AddEvent("cloudfetch.downloader_stop_error", [
                    new("error_message", ex.Message),
                    new("error_type", ex.GetType().Name)
                ]);
            }
            finally
            {
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
                _downloadTask = null;
            }
        }

        /// <inheritdoc />
        public async Task<IDownloadResult?> GetNextDownloadedFileAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Check if there's an error before trying to take from the queue
                if (HasError)
                {
                    throw new AdbcException("Error in download process", _error ?? new Exception("Unknown error"));
                }

                // Try to take the next result from the queue
                IDownloadResult result = await Task.Run(() => _resultQueue.Take(cancellationToken), cancellationToken);

                Activity.Current?.AddEvent("cloudfetch.result_dequeued", [
                    new("chunk_index", result?.ChunkIndex ?? -1),
                    new("is_end_guard", result == EndOfResultsGuard.Instance)
                ]);

                // Check if this is the end of results guard
                if (result == EndOfResultsGuard.Instance)
                {
                    _isCompleted = true;
                    return null;
                }

                return result;
            }
            catch (OperationCanceledException)
            {
                // Cancellation was requested
                return null;
            }
            catch (InvalidOperationException) when (_resultQueue.IsCompleted)
            {
                // Queue is completed and empty
                _isCompleted = true;
                return null;
            }
            catch (AdbcException)
            {
                // Re-throw AdbcExceptions (these are our own errors)
                throw;
            }
            catch (Exception ex)
            {
                // If there's an error, set the error state and propagate it
                SetError(ex);
                throw;
            }
        }

        private async Task DownloadFilesAsync(CancellationToken cancellationToken)
        {
            await _activityTracer.TraceActivityAsync(async activity =>
            {
                await Task.Yield();

                int totalFiles = 0;
                int successfulDownloads = 0;
                int failedDownloads = 0;
                long totalBytes = 0;
                var overallStopwatch = Stopwatch.StartNew();

                try
                {
                    // Keep track of active download tasks
                    var downloadTasks = new ConcurrentDictionary<Task, IDownloadResult>();
                    var downloadTaskCompletionSource = new TaskCompletionSource<bool>();

                    // Process items from the download queue until it's completed
                    activity?.AddEvent("cloudfetch.download_loop_start", [
                        new("download_queue_count", _downloadQueue.Count)
                    ]);

                    foreach (var downloadResult in _downloadQueue.GetConsumingEnumerable(cancellationToken))
                    {
                        activity?.AddEvent("cloudfetch.download_item_dequeued", [
                            new("chunk_index", downloadResult?.ChunkIndex ?? -1),
                            new("is_end_guard", downloadResult == EndOfResultsGuard.Instance)
                        ]);

                        // Check if there's an error before processing more downloads
                        if (HasError)
                        {
                            activity?.AddEvent("cloudfetch.download_loop_error_break");
                            // Add the failed download result to the queue to signal the error
                            // This will be caught by GetNextDownloadedFileAsync
                            break;
                        }

                        // Check if this is the end of results guard
                        if (downloadResult == EndOfResultsGuard.Instance)
                        {
                            activity?.AddEvent("cloudfetch.end_of_results_guard_received");
                            // Wait for all active downloads to complete
                            if (downloadTasks.Count > 0)
                            {
                                try
                                {
                                    await Task.WhenAll(downloadTasks.Keys).ConfigureAwait(false);
                                }
                                catch (Exception ex)
                                {
                                    activity?.AddException(ex, [new("error.context", "cloudfetch.wait_for_downloads")]);
                                    // Don't set error here, as individual download tasks will handle their own errors
                                }
                            }

                            // Only add the guard if there's no error
                            if (!HasError)
                            {
                                // Add the guard to the result queue to signal the end of results
                                _resultQueue.Add(EndOfResultsGuard.Instance, cancellationToken);
                                _isCompleted = true;
                            }
                            break;
                        }

                        // This is a real file, count it
                        totalFiles++;

                        // Check if the URL is expired or about to expire
                        if (downloadResult.IsExpiredOrExpiringSoon(_urlExpirationBufferSeconds))
                        {
                            activity?.AddEvent("cloudfetch.url_expired_refreshing", [
                                new("chunk_index", downloadResult.ChunkIndex),
                                new("start_row_offset", downloadResult.StartRowOffset)
                            ]);
                            // Get refreshed URLs starting from this offset
                            var refreshedResults = await _resultFetcher.RefreshUrlsAsync(downloadResult.StartRowOffset, cancellationToken);
                            var refreshedResult = refreshedResults.FirstOrDefault(r => r.StartRowOffset == downloadResult.StartRowOffset);
                            if (refreshedResult != null)
                            {
                                // Update the download result with the refreshed URL
                                downloadResult.UpdateWithRefreshedUrl(refreshedResult.FileUrl, refreshedResult.ExpirationTime, refreshedResult.HttpHeaders);
                                activity?.AddEvent("cloudfetch.url_refreshed_before_download", [
                                    new("offset", refreshedResult.StartRowOffset)
                                ]);
                            }
                        }

                        // Acquire a download slot
                        await _downloadSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                        // Acquire memory for this download (FIFO - acquired in sequential loop)
                        long size = downloadResult.Size;
                        await _memoryManager.AcquireMemoryAsync(size, cancellationToken).ConfigureAwait(false);

                        // Acquire sequential permit if in fallback mode (serialization)
                        SequentialDownloadPermit? sequentialPermit = null;
                        if (_stragglerDetector != null)
                        {
                            sequentialPermit = await _stragglerDetector.AcquireSequentialPermitIfNeeded(cancellationToken, activity)
                                .ConfigureAwait(false);
                        }

                        activity?.AddEvent("cloudfetch.download_slot_acquired", [
                            new("chunk_index", downloadResult.ChunkIndex),
                            new("is_sequential_mode", sequentialPermit != null && sequentialPermit != SequentialDownloadPermit.NoOp)
                        ]);

                        Task downloadTask;
                        try
                        {
                            // Start the download task
                            downloadTask = DownloadFileAsync(downloadResult, cancellationToken)
                                .ContinueWith(t =>
                                {
                                    // Release in reverse order (LIFO)
                                    sequentialPermit?.Dispose();
                                    _downloadSemaphore.Release();

                                // Remove the task from the dictionary
                                downloadTasks.TryRemove(t, out _);

                                // Handle any exceptions
                                if (t.IsFaulted)
                                {
                                    Exception ex = t.Exception?.InnerException ?? new Exception("Unknown error");
                                    string sanitizedUrl = SanitizeUrl(downloadResult.FileUrl);
                                    activity?.AddException(ex, [
                                        new("error.context", "cloudfetch.download_failed"),
                                        new("offset", downloadResult.StartRowOffset),
                                        new("sanitized_url", sanitizedUrl)
                                    ]);

                                    // Set the download as failed
                                    downloadResult.SetFailed(ex);
                                    failedDownloads++;

                                    // Set the error state to stop the download process
                                    SetError(ex, activity);

                                    // Signal that we should stop processing downloads
                                    downloadTaskCompletionSource.TrySetException(ex);
                                }
                                else if (!t.IsFaulted && !t.IsCanceled)
                                {
                                    successfulDownloads++;
                                    totalBytes += downloadResult.Size;
                                }
                            }, cancellationToken);

                            // Add the task to the dictionary
                            downloadTasks[downloadTask] = downloadResult;
                        }
                        catch (Exception ex)
                        {
                            // If task creation fails, release all acquired resources to prevent leak
                            sequentialPermit?.Dispose();
                            _downloadSemaphore.Release();
                            _memoryManager.ReleaseMemory(size);
                            SetError(ex, activity);
                            break;
                        }

                        // Add the result to the result queue add the result here to assure the download sequence.
                        _resultQueue.Add(downloadResult, cancellationToken);

                        activity?.AddEvent("cloudfetch.result_enqueued", [
                            new("chunk_index", downloadResult.ChunkIndex),
                            new("result_queue_count", _resultQueue.Count)
                        ]);

                        // If there's an error, stop processing more downloads
                        if (HasError)
                        {
                            break;
                        }
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // Expected when cancellation is requested
                    activity?.AddEvent("cloudfetch.download_cancelled");
                }
                catch (Exception ex)
                {
                    activity?.AddException(ex, [new("error.context", "cloudfetch.download_loop")]);
                    SetError(ex, activity);
                }
                finally
                {
                    overallStopwatch.Stop();

                    activity?.AddEvent("cloudfetch.download_summary", [
                        new("total_files", totalFiles),
                        new("successful_downloads", successfulDownloads),
                        new("failed_downloads", failedDownloads),
                        new("total_bytes", totalBytes),
                        new("total_mb", totalBytes / 1024.0 / 1024.0),
                        new("total_time_ms", overallStopwatch.ElapsedMilliseconds),
                        new("total_time_sec", overallStopwatch.ElapsedMilliseconds / 1000.0)
                    ]);

                    // If there's an error, add the error to the result queue
                    if (HasError)
                    {
                        CompleteWithError(activity);
                    }
                }
            });
        }

        private async Task DownloadFileAsync(IDownloadResult downloadResult, CancellationToken cancellationToken)
        {
            await _activityTracer.TraceActivityAsync(async activity =>
            {
                string url = downloadResult.FileUrl;
                string sanitizedUrl = SanitizeUrl(downloadResult.FileUrl);
                byte[]? fileData = null;

                // Use the size directly from the download result
                long size = downloadResult.Size;

                // Add tags to the Activity for filtering/searching
                activity?.SetTag("cloudfetch.offset", downloadResult.StartRowOffset);
                activity?.SetTag("cloudfetch.sanitized_url", sanitizedUrl);
                activity?.SetTag("cloudfetch.expected_size_bytes", size);

                // Create a stopwatch to track download time
                var stopwatch = Stopwatch.StartNew();

                // Log download start
                activity?.AddEvent("cloudfetch.download_start", [
                new("offset", downloadResult.StartRowOffset),
                    new("sanitized_url", sanitizedUrl),
                    new("expected_size_bytes", size),
                    new("expected_size_kb", size / 1024.0)
            ]);

                long fileOffset = downloadResult.StartRowOffset;
                CancellationToken effectiveToken = _stragglerDetector?.RegisterDownload(fileOffset, size, cancellationToken, activity)
                                                   ?? cancellationToken;

                // Retry logic for downloading files
                for (int retry = 0; retry < _maxRetries; retry++)
                {
                    try
                    {
                        // Create HTTP request with optional custom headers
                        using var request = new HttpRequestMessage(HttpMethod.Get, url);

                        // Add custom headers if provided
                        if (downloadResult.HttpHeaders != null)
                        {
                            foreach (var header in downloadResult.HttpHeaders)
                            {
                                request.Headers.TryAddWithoutValidation(header.Key, header.Value);
                            }
                        }

                        // Download the file directly
                        using HttpResponseMessage response = await _httpClient.SendAsync(
                            request,
                            HttpCompletionOption.ResponseHeadersRead,
                            effectiveToken).ConfigureAwait(false);

                        // Check if the response indicates an expired URL (typically 403 or 401)
                        if (response.StatusCode == System.Net.HttpStatusCode.Forbidden ||
                            response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                        {
                            // If we've already tried refreshing too many times, fail
                            if (downloadResult.RefreshAttempts >= _maxUrlRefreshAttempts)
                            {
                                throw new InvalidOperationException($"Failed to download file after {downloadResult.RefreshAttempts} URL refresh attempts.");
                            }

                            // Try to refresh the URL
                            var refreshedResults = await _resultFetcher.RefreshUrlsAsync(downloadResult.StartRowOffset, cancellationToken);
                            var refreshedResult = refreshedResults.FirstOrDefault(r => r.StartRowOffset == downloadResult.StartRowOffset);
                            if (refreshedResult != null)
                            {
                                // Update the download result with the refreshed URL
                                downloadResult.UpdateWithRefreshedUrl(refreshedResult.FileUrl, refreshedResult.ExpirationTime, refreshedResult.HttpHeaders);
                                url = refreshedResult.FileUrl;
                                sanitizedUrl = SanitizeUrl(url);

                                activity?.AddEvent("cloudfetch.url_refreshed_after_auth_error", [
                                    new("offset", refreshedResult.StartRowOffset),
                                    new("sanitized_url", sanitizedUrl)
                                ]);

                                // Continue to the next retry attempt with the refreshed URL
                                continue;
                            }
                            else
                            {
                                // If refresh failed, throw an exception
                                throw new InvalidOperationException("Failed to refresh expired URL.");
                            }
                        }

                        response.EnsureSuccessStatusCode();

                        // Log the download size if available from response headers
                        long? contentLength = response.Content.Headers.ContentLength;
                        if (contentLength.HasValue && contentLength.Value > 0)
                        {
                            activity?.AddEvent("cloudfetch.content_length", [
                                new("offset", downloadResult.StartRowOffset),
                                new("sanitized_url", sanitizedUrl),
                                new("content_length_bytes", contentLength.Value),
                                new("content_length_mb", contentLength.Value / 1024.0 / 1024.0)
                            ]);
                        }

                        // When straggler detection is enabled, we need cancellable body reads.
                        // HttpCompletionOption.ResponseHeadersRead means SendAsync returns after headers only —
                        // the body (10-100MB+) is downloaded below. Without passing effectiveToken here,
                        // straggler cancellation via cts.Cancel() has no effect during the body transfer.
                        //
                        // This is gated behind straggler detection to avoid changing the existing download
                        // behavior when the feature is disabled.
                        if (_stragglerDetector != null)
                        {
#if NET5_0_OR_GREATER
                            fileData = await response.Content.ReadAsByteArrayAsync(effectiveToken).ConfigureAwait(false);
#else
                            // net472/netstandard2.0: ReadAsByteArrayAsync(CancellationToken) does not exist.
                            // CopyToAsync passes the token to the underlying socket ReadAsync on each 81920-byte
                            // chunk (Stream.DefaultCopyBufferSize), matching ReadAsByteArrayAsync's internal behavior.
                            using (var contentStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                            {
                                int capacity = contentLength.HasValue && contentLength.Value > 0
                                    ? (int)Math.Min(contentLength.Value, int.MaxValue)
                                    : 0;
                                var memoryStream = new MemoryStream(capacity);
                                await contentStream.CopyToAsync(memoryStream, 81920, effectiveToken).ConfigureAwait(false);
                                fileData = memoryStream.ToArray();
                            }
#endif
                        }
                        else
                        {
                            fileData = await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false);
                        }
                        break; // Success, exit retry loop
                    }
                    catch (OperationCanceledException) when (
                        effectiveToken.IsCancellationRequested
                        && !cancellationToken.IsCancellationRequested)
                    {
                        if (fileData != null)
                        {
                            activity?.AddEvent("cloudfetch.straggler_cancelled_but_completed", [
                                new("offset", downloadResult.StartRowOffset),
                                new("sanitized_url", sanitizedUrl),
                                new("file_size_mb", size / 1024.0 / 1024.0)
                            ]);
                            break;
                        }

                        if (retry < _maxRetries - 1)
                        {
                            activity?.AddEvent("cloudfetch.straggler_cancelled", [
                                new("offset", downloadResult.StartRowOffset),
                                new("sanitized_url", sanitizedUrl),
                                new("file_size_mb", size / 1024.0 / 1024.0),
                                new("elapsed_seconds", stopwatch.ElapsedMilliseconds / 1000.0),
                                new("attempt", retry + 1),
                                new("max_retries", _maxRetries)
                            ]);

                            _stragglerDetector?.MarkCancelledAsStragler(fileOffset, activity);
                            effectiveToken = _stragglerDetector?.RegisterDownload(fileOffset, size, cancellationToken, activity)
                                           ?? cancellationToken;

                            await Task.Delay(_retryDelayMs * (retry + 1), cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            activity?.AddEvent("cloudfetch.straggler_cancelled_last_retry", [
                                new("offset", downloadResult.StartRowOffset),
                                new("sanitized_url", sanitizedUrl)
                            ]);
                            break;
                        }
                    }
                    catch (Exception ex) when (retry < _maxRetries - 1 && !cancellationToken.IsCancellationRequested)
                    {
                        activity?.AddException(ex, [
                            new("error.context", "cloudfetch.download_retry"),
                            new("offset", downloadResult.StartRowOffset),
                            new("sanitized_url", SanitizeUrl(url)),
                            new("attempt", retry + 1),
                            new("max_retries", _maxRetries)
                        ]);

                        await Task.Delay(_retryDelayMs * (retry + 1), cancellationToken).ConfigureAwait(false);
                    }
                }

                if (fileData == null)
                {
                    stopwatch.Stop();
                    activity?.AddEvent("cloudfetch.download_failed_all_retries", [
                        new("offset", downloadResult.StartRowOffset),
                        new("sanitized_url", sanitizedUrl),
                        new("max_retries", _maxRetries),
                        new("elapsed_time_ms", stopwatch.ElapsedMilliseconds)
                    ]);

                    // Release the memory we acquired
                    _memoryManager.ReleaseMemory(size);
                    throw new InvalidOperationException($"Failed to download file from {url} after {_maxRetries} attempts.");
                }

                // Process the downloaded file data
                Stream dataStream;
                long actualSize = fileData.Length;

                // If the data is LZ4 compressed, decompress it
                if (_isLz4Compressed)
                {
                    try
                    {
                        var decompressStopwatch = Stopwatch.StartNew();

                        // Use shared Lz4Utilities for decompression with both RecyclableMemoryStream and ArrayPool
                        // The returned stream must be disposed by Arrow after reading
                        // Protocol-agnostic: use resources from config (populated by caller from connection)
                        // Falls back to creating new instances if not provided (less efficient but works)
                        var memoryStreamManager = _memoryStreamManager ?? new RecyclableMemoryStreamManager();
                        var lz4BufferPool = _lz4BufferPool ?? ArrayPool<byte>.Shared;
                        dataStream = await Lz4Utilities.DecompressLz4Async(
                            fileData,
                            memoryStreamManager,
                            lz4BufferPool,
                            cancellationToken).ConfigureAwait(false);

                        decompressStopwatch.Stop();

                        // Calculate throughput metrics
                        double compressionRatio = (double)dataStream.Length / actualSize;

                        activity?.AddEvent("cloudfetch.decompression_complete", [
                            new("offset", downloadResult.StartRowOffset),
                            new("sanitized_url", sanitizedUrl),
                            new("decompression_time_ms", decompressStopwatch.ElapsedMilliseconds),
                            new("compressed_size_bytes", actualSize),
                            new("compressed_size_kb", actualSize / 1024.0),
                            new("decompressed_size_bytes", dataStream.Length),
                            new("decompressed_size_kb", dataStream.Length / 1024.0),
                            new("compression_ratio", compressionRatio)
                        ]);

                        actualSize = dataStream.Length;
                    }
                    catch (Exception ex)
                    {
                        stopwatch.Stop();
                        activity?.AddException(ex, [
                            new("error.context", "cloudfetch.decompression"),
                            new("offset", downloadResult.StartRowOffset),
                            new("sanitized_url", sanitizedUrl),
                            new("elapsed_time_ms", stopwatch.ElapsedMilliseconds)
                        ]);

                        // Release the memory we acquired
                        _memoryManager.ReleaseMemory(size);
                        throw new InvalidOperationException($"Error decompressing data: {ex.Message}", ex);
                    }
                }
                else
                {
                    dataStream = new MemoryStream(fileData);
                }

                // Stop the stopwatch and log download completion
                stopwatch.Stop();
                double throughputMBps = (actualSize / 1024.0 / 1024.0) / (stopwatch.ElapsedMilliseconds / 1000.0);
                activity?.AddEvent("cloudfetch.download_complete", [
                    new("offset", downloadResult.StartRowOffset),
                    new("sanitized_url", sanitizedUrl),
                    new("actual_size_bytes", actualSize),
                    new("actual_size_kb", actualSize / 1024.0),
                    new("latency_ms", stopwatch.ElapsedMilliseconds),
                    new("throughput_mbps", throughputMBps)
                ]);

                downloadResult.SetCompleted(dataStream, size);
                _stragglerDetector?.MarkCompleted(fileOffset, activity);
            }, activityName: "DownloadFile");
        }

        private void SetError(Exception ex, Activity? activity = null)
        {
            lock (_errorLock)
            {
                if (_error == null)
                {
                    activity?.AddException(ex, [new("error.context", "cloudfetch.error_state_set")]);
                    _error = ex;
                }
            }
        }

        private void CompleteWithError(Activity? activity = null)
        {
            try
            {
                // Mark the result queue as completed to prevent further additions
                _resultQueue.CompleteAdding();

                // Mark the download as completed with error
                _isCompleted = true;
            }
            catch (Exception ex)
            {
                activity?.AddException(ex, [new("error.context", "cloudfetch.complete_with_error_failed")]);
            }
        }

        // Helper method to sanitize URLs for logging (to avoid exposing sensitive information)
        private string SanitizeUrl(string url)
        {
            try
            {
                var uri = new Uri(url);
                return $"{uri.Scheme}://{uri.Host}/{Path.GetFileName(uri.LocalPath)}";
            }
            catch
            {
                // If URL parsing fails, return a generic identifier
                return "cloud-storage-url";
            }
        }
    }
}
