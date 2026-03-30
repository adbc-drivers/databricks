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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
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
        private readonly int _retryTimeoutSeconds;
        private readonly int _retryDelayMs;
        private readonly int _maxUrlRefreshAttempts;
        private readonly int _urlExpirationBufferSeconds;
        private readonly int _timeoutMinutes;
        private readonly SemaphoreSlim _downloadSemaphore;
        private readonly RecyclableMemoryStreamManager? _memoryStreamManager;
        private readonly ArrayPool<byte>? _lz4BufferPool;
        private Task? _downloadTask;
        private CancellationTokenSource? _cancellationTokenSource;
        private bool _isCompleted;
        private Exception? _error;
        private readonly object _errorLock = new object();

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

            _maxParallelDownloads = config.ParallelDownloads;
            _isLz4Compressed = config.IsLz4Compressed;
            _maxRetries = config.MaxRetries;
            _retryTimeoutSeconds = config.RetryTimeoutSeconds;
            _retryDelayMs = config.RetryDelayMs;
            _maxUrlRefreshAttempts = config.MaxUrlRefreshAttempts;
            _urlExpirationBufferSeconds = config.UrlExpirationBufferSeconds;
            _timeoutMinutes = config.TimeoutMinutes;
            _memoryStreamManager = config.MemoryStreamManager;
            _lz4BufferPool = config.Lz4BufferPool;
            _downloadSemaphore = new SemaphoreSlim(_maxParallelDownloads, _maxParallelDownloads);
            _isCompleted = false;
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
        /// <param name="maxRetries">Total number of attempts. 0 = no limit (use timeout only), positive = max total attempts.</param>
        /// <param name="retryTimeoutSeconds">Time budget for retries in seconds (optional, default 300).</param>
        /// <param name="retryDelayMs">Initial delay between retries in ms (optional, default 500).</param>
        internal CloudFetchDownloader(
            IActivityTracer activityTracer,
            BlockingCollection<IDownloadResult> downloadQueue,
            BlockingCollection<IDownloadResult> resultQueue,
            ICloudFetchMemoryBufferManager memoryManager,
            HttpClient httpClient,
            ICloudFetchResultFetcher resultFetcher,
            int maxParallelDownloads,
            bool isLz4Compressed,
            int maxRetries = CloudFetchConfiguration.DefaultMaxRetries,
            int retryTimeoutSeconds = CloudFetchConfiguration.DefaultRetryTimeoutSeconds,
            int retryDelayMs = CloudFetchConfiguration.DefaultRetryDelayMs)
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
            _retryTimeoutSeconds = retryTimeoutSeconds;
            _retryDelayMs = retryDelayMs;
            _maxUrlRefreshAttempts = CloudFetchConfiguration.DefaultMaxUrlRefreshAttempts;
            _urlExpirationBufferSeconds = CloudFetchConfiguration.DefaultUrlExpirationBufferSeconds;
            _timeoutMinutes = CloudFetchConfiguration.DefaultTimeoutMinutes;
            _memoryStreamManager = null;
            _lz4BufferPool = null;
            _downloadSemaphore = new SemaphoreSlim(_maxParallelDownloads, _maxParallelDownloads);
            _isCompleted = false;
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

            // Wait for the download task to start
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

                        activity?.AddEvent("cloudfetch.download_slot_acquired", [
                            new("chunk_index", downloadResult.ChunkIndex)
                        ]);

                        // Start the download task
                        Task downloadTask = DownloadFileAsync(downloadResult, cancellationToken)
                            .ContinueWith(t =>
                            {
                                // Release the download slot
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

                    // Always mark the result queue as complete when the download
                    // loop exits. Without this, a subsequent Take() call would
                    // block forever on an empty, non-completed queue if the caller
                    // retries after an exception (e.g. a fetcher error that the
                    // downloader doesn't know about).
                    if (HasError)
                    {
                        CompleteWithError(activity);
                    }
                    else
                    {
                        _isCompleted = true;
                        try { _resultQueue.CompleteAdding(); }
                        catch (Exception ex)
                        {
                            activity?.AddException(ex, [new("error.context", "cloudfetch.result_queue_already_completed")]);
                        }
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

                // Retry logic with time-budget approach and exponential backoff with jitter.
                // Same pattern as RetryHttpHandler: tracks cumulative backoff sleep time against
                // the budget. This gives transient issues (firewall, proxy 502, connection drops)
                // enough time to resolve.
                int currentBackoffMs = (int)Math.Min(Math.Max(0L, (long)_retryDelayMs), 32_000L);
                long retryTimeoutMs = Math.Min((long)_retryTimeoutSeconds, int.MaxValue / 1000L) * 1000L;
                long totalRetryWaitMs = 0;
                int attemptCount = 0;
                Exception? lastException = null;

                while (!cancellationToken.IsCancellationRequested)
                {
                    // Check max retry count before each attempt (0 = no limit, >0 = total attempts)
                    if (_maxRetries > 0 && attemptCount >= _maxRetries)
                    {
                        activity?.AddEvent("cloudfetch.download_max_retries_exceeded", [
                            new("offset", downloadResult.StartRowOffset),
                            new("sanitized_url", sanitizedUrl),
                            new("total_attempts", attemptCount),
                            new("max_retries", _maxRetries)
                        ]);
                        break;
                    }

                    attemptCount++;
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
                            cancellationToken).ConfigureAwait(false);

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

                        // Log the download size from response headers
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
                        else
                        {
                            activity?.AddEvent("cloudfetch.content_length_missing", [
                                new("offset", downloadResult.StartRowOffset),
                                new("sanitized_url", sanitizedUrl)
                            ]);
                        }

                        // Read the file data with an explicit timeout.
                        // ReadAsByteArrayAsync() on net472 has no CancellationToken overload,
                        // and HttpClient.Timeout does not protect body reads when
                        // HttpCompletionOption.ResponseHeadersRead is used — SendAsync returns
                        // after headers, and the subsequent body read is a separate call on
                        // HttpContent with no timeout coverage.
                        // Using CopyToAsync with an explicit token ensures dead TCP connections
                        // are detected on every 81920-byte chunk read.
                        using (var bodyTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                        {
                            bodyTimeoutCts.CancelAfter(TimeSpan.FromMinutes(_timeoutMinutes));
                            using (var contentStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                            {
                                // Pre-allocate with Content-Length when available (CloudFetch always provides it).
                                int capacity = contentLength.HasValue && contentLength.Value > 0
                                    ? (int)contentLength.Value
                                    : 0;
                                using (var memoryStream = new MemoryStream(capacity))
                                {
                                    await contentStream.CopyToAsync(memoryStream, 81920, bodyTimeoutCts.Token).ConfigureAwait(false);
                                    fileData = memoryStream.ToArray();
                                }
                            }
                        }
                        break; // Success, exit retry loop
                    }
                    catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                    {
                        lastException = ex;

                        // Exponential backoff with jitter (80-120% of base)
                        int waitMs = (int)Math.Max(100, currentBackoffMs * (0.8 + new Random().NextDouble() * 0.4));

                        // Check if we would exceed the time budget
                        if (retryTimeoutMs > 0 && totalRetryWaitMs + waitMs > retryTimeoutMs)
                        {
                            activity?.AddEvent("cloudfetch.download_retry_timeout_exceeded", [
                                new("offset", downloadResult.StartRowOffset),
                                new("sanitized_url", sanitizedUrl),
                                new("total_attempts", attemptCount),
                                new("total_retry_wait_ms", totalRetryWaitMs),
                                new("retry_timeout_seconds", _retryTimeoutSeconds),
                                new("last_error", ex.GetType().Name)
                            ]);
                            break;
                        }

                        totalRetryWaitMs += waitMs;

                        activity?.AddEvent("cloudfetch.download_retry", [
                            new("offset", downloadResult.StartRowOffset),
                            new("sanitized_url", sanitizedUrl),
                            new("attempt", attemptCount),
                            new("total_retry_wait_ms", totalRetryWaitMs),
                            new("retry_timeout_seconds", _retryTimeoutSeconds),
                            new("error_type", ex.GetType().Name),
                            new("backoff_ms", waitMs)
                        ]);

                        await Task.Delay(waitMs, cancellationToken).ConfigureAwait(false);
                        currentBackoffMs = (int)Math.Min((long)currentBackoffMs * 2L, 32_000L);
                    }
                }

                if (fileData == null)
                {
                    stopwatch.Stop();
                    activity?.AddEvent("cloudfetch.download_failed_all_retries", [
                        new("offset", downloadResult.StartRowOffset),
                        new("sanitized_url", sanitizedUrl),
                        new("total_attempts", attemptCount),
                        new("total_retry_wait_ms", totalRetryWaitMs),
                        new("elapsed_time_ms", stopwatch.ElapsedMilliseconds)
                    ]);

                    // Release the memory we acquired
                    _memoryManager.ReleaseMemory(size);
                    string retryLimits = _maxRetries > 0
                        ? $"max_retries: {_maxRetries}, timeout: {_retryTimeoutSeconds}s"
                        : $"timeout: {_retryTimeoutSeconds}s";
                    throw new InvalidOperationException(
                        $"Failed to download file from {sanitizedUrl} after {attemptCount} attempts over {stopwatch.Elapsed.TotalSeconds:F1}s ({retryLimits}). Last error: {lastException?.GetType().Name ?? "unknown"}");
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

                // Set the download as completed with the original size
                downloadResult.SetCompleted(dataStream, size);
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
            // Mark the download as completed with error
            _isCompleted = true;

            try
            {
                // Mark the result queue as completed to prevent further additions
                _resultQueue.CompleteAdding();
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
