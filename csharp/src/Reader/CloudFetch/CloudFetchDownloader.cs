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
        private volatile bool _isCompleted;
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
            // Note: When JdbcStyleChunkMap is used, this downloader's async loop is not started.
            // The factory calls resultFetcher.StartAsync() directly and creates chunk map workers instead.
            _downloadTask = DownloadFilesAsync(_cancellationTokenSource.Token);
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

                        // Acquire a download slot (limits HTTP concurrency)
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

                // On .NET Framework, ensure the ServicePoint for this cloud storage host
                // has a high connection limit. DefaultConnectionLimit may have been set too
                // late (after a ServicePoint was already created with limit=2).
                // This explicitly overrides the per-host limit on first download.
                // ServicePoint tuning handled by JdbcStyleChunkMap


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
                        // Download the file data and get HTTP status
                        System.Net.HttpStatusCode statusCode;
                        (fileData, statusCode) = await DownloadBytesAsync(url, downloadResult.HttpHeaders, cancellationToken).ConfigureAwait(false);

                        // Check if the response indicates an expired URL (typically 403 or 401)
                        if (statusCode == System.Net.HttpStatusCode.Forbidden ||
                            statusCode == System.Net.HttpStatusCode.Unauthorized)
                        {
                            fileData = null; // Don't use the error response body

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
                                downloadResult.UpdateWithRefreshedUrl(refreshedResult.FileUrl, refreshedResult.ExpirationTime, refreshedResult.HttpHeaders);
                                url = refreshedResult.FileUrl;
                                sanitizedUrl = SanitizeUrl(url);

                                activity?.AddEvent("cloudfetch.url_refreshed_after_auth_error", [
                                    new("offset", refreshedResult.StartRowOffset),
                                    new("sanitized_url", sanitizedUrl)
                                ]);
                                continue;
                            }
                            else
                            {
                                throw new InvalidOperationException("Failed to refresh expired URL.");
                            }
                        }

                        if ((int)statusCode >= 400)
                        {
                            throw new HttpRequestException($"HTTP {(int)statusCode} from {sanitizedUrl}");
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

                // Decompress if LZ4-compressed, then feed to Arrow parser
                Stream arrowInputStream;
                if (_isLz4Compressed)
                {
                    var memoryStreamManager = _memoryStreamManager ?? new RecyclableMemoryStreamManager();
                    var lz4BufferPool = _lz4BufferPool ?? ArrayPool<byte>.Shared;
                    arrowInputStream = await Lz4Utilities.DecompressLz4Async(
                        fileData,
                        memoryStreamManager,
                        lz4BufferPool,
                        cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    arrowInputStream = new MemoryStream(fileData);
                }

                // Pre-parse Arrow IPC on the download thread (JDBC parity).
                // Arrow parsing happens directly from the streaming decompression pipeline.
                // With 16 download threads doing HTTP→LZ4→Arrow in parallel, the single
                // reader thread just iterates pre-parsed RecordBatch objects (pure memory access).
                var preParsedBatches = new List<Apache.Arrow.RecordBatch>();
                try
                {
                    using (var arrowReader = new Apache.Arrow.Ipc.ArrowStreamReader(arrowInputStream, leaveOpen: false))
                    {
                        while (true)
                        {
                            var batch = await arrowReader.ReadNextRecordBatchAsync(cancellationToken);
                            if (batch == null) break;
                            preParsedBatches.Add(batch);
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Clean up any batches already parsed before the error
                    foreach (var batch in preParsedBatches) batch.Dispose();
                    _memoryManager.ReleaseMemory(size);
                    throw new InvalidOperationException($"Error parsing Arrow data: {ex.Message}", ex);
                }

                // Stop the stopwatch and log download completion
                stopwatch.Stop();
                long actualSize = preParsedBatches.Sum(b => (long)b.Length * b.ColumnCount * 8); // rough estimate
                double throughputMBps = (size / 1024.0 / 1024.0) / (stopwatch.ElapsedMilliseconds / 1000.0);
                activity?.AddEvent("cloudfetch.download_complete", [
                    new("offset", downloadResult.StartRowOffset),
                    new("sanitized_url", sanitizedUrl),
                    new("actual_size_bytes", actualSize),
                    new("actual_size_kb", actualSize / 1024.0),
                    new("latency_ms", stopwatch.ElapsedMilliseconds),
                    new("throughput_mbps", throughputMBps),
                    new("pre_parsed_batches", preParsedBatches.Count)
                ]);

                // Set the download as completed with pre-parsed batches
                if (downloadResult is DownloadResult concreteResult)
                {
                    concreteResult.SetCompletedWithBatches(preParsedBatches, size);
                }
                else
                {
                    // Fallback for non-DownloadResult implementations
                    downloadResult.SetCompleted(new MemoryStream(), size);
                }
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

#if !NET8_0_OR_GREATER
        /// <summary>
        /// JDBC-style worker thread loop. Each dedicated thread independently pulls chunks
        /// from the download queue and processes them synchronously: HTTP GET → decompress → Arrow parse.
        /// Multiple threads run concurrently — true parallelism without ThreadPool or async overhead.
        /// This matches JDBC's ExecutorService.submit(new ChunkDownloadTask(chunk)) pattern.
        /// </summary>
        private void WorkerThreadLoop(int workerId, CancellationToken cancellationToken)
        {
            try
            {
                foreach (var downloadResult in _downloadQueue.GetConsumingEnumerable(cancellationToken))
                {
                    if (HasError || cancellationToken.IsCancellationRequested) break;

                    // EndOfResultsGuard: pass through to result queue and stop
                    if (downloadResult == EndOfResultsGuard.Instance)
                    {
                        _resultQueue.Add(EndOfResultsGuard.Instance, cancellationToken);
                        _isCompleted = true;
                        break;
                    }

                    try
                    {
                        string url = downloadResult.FileUrl;
                        long size = downloadResult.Size;

                        // ServicePoint tuning handled by JdbcStyleChunkMap

                        // Acquire memory before download (same as async path)
                        _memoryManager.AcquireMemoryAsync(size, cancellationToken).Wait(cancellationToken);

                        // Check URL expiration
                        if (downloadResult.IsExpiredOrExpiringSoon(_urlExpirationBufferSeconds))
                        {
                            var refreshed = _resultFetcher.RefreshUrlsAsync(downloadResult.StartRowOffset, cancellationToken)
                                .ConfigureAwait(false).GetAwaiter().GetResult();
                            var match = refreshed.FirstOrDefault(r => r.StartRowOffset == downloadResult.StartRowOffset);
                            if (match != null)
                            {
                                downloadResult.UpdateWithRefreshedUrl(match.FileUrl, match.ExpirationTime, match.HttpHeaders);
                                url = match.FileUrl;
                            }
                        }

                        // Synchronous HTTP download (the whole point — no async overhead)
                        var webRequest = (System.Net.HttpWebRequest)System.Net.WebRequest.Create(url);
                        webRequest.Method = "GET";
                        webRequest.Timeout = (int)TimeSpan.FromMinutes(_timeoutMinutes).TotalMilliseconds;
                        webRequest.ReadWriteTimeout = (int)TimeSpan.FromMinutes(_timeoutMinutes).TotalMilliseconds;
                        webRequest.ServicePoint.Expect100Continue = false;
                        webRequest.ServicePoint.UseNagleAlgorithm = false;
                        webRequest.ServicePoint.ConnectionLimit = Math.Max(webRequest.ServicePoint.ConnectionLimit, _maxParallelDownloads);

                        if (downloadResult.HttpHeaders != null)
                        {
                            foreach (var header in downloadResult.HttpHeaders)
                                webRequest.Headers[header.Key] = header.Value;
                        }

                        byte[] fileData;
                        System.Net.HttpWebResponse? webResponse = null;
                        try
                        {
                            webResponse = (System.Net.HttpWebResponse)webRequest.GetResponse();
                        }
                        catch (System.Net.WebException ex) when (ex.Response is System.Net.HttpWebResponse errResp)
                        {
                            if (errResp.StatusCode == System.Net.HttpStatusCode.Forbidden ||
                                errResp.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                            {
                                // URL refresh + retry (simplified — one attempt)
                                var refreshed2 = _resultFetcher.RefreshUrlsAsync(downloadResult.StartRowOffset, cancellationToken)
                                    .ConfigureAwait(false).GetAwaiter().GetResult();
                                var match2 = refreshed2.FirstOrDefault(r => r.StartRowOffset == downloadResult.StartRowOffset);
                                if (match2 != null)
                                {
                                    downloadResult.UpdateWithRefreshedUrl(match2.FileUrl, match2.ExpirationTime, match2.HttpHeaders);
                                    var retryReq = (System.Net.HttpWebRequest)System.Net.WebRequest.Create(match2.FileUrl);
                                    retryReq.Method = "GET";
                                    retryReq.Timeout = webRequest.Timeout;
                                    retryReq.ReadWriteTimeout = webRequest.ReadWriteTimeout;
                                    if (downloadResult.HttpHeaders != null)
                                        foreach (var h in downloadResult.HttpHeaders)
                                            retryReq.Headers[h.Key] = h.Value;
                                    webResponse = (System.Net.HttpWebResponse)retryReq.GetResponse();
                                }
                                else throw;
                            }
                            else throw;
                        }

                        using (webResponse)
                        {
                            long contentLength = webResponse.ContentLength;
                            int capacity = (contentLength > 0 && contentLength <= 100 * 1024 * 1024)
                                ? (int)contentLength : 0;
                            using (var responseStream = webResponse.GetResponseStream())
                            {
                                var ms = capacity > 0 ? new MemoryStream(capacity) : new MemoryStream();
                                responseStream.CopyTo(ms, 81920);
                                if (ms.TryGetBuffer(out ArraySegment<byte> buf) && buf.Count == (int)ms.Length)
                                    fileData = buf.Array!;
                                else
                                    fileData = ms.ToArray();
                            }
                        }

                        // Decompress — use SYNCHRONOUS LZ4 decompression on dedicated threads.
                        // The async DecompressLz4Async uses CopyToAsync internally which causes
                        // TaskCanceledException on .NET Framework when called from a synchronous
                        // thread via .GetAwaiter().GetResult(). The sync version avoids this entirely.
                        Stream arrowStream;
                        if (_isLz4Compressed)
                        {
                            var bufPool = _lz4BufferPool ?? ArrayPool<byte>.Shared;
                            ReadOnlyMemory<byte> decompressed = Lz4Utilities.DecompressLz4(fileData, bufPool);
                            // Use MemoryMarshal to get the underlying array without copying
                            if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(decompressed, out ArraySegment<byte> segment))
                                arrowStream = new MemoryStream(segment.Array!, segment.Offset, segment.Count, writable: false);
                            else
                                arrowStream = new MemoryStream(decompressed.ToArray());
                        }
                        else
                        {
                            arrowStream = new MemoryStream(fileData);
                        }

                        // Pre-parse Arrow IPC (JDBC parity — parsing on download thread)
                        var batches = new System.Collections.Generic.List<Apache.Arrow.RecordBatch>();
                        using (var arrowReader = new Apache.Arrow.Ipc.ArrowStreamReader(arrowStream, leaveOpen: false))
                        {
                            while (true)
                            {
                                // Use synchronous read on dedicated thread
                                var batch = arrowReader.ReadNextRecordBatch();
                                if (batch == null) break;
                                batches.Add(batch);
                            }
                        }

                        // Complete the download result with pre-parsed batches
                        if (downloadResult is DownloadResult concrete)
                            concrete.SetCompletedWithBatches(batches, size);
                        else
                            downloadResult.SetCompleted(new MemoryStream(), size);

                        // Add to result queue (may block if queue full — this IS the sliding window)
                        _resultQueue.Add(downloadResult, cancellationToken);
                    }
                    catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                    {
                        _memoryManager.ReleaseMemory(downloadResult.Size);
                        downloadResult.SetFailed(ex);
                        _resultQueue.Add(downloadResult, cancellationToken);
                        SetError(ex);
                        break;
                    }
                }
            }
            catch (OperationCanceledException) { /* Expected on shutdown */ }
            catch (InvalidOperationException) when (_downloadQueue.IsCompleted) { /* Queue completed */ }
        }
#endif

        /// <summary>
        /// Downloads bytes from a URL. On .NET Framework / netstandard2.0, uses synchronous
        /// HttpWebRequest on a dedicated ThreadPool thread (via Task.Run) — matching JDBC's
        /// approach of dedicated threads with synchronous HttpGet.execute(). This avoids
        /// .NET Framework's HttpClient async pitfalls: internal sync-over-async in
        /// HttpWebRequest.GetResponseAsync(), ThreadPool starvation from blocked continuations,
        /// and state machine allocation overhead.
        ///
        /// On .NET 8+, uses the truly-async HttpClient.SendAsync() which has none of these issues.
        /// </summary>
        private async Task<(byte[] data, System.Net.HttpStatusCode statusCode)> DownloadBytesAsync(
            string url, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken)
        {
#if NET8_0_OR_GREATER
            // .NET 8+: truly async HttpClient — no ThreadPool thread blocking
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            if (headers != null)
            {
                foreach (var header in headers)
                {
                    request.Headers.TryAddWithoutValidation(header.Key, header.Value);
                }
            }

            using HttpResponseMessage response = await _httpClient.SendAsync(
                request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);

            if (response.StatusCode == System.Net.HttpStatusCode.Forbidden ||
                response.StatusCode == System.Net.HttpStatusCode.Unauthorized ||
                (int)response.StatusCode >= 400)
            {
                return (Array.Empty<byte>(), response.StatusCode);
            }

            // Body read with explicit timeout — HttpClient.Timeout only covers SendAsync
            // (header phase with ResponseHeadersRead). The body read is a separate call
            // with no timeout coverage. (Ported from PR #374)
            using (var bodyTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
            {
                bodyTimeoutCts.CancelAfter(TimeSpan.FromMinutes(_timeoutMinutes));
                try
                {
                    using (var contentStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                    {
                        long? contentLength = response.Content.Headers.ContentLength;
                        int capacity = (contentLength.HasValue && contentLength.Value > 0)
                            ? (int)Math.Min(contentLength.Value, 100 * 1024 * 1024) : 0;
                        using (var ms = new MemoryStream(capacity))
                        {
                            await contentStream.CopyToAsync(ms, 81920, bodyTimeoutCts.Token).ConfigureAwait(false);
                            return (ms.ToArray(), response.StatusCode);
                        }
                    }
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    throw new TimeoutException("Timed out while reading the response body.");
                }
            }
#else
            // .NET Framework / netstandard2.0: synchronous HttpWebRequest on dedicated thread.
            // This is the JDBC pattern — each download gets a real thread doing blocking I/O,
            // avoiding all async overhead and ThreadPool starvation issues.
            return await Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                var webRequest = (System.Net.HttpWebRequest)System.Net.WebRequest.Create(url);
                webRequest.Method = "GET";
                webRequest.Timeout = (int)TimeSpan.FromMinutes(5).TotalMilliseconds;
                webRequest.ReadWriteTimeout = (int)TimeSpan.FromMinutes(5).TotalMilliseconds;

                // Disable expect-100 and Nagle for each request
                webRequest.ServicePoint.Expect100Continue = false;
                webRequest.ServicePoint.UseNagleAlgorithm = false;

                if (headers != null)
                {
                    foreach (var header in headers)
                    {
                        webRequest.Headers[header.Key] = header.Value;
                    }
                }

                System.Net.HttpWebResponse? webResponse = null;
                try
                {
                    webResponse = (System.Net.HttpWebResponse)webRequest.GetResponse();
                }
                catch (System.Net.WebException ex) when (ex.Response is System.Net.HttpWebResponse errorResponse)
                {
                    // Capture 403/401 responses instead of throwing
                    return (Array.Empty<byte>(), errorResponse.StatusCode);
                }

                using (webResponse)
                {
                    var statusCode = webResponse.StatusCode;
                    if ((int)statusCode >= 400)
                    {
                        return (Array.Empty<byte>(), statusCode);
                    }

                    // Pre-size MemoryStream from Content-Length to avoid repeated
                    // internal buffer resize+copy operations (256→512→...→32MB for a 20MB file).
                    // Then use GetBuffer() to avoid the extra ToArray() copy.
                    long contentLength = webResponse.ContentLength;
                    int capacity = (contentLength > 0 && contentLength <= 100 * 1024 * 1024)
                        ? (int)contentLength : 0;

                    using (var responseStream = webResponse.GetResponseStream())
                    {
                        var memoryStream = capacity > 0 ? new MemoryStream(capacity) : new MemoryStream();
                        responseStream.CopyTo(memoryStream, 81920);

                        // Use GetBuffer + length to avoid ToArray() which allocates a new byte[].
                        // This eliminates one full copy of the download data (~20MB per chunk).
                        if (memoryStream.TryGetBuffer(out ArraySegment<byte> buffer))
                        {
                            // Buffer may be larger than actual data — slice to actual length
                            if (buffer.Count == (int)memoryStream.Length)
                            {
                                return (buffer.Array!, statusCode);
                            }
                        }
                        // Fallback: exact-size copy (only if TryGetBuffer fails or buffer is oversized)
                        return (memoryStream.ToArray(), statusCode);
                    }
                }
            }, cancellationToken).ConfigureAwait(false);
#endif
        }

        /// <summary>
        /// On .NET Framework, sets ConnectionLimit on the specific ServicePoint for this
        /// cloud storage host. This is critical because:
        /// 1. DefaultConnectionLimit only affects ServicePoints created AFTER it's set
        /// 2. If any HTTP request was made before our tuning, the ServicePoint exists with limit=2
        /// 3. This explicitly overrides the per-host limit regardless of creation order
        /// Also disables Nagle algorithm and Expect:100-Continue for lower latency.
        /// Runs once per downloader instance (first URL seen).
        /// </summary>
        // ServicePoint tuning is handled by JdbcStyleChunkMap.DownloadBytes()

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
