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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Tracing;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Fetches result chunks from the Thrift server and manages URL caching and refreshing.
    /// </summary>
    internal class CloudFetchResultFetcher : BaseResultFetcher
    {
        private readonly IHiveServer2Statement _statement;
        private readonly IResponse _response;
        private readonly TFetchResultsResp? _initialResults;
        private readonly SemaphoreSlim _fetchLock = new SemaphoreSlim(1, 1);
        private readonly ConcurrentDictionary<long, IDownloadResult> _urlsByOffset = new ConcurrentDictionary<long, IDownloadResult>();
        private readonly int _expirationBufferSeconds;
        private readonly IClock _clock;
        private long _startOffset;
        private long _batchSize;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchResultFetcher"/> class.
        /// </summary>
        /// <param name="statement">The HiveServer2 statement interface.</param>
        /// <param name="response">The query response.</param>
        /// <param name="initialResults">Initial results, if available.</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <param name="downloadQueue">The queue to add download tasks to.</param>
        /// <param name="batchSize">The number of rows to fetch in each batch.</param>
        /// <param name="expirationBufferSeconds">Buffer time in seconds before URL expiration to trigger refresh.</param>
        /// <param name="clock">Clock implementation for time operations. If null, uses system clock.</param>
        public CloudFetchResultFetcher(
            IHiveServer2Statement statement,
            IResponse response,
            TFetchResultsResp? initialResults,
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue,
            long batchSize,
            int expirationBufferSeconds = 60,
            IClock? clock = null)
            : base(memoryManager, downloadQueue)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _response = response;
            _initialResults = initialResults;
            _batchSize = batchSize;
            _expirationBufferSeconds = expirationBufferSeconds;
            _clock = clock ?? new SystemClock();
        }

        /// <inheritdoc />
        protected override void ResetState()
        {
            _startOffset = 0;
            _urlsByOffset.Clear();
        }

        /// <inheritdoc />
        public override async Task<IDownloadResult?> GetDownloadResultAsync(long offset, CancellationToken cancellationToken)
        {
            // Check if we have a non-expired URL in the cache
            if (_urlsByOffset.TryGetValue(offset, out var cachedResult) && !cachedResult.IsExpiredOrExpiringSoon(_expirationBufferSeconds))
            {
                return cachedResult;
            }

            // Need to fetch or refresh the URL
            await _fetchLock.WaitAsync(cancellationToken);
            try
            {
                // Create fetch request for the specific offset
                TFetchResultsReq request = new TFetchResultsReq(
                    _response.OperationHandle!,
                    TFetchOrientation.FETCH_NEXT,
                    1);

                request.StartRowOffset = offset;

                // Cancelling mid-request breaks the client; Dispose() should not break the underlying client
                CancellationToken expiringToken = ApacheUtility.GetCancellationToken(_statement.QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);

                // Fetch results
                TFetchResultsResp response = await _statement.Client.FetchResults(request, expiringToken);

                // Process the results
                if (response.Status.StatusCode == TStatusCode.SUCCESS_STATUS &&
                    response.Results.__isset.resultLinks &&
                    response.Results.ResultLinks != null &&
                    response.Results.ResultLinks.Count > 0)
                {
                    var refreshedLink = response.Results.ResultLinks.FirstOrDefault(l => l.StartRowOffset == offset);
                    if (refreshedLink != null)
                    {
                        Activity.Current?.AddEvent("cloudfetch.url_fetched", [
                            new("offset", offset),
                            new("url_length", refreshedLink.FileLink?.Length ?? 0)
                        ]);

                        // Create a download result for the refreshed link using factory method
                        var downloadResult = DownloadResult.FromThriftLink(refreshedLink, _memoryManager);
                        _urlsByOffset[offset] = downloadResult;

                        return downloadResult;
                    }
                }

                Activity.Current?.AddEvent("cloudfetch.url_fetch_failed", [new("offset", offset)]);
                return null;
            }
            finally
            {
                _fetchLock.Release();
            }
        }

        /// <summary>
        /// Gets all currently cached download results.
        /// </summary>
        /// <returns>A dictionary mapping offsets to their download results.</returns>
        public Dictionary<long, IDownloadResult> GetAllCachedResults()
        {
            return _urlsByOffset.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }

        /// <summary>
        /// Clears all cached URLs.
        /// </summary>
        public void ClearCache()
        {
            _urlsByOffset.Clear();
        }

        /// <inheritdoc />
        protected override async Task FetchAllResultsAsync(CancellationToken cancellationToken)
        {
            // Process direct results first, if available
            if ((_statement.TryGetDirectResults(_response, out TSparkDirectResults? directResults)
                && directResults!.ResultSet?.Results?.ResultLinks?.Count > 0)
                || _initialResults?.Results?.ResultLinks?.Count > 0)
            {
                // Yield execution so the download queue doesn't get blocked before downloader is started
                await Task.Yield();
                ProcessDirectResultsAsync(cancellationToken);
            }

            // Continue fetching as needed
            while (_hasMoreResults && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Fetch more results from the server
                    await FetchNextResultBatchAsync(null, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // Expected when cancellation is requested
                    break;
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"Error fetching results: {ex.Message}");
                    _error = ex;
                    _hasMoreResults = false;
                    throw;
                }
            }
        }

        private async Task FetchNextResultBatchAsync(long? offset, CancellationToken cancellationToken)
        {
            // Create fetch request
            TFetchResultsReq request = new TFetchResultsReq(_response.OperationHandle!, TFetchOrientation.FETCH_NEXT, _batchSize);

            if (_statement is DatabricksStatement databricksStatement)
            {
                request.MaxBytes = databricksStatement.MaxBytesPerFetchRequest;
            }

            // Set the start row offset
            long startOffset = offset ?? _startOffset;
            if (startOffset > 0)
            {
                request.StartRowOffset = startOffset;
            }

            // Fetch results
            TFetchResultsResp response;
            try
            {
                // Use the statement's configured query timeout

                using var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(_statement.QueryTimeoutSeconds));
                using var combinedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutTokenSource.Token);

                response = await _statement.Client.FetchResults(request, combinedTokenSource.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error fetching results from server: {ex.Message}");
                _hasMoreResults = false;
                throw;
            }

            // Check if we have URL-based results
            if (response.Results.__isset.resultLinks &&
                response.Results.ResultLinks != null &&
                response.Results.ResultLinks.Count > 0)
            {
                List<TSparkArrowResultLink> resultLinks = response.Results.ResultLinks;
                long maxOffset = 0;

                // Process each link
                foreach (var link in resultLinks)
                {
                    // Create download result using factory method
                    var downloadResult = DownloadResult.FromThriftLink(link, _memoryManager);

                    // Add to download queue and cache
                    AddDownloadResult(downloadResult, cancellationToken);
                    _urlsByOffset[link.StartRowOffset] = downloadResult;

                    // Track the maximum offset for future fetches
                    long endOffset = link.StartRowOffset + link.RowCount;
                    maxOffset = Math.Max(maxOffset, endOffset);
                }

                // Update the start offset for the next fetch
                if (!offset.HasValue)  // Only update if this was a sequential fetch
                {
                    _startOffset = maxOffset;
                }

                // Update whether there are more results
                _hasMoreResults = response.HasMoreRows;
            }
            else
            {
                // No more results
                _hasMoreResults = false;
            }
        }

        private void ProcessDirectResultsAsync(CancellationToken cancellationToken)
        {
            TFetchResultsResp fetchResults;
            if (_statement.TryGetDirectResults(_response, out TSparkDirectResults? directResults)
                && directResults!.ResultSet?.Results?.ResultLinks?.Count > 0)
            {
                fetchResults = directResults.ResultSet;
            }
            else
            {
                fetchResults = _initialResults!;
            }

            List<TSparkArrowResultLink> resultLinks = fetchResults.Results.ResultLinks;

            long maxOffset = 0;

            // Process each link
            foreach (var link in resultLinks)
            {
                // Create download result using factory method
                var downloadResult = DownloadResult.FromThriftLink(link, _memoryManager);

                // Add to download queue and cache
                AddDownloadResult(downloadResult, cancellationToken);
                _urlsByOffset[link.StartRowOffset] = downloadResult;

                // Track the maximum offset for future fetches
                long endOffset = link.StartRowOffset + link.RowCount;
                maxOffset = Math.Max(maxOffset, endOffset);
            }

            // Update the start offset for the next fetch
            _startOffset = maxOffset;

            // Update whether there are more results
            _hasMoreResults = fetchResults.HasMoreRows;
        }
    }
}
