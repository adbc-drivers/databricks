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
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Tracing;
using Apache.Hive.Service.Rpc.Thrift;

namespace AdbcDrivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Thrift-specific implementation that fetches result chunks from the Thrift server and manages URL caching and refreshing.
    /// </summary>
    internal class ThriftResultFetcher : CloudFetchResultFetcher
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
        private long _nextChunkIndex = 0;

        /// <summary>
        /// Initializes a new instance of the <see cref="ThriftResultFetcher"/> class.
        /// </summary>
        /// <param name="statement">The HiveServer2 statement interface.</param>
        /// <param name="response">The query response.</param>
        /// <param name="initialResults">Initial results, if available.</param>
        /// <param name="batchSize">The number of rows to fetch in each batch.</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <param name="downloadQueue">The queue to add download tasks to.</param>
        /// <param name="expirationBufferSeconds">Buffer time in seconds before URL expiration to trigger refresh.</param>
        /// <param name="clock">Clock implementation for time operations. If null, uses system clock.</param>
        public ThriftResultFetcher(
            IHiveServer2Statement statement,
            IResponse response,
            TFetchResultsResp? initialResults,
            long batchSize,
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue,
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
        protected override Task<bool> HasInitialResultsAsync(CancellationToken cancellationToken)
        {
            bool hasResults = (_statement.TryGetDirectResults(_response, out TSparkDirectResults? directResults)
                && directResults!.ResultSet?.Results?.ResultLinks?.Count > 0)
                || _initialResults?.Results?.ResultLinks?.Count > 0;

            return Task.FromResult(hasResults);
        }

        /// <inheritdoc />
        protected override void ProcessInitialResultsAsync(CancellationToken cancellationToken)
        {
            // Get initial results from direct results or initial fetch response
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

            // Process all result links
            long maxOffset = ProcessResultLinks(resultLinks, cancellationToken);

            // Update the start offset for the next fetch
            _startOffset = maxOffset;

            // Update whether there are more results
            _hasMoreResults = fetchResults.HasMoreRows;
        }

        /// <inheritdoc />
        protected override Task FetchNextBatchAsync(CancellationToken cancellationToken)
        {
            return FetchNextResultBatchAsync(null, cancellationToken);
        }

        /// <summary>
        /// Common method to fetch results from the Thrift server.
        /// </summary>
        /// <param name="startOffset">The start row offset for the fetch.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The fetch results response.</returns>
        private async Task<TFetchResultsResp> FetchResultsFromServerAsync(long startOffset, CancellationToken cancellationToken)
        {
            // Create fetch request
            TFetchResultsReq request = new TFetchResultsReq(_response.OperationHandle!, TFetchOrientation.FETCH_NEXT, _batchSize);

            if (_statement is DatabricksStatement databricksStatement)
            {
                request.MaxBytes = databricksStatement.MaxBytesPerFetchRequest;
            }

            // Set the start row offset
            if (startOffset > 0)
            {
                request.StartRowOffset = startOffset;
            }

            // Use the statement's configured query timeout
            using var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(_statement.QueryTimeoutSeconds));
            using var combinedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutTokenSource.Token);

            return await _statement.Client.FetchResults(request, combinedTokenSource.Token).ConfigureAwait(false);
        }

        private async Task FetchNextResultBatchAsync(long? offset, CancellationToken cancellationToken)
        {
            // Determine the start offset
            long startOffset = offset ?? _startOffset;

            // Fetch results from server
            TFetchResultsResp response;
            try
            {
                response = await FetchResultsFromServerAsync(startOffset, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Activity.Current?.AddEvent("cloudfetch.fetch_from_server_error", [
                    new("error_message", ex.Message),
                    new("error_type", ex.GetType().Name)
                ]);
                _hasMoreResults = false;
                throw;
            }

            // Check if we have URL-based results
            if (response.Results.__isset.resultLinks &&
                response.Results.ResultLinks != null &&
                response.Results.ResultLinks.Count > 0)
            {
                List<TSparkArrowResultLink> resultLinks = response.Results.ResultLinks;

                // Process all result links
                long maxOffset = ProcessResultLinks(resultLinks, cancellationToken);

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

        /// <summary>
        /// Processes a collection of result links by creating download results, adding them to the queue and cache.
        /// </summary>
        /// <param name="resultLinks">The collection of Thrift result links to process.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The maximum row offset encountered.</returns>
        private long ProcessResultLinks(List<TSparkArrowResultLink> resultLinks, CancellationToken cancellationToken)
        {
            long maxOffset = 0;

            foreach (var link in resultLinks)
            {
                // Create download result using factory method with chunk index
                var downloadResult = DownloadResult.FromThriftLink(_nextChunkIndex++, link, _memoryManager);

                // Add to download queue and cache
                _downloadQueue!.Add(downloadResult, cancellationToken);
                _urlsByOffset[link.StartRowOffset] = downloadResult;

                // Track the maximum offset for future fetches
                long endOffset = link.StartRowOffset + link.RowCount;
                maxOffset = Math.Max(maxOffset, endOffset);
            }

            return maxOffset;
        }

        /// <summary>
        /// Processes result links for URL refresh, creating download results and updating cache only (not adding to queue).
        /// </summary>
        /// <param name="resultLinks">The collection of Thrift result links to process.</param>
        /// <returns>A list of download results with refreshed URLs.</returns>
        private List<IDownloadResult> ProcessRefreshedResultLinks(List<TSparkArrowResultLink> resultLinks)
        {
            var refreshedResults = new List<IDownloadResult>();

            foreach (var link in resultLinks)
            {
                // Create download result with fresh URL
                var downloadResult = DownloadResult.FromThriftLink(_nextChunkIndex++, link, _memoryManager);
                refreshedResults.Add(downloadResult);

                // Update cache
                _urlsByOffset[link.StartRowOffset] = downloadResult;
            }

            return refreshedResults;
        }

        /// <inheritdoc />
        public override async Task<IEnumerable<IDownloadResult>> RefreshUrlsAsync(
            long startRowOffset,
            CancellationToken cancellationToken)
        {
            await _fetchLock.WaitAsync(cancellationToken);
            try
            {
                // Fetch results from server using the common helper
                TFetchResultsResp response = await FetchResultsFromServerAsync(startRowOffset, cancellationToken).ConfigureAwait(false);

                // Process the results if available
                if (response.Status.StatusCode == TStatusCode.SUCCESS_STATUS &&
                    response.Results.__isset.resultLinks &&
                    response.Results.ResultLinks != null &&
                    response.Results.ResultLinks.Count > 0)
                {
                    var refreshedResults = ProcessRefreshedResultLinks(response.Results.ResultLinks);

                    Activity.Current?.AddEvent("cloudfetch.urls_refreshed", [
                        new("count", refreshedResults.Count),
                        new("start_offset", startRowOffset)
                    ]);

                    return refreshedResults;
                }

                return new List<IDownloadResult>();
            }
            finally
            {
                _fetchLock.Release();
            }
        }
    }
}
