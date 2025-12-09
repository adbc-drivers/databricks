/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
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
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;
using Apache.Arrow.Adbc.Tracing;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Statement Execution API-specific implementation that fetches result chunks
    /// from the REST API. The initial response includes external links for chunk 0,
    /// and subsequent chunks are fetched via GetResultChunkAsync.
    /// </summary>
    internal class StatementExecutionResultFetcher : CloudFetchResultFetcher, IDisposable
    {
        private readonly IStatementExecutionClient _client;
        private readonly string _statementId;
        private readonly ResultManifest _manifest;
        private readonly List<ExternalLink>? _initialExternalLinks;
        private readonly SemaphoreSlim _fetchLock = new SemaphoreSlim(1, 1);
        private int _currentChunkIndex;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="StatementExecutionResultFetcher"/> class.
        /// </summary>
        /// <param name="client">The Statement Execution API client.</param>
        /// <param name="statementId">The statement ID for fetching results.</param>
        /// <param name="manifest">The result manifest containing chunk metadata.</param>
        /// <param name="initialExternalLinks">External links for chunk 0 from the initial response (may be null).</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <param name="downloadQueue">The queue to add download tasks to.</param>
        public StatementExecutionResultFetcher(
            IStatementExecutionClient client,
            string statementId,
            ResultManifest manifest,
            List<ExternalLink>? initialExternalLinks,
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue)
            : base(memoryManager, downloadQueue)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _statementId = statementId ?? throw new ArgumentNullException(nameof(statementId));
            _manifest = manifest ?? throw new ArgumentNullException(nameof(manifest));
            _initialExternalLinks = initialExternalLinks;
        }

        /// <inheritdoc />
        protected override void ResetState()
        {
            _currentChunkIndex = 0;
        }

        /// <inheritdoc />
        protected override Task<bool> HasInitialResultsAsync(CancellationToken cancellationToken)
        {
            // Check if we have initial external links from the response
            bool hasResults = _initialExternalLinks != null && _initialExternalLinks.Count > 0;
            return Task.FromResult(hasResults);
        }

        /// <inheritdoc />
        protected override void ProcessInitialResultsAsync(CancellationToken cancellationToken)
        {
            if (_initialExternalLinks == null || _initialExternalLinks.Count == 0)
            {
                // No initial links - will fetch chunk 0 via API
                return;
            }

            // Process the initial external links (chunk 0)
            ProcessExternalLinks(_initialExternalLinks, cancellationToken);
            _currentChunkIndex = 1; // Start fetching from chunk 1

            // Check if there are more chunks to fetch
            _hasMoreResults = _currentChunkIndex < _manifest.TotalChunkCount;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Note: This method does NOT use a lock because it's called sequentially by a single
        /// fetcher background task (same pattern as ThriftResultFetcher). The _fetchLock is only
        /// used in RefreshUrlsAsync to serialize concurrent URL refresh requests from the downloader.
        /// </remarks>
        protected override async Task FetchNextBatchAsync(CancellationToken cancellationToken)
        {
            if (_currentChunkIndex >= _manifest.TotalChunkCount)
            {
                _hasMoreResults = false;
                return;
            }

            int chunkToFetch = _currentChunkIndex;

            Activity.Current?.AddEvent("cloudfetch.fetch_chunk_start", [
                new("chunk_index", chunkToFetch),
                new("total_chunks", _manifest.TotalChunkCount)
            ]);

            try
            {
                var resultData = await _client.GetResultChunkAsync(
                    _statementId,
                    chunkToFetch,
                    cancellationToken).ConfigureAwait(false);

                Activity.Current?.AddEvent("cloudfetch.fetch_chunk_complete", [
                    new("chunk_index", chunkToFetch),
                    new("external_links_count", resultData.ExternalLinks?.Count ?? 0)
                ]);

                if (resultData.ExternalLinks != null && resultData.ExternalLinks.Count > 0)
                {
                    ProcessExternalLinks(resultData.ExternalLinks, cancellationToken);
                }

                _currentChunkIndex++;
                _hasMoreResults = _currentChunkIndex < _manifest.TotalChunkCount;
            }
            catch (Exception ex)
            {
                Activity.Current?.AddEvent("cloudfetch.fetch_chunk_error", [
                    new("error_message", ex.Message),
                    new("error_type", ex.GetType().Name),
                    new("chunk_index", chunkToFetch)
                ]);
                _hasMoreResults = false;
                throw;
            }
        }

        /// <summary>
        /// Processes a collection of external links by creating download results and adding them to the queue.
        /// </summary>
        /// <param name="externalLinks">The collection of external links to process.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        private void ProcessExternalLinks(List<ExternalLink> externalLinks, CancellationToken cancellationToken)
        {
            foreach (var link in externalLinks)
            {
                var downloadResult = CreateDownloadResultFromLink(link);
                _downloadQueue.Add(downloadResult, cancellationToken);
            }
        }

        /// <summary>
        /// Creates a DownloadResult from an ExternalLink.
        /// </summary>
        /// <param name="link">The external link.</param>
        /// <returns>A new DownloadResult instance.</returns>
        private DownloadResult CreateDownloadResultFromLink(ExternalLink link)
        {
            // Parse expiration time from ISO 8601 string
            DateTime expirationTime = DateTime.UtcNow.AddHours(1); // Default to 1 hour
            if (!string.IsNullOrEmpty(link.Expiration))
            {
                if (DateTime.TryParse(link.Expiration, null, System.Globalization.DateTimeStyles.RoundtripKind, out var parsedTime))
                {
                    expirationTime = parsedTime.ToUniversalTime();
                }
                else
                {
                    Activity.Current?.AddEvent("cloudfetch.expiration_parse_failed", [
                        new("chunk_index", link.ChunkIndex),
                        new("expiration_string", link.Expiration),
                        new("default_expiration_minutes", 60)
                    ]);
                }
            }
            else
            {
                Activity.Current?.AddEvent("cloudfetch.expiration_missing", [
                    new("chunk_index", link.ChunkIndex),
                    new("default_expiration_minutes", 60)
                ]);
            }

            return new DownloadResult(
                chunkIndex: link.ChunkIndex,
                fileUrl: link.ExternalLinkUrl,
                startRowOffset: link.RowOffset,
                rowCount: link.RowCount,
                byteCount: link.ByteCount,
                expirationTime: expirationTime,
                memoryManager: _memoryManager,
                httpHeaders: link.HttpHeaders);
        }

        /// <inheritdoc />
        /// <remarks>
        /// This method uses _fetchLock to serialize concurrent URL refresh requests from the downloader.
        /// This matches the pattern used in ThriftResultFetcher.RefreshUrlsAsync. The lock is held during
        /// the entire operation including the HTTP call to prevent multiple concurrent refresh requests
        /// for the same or nearby chunks.
        /// </remarks>
        public override async Task<IEnumerable<IDownloadResult>> RefreshUrlsAsync(
            long startRowOffset,
            CancellationToken cancellationToken)
        {
            await _fetchLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                // Find the chunk index that contains or follows the given row offset
                long targetChunkIndex = FindChunkIndexForRowOffset(startRowOffset);

                if (targetChunkIndex < 0 || targetChunkIndex >= _manifest.TotalChunkCount)
                {
                    return new List<IDownloadResult>();
                }

                Activity.Current?.AddEvent("cloudfetch.refresh_urls_start", [
                    new("start_row_offset", startRowOffset),
                    new("target_chunk_index", targetChunkIndex)
                ]);

                var resultData = await _client.GetResultChunkAsync(
                    _statementId,
                    targetChunkIndex,
                    cancellationToken).ConfigureAwait(false);

                var refreshedResults = new List<IDownloadResult>();

                if (resultData.ExternalLinks != null && resultData.ExternalLinks.Count > 0)
                {
                    foreach (var link in resultData.ExternalLinks)
                    {
                        var downloadResult = CreateDownloadResultFromLink(link);
                        refreshedResults.Add(downloadResult);
                    }

                    Activity.Current?.AddEvent("cloudfetch.urls_refreshed", [
                        new("count", refreshedResults.Count),
                        new("start_offset", startRowOffset),
                        new("chunk_index", targetChunkIndex)
                    ]);
                }

                return refreshedResults;
            }
            finally
            {
                _fetchLock.Release();
            }
        }

        /// <summary>
        /// Finds the chunk index that contains or follows the given row offset.
        /// </summary>
        /// <param name="rowOffset">The row offset to find.</param>
        /// <returns>The chunk index, or -1 if not found.</returns>
        private long FindChunkIndexForRowOffset(long rowOffset)
        {
            if (_manifest.Chunks == null || _manifest.Chunks.Count == 0)
            {
                Activity.Current?.AddEvent("cloudfetch.find_chunk_failed", [
                    new("reason", "manifest_chunks_empty"),
                    new("row_offset", rowOffset)
                ]);
                return -1;
            }

            // Look for the chunk that contains this row offset
            foreach (var chunk in _manifest.Chunks)
            {
                if (rowOffset >= chunk.RowOffset && rowOffset < chunk.RowOffset + chunk.RowCount)
                {
                    return chunk.ChunkIndex;
                }
            }

            // Chunk not found in manifest - log and return failure
            Activity.Current?.AddEvent("cloudfetch.find_chunk_failed", [
                new("reason", "chunk_not_in_manifest"),
                new("row_offset", rowOffset),
                new("manifest_chunk_count", _manifest.Chunks.Count),
                new("total_chunk_count", _manifest.TotalChunkCount)
            ]);

            return -1;
        }

        /// <summary>
        /// Disposes the fetcher and releases the semaphore.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes managed resources.
        /// </summary>
        /// <param name="disposing">True if disposing managed resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _fetchLock?.Dispose();
                }
                _disposed = true;
            }
        }
    }
}
