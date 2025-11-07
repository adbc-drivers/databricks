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
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Fetches result chunks from the Statement Execution REST API.
    /// Supports both manifest-based fetching (all links available upfront) and
    /// incremental chunk fetching via GetResultChunkAsync().
    /// </summary>
    internal class StatementExecutionResultFetcher : BaseResultFetcher
    {
        private readonly IStatementExecutionClient _client;
        private readonly string _statementId;
        private readonly GetStatementResponse _initialResponse;

        /// <summary>
        /// Initializes a new instance of the <see cref="StatementExecutionResultFetcher"/> class.
        /// Resources (memoryManager, downloadQueue) will be initialized by CloudFetchDownloadManager
        /// via the Initialize() method.
        /// </summary>
        /// <param name="client">The Statement Execution API client.</param>
        /// <param name="statementId">The statement ID for fetching results.</param>
        /// <param name="initialResponse">The initial GetStatement response containing the first result.</param>
<<<<<<< HEAD
        public StatementExecutionResultFetcher(
            IStatementExecutionClient client,
            string statementId,
            GetStatementResponse initialResponse)
            : base(null, null)  // Resources will be injected via Initialize()
        /// <param name="manifest">The result manifest containing chunk information.</param>
=======
>>>>>>> defec99 (fix(csharp): use GetStatementResponse.Result and follow next_chunk_index chain)
        public StatementExecutionResultFetcher(
            IStatementExecutionClient client,
            string statementId,
            GetStatementResponse initialResponse)
            : base(null, null)  // Resources will be injected via Initialize()
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _statementId = statementId ?? throw new ArgumentNullException(nameof(statementId));
            _initialResponse = initialResponse ?? throw new ArgumentNullException(nameof(initialResponse));
        }

        /// <inheritdoc />
        public override Task<IDownloadResult?> GetDownloadResultAsync(long offset, CancellationToken cancellationToken)
        {
            // For REST API, presigned URLs are long-lived and don't need refresh.
            // All URLs are obtained during the initial fetch in FetchAllResultsAsync.
            // URL refresh is not supported for Statement Execution API.
            return Task.FromResult<IDownloadResult?>(null);
        }

        /// <inheritdoc />
        public override async Task<IEnumerable<IDownloadResult>> RefreshUrlsAsync(
            long startChunkIndex,
            long endChunkIndex,
            CancellationToken cancellationToken)
        {
            // REST API presigned URLs expire (typically 1 hour), so we need to refresh them
            // using GetResultChunkAsync() which provides fresh URLs for specific chunk indices
            var refreshedResults = new List<IDownloadResult>();

            for (long chunkIndex = startChunkIndex; chunkIndex <= endChunkIndex; chunkIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    // Fetch fresh URLs for this chunk
                    var resultData = await _client.GetResultChunkAsync(
                        _statementId,
                        chunkIndex,
                        cancellationToken).ConfigureAwait(false);

                    if (resultData.ExternalLinks != null && resultData.ExternalLinks.Any())
                    {
                        foreach (var link in resultData.ExternalLinks)
                        {
                            // Parse the expiration time from ISO 8601 format
                            DateTime expirationTime = DateTime.UtcNow.AddHours(1);
                            if (!string.IsNullOrEmpty(link.Expiration))
                            {
                                try
                                {
                                    expirationTime = DateTime.Parse(link.Expiration, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                                }
                                catch (FormatException)
                                {
                                    // Use default expiration time if parsing fails
                                }
                            }

                            // Create refreshed download result
                            var downloadResult = new DownloadResult(
                                chunkIndex: link.ChunkIndex,
                                fileUrl: link.ExternalLinkUrl,
                                startRowOffset: link.RowOffset,
                                rowCount: link.RowCount,
                                byteCount: link.ByteCount,
                                expirationTime: expirationTime,
                                memoryManager: _memoryManager,
                                httpHeaders: link.HttpHeaders);

                            refreshedResults.Add(downloadResult);
                        }
                    }
                }
                catch (Exception)
                {
                    // Continue with other chunks even if one fails
                    continue;
                }
            }

            return refreshedResults;
        }

        /// <inheritdoc />
        protected override async Task FetchAllResultsAsync(CancellationToken cancellationToken)
        {
            // Yield execution so the download queue doesn't get blocked before downloader is started
            await Task.Yield();

            // Start with the initial result from GetStatement response
            var currentResult = _initialResponse.Result;

            if (currentResult == null)
            {
                // No result data available
                _hasMoreResults = false;
                return;
            }

            // Follow the chain of results using next_chunk_index/next_chunk_internal_link
            while (currentResult != null)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Process external links in the current result
                if (currentResult.ExternalLinks != null && currentResult.ExternalLinks.Any())
                {
                    foreach (var link in currentResult.ExternalLinks)
                    {
                        CreateAndAddDownloadResult(link, cancellationToken);
                    }
                }

                // Check if there are more chunks to fetch
                if (currentResult.NextChunkIndex.HasValue)
                {
                    // Fetch the next chunk by index
                    currentResult = await _client.GetResultChunkAsync(
                        _statementId,
                        currentResult.NextChunkIndex.Value,
                        cancellationToken).ConfigureAwait(false);
                }
                else if (!string.IsNullOrEmpty(currentResult.NextChunkInternalLink))
                {
                    // TODO: Support NextChunkInternalLink fetching if needed
                    // For now, we rely on NextChunkIndex
                    throw new NotSupportedException(
                        "NextChunkInternalLink is not yet supported. " +
                        "Please use NextChunkIndex-based fetching.");
                }
                else
                {
                    // No more chunks to fetch
                    currentResult = null;
                }
            }

            // All chunks have been processed
            _hasMoreResults = false;
        }

        /// <summary>
        /// Creates a DownloadResult from an ExternalLink and adds it to the download queue.
        /// </summary>
        /// <param name="link">The external link from the REST API.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        private void CreateAndAddDownloadResult(ExternalLink link, CancellationToken cancellationToken)
        {
            // Parse the expiration time from ISO 8601 format
            DateTime expirationTime = DateTime.UtcNow.AddHours(1); // Default to 1 hour if parsing fails
            if (!string.IsNullOrEmpty(link.Expiration))
            {
                try
                {
                    expirationTime = DateTime.Parse(link.Expiration, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                }
                catch (FormatException)
                {
                    // Use default expiration time if parsing fails
                }
            }

            // Create download result from REST API link
            var downloadResult = new DownloadResult(
                chunkIndex: link.ChunkIndex,
                fileUrl: link.ExternalLinkUrl,
                startRowOffset: link.RowOffset,
                rowCount: link.RowCount,
                byteCount: link.ByteCount,
                expirationTime: expirationTime,
                memoryManager: _memoryManager,
                httpHeaders: link.HttpHeaders); // Pass custom headers for cloud storage auth

            // Add to download queue
            AddDownloadResult(downloadResult, cancellationToken);
        }
    }
}
