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
        private readonly ResultManifest _manifest;

        /// <summary>
        /// Initializes a new instance of the <see cref="StatementExecutionResultFetcher"/> class.
        /// Resources (memoryManager, downloadQueue) will be initialized by CloudFetchDownloadManager
        /// via the Initialize() method.
        /// </summary>
        /// <param name="client">The Statement Execution API client.</param>
        /// <param name="statementId">The statement ID for fetching results.</param>
        /// <param name="manifest">The result manifest containing chunk information.</param>
        public StatementExecutionResultFetcher(
            IStatementExecutionClient client,
            string statementId,
            ResultManifest manifest)
            : base(null, null)  // Resources will be injected via Initialize()
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _statementId = statementId ?? throw new ArgumentNullException(nameof(statementId));
            _manifest = manifest ?? throw new ArgumentNullException(nameof(manifest));
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
        public override Task<System.Collections.Generic.IEnumerable<IDownloadResult>> RefreshUrlsAsync(
            long startChunkIndex,
            long endChunkIndex,
            CancellationToken cancellationToken)
        {
            // For REST API, presigned URLs from ResultManifest are long-lived (~1 hour).
            // URL refresh via GetResultChunkAsync() is not currently implemented.
            // If URLs expire, queries will fail and need to be rerun.
            return Task.FromResult(System.Linq.Enumerable.Empty<IDownloadResult>());
        }

        /// <inheritdoc />
        protected override async Task FetchAllResultsAsync(CancellationToken cancellationToken)
        {
            // Yield execution so the download queue doesn't get blocked before downloader is started
            await Task.Yield();

            if (_manifest.Chunks == null || _manifest.Chunks.Count == 0)
            {
                // No chunks to process
                _hasMoreResults = false;
                return;
            }

            // Process all chunks from the manifest
            foreach (var chunk in _manifest.Chunks)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Check if chunk has external links in the manifest
                if (chunk.ExternalLinks != null && chunk.ExternalLinks.Any())
                {
                    // Manifest-based fetching: all links available upfront
                    foreach (var link in chunk.ExternalLinks)
                    {
                        CreateAndAddDownloadResult(link, cancellationToken);
                    }
                }
                else
                {
                    // Incremental chunk fetching: fetch external links for this chunk
                    // This handles cases where the manifest doesn't contain all links upfront
                    var resultData = await _client.GetResultChunkAsync(
                        _statementId,
                        chunk.ChunkIndex,
                        cancellationToken).ConfigureAwait(false);

                    if (resultData.ExternalLinks != null && resultData.ExternalLinks.Any())
                    {
                        foreach (var link in resultData.ExternalLinks)
                        {
                            CreateAndAddDownloadResult(link, cancellationToken);
                        }
                    }
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
                memoryManager: _memoryManager!,
                httpHeaders: link.HttpHeaders); // Pass custom headers for cloud storage auth

            // Add to download queue
            AddDownloadResult(downloadResult, cancellationToken);
        }
    }
}
