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
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using AdbcDrivers.Databricks.StatementExecution;
using Apache.Arrow;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Tracing;
using Apache.Hive.Service.Rpc.Thrift;
using Microsoft.IO;

namespace AdbcDrivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Factory for creating CloudFetch readers and related components.
    /// Centralizes object creation and eliminates circular dependencies by
    /// creating shared resources first and passing them to component constructors.
    /// </summary>
    internal static class CloudFetchReaderFactory
    {
        /// <summary>
        /// Creates a CloudFetch reader for the Thrift protocol.
        /// </summary>
        /// <param name="statement">The HiveServer2 statement.</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="response">The query response.</param>
        /// <param name="initialResults">Initial fetch results (may be null if not from direct results).</param>
        /// <param name="httpClient">The HTTP client for downloads.</param>
        /// <param name="isLz4Compressed">Whether results are LZ4 compressed.</param>
        /// <returns>A CloudFetchReader configured for Thrift protocol.</returns>
        public static CloudFetchReader CreateThriftReader(
            IHiveServer2Statement statement,
            Schema schema,
            IResponse response,
            TFetchResultsResp? initialResults,
            HttpClient httpClient,
            bool isLz4Compressed)
        {
            if (statement == null) throw new ArgumentNullException(nameof(statement));
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (httpClient == null) throw new ArgumentNullException(nameof(httpClient));

            // Build configuration from connection properties
            var config = CloudFetchConfiguration.FromProperties(
                statement.Connection.Properties,
                schema,
                isLz4Compressed);

            // Populate LZ4 resources from the connection
            var connection = (DatabricksConnection)statement.Connection;
            config.MemoryStreamManager = connection.RecyclableMemoryStreamManager;
            config.Lz4BufferPool = connection.Lz4BufferPool;

            // Create shared resources
            var memoryManager = new CloudFetchMemoryBufferManager(config.MemoryBufferSizeMB);
            var downloadQueue = new BlockingCollection<IDownloadResult>(
                new ConcurrentQueue<IDownloadResult>(),
                config.PrefetchCount * 2);
            var resultQueue = new BlockingCollection<IDownloadResult>(
                new ConcurrentQueue<IDownloadResult>(),
                config.PrefetchCount * 2);

            // Create the result fetcher with shared resources
            var resultFetcher = new ThriftResultFetcher(
                statement,
                response,
                initialResults,
                statement.BatchSize,
                memoryManager,
                downloadQueue,
                config.UrlExpirationBufferSeconds);

            // Create the downloader
            var downloader = new CloudFetchDownloader(
                statement,
                downloadQueue,
                resultQueue,
                memoryManager,
                httpClient,
                resultFetcher,
                config);

            // Create the download manager with pre-built components
            var downloadManager = new CloudFetchDownloadManager(
                resultFetcher,
                downloader,
                memoryManager,
                downloadQueue,
                resultQueue,
                config);

            // Start the download manager
            downloadManager.StartAsync().Wait();

            // For Thrift, use chunk-level row count limiting (pass 0 for totalExpectedRows)
            // because we don't know the total upfront - the fetcher accumulates as it goes
            return new CloudFetchReader(statement, schema, response, downloadManager, totalExpectedRows: 0);
        }

        /// <summary>
        /// Creates a CloudFetch reader for the Statement Execution REST API protocol.
        /// </summary>
        /// <param name="client">The Statement Execution API client.</param>
        /// <param name="statementId">The statement ID.</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="manifest">The result manifest containing chunk metadata.</param>
        /// <param name="initialExternalLinks">External links for chunk 0 from the initial response (avoids extra API call).</param>
        /// <param name="httpClient">The HTTP client for downloads.</param>
        /// <param name="properties">Connection properties for configuration.</param>
        /// <param name="memoryStreamManager">The recyclable memory stream manager.</param>
        /// <param name="lz4BufferPool">The LZ4 buffer pool for decompression.</param>
        /// <param name="statement">The statement for tracing support (StatementExecutionStatement implements ITracingStatement).</param>
        /// <returns>A CloudFetchReader configured for Statement Execution API protocol.</returns>
        public static CloudFetchReader CreateStatementExecutionReader(
            IStatementExecutionClient client,
            string statementId,
            Schema schema,
            ResultManifest manifest,
            List<ExternalLink>? initialExternalLinks,
            HttpClient httpClient,
            IReadOnlyDictionary<string, string> properties,
            RecyclableMemoryStreamManager memoryStreamManager,
            ArrayPool<byte> lz4BufferPool,
            ITracingStatement statement)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (string.IsNullOrEmpty(statementId)) throw new ArgumentNullException(nameof(statementId));
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (manifest == null) throw new ArgumentNullException(nameof(manifest));
            if (httpClient == null) throw new ArgumentNullException(nameof(httpClient));
            if (properties == null) throw new ArgumentNullException(nameof(properties));
            if (memoryStreamManager == null) throw new ArgumentNullException(nameof(memoryStreamManager));
            if (lz4BufferPool == null) throw new ArgumentNullException(nameof(lz4BufferPool));
            if (statement == null) throw new ArgumentNullException(nameof(statement));

            // Determine if LZ4 compression is enabled
            bool isLz4Compressed = properties.TryGetValue(DatabricksParameters.CanDecompressLz4, out var canDecompress) &&
                                   canDecompress.Equals("true", StringComparison.OrdinalIgnoreCase);

            // Build configuration from connection properties
            var config = CloudFetchConfiguration.FromProperties(properties, schema, isLz4Compressed);

            // Populate LZ4 resources
            config.MemoryStreamManager = memoryStreamManager;
            config.Lz4BufferPool = lz4BufferPool;

            // Create shared resources
            var memoryManager = new CloudFetchMemoryBufferManager(config.MemoryBufferSizeMB);
            var downloadQueue = new BlockingCollection<IDownloadResult>(
                new ConcurrentQueue<IDownloadResult>(),
                config.PrefetchCount * 2);
            var resultQueue = new BlockingCollection<IDownloadResult>(
                new ConcurrentQueue<IDownloadResult>(),
                config.PrefetchCount * 2);

            // Create the result fetcher for Statement Execution API
            var resultFetcher = new StatementExecutionResultFetcher(
                client,
                statementId,
                manifest,
                initialExternalLinks,
                memoryManager,
                downloadQueue);

            // Create the downloader - statement implements IActivityTracer via ITracingStatement
            var downloader = new CloudFetchDownloader(
                statement,
                downloadQueue,
                resultQueue,
                memoryManager,
                httpClient,
                resultFetcher,
                config);

            // Create the download manager
            var downloadManager = new CloudFetchDownloadManager(
                resultFetcher,
                downloader,
                memoryManager,
                downloadQueue,
                resultQueue,
                config);

            // Start the download manager
            downloadManager.StartAsync().Wait();

            // For REST API (SEA), use global row count limiting from manifest.TotalRowCount.
            // The manifest contains the adjusted total row count that respects LIMIT queries.
            return new CloudFetchReader(statement, schema, response: null, downloadManager, totalExpectedRows: manifest.TotalRowCount);
        }
    }
}
