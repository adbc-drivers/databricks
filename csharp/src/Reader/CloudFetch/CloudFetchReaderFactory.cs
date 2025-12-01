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
using System.Net.Http;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Tracing;
using Apache.Hive.Service.Rpc.Thrift;
using Microsoft.IO;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
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
                httpClient,
                config);

            // Start the download manager
            downloadManager.StartAsync().Wait();

            // Create and return the reader
            return new CloudFetchReader(statement, schema, response, downloadManager);
        }

        // Future: CreateRestReader method will be added for REST API protocol support
        // public static CloudFetchReader CreateRestReader(...)
    }
}
