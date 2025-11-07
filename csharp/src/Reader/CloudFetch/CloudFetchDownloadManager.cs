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
using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Tracing;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Manages the CloudFetch download pipeline.
    /// Protocol-agnostic - works with both Thrift and REST implementations.
    /// </summary>
    internal sealed class CloudFetchDownloadManager : ICloudFetchDownloadManager
    {
        private readonly Schema _schema;
        private readonly ICloudFetchMemoryBufferManager _memoryManager;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly BlockingCollection<IDownloadResult> _resultQueue;
        private readonly ICloudFetchResultFetcher _resultFetcher;
        private readonly ICloudFetchDownloader _downloader;
        private readonly HttpClient? _httpClient;
        private bool _isDisposed;
        private bool _isStarted;
        private CancellationTokenSource? _cancellationTokenSource;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchDownloadManager"/> class.
        /// Protocol-agnostic constructor using dependency injection.
        /// </summary>
        /// <param name="resultFetcher">The result fetcher (protocol-specific).</param>
        /// <param name="httpClient">The HTTP client for downloading files.</param>
        /// <param name="config">The CloudFetch configuration.</param>
        /// <param name="tracingStatement">Optional tracing statement for Activity tracking.</param>
        public CloudFetchDownloadManager(
            ICloudFetchResultFetcher resultFetcher,
            HttpClient httpClient,
            CloudFetchConfiguration config,
            ITracingStatement? tracingStatement = null)
        {
            _resultFetcher = resultFetcher ?? throw new ArgumentNullException(nameof(resultFetcher));
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _schema = config?.Schema ?? throw new ArgumentNullException(nameof(config));

            // Set HTTP client timeout
            _httpClient.Timeout = TimeSpan.FromMinutes(config.TimeoutMinutes);

            // Initialize the memory manager
            _memoryManager = new CloudFetchMemoryBufferManager(config.MemoryBufferSizeMB);

            // Initialize the queues with bounded capacity
            _downloadQueue = new BlockingCollection<IDownloadResult>(
                new ConcurrentQueue<IDownloadResult>(),
                config.PrefetchCount * 2);
            _resultQueue = new BlockingCollection<IDownloadResult>(
                new ConcurrentQueue<IDownloadResult>(),
                config.PrefetchCount * 2);

            // Initialize the downloader
            _downloader = new CloudFetchDownloader(
                tracingStatement,
                _downloadQueue,
                _resultQueue,
                _memoryManager,
                _httpClient,
                _resultFetcher,
                config.ParallelDownloads,
                config.IsLz4Compressed,
                config.MaxRetries,
                config.RetryDelayMs,
                config.MaxUrlRefreshAttempts,
                config.UrlExpirationBufferSeconds);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchDownloadManager"/> class.
        /// Legacy Thrift-specific constructor for backward compatibility.
        /// </summary>
        /// <param name="statement">The HiveServer2 statement.</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="response">The query response.</param>
        /// <param name="initialResults">Initial results.</param>
        /// <param name="isLz4Compressed">Whether the results are LZ4 compressed.</param>
        /// <param name="httpClient">The HTTP client.</param>
        [Obsolete("Use the protocol-agnostic constructor with CloudFetchConfiguration instead.")]
        public CloudFetchDownloadManager(
            IHiveServer2Statement statement,
            Schema schema,
            IResponse response,
            TFetchResultsResp? initialResults,
            bool isLz4Compressed,
            HttpClient httpClient)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));

            // Parse configuration from properties
            var config = CloudFetchConfiguration.FromProperties(statement.Connection.Properties, schema, isLz4Compressed);

            // Set HTTP client timeout
            _httpClient.Timeout = TimeSpan.FromMinutes(config.TimeoutMinutes);

            // Initialize shared resources
            _memoryManager = new CloudFetchMemoryBufferManager(config.MemoryBufferSizeMB);
            _downloadQueue = new BlockingCollection<IDownloadResult>(
                new ConcurrentQueue<IDownloadResult>(),
                config.PrefetchCount * 2);
            _resultQueue = new BlockingCollection<IDownloadResult>(
                new ConcurrentQueue<IDownloadResult>(),
                config.PrefetchCount * 2);

            // Create result fetcher with shared resources
            _resultFetcher = new CloudFetchResultFetcher(
                statement,
                response,
                initialResults,
                _memoryManager,
                _downloadQueue,
                statement.BatchSize,
                config.UrlExpirationBufferSeconds);

            // Create downloader with shared resources
            _downloader = new CloudFetchDownloader(
                statement as ITracingStatement,
                _downloadQueue,
                _resultQueue,
                _memoryManager,
                _httpClient,
                _resultFetcher,
                config.ParallelDownloads,
                config.IsLz4Compressed,
                config.MaxRetries,
                config.RetryDelayMs,
                config.MaxUrlRefreshAttempts,
                config.UrlExpirationBufferSeconds);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchDownloadManager"/> class.
        /// This constructor is intended for testing purposes only.
        /// </summary>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="resultFetcher">The result fetcher.</param>
        /// <param name="downloader">The downloader.</param>
        /// <param name="memoryBufferSizeMB">Memory buffer size in MB for testing.</param>
        internal CloudFetchDownloadManager(
            Schema schema,
            ICloudFetchResultFetcher resultFetcher,
            ICloudFetchDownloader downloader,
            int memoryBufferSizeMB = 200)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _resultFetcher = resultFetcher ?? throw new ArgumentNullException(nameof(resultFetcher));
            _downloader = downloader ?? throw new ArgumentNullException(nameof(downloader));

            // Create minimal resources for testing
            _memoryManager = new CloudFetchMemoryBufferManager(memoryBufferSizeMB);
            _downloadQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);
            _resultQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);
            _httpClient = new HttpClient();
        }

        /// <inheritdoc />
        public bool HasMoreResults => !_downloader.IsCompleted || !_resultQueue.IsCompleted;

        /// <inheritdoc />
        public async Task<IDownloadResult?> GetNextDownloadedFileAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            if (!_isStarted)
            {
                throw new InvalidOperationException("Download manager has not been started.");
            }

            try
            {
                return await _downloader.GetNextDownloadedFileAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (_resultFetcher.HasError)
            {
                throw new AggregateException("Errors in download pipeline", new[] { ex, _resultFetcher.Error! });
            }
        }

        /// <inheritdoc />
        public async Task StartAsync()
        {
            ThrowIfDisposed();

            if (_isStarted)
            {
                throw new InvalidOperationException("Download manager is already started.");
            }

            // Create a new cancellation token source
            _cancellationTokenSource = new CancellationTokenSource();

            // Start the result fetcher
            await _resultFetcher.StartAsync(_cancellationTokenSource.Token).ConfigureAwait(false);

            // Start the downloader
            await _downloader.StartAsync(_cancellationTokenSource.Token).ConfigureAwait(false);

            _isStarted = true;
        }

        /// <inheritdoc />
        public async Task StopAsync()
        {
            if (!_isStarted)
            {
                return;
            }

            // Cancel the token to signal all operations to stop
            _cancellationTokenSource?.Cancel();

            // Stop the downloader
            await _downloader.StopAsync().ConfigureAwait(false);

            // Stop the result fetcher
            await _resultFetcher.StopAsync().ConfigureAwait(false);

            // Dispose the cancellation token source
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;

            _isStarted = false;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_isDisposed)
            {
                return;
            }

            // Stop the pipeline
            StopAsync().GetAwaiter().GetResult();

            // Dispose the HTTP client
            _httpClient.Dispose();

            // Dispose the cancellation token source if it hasn't been disposed yet
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;

            // Mark the queues as completed to release any waiting threads
            _downloadQueue.CompleteAdding();
            _resultQueue.CompleteAdding();

            // Dispose any remaining results
            foreach (var result in _resultQueue.GetConsumingEnumerable(CancellationToken.None))
            {
                result.Dispose();
            }

            foreach (var result in _downloadQueue.GetConsumingEnumerable(CancellationToken.None))
            {
                result.Dispose();
            }

            _downloadQueue.Dispose();
            _resultQueue.Dispose();

            _isDisposed = true;
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(CloudFetchDownloadManager));
            }
        }
    }
}
