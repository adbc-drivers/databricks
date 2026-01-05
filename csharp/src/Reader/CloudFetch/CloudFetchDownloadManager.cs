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
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Manages the CloudFetch download pipeline.
    /// Protocol-agnostic - works with both Thrift and REST implementations.
    /// Use <see cref="CloudFetchReaderFactory"/> to create instances.
    /// </summary>
    internal sealed class CloudFetchDownloadManager : ICloudFetchDownloadManager
    {
        private readonly ICloudFetchMemoryBufferManager _memoryManager;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly BlockingCollection<IDownloadResult> _resultQueue;
        private readonly ICloudFetchResultFetcher _resultFetcher;
        private readonly ICloudFetchDownloader _downloader;
        private bool _isDisposed;
        private bool _isStarted;
        private CancellationTokenSource? _cancellationTokenSource;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchDownloadManager"/> class.
        /// All components are pre-built by the factory - this class only orchestrates the pipeline.
        /// </summary>
        /// <param name="resultFetcher">The result fetcher (protocol-specific, pre-configured).</param>
        /// <param name="downloader">The downloader (pre-configured).</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <param name="downloadQueue">The download queue (shared with fetcher and downloader).</param>
        /// <param name="resultQueue">The result queue (shared with downloader).</param>
        /// <param name="config">The CloudFetch configuration.</param>
        public CloudFetchDownloadManager(
            ICloudFetchResultFetcher resultFetcher,
            ICloudFetchDownloader downloader,
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue,
            BlockingCollection<IDownloadResult> resultQueue,
            CloudFetchConfiguration config)
        {
            _resultFetcher = resultFetcher ?? throw new ArgumentNullException(nameof(resultFetcher));
            _downloader = downloader ?? throw new ArgumentNullException(nameof(downloader));
            _memoryManager = memoryManager ?? throw new ArgumentNullException(nameof(memoryManager));
            _downloadQueue = downloadQueue ?? throw new ArgumentNullException(nameof(downloadQueue));
            _resultQueue = resultQueue ?? throw new ArgumentNullException(nameof(resultQueue));

            if (config == null) throw new ArgumentNullException(nameof(config));
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
