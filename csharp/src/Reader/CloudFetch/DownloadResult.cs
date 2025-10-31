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
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Represents a downloaded result file with its associated metadata.
    /// </summary>
    internal sealed class DownloadResult : IDownloadResult
    {
        private readonly TaskCompletionSource<bool> _downloadCompletionSource;
        private readonly ICloudFetchMemoryBufferManager _memoryManager;
        private Stream? _dataStream;
        private bool _isDisposed;
        private long _size;
        private string _fileUrl;
        private DateTime _expirationTime;
        private IReadOnlyDictionary<string, string>? _httpHeaders;

        /// <summary>
        /// Initializes a new instance of the <see cref="DownloadResult"/> class.
        /// </summary>
        /// <param name="fileUrl">The URL for downloading the file.</param>
        /// <param name="startRowOffset">The starting row offset for this result chunk.</param>
        /// <param name="rowCount">The number of rows in this result chunk.</param>
        /// <param name="byteCount">The size in bytes of this result chunk.</param>
        /// <param name="expirationTime">The expiration time of the URL in UTC.</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <param name="httpHeaders">Optional HTTP headers for downloading the file.</param>
        public DownloadResult(
            string fileUrl,
            long startRowOffset,
            long rowCount,
            long byteCount,
            DateTime expirationTime,
            ICloudFetchMemoryBufferManager memoryManager,
            IReadOnlyDictionary<string, string>? httpHeaders = null)
        {
            _fileUrl = fileUrl ?? throw new ArgumentNullException(nameof(fileUrl));
            StartRowOffset = startRowOffset;
            RowCount = rowCount;
            ByteCount = byteCount;
            _expirationTime = expirationTime;
            _memoryManager = memoryManager ?? throw new ArgumentNullException(nameof(memoryManager));
            _httpHeaders = httpHeaders;
            _downloadCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _size = byteCount;
        }

        /// <summary>
        /// Creates a DownloadResult from a Thrift link for backward compatibility.
        /// </summary>
        /// <param name="link">The Thrift link information.</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <returns>A new DownloadResult instance.</returns>
        public static DownloadResult FromThriftLink(TSparkArrowResultLink link, ICloudFetchMemoryBufferManager memoryManager)
        {
            if (link == null) throw new ArgumentNullException(nameof(link));
            if (memoryManager == null) throw new ArgumentNullException(nameof(memoryManager));

            var expirationTime = DateTimeOffset.FromUnixTimeMilliseconds(link.ExpiryTime).UtcDateTime;

            return new DownloadResult(
                fileUrl: link.FileLink,
                startRowOffset: link.StartRowOffset,
                rowCount: link.RowCount,
                byteCount: link.BytesNum,
                expirationTime: expirationTime,
                memoryManager: memoryManager,
                httpHeaders: null);
        }

        /// <inheritdoc />
        public string FileUrl => _fileUrl;

        /// <inheritdoc />
        public long StartRowOffset { get; }

        /// <inheritdoc />
        public long RowCount { get; }

        /// <inheritdoc />
        public long ByteCount { get; }

        /// <inheritdoc />
        public DateTime ExpirationTime => _expirationTime;

        /// <inheritdoc />
        public IReadOnlyDictionary<string, string>? HttpHeaders => _httpHeaders;

        /// <inheritdoc />
        public Stream DataStream
        {
            get
            {
                ThrowIfDisposed();
                if (!IsCompleted)
                {
                    throw new InvalidOperationException("Download has not completed yet.");
                }
                return _dataStream!;
            }
        }

        /// <inheritdoc />
        public long Size => _size;

        /// <inheritdoc />
        public Task DownloadCompletedTask => _downloadCompletionSource.Task;

        /// <inheritdoc />
        public bool IsCompleted => _downloadCompletionSource.Task.IsCompleted && !_downloadCompletionSource.Task.IsFaulted;

        /// <summary>
        /// Gets the number of URL refresh attempts for this download.
        /// </summary>
        public int RefreshAttempts { get; private set; } = 0;

        /// <summary>
        /// Checks if the URL is expired or about to expire.
        /// </summary>
        /// <param name="expirationBufferSeconds">Buffer time in seconds before expiration to consider a URL as expiring soon.</param>
        /// <returns>True if the URL is expired or about to expire, false otherwise.</returns>
        public bool IsExpiredOrExpiringSoon(int expirationBufferSeconds = 60)
        {
            // Check if the URL is already expired or will expire soon
            return DateTime.UtcNow.AddSeconds(expirationBufferSeconds) >= _expirationTime;
        }

        /// <summary>
        /// Updates this download result with a refreshed URL and expiration time.
        /// </summary>
        /// <param name="fileUrl">The refreshed file URL.</param>
        /// <param name="expirationTime">The new expiration time.</param>
        /// <param name="httpHeaders">Optional HTTP headers for the refreshed URL.</param>
        public void UpdateWithRefreshedUrl(string fileUrl, DateTime expirationTime, IReadOnlyDictionary<string, string>? httpHeaders = null)
        {
            ThrowIfDisposed();
            _fileUrl = fileUrl ?? throw new ArgumentNullException(nameof(fileUrl));
            _expirationTime = expirationTime;
            _httpHeaders = httpHeaders;
            RefreshAttempts++;
        }

        /// <inheritdoc />
        public void SetCompleted(Stream dataStream, long size)
        {
            ThrowIfDisposed();
            _dataStream = dataStream ?? throw new ArgumentNullException(nameof(dataStream));
            _downloadCompletionSource.TrySetResult(true);
            _size = size;
        }

        /// <inheritdoc />
        public void SetFailed(Exception exception)
        {
            ThrowIfDisposed();
            _downloadCompletionSource.TrySetException(exception ?? throw new ArgumentNullException(nameof(exception)));
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_isDisposed)
            {
                return;
            }

            if (_dataStream != null)
            {
                _dataStream.Dispose();
                _dataStream = null;

                // Release memory back to the manager
                if (_size > 0)
                {
                    _memoryManager.ReleaseMemory(_size);
                }
            }

            // Ensure any waiting tasks are completed if not already
            if (!_downloadCompletionSource.Task.IsCompleted)
            {
                _downloadCompletionSource.TrySetCanceled();
            }

            _isDisposed = true;
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(DownloadResult));
            }
        }
    }
}
