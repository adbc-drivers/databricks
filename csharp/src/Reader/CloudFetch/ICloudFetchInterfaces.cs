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
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Represents a downloaded result file with its associated metadata.
    /// Protocol-agnostic interface that works with both Thrift and REST APIs.
    /// </summary>
    internal interface IDownloadResult : IDisposable
    {
        /// <summary>
        /// Gets the chunk index for this download result.
        /// Used for targeted URL refresh in REST API.
        /// </summary>
        long ChunkIndex { get; }

        /// <summary>
        /// Gets the URL for downloading the file.
        /// </summary>
        string FileUrl { get; }

        /// <summary>
        /// Gets the starting row offset for this result chunk.
        /// </summary>
        long StartRowOffset { get; }

        /// <summary>
        /// Gets the number of rows in this result chunk.
        /// </summary>
        long RowCount { get; }

        /// <summary>
        /// Gets the size in bytes of this result chunk.
        /// </summary>
        long ByteCount { get; }

        /// <summary>
        /// Gets the expiration time of the URL in UTC.
        /// </summary>
        DateTime ExpirationTime { get; }

        /// <summary>
        /// Gets optional HTTP headers to include when downloading the file.
        /// Used for authentication or other custom headers required by the download endpoint.
        /// </summary>
        IReadOnlyDictionary<string, string>? HttpHeaders { get; }

        /// <summary>
        /// Gets the stream containing the downloaded data.
        /// </summary>
        Stream DataStream { get; }

        /// <summary>
        /// Gets the size of the downloaded data in bytes.
        /// </summary>
        long Size { get; }

        /// <summary>
        /// Gets a task that completes when the download is finished.
        /// </summary>
        Task DownloadCompletedTask { get; }

        /// <summary>
        /// Gets a value indicating whether the download has completed.
        /// </summary>
        bool IsCompleted { get; }

        /// <summary>
        /// Gets the number of URL refresh attempts for this download.
        /// </summary>
        int RefreshAttempts { get; }

        /// <summary>
        /// Sets the download as completed with the provided data stream.
        /// </summary>
        /// <param name="dataStream">The stream containing the downloaded data.</param>
        /// <param name="size">The size of the downloaded data in bytes.</param>
        void SetCompleted(Stream dataStream, long size);

        /// <summary>
        /// Sets the download as failed with the specified exception.
        /// </summary>
        /// <param name="exception">The exception that caused the failure.</param>
        void SetFailed(Exception exception);

        /// <summary>
        /// Updates this download result with a refreshed URL and expiration time.
        /// </summary>
        /// <param name="fileUrl">The refreshed file URL.</param>
        /// <param name="expirationTime">The new expiration time.</param>
        /// <param name="httpHeaders">Optional HTTP headers for the refreshed URL.</param>
        void UpdateWithRefreshedUrl(string fileUrl, DateTime expirationTime, IReadOnlyDictionary<string, string>? httpHeaders = null);

        /// <summary>
        /// Checks if the URL is expired or about to expire.
        /// </summary>
        /// <param name="expirationBufferSeconds">Buffer time in seconds before expiration to consider a URL as expiring soon.</param>
        /// <returns>True if the URL is expired or about to expire, false otherwise.</returns>
        bool IsExpiredOrExpiringSoon(int expirationBufferSeconds = 60);
    }

    /// <summary>
    /// Manages memory allocation for prefetched files.
    /// </summary>
    internal interface ICloudFetchMemoryBufferManager
    {
        /// <summary>
        /// Gets the maximum memory allowed for buffering in bytes.
        /// </summary>
        long MaxMemory { get; }

        /// <summary>
        /// Gets the currently used memory in bytes.
        /// </summary>
        long UsedMemory { get; }

        /// <summary>
        /// Tries to acquire memory for a download without blocking.
        /// </summary>
        /// <param name="size">The size in bytes to acquire.</param>
        /// <returns>True if memory was successfully acquired, false otherwise.</returns>
        bool TryAcquireMemory(long size);

        /// <summary>
        /// Acquires memory for a download, blocking until memory is available.
        /// </summary>
        /// <param name="size">The size in bytes to acquire.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task AcquireMemoryAsync(long size, CancellationToken cancellationToken);

        /// <summary>
        /// Releases previously acquired memory.
        /// </summary>
        /// <param name="size">The size in bytes to release.</param>
        void ReleaseMemory(long size);
    }

    /// <summary>
    /// Fetches result chunks from the server (Thrift or REST).
    /// </summary>
    internal interface ICloudFetchResultFetcher
    {
        /// <summary>
        /// Starts the result fetcher.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task StartAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Stops the result fetcher.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task StopAsync();

        /// <summary>
        /// Gets a value indicating whether there are more results available.
        /// </summary>
        bool HasMoreResults { get; }

        /// <summary>
        /// Gets a value indicating whether the fetcher has completed fetching all results.
        /// </summary>
        bool IsCompleted { get; }

        /// <summary>
        /// Gets a value indicating whether the fetcher encountered an error.
        /// </summary>
        bool HasError { get; }

        /// <summary>
        /// Gets the error encountered by the fetcher, if any.
        /// </summary>
        Exception? Error { get; }

        /// <summary>
        /// Re-fetches URLs for chunks starting from the specified row offset.
        /// Used when URLs expire before download completes.
        /// </summary>
        /// <param name="startRowOffset">The starting row offset to fetch from.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A collection of download results with refreshed URLs.</returns>
        Task<IEnumerable<IDownloadResult>> RefreshUrlsAsync(long startRowOffset, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Downloads files from URLs.
    /// </summary>
    internal interface ICloudFetchDownloader
    {
        /// <summary>
        /// Starts the downloader.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task StartAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Stops the downloader.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task StopAsync();

        /// <summary>
        /// Gets the next downloaded file.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The next downloaded file, or null if there are no more files.</returns>
        Task<IDownloadResult?> GetNextDownloadedFileAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Gets a value indicating whether the downloader has completed all downloads.
        /// </summary>
        bool IsCompleted { get; }

        /// <summary>
        /// Gets a value indicating whether the downloader encountered an error.
        /// </summary>
        bool HasError { get; }

        /// <summary>
        /// Gets the error encountered by the downloader, if any.
        /// </summary>
        Exception? Error { get; }
    }

    /// <summary>
    /// Manages the CloudFetch download pipeline.
    /// </summary>
    internal interface ICloudFetchDownloadManager : IDisposable
    {
        /// <summary>
        /// Gets the next downloaded file.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The next downloaded file, or null if there are no more files.</returns>
        Task<IDownloadResult?> GetNextDownloadedFileAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Starts the download manager.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task StartAsync();

        /// <summary>
        /// Stops the download manager.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task StopAsync();

        /// <summary>
        /// Gets a value indicating whether there are more results available.
        /// </summary>
        bool HasMoreResults { get; }
    }
}
