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
using System.Collections.Generic;
using Apache.Arrow;
using Microsoft.IO;

namespace AdbcDrivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Configuration for the CloudFetch download pipeline.
    /// Protocol-agnostic - works with both Thrift and REST implementations.
    /// </summary>
    internal sealed class CloudFetchConfiguration
    {
        // Default values
        internal const int DefaultParallelDownloads = 3;
        internal const int DefaultPrefetchCount = 2;
        internal const int DefaultMemoryBufferSizeMB = 100;
        internal const int DefaultTimeoutMinutes = 5;
        internal const int DefaultMaxRetries = 3;
        internal const int DefaultRetryDelayMs = 500;
        internal const int DefaultMaxUrlRefreshAttempts = 3;
        internal const int DefaultUrlExpirationBufferSeconds = 60;

        /// <summary>
        /// Number of parallel downloads to perform.
        /// </summary>
        public int ParallelDownloads { get; set; } = DefaultParallelDownloads;

        /// <summary>
        /// Number of files to prefetch ahead of the reader.
        /// </summary>
        public int PrefetchCount { get; set; } = DefaultPrefetchCount;

        /// <summary>
        /// Memory buffer size limit in MB for buffered files.
        /// </summary>
        public int MemoryBufferSizeMB { get; set; } = DefaultMemoryBufferSizeMB;

        /// <summary>
        /// HTTP client timeout for downloads (in minutes).
        /// </summary>
        public int TimeoutMinutes { get; set; } = DefaultTimeoutMinutes;

        /// <summary>
        /// Maximum retry attempts for failed downloads.
        /// </summary>
        public int MaxRetries { get; set; } = DefaultMaxRetries;

        /// <summary>
        /// Delay between retry attempts (in milliseconds).
        /// </summary>
        public int RetryDelayMs { get; set; } = DefaultRetryDelayMs;

        /// <summary>
        /// Maximum attempts to refresh expired URLs.
        /// </summary>
        public int MaxUrlRefreshAttempts { get; set; } = DefaultMaxUrlRefreshAttempts;

        /// <summary>
        /// Buffer time before URL expiration to trigger refresh (in seconds).
        /// </summary>
        public int UrlExpirationBufferSeconds { get; set; } = DefaultUrlExpirationBufferSeconds;

        /// <summary>
        /// Whether the result data is LZ4 compressed.
        /// </summary>
        public bool IsLz4Compressed { get; set; }

        /// <summary>
        /// The Arrow schema for the results.
        /// </summary>
        public Schema Schema { get; set; }

        /// <summary>
        /// RecyclableMemoryStreamManager for LZ4 decompression output streams.
        /// If not provided, a new instance will be created when needed.
        /// For optimal memory pooling, this should be shared from the connection/database level.
        /// </summary>
        public RecyclableMemoryStreamManager? MemoryStreamManager { get; set; }

        /// <summary>
        /// ArrayPool for LZ4 decompression buffers.
        /// If not provided, ArrayPool&lt;byte&gt;.Shared will be used.
        /// For optimal memory pooling, this should be shared from the connection/database level.
        /// </summary>
        public ArrayPool<byte>? Lz4BufferPool { get; set; }

        /// <summary>
        /// Creates configuration from connection properties.
        /// Works with UNIFIED properties that are shared across ALL protocols (Thrift, REST, future protocols).
        /// Same property names (e.g., "adbc.databricks.cloudfetch.parallel_downloads") work for all protocols.
        /// </summary>
        /// <param name="properties">Connection properties from either Thrift or REST connection.</param>
        /// <param name="schema">Arrow schema for the results.</param>
        /// <param name="isLz4Compressed">Whether results are LZ4 compressed.</param>
        /// <returns>CloudFetch configuration parsed from unified properties.</returns>
        public static CloudFetchConfiguration FromProperties(
            IReadOnlyDictionary<string, string> properties,
            Schema schema,
            bool isLz4Compressed)
        {
            var config = new CloudFetchConfiguration
            {
                Schema = schema ?? throw new ArgumentNullException(nameof(schema)),
                IsLz4Compressed = isLz4Compressed,
                ParallelDownloads = PropertyHelper.GetPositiveIntPropertyWithValidation(properties, DatabricksParameters.CloudFetchParallelDownloads, DefaultParallelDownloads),
                PrefetchCount = PropertyHelper.GetPositiveIntPropertyWithValidation(properties, DatabricksParameters.CloudFetchPrefetchCount, DefaultPrefetchCount),
                MemoryBufferSizeMB = PropertyHelper.GetPositiveIntPropertyWithValidation(properties, DatabricksParameters.CloudFetchMemoryBufferSize, DefaultMemoryBufferSizeMB),
                TimeoutMinutes = PropertyHelper.GetPositiveIntPropertyWithValidation(properties, DatabricksParameters.CloudFetchTimeoutMinutes, DefaultTimeoutMinutes),
                MaxRetries = PropertyHelper.GetPositiveIntPropertyWithValidation(properties, DatabricksParameters.CloudFetchMaxRetries, DefaultMaxRetries),
                RetryDelayMs = PropertyHelper.GetPositiveIntPropertyWithValidation(properties, DatabricksParameters.CloudFetchRetryDelayMs, DefaultRetryDelayMs),
                MaxUrlRefreshAttempts = PropertyHelper.GetPositiveIntPropertyWithValidation(properties, DatabricksParameters.CloudFetchMaxUrlRefreshAttempts, DefaultMaxUrlRefreshAttempts),
                UrlExpirationBufferSeconds = PropertyHelper.GetPositiveIntPropertyWithValidation(properties, DatabricksParameters.CloudFetchUrlExpirationBufferSeconds, DefaultUrlExpirationBufferSeconds)
            };

            return config;
        }
    }
}
