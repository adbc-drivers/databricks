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

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Configuration for the CloudFetch download pipeline.
    /// Protocol-agnostic - works with both Thrift and REST implementations.
    /// </summary>
    internal sealed class CloudFetchConfiguration
    {
        // Default values
        private const int DefaultParallelDownloads = 3;
        private const int DefaultPrefetchCount = 2;
        private const int DefaultMemoryBufferSizeMB = 200;
        private const int DefaultTimeoutMinutes = 5;
        private const int DefaultMaxRetries = 3;
        private const int DefaultRetryDelayMs = 500;
        private const int DefaultMaxUrlRefreshAttempts = 3;
        private const int DefaultUrlExpirationBufferSeconds = 60;

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
                IsLz4Compressed = isLz4Compressed
            };

            // Parse parallel downloads
            if (properties.TryGetValue(DatabricksParameters.CloudFetchParallelDownloads, out string? parallelStr))
            {
                if (int.TryParse(parallelStr, out int parallel) && parallel > 0)
                    config.ParallelDownloads = parallel;
                else
                    throw new ArgumentException($"Invalid {DatabricksParameters.CloudFetchParallelDownloads}: {parallelStr}. Expected a positive integer.");
            }

            // Parse prefetch count
            if (properties.TryGetValue(DatabricksParameters.CloudFetchPrefetchCount, out string? prefetchStr))
            {
                if (int.TryParse(prefetchStr, out int prefetch) && prefetch > 0)
                    config.PrefetchCount = prefetch;
                else
                    throw new ArgumentException($"Invalid {DatabricksParameters.CloudFetchPrefetchCount}: {prefetchStr}. Expected a positive integer.");
            }

            // Parse memory buffer size
            if (properties.TryGetValue(DatabricksParameters.CloudFetchMemoryBufferSize, out string? memoryStr))
            {
                if (int.TryParse(memoryStr, out int memory) && memory > 0)
                    config.MemoryBufferSizeMB = memory;
                else
                    throw new ArgumentException($"Invalid {DatabricksParameters.CloudFetchMemoryBufferSize}: {memoryStr}. Expected a positive integer.");
            }

            // Parse timeout
            if (properties.TryGetValue(DatabricksParameters.CloudFetchTimeoutMinutes, out string? timeoutStr))
            {
                if (int.TryParse(timeoutStr, out int timeout) && timeout > 0)
                    config.TimeoutMinutes = timeout;
                else
                    throw new ArgumentException($"Invalid {DatabricksParameters.CloudFetchTimeoutMinutes}: {timeoutStr}. Expected a positive integer.");
            }

            // Parse max retries
            if (properties.TryGetValue(DatabricksParameters.CloudFetchMaxRetries, out string? retriesStr))
            {
                if (int.TryParse(retriesStr, out int retries) && retries > 0)
                    config.MaxRetries = retries;
                else
                    throw new ArgumentException($"Invalid {DatabricksParameters.CloudFetchMaxRetries}: {retriesStr}. Expected a positive integer.");
            }

            // Parse retry delay
            if (properties.TryGetValue(DatabricksParameters.CloudFetchRetryDelayMs, out string? retryDelayStr))
            {
                if (int.TryParse(retryDelayStr, out int retryDelay) && retryDelay > 0)
                    config.RetryDelayMs = retryDelay;
                else
                    throw new ArgumentException($"Invalid {DatabricksParameters.CloudFetchRetryDelayMs}: {retryDelayStr}. Expected a positive integer.");
            }

            // Parse max URL refresh attempts
            if (properties.TryGetValue(DatabricksParameters.CloudFetchMaxUrlRefreshAttempts, out string? maxUrlRefreshStr))
            {
                if (int.TryParse(maxUrlRefreshStr, out int maxUrlRefresh) && maxUrlRefresh > 0)
                    config.MaxUrlRefreshAttempts = maxUrlRefresh;
                else
                    throw new ArgumentException($"Invalid {DatabricksParameters.CloudFetchMaxUrlRefreshAttempts}: {maxUrlRefreshStr}. Expected a positive integer.");
            }

            // Parse URL expiration buffer
            if (properties.TryGetValue(DatabricksParameters.CloudFetchUrlExpirationBufferSeconds, out string? urlExpirationStr))
            {
                if (int.TryParse(urlExpirationStr, out int urlExpiration) && urlExpiration > 0)
                    config.UrlExpirationBufferSeconds = urlExpiration;
                else
                    throw new ArgumentException($"Invalid {DatabricksParameters.CloudFetchUrlExpirationBufferSeconds}: {urlExpirationStr}. Expected a positive integer.");
            }

            return config;
        }
    }
}
