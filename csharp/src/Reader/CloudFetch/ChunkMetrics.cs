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

namespace AdbcDrivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Aggregated metrics for CloudFetch chunk downloads.
    /// Tracks timing and count metrics across all chunks in a result set.
    /// </summary>
    internal sealed class ChunkMetrics
    {
        /// <summary>
        /// Gets or sets the total number of chunks present in the result.
        /// This represents the total number of download links provided by the server.
        /// </summary>
        public int TotalChunksPresent { get; internal set; }

        /// <summary>
        /// Gets the number of chunks actually iterated by the client.
        /// This may be less than TotalChunksPresent if the client stops reading early.
        /// </summary>
        public int TotalChunksIterated { get; internal set; }

        /// <summary>
        /// Gets the time taken to download the first chunk in milliseconds.
        /// Represents the initial latency before the first data is available to the client.
        /// </summary>
        public long InitialChunkLatencyMs { get; internal set; }

        /// <summary>
        /// Gets the maximum time taken to download any single chunk in milliseconds.
        /// Identifies the slowest chunk download, useful for identifying performance outliers.
        /// </summary>
        public long SlowestChunkLatencyMs { get; internal set; }

        /// <summary>
        /// Gets the sum of download times for all chunks in milliseconds.
        /// This is the total time spent downloading (excluding parallel overlap).
        /// </summary>
        public long SumChunksDownloadTimeMs { get; internal set; }
    }
}
