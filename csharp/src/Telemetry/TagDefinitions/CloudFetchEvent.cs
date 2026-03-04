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

using System.Collections.Generic;

namespace AdbcDrivers.Databricks.Telemetry.TagDefinitions
{
    /// <summary>
    /// Tag definitions for CloudFetch download events.
    /// </summary>
    public static class CloudFetchEvent
    {
        /// <summary>
        /// The event name for the CloudFetch download summary.
        /// </summary>
        public const string DownloadSummaryEventName = "cloudfetch.download_summary";

        /// <summary>
        /// Total number of files downloaded via CloudFetch.
        /// </summary>
        [TelemetryTag("total_files",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Total CloudFetch files")]
        public const string TotalFiles = "total_files";

        /// <summary>
        /// Number of successful CloudFetch downloads.
        /// </summary>
        [TelemetryTag("successful_downloads",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Successful CloudFetch downloads")]
        public const string SuccessfulDownloads = "successful_downloads";

        /// <summary>
        /// Total CloudFetch download time in milliseconds.
        /// </summary>
        [TelemetryTag("total_time_ms",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Total download time in ms")]
        public const string TotalTimeMs = "total_time_ms";

        /// <summary>
        /// Latency of the initial chunk download in milliseconds.
        /// </summary>
        [TelemetryTag("initial_chunk_latency_ms",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Initial chunk download latency in ms")]
        public const string InitialChunkLatencyMs = "initial_chunk_latency_ms";

        /// <summary>
        /// Latency of the slowest chunk download in milliseconds.
        /// </summary>
        [TelemetryTag("slowest_chunk_latency_ms",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Slowest chunk download latency in ms")]
        public const string SlowestChunkLatencyMs = "slowest_chunk_latency_ms";

        /// <summary>
        /// Gets all tag names for the download summary event.
        /// </summary>
        /// <returns>A set of tag names for CloudFetch export.</returns>
        public static HashSet<string> GetDatabricksExportTags()
        {
            return new HashSet<string>
            {
                TotalFiles,
                SuccessfulDownloads,
                TotalTimeMs,
                InitialChunkLatencyMs,
                SlowestChunkLatencyMs
            };
        }
    }
}
