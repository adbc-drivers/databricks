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
using System.Text.Json.Serialization;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Response model for the feature flags API.
    /// Maps to the JSON response from /api/2.0/connector-service/feature-flags/{driver_type}/{version}.
    /// </summary>
    internal sealed class FeatureFlagsResponse
    {
        /// <summary>
        /// Array of feature flag entries with name and value.
        /// </summary>
        [JsonPropertyName("flags")]
        public IReadOnlyList<FeatureFlagEntry>? Flags { get; set; }

        /// <summary>
        /// Server-controlled refresh interval in seconds.
        /// Default is 900 (15 minutes) if not provided.
        /// </summary>
        [JsonPropertyName("ttl_seconds")]
        public int? TtlSeconds { get; set; }
    }

    /// <summary>
    /// Individual feature flag entry with name and value.
    /// </summary>
    internal sealed class FeatureFlagEntry
    {
        /// <summary>
        /// The feature flag name (e.g., "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForAdbc").
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// The feature flag value as a string (e.g., "true", "false", "10").
        /// </summary>
        [JsonPropertyName("value")]
        public string Value { get; set; } = string.Empty;
    }
}
