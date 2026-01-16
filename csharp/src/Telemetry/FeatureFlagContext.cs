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

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Holds feature flag state and reference count for a host.
    /// Tracks when feature flags were last fetched and whether they have expired.
    /// </summary>
    internal sealed class FeatureFlagContext
    {
        /// <summary>
        /// Gets or sets whether telemetry is enabled for this host.
        /// Null indicates the feature flag has not been fetched yet.
        /// </summary>
        public bool? TelemetryEnabled { get; set; }

        /// <summary>
        /// Gets or sets the timestamp when the feature flag was last fetched.
        /// Null indicates the feature flag has never been fetched.
        /// </summary>
        public DateTime? LastFetched { get; set; }

        /// <summary>
        /// Gets or sets the reference count for this context.
        /// Incremented when a connection is opened, decremented when closed.
        /// When reference count reaches zero, the context is removed from the cache.
        /// </summary>
        public int RefCount { get; set; }

        /// <summary>
        /// Gets the cache duration for feature flag values.
        /// Default is 15 minutes as specified in the design document.
        /// </summary>
        public TimeSpan CacheDuration { get; } = TimeSpan.FromMinutes(15);

        /// <summary>
        /// Gets a value indicating whether the cached feature flag value has expired.
        /// Returns true if the feature flag has never been fetched or if the cache duration has elapsed.
        /// </summary>
        public bool IsExpired => LastFetched == null ||
            DateTime.UtcNow - LastFetched.Value > CacheDuration;
    }
}
