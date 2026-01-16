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
using System.Collections.Concurrent;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Singleton that manages feature flag cache per host.
    /// Prevents rate limiting by caching feature flag responses.
    /// Thread-safe using ConcurrentDictionary for concurrent access from multiple connections.
    /// </summary>
    internal sealed class FeatureFlagCache
    {
        private static readonly FeatureFlagCache Instance = new();
        private readonly ConcurrentDictionary<string, FeatureFlagContext> _cache = new();

        private FeatureFlagCache()
        {
        }

        /// <summary>
        /// Gets the singleton instance of the FeatureFlagCache.
        /// </summary>
        /// <returns>The singleton FeatureFlagCache instance.</returns>
        public static FeatureFlagCache GetInstance() => Instance;

        /// <summary>
        /// Gets or creates a feature flag context for the host.
        /// Atomically increments the reference count.
        /// </summary>
        /// <param name="host">The Databricks host (e.g., "host.cloud.databricks.com").</param>
        /// <returns>The FeatureFlagContext for the host.</returns>
        public FeatureFlagContext GetOrCreateContext(string host)
        {
            if (string.IsNullOrEmpty(host))
            {
                throw new ArgumentNullException(nameof(host));
            }

            return _cache.AddOrUpdate(
                host,
                // Create new context with RefCount=1
                _ => new FeatureFlagContext { RefCount = 1 },
                // Increment existing context's RefCount
                (_, existingContext) =>
                {
                    Interlocked.Increment(ref existingContext.RefCount);
                    return existingContext;
                });
        }

        /// <summary>
        /// Decrements the reference count for the host.
        /// Removes the context when reference count reaches zero.
        /// </summary>
        /// <param name="host">The Databricks host.</param>
        public void ReleaseContext(string host)
        {
            if (string.IsNullOrEmpty(host))
            {
                return;
            }

            if (_cache.TryGetValue(host, out var context))
            {
                var newRefCount = Interlocked.Decrement(ref context.RefCount);
                if (newRefCount <= 0)
                {
                    // Remove from cache when last reference is released
                    _cache.TryRemove(host, out _);
                }
            }
        }

        /// <summary>
        /// Checks if telemetry is enabled for the host.
        /// Uses cached value if available and not expired.
        /// Fetches from server if cache is expired or missing.
        /// </summary>
        /// <param name="host">The Databricks host.</param>
        /// <param name="httpClient">The HttpClient for making API calls.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>True if telemetry is enabled; false otherwise.</returns>
        public async Task<bool> IsTelemetryEnabledAsync(
            string host,
            HttpClient httpClient,
            CancellationToken ct = default)
        {
            if (string.IsNullOrEmpty(host))
            {
                return false;
            }

            if (httpClient == null)
            {
                throw new ArgumentNullException(nameof(httpClient));
            }

            // Try to get existing context
            if (_cache.TryGetValue(host, out var context))
            {
                // Use cached value if not expired
                if (!context.IsExpired && context.TelemetryEnabled.HasValue)
                {
                    return context.TelemetryEnabled.Value;
                }
            }

            // Cache expired or missing - fetch from server
            try
            {
                var enabled = await FetchFeatureFlagAsync(host, httpClient, ct);

                // Update context with new value
                if (context != null)
                {
                    context.TelemetryEnabled = enabled;
                    context.LastFetched = DateTime.UtcNow;
                }

                return enabled;
            }
            catch (Exception)
            {
                // On error, default to false (telemetry disabled)
                // All exceptions swallowed per design requirement
                return false;
            }
        }

        /// <summary>
        /// Fetches the feature flag value from the Databricks feature flag endpoint.
        /// </summary>
        /// <param name="host">The Databricks host.</param>
        /// <param name="httpClient">The HttpClient for making API calls.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>True if telemetry is enabled; false otherwise.</returns>
        private async Task<bool> FetchFeatureFlagAsync(
            string host,
            HttpClient httpClient,
            CancellationToken ct = default)
        {
            // Build the feature flag URL
            // Format: https://host/api/2.0/feature-flags?flag=databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForAdbc
            var url = $"https://{host}/api/2.0/feature-flags?flag={TelemetryConfiguration.FeatureFlagName}";

            try
            {
                var response = await httpClient.GetAsync(url, ct);

                // If request fails, default to disabled
                if (!response.IsSuccessStatusCode)
                {
                    return false;
                }

                var responseBody = await response.Content.ReadAsStringAsync();

                // Parse JSON response
                // Expected format: { "flag": "...", "enabled": true/false }
                using var doc = JsonDocument.Parse(responseBody);
                if (doc.RootElement.TryGetProperty("enabled", out var enabledElement))
                {
                    return enabledElement.GetBoolean();
                }

                // If enabled field not found, default to false
                return false;
            }
            catch (Exception)
            {
                // All exceptions swallowed, default to disabled
                return false;
            }
        }
    }
}
