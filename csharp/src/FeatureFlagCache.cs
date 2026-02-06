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
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Microsoft.Extensions.Caching.Memory;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Singleton that manages feature flag cache per host using IMemoryCache.
    /// Prevents rate limiting by caching feature flag responses with TTL-based expiration.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This class implements a per-host caching pattern:
    /// - Feature flags are cached by host to prevent rate limiting
    /// - TTL-based expiration using server's ttl_seconds
    /// - Background refresh task runs based on TTL within each FeatureFlagContext
    /// - Automatic cleanup via IMemoryCache eviction
    /// - Thread-safe using IMemoryCache and SemaphoreSlim for async locking
    /// </para>
    /// <para>
    /// JDBC Reference: DatabricksDriverFeatureFlagsContextFactory.java
    /// </para>
    /// </remarks>
    internal sealed class FeatureFlagCache : IDisposable
    {
        private static readonly FeatureFlagCache s_instance = new FeatureFlagCache();

        /// <summary>
        /// Activity source for feature flag tracing.
        /// </summary>
        private static readonly ActivitySource s_activitySource = new ActivitySource("AdbcDrivers.Databricks.FeatureFlagCache");

        private readonly IMemoryCache _cache;
        private readonly SemaphoreSlim _createLock = new SemaphoreSlim(1, 1);
        private bool _disposed;

        /// <summary>
        /// Gets the singleton instance of the FeatureFlagCache.
        /// </summary>
        public static FeatureFlagCache GetInstance() => s_instance;

        /// <summary>
        /// Creates a new FeatureFlagCache with default MemoryCache.
        /// </summary>
        internal FeatureFlagCache() : this(new MemoryCache(new MemoryCacheOptions()))
        {
        }

        /// <summary>
        /// Creates a new FeatureFlagCache with custom IMemoryCache (for testing).
        /// </summary>
        /// <param name="cache">The memory cache to use.</param>
        internal FeatureFlagCache(IMemoryCache cache)
        {
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));
        }

        /// <summary>
        /// Gets or creates a feature flag context for the host asynchronously.
        /// Waits for initial fetch to complete before returning.
        /// </summary>
        /// <param name="host">The host (Databricks workspace URL) to get or create a context for.</param>
        /// <param name="httpClient">
        /// HttpClient from the connection, pre-configured with:
        /// - Base address (https://{host})
        /// - Auth headers (Bearer token)
        /// - Custom User-Agent for connector service
        /// </param>
        /// <param name="driverVersion">The driver version for the API endpoint.</param>
        /// <param name="endpointFormat">Optional custom endpoint format. If null, uses the default endpoint.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The feature flag context for the host.</returns>
        /// <exception cref="ArgumentException">Thrown when host is null or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown when httpClient is null.</exception>
        public async Task<FeatureFlagContext> GetOrCreateContextAsync(
            string host,
            HttpClient httpClient,
            string driverVersion,
            string? endpointFormat = null,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            if (httpClient == null)
            {
                throw new ArgumentNullException(nameof(httpClient));
            }

            var cacheKey = GetCacheKey(host);

            // Try to get existing context (fast path)
            if (_cache.TryGetValue(cacheKey, out FeatureFlagContext? context) && context != null)
            {
                Activity.Current?.AddEvent(new ActivityEvent("feature_flags.cache_hit",
                    tags: new ActivityTagsCollection { { "host", host } }));

                return context;
            }

            // Cache miss - create new context with async lock to prevent duplicate creation
            await _createLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                // Double-check after acquiring lock
                if (_cache.TryGetValue(cacheKey, out context) && context != null)
                {
                    return context;
                }

                // Create context asynchronously - this waits for initial fetch to complete
                context = await FeatureFlagContext.CreateAsync(
                    host,
                    httpClient,
                    driverVersion,
                    endpointFormat,
                    cancellationToken).ConfigureAwait(false);

                // Set cache options with TTL from context
                var cacheOptions = new MemoryCacheEntryOptions()
                    .SetAbsoluteExpiration(context.Ttl)
                    .RegisterPostEvictionCallback(OnCacheEntryEvicted);

                _cache.Set(cacheKey, context, cacheOptions);

                Activity.Current?.AddEvent(new ActivityEvent("feature_flags.context_created",
                    tags: new ActivityTagsCollection
                    {
                        { "host", host },
                        { "ttl_seconds", context.Ttl.TotalSeconds }
                    }));

                return context;
            }
            finally
            {
                _createLock.Release();
            }
        }

        /// <summary>
        /// Synchronous wrapper for GetOrCreateContextAsync.
        /// Used for backward compatibility with synchronous callers.
        /// </summary>
        public FeatureFlagContext GetOrCreateContext(
            string host,
            HttpClient httpClient,
            string driverVersion,
            string? endpointFormat = null)
        {
            return GetOrCreateContextAsync(host, httpClient, driverVersion, endpointFormat)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Callback invoked when a cache entry is evicted.
        /// Disposes the context to clean up resources (stops background refresh task).
        /// </summary>
        private static void OnCacheEntryEvicted(object key, object? value, EvictionReason reason, object? state)
        {
            if (value is FeatureFlagContext context)
            {
                context.Dispose();

                Activity.Current?.AddEvent(new ActivityEvent("feature_flags.context_evicted",
                    tags: new ActivityTagsCollection
                    {
                        { "host", context.Host },
                        { "reason", reason.ToString() }
                    }));
            }
        }

        /// <summary>
        /// Gets the cache key for a host.
        /// </summary>
        private static string GetCacheKey(string host)
        {
            return $"feature_flags:{host.ToLowerInvariant()}";
        }

        /// <summary>
        /// Gets the number of hosts currently cached.
        /// Note: IMemoryCache doesn't expose count directly, so this returns -1 if not available.
        /// </summary>
        internal int CachedHostCount
        {
            get
            {
                if (_cache is MemoryCache memoryCache)
                {
                    return memoryCache.Count;
                }
                return -1;
            }
        }

        /// <summary>
        /// Checks if a context exists for the specified host.
        /// </summary>
        /// <param name="host">The host to check.</param>
        /// <returns>True if a context exists, false otherwise.</returns>
        internal bool HasContext(string host)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                return false;
            }

            return _cache.TryGetValue(GetCacheKey(host), out _);
        }

        /// <summary>
        /// Gets the context for the specified host, if it exists.
        /// Does not create a new context.
        /// </summary>
        /// <param name="host">The host to get the context for.</param>
        /// <param name="context">The context if found, null otherwise.</param>
        /// <returns>True if the context was found, false otherwise.</returns>
        internal bool TryGetContext(string host, out FeatureFlagContext? context)
        {
            context = null;

            if (string.IsNullOrWhiteSpace(host))
            {
                return false;
            }

            return _cache.TryGetValue(GetCacheKey(host), out context);
        }

        /// <summary>
        /// Removes a context from the cache.
        /// </summary>
        /// <param name="host">The host to remove.</param>
        internal void RemoveContext(string host)
        {
            if (!string.IsNullOrWhiteSpace(host))
            {
                _cache.Remove(GetCacheKey(host));
            }
        }

        /// <summary>
        /// Clears all cached contexts.
        /// This is primarily for testing purposes.
        /// </summary>
        internal void Clear()
        {
            if (_cache is MemoryCache memoryCache)
            {
                memoryCache.Compact(1.0); // Remove all entries
            }
        }

        /// <summary>
        /// Disposes the cache and all cached contexts.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            _createLock.Dispose();

            if (_cache is IDisposable disposableCache)
            {
                disposableCache.Dispose();
            }
        }

        /// <summary>
        /// Merges feature flags from server into properties asynchronously.
        /// Feature flags (remote properties) have lower priority than user-specified properties (local properties).
        /// Priority: Local Properties > Remote Properties (Feature Flags) > Driver Defaults
        /// </summary>
        /// <param name="localProperties">Local properties from user configuration and environment.</param>
        /// <param name="assemblyVersion">The driver version for the API endpoint.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Properties with remote feature flags merged in (local properties take precedence).</returns>
        public async Task<IReadOnlyDictionary<string, string>> MergePropertiesWithFeatureFlagsAsync(
            IReadOnlyDictionary<string, string> localProperties,
            string assemblyVersion,
            CancellationToken cancellationToken = default)
        {
            using var activity = s_activitySource.StartActivity("MergePropertiesWithFeatureFlags");

            try
            {
                // Extract host from local properties
                var host = TryGetHost(localProperties);
                if (string.IsNullOrEmpty(host))
                {
                    activity?.AddEvent(new ActivityEvent("feature_flags.skipped",
                        tags: new ActivityTagsCollection { { "reason", "no_host" } }));
                    return localProperties;
                }

                activity?.SetTag("feature_flags.host", host);

                // Create HttpClient for feature flag API
                using var httpClient = CreateFeatureFlagHttpClient(host, assemblyVersion, localProperties);

                if (httpClient == null)
                {
                    activity?.AddEvent(new ActivityEvent("feature_flags.skipped",
                        tags: new ActivityTagsCollection { { "reason", "no_auth_credentials" } }));
                    return localProperties;
                }

                // Get or create feature flag context asynchronously (waits for initial fetch)
                var context = await GetOrCreateContextAsync(host, httpClient, assemblyVersion, cancellationToken: cancellationToken).ConfigureAwait(false);

                // Get all flags from cache (remote properties)
                var remoteProperties = context.GetAllFlags();

                if (remoteProperties.Count == 0)
                {
                    activity?.AddEvent(new ActivityEvent("feature_flags.skipped",
                        tags: new ActivityTagsCollection { { "reason", "no_flags_returned" } }));
                    return localProperties;
                }

                activity?.SetTag("feature_flags.count", remoteProperties.Count);
                activity?.AddEvent(new ActivityEvent("feature_flags.merging",
                    tags: new ActivityTagsCollection { { "flags_count", remoteProperties.Count } }));

                // Merge: remote properties (feature flags) as base, local properties override
                // This ensures local properties take precedence over remote flags
                return MergeProperties(remoteProperties, localProperties);
            }
            catch (Exception ex)
            {
                // Feature flag failures should never break the connection
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity?.AddEvent(new ActivityEvent("feature_flags.error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.type", ex.GetType().Name },
                        { "error.message", ex.Message }
                    }));
                return localProperties;
            }
        }

        /// <summary>
        /// Synchronous wrapper for MergePropertiesWithFeatureFlagsAsync.
        /// Used for backward compatibility with synchronous callers.
        /// </summary>
        public IReadOnlyDictionary<string, string> MergePropertiesWithFeatureFlags(
            IReadOnlyDictionary<string, string> localProperties,
            string assemblyVersion)
        {
            return MergePropertiesWithFeatureFlagsAsync(localProperties, assemblyVersion)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Tries to extract the host from properties without throwing.
        /// Handles cases where user puts protocol in host (e.g., "https://myhost.databricks.com").
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <returns>The host (without protocol), or null if not found.</returns>
        internal static string? TryGetHost(IReadOnlyDictionary<string, string> properties)
        {
            if (properties.TryGetValue(SparkParameters.HostName, out string? host) && !string.IsNullOrEmpty(host))
            {
                // Handle case where user puts protocol in host
                return StripProtocol(host);
            }

            if (properties.TryGetValue(Apache.Arrow.Adbc.AdbcOptions.Uri, out string? uri) && !string.IsNullOrEmpty(uri))
            {
                if (Uri.TryCreate(uri, UriKind.Absolute, out Uri? parsedUri))
                {
                    return parsedUri.Host;
                }
            }

            return null;
        }

        /// <summary>
        /// Strips protocol prefix from a host string if present.
        /// </summary>
        /// <param name="host">The host string that may contain a protocol.</param>
        /// <returns>The host without protocol prefix.</returns>
        private static string StripProtocol(string host)
        {
            // Try to parse as URI first to handle full URLs
            if (Uri.TryCreate(host, UriKind.Absolute, out Uri? parsedUri) &&
                (parsedUri.Scheme == Uri.UriSchemeHttp || parsedUri.Scheme == Uri.UriSchemeHttps))
            {
                return parsedUri.Host;
            }

            // Fallback: strip common protocol prefixes manually
            if (host.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            {
                return host.Substring(8);
            }
            if (host.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            {
                return host.Substring(7);
            }

            return host;
        }

        /// <summary>
        /// Creates an HttpClient configured for the feature flag API.
        /// Supports all authentication methods including:
        /// - PAT (Personal Access Token)
        /// - OAuth M2M (client_credentials)
        /// - OAuth U2M (access_token)
        /// - Workload Identity Federation (via MandatoryTokenExchangeDelegatingHandler)
        /// Respects proxy settings and TLS options from connection properties.
        /// </summary>
        /// <param name="host">The Databricks host (without protocol).</param>
        /// <param name="assemblyVersion">The driver version for the User-Agent.</param>
        /// <param name="properties">Connection properties for proxy, TLS, and auth configuration.</param>
        /// <returns>Configured HttpClient, or null if no valid authentication is available.</returns>
        private static HttpClient? CreateFeatureFlagHttpClient(
            string host,
            string assemblyVersion,
            IReadOnlyDictionary<string, string> properties)
        {
            // Use centralized factory to create HttpClient with full auth handler chain
            // This properly supports Workload Identity Federation via MandatoryTokenExchangeDelegatingHandler
            return Http.HttpClientFactory.CreateFeatureFlagHttpClient(properties, host, assemblyVersion);
        }

        /// <summary>
        /// Merges two property dictionaries. Additional properties override base properties.
        /// </summary>
        /// <param name="baseProperties">Base properties (lower priority).</param>
        /// <param name="additionalProperties">Additional properties (higher priority).</param>
        /// <returns>Merged properties.</returns>
        private static IReadOnlyDictionary<string, string> MergeProperties(
            IReadOnlyDictionary<string, string> baseProperties,
            IReadOnlyDictionary<string, string> additionalProperties)
        {
            var merged = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Add base properties first
            foreach (var kvp in baseProperties)
            {
                merged[kvp.Key] = kvp.Value;
            }

            // Additional properties override base properties
            foreach (var kvp in additionalProperties)
            {
                merged[kvp.Key] = kvp.Value;
            }

            return merged;
        }
    }
}
