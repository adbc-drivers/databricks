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
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Net.Http.Headers;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Singleton that manages feature flag cache per host.
    /// Prevents rate limiting by caching feature flag responses.
    /// This is a generic cache for all feature flags, not just telemetry.
    /// </summary>
    /// <remarks>
    /// This class implements the per-host caching pattern from the JDBC driver:
    /// - Feature flags are cached by host to prevent rate limiting
    /// - Reference counting tracks number of connections per host
    /// - Cache is automatically cleaned up when all connections to a host close
    /// - Thread-safe using ConcurrentDictionary
    ///
    /// JDBC Reference: DatabricksDriverFeatureFlagsContextFactory.java
    /// </remarks>
    internal sealed class FeatureFlagCache
    {
        private static readonly FeatureFlagCache s_instance = new FeatureFlagCache();

        /// <summary>
        /// Activity source for feature flag tracing.
        /// </summary>
        private static readonly ActivitySource s_activitySource = new ActivitySource("AdbcDrivers.Databricks.FeatureFlagCache");

        private readonly ConcurrentDictionary<string, FeatureFlagContext> _contexts;

        /// <summary>
        /// Gets the singleton instance of the FeatureFlagCache.
        /// </summary>
        public static FeatureFlagCache GetInstance() => s_instance;

        /// <summary>
        /// Creates a new FeatureFlagCache.
        /// </summary>
        internal FeatureFlagCache()
        {
            _contexts = new ConcurrentDictionary<string, FeatureFlagContext>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Gets or creates a feature flag context for the host.
        /// Increments reference count.
        /// Makes initial blocking fetch if context is new.
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
        /// <returns>The feature flag context for the host.</returns>
        /// <exception cref="ArgumentException">Thrown when host is null or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown when httpClient is null.</exception>
        public FeatureFlagContext GetOrCreateContext(string host, HttpClient httpClient, string driverVersion, string? endpointFormat = null)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            if (httpClient == null)
            {
                throw new ArgumentNullException(nameof(httpClient));
            }

            var context = _contexts.GetOrAdd(host, _ => new FeatureFlagContext(host, httpClient, driverVersion, endpointFormat));
            context.IncrementRefCount();

            Activity.Current?.AddEvent(new ActivityEvent("feature_flags.context.acquired",
                tags: new ActivityTagsCollection
                {
                    { "host", host },
                    { "ref_count", context.RefCount }
                }));

            return context;
        }

        /// <summary>
        /// Decrements reference count for the host.
        /// Removes context and stops refresh scheduler when ref count reaches zero.
        /// </summary>
        /// <param name="host">The host to release the context for.</param>
        /// <remarks>
        /// This method is thread-safe. If the reference count reaches zero,
        /// the context is removed from the cache and its refresh scheduler is stopped.
        /// If multiple threads try to release the same context simultaneously,
        /// only one will successfully remove it.
        /// </remarks>
        public void ReleaseContext(string host)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                return;
            }

            if (_contexts.TryGetValue(host, out var context))
            {
                var newRefCount = context.DecrementRefCount();

                Activity.Current?.AddEvent(new ActivityEvent("feature_flags.context.released",
                    tags: new ActivityTagsCollection
                    {
                        { "host", host },
                        { "ref_count", newRefCount }
                    }));

                if (newRefCount <= 0)
                {
                    // Try to remove the context. Use a compare-and-remove pattern
                    // to avoid race conditions where a new connection added a reference.
                    if (context.RefCount <= 0)
                    {
                        // Note: We check RefCount again because another thread might have
                        // incremented it between our check and the removal attempt.
                        if (_contexts.TryGetValue(host, out var currentContext) &&
                            ReferenceEquals(currentContext, context) &&
                            currentContext.RefCount <= 0)
                        {
                            // Use IDictionary.Remove to atomically check and remove
                            var removed = ((IDictionary<string, FeatureFlagContext>)_contexts)
                                .Remove(new KeyValuePair<string, FeatureFlagContext>(host, context));

                            if (removed)
                            {
                                // Stop the refresh scheduler and dispose the context
                                context.Dispose();

                                Activity.Current?.AddEvent(new ActivityEvent("feature_flags.context.disposed",
                                    tags: new ActivityTagsCollection { { "host", host } }));
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Gets the number of hosts currently cached.
        /// </summary>
        internal int CachedHostCount => _contexts.Count;

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

            return _contexts.ContainsKey(host);
        }

        /// <summary>
        /// Gets the context for the specified host, if it exists.
        /// Does not create a new context or modify reference count.
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

            if (_contexts.TryGetValue(host, out var foundContext))
            {
                context = foundContext;
                return true;
            }

            return false;
        }

        /// <summary>
        /// Clears all cached contexts and disposes them.
        /// This is primarily for testing purposes.
        /// </summary>
        internal void Clear()
        {
            foreach (var context in _contexts.Values)
            {
                context.Dispose();
            }
            _contexts.Clear();
        }

        /// <summary>
        /// Merges feature flags from server into properties.
        /// Feature flags have lower priority than user-specified properties.
        /// Priority: User Properties > Feature Flags > Driver Defaults
        /// </summary>
        /// <param name="properties">Properties after environment config merge.</param>
        /// <param name="assemblyVersion">The driver version for the API endpoint.</param>
        /// <returns>Properties with feature flags merged in.</returns>
        public IReadOnlyDictionary<string, string> MergePropertiesWithFeatureFlags(
            IReadOnlyDictionary<string, string> properties,
            string assemblyVersion)
        {
            using var activity = s_activitySource.StartActivity("MergePropertiesWithFeatureFlags");

            try
            {
                // Extract host from properties
                var host = TryGetHost(properties);
                if (string.IsNullOrEmpty(host))
                {
                    activity?.AddEvent(new ActivityEvent("feature_flags.skipped",
                        tags: new ActivityTagsCollection { { "reason", "no_host" } }));
                    return properties;
                }

                activity?.SetTag("feature_flags.host", host);

                // Extract token for authentication
                string? token = null;
                if (properties.TryGetValue(SparkParameters.Token, out var tokenValue))
                {
                    token = tokenValue;
                }

                if (string.IsNullOrEmpty(token))
                {
                    activity?.AddEvent(new ActivityEvent("feature_flags.skipped",
                        tags: new ActivityTagsCollection { { "reason", "no_token" } }));
                    return properties;
                }

                // Create HttpClient for feature flag API
                using var httpClient = CreateFeatureFlagHttpClient(host, token, assemblyVersion);

                // Get or create feature flag context (this makes the initial blocking fetch)
                var context = GetOrCreateContext(host, httpClient, assemblyVersion);

                // Get all flags from cache
                var featureFlags = context.GetAllFlags();

                if (featureFlags.Count == 0)
                {
                    activity?.AddEvent(new ActivityEvent("feature_flags.skipped",
                        tags: new ActivityTagsCollection { { "reason", "no_flags_returned" } }));
                    return properties;
                }

                activity?.SetTag("feature_flags.count", featureFlags.Count);
                activity?.AddEvent(new ActivityEvent("feature_flags.merging",
                    tags: new ActivityTagsCollection { { "flags_count", featureFlags.Count } }));

                // Merge: feature flags as base, user properties override
                // This ensures user properties take precedence over server flags
                return MergeProperties(featureFlags, properties);
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
                return properties;
            }
        }

        /// <summary>
        /// Tries to extract the host from properties without throwing.
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <returns>The host, or null if not found.</returns>
        internal static string? TryGetHost(IReadOnlyDictionary<string, string> properties)
        {
            if (properties.TryGetValue(SparkParameters.HostName, out string? host) && !string.IsNullOrEmpty(host))
            {
                return host;
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
        /// Creates an HttpClient configured for the feature flag API.
        /// </summary>
        /// <param name="host">The Databricks host.</param>
        /// <param name="token">The authentication token.</param>
        /// <param name="assemblyVersion">The driver version for the User-Agent.</param>
        /// <returns>Configured HttpClient.</returns>
        private static HttpClient CreateFeatureFlagHttpClient(string host, string token, string assemblyVersion)
        {
            var httpClient = new HttpClient
            {
                BaseAddress = new Uri($"https://{host}"),
                Timeout = TimeSpan.FromSeconds(10) // Short timeout for feature flags
            };

            httpClient.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            // Set User-Agent for connector service
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(
                $"DatabricksADBCDriverOSS/{assemblyVersion} (FeatureFlagClient)");

            return httpClient;
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
