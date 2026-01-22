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
using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Singleton that manages feature flag cache per host.
    /// Prevents rate limiting by caching feature flag responses.
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

        private readonly ConcurrentDictionary<string, FeatureFlagContext> _contexts;
        private readonly TimeSpan _defaultCacheDuration;

        /// <summary>
        /// Gets the singleton instance of the FeatureFlagCache.
        /// </summary>
        public static FeatureFlagCache GetInstance() => s_instance;

        /// <summary>
        /// Creates a new FeatureFlagCache with default cache duration (15 minutes).
        /// </summary>
        internal FeatureFlagCache()
            : this(FeatureFlagContext.DefaultCacheDuration)
        {
        }

        /// <summary>
        /// Creates a new FeatureFlagCache with the specified default cache duration.
        /// </summary>
        /// <param name="defaultCacheDuration">The default cache duration for new contexts.</param>
        internal FeatureFlagCache(TimeSpan defaultCacheDuration)
        {
            if (defaultCacheDuration <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(defaultCacheDuration), "Cache duration must be greater than zero.");
            }

            _contexts = new ConcurrentDictionary<string, FeatureFlagContext>(StringComparer.OrdinalIgnoreCase);
            _defaultCacheDuration = defaultCacheDuration;
        }

        /// <summary>
        /// Gets or creates a feature flag context for the host.
        /// Increments reference count.
        /// </summary>
        /// <param name="host">The host (Databricks workspace URL) to get or create a context for.</param>
        /// <returns>The feature flag context for the host.</returns>
        /// <exception cref="ArgumentException">Thrown when host is null or whitespace.</exception>
        public FeatureFlagContext GetOrCreateContext(string host)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            var context = _contexts.GetOrAdd(host, _ => new FeatureFlagContext(_defaultCacheDuration));
            context.IncrementRefCount();

            Debug.WriteLine($"[TRACE] FeatureFlagCache: GetOrCreateContext for host '{host}', RefCount={context.RefCount}");

            return context;
        }

        /// <summary>
        /// Decrements reference count for the host.
        /// Removes context when ref count reaches zero.
        /// </summary>
        /// <param name="host">The host to release the context for.</param>
        /// <remarks>
        /// This method is thread-safe. If the reference count reaches zero,
        /// the context is removed from the cache. If multiple threads try to
        /// release the same context simultaneously, only one will successfully
        /// remove it.
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
                Debug.WriteLine($"[TRACE] FeatureFlagCache: ReleaseContext for host '{host}', RefCount={newRefCount}");

                if (newRefCount <= 0)
                {
                    // Try to remove the context. Use TryRemove with the specific value
                    // to avoid race conditions where a new connection added a reference.
                    if (context.RefCount <= 0)
                    {
                        // Note: We check RefCount again because another thread might have
                        // incremented it between our check and the removal attempt.
#if NET5_0_OR_GREATER
                        _contexts.TryRemove(new System.Collections.Generic.KeyValuePair<string, FeatureFlagContext>(host, context));
#else
                        // For netstandard2.0, we need to be more careful about the removal
                        // to avoid race conditions.
                        if (_contexts.TryGetValue(host, out var currentContext) && currentContext == context && currentContext.RefCount <= 0)
                        {
                            ((System.Collections.Generic.IDictionary<string, FeatureFlagContext>)_contexts).Remove(new System.Collections.Generic.KeyValuePair<string, FeatureFlagContext>(host, context));
                        }
#endif
                        Debug.WriteLine($"[TRACE] FeatureFlagCache: Removed context for host '{host}'");
                    }
                }
            }
        }

        /// <summary>
        /// Checks if telemetry is enabled for the host.
        /// Uses cached value if available and not expired.
        /// </summary>
        /// <param name="host">The host to check telemetry status for.</param>
        /// <param name="featureFlagFetcher">Function to fetch the feature flag from the server.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>True if telemetry is enabled, false otherwise.</returns>
        /// <remarks>
        /// This method:
        /// 1. Returns the cached value if available and not expired
        /// 2. Otherwise fetches the feature flag using the provided fetcher
        /// 3. Caches the result for future calls
        ///
        /// All exceptions from the fetcher are caught and logged at TRACE level.
        /// On error, returns false (telemetry disabled) as a safe default.
        /// </remarks>
        public async Task<bool> IsTelemetryEnabledAsync(
            string host,
            Func<CancellationToken, Task<bool>> featureFlagFetcher,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                return false;
            }

            if (featureFlagFetcher == null)
            {
                return false;
            }

            try
            {
                if (!_contexts.TryGetValue(host, out var context))
                {
                    // No context for this host, return false
                    return false;
                }

                // Check if we have a valid cached value
                if (context.TryGetCachedValue(out bool cachedValue))
                {
                    Debug.WriteLine($"[TRACE] FeatureFlagCache: Using cached value for host '{host}': {cachedValue}");
                    return cachedValue;
                }

                // Cache miss or expired - fetch from server
                Debug.WriteLine($"[TRACE] FeatureFlagCache: Cache miss for host '{host}', fetching from server");
                var enabled = await featureFlagFetcher(ct).ConfigureAwait(false);

                // Update the cache
                context.SetTelemetryEnabled(enabled);
                Debug.WriteLine($"[TRACE] FeatureFlagCache: Updated cache for host '{host}': {enabled}");

                return enabled;
            }
            catch (OperationCanceledException)
            {
                // Don't swallow cancellation
                throw;
            }
            catch (Exception ex)
            {
                // Swallow all other exceptions per telemetry requirement
                // Log at TRACE level to avoid customer anxiety
                Debug.WriteLine($"[TRACE] FeatureFlagCache: Error fetching feature flag for host '{host}': {ex.Message}");
                return false;
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
        /// Clears all cached contexts.
        /// This is primarily for testing purposes.
        /// </summary>
        internal void Clear()
        {
            _contexts.Clear();
        }
    }
}
