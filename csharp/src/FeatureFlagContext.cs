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
using System.Text.Json;
using System.Threading;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Holds feature flag state and reference count for a host.
    /// Manages background refresh scheduling.
    /// Uses the HttpClient provided by the connection for API calls.
    /// </summary>
    /// <remarks>
    /// Each host (Databricks workspace) has one FeatureFlagContext instance
    /// that is shared across all connections to that host. The context:
    /// - Caches all feature flags returned by the server
    /// - Schedules background refreshes at intervals specified by server's ttl_seconds
    /// - Uses reference counting for proper cleanup
    ///
    /// Thread-safety is ensured using:
    /// - ConcurrentDictionary for flag storage
    /// - Interlocked operations for reference count
    /// - Lock-based synchronization for timer management
    ///
    /// JDBC Reference: DatabricksDriverFeatureFlagsContext.java
    /// </remarks>
    internal sealed class FeatureFlagContext : IDisposable
    {
        /// <summary>
        /// Default refresh interval (15 minutes) if server doesn't specify ttl_seconds.
        /// </summary>
        public static readonly TimeSpan DefaultRefreshInterval = TimeSpan.FromMinutes(15);

        /// <summary>
        /// Feature flag endpoint format. {0} = driver version.
        /// NOTE: Using OSS_JDBC endpoint until OSS_ADBC is configured server-side.
        /// </summary>
        internal const string FeatureFlagEndpointFormat = "/api/2.0/connector-service/feature-flags/OSS_JDBC/{0}";

        private readonly string _host;
        private readonly string _driverVersion;
        private readonly HttpClient _httpClient;
        private readonly ConcurrentDictionary<string, string> _flags;
        private readonly object _timerLock = new object();

        private Timer? _refreshTimer;
        private TimeSpan _refreshInterval;
        private int _refCount;
        private bool _disposed;

        /// <summary>
        /// Gets the current refresh interval (from server ttl_seconds).
        /// </summary>
        public TimeSpan RefreshInterval
        {
            get
            {
                lock (_timerLock)
                {
                    return _refreshInterval;
                }
            }
        }

        /// <summary>
        /// Gets the current reference count (number of connections using this context).
        /// </summary>
        public int RefCount => Volatile.Read(ref _refCount);

        /// <summary>
        /// Creates a new context with the given HTTP client.
        /// Makes initial blocking fetch to populate cache.
        /// Starts background refresh scheduler.
        /// </summary>
        /// <param name="host">The Databricks host.</param>
        /// <param name="httpClient">
        /// HttpClient from the connection, pre-configured with:
        /// - Base address (https://{host})
        /// - Auth headers (Bearer token)
        /// - Custom User-Agent for connector service
        /// </param>
        /// <param name="driverVersion">The driver version for the API endpoint.</param>
        public FeatureFlagContext(string host, HttpClient httpClient, string driverVersion)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            _host = host;
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _driverVersion = driverVersion ?? "1.0.0";
            _flags = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _refreshInterval = DefaultRefreshInterval;
            _refCount = 0;

            // Initial blocking fetch
            FetchFeatureFlagsBlocking();

            // Start background refresh scheduler
            StartRefreshScheduler();
        }

        /// <summary>
        /// Creates a new context for testing with pre-populated flags.
        /// Does not make API calls or start background refresh.
        /// </summary>
        /// <param name="initialFlags">Initial flags to populate.</param>
        /// <param name="refreshInterval">Optional refresh interval.</param>
        internal FeatureFlagContext(
            IReadOnlyDictionary<string, string>? initialFlags = null,
            TimeSpan? refreshInterval = null)
        {
            _host = "test-host";
            _httpClient = null!;
            _driverVersion = "1.0.0";
            _flags = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _refreshInterval = refreshInterval ?? DefaultRefreshInterval;
            _refCount = 0;

            if (initialFlags != null)
            {
                foreach (var kvp in initialFlags)
                {
                    _flags[kvp.Key] = kvp.Value;
                }
            }
        }

        /// <summary>
        /// Gets a feature flag value by name.
        /// Returns null if the flag is not found.
        /// </summary>
        /// <param name="flagName">The feature flag name.</param>
        /// <returns>The flag value, or null if not found.</returns>
        public string? GetFlagValue(string flagName)
        {
            if (string.IsNullOrWhiteSpace(flagName))
            {
                return null;
            }

            return _flags.TryGetValue(flagName, out var value) ? value : null;
        }

        /// <summary>
        /// Checks if a feature flag is enabled (value is "true").
        /// Returns false if flag is not found or value is not "true".
        /// </summary>
        /// <param name="flagName">The feature flag name.</param>
        /// <returns>True if the flag value is "true", false otherwise.</returns>
        public bool IsFeatureEnabled(string flagName)
        {
            var value = GetFlagValue(flagName);
            return string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Gets all cached feature flags as a dictionary.
        /// Can be used to merge with user properties.
        /// </summary>
        /// <returns>A read-only dictionary of all cached flags.</returns>
        public IReadOnlyDictionary<string, string> GetAllFlags()
        {
            // Return a snapshot to avoid concurrency issues
            return new Dictionary<string, string>(_flags, StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Increments the reference count.
        /// </summary>
        /// <returns>The new reference count.</returns>
        public int IncrementRefCount()
        {
            return Interlocked.Increment(ref _refCount);
        }

        /// <summary>
        /// Decrements the reference count.
        /// </summary>
        /// <returns>The new reference count.</returns>
        public int DecrementRefCount()
        {
            return Interlocked.Decrement(ref _refCount);
        }

        /// <summary>
        /// Stops the background refresh scheduler.
        /// </summary>
        public void Shutdown()
        {
            lock (_timerLock)
            {
                if (_refreshTimer != null)
                {
                    _refreshTimer.Dispose();
                    _refreshTimer = null;
                    Debug.WriteLine($"[TRACE] FeatureFlagContext: Stopped refresh scheduler for host '{_host}'");
                }
            }
        }

        /// <summary>
        /// Disposes the context and stops the background refresh scheduler.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            Shutdown();
            _disposed = true;
        }

        /// <summary>
        /// Performs the initial blocking fetch of feature flags.
        /// </summary>
        private void FetchFeatureFlagsBlocking()
        {
            try
            {
                var endpoint = string.Format(FeatureFlagEndpointFormat, _driverVersion);
                Debug.WriteLine($"[TRACE] FeatureFlagContext: Initial fetch from '{endpoint}' for host '{_host}'");

                var response = _httpClient.GetAsync(endpoint).ConfigureAwait(false).GetAwaiter().GetResult();

                if (response.IsSuccessStatusCode)
                {
                    var content = response.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                    ProcessResponse(content);
                }
                else
                {
                    Debug.WriteLine($"[TRACE] FeatureFlagContext: Initial fetch failed with status {response.StatusCode} for host '{_host}'");
                }
            }
            catch (Exception ex)
            {
                // Swallow exceptions - telemetry should not break the connection
                Debug.WriteLine($"[TRACE] FeatureFlagContext: Initial fetch failed for host '{_host}': {ex.Message}");
            }
        }

        /// <summary>
        /// Starts the background refresh scheduler.
        /// </summary>
        private void StartRefreshScheduler()
        {
            lock (_timerLock)
            {
                _refreshTimer = new Timer(
                    RefreshCallback,
                    null,
                    _refreshInterval,
                    _refreshInterval);

                Debug.WriteLine($"[TRACE] FeatureFlagContext: Started refresh scheduler for host '{_host}' with interval {_refreshInterval.TotalSeconds}s");
            }
        }

        /// <summary>
        /// Timer callback for background refresh.
        /// </summary>
        private void RefreshCallback(object? state)
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                var endpoint = string.Format(FeatureFlagEndpointFormat, _driverVersion);
                Debug.WriteLine($"[TRACE] FeatureFlagContext: Background refresh from '{endpoint}' for host '{_host}'");

                var response = _httpClient.GetAsync(endpoint).ConfigureAwait(false).GetAwaiter().GetResult();

                if (response.IsSuccessStatusCode)
                {
                    var content = response.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                    ProcessResponse(content);
                }
                else
                {
                    Debug.WriteLine($"[TRACE] FeatureFlagContext: Background refresh failed with status {response.StatusCode} for host '{_host}'");
                }
            }
            catch (Exception ex)
            {
                // Swallow exceptions - telemetry should not break the connection
                Debug.WriteLine($"[TRACE] FeatureFlagContext: Background refresh failed for host '{_host}': {ex.Message}");
            }
        }

        /// <summary>
        /// Processes the JSON response and updates the cache.
        /// </summary>
        private void ProcessResponse(string content)
        {
            try
            {
                var response = JsonSerializer.Deserialize<FeatureFlagsResponse>(content);

                if (response?.Flags != null)
                {
                    foreach (var flag in response.Flags)
                    {
                        if (!string.IsNullOrEmpty(flag.Name))
                        {
                            _flags[flag.Name] = flag.Value ?? string.Empty;
                        }
                    }

                    Debug.WriteLine($"[TRACE] FeatureFlagContext: Updated {response.Flags.Count} flags for host '{_host}'");
                }

                // Update refresh interval if server provides a different TTL
                if (response?.TtlSeconds != null && response.TtlSeconds > 0)
                {
                    var newInterval = TimeSpan.FromSeconds(response.TtlSeconds.Value);
                    UpdateRefreshInterval(newInterval);
                }
            }
            catch (JsonException ex)
            {
                Debug.WriteLine($"[TRACE] FeatureFlagContext: Failed to parse response for host '{_host}': {ex.Message}");
            }
        }

        /// <summary>
        /// Updates the refresh interval if it has changed.
        /// </summary>
        private void UpdateRefreshInterval(TimeSpan newInterval)
        {
            lock (_timerLock)
            {
                if (_refreshInterval == newInterval)
                {
                    return;
                }

                _refreshInterval = newInterval;

                if (_refreshTimer != null)
                {
                    _refreshTimer.Change(newInterval, newInterval);
                    Debug.WriteLine($"[TRACE] FeatureFlagContext: Updated refresh interval to {newInterval.TotalSeconds}s for host '{_host}'");
                }
            }
        }

        /// <summary>
        /// Clears all cached flags.
        /// This is primarily for testing purposes.
        /// </summary>
        internal void ClearFlags()
        {
            _flags.Clear();
        }

        /// <summary>
        /// Sets a flag value directly.
        /// This is primarily for testing purposes.
        /// </summary>
        internal void SetFlag(string name, string value)
        {
            _flags[name] = value;
        }
    }
}
