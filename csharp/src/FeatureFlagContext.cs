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
using Apache.Arrow.Adbc.Tracing;

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
        /// Activity source for feature flag tracing.
        /// </summary>
        private static readonly ActivitySource s_activitySource = new ActivitySource("AdbcDrivers.Databricks.FeatureFlags");

        /// <summary>
        /// Default refresh interval (15 minutes) if server doesn't specify ttl_seconds.
        /// </summary>
        public static readonly TimeSpan DefaultRefreshInterval = TimeSpan.FromMinutes(15);

        /// <summary>
        /// Default feature flag endpoint format. {0} = driver version.
        /// NOTE: Using OSS_JDBC endpoint until OSS_ADBC is configured server-side.
        /// </summary>
        internal const string DefaultFeatureFlagEndpointFormat = "/api/2.0/connector-service/feature-flags/OSS_JDBC/{0}";

        private readonly string _host;
        private readonly string _driverVersion;
        private readonly string _endpointFormat;
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
        /// <param name="endpointFormat">Optional custom endpoint format. If null, uses the default endpoint.</param>
        public FeatureFlagContext(string host, HttpClient httpClient, string driverVersion, string? endpointFormat = null)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            _host = host;
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _driverVersion = driverVersion ?? "1.0.0";
            _endpointFormat = endpointFormat ?? DefaultFeatureFlagEndpointFormat;
            _flags = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _refreshInterval = DefaultRefreshInterval;
            _refCount = 0;

            // Initial blocking fetch
            FetchFeatureFlags("Initial");

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
            _endpointFormat = DefaultFeatureFlagEndpointFormat;
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

                    Activity.Current?.AddEvent("feature_flags.scheduler.stopped", [
                        new("host", _host)
                    ]);
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
        /// Fetches feature flags from the API endpoint and processes the response.
        /// This is a common method used by both initial fetch and background refresh.
        /// </summary>
        /// <param name="fetchType">Type of fetch for logging purposes (e.g., "Initial" or "Background").</param>
        private void FetchFeatureFlags(string fetchType)
        {
            using var activity = s_activitySource.StartActivity($"FetchFeatureFlags.{fetchType}");
            activity?.SetTag("feature_flags.host", _host);
            activity?.SetTag("feature_flags.fetch_type", fetchType);

            try
            {
                var endpoint = string.Format(_endpointFormat, _driverVersion);
                activity?.SetTag("feature_flags.endpoint", endpoint);

                var response = _httpClient.GetAsync(endpoint).ConfigureAwait(false).GetAwaiter().GetResult();

                EnsureSuccessStatusCode(response, fetchType, activity);

                var content = response.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                ProcessResponse(content, activity);

                activity?.SetStatus(ActivityStatusCode.Ok);
            }
            catch (Exception ex)
            {
                // Swallow exceptions - telemetry should not break the connection
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity?.AddEvent("feature_flags.fetch.failed", [
                    new("error.message", ex.Message),
                    new("error.type", ex.GetType().Name)
                ]);
            }
        }

        /// <summary>
        /// Ensures the HTTP response indicates success, otherwise logs and throws an exception.
        /// </summary>
        /// <param name="response">The HTTP response message.</param>
        /// <param name="fetchType">Type of fetch for logging purposes.</param>
        /// <param name="activity">The current activity for tracing.</param>
        private void EnsureSuccessStatusCode(HttpResponseMessage response, string fetchType, Activity? activity)
        {
            activity?.SetTag("feature_flags.response.status_code", (int)response.StatusCode);

            if (response.IsSuccessStatusCode)
            {
                return;
            }

            var errorContent = response.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult();
            var errorMessage = $"Feature flag API request failed with status code {(int)response.StatusCode} ({response.StatusCode})";

            if (!string.IsNullOrWhiteSpace(errorContent))
            {
                errorMessage = $"{errorMessage}. Response: {errorContent}";
            }

            activity?.AddEvent("feature_flags.response.error", [
                new("status_code", (int)response.StatusCode),
                new("error.message", errorMessage)
            ]);

            throw new HttpRequestException(errorMessage);
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

                Activity.Current?.AddEvent("feature_flags.scheduler.started", [
                    new("host", _host),
                    new("interval_seconds", _refreshInterval.TotalSeconds)
                ]);
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

            FetchFeatureFlags("Background");
        }

        /// <summary>
        /// Processes the JSON response and updates the cache.
        /// </summary>
        /// <param name="content">The JSON response content.</param>
        /// <param name="activity">The current activity for tracing.</param>
        private void ProcessResponse(string content, Activity? activity)
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

                    activity?.SetTag("feature_flags.count", response.Flags.Count);
                    activity?.AddEvent("feature_flags.updated", [
                        new("flags_count", response.Flags.Count)
                    ]);
                }

                // Update refresh interval if server provides a different TTL
                if (response?.TtlSeconds != null && response.TtlSeconds > 0)
                {
                    var newInterval = TimeSpan.FromSeconds(response.TtlSeconds.Value);
                    activity?.SetTag("feature_flags.ttl_seconds", response.TtlSeconds.Value);
                    UpdateRefreshInterval(newInterval, activity);
                }
            }
            catch (JsonException ex)
            {
                activity?.AddEvent("feature_flags.parse.failed", [
                    new("error.message", ex.Message),
                    new("error.type", ex.GetType().Name)
                ]);
            }
        }

        /// <summary>
        /// Updates the refresh interval if it has changed.
        /// </summary>
        /// <param name="newInterval">The new refresh interval.</param>
        /// <param name="activity">The current activity for tracing.</param>
        private void UpdateRefreshInterval(TimeSpan newInterval, Activity? activity = null)
        {
            lock (_timerLock)
            {
                if (_refreshInterval == newInterval)
                {
                    return;
                }

                var oldInterval = _refreshInterval;
                _refreshInterval = newInterval;

                if (_refreshTimer != null)
                {
                    _refreshTimer.Change(newInterval, newInterval);
                    activity?.AddEvent("feature_flags.interval.updated", [
                        new("old_interval_seconds", oldInterval.TotalSeconds),
                        new("new_interval_seconds", newInterval.TotalSeconds)
                    ]);
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
