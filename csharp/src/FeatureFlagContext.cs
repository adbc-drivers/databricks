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
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2;
using Apache.Arrow.Adbc.Tracing;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Holds feature flag state for a host.
    /// Cached by FeatureFlagCache with TTL-based expiration.
    /// </summary>
    /// <remarks>
    /// Each host (Databricks workspace) has one FeatureFlagContext instance
    /// that is shared across all connections to that host. The context:
    /// - Caches all feature flags returned by the server
    /// - Tracks TTL from server response for cache expiration
    /// - Runs a background refresh task based on TTL
    ///
    /// Thread-safety is ensured using ConcurrentDictionary for flag storage.
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
        /// Assembly version for the driver.
        /// </summary>
        private static readonly string s_assemblyVersion = ApacheUtility.GetAssemblyVersion(typeof(FeatureFlagContext));

        /// <summary>
        /// Default TTL (15 minutes) if server doesn't specify ttl_seconds.
        /// </summary>
        public static readonly TimeSpan DefaultTtl = TimeSpan.FromMinutes(15);

        /// <summary>
        /// Default feature flag endpoint format. {0} = driver version.
        /// </summary>
        internal const string DefaultFeatureFlagEndpointFormat = "/api/2.0/connector-service/feature-flags/ADBC/{0}";

        private readonly string _host;
        private readonly string _driverVersion;
        private readonly string _endpointFormat;
        private readonly HttpClient? _httpClient;
        private readonly ConcurrentDictionary<string, string> _flags;
        private readonly CancellationTokenSource _refreshCts;
        private readonly object _ttlLock = new object();

        private Task? _refreshTask;
        private TimeSpan _ttl;
        private bool _disposed;

        /// <summary>
        /// Gets the current TTL (from server ttl_seconds).
        /// </summary>
        public TimeSpan Ttl
        {
            get
            {
                lock (_ttlLock)
                {
                    return _ttl;
                }
            }
            internal set
            {
                lock (_ttlLock)
                {
                    _ttl = value;
                }
            }
        }

        /// <summary>
        /// Gets the host this context is for.
        /// </summary>
        public string Host => _host;

        /// <summary>
        /// Gets the current refresh interval (alias for Ttl).
        /// </summary>
        public TimeSpan RefreshInterval => Ttl;

        /// <summary>
        /// Internal constructor - use CreateAsync factory method for production code.
        /// Made internal to allow test code to create instances without HTTP calls.
        /// </summary>
        internal FeatureFlagContext(string host, HttpClient? httpClient, string driverVersion, string? endpointFormat)
        {
            _host = host;
            _httpClient = httpClient;
            _driverVersion = driverVersion ?? "1.0.0";
            _endpointFormat = endpointFormat ?? DefaultFeatureFlagEndpointFormat;
            _flags = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _ttl = DefaultTtl;
            _refreshCts = new CancellationTokenSource();
        }

        /// <summary>
        /// Creates a new context with the given HTTP client.
        /// Performs initial async fetch to populate cache, then starts background refresh task.
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
        /// <param name="cancellationToken">Cancellation token for the initial fetch.</param>
        /// <returns>A fully initialized FeatureFlagContext.</returns>
        public static async Task<FeatureFlagContext> CreateAsync(
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

            var context = new FeatureFlagContext(host, httpClient, driverVersion, endpointFormat);

            // Initial async fetch - wait for it to complete
            await context.FetchFeatureFlagsAsync("Initial", cancellationToken).ConfigureAwait(false);

            // Start background refresh task
            context.StartBackgroundRefresh();

            return context;
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
        /// Starts the background refresh task that periodically fetches flags based on TTL.
        /// </summary>
        private void StartBackgroundRefresh()
        {
            _refreshTask = Task.Run(async () =>
            {
                while (!_refreshCts.Token.IsCancellationRequested)
                {
                    try
                    {
                        // Wait for TTL duration before refreshing
                        await Task.Delay(Ttl, _refreshCts.Token).ConfigureAwait(false);

                        if (!_refreshCts.Token.IsCancellationRequested)
                        {
                            await FetchFeatureFlagsAsync("Background", _refreshCts.Token).ConfigureAwait(false);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Normal cancellation, exit the loop
                        break;
                    }
                    catch (Exception ex)
                    {
                        // Log error but continue the refresh loop
                        // Use StartActivity since Activity.Current is null in background tasks
                        using var activity = s_activitySource.StartActivity("BackgroundRefresh.Error");
                        activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                        activity?.AddEvent(new ActivityEvent("feature_flags.background_refresh.error",
                            tags: new ActivityTagsCollection
                            {
                                { "error.message", ex.Message },
                                { "error.type", ex.GetType().Name },
                                { "host", _host }
                            }));
                    }
                }
            }, _refreshCts.Token);
        }

        /// <summary>
        /// Disposes the context and stops the background refresh task.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            // Cancel the background refresh task
            _refreshCts.Cancel();

            // Wait briefly for the task to complete (don't block indefinitely)
            try
            {
                _refreshTask?.Wait(TimeSpan.FromSeconds(1));
            }
            catch (AggregateException)
            {
                // Task was cancelled, ignore
            }

            _refreshCts.Dispose();
        }

        /// <summary>
        /// Fetches feature flags from the API endpoint asynchronously.
        /// </summary>
        /// <param name="fetchType">Type of fetch for logging purposes (e.g., "Initial" or "Background").</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private async Task FetchFeatureFlagsAsync(string fetchType, CancellationToken cancellationToken)
        {
            if (_httpClient == null)
            {
                return;
            }

            using var activity = s_activitySource.StartActivity($"FetchFeatureFlags.{fetchType}");
            activity?.SetTag("feature_flags.host", _host);
            activity?.SetTag("feature_flags.fetch_type", fetchType);

            try
            {
                var endpoint = string.Format(_endpointFormat, _driverVersion);
                activity?.SetTag("feature_flags.endpoint", endpoint);

                var response = await _httpClient.GetAsync(endpoint, cancellationToken).ConfigureAwait(false);

                activity?.SetTag("feature_flags.response.status_code", (int)response.StatusCode);

                // Use the standard EnsureSuccessOrThrow extension method
                response.EnsureSuccessOrThrow();

                var content = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                ProcessResponse(content, activity);

                activity?.SetStatus(ActivityStatusCode.Ok);
            }
            catch (OperationCanceledException)
            {
                // Propagate cancellation
                throw;
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

                // Update TTL if server provides a different value
                if (response?.TtlSeconds != null && response.TtlSeconds > 0)
                {
                    Ttl = TimeSpan.FromSeconds(response.TtlSeconds.Value);
                    activity?.SetTag("feature_flags.ttl_seconds", response.TtlSeconds.Value);
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
