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
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Global singleton ActivityListener that subscribes to the "Databricks.Adbc.Driver"
    /// ActivitySource and routes completed activities to the appropriate
    /// <see cref="MetricsAggregator"/> based on the activity's session.id tag.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Connections register their aggregator via <see cref="RegisterAggregator"/> on open,
    /// and unregister via <see cref="UnregisterAggregatorAsync"/> on close (which flushes
    /// remaining metrics before removal).
    /// </para>
    /// <para>
    /// The ActivityListener is configured with:
    /// <list type="bullet">
    ///   <item><description>ShouldListenTo filtering for "Databricks.Adbc.Driver"</description></item>
    ///   <item><description>ActivityStopped callback that routes to the correct aggregator</description></item>
    ///   <item><description>Sample callback returning AllDataAndRecorded</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// All exceptions in callbacks are swallowed to ensure telemetry never impacts driver operations.
    /// </para>
    /// </remarks>
    internal sealed class DatabricksActivityListener : IDisposable
    {
        /// <summary>
        /// The ActivitySource name used by the Databricks ADBC driver.
        /// This must match the assembly name used by <see cref="DatabricksConnection"/>
        /// as the ActivitySource name in <c>TracingConnection</c>.
        /// </summary>
        internal static readonly string DatabricksActivitySourceName =
            typeof(DatabricksConnection).Assembly.GetName().Name!;

        private static readonly Lazy<DatabricksActivityListener> s_instance =
            new Lazy<DatabricksActivityListener>(() => new DatabricksActivityListener());

        private readonly ConcurrentDictionary<string, MetricsAggregator> _aggregators;
        private ActivityListener? _listener;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabricksActivityListener"/> class.
        /// Private constructor to enforce singleton pattern. Use <see cref="Instance"/> for
        /// production code or <see cref="CreateForTesting"/> for test isolation.
        /// </summary>
        private DatabricksActivityListener()
        {
            _aggregators = new ConcurrentDictionary<string, MetricsAggregator>(StringComparer.Ordinal);
        }

        /// <summary>
        /// Gets the singleton instance of the <see cref="DatabricksActivityListener"/>.
        /// </summary>
        public static DatabricksActivityListener Instance => s_instance.Value;

        /// <summary>
        /// Creates an isolated instance of <see cref="DatabricksActivityListener"/> for testing.
        /// This instance is independent of the singleton and can be disposed without
        /// affecting the global listener.
        /// </summary>
        /// <returns>A new isolated <see cref="DatabricksActivityListener"/> instance.</returns>
        internal static DatabricksActivityListener CreateForTesting()
        {
            return new DatabricksActivityListener();
        }

        /// <summary>
        /// Gets the number of registered aggregators. Exposed for testing.
        /// </summary>
        internal int RegisteredAggregatorCount => _aggregators.Count;

        /// <summary>
        /// Registers a <see cref="MetricsAggregator"/> for the given session ID.
        /// Called by connections on open to start receiving activity data.
        /// </summary>
        /// <param name="sessionId">The connection session ID.</param>
        /// <param name="aggregator">The per-connection metrics aggregator.</param>
        public void RegisterAggregator(string sessionId, MetricsAggregator aggregator)
        {
            try
            {
                if (string.IsNullOrEmpty(sessionId) || aggregator == null)
                {
                    return;
                }

                _aggregators[sessionId] = aggregator;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] DatabricksActivityListener.RegisterAggregator error: {ex.Message}");
            }
        }

        /// <summary>
        /// Unregisters the <see cref="MetricsAggregator"/> for the given session ID.
        /// Flushes remaining metrics on the aggregator before removal.
        /// Called by connections on close.
        /// </summary>
        /// <param name="sessionId">The connection session ID.</param>
        /// <returns>A task that completes when the aggregator has been flushed and removed.</returns>
        public async Task UnregisterAggregatorAsync(string sessionId)
        {
            try
            {
                if (string.IsNullOrEmpty(sessionId))
                {
                    return;
                }

                if (_aggregators.TryRemove(sessionId, out MetricsAggregator? aggregator))
                {
                    if (aggregator != null)
                    {
                        await aggregator.FlushAsync().ConfigureAwait(false);
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] DatabricksActivityListener.UnregisterAggregatorAsync error: {ex.Message}");
            }
        }

        /// <summary>
        /// Creates and registers the <see cref="ActivityListener"/> to start
        /// listening to the "Databricks.Adbc.Driver" ActivitySource.
        /// </summary>
        public void Start()
        {
            try
            {
                if (_disposed || _listener != null)
                {
                    return;
                }

                _listener = new ActivityListener
                {
                    ShouldListenTo = ShouldListenTo,
                    ActivityStopped = OnActivityStopped,
                    Sample = OnSample
                };

                ActivitySource.AddActivityListener(_listener);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] DatabricksActivityListener.Start error: {ex.Message}");
            }
        }

        /// <summary>
        /// Determines whether this listener should listen to the given ActivitySource.
        /// Only listens to the "Databricks.Adbc.Driver" source.
        /// </summary>
        /// <param name="source">The ActivitySource to evaluate.</param>
        /// <returns>True if the source name matches "Databricks.Adbc.Driver"; otherwise, false.</returns>
        internal bool ShouldListenTo(ActivitySource source)
        {
            try
            {
                return source.Name == DatabricksActivitySourceName;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] DatabricksActivityListener.ShouldListenTo error: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Callback invoked when an activity is stopped. Extracts the session.id tag
        /// and routes the activity to the correct <see cref="MetricsAggregator"/>.
        /// </summary>
        /// <param name="activity">The stopped activity.</param>
        internal void OnActivityStopped(Activity activity)
        {
            try
            {
                if (activity == null)
                {
                    return;
                }

                // Extract session.id tag - activities without it are silently ignored
                string? sessionId = activity.GetTagItem("session.id") as string;
                if (string.IsNullOrEmpty(sessionId))
                {
                    return;
                }

                // Route to the correct aggregator
                if (_aggregators.TryGetValue(sessionId!, out MetricsAggregator? aggregator))
                {
                    aggregator.ProcessActivity(activity);
                }
                // Unknown session IDs are silently ignored
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] DatabricksActivityListener.OnActivityStopped error: {ex.Message}");
            }
        }

        /// <summary>
        /// Sample callback that returns <see cref="ActivitySamplingResult.AllDataAndRecorded"/>
        /// to ensure all activity data is captured for telemetry.
        /// </summary>
        /// <param name="options">The activity creation options.</param>
        /// <returns><see cref="ActivitySamplingResult.AllDataAndRecorded"/>.</returns>
        internal ActivitySamplingResult OnSample(ref ActivityCreationOptions<ActivityContext> options)
        {
            try
            {
                return ActivitySamplingResult.AllDataAndRecorded;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] DatabricksActivityListener.OnSample error: {ex.Message}");
                return ActivitySamplingResult.None;
            }
        }

        /// <summary>
        /// Disposes the <see cref="ActivityListener"/> and clears the aggregator registry.
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;

                try
                {
                    _listener?.Dispose();
                    _listener = null;
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"[TRACE] DatabricksActivityListener.Dispose error: {ex.Message}");
                }

                _aggregators.Clear();
            }
        }
    }
}
