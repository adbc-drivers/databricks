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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Custom ActivityListener that aggregates metrics from Activity events
    /// and exports them to Databricks telemetry service.
    /// </summary>
    /// <remarks>
    /// This class:
    /// - Listens to activities from the "Databricks.Adbc.Driver" ActivitySource
    /// - Uses Sample callback to respect feature flag (enabled via TelemetryConfiguration)
    /// - Delegates metric processing to MetricsAggregator on ActivityStopped
    /// - All callbacks wrapped in try-catch with TRACE logging to prevent impacting driver operations
    /// - All exceptions are swallowed to prevent impacting driver operations
    ///
    /// JDBC Reference: DatabricksDriverTelemetryHelper.java - sets up telemetry collection
    /// </remarks>
    internal sealed class DatabricksActivityListener : IDisposable
    {
        /// <summary>
        /// The ActivitySource name that this listener listens to.
        /// </summary>
        public const string DatabricksActivitySourceName = "Databricks.Adbc.Driver";

        private readonly MetricsAggregator _aggregator;
        private readonly TelemetryConfiguration _config;
        private readonly Func<bool>? _featureFlagChecker;
        private ActivityListener? _listener;
        private bool _started;
        private bool _disposed;
        private readonly object _lock = new object();

        /// <summary>
        /// Creates a new DatabricksActivityListener.
        /// </summary>
        /// <param name="aggregator">The MetricsAggregator to delegate activity processing to.</param>
        /// <param name="config">The telemetry configuration.</param>
        /// <param name="featureFlagChecker">
        /// Optional function to check if telemetry is enabled at runtime.
        /// If provided, this is called on each activity sample. If null, uses config.Enabled.
        /// </param>
        /// <exception cref="ArgumentNullException">Thrown when aggregator or config is null.</exception>
        public DatabricksActivityListener(
            MetricsAggregator aggregator,
            TelemetryConfiguration config,
            Func<bool>? featureFlagChecker = null)
        {
            _aggregator = aggregator ?? throw new ArgumentNullException(nameof(aggregator));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _featureFlagChecker = featureFlagChecker;
        }

        /// <summary>
        /// Gets whether the listener has been started.
        /// </summary>
        public bool IsStarted => _started;

        /// <summary>
        /// Gets whether the listener has been disposed.
        /// </summary>
        public bool IsDisposed => _disposed;

        /// <summary>
        /// Starts listening to activities from the Databricks ActivitySource.
        /// </summary>
        /// <remarks>
        /// This method creates and registers an ActivityListener with the following configuration:
        /// - ShouldListenTo returns true only for "Databricks.Adbc.Driver" ActivitySource
        /// - Sample returns AllDataAndRecorded when telemetry is enabled, None when disabled
        /// - ActivityStopped delegates to MetricsAggregator.ProcessActivity
        ///
        /// This method is thread-safe and idempotent (calling multiple times has no additional effect).
        /// All exceptions are caught and logged at TRACE level.
        /// </remarks>
        public void Start()
        {
            lock (_lock)
            {
                if (_started || _disposed)
                {
                    return;
                }

                try
                {
                    _listener = CreateListener();
                    ActivitySource.AddActivityListener(_listener);
                    _started = true;

                    Debug.WriteLine("[TRACE] DatabricksActivityListener: Started listening to activities");
                }
                catch (Exception ex)
                {
                    // Swallow all exceptions per telemetry requirement
                    Debug.WriteLine($"[TRACE] DatabricksActivityListener: Error starting listener: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Stops listening to activities and flushes any pending metrics.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the asynchronous stop operation.</returns>
        /// <remarks>
        /// This method:
        /// 1. Disposes the ActivityListener to stop receiving new activities
        /// 2. Flushes all pending metrics via MetricsAggregator.FlushAsync
        /// 3. Disposes the MetricsAggregator
        ///
        /// This method is thread-safe. All exceptions are caught and logged at TRACE level.
        /// </remarks>
        public async Task StopAsync(CancellationToken ct = default)
        {
            lock (_lock)
            {
                if (!_started || _disposed)
                {
                    return;
                }

                try
                {
                    // Stop receiving new activities
                    _listener?.Dispose();
                    _listener = null;
                    _started = false;

                    Debug.WriteLine("[TRACE] DatabricksActivityListener: Stopped listening to activities");
                }
                catch (Exception ex)
                {
                    // Swallow all exceptions per telemetry requirement
                    Debug.WriteLine($"[TRACE] DatabricksActivityListener: Error stopping listener: {ex.Message}");
                }
            }

            // Flush pending metrics outside the lock to avoid deadlocks
            try
            {
                await _aggregator.FlushAsync(ct).ConfigureAwait(false);
                Debug.WriteLine("[TRACE] DatabricksActivityListener: Flushed pending metrics");
            }
            catch (OperationCanceledException)
            {
                // Don't swallow cancellation
                throw;
            }
            catch (Exception ex)
            {
                // Swallow all other exceptions per telemetry requirement
                Debug.WriteLine($"[TRACE] DatabricksActivityListener: Error flushing metrics: {ex.Message}");
            }
        }

        /// <summary>
        /// Disposes the DatabricksActivityListener.
        /// </summary>
        /// <remarks>
        /// This method stops listening and disposes resources synchronously.
        /// Use StopAsync for graceful shutdown with flush.
        /// All exceptions are caught and logged at TRACE level.
        /// </remarks>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            lock (_lock)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;

                try
                {
                    _listener?.Dispose();
                    _listener = null;
                    _started = false;

                    // Dispose aggregator (this also flushes synchronously)
                    _aggregator.Dispose();

                    Debug.WriteLine("[TRACE] DatabricksActivityListener: Disposed");
                }
                catch (Exception ex)
                {
                    // Swallow all exceptions per telemetry requirement
                    Debug.WriteLine($"[TRACE] DatabricksActivityListener: Error during dispose: {ex.Message}");
                }
            }
        }

        #region Private Methods

        /// <summary>
        /// Creates the ActivityListener with the appropriate callbacks.
        /// </summary>
        private ActivityListener CreateListener()
        {
            return new ActivityListener
            {
                ShouldListenTo = ShouldListenTo,
                Sample = Sample,
                ActivityStarted = OnActivityStarted,
                ActivityStopped = OnActivityStopped
            };
        }

        /// <summary>
        /// Determines if the listener should listen to the given ActivitySource.
        /// </summary>
        /// <param name="source">The ActivitySource to check.</param>
        /// <returns>True if the source is "Databricks.Adbc.Driver", false otherwise.</returns>
        private bool ShouldListenTo(ActivitySource source)
        {
            try
            {
                return string.Equals(source.Name, DatabricksActivitySourceName, StringComparison.Ordinal);
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per telemetry requirement
                Debug.WriteLine($"[TRACE] DatabricksActivityListener: Error in ShouldListenTo: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Determines the sampling result for activity creation.
        /// </summary>
        /// <param name="options">The activity creation options.</param>
        /// <returns>
        /// AllDataAndRecorded when telemetry is enabled, None when disabled.
        /// </returns>
        private ActivitySamplingResult Sample(ref ActivityCreationOptions<ActivityContext> options)
        {
            try
            {
                // Check feature flag if provided, otherwise use config
                bool enabled = _featureFlagChecker?.Invoke() ?? _config.Enabled;

                return enabled
                    ? ActivitySamplingResult.AllDataAndRecorded
                    : ActivitySamplingResult.None;
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per telemetry requirement
                // On error, return None (don't sample) as a safe default
                Debug.WriteLine($"[TRACE] DatabricksActivityListener: Error in Sample: {ex.Message}");
                return ActivitySamplingResult.None;
            }
        }

        /// <summary>
        /// Called when an activity starts.
        /// </summary>
        /// <param name="activity">The started activity.</param>
        /// <remarks>
        /// Currently a no-op. The listener primarily processes activities on stop.
        /// All exceptions are caught and logged at TRACE level.
        /// </remarks>
        private void OnActivityStarted(Activity activity)
        {
            try
            {
                // Currently no processing needed on start
                // The listener primarily processes activities when they stop
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per telemetry requirement
                Debug.WriteLine($"[TRACE] DatabricksActivityListener: Error in OnActivityStarted: {ex.Message}");
            }
        }

        /// <summary>
        /// Called when an activity stops.
        /// </summary>
        /// <param name="activity">The stopped activity.</param>
        /// <remarks>
        /// Delegates to MetricsAggregator.ProcessActivity to extract and aggregate metrics.
        /// Only processes activities when telemetry is enabled (checked via feature flag or config).
        /// All exceptions are caught and logged at TRACE level.
        /// </remarks>
        private void OnActivityStopped(Activity activity)
        {
            try
            {
                // Check if telemetry is enabled before processing
                // This is needed because ActivityStopped is called even if Sample returned None
                // (when another listener requested the activity)
                bool enabled = _featureFlagChecker?.Invoke() ?? _config.Enabled;
                if (!enabled)
                {
                    return;
                }

                _aggregator.ProcessActivity(activity);
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per telemetry requirement
                // Use TRACE level to avoid customer anxiety
                Debug.WriteLine($"[TRACE] DatabricksActivityListener: Error in OnActivityStopped: {ex.Message}");
            }
        }

        #endregion
    }
}
