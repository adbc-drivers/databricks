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
    /// Listens to System.Diagnostics.Activity events from 'Databricks.Adbc.Driver' ActivitySource
    /// and delegates to MetricsAggregator for telemetry processing.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This component bridges the Activity infrastructure to the telemetry pipeline. It filters
    /// activities to only listen to the Databricks ActivitySource and respects the telemetry
    /// feature flag to control whether activities are recorded.
    /// </para>
    /// <para>
    /// Key Behaviors:
    /// - Filters to only "Databricks.Adbc.Driver" ActivitySource
    /// - Sample callback respects feature flag (returns AllDataAndRecorded or None)
    /// - ActivityStopped callback delegates to MetricsAggregator.ProcessActivity()
    /// - All callbacks wrapped in try-catch (never throws to Activity infrastructure)
    /// - StopAsync() flushes pending metrics and disposes listener
    /// </para>
    /// <para>
    /// Critical Safety: All exceptions in callbacks are swallowed to ensure we never break
    /// the Activity tracing infrastructure, which would impact driver functionality.
    /// </para>
    /// </remarks>
    internal sealed class DatabricksActivityListener : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// The ActivitySource name for the Databricks ADBC driver.
        /// </summary>
        private const string DatabricksActivitySourceName = "Databricks.Adbc.Driver";

        /// <summary>
        /// Activity source for tracing listener operations.
        /// </summary>
        private static readonly ActivitySource s_activitySource = new ActivitySource("AdbcDrivers.Databricks.ActivityListener");

        private readonly MetricsAggregator _metricsAggregator;
        private readonly TelemetryConfiguration _configuration;
        private readonly ActivityListener _activityListener;
        private volatile bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabricksActivityListener"/> class.
        /// </summary>
        /// <param name="metricsAggregator">The metrics aggregator to delegate activity processing to.</param>
        /// <param name="configuration">The telemetry configuration.</param>
        /// <exception cref="ArgumentNullException">Thrown when metricsAggregator or configuration is null.</exception>
        public DatabricksActivityListener(
            MetricsAggregator metricsAggregator,
            TelemetryConfiguration configuration)
        {
            _metricsAggregator = metricsAggregator ?? throw new ArgumentNullException(nameof(metricsAggregator));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

            // Create and configure the ActivityListener
            _activityListener = new ActivityListener
            {
                ShouldListenTo = ShouldListenTo,
                Sample = Sample,
                ActivityStopped = ActivityStopped
            };

            // Register the listener with the Activity infrastructure
            ActivitySource.AddActivityListener(_activityListener);

            Activity.Current?.AddEvent(new ActivityEvent("telemetry.listener.started",
                tags: new ActivityTagsCollection
                {
                    { "activity_source", DatabricksActivitySourceName },
                    { "enabled", _configuration.Enabled }
                }));
        }

        /// <summary>
        /// Determines whether to listen to a specific ActivitySource.
        /// </summary>
        /// <param name="source">The ActivitySource to evaluate.</param>
        /// <returns>True if the source is the Databricks ADBC driver source; otherwise, false.</returns>
        /// <remarks>
        /// This callback is invoked by the Activity infrastructure and must never throw exceptions.
        /// We only listen to the "Databricks.Adbc.Driver" ActivitySource to avoid processing
        /// activities from other components.
        /// </remarks>
        private bool ShouldListenTo(ActivitySource source)
        {
            try
            {
                bool shouldListen = source.Name == DatabricksActivitySourceName;

                if (shouldListen)
                {
                    Activity.Current?.AddEvent(new ActivityEvent("telemetry.listener.source_matched",
                        tags: new ActivityTagsCollection
                        {
                            { "source_name", source.Name },
                            { "source_version", source.Version ?? "(none)" }
                        }));
                }

                return shouldListen;
            }
            catch (Exception ex)
            {
                // Swallow all exceptions - critical to never throw in Activity callbacks
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.listener.should_listen_error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));

                // Return false on error (fail safe - don't listen if we can't determine)
                return false;
            }
        }

        /// <summary>
        /// Determines whether an Activity should be sampled (recorded).
        /// </summary>
        /// <param name="options">The activity creation options.</param>
        /// <returns>
        /// AllDataAndRecorded if telemetry is enabled; None if disabled.
        /// </returns>
        /// <remarks>
        /// This callback is invoked by the Activity infrastructure and must never throw exceptions.
        /// It respects the feature flag to control whether activities are recorded. When disabled,
        /// activities are not created, reducing performance overhead.
        /// </remarks>
        private ActivitySamplingResult Sample(ref ActivityCreationOptions<ActivityContext> options)
        {
            try
            {
                // Respect the feature flag - return AllDataAndRecorded if enabled, None if disabled
                ActivitySamplingResult result = _configuration.Enabled
                    ? ActivitySamplingResult.AllDataAndRecorded
                    : ActivitySamplingResult.None;

                return result;
            }
            catch (Exception ex)
            {
                // Swallow all exceptions - critical to never throw in Activity callbacks
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.listener.sample_error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));

                // Return None on error (fail safe - don't record if we can't determine)
                return ActivitySamplingResult.None;
            }
        }

        /// <summary>
        /// Handles the ActivityStopped event and delegates to the MetricsAggregator.
        /// </summary>
        /// <param name="activity">The activity that stopped.</param>
        /// <remarks>
        /// This callback is invoked by the Activity infrastructure and must never throw exceptions.
        /// It delegates activity processing to the MetricsAggregator which extracts metrics and
        /// coordinates export through the circuit breaker-protected exporter.
        /// </remarks>
        private void ActivityStopped(Activity activity)
        {
            if (_disposed || activity == null)
            {
                return;
            }

            try
            {
                // Delegate to MetricsAggregator for processing
                _metricsAggregator.ProcessActivity(activity);

                Activity.Current?.AddEvent(new ActivityEvent("telemetry.listener.activity_processed",
                    tags: new ActivityTagsCollection
                    {
                        { "activity.name", activity.DisplayName ?? activity.OperationName },
                        { "activity.duration_ms", activity.Duration.TotalMilliseconds }
                    }));
            }
            catch (Exception ex)
            {
                // Swallow all exceptions - critical to never throw in Activity callbacks
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.listener.activity_stopped_error",
                    tags: new ActivityTagsCollection
                    {
                        { "activity.name", activity.DisplayName ?? activity.OperationName },
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));
            }
        }

        /// <summary>
        /// Stops the listener, flushes pending metrics, and disposes resources asynchronously.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task that completes when the listener is stopped and resources are disposed.</returns>
        public async Task StopAsync(CancellationToken ct = default)
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                // Flush any pending metrics before disposing
                await _metricsAggregator.FlushAsync(ct).ConfigureAwait(false);

                Activity.Current?.AddEvent(new ActivityEvent("telemetry.listener.stopped",
                    tags: new ActivityTagsCollection
                    {
                        { "flushed", true }
                    }));
            }
            catch (Exception ex)
            {
                // Swallow exceptions during stop - log at trace level
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.listener.stop_error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));
            }
            finally
            {
                // Mark as disposed
                _disposed = true;

                // Dispose the activity listener
                _activityListener.Dispose();
            }
        }

        /// <summary>
        /// Disposes the listener synchronously.
        /// </summary>
        /// <remarks>
        /// This synchronous dispose method delegates to StopAsync. For proper resource cleanup,
        /// prefer using DisposeAsync.
        /// </remarks>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                // Call StopAsync synchronously
                StopAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch
            {
                // Swallow exceptions in synchronous dispose
                // Exceptions are already logged in StopAsync
            }
        }

        /// <summary>
        /// Disposes the listener asynchronously.
        /// </summary>
        /// <returns>A ValueTask that completes when disposal is finished.</returns>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                await StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // Swallow exceptions in async dispose
                // Exceptions are already logged in StopAsync
            }
        }
    }
}
