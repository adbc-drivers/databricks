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
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Custom ActivityListener that aggregates metrics from Activity events
    /// and exports them to Databricks telemetry service.
    /// All exceptions are swallowed to prevent impacting driver operations.
    /// </summary>
    internal sealed class DatabricksActivityListener : IDisposable
    {
        private readonly string _host;
        private readonly ITelemetryClient _telemetryClient;
        private readonly TelemetryConfiguration _config;
        private readonly MetricsAggregator _aggregator;
        private ActivityListener? _listener;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the DatabricksActivityListener class.
        /// </summary>
        /// <param name="host">The Databricks host string.</param>
        /// <param name="telemetryClient">The shared telemetry client for this host.</param>
        /// <param name="config">The telemetry configuration.</param>
        public DatabricksActivityListener(
            string host,
            ITelemetryClient telemetryClient,
            TelemetryConfiguration config)
        {
            if (string.IsNullOrEmpty(host))
            {
                throw new ArgumentException("Host cannot be null or empty.", nameof(host));
            }

            _host = host ?? throw new ArgumentNullException(nameof(host));
            _telemetryClient = telemetryClient ?? throw new ArgumentNullException(nameof(telemetryClient));
            _config = config ?? throw new ArgumentNullException(nameof(config));

            // Create the aggregator with a wrapper exporter that delegates to the telemetry client
            var exporter = new TelemetryClientExporter(_telemetryClient);
            _aggregator = new MetricsAggregator(exporter, _config);
        }

        /// <summary>
        /// Starts listening to activities from the Databricks.Adbc.Driver ActivitySource.
        /// </summary>
        public void Start()
        {
            if (_listener != null)
            {
                // Already started
                return;
            }

            _listener = CreateListener();
            ActivitySource.AddActivityListener(_listener);
        }

        /// <summary>
        /// Stops listening and flushes pending metrics.
        /// All exceptions are swallowed and logged at TRACE level.
        /// </summary>
        /// <returns>Task representing the async operation.</returns>
        public async Task StopAsync()
        {
            try
            {
                if (_listener != null)
                {
                    _listener.Dispose();
                    _listener = null;
                }

                // Flush pending metrics before stopping
                await _aggregator.FlushAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] Error stopping activity listener: {ex.Message}");
            }
        }

        /// <summary>
        /// Disposes the activity listener and releases resources.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            try
            {
                // Stop the listener and flush metrics
                StopAsync().GetAwaiter().GetResult();

                // Dispose the aggregator
                _aggregator?.Dispose();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] Error disposing activity listener: {ex.Message}");
            }
        }

        // Private helper methods

        private ActivityListener CreateListener()
        {
            return new ActivityListener
            {
                // Filter to only listen to Databricks.Adbc.Driver ActivitySource
                ShouldListenTo = source =>
                    source.Name == "Databricks.Adbc.Driver",

                // Sample callback respects feature flag to enable/disable collection
                Sample = (ref ActivityCreationOptions<ActivityContext> options) =>
                    _config.Enabled ? ActivitySamplingResult.AllDataAndRecorded
                                    : ActivitySamplingResult.None,

                // ActivityStarted callback (currently no-op, can be used for future enhancements)
                ActivityStarted = OnActivityStarted,

                // ActivityStopped callback extracts metrics
                ActivityStopped = OnActivityStopped
            };
        }

        private void OnActivityStarted(Activity activity)
        {
            // Currently no processing needed on activity start
            // This can be used for future enhancements if needed
            try
            {
                // Placeholder for future logic
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per requirement
                Debug.WriteLine($"[TRACE] Telemetry listener error in OnActivityStarted: {ex.Message}");
            }
        }

        private void OnActivityStopped(Activity activity)
        {
            try
            {
                // Delegate to aggregator for processing
                _aggregator.ProcessActivity(activity);
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per requirement
                // Use TRACE level to avoid customer anxiety
                Debug.WriteLine($"[TRACE] Telemetry listener error in OnActivityStopped: {ex.Message}");
            }
        }

        /// <summary>
        /// Internal wrapper that adapts ITelemetryClient to ITelemetryExporter interface.
        /// </summary>
        private sealed class TelemetryClientExporter : ITelemetryExporter
        {
            private readonly ITelemetryClient _client;

            public TelemetryClientExporter(ITelemetryClient client)
            {
                _client = client ?? throw new ArgumentNullException(nameof(client));
            }

            public async Task ExportAsync(
                System.Collections.Generic.IReadOnlyList<TelemetryMetric> metrics,
                System.Threading.CancellationToken ct = default)
            {
                await _client.ExportAsync(metrics, ct).ConfigureAwait(false);
            }
        }
    }
}
