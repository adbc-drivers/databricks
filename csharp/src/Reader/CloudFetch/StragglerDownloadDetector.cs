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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tracing;

namespace AdbcDrivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Detects straggler downloads based on median throughput analysis.
    /// </summary>
    internal class StragglerDownloadDetector : IDisposable
    {
        // Timing constants for monitoring
        private static readonly TimeSpan StragglerMonitoringInterval = TimeSpan.FromSeconds(2);

        private readonly CloudFetchStragglerMitigationConfig _config;
        private readonly IActivityTracer _activityTracer;
        private long _totalStragglersDetectedInQuery;

        private readonly ConcurrentDictionary<long, FileDownloadMetrics> _activeDownloadMetrics;
        private readonly ConcurrentDictionary<long, FileDownloadMetrics> _completedDownloadMetrics;
        private readonly ConcurrentDictionary<long, CancellationTokenSource> _perFileDownloadCancellationTokens;
        private readonly ConcurrentDictionary<long, bool> _alreadyCountedStragglers;
        private readonly SemaphoreSlim _sequentialSemaphore;

        private Task? _monitoringTask;
        private CancellationTokenSource? _monitoringCts;
        private volatile bool _hasTriggeredSequentialDownloadFallback;

        /// <summary>
        /// Initializes a new instance of the <see cref="StragglerDownloadDetector"/> class.
        /// </summary>
        /// <param name="config">Straggler mitigation configuration.</param>
        /// <param name="activityTracer">Activity tracer for telemetry.</param>
        public StragglerDownloadDetector(
            CloudFetchStragglerMitigationConfig config,
            IActivityTracer activityTracer)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _activityTracer = activityTracer ?? throw new ArgumentNullException(nameof(activityTracer));

            if (!config.Enabled)
            {
                throw new ArgumentException("Cannot create detector with disabled config", nameof(config));
            }

            // Validate configuration parameters
            if (config.Multiplier <= 1.0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(config),
                    config.Multiplier,
                    "Straggler throughput multiplier must be greater than 1.0");
            }

            if (config.Quantile <= 0.0 || config.Quantile > 1.0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(config),
                    config.Quantile,
                    "Minimum completion quantile must be between 0.0 and 1.0");
            }

            if (config.Padding < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(config),
                    config.Padding,
                    "Straggler detection padding must be non-negative");
            }

            if (config.MaxStragglersBeforeFallback < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(config),
                    config.MaxStragglersBeforeFallback,
                    "Max stragglers before fallback must be non-negative");
            }

            // Initialize tracking dictionaries
            _activeDownloadMetrics = new ConcurrentDictionary<long, FileDownloadMetrics>();
            _completedDownloadMetrics = new ConcurrentDictionary<long, FileDownloadMetrics>();
            _perFileDownloadCancellationTokens = new ConcurrentDictionary<long, CancellationTokenSource>();
            _alreadyCountedStragglers = new ConcurrentDictionary<long, bool>();
            _sequentialSemaphore = new SemaphoreSlim(1, 1);
            _totalStragglersDetectedInQuery = 0;
            _hasTriggeredSequentialDownloadFallback = false;
        }

        /// <summary>
        /// Gets a value indicating whether the query should fall back to sequential downloads
        /// due to exceeding the maximum straggler threshold.
        /// </summary>
        public bool ShouldFallbackToSequentialDownloads =>
            _totalStragglersDetectedInQuery >= (_config.SynchronousFallbackEnabled ? _config.MaxStragglersBeforeFallback : long.MaxValue);

        /// <summary>
        /// Starts the straggler monitoring background task.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for monitoring.</param>
        public void StartMonitoring(CancellationToken cancellationToken)
        {
            if (_monitoringTask != null)
            {
                throw new InvalidOperationException("Monitoring is already started");
            }

            _monitoringCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _monitoringTask = MonitoringLoopAsync(_monitoringCts.Token);

            Activity.Current?.AddEvent("cloudfetch.straggler_monitoring_started", [
                new("multiplier", _config.Multiplier),
                new("quantile", _config.Quantile),
                new("padding_seconds", _config.Padding.TotalSeconds),
                new("monitoring_interval_seconds", StragglerMonitoringInterval.TotalSeconds),
                new("max_stragglers_before_fallback", _config.MaxStragglersBeforeFallback),
                new("synchronous_fallback_enabled", _config.SynchronousFallbackEnabled)
            ]);
        }

        /// <summary>
        /// Stops the straggler monitoring background task and cleans up resources.
        /// </summary>
        public async Task StopMonitoring()
        {
            if (_monitoringTask == null)
            {
                return;
            }

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            int activeDownloadsAtStop = _activeDownloadMetrics.Count;
            long totalStragglersDetected = GetTotalStragglersDetectedInQuery();
            bool fallbackWasTriggered = _hasTriggeredSequentialDownloadFallback;

            _monitoringCts?.Cancel();

            try
            {
                await _monitoringTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation is requested
            }
            catch (Exception ex)
            {
                Activity.Current?.AddEvent("cloudfetch.straggler_monitoring_stop_error", [
                    new("error_message", ex.Message),
                    new("error_type", ex.GetType().Name)
                ]);
            }
            finally
            {
                _monitoringCts?.Dispose();
                _monitoringCts = null;
                _monitoringTask = null;
            }

            // Cleanup remaining per-file cancellation tokens
            foreach (var cts in _perFileDownloadCancellationTokens.Values)
            {
                cts?.Dispose();
            }
            _perFileDownloadCancellationTokens.Clear();

            // Clear completed download metrics history
            _completedDownloadMetrics.Clear();

            stopwatch.Stop();

            Activity.Current?.AddEvent("cloudfetch.straggler_monitoring_stopped", [
                new("total_stragglers_detected", totalStragglersDetected),
                new("sequential_fallback_triggered", fallbackWasTriggered),
                new("active_downloads_at_stop", activeDownloadsAtStop),
                new("cleanup_duration_ms", stopwatch.ElapsedMilliseconds)
            ]);
        }

        /// <summary>
        /// Registers a new download and returns the cancellation token to use for it.
        /// </summary>
        /// <param name="fileOffset">File offset identifier.</param>
        /// <param name="fileSizeBytes">Size of the file in bytes.</param>
        /// <param name="globalCancellationToken">Global cancellation token to link with.</param>
        /// <param name="activity">Optional activity for tracing.</param>
        /// <returns>Cancellation token for this specific download.</returns>
        public CancellationToken RegisterDownload(long fileOffset, long fileSizeBytes, CancellationToken globalCancellationToken, Activity? activity = null)
        {
            // Check if this download was previously identified as a straggler
            // If so, preserve the flag to prevent re-detection on retry
            bool wasPreviouslyCancelled = _alreadyCountedStragglers.ContainsKey(fileOffset);

            var metrics = new FileDownloadMetrics(fileOffset, fileSizeBytes);

            // Preserve the cancelled flag for retries (used for tracing/debugging)
            if (wasPreviouslyCancelled)
            {
                metrics.MarkCancelledAsStragler();
            }

            _activeDownloadMetrics[fileOffset] = metrics;

            // Dispose old CTS if exists (retry scenario after straggler cancellation)
            bool isRetry = false;
            if (_perFileDownloadCancellationTokens.TryGetValue(fileOffset, out var oldCts))
            {
                oldCts?.Dispose();
                isRetry = true;
            }

            var cts = CancellationTokenSource.CreateLinkedTokenSource(globalCancellationToken);
            _perFileDownloadCancellationTokens[fileOffset] = cts;

            activity?.AddEvent("cloudfetch.straggler_download_registered", [
                new("offset", fileOffset),
                new("size_bytes", fileSizeBytes),
                new("size_mb", fileSizeBytes / 1024.0 / 1024.0),
                new("is_retry", isRetry),
                new("active_downloads", _activeDownloadMetrics.Count)
            ]);

            return cts.Token;
        }

        /// <summary>
        /// Marks a download as completed.
        /// </summary>
        /// <param name="fileOffset">File offset identifier.</param>
        /// <param name="activity">Optional activity for tracing.</param>
        public void MarkCompleted(long fileOffset, Activity? activity = null)
        {
            if (_activeDownloadMetrics.TryGetValue(fileOffset, out var metrics))
            {
                metrics.MarkDownloadCompleted();

                var throughput = metrics.CalculateThroughputBytesPerSecond();
                var duration = metrics.DownloadEndTime.HasValue
                    ? (metrics.DownloadEndTime.Value - metrics.DownloadStartTime).TotalMilliseconds
                    : 0;

                activity?.AddEvent("cloudfetch.straggler_download_completed", [
                    new("offset", fileOffset),
                    new("duration_ms", duration),
                    new("throughput_mbps", throughput.HasValue ? throughput.Value / (1024.0 * 1024.0) : 0),
                    new("was_cancelled_as_straggler", metrics.WasCancelledAsStragler)
                ]);
            }

            // Schedule cleanup after delay
            ScheduleCleanup(fileOffset);
        }

        /// <summary>
        /// Marks a download as cancelled due to being identified as a straggler.
        /// </summary>
        /// <param name="fileOffset">File offset identifier.</param>
        /// <param name="activity">Optional activity for tracing.</param>
        public void MarkCancelledAsStragler(long fileOffset, Activity? activity = null)
        {
            if (_activeDownloadMetrics.TryGetValue(fileOffset, out var metrics))
            {
                metrics.MarkCancelledAsStragler();

                var elapsed = (DateTime.UtcNow - metrics.DownloadStartTime).TotalMilliseconds;

                activity?.AddEvent("cloudfetch.straggler_download_marked_cancelled", [
                    new("offset", fileOffset),
                    new("elapsed_ms", elapsed),
                    new("total_stragglers_count", GetTotalStragglersDetectedInQuery())
                ]);
            }
        }

        /// <summary>
        /// Acquires sequential download permit if fallback mode is active.
        /// Returns a permit that must be disposed to release the sequential semaphore.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <param name="activity">Optional activity for tracing.</param>
        /// <returns>A permit that releases the sequential semaphore on disposal.</returns>
        public async Task<SequentialDownloadPermit> AcquireSequentialPermitIfNeeded(
            CancellationToken cancellationToken,
            Activity? activity = null)
        {
            if (ShouldFallbackToSequentialDownloads)
            {
                activity?.AddEvent("cloudfetch.sequential_permit_wait_start", [
                    new("total_stragglers", GetTotalStragglersDetectedInQuery())
                ]);

                await _sequentialSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                return new SequentialDownloadPermit(_sequentialSemaphore);
            }
            return SequentialDownloadPermit.NoOp;
        }

        /// <summary>
        /// Identifies straggler downloads from a collection of download metrics.
        /// </summary>
        internal IEnumerable<long> IdentifyStragglerDownloads(
            IReadOnlyList<FileDownloadMetrics> allDownloadMetrics,
            DateTime currentTime,
            ConcurrentDictionary<long, bool>? alreadyCounted = null)
        {
            if (allDownloadMetrics == null || allDownloadMetrics.Count == 0)
            {
                return Enumerable.Empty<long>();
            }

            // Determine which dictionary to use for tracking already-detected stragglers
            var countingDict = alreadyCounted ?? _alreadyCountedStragglers;

            // Get completed downloads from both active metrics (just completed) and historical completed metrics
            var completedFromActive = allDownloadMetrics.Where(m => m.IsDownloadCompleted).ToList();
            var completedFromHistory = _completedDownloadMetrics.Values.ToList();
            var completedDownloads = completedFromActive.Concat(completedFromHistory).ToList();

            // Filter out downloads that were already detected as stragglers (prevents re-detection on retry)
            var activeDownloads = allDownloadMetrics.Where(m => !m.IsDownloadCompleted && !countingDict.ContainsKey(m.FileOffset)).ToList();

            if (activeDownloads.Count == 0)
            {
                return Enumerable.Empty<long>();
            }

            // Check if we have enough completed downloads to calculate median
            int totalDownloads = allDownloadMetrics.Count + _completedDownloadMetrics.Count;
            int requiredCompletions = (int)Math.Ceiling(totalDownloads * _config.Quantile);

            if (completedDownloads.Count < requiredCompletions)
            {
                return Enumerable.Empty<long>();
            }

            // Calculate median throughput from completed downloads
            double medianThroughput = CalculateMedianThroughput(completedDownloads);

            if (medianThroughput <= 0)
            {
                return Enumerable.Empty<long>();
            }

            // Identify stragglers
            var stragglers = new List<long>();

            foreach (var download in activeDownloads)
            {
                TimeSpan elapsed = currentTime - download.DownloadStartTime;
                double elapsedSeconds = elapsed.TotalSeconds;

                // Calculate expected time: (multiplier × fileSize / medianThroughput) + padding
                double expectedSeconds = (_config.Multiplier * download.FileSizeBytes / medianThroughput)
                    + _config.Padding.TotalSeconds;

                if (elapsedSeconds > expectedSeconds)
                {
                    stragglers.Add(download.FileOffset);

                    // Only increment counter if not already counted (prevents duplicate counting on retries)
                    if (countingDict.TryAdd(download.FileOffset, true))
                    {
                        Interlocked.Increment(ref _totalStragglersDetectedInQuery);
                    }
                }
            }

            return stragglers;
        }

        private IEnumerable<long> IdentifyStragglerDownloads(DateTime currentTime, Activity? activity = null)
        {
            var allDownloadMetrics = _activeDownloadMetrics.Values.ToList();

            if (allDownloadMetrics.Count == 0)
            {
                return Enumerable.Empty<long>();
            }

            // Delegate to internal method - it handles all the detection logic
            var stragglers = IdentifyStragglerDownloads(allDownloadMetrics, currentTime, _alreadyCountedStragglers).ToList();

            // Add tracing only if activity is provided
            if (activity != null)
            {
                // Calculate metrics for tracing only when needed
                var completedDownloads = allDownloadMetrics.Where(m => m.IsDownloadCompleted).ToList();
                var activeDownloads = allDownloadMetrics.Where(m => !m.IsDownloadCompleted && !m.WasCancelledAsStragler).ToList();
                int requiredCompletions = (int)Math.Ceiling(allDownloadMetrics.Count * _config.Quantile);

                if (completedDownloads.Count >= requiredCompletions && activeDownloads.Count > 0)
                {
                    double medianThroughput = CalculateMedianThroughput(completedDownloads);

                    if (medianThroughput > 0)
                    {
                        activity.AddEvent("cloudfetch.straggler_detection_check", [
                            new("completed_downloads", completedDownloads.Count),
                            new("active_downloads", activeDownloads.Count),
                            new("median_throughput_mbps", medianThroughput / (1024.0 * 1024.0))
                        ]);

                        // Add per-straggler identification tracing
                        foreach (var offset in stragglers)
                        {
                            var download = allDownloadMetrics.FirstOrDefault(m => m.FileOffset == offset);
                            if (download != null)
                            {
                                TimeSpan elapsed = currentTime - download.DownloadStartTime;
                                double elapsedSeconds = elapsed.TotalSeconds;

                                activity.AddEvent("cloudfetch.straggler_identified", [
                                    new("offset", download.FileOffset),
                                    new("elapsed_seconds", elapsedSeconds),
                                    new("file_size_mb", download.FileSizeBytes / (1024.0 / 1024.0))
                                ]);
                            }
                        }
                    }
                }
            }

            return stragglers;
        }

        /// <summary>
        /// Gets the total number of stragglers detected in the current query.
        /// </summary>
        /// <returns>The total straggler count.</returns>
        public long GetTotalStragglersDetectedInQuery()
        {
            return Interlocked.Read(ref _totalStragglersDetectedInQuery);
        }

        /// <summary>
        /// Calculates the median throughput from a collection of completed downloads.
        /// </summary>
        /// <param name="completedDownloads">Completed download metrics.</param>
        /// <returns>Median throughput in bytes per second.</returns>
        private double CalculateMedianThroughput(List<FileDownloadMetrics> completedDownloads)
        {
            if (completedDownloads.Count == 0)
            {
                return 0;
            }

            var throughputs = completedDownloads
                .Select(m => m.CalculateThroughputBytesPerSecond())
                .Where(t => t.HasValue && t.Value > 0)
                .Select(t => t!.Value)  // Null-forgiving operator: We know it's not null due to Where filter
                .OrderBy(t => t)
                .ToList();

            if (throughputs.Count == 0)
            {
                return 0;
            }

            int count = throughputs.Count;
            if (count % 2 == 1)
            {
                // Odd count: return middle element
                return throughputs[count / 2];
            }
            else
            {
                // Even count: return average of two middle elements
                int midIndex = count / 2;
                return (throughputs[midIndex - 1] + throughputs[midIndex]) / 2.0;
            }
        }

        /// <summary>
        /// Background monitoring loop that periodically checks for straggler downloads.
        /// </summary>
        private async Task MonitoringLoopAsync(CancellationToken cancellationToken)
        {
            await _activityTracer.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("straggler.monitoring_interval_seconds", StragglerMonitoringInterval.TotalSeconds);
                activity?.SetTag("straggler.enabled", true);
                activity?.SetTag("straggler.multiplier", _config.Multiplier);
                activity?.SetTag("straggler.quantile", _config.Quantile);
                activity?.SetTag("straggler.padding_seconds", _config.Padding.TotalSeconds);

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(StragglerMonitoringInterval, cancellationToken).ConfigureAwait(false);

                        // Check for fallback condition
                        if (ShouldFallbackToSequentialDownloads && !_hasTriggeredSequentialDownloadFallback)
                        {
                            _hasTriggeredSequentialDownloadFallback = true;
                            activity?.AddEvent("cloudfetch.sequential_fallback_triggered", [
                                new("total_stragglers_in_query", GetTotalStragglersDetectedInQuery()),
                                new("new_parallelism", 1)
                            ]);
                        }

                        // Identify stragglers
                        var stragglerOffsets = IdentifyStragglerDownloads(DateTime.UtcNow, activity);
                        var stragglerList = stragglerOffsets.ToList();

                        if (stragglerList.Count > 0)
                        {
                            var metricsSnapshot = _activeDownloadMetrics.Values.ToList();
                            activity?.AddEvent("cloudfetch.straggler_check", [
                                new("active_downloads", metricsSnapshot.Count(m => !m.IsDownloadCompleted)),
                                new("completed_downloads", metricsSnapshot.Count(m => m.IsDownloadCompleted)),
                                new("stragglers_identified", stragglerList.Count)
                            ]);

                            foreach (long offset in stragglerList)
                            {
                                if (_perFileDownloadCancellationTokens.TryGetValue(offset, out var cts))
                                {
                                    activity?.AddEvent("cloudfetch.straggler_cancelling", [
                                        new("offset", offset)
                                    ]);

                                    try
                                    {
                                        cts.Cancel();
                                    }
                                    catch (ObjectDisposedException)
                                    {
                                        // Expected race condition: CTS was disposed between TryGetValue and Cancel
                                        // This is harmless - the download has already completed
                                    }
                                }
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected when stopping
                        break;
                    }
                    catch (Exception ex)
                    {
                        activity?.AddException(ex, [new("error.context", "cloudfetch.straggler_monitoring_error")]);
                        // Continue monitoring despite errors
                    }
                }
            }, activityName: "MonitorStragglerDownloads");
        }

        /// <summary>
        /// Performs immediate cleanup when download completes.
        /// Moves metrics from active to completed for median throughput calculation.
        /// Note: _alreadyCountedStragglers is NOT cleaned up here - it persists for the entire query
        /// to prevent re-detection of stragglers on retry.
        /// </summary>
        private void ScheduleCleanup(long fileOffset)
        {
            // Move completed metrics to completed dictionary for median calculation
            if (_activeDownloadMetrics.TryRemove(fileOffset, out var metrics))
            {
                // Only store if actually completed (not just cancelled without completion)
                if (metrics.IsDownloadCompleted)
                {
                    _completedDownloadMetrics[fileOffset] = metrics;
                }
            }

            // Immediately remove and dispose CTS
            // Race condition with monitoring thread is handled by try-catch in MonitoringLoopAsync
            if (_perFileDownloadCancellationTokens.TryRemove(fileOffset, out var cts))
            {
                cts?.Dispose();
            }
        }

        public void Dispose()
        {
            _monitoringCts?.Cancel();
            _monitoringCts?.Dispose();

            foreach (var cts in _perFileDownloadCancellationTokens.Values)
            {
                cts?.Dispose();
            }
            _perFileDownloadCancellationTokens.Clear();

            _sequentialSemaphore?.Dispose();
        }
    }

    /// <summary>
    /// RAII-style permit holder that releases sequential semaphore on disposal.
    /// Thread-safe and idempotent - can be disposed multiple times safely.
    /// </summary>
    internal sealed class SequentialDownloadPermit : IDisposable
    {
        private readonly SemaphoreSlim? _semaphore;
        private int _disposed;

        internal SequentialDownloadPermit(SemaphoreSlim? semaphore)
        {
            _semaphore = semaphore;
            _disposed = 0;
        }

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) == 0)
            {
                _semaphore?.Release();
            }
        }

        /// <summary>
        /// Singleton instance for no-op permits (when sequential mode is not active).
        /// </summary>
        internal static readonly SequentialDownloadPermit NoOp = new SequentialDownloadPermit(null);
    }
}
