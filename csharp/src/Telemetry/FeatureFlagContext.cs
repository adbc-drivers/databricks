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
using System.Threading;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Holds feature flag state and reference count for a host.
    /// </summary>
    /// <remarks>
    /// Each host (Databricks workspace) has one FeatureFlagContext instance
    /// that is shared across all connections to that host. The context tracks:
    /// - Cached telemetry enabled state
    /// - When the cache was last refreshed
    /// - Reference count for proper cleanup
    ///
    /// Thread-safety is ensured using Interlocked operations for the reference count
    /// and lock-based synchronization for the cached value updates.
    /// </remarks>
    internal sealed class FeatureFlagContext
    {
        /// <summary>
        /// Default cache duration (15 minutes).
        /// </summary>
        public static readonly TimeSpan DefaultCacheDuration = TimeSpan.FromMinutes(15);

        private readonly object _lock = new object();
        private bool? _telemetryEnabled;
        private DateTime? _lastFetched;
        private int _refCount;

        /// <summary>
        /// Gets the cache duration for feature flags.
        /// </summary>
        public TimeSpan CacheDuration { get; }

        /// <summary>
        /// Gets the current reference count (number of connections using this context).
        /// </summary>
        public int RefCount => Volatile.Read(ref _refCount);

        /// <summary>
        /// Creates a new FeatureFlagContext with default cache duration (15 minutes).
        /// </summary>
        public FeatureFlagContext()
            : this(DefaultCacheDuration)
        {
        }

        /// <summary>
        /// Creates a new FeatureFlagContext with the specified cache duration.
        /// </summary>
        /// <param name="cacheDuration">The duration to cache feature flag values.</param>
        public FeatureFlagContext(TimeSpan cacheDuration)
        {
            if (cacheDuration <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(cacheDuration), "Cache duration must be greater than zero.");
            }

            CacheDuration = cacheDuration;
            _refCount = 0;
        }

        /// <summary>
        /// Gets the cached telemetry enabled value, or null if not cached.
        /// </summary>
        public bool? TelemetryEnabled
        {
            get
            {
                lock (_lock)
                {
                    return _telemetryEnabled;
                }
            }
        }

        /// <summary>
        /// Gets the timestamp when the cache was last fetched, or null if never fetched.
        /// </summary>
        public DateTime? LastFetched
        {
            get
            {
                lock (_lock)
                {
                    return _lastFetched;
                }
            }
        }

        /// <summary>
        /// Gets whether the cached value has expired and needs to be refreshed.
        /// </summary>
        /// <remarks>
        /// Returns true if:
        /// - The cache has never been fetched (LastFetched is null)
        /// - The cache duration has elapsed since LastFetched
        /// </remarks>
        public bool IsExpired
        {
            get
            {
                lock (_lock)
                {
                    if (_lastFetched == null)
                    {
                        return true;
                    }

                    return DateTime.UtcNow - _lastFetched.Value > CacheDuration;
                }
            }
        }

        /// <summary>
        /// Updates the cached telemetry enabled value.
        /// </summary>
        /// <param name="enabled">Whether telemetry is enabled.</param>
        public void SetTelemetryEnabled(bool enabled)
        {
            lock (_lock)
            {
                _telemetryEnabled = enabled;
                _lastFetched = DateTime.UtcNow;
            }
        }

        /// <summary>
        /// Gets the cached value if not expired, otherwise returns null.
        /// </summary>
        /// <param name="value">The cached value if not expired.</param>
        /// <returns>True if a valid cached value was returned, false if expired or not cached.</returns>
        public bool TryGetCachedValue(out bool value)
        {
            lock (_lock)
            {
                value = false;

                if (_telemetryEnabled == null || _lastFetched == null)
                {
                    return false;
                }

                if (DateTime.UtcNow - _lastFetched.Value > CacheDuration)
                {
                    return false;
                }

                value = _telemetryEnabled.Value;
                return true;
            }
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
        /// Resets the cache, clearing the cached value and last fetched time.
        /// Does not affect the reference count.
        /// </summary>
        internal void ResetCache()
        {
            lock (_lock)
            {
                _telemetryEnabled = null;
                _lastFetched = null;
            }
        }
    }
}
