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
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Singleton factory that manages one <see cref="ITelemetryClient"/> per host.
    /// Prevents rate limiting by sharing clients across connections to the same host.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Large customers may open many parallel connections to the same host. This manager
    /// ensures that all connections to a given host share a single telemetry client, which
    /// batches events from all connections and avoids multiple concurrent flushes that could
    /// trigger rate limiting.
    /// </para>
    /// <para>
    /// Reference counting tracks active connections. When the last connection to a host is
    /// closed, the telemetry client is shut down gracefully via <see cref="ITelemetryClient.CloseAsync"/>.
    /// </para>
    /// <para>
    /// Host matching is case-insensitive since DNS hostnames are case-insensitive.
    /// </para>
    /// </remarks>
    internal sealed class TelemetryClientManager
    {
        private static TelemetryClientManager s_instance = new TelemetryClientManager();

        private readonly ConcurrentDictionary<string, TelemetryClientHolder> _clients =
            new ConcurrentDictionary<string, TelemetryClientHolder>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Private constructor to enforce singleton pattern.
        /// </summary>
        private TelemetryClientManager()
        {
        }

        /// <summary>
        /// Gets the singleton instance of <see cref="TelemetryClientManager"/>.
        /// </summary>
        /// <returns>The singleton <see cref="TelemetryClientManager"/> instance.</returns>
        public static TelemetryClientManager GetInstance() => s_instance;

        /// <summary>
        /// Gets or creates a telemetry client for the specified host.
        /// If a client already exists for the host, its reference count is incremented
        /// and the same client is returned. Otherwise, a new client is created using the
        /// provided exporter factory and configuration.
        /// </summary>
        /// <param name="host">The host identifier (e.g., "workspace.databricks.com").</param>
        /// <param name="exporterFactory">
        /// A factory function that creates an <see cref="ITelemetryExporter"/> for the new client.
        /// Only called when a new client needs to be created (not for existing hosts).
        /// </param>
        /// <param name="config">The telemetry configuration for the new client.</param>
        /// <returns>The shared <see cref="ITelemetryClient"/> for the specified host.</returns>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="host"/>, <paramref name="exporterFactory"/>,
        /// or <paramref name="config"/> is null.
        /// </exception>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="host"/> is empty or whitespace.
        /// </exception>
        public ITelemetryClient GetOrCreateClient(
            string host,
            Func<ITelemetryExporter> exporterFactory,
            TelemetryConfiguration config)
        {
            if (host == null)
            {
                throw new ArgumentNullException(nameof(host));
            }

            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be empty or whitespace.", nameof(host));
            }

            if (exporterFactory == null)
            {
                throw new ArgumentNullException(nameof(exporterFactory));
            }

            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            TelemetryClientHolder holder = _clients.AddOrUpdate(
                host,
                // Add factory: create a new holder with a new TelemetryClient
                _ =>
                {
                    ITelemetryExporter exporter = exporterFactory();
                    ITelemetryClient client = new TelemetryClient(exporter, config);
                    return new TelemetryClientHolder(client);
                },
                // Update factory: increment reference count on existing holder
                (_, existingHolder) =>
                {
                    Interlocked.Increment(ref existingHolder._refCount);
                    return existingHolder;
                });

            return holder.Client;
        }

        /// <summary>
        /// Decrements the reference count for the specified host.
        /// When the reference count reaches zero, the client is removed from the cache
        /// and <see cref="ITelemetryClient.CloseAsync"/> is called to gracefully shut it down.
        /// </summary>
        /// <param name="host">The host identifier.</param>
        /// <returns>A task that completes when the release operation has finished.</returns>
        /// <remarks>
        /// This method is safe to call with an unknown host; it simply does nothing.
        /// All exceptions from <see cref="ITelemetryClient.CloseAsync"/> are swallowed
        /// to follow the telemetry design principle that telemetry should never impact
        /// driver operations.
        /// </remarks>
        public async Task ReleaseClientAsync(string host)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                return;
            }

            if (!_clients.TryGetValue(host, out TelemetryClientHolder? holder))
            {
                return;
            }

            int newRefCount = Interlocked.Decrement(ref holder._refCount);

            if (newRefCount <= 0)
            {
                // Remove from cache. Use TryRemove to handle concurrent removal.
                if (_clients.TryRemove(host, out TelemetryClientHolder? removedHolder))
                {
                    try
                    {
                        await removedHolder.Client.CloseAsync().ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"[TRACE] TelemetryClientManager.ReleaseClientAsync error closing client for host '{host}': {ex.Message}");
                    }
                }
            }
        }

        /// <summary>
        /// Replaces the singleton instance with a test instance for the duration of the returned
        /// <see cref="IDisposable"/> scope. Disposing the returned object restores the original instance.
        /// </summary>
        /// <param name="testInstance">The test instance to use as the singleton.</param>
        /// <returns>An <see cref="IDisposable"/> that restores the original instance when disposed.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="testInstance"/> is null.</exception>
        /// <remarks>
        /// This method is intended for testing purposes only. It allows tests to inject a custom
        /// <see cref="TelemetryClientManager"/> instance to isolate test state.
        /// </remarks>
        internal static IDisposable UseTestInstance(TelemetryClientManager testInstance)
        {
            if (testInstance == null)
            {
                throw new ArgumentNullException(nameof(testInstance));
            }

            TelemetryClientManager original = s_instance;
            s_instance = testInstance;
            return new TestInstanceScope(original);
        }

        /// <summary>
        /// Resets the manager by closing and removing all clients.
        /// This is primarily intended for testing purposes.
        /// </summary>
        internal void Reset()
        {
            // Snapshot and clear all holders
            foreach (System.Collections.Generic.KeyValuePair<string, TelemetryClientHolder> kvp in _clients)
            {
                if (_clients.TryRemove(kvp.Key, out TelemetryClientHolder? holder))
                {
                    try
                    {
                        holder.Client.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"[TRACE] TelemetryClientManager.Reset error closing client for host '{kvp.Key}': {ex.Message}");
                    }
                }
            }
        }

        /// <summary>
        /// Creates a new <see cref="TelemetryClientManager"/> instance for testing.
        /// </summary>
        /// <returns>A new <see cref="TelemetryClientManager"/> instance.</returns>
        internal static TelemetryClientManager CreateForTesting()
        {
            return new TelemetryClientManager();
        }

        /// <summary>
        /// Disposable scope that restores the original singleton instance when disposed.
        /// </summary>
        private sealed class TestInstanceScope : IDisposable
        {
            private readonly TelemetryClientManager _original;
            private bool _disposed;

            public TestInstanceScope(TelemetryClientManager original)
            {
                _original = original;
            }

            public void Dispose()
            {
                if (!_disposed)
                {
                    s_instance = _original;
                    _disposed = true;
                }
            }
        }
    }
}
