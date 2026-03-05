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
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Singleton factory that manages one telemetry client per host.
    /// Prevents rate limiting by sharing clients across concurrent connections to the same host.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Large customers (e.g., Celonis) open many parallel connections to the same host.
    /// Without per-host client sharing, each connection would create its own telemetry client,
    /// resulting in multiple concurrent flush operations that could trigger rate limiting.
    /// </para>
    /// <para>
    /// This manager maintains a single ITelemetryClient per host, shared across all connections
    /// to that host. Reference counting ensures proper cleanup: the client is only closed and
    /// removed when the last connection to that host is closed.
    /// </para>
    /// <para>
    /// Thread Safety: All methods are thread-safe and can be called concurrently from multiple
    /// connections. Uses ConcurrentDictionary and Interlocked operations for synchronization.
    /// </para>
    /// </remarks>
    internal sealed class TelemetryClientManager
    {
        private static readonly TelemetryClientManager s_instance = new TelemetryClientManager();

        private readonly Dictionary<string, TelemetryClientHolder> _clients = new Dictionary<string, TelemetryClientHolder>();
        private readonly object _lock = new object();

        /// <summary>
        /// Private constructor to enforce singleton pattern.
        /// </summary>
        private TelemetryClientManager()
        {
        }

        /// <summary>
        /// Internal constructor for testing. Allows creating non-singleton instances.
        /// </summary>
        internal TelemetryClientManager(bool forTesting)
        {
        }

        /// <summary>
        /// Gets the singleton instance of TelemetryClientManager.
        /// </summary>
        /// <returns>The singleton TelemetryClientManager instance.</returns>
        public static TelemetryClientManager GetInstance() => s_instance;

        /// <summary>
        /// Gets or creates a telemetry client for the specified host.
        /// Increments the reference count if the client already exists.
        /// </summary>
        /// <param name="host">The host identifier (e.g., "databricks-workspace.cloud.databricks.com").</param>
        /// <param name="exporterFactory">Factory function to create an ITelemetryExporter. Called only if a new client needs to be created.</param>
        /// <param name="config">Telemetry configuration for the client.</param>
        /// <returns>The telemetry client for the specified host.</returns>
        /// <remarks>
        /// <para>
        /// Thread Safety: This method is thread-safe. Uses a lock to ensure atomicity of
        /// the lookup-or-create and reference count increment operations, preventing race
        /// conditions with concurrent ReleaseClientAsync calls.
        /// </para>
        /// <para>
        /// Reference Counting: The first call creates a client with RefCount=1. Subsequent calls
        /// for the same host return the existing client and increment the reference count.
        /// </para>
        /// <para>
        /// The exporterFactory is only invoked when creating a new client, not when returning
        /// an existing client. This avoids unnecessary object creation.
        /// </para>
        /// </remarks>
        public ITelemetryClient GetOrCreateClient(
            string host,
            Func<ITelemetryExporter> exporterFactory,
            TelemetryConfiguration config)
        {
            lock (_lock)
            {
                if (_clients.TryGetValue(host, out TelemetryClientHolder? existing))
                {
                    existing._refCount++;
                    return existing.Client;
                }

                TelemetryClientHolder holder = new TelemetryClientHolder(new TelemetryClient(exporterFactory(), config));
                _clients[host] = holder;
                return holder.Client;
            }
        }

        /// <summary>
        /// Decrements the reference count for the specified host's telemetry client.
        /// Closes and removes the client when the reference count reaches zero.
        /// </summary>
        /// <param name="host">The host identifier.</param>
        /// <returns>A task that completes when the operation finishes.</returns>
        /// <remarks>
        /// <para>
        /// Thread Safety: This method is thread-safe. Uses a lock to ensure atomicity of
        /// the decrement and conditional removal, preventing race conditions with concurrent
        /// GetOrCreateClient calls.
        /// </para>
        /// <para>
        /// Cleanup: When the reference count reaches zero (last connection closes), the client
        /// is removed from the dictionary and CloseAsync() is called outside the lock to flush
        /// any pending telemetry events and dispose resources.
        /// </para>
        /// <para>
        /// If the host is not found in the dictionary (e.g., already removed by another thread),
        /// this method returns immediately without error.
        /// </para>
        /// </remarks>
        public async Task ReleaseClientAsync(string host)
        {
            TelemetryClientHolder? toClose = null;
            lock (_lock)
            {
                if (_clients.TryGetValue(host, out TelemetryClientHolder? holder))
                {
                    holder._refCount--;
                    if (holder._refCount == 0)
                    {
                        _clients.Remove(host);
                        toClose = holder;
                    }
                }
            }

            if (toClose != null)
            {
                await toClose.Client.CloseAsync().ConfigureAwait(false);
            }
        }
    }
}
