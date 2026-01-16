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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Singleton factory that manages one telemetry client per host.
    /// Prevents rate limiting by sharing clients across connections to the same host.
    /// Reference counting ensures proper cleanup when the last connection closes.
    /// Thread-safe using ConcurrentDictionary for concurrent access from multiple connections.
    /// </summary>
    internal sealed class TelemetryClientManager
    {
        private static readonly TelemetryClientManager Instance = new();
        private readonly ConcurrentDictionary<string, TelemetryClientHolder> _clients = new();

        private TelemetryClientManager()
        {
        }

        /// <summary>
        /// Gets the singleton instance of the TelemetryClientManager.
        /// </summary>
        /// <returns>The singleton TelemetryClientManager instance.</returns>
        public static TelemetryClientManager GetInstance() => Instance;

        /// <summary>
        /// Gets or creates a telemetry client for the host.
        /// Atomically increments the reference count.
        /// Multiple connections to the same host will receive the same client instance.
        /// </summary>
        /// <param name="host">The Databricks host (e.g., "host.cloud.databricks.com").</param>
        /// <param name="httpClient">The HttpClient for making telemetry requests.</param>
        /// <param name="config">The telemetry configuration.</param>
        /// <returns>The shared ITelemetryClient instance for the host.</returns>
        public ITelemetryClient GetOrCreateClient(
            string host,
            HttpClient httpClient,
            TelemetryConfiguration config)
        {
            if (string.IsNullOrEmpty(host))
            {
                throw new ArgumentNullException(nameof(host));
            }

            if (httpClient == null)
            {
                throw new ArgumentNullException(nameof(httpClient));
            }

            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            var holder = _clients.AddOrUpdate(
                host,
                // Create new client with RefCount=1
                _ => CreateClientHolder(host, httpClient, config),
                // Increment existing client's RefCount
                (_, existingHolder) =>
                {
                    Interlocked.Increment(ref existingHolder.RefCount);
                    return existingHolder;
                });

            return holder.Client;
        }

        /// <summary>
        /// Decrements the reference count for the host.
        /// Closes and removes the client when reference count reaches zero.
        /// </summary>
        /// <param name="host">The Databricks host.</param>
        /// <returns>Task representing the async release operation.</returns>
        public async Task ReleaseClientAsync(string host)
        {
            if (string.IsNullOrEmpty(host))
            {
                return;
            }

            if (_clients.TryGetValue(host, out var holder))
            {
                var newRefCount = Interlocked.Decrement(ref holder.RefCount);
                if (newRefCount <= 0)
                {
                    // Remove from cache when last reference is released
                    if (_clients.TryRemove(host, out var removedHolder))
                    {
                        try
                        {
                            // Close the client gracefully
                            await removedHolder.Client.CloseAsync();
                        }
                        catch (Exception)
                        {
                            // All exceptions swallowed per design requirement
                            // CloseAsync should never throw, but catch anyway for safety
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Creates a new TelemetryClientHolder with a fresh client instance.
        /// </summary>
        /// <param name="host">The Databricks host.</param>
        /// <param name="httpClient">The HttpClient for making telemetry requests.</param>
        /// <param name="config">The telemetry configuration.</param>
        /// <returns>A new TelemetryClientHolder with RefCount=1.</returns>
        private TelemetryClientHolder CreateClientHolder(
            string host,
            HttpClient httpClient,
            TelemetryConfiguration config)
        {
            // Create a new TelemetryClient instance
            // Note: isAuthenticated is set to true by default
            // This can be made configurable if needed in the future
            var client = new TelemetryClient(host, httpClient, config, isAuthenticated: true);

            return new TelemetryClientHolder(client);
        }
    }
}
