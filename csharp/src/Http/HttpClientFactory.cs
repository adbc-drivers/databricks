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
using System.Net.Http;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Databricks.Http
{
    /// <summary>
    /// Centralized factory for creating HttpClient instances with proper TLS and proxy configuration.
    /// All HttpClient creation should go through this factory to ensure consistent configuration.
    /// </summary>
    internal static class HttpClientFactory
    {
        /// <summary>
        /// Creates an HttpClientHandler with TLS and proxy settings from connection properties.
        /// This is the base handler used by all HttpClient instances.
        /// </summary>
        /// <param name="properties">Connection properties containing TLS and proxy configuration.</param>
        /// <returns>Configured HttpClientHandler.</returns>
        public static HttpClientHandler CreateHandler(IReadOnlyDictionary<string, string> properties)
        {
            var tlsOptions = HiveServer2TlsImpl.GetHttpTlsOptions(properties);
            var proxyConfigurator = HiveServer2ProxyConfigurator.FromProperties(properties);
            return HiveServer2TlsImpl.NewHttpClientHandler(tlsOptions, proxyConfigurator);
        }

        /// <summary>
        /// Creates a basic HttpClient with TLS and proxy settings.
        /// Use this for simple HTTP operations that don't need auth handlers or retries.
        /// </summary>
        /// <param name="properties">Connection properties containing TLS and proxy configuration.</param>
        /// <param name="timeout">Optional timeout. If not specified, uses default HttpClient timeout.</param>
        /// <returns>Configured HttpClient.</returns>
        public static HttpClient CreateBasicHttpClient(IReadOnlyDictionary<string, string> properties, TimeSpan? timeout = null)
        {
            var handler = CreateHandler(properties);
            var httpClient = new HttpClient(handler);

            if (timeout.HasValue)
            {
                httpClient.Timeout = timeout.Value;
            }

            return httpClient;
        }

        /// <summary>
        /// Creates an HttpClient for CloudFetch downloads.
        /// Includes TLS and proxy settings but no auth headers (CloudFetch uses pre-signed URLs).
        /// On .NET Framework / netstandard2.0, applies critical performance fixes:
        /// - Raises ServicePointManager.DefaultConnectionLimit (default 2 per server)
        /// - Disables Expect: 100-Continue (saves a round-trip)
        /// - Ensures ThreadPool has enough min threads for parallel downloads
        /// </summary>
        /// <param name="properties">Connection properties containing TLS and proxy configuration.</param>
        /// <returns>Configured HttpClient for CloudFetch.</returns>
        public static HttpClient CreateCloudFetchHttpClient(IReadOnlyDictionary<string, string> properties)
        {
            int timeoutMinutes = PropertyHelper.GetPositiveIntPropertyWithValidation(
                properties,
                DatabricksParameters.CloudFetchTimeoutMinutes,
                DatabricksConstants.DefaultCloudFetchTimeoutMinutes);

            int parallelDownloads = PropertyHelper.GetPositiveIntPropertyWithValidation(
                properties,
                DatabricksParameters.CloudFetchParallelDownloads,
                CloudFetchConfiguration.DefaultParallelDownloads);

            ApplyNetFrameworkTuning(parallelDownloads);

            return CreateBasicHttpClient(properties, TimeSpan.FromMinutes(timeoutMinutes));
        }

        /// <summary>
        /// Applies .NET Framework-specific performance tuning for parallel CloudFetch downloads.
        /// These settings are no-ops on .NET Core/.NET 5+ which uses SocketsHttpHandler.
        /// </summary>
        private static void ApplyNetFrameworkTuning(int parallelDownloads)
        {
#if NETFRAMEWORK || NETSTANDARD2_0
            // FIX 1: Connection limit.
            // .NET Framework defaults to 2 connections per server (ServicePointManager).
            // This throttles CloudFetch to 2 concurrent HTTP downloads regardless of
            // SemaphoreSlim(16). Must be set BEFORE any ServicePoint is created for the
            // cloud storage host, AND we set it high enough for all parallel downloads.
            // Using int.MaxValue to ensure no ServicePoint created later is ever throttled.
            if (System.Net.ServicePointManager.DefaultConnectionLimit < parallelDownloads)
            {
                System.Net.ServicePointManager.DefaultConnectionLimit = Math.Max(128, parallelDownloads);
            }

            // FIX 2: Disable Expect: 100-Continue header.
            // Saves one HTTP round-trip per request. Not needed for GET downloads.
            System.Net.ServicePointManager.Expect100Continue = false;

            // FIX 3: ThreadPool starvation.
            // .NET Framework's ThreadPool starts with min = ProcessorCount and ramps
            // at 1 thread per 500ms. With 16 parallel async downloads on a 4-core machine,
            // it takes ~6 seconds for the pool to have enough threads. Pre-allocate them.
            int minWorker, minIOCP;
            System.Threading.ThreadPool.GetMinThreads(out minWorker, out minIOCP);
            int required = Math.Max(parallelDownloads * 2, 50);
            if (minWorker < required || minIOCP < required)
            {
                System.Threading.ThreadPool.SetMinThreads(
                    Math.Max(minWorker, required),
                    Math.Max(minIOCP, required));
            }
#endif
        }

        /// <summary>
        /// Creates an HttpClient for feature flag API calls.
        /// Includes TLS, proxy settings, and full authentication handler chain.
        /// Supports PAT, OAuth M2M, OAuth U2M, and Workload Identity Federation.
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <param name="host">The Databricks host (without protocol).</param>
        /// <param name="assemblyVersion">The driver version for the User-Agent.</param>
        /// <returns>Configured HttpClient for feature flags.</returns>
        public static HttpClient CreateFeatureFlagHttpClient(
            IReadOnlyDictionary<string, string> properties,
            string host,
            string assemblyVersion)
        {
            const int DefaultFeatureFlagTimeoutSeconds = 10;

            var timeoutSeconds = PropertyHelper.GetPositiveIntPropertyWithValidation(
                properties,
                DatabricksParameters.FeatureFlagTimeoutSeconds,
                DefaultFeatureFlagTimeoutSeconds);

            // Create handler with full auth chain (including WIF support)
            var handler = HttpHandlerFactory.CreateFeatureFlagHandler(properties, host, timeoutSeconds);

            var httpClient = new HttpClient(handler)
            {
                BaseAddress = new Uri($"https://{host}"),
                Timeout = TimeSpan.FromSeconds(timeoutSeconds)
            };

            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(
                UserAgentHelper.GetUserAgent(assemblyVersion, properties));

            string? orgId = PropertyHelper.ParseOrgIdFromProperties(properties);
            if (!string.IsNullOrEmpty(orgId))
                httpClient.DefaultRequestHeaders.TryAddWithoutValidation(DatabricksConstants.OrgIdHeader, orgId);

            return httpClient;
        }

        /// <summary>
        /// Creates an HttpClient for telemetry export.
        /// Includes TLS, proxy settings, and full authentication handler chain.
        /// Uses the same auth/proxy pipeline as feature flag client.
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <param name="host">The Databricks host (without protocol).</param>
        /// <param name="assemblyVersion">The driver version for the User-Agent.</param>
        /// <param name="existingTokenProvider">Optional existing OAuthClientCredentialsProvider to reuse for token caching.</param>
        /// <returns>Configured HttpClient for telemetry.</returns>
        public static HttpClient CreateTelemetryHttpClient(
            IReadOnlyDictionary<string, string> properties,
            string host,
            string assemblyVersion,
            Auth.OAuthClientCredentialsProvider? existingTokenProvider = null)
        {
            const int DefaultTelemetryTimeoutSeconds = 10;

            // Use the same auth handler chain as feature flags (PAT, OAuth, WIF, proxy)
            // Reuse existing token provider to avoid duplicate OAuth token fetches
            var handler = HttpHandlerFactory.CreateFeatureFlagHandler(properties, host, DefaultTelemetryTimeoutSeconds, existingTokenProvider);

            var httpClient = new HttpClient(handler)
            {
                BaseAddress = new Uri($"https://{host}"),
                Timeout = TimeSpan.FromSeconds(DefaultTelemetryTimeoutSeconds)
            };

            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(
                UserAgentHelper.GetUserAgent(assemblyVersion, properties));

            string? orgId = PropertyHelper.ParseOrgIdFromProperties(properties);
            if (!string.IsNullOrEmpty(orgId))
                httpClient.DefaultRequestHeaders.TryAddWithoutValidation(DatabricksConstants.OrgIdHeader, orgId);

            return httpClient;
        }

    }
}
