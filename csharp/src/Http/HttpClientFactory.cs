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
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;

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
        /// </summary>
        /// <param name="properties">Connection properties containing TLS and proxy configuration.</param>
        /// <returns>Configured HttpClient for CloudFetch.</returns>
        public static HttpClient CreateCloudFetchHttpClient(IReadOnlyDictionary<string, string> properties)
        {
            int timeoutMinutes = PropertyHelper.GetPositiveIntPropertyWithValidation(
                properties,
                DatabricksParameters.CloudFetchTimeoutMinutes,
                DatabricksConstants.DefaultCloudFetchTimeoutMinutes);

            return CreateBasicHttpClient(properties, TimeSpan.FromMinutes(timeoutMinutes));
        }

        /// <summary>
        /// Creates an HttpClient for feature flag API calls.
        /// Includes TLS, proxy settings, and full authentication handler chain.
        /// Supports PAT, OAuth M2M, OAuth U2M, and Workload Identity Federation.
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <param name="host">The Databricks host (without protocol).</param>
        /// <param name="assemblyVersion">The driver version for the User-Agent.</param>
        /// <returns>Configured HttpClient for feature flags, or null if no valid authentication is available.</returns>
        public static HttpClient? CreateFeatureFlagHttpClient(
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
            if (handler == null)
            {
                return null;
            }

            var httpClient = new HttpClient(handler)
            {
                BaseAddress = new Uri($"https://{host}"),
                Timeout = TimeSpan.FromSeconds(timeoutSeconds)
            };

            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(
                UserAgentHelper.GetUserAgent(assemblyVersion, properties));

            return httpClient;
        }
    }
}
