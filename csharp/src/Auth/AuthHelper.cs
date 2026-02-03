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
using Apache.Arrow.Adbc.Drivers.Apache.Spark;

namespace AdbcDrivers.Databricks.Auth
{
    /// <summary>
    /// Helper methods for authentication operations.
    /// Provides shared functionality used by HttpHandlerFactory and FeatureFlagCache.
    /// </summary>
    internal static class AuthHelper
    {
        /// <summary>
        /// Gets the access token from connection properties.
        /// Tries access_token first, then falls back to token.
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <returns>The token, or null if not found.</returns>
        public static string? GetTokenFromProperties(IReadOnlyDictionary<string, string> properties)
        {
            if (properties.TryGetValue(SparkParameters.AccessToken, out string? accessToken) && !string.IsNullOrEmpty(accessToken))
            {
                return accessToken;
            }

            if (properties.TryGetValue(SparkParameters.Token, out string? token) && !string.IsNullOrEmpty(token))
            {
                return token;
            }

            return null;
        }

        /// <summary>
        /// Gets the access token based on authentication configuration.
        /// Supports token-based (PAT) and OAuth M2M (client_credentials) authentication.
        /// </summary>
        /// <param name="host">The Databricks host.</param>
        /// <param name="properties">Connection properties containing auth configuration.</param>
        /// <param name="timeout">HTTP client timeout for OAuth token requests.</param>
        /// <returns>The access token, or null if no valid authentication is configured.</returns>
        public static string? GetAccessToken(string host, IReadOnlyDictionary<string, string> properties, TimeSpan timeout)
        {
            // Check if OAuth authentication is configured
            bool useOAuth = properties.TryGetValue(SparkParameters.AuthType, out string? authType) &&
                SparkAuthTypeParser.TryParse(authType, out SparkAuthType authTypeValue) &&
                authTypeValue == SparkAuthType.OAuth;

            if (useOAuth)
            {
                // Determine grant type (defaults to AccessToken if not specified)
                properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantTypeStr);
                DatabricksOAuthGrantTypeParser.TryParse(grantTypeStr, out DatabricksOAuthGrantType grantType);

                if (grantType == DatabricksOAuthGrantType.ClientCredentials)
                {
                    // OAuth M2M authentication
                    return GetOAuthClientCredentialsToken(host, properties, timeout);
                }
                else if (grantType == DatabricksOAuthGrantType.AccessToken)
                {
                    // OAuth with access_token grant type
                    return GetTokenFromProperties(properties);
                }
            }

            // Non-OAuth authentication: use static Bearer token if provided
            return GetTokenFromProperties(properties);
        }

        /// <summary>
        /// Gets an OAuth access token using client credentials (M2M) flow.
        /// </summary>
        /// <param name="host">The Databricks host.</param>
        /// <param name="properties">Connection properties containing OAuth credentials.</param>
        /// <param name="timeout">HTTP client timeout.</param>
        /// <returns>The access token, or null if credentials are missing or token acquisition fails.</returns>
        public static string? GetOAuthClientCredentialsToken(string host, IReadOnlyDictionary<string, string> properties, TimeSpan timeout)
        {
            properties.TryGetValue(DatabricksParameters.OAuthClientId, out string? clientId);
            properties.TryGetValue(DatabricksParameters.OAuthClientSecret, out string? clientSecret);
            properties.TryGetValue(DatabricksParameters.OAuthScope, out string? scope);

            if (string.IsNullOrEmpty(clientId) || string.IsNullOrEmpty(clientSecret))
            {
                return null;
            }

            try
            {
                // Create a separate HttpClient for OAuth token acquisition with TLS and proxy settings
                using var oauthHttpClient = Http.HttpClientFactory.CreateOAuthHttpClient(properties, timeout);

                using var tokenProvider = new OAuthClientCredentialsProvider(
                    oauthHttpClient,
                    clientId,
                    clientSecret,
                    host,
                    scope: scope ?? "sql",
                    timeoutMinutes: 1);

                // Get access token synchronously (blocking call)
                return tokenProvider.GetAccessTokenAsync().GetAwaiter().GetResult();
            }
            catch
            {
                // Auth failures should be handled by caller
                return null;
            }
        }
    }
}
