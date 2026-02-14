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
using AdbcDrivers.Databricks.Auth;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc.Tracing;

namespace AdbcDrivers.Databricks.Http
{
    /// <summary>
    /// Factory for creating HTTP handlers with OAuth and other delegating handlers.
    /// This provides a common implementation for both Thrift and Statement Execution API connections.
    /// </summary>
    internal static class HttpHandlerFactory
    {
        /// <summary>
        /// Configuration for creating HTTP handlers.
        /// </summary>
        internal class HandlerConfig
        {
            /// <summary>
            /// Base HTTP handler to wrap with delegating handlers.
            /// </summary>
            public HttpMessageHandler BaseHandler { get; set; } = null!;

            /// <summary>
            /// Base HTTP handler for OAuth token operations (separate client).
            /// </summary>
            public HttpMessageHandler BaseAuthHandler { get; set; } = null!;

            /// <summary>
            /// Connection properties containing configuration.
            /// </summary>
            public IReadOnlyDictionary<string, string> Properties { get; set; } = null!;

            /// <summary>
            /// Host URL for OAuth operations.
            /// </summary>
            public string Host { get; set; } = null!;

            /// <summary>
            /// Activity tracer for retry operations.
            /// </summary>
            public IActivityTracer ActivityTracer { get; set; } = null!;

            /// <summary>
            /// Whether trace propagation is enabled.
            /// </summary>
            public bool TracePropagationEnabled { get; set; }

            /// <summary>
            /// Name of the trace parent header.
            /// </summary>
            public string TraceParentHeaderName { get; set; } = "traceparent";

            /// <summary>
            /// Whether trace state is enabled.
            /// </summary>
            public bool TraceStateEnabled { get; set; }

            /// <summary>
            /// Identity federation client ID (optional).
            /// </summary>
            public string? IdentityFederationClientId { get; set; }

            /// <summary>
            /// Whether to enable temporarily unavailable retry.
            /// </summary>
            public bool TemporarilyUnavailableRetry { get; set; } = true;

            /// <summary>
            /// Timeout for temporarily unavailable retry in seconds.
            /// </summary>
            public int TemporarilyUnavailableRetryTimeout { get; set; }

            /// <summary>
            /// Whether to enable rate limit retry.
            /// </summary>
            public bool RateLimitRetry { get; set; } = true;

            /// <summary>
            /// Timeout for rate limit retry in seconds.
            /// </summary>
            public int RateLimitRetryTimeout { get; set; }

            /// <summary>
            /// Timeout in minutes for HTTP operations.
            /// </summary>
            public int TimeoutMinutes { get; set; }

            /// <summary>
            /// Whether to add Thrift error handler.
            /// </summary>
            public bool AddThriftErrorHandler { get; set; }
        }

        /// <summary>
        /// Checks if OAuth authentication is configured in properties.
        /// </summary>
        private static bool IsOAuthEnabled(IReadOnlyDictionary<string, string> properties)
        {
            return properties.TryGetValue(SparkParameters.AuthType, out string? authType) &&
                SparkAuthTypeParser.TryParse(authType, out SparkAuthType authTypeValue) &&
                authTypeValue == SparkAuthType.OAuth;
        }

        /// <summary>
        /// Gets the OAuth grant type from properties.
        /// </summary>
        private static DatabricksOAuthGrantType GetOAuthGrantType(IReadOnlyDictionary<string, string> properties)
        {
            properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantTypeStr);
            DatabricksOAuthGrantTypeParser.TryParse(grantTypeStr, out DatabricksOAuthGrantType grantType);
            return grantType;
        }

        /// <summary>
        /// Creates an OAuthClientCredentialsProvider for M2M authentication.
        /// Returns null if client ID or secret is missing.
        /// </summary>
        private static OAuthClientCredentialsProvider? CreateOAuthClientCredentialsProvider(
            IReadOnlyDictionary<string, string> properties,
            HttpClient authHttpClient,
            string host)
        {
            properties.TryGetValue(DatabricksParameters.OAuthClientId, out string? clientId);
            properties.TryGetValue(DatabricksParameters.OAuthClientSecret, out string? clientSecret);
            properties.TryGetValue(DatabricksParameters.OAuthScope, out string? scope);

            if (string.IsNullOrEmpty(clientId) || string.IsNullOrEmpty(clientSecret))
            {
                return null;
            }

            return new OAuthClientCredentialsProvider(
                authHttpClient,
                clientId,
                clientSecret,
                host,
                scope: scope ?? "sql",
                timeoutMinutes: 1);
        }

        /// <summary>
        /// Adds authentication handlers to the handler chain.
        /// Always returns a handler. Throws exceptions on configuration errors.
        /// If no auth is configured, returns the handler unchanged.
        /// </summary>
        /// <param name="handler">The current handler chain.</param>
        /// <param name="properties">Connection properties.</param>
        /// <param name="host">The Databricks host.</param>
        /// <param name="authHttpClient">HTTP client for auth operations (required for OAuth).</param>
        /// <param name="identityFederationClientId">Identity federation client ID (optional).</param>
        /// <returns>Handler with auth handlers added, or the original handler if no auth is configured.</returns>
        /// <exception cref="ArgumentException">Thrown when OAuth is configured but required credentials are missing.</exception>
        private static HttpMessageHandler AddAuthHandlers(
            HttpMessageHandler handler,
            IReadOnlyDictionary<string, string> properties,
            string host,
            HttpClient? authHttpClient)
        {
            // Get identity federation client ID
            properties.TryGetValue(DatabricksParameters.IdentityFederationClientId, out string? identityFederationClientId);

            if (IsOAuthEnabled(properties))
            {
                if (authHttpClient == null)
                {
                    throw new ArgumentException("OAuth authentication requires an auth HTTP client.");
                }

                ITokenExchangeClient tokenExchangeClient = new TokenExchangeClient(authHttpClient, host);

                // Mandatory token exchange should be the inner handler so that it happens
                // AFTER the OAuth handlers (e.g. after M2M sets the access token)
                handler = new MandatoryTokenExchangeDelegatingHandler(
                    handler,
                    tokenExchangeClient,
                    identityFederationClientId);

                var grantType = GetOAuthGrantType(properties);

                if (grantType == DatabricksOAuthGrantType.ClientCredentials)
                {
                    var tokenProvider = CreateOAuthClientCredentialsProvider(properties, authHttpClient, host);
                    if (tokenProvider == null)
                    {
                        throw new ArgumentException(
                            $"OAuth client_credentials grant type requires '{DatabricksParameters.OAuthClientId}' and '{DatabricksParameters.OAuthClientSecret}' parameters.");
                    }
                    handler = new OAuthDelegatingHandler(handler, tokenProvider);
                }
                else if (grantType == DatabricksOAuthGrantType.AccessToken)
                {
                    string? accessToken = GetTokenFromProperties(properties);
                    if (string.IsNullOrEmpty(accessToken))
                    {
                        throw new ArgumentException(
                            $"OAuth access_token grant type requires '{SparkParameters.AccessToken}' or '{SparkParameters.Token}' parameter.");
                    }

                    // Enable token refresh if configured and token is JWT with expiry
                    if (properties.TryGetValue(DatabricksParameters.TokenRenewLimit, out string? tokenRenewLimitStr) &&
                        int.TryParse(tokenRenewLimitStr, out int tokenRenewLimit) &&
                        tokenRenewLimit > 0 &&
                        JwtTokenDecoder.TryGetExpirationTime(accessToken, out DateTime expiryTime))
                    {
                        handler = new TokenRefreshDelegatingHandler(
                            handler,
                            tokenExchangeClient,
                            accessToken,
                            expiryTime,
                            tokenRenewLimit);
                    }
                    else
                    {
                        handler = new StaticBearerTokenHandler(handler, accessToken);
                    }
                }
                else
                {
                    throw new ArgumentException(
                        $"Unknown OAuth grant type. Supported values: '{DatabricksConstants.OAuthGrantTypes.AccessToken}', '{DatabricksConstants.OAuthGrantTypes.ClientCredentials}'.");
                }
            }
            else
            {
                // Non-OAuth authentication: use static Bearer token if provided
                string? accessToken = GetTokenFromProperties(properties);
                if (!string.IsNullOrEmpty(accessToken))
                {
                    handler = new StaticBearerTokenHandler(handler, accessToken);
                }
                // If no token, return handler unchanged (no auth)
            }

            return handler;
        }

        /// <summary>
        /// Gets the access token from connection properties.
        /// Tries access_token first, then falls back to token.
        /// </summary>
        private static string? GetTokenFromProperties(IReadOnlyDictionary<string, string> properties)
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
        /// Creates HTTP handlers with OAuth and other delegating handlers.
        ///
        /// Handler chain order (outermost to innermost):
        /// 1. OAuth handlers (OAuthDelegatingHandler or TokenRefreshDelegatingHandler) - token management
        /// 2. MandatoryTokenExchangeDelegatingHandler (if OAuth) - workload identity federation
        /// 3. ThriftErrorMessageHandler (optional, for Thrift only) - extracts Thrift error messages
        /// 4. RetryHttpHandler - retries 408, 429, 502, 503, 504 with Retry-After support
        /// 5. TracingDelegatingHandler - propagates W3C trace context (closest to network)
        /// 6. Base HTTP handler - actual network communication
        /// </summary>
        public static HttpMessageHandler CreateHandlers(HandlerConfig config)
        {
            HttpMessageHandler handler = config.BaseHandler;
            HttpMessageHandler authHandler = config.BaseAuthHandler;

            // Add tracing handler (INNERMOST - closest to network) if enabled
            if (config.TracePropagationEnabled)
            {
                handler = new TracingDelegatingHandler(handler, config.ActivityTracer, config.TraceParentHeaderName, config.TraceStateEnabled);
                authHandler = new TracingDelegatingHandler(authHandler, config.ActivityTracer, config.TraceParentHeaderName, config.TraceStateEnabled);
            }

            // Add retry handler (OUTSIDE tracing)
            if (config.TemporarilyUnavailableRetry || config.RateLimitRetry)
            {
                handler = new RetryHttpHandler(
                    handler,
                    config.ActivityTracer,
                    config.TemporarilyUnavailableRetryTimeout,
                    config.RateLimitRetryTimeout,
                    config.TemporarilyUnavailableRetry,
                    config.RateLimitRetry);
                authHandler = new RetryHttpHandler(
                    authHandler,
                    config.ActivityTracer,
                    config.TemporarilyUnavailableRetryTimeout,
                    config.RateLimitRetryTimeout,
                    config.TemporarilyUnavailableRetry,
                    config.RateLimitRetry);
            }

            // Add Thrift error handler if requested (for Thrift connections only)
            if (config.AddThriftErrorHandler)
            {
                handler = new ThriftErrorMessageHandler(handler);
                authHandler = new ThriftErrorMessageHandler(authHandler);
            }

            // Create auth HTTP client for OAuth if needed
            HttpClient? authHttpClient = null;
            if (IsOAuthEnabled(config.Properties))
            {
                authHttpClient = new HttpClient(authHandler)
                {
                    Timeout = TimeSpan.FromMinutes(config.TimeoutMinutes)
                };
            }

            // Add auth handlers
            handler = AddAuthHandlers(
                handler,
                config.Properties,
                config.Host,
                authHttpClient);

            return handler;
        }

        /// <summary>
        /// Creates an HTTP handler chain specifically for feature flag API calls.
        /// This is a simplified version of CreateHandlers that includes auth but not
        /// tracing, retry, or Thrift error handlers.
        ///
        /// Handler chain order (outermost to innermost):
        /// 1. OAuth handlers (OAuthDelegatingHandler or StaticBearerTokenHandler) - token management
        /// 2. MandatoryTokenExchangeDelegatingHandler (if OAuth) - workload identity federation
        /// 3. Base HTTP handler - actual network communication
        ///
        /// This properly supports all authentication methods including:
        /// - PAT (Personal Access Token)
        /// - OAuth M2M (client_credentials)
        /// - OAuth U2M (access_token)
        /// - Workload Identity Federation (via MandatoryTokenExchangeDelegatingHandler)
        /// </summary>
        /// <param name="properties">Connection properties containing configuration.</param>
        /// <param name="host">The Databricks host (without protocol).</param>
        /// <param name="timeoutSeconds">HTTP client timeout in seconds.</param>
        /// <returns>Configured HttpMessageHandler.</returns>
        public static HttpMessageHandler CreateFeatureFlagHandler(
            IReadOnlyDictionary<string, string> properties,
            string host,
            int timeoutSeconds)
        {
            HttpMessageHandler baseHandler = HttpClientFactory.CreateHandler(properties);

            // Create auth HTTP client for OAuth if needed
            HttpClient? authHttpClient = null;
            if (IsOAuthEnabled(properties))
            {
                HttpMessageHandler baseAuthHandler = HttpClientFactory.CreateHandler(properties);
                authHttpClient = new HttpClient(baseAuthHandler)
                {
                    Timeout = TimeSpan.FromSeconds(timeoutSeconds)
                };
            }

            // Add auth handlers
            return AddAuthHandlers(
                baseHandler,
                properties,
                host,
                authHttpClient);
        }
    }
}
