/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
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
        /// Result of creating HTTP handlers.
        /// </summary>
        internal class HandlerResult
        {
            /// <summary>
            /// HTTP handler chain for API requests.
            /// </summary>
            public HttpMessageHandler Handler { get; set; } = null!;

            /// <summary>
            /// HTTP client for OAuth token operations (may be null if OAuth not configured).
            /// </summary>
            public HttpClient? AuthHttpClient { get; set; }
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
        public static HandlerResult CreateHandlers(HandlerConfig config)
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

            HttpClient? authHttpClient = null;

            // Check if OAuth authentication is configured
            bool useOAuth = config.Properties.TryGetValue(SparkParameters.AuthType, out string? authType) &&
                SparkAuthTypeParser.TryParse(authType, out SparkAuthType authTypeValue) &&
                authTypeValue == SparkAuthType.OAuth;

            if (useOAuth)
            {
                // Create auth HTTP client for token operations
                authHttpClient = new HttpClient(authHandler)
                {
                    Timeout = TimeSpan.FromMinutes(config.TimeoutMinutes)
                };

                ITokenExchangeClient tokenExchangeClient = new TokenExchangeClient(authHttpClient, config.Host);

                // Mandatory token exchange should be the inner handler so that it happens
                // AFTER the OAuth handlers (e.g. after M2M sets the access token)
                handler = new MandatoryTokenExchangeDelegatingHandler(
                    handler,
                    tokenExchangeClient,
                    config.IdentityFederationClientId);

                // Determine grant type (defaults to AccessToken if not specified)
                config.Properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantTypeStr);
                DatabricksOAuthGrantTypeParser.TryParse(grantTypeStr, out DatabricksOAuthGrantType grantType);

                // Add OAuth client credentials handler if OAuth M2M authentication is being used
                if (grantType == DatabricksOAuthGrantType.ClientCredentials)
                {
                    config.Properties.TryGetValue(DatabricksParameters.OAuthClientId, out string? clientId);
                    config.Properties.TryGetValue(DatabricksParameters.OAuthClientSecret, out string? clientSecret);
                    config.Properties.TryGetValue(DatabricksParameters.OAuthScope, out string? scope);

                    var tokenProvider = new OAuthClientCredentialsProvider(
                        authHttpClient,
                        clientId!,
                        clientSecret!,
                        config.Host,
                        scope: scope ?? "sql",
                        timeoutMinutes: 1
                    );

                    handler = new OAuthDelegatingHandler(handler, tokenProvider);
                }
                // For access_token grant type, get the access token from properties
                else if (grantType == DatabricksOAuthGrantType.AccessToken)
                {
                    // Get the access token from properties
                    string accessToken = string.Empty;
                    if (config.Properties.TryGetValue(SparkParameters.AccessToken, out string? token))
                    {
                        accessToken = token ?? string.Empty;
                    }
                    else if (config.Properties.TryGetValue(SparkParameters.Token, out string? fallbackToken))
                    {
                        accessToken = fallbackToken ?? string.Empty;
                    }

                    if (!string.IsNullOrEmpty(accessToken))
                    {
                        // Check if token renewal is configured and token is JWT
                        if (config.Properties.TryGetValue(DatabricksParameters.TokenRenewLimit, out string? tokenRenewLimitStr) &&
                            int.TryParse(tokenRenewLimitStr, out int tokenRenewLimit) &&
                            tokenRenewLimit > 0 &&
                            JwtTokenDecoder.TryGetExpirationTime(accessToken, out DateTime expiryTime))
                        {
                            // Use TokenRefreshDelegatingHandler for JWT tokens with renewal configured
                            handler = new TokenRefreshDelegatingHandler(
                                handler,
                                tokenExchangeClient,
                                accessToken,
                                expiryTime,
                                tokenRenewLimit);
                        }
                        else
                        {
                            // Use StaticBearerTokenHandler for tokens without renewal
                            handler = new StaticBearerTokenHandler(handler, accessToken);
                        }
                    }
                }
            }
            else
            {
                // Non-OAuth authentication: use static Bearer token if provided
                // Try access_token first, then fall back to token
                string accessToken = string.Empty;
                if (config.Properties.TryGetValue(SparkParameters.AccessToken, out string? token))
                {
                    accessToken = token ?? string.Empty;
                }
                else if (config.Properties.TryGetValue(SparkParameters.Token, out string? fallbackToken))
                {
                    accessToken = fallbackToken ?? string.Empty;
                }

                if (!string.IsNullOrEmpty(accessToken))
                {
                    handler = new StaticBearerTokenHandler(handler, accessToken);
                }
            }

            return new HandlerResult
            {
                Handler = handler,
                AuthHttpClient = authHttpClient
            };
        }
    }
}
