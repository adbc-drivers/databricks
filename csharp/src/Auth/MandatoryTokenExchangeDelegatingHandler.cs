/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
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
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Auth
{
    /// <summary>
    /// HTTP message handler that performs mandatory token exchange for non-Databricks tokens.
    /// Blocks requests while exchanging tokens to ensure the exchanged token is used.
    /// Falls back to the original token if the exchange fails.
    /// </summary>
    internal class MandatoryTokenExchangeDelegatingHandler : DelegatingHandler
    {
        private readonly string? _identityFederationClientId;
        private readonly SemaphoreSlim _exchangeLock = new SemaphoreSlim(1, 1);
        private readonly ITokenExchangeClient _tokenExchangeClient;

        // Maps each original (external) token to its exchanged Databricks token.
        // On failure, the original token is mapped to itself to prevent repeated exchange attempts.
        // Keyed by the original bearer token so that when the upstream token rotates
        // (e.g. OAuthDelegatingHandler refreshes the M2M token), the new token is exchanged fresh.
        private readonly ConcurrentDictionary<string, string> _tokenCache = new ConcurrentDictionary<string, string>();

        /// <summary>
        /// Initializes a new instance of the <see cref="MandatoryTokenExchangeDelegatingHandler"/> class.
        /// </summary>
        /// <param name="innerHandler">The inner handler to delegate to.</param>
        /// <param name="tokenExchangeClient">The client for token exchange operations.</param>
        /// <param name="identityFederationClientId">Optional identity federation client ID.</param>
        public MandatoryTokenExchangeDelegatingHandler(
            HttpMessageHandler innerHandler,
            ITokenExchangeClient tokenExchangeClient,
            string? identityFederationClientId)
            : base(innerHandler)
        {
            _tokenExchangeClient = tokenExchangeClient ?? throw new ArgumentNullException(nameof(tokenExchangeClient));
            _identityFederationClientId = identityFederationClientId;
        }

        /// <summary>
        /// Determines if token exchange is needed by checking if the token is a Databricks token.
        /// </summary>
        /// <returns>True if token exchange is needed, false otherwise.</returns>
        private bool NeedsTokenExchange(string bearerToken)
        {
            // If we can't parse the token as JWT, default to use existing token
            if (!JwtTokenDecoder.TryGetIssuer(bearerToken, out string issuer))
            {
                return false;
            }

            // Check if the issuer matches the current workspace host
            // If the issuer is from the same host, it's already a Databricks token
            string normalizedHost = _tokenExchangeClient.TokenExchangeEndpoint.Replace("/v1/token", "").ToLowerInvariant();
            string normalizedIssuer = issuer.TrimEnd('/').ToLowerInvariant();

            return normalizedIssuer != normalizedHost;
        }

        /// <summary>
        /// Returns the token to use for the request, performing exchange if needed.
        /// If the token is already Databricks-issued, returns it directly without exchange.
        /// Results are cached per original token so that when the upstream token rotates,
        /// the new token is exchanged fresh. On failure, the original token is cached to
        /// prevent repeated exchange attempts for the same token.
        /// </summary>
        private async Task<string> GetTokenAsync(string bearerToken, CancellationToken cancellationToken)
        {
            // Token is already Databricks-issued — pass it through directly.
            // Do not fall back to cached token here: upstream may have provided a fresher
            // Databricks token (e.g. after TokenRefreshDelegatingHandler refreshed).
            if (!NeedsTokenExchange(bearerToken))
                return bearerToken;

            await _exchangeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (!_tokenCache.TryGetValue(bearerToken, out string? cached))
                    await DoExchangeAsync(bearerToken, cancellationToken).ConfigureAwait(false);

                return _tokenCache[bearerToken];
            }
            finally
            {
                _exchangeLock.Release();
            }
        }

        /// <summary>
        /// Performs the actual token exchange operation.
        /// Caches the exchanged token on success, or the original bearer token on failure
        /// to prevent repeated exchange attempts for the same token.
        /// </summary>
        private async Task DoExchangeAsync(string bearerToken, CancellationToken cancellationToken)
        {
            Activity.Current?.AddEvent(new ActivityEvent("auth.token_exchange", tags: new ActivityTagsCollection
            {
                { "endpoint", _tokenExchangeClient.TokenExchangeEndpoint },
                { "has_identity_federation", !string.IsNullOrEmpty(_identityFederationClientId) }
            }));

            try
            {
                TokenExchangeResponse response = await _tokenExchangeClient.ExchangeTokenAsync(
                    bearerToken,
                    _identityFederationClientId,
                    cancellationToken);

                _tokenCache[bearerToken] = response.AccessToken;

                Activity.Current?.AddEvent(new ActivityEvent("auth.token_exchange.completed", tags: new ActivityTagsCollection
                {
                    { "expires_in", response.ExpiresIn }
                }));
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Mandatory token exchange failed: {ex.Message}. Continuing with original token.");
                // Cache the original token so subsequent requests with the same token don't retry the failed exchange.
                _tokenCache[bearerToken] = bearerToken;
            }
        }

        /// <summary>
        /// Sends an HTTP request with the current token.
        /// </summary>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            string? bearerToken = request.Headers.Authorization?.Parameter;
            if (!string.IsNullOrEmpty(bearerToken))
            {
                string tokenToUse = await GetTokenAsync(bearerToken!, cancellationToken);
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", tokenToUse);
            }

            return await base.SendAsync(request, cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _exchangeLock.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
