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

        // Holds the token to use for outgoing requests. Set on first exchange attempt:
        // the exchanged Databricks token on success, or the original bearer token on failure.
        // Once set, all subsequent requests reuse it without re-exchanging.
        private string? _currentToken;

        /// <summary>
        /// Initializes a new instance of the <see cref="MandatoryTokenExchangeDelegatingHandler"/> class.
        /// </summary>
        /// <param name="innerHandler">The inner handler to delegate to.</param>
        /// <param name="tokenExchangeClient">The client for token exchange operations.</param>
        /// <param name="identityFederationClientId">Optional identity federation client ID.</param>
        public MandatoryTokenExchangeDelegatingHandler(
            HttpMessageHandler innerHandler,
            ITokenExchangeClient tokenExchangeClient,
            string? identityFederationClientId = null)
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
        /// Exchange is attempted at most once per handler instance; on failure, the original
        /// token is cached to prevent repeated exchange attempts.
        /// </summary>
        private async Task<string> GetTokenAsync(string bearerToken, CancellationToken cancellationToken)
        {
            // Token is already Databricks-issued — pass it through directly.
            // Do not fall back to _currentToken here: upstream may have provided a fresher
            // Databricks token (e.g. after TokenRefreshDelegatingHandler refreshed).
            if (!NeedsTokenExchange(bearerToken))
                return bearerToken;

            await _exchangeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_currentToken == null)
                    await DoExchangeAsync(bearerToken, cancellationToken).ConfigureAwait(false);

                return _currentToken!;
            }
            finally
            {
                _exchangeLock.Release();
            }
        }

        /// <summary>
        /// Performs the actual token exchange operation.
        /// Sets _currentToken to the exchanged token on success, or to the original bearer
        /// token on failure so subsequent requests skip re-exchanging.
        /// </summary>
        private async Task DoExchangeAsync(string bearerToken, CancellationToken cancellationToken)
        {
            try
            {
                TokenExchangeResponse response = await _tokenExchangeClient.ExchangeTokenAsync(
                    bearerToken,
                    _identityFederationClientId,
                    cancellationToken);

                _currentToken = response.AccessToken;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Mandatory token exchange failed: {ex.Message}. Continuing with original token.");
                // Cache the original token so subsequent requests don't retry the failed exchange.
                _currentToken = bearerToken;
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
