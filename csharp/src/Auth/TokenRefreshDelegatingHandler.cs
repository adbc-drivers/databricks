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
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Auth
{
    /// <summary>
    /// HTTP message handler that automatically refreshes OAuth tokens before they expire.
    /// Blocks the request while refreshing to ensure a fresh token is always used.
    /// Supports multiple refresh cycles: once a refreshed token itself nears expiry,
    /// it is refreshed again.
    /// </summary>
    internal class TokenRefreshDelegatingHandler : DelegatingHandler
    {
        private readonly int _tokenRenewLimitMinutes;
        private readonly SemaphoreSlim _refreshLock = new SemaphoreSlim(1, 1);
        private readonly ITokenExchangeClient _tokenExchangeClient;

        // Maps each near-expiry token to its refreshed replacement.
        // On failure, the token is mapped to itself to prevent retrying on every request.
        private readonly Dictionary<string, string> _tokenCache = new Dictionary<string, string>();

        private string _currentToken;
        private DateTime _tokenExpiryTime;

        /// <summary>
        /// Initializes a new instance of the <see cref="TokenRefreshDelegatingHandler"/> class.
        /// </summary>
        /// <param name="innerHandler">The inner handler to delegate to.</param>
        /// <param name="tokenExchangeClient">The client for token exchange operations.</param>
        /// <param name="initialToken">The initial token from the connection string.</param>
        /// <param name="tokenExpiryTime">The expiry time of the initial token.</param>
        /// <param name="tokenRenewLimitMinutes">Minutes before expiration at which to start renewing.</param>
        public TokenRefreshDelegatingHandler(
            HttpMessageHandler innerHandler,
            ITokenExchangeClient tokenExchangeClient,
            string initialToken,
            DateTime tokenExpiryTime,
            int tokenRenewLimitMinutes)
            : base(innerHandler)
        {
            _tokenExchangeClient = tokenExchangeClient ?? throw new ArgumentNullException(nameof(tokenExchangeClient));
            _currentToken = initialToken ?? throw new ArgumentNullException(nameof(initialToken));
            _tokenExpiryTime = tokenExpiryTime;
            _tokenRenewLimitMinutes = tokenRenewLimitMinutes;
        }

        /// <summary>
        /// Checks if the current token needs to be renewed based on its expiry time.
        /// </summary>
        private bool NeedsTokenRenewal()
        {
            return DateTime.UtcNow.AddMinutes(_tokenRenewLimitMinutes) >= _tokenExpiryTime;
        }

        /// <summary>
        /// Ensures the token is refreshed if needed, blocking until complete.
        /// Returns the current token (fresh or existing) to use for the request.
        /// </summary>
        private async Task<string> EnsureTokenFreshAsync(CancellationToken cancellationToken)
        {
            await _refreshLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                // Refresh only if near expiry and not already attempted for this token.
                if (NeedsTokenRenewal() && !_tokenCache.TryGetValue(_currentToken, out _))
                {
                    await DoRefreshAsync(_currentToken, cancellationToken).ConfigureAwait(false);
                }
                return _currentToken;
            }
            finally
            {
                _refreshLock.Release();
            }
        }

        /// <summary>
        /// Performs the token refresh network call.
        /// On success, updates _currentToken and caches the mapping.
        /// On failure, caches the current token to itself to prevent repeated retries.
        /// </summary>
        private async Task DoRefreshAsync(string tokenToRefresh, CancellationToken cancellationToken)
        {
            try
            {
                TokenExchangeResponse response = await _tokenExchangeClient.RefreshTokenAsync(tokenToRefresh, cancellationToken);
                _tokenCache[tokenToRefresh] = response.AccessToken;
                _currentToken = response.AccessToken;
                _tokenExpiryTime = response.ExpiryTime;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Token refresh failed: {ex.Message}");
                // Cache to prevent retrying the failed refresh for this token on every request.
                _tokenCache[tokenToRefresh] = tokenToRefresh;
            }
        }

        /// <summary>
        /// Sends an HTTP request, blocking to refresh the token if it is near expiry.
        /// </summary>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            string tokenToUse = await EnsureTokenFreshAsync(cancellationToken);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", tokenToUse);
            return await base.SendAsync(request, cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _refreshLock.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
