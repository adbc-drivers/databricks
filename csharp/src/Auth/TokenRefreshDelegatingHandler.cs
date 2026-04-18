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
    /// HTTP message handler that automatically refreshes OAuth tokens before they expire.
    /// Blocks the request while refreshing to ensure a fresh token is always used.
    /// </summary>
    internal class TokenRefreshDelegatingHandler : DelegatingHandler
    {
        private readonly string _initialToken;
        private readonly int _tokenRenewLimitMinutes;
        private readonly object _tokenLock = new object();
        private readonly ITokenExchangeClient _tokenExchangeClient;

        private string _currentToken;
        private DateTime _tokenExpiryTime;
        private bool _tokenExchangeAttempted = false;
        private Task? _refreshTask = null;

        /// <summary>
        /// Initializes a new instance of the <see cref="TokenRefreshDelegatingHandler"/> class.
        /// </summary>
        /// <param name="innerHandler">The inner handler to delegate to.</param>
        /// <param name="tokenExchangeClient">The client for token exchange operations.</param>
        /// <param name="initialToken">The initial token from the connection string.</param>
        /// <param name="tokenExpiryTime">The expiry time of the initial token.</param>
        /// <param name="tokenRenewLimitMinutes">The minutes before token expiration when we should start renewing the token.</param>
        public TokenRefreshDelegatingHandler(
            HttpMessageHandler innerHandler,
            ITokenExchangeClient tokenExchangeClient,
            string initialToken,
            DateTime tokenExpiryTime,
            int tokenRenewLimitMinutes)
            : base(innerHandler)
        {
            _tokenExchangeClient = tokenExchangeClient ?? throw new ArgumentNullException(nameof(tokenExchangeClient));
            _initialToken = initialToken ?? throw new ArgumentNullException(nameof(initialToken));
            _tokenExpiryTime = tokenExpiryTime;
            _tokenRenewLimitMinutes = tokenRenewLimitMinutes;
            _currentToken = initialToken;
        }

        /// <summary>
        /// Checks if the token needs to be renewed.
        /// </summary>
        private bool NeedsTokenRenewal()
        {
            return !_tokenExchangeAttempted &&
                   DateTime.UtcNow.AddMinutes(_tokenRenewLimitMinutes) >= _tokenExpiryTime;
        }

        /// <summary>
        /// Ensures the token is refreshed if needed, blocking until the refresh completes.
        /// Concurrent requests share the same refresh task so only one network call is made.
        /// </summary>
        private async Task EnsureTokenFreshAsync(CancellationToken cancellationToken)
        {
            Task? taskToAwait;
            lock (_tokenLock)
            {
                if (NeedsTokenRenewal())
                {
                    _tokenExchangeAttempted = true;
                    _refreshTask = DoRefreshAsync(cancellationToken);
                }
                taskToAwait = _refreshTask;
            }

            if (taskToAwait != null)
            {
                await taskToAwait;
            }
        }

        /// <summary>
        /// Performs the token refresh network call.
        /// </summary>
        private async Task DoRefreshAsync(CancellationToken cancellationToken)
        {
            try
            {
                TokenExchangeResponse response = await _tokenExchangeClient.RefreshTokenAsync(_initialToken, cancellationToken);
                lock (_tokenLock)
                {
                    _currentToken = response.AccessToken;
                    _tokenExpiryTime = response.ExpiryTime;
                }
            }
            catch (Exception ex)
            {
                // Log the error but continue with the current token
                System.Diagnostics.Debug.WriteLine($"Token refresh failed: {ex.Message}");
            }
        }

        /// <summary>
        /// Sends an HTTP request, blocking to refresh the token if it is near expiry.
        /// </summary>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await EnsureTokenFreshAsync(cancellationToken);

            string tokenToUse;
            lock (_tokenLock)
            {
                tokenToUse = _currentToken;
            }

            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", tokenToUse);
            return await base.SendAsync(request, cancellationToken);
        }
    }
}
