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
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Auth;
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Auth
{
    public class TokenRefreshDelegatingHandlerTests : IDisposable
    {
        private readonly Mock<HttpMessageHandler> _mockInnerHandler;
        private readonly Mock<ITokenExchangeClient> _mockTokenExchangeClient;
        private readonly string _initialToken = "initial-token";
        private readonly int _tokenRenewLimitMinutes = 10;
        private readonly DateTime _initialTokenExpiry = DateTime.UtcNow.AddHours(1);

        public TokenRefreshDelegatingHandlerTests()
        {
            _mockInnerHandler = new Mock<HttpMessageHandler>();
            _mockTokenExchangeClient = new Mock<ITokenExchangeClient>();
        }

        [Fact]
        public void Constructor_WithValidParameters_InitializesCorrectly()
        {
            var handler = new TokenRefreshDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _initialToken,
                _initialTokenExpiry,
                _tokenRenewLimitMinutes);

            Assert.NotNull(handler);
        }

        [Fact]
        public void Constructor_WithNullTokenExchangeClient_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new TokenRefreshDelegatingHandler(
                _mockInnerHandler.Object,
                null!,
                _initialToken,
                _initialTokenExpiry,
                _tokenRenewLimitMinutes));
        }

        [Fact]
        public void Constructor_WithNullInitialToken_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new TokenRefreshDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                null!,
                _initialTokenExpiry,
                _tokenRenewLimitMinutes));
        }

        [Fact]
        public async Task SendAsync_WithValidTokenNotNearExpiry_UsesInitialTokenWithoutRenewal()
        {
            var futureExpiry = DateTime.UtcNow.AddHours(2); // Well beyond renewal limit
            var handler = new TokenRefreshDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _initialToken,
                futureExpiry,
                _tokenRenewLimitMinutes);

            var request = new HttpRequestMessage(HttpMethod.Get, "https://example.com");
            var expectedResponse = new HttpResponseMessage(HttpStatusCode.OK);

            HttpRequestMessage? capturedRequest = null;

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest = req)
                .ReturnsAsync(expectedResponse);

            var httpClient = new HttpClient(handler);
            var response = await httpClient.SendAsync(request);

            Assert.Equal(expectedResponse, response);
            Assert.NotNull(capturedRequest);
            Assert.Equal("Bearer", capturedRequest.Headers.Authorization?.Scheme);
            Assert.Equal(_initialToken, capturedRequest.Headers.Authorization?.Parameter);

            _mockTokenExchangeClient.Verify(
                x => x.RefreshTokenAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task SendAsync_WithTokenNearExpiry_BlocksAndUsesNewToken()
        {
            var nearExpiryTime = DateTime.UtcNow.AddMinutes(5); // Within renewal limit
            var newToken = "new-renewed-token";
            var newExpiry = DateTime.UtcNow.AddHours(1);

            var handler = new TokenRefreshDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _initialToken,
                nearExpiryTime,
                _tokenRenewLimitMinutes);

            var tokenExchangeResponse = new TokenExchangeResponse
            {
                AccessToken = newToken,
                TokenType = "Bearer",
                ExpiresIn = 3600,
                ExpiryTime = newExpiry
            };

            _mockTokenExchangeClient
                .Setup(x => x.RefreshTokenAsync(_initialToken, It.IsAny<CancellationToken>()))
                .ReturnsAsync(tokenExchangeResponse);

            HttpRequestMessage? capturedRequest = null;

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest = req)
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));

            var httpClient = new HttpClient(handler);
            await httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, "https://example.com"));

            // Request blocks until refresh completes — first request already uses the new token
            Assert.NotNull(capturedRequest);
            Assert.Equal(newToken, capturedRequest.Headers.Authorization?.Parameter);

            _mockTokenExchangeClient.Verify(
                x => x.RefreshTokenAsync(_initialToken, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task SendAsync_WithTokenRefreshFailure_ContinuesWithOriginalToken()
        {
            var nearExpiryTime = DateTime.UtcNow.AddMinutes(5); // Within renewal limit

            var handler = new TokenRefreshDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _initialToken,
                nearExpiryTime,
                _tokenRenewLimitMinutes);

            _mockTokenExchangeClient
                .Setup(x => x.RefreshTokenAsync(_initialToken, It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Token refresh failed"));

            HttpRequestMessage? capturedRequest = null;

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest = req)
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));

            var httpClient = new HttpClient(handler);
            await httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, "https://example.com"));

            // Refresh failed — falls back to original token on the same (blocking) request
            Assert.NotNull(capturedRequest);
            Assert.Equal(_initialToken, capturedRequest.Headers.Authorization?.Parameter);

            _mockTokenExchangeClient.Verify(
                x => x.RefreshTokenAsync(_initialToken, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task SendAsync_WithRenewedToken_DoesNotRenewAgain()
        {
            var nearExpiryTime = DateTime.UtcNow.AddMinutes(5); // Within renewal limit
            var newToken = "new-renewed-token";
            var newExpiry = DateTime.UtcNow.AddHours(1); // New token is fresh, well beyond renewal limit

            var handler = new TokenRefreshDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _initialToken,
                nearExpiryTime,
                _tokenRenewLimitMinutes);

            var tokenExchangeResponse = new TokenExchangeResponse
            {
                AccessToken = newToken,
                TokenType = "Bearer",
                ExpiresIn = 180,
                ExpiryTime = newExpiry
            };

            _mockTokenExchangeClient
                .Setup(x => x.RefreshTokenAsync(_initialToken, It.IsAny<CancellationToken>()))
                .ReturnsAsync(tokenExchangeResponse);

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));

            var httpClient = new HttpClient(handler);

            // First request triggers blocking refresh
            await httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, "https://example.com/1"));
            // Second request — refresh already done, should not trigger again
            await httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, "https://example.com/2"));

            _mockTokenExchangeClient.Verify(
                x => x.RefreshTokenAsync(_initialToken, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task SendAsync_WithConcurrentRequests_OnlyRenewsTokenOnce()
        {
            var nearExpiryTime = DateTime.UtcNow.AddMinutes(5); // Within renewal limit
            var newToken = "new-renewed-token";
            var newExpiry = DateTime.UtcNow.AddHours(1);

            var handler = new TokenRefreshDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _initialToken,
                nearExpiryTime,
                _tokenRenewLimitMinutes);

            var tokenExchangeResponse = new TokenExchangeResponse
            {
                AccessToken = newToken,
                TokenType = "Bearer",
                ExpiresIn = 3600,
                ExpiryTime = newExpiry
            };

            // Add a small delay to token exchange to simulate concurrent access
            _mockTokenExchangeClient
                .Setup(x => x.RefreshTokenAsync(_initialToken, It.IsAny<CancellationToken>()))
                .Returns(async () =>
                {
                    await Task.Delay(200);
                    return tokenExchangeResponse;
                });

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));

            var httpClient = new HttpClient(handler);

            // Make concurrent requests
            var tasks = new[]
            {
                httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, "https://example.com/1")),
                httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, "https://example.com/2")),
                httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, "https://example.com/3"))
            };

            await Task.WhenAll(tasks);

            // Token exchange should only be called once despite concurrent requests
            _mockTokenExchangeClient.Verify(
                x => x.RefreshTokenAsync(_initialToken, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task SendAsync_WithCancellationToken_PropagatesCancellation()
        {
            var handler = new TokenRefreshDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _initialToken,
                _initialTokenExpiry,
                _tokenRenewLimitMinutes);

            var request = new HttpRequestMessage(HttpMethod.Get, "https://example.com");
            var cts = new CancellationTokenSource();

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>((req, ct) =>
                {
                    ct.ThrowIfCancellationRequested();
                    return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
                });

            cts.Cancel();
            var httpClient = new HttpClient(handler);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                httpClient.SendAsync(request, cts.Token));
        }

        [Fact]
        public async Task SendAsync_WithTokenRenewalAndCancellation_HandlesCancellationGracefully()
        {
            var nearExpiryTime = DateTime.UtcNow.AddMinutes(5); // Within renewal limit
            var handler = new TokenRefreshDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _initialToken,
                nearExpiryTime,
                _tokenRenewLimitMinutes);

            var request = new HttpRequestMessage(HttpMethod.Get, "https://example.com");
            var cts = new CancellationTokenSource();

            _mockTokenExchangeClient
                .Setup(x => x.RefreshTokenAsync(_initialToken, It.IsAny<CancellationToken>()))
                .Returns<string, CancellationToken>((token, ct) =>
                {
                    ct.ThrowIfCancellationRequested();
                    return Task.FromResult(new TokenExchangeResponse
                    {
                        AccessToken = "new-token",
                        TokenType = "Bearer",
                        ExpiresIn = 3600,
                        ExpiryTime = DateTime.UtcNow.AddHours(1)
                    });
                });

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>((req, ct) =>
                {
                    ct.ThrowIfCancellationRequested();
                    return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
                });

            cts.Cancel();
            var httpClient = new HttpClient(handler);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                httpClient.SendAsync(request, cts.Token));
        }

        [Fact]
        public void Dispose_ReleasesResources()
        {
            var handler = new TokenRefreshDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _initialToken,
                _initialTokenExpiry,
                _tokenRenewLimitMinutes);

            handler.Dispose();
            handler.Dispose(); // Should be safe to call multiple times
        }

        [Theory]
        [InlineData(0)]
        [InlineData(5)]
        [InlineData(15)]
        public async Task SendAsync_WithDifferentRenewalLimits_RenewsTokenAppropriately(int renewalLimitMinutes)
        {
            var tokenExpiryTime = DateTime.UtcNow.AddMinutes(renewalLimitMinutes / 2); // Half the renewal limit
            var handler = new TokenRefreshDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _initialToken,
                tokenExpiryTime,
                renewalLimitMinutes);

            var request = new HttpRequestMessage(HttpMethod.Get, "https://example.com");

            _mockTokenExchangeClient
                .Setup(x => x.RefreshTokenAsync(_initialToken, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TokenExchangeResponse
                {
                    AccessToken = "new-token",
                    TokenType = "Bearer",
                    ExpiresIn = 3600,
                    ExpiryTime = DateTime.UtcNow.AddHours(1)
                });

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));

            var httpClient = new HttpClient(handler);
            await httpClient.SendAsync(request);

            _mockTokenExchangeClient.Verify(
                x => x.RefreshTokenAsync(_initialToken, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _mockInnerHandler?.Object?.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
