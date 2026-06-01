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
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Auth;
using Apache.Arrow.Adbc;
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Auth
{
    public class OAuthClientCredentialsProviderTests : IDisposable
    {
        private readonly Mock<HttpMessageHandler> _mockHttpMessageHandler;
        private readonly HttpClient _httpClient;
        private readonly string _testHost = "test.databricks.com";
        private readonly string _clientId = "test-client-id";
        private readonly string _clientSecret = "test-client-secret";

        public OAuthClientCredentialsProviderTests()
        {
            _mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            _httpClient = new HttpClient(_mockHttpMessageHandler.Object);
        }

        private void SetupMockResponse(HttpStatusCode statusCode, string responseContent)
        {
            _mockHttpMessageHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    StatusCode = statusCode,
                    Content = new StringContent(responseContent)
                });
        }

        [Fact]
        public async Task GetAccessTokenAsync_WithValidResponse_ReturnsAccessToken()
        {
            var expectedToken = "access-token-123";
            var responseJson = JsonSerializer.Serialize(new
            {
                access_token = expectedToken,
                expires_in = 3600,
                scope = "sql"
            });
            SetupMockResponse(HttpStatusCode.OK, responseJson);

            var provider = new OAuthClientCredentialsProvider(
                _httpClient, _clientId, _clientSecret, _testHost);

            string token = await provider.GetAccessTokenAsync();

            Assert.Equal(expectedToken, token);
        }

        [Fact]
        public async Task GetAccessTokenAsync_Returns401_ThrowsUnauthorized()
        {
            SetupMockResponse(HttpStatusCode.Unauthorized, "{\"error\":\"unauthorized_client\"}");

            var provider = new OAuthClientCredentialsProvider(
                _httpClient, _clientId, _clientSecret, _testHost);

            var ex = await Assert.ThrowsAsync<DatabricksException>(
                () => provider.GetAccessTokenAsync());

            Assert.Equal(AdbcStatusCode.Unauthorized, ex.Status);
        }

        [Fact]
        public async Task GetAccessTokenAsync_Returns403_ThrowsUnauthorized()
        {
            SetupMockResponse(HttpStatusCode.Forbidden, "{\"error\":\"access_denied\"}");

            var provider = new OAuthClientCredentialsProvider(
                _httpClient, _clientId, _clientSecret, _testHost);

            var ex = await Assert.ThrowsAsync<DatabricksException>(
                () => provider.GetAccessTokenAsync());

            Assert.Equal(AdbcStatusCode.Unauthorized, ex.Status);
        }

        [Fact]
        public async Task GetAccessTokenAsync_Returns500_ThrowsUnknownError()
        {
            SetupMockResponse(HttpStatusCode.InternalServerError, "{\"error\":\"server_error\"}");

            var provider = new OAuthClientCredentialsProvider(
                _httpClient, _clientId, _clientSecret, _testHost);

            var ex = await Assert.ThrowsAsync<DatabricksException>(
                () => provider.GetAccessTokenAsync());

            Assert.Equal(AdbcStatusCode.UnknownError, ex.Status);
        }

        public void Dispose()
        {
            _httpClient.Dispose();
        }
    }
}
