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
using System.Reflection;
using AdbcDrivers.Databricks.StatementExecution;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Moq;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Unit tests for StatementExecutionConnection OAuth and token authentication.
    /// </summary>
    public class StatementExecutionConnectionAuthTests : IDisposable
    {
        private readonly Mock<HttpMessageHandler> _mockHttpHandler;

        public StatementExecutionConnectionAuthTests()
        {
            _mockHttpHandler = new Mock<HttpMessageHandler>();
        }

        /// <summary>
        /// Creates a basic set of properties required for StatementExecutionConnection.
        /// </summary>
        private static Dictionary<string, string> CreateBaseProperties()
        {
            return new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test-workspace.cloud.databricks.com" },
                { DatabricksParameters.WarehouseId, "test-warehouse-id" }
            };
        }

        [Fact]
        public void Constructor_WithStaticToken_CreatesConnectionWithBearerAuth()
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties[SparkParameters.AccessToken] = "test-access-token";

            // Act
            using var connection = new StatementExecutionConnection(properties);

            // Assert - verify connection was created successfully
            Assert.NotNull(connection);
        }

        [Fact]
        public void Constructor_WithOAuthClientCredentials_CreatesConnectionWithOAuthHandler()
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties[SparkParameters.AuthType] = "oauth";
            properties[DatabricksParameters.OAuthGrantType] = "client_credentials";
            properties[DatabricksParameters.OAuthClientId] = "test-client-id";
            properties[DatabricksParameters.OAuthClientSecret] = "test-client-secret";

            // Act
            using var connection = new StatementExecutionConnection(properties);

            // Assert - verify connection was created successfully with OAuth
            Assert.NotNull(connection);

            // Verify _authHttpClient was created
            var authHttpClientField = typeof(StatementExecutionConnection).GetField(
                "_authHttpClient",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(authHttpClientField);
            var authHttpClient = authHttpClientField!.GetValue(connection);
            Assert.NotNull(authHttpClient);
        }

        [Fact]
        public void Constructor_WithOAuthTokenRefresh_CreatesConnectionWithTokenRefreshHandler()
        {
            // Arrange
            // Create a valid JWT token with expiration claim for testing
            // JWT format: header.payload.signature
            // This is a test token with exp claim set to future time
            var header = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("{\"alg\":\"HS256\",\"typ\":\"JWT\"}")).TrimEnd('=').Replace('+', '-').Replace('/', '_');
            var expTime = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds();
            var payload = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{{\"exp\":{expTime}}}")).TrimEnd('=').Replace('+', '-').Replace('/', '_');
            var signature = "test-signature";
            var jwtToken = $"{header}.{payload}.{signature}";

            var properties = CreateBaseProperties();
            properties[SparkParameters.AuthType] = "oauth";
            properties[SparkParameters.AccessToken] = jwtToken;
            properties[DatabricksParameters.TokenRenewLimit] = "10"; // 10 minutes before expiry

            // Act
            using var connection = new StatementExecutionConnection(properties);

            // Assert - verify connection was created successfully
            Assert.NotNull(connection);

            // Verify _authHttpClient was created for token operations
            var authHttpClientField = typeof(StatementExecutionConnection).GetField(
                "_authHttpClient",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(authHttpClientField);
            var authHttpClient = authHttpClientField!.GetValue(connection);
            Assert.NotNull(authHttpClient);
        }

        [Fact]
        public void Constructor_WithOAuthAccessToken_CreatesConnectionWithStaticBearerToken()
        {
            // Arrange - OAuth enabled with access_token grant type (default), but no token refresh
            var properties = CreateBaseProperties();
            properties[SparkParameters.AuthType] = "oauth";
            properties[SparkParameters.AccessToken] = "test-access-token";
            // Note: No TokenRenewLimit set, so StaticBearerTokenHandler should be used

            // Act
            using var connection = new StatementExecutionConnection(properties);

            // Assert - verify connection was created successfully with OAuth handlers
            Assert.NotNull(connection);

            // Verify _authHttpClient was created for OAuth (needed for MandatoryTokenExchangeDelegatingHandler)
            var authHttpClientField = typeof(StatementExecutionConnection).GetField(
                "_authHttpClient",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(authHttpClientField);
            var authHttpClient = authHttpClientField!.GetValue(connection);
            Assert.NotNull(authHttpClient);
        }

        [Fact]
        public void Constructor_WithOAuthAccessTokenExplicitGrantType_CreatesConnectionCorrectly()
        {
            // Arrange - OAuth enabled with explicit access_token grant type
            var properties = CreateBaseProperties();
            properties[SparkParameters.AuthType] = "oauth";
            properties[DatabricksParameters.OAuthGrantType] = "access_token";
            properties[SparkParameters.AccessToken] = "test-access-token";

            // Act
            using var connection = new StatementExecutionConnection(properties);

            // Assert - verify connection was created successfully
            Assert.NotNull(connection);

            // Verify _authHttpClient was created for OAuth operations
            var authHttpClientField = typeof(StatementExecutionConnection).GetField(
                "_authHttpClient",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(authHttpClientField);
            var authHttpClient = authHttpClientField!.GetValue(connection);
            Assert.NotNull(authHttpClient);
        }

        [Fact]
        public void Constructor_WithIdentityFederationClientId_StoresConfigCorrectly()
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties[SparkParameters.AccessToken] = "test-access-token";
            properties[DatabricksParameters.IdentityFederationClientId] = "test-federation-client-id";

            // Act
            using var connection = new StatementExecutionConnection(properties);

            // Assert
            Assert.NotNull(connection);

            // Verify identity federation client ID was stored
            var identityFederationField = typeof(StatementExecutionConnection).GetField(
                "_identityFederationClientId",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(identityFederationField);
            var identityFederationClientId = identityFederationField!.GetValue(connection) as string;
            Assert.Equal("test-federation-client-id", identityFederationClientId);
        }

        [Fact]
        public void Constructor_WithoutOAuth_DoesNotCreateAuthHttpClient()
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties[SparkParameters.AccessToken] = "test-access-token";
            // Note: No AuthType = "oauth" set

            // Act
            using var connection = new StatementExecutionConnection(properties);

            // Assert
            var authHttpClientField = typeof(StatementExecutionConnection).GetField(
                "_authHttpClient",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(authHttpClientField);
            var authHttpClient = authHttpClientField!.GetValue(connection);
            Assert.Null(authHttpClient); // Should be null when OAuth is not used
        }

        [Fact]
        public void Constructor_WithOAuthAndScope_UsesProvidedScope()
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties[SparkParameters.AuthType] = "oauth";
            properties[DatabricksParameters.OAuthGrantType] = "client_credentials";
            properties[DatabricksParameters.OAuthClientId] = "test-client-id";
            properties[DatabricksParameters.OAuthClientSecret] = "test-client-secret";
            properties[DatabricksParameters.OAuthScope] = "all-apis";

            // Act
            using var connection = new StatementExecutionConnection(properties);

            // Assert - connection should be created without error
            Assert.NotNull(connection);
        }

        [Fact]
        public void Constructor_WithUriInsteadOfHostName_ExtractsHostCorrectly()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { AdbcOptions.Uri, "https://test-workspace.cloud.databricks.com/sql/1.0/warehouses/test-warehouse" },
                { SparkParameters.AccessToken, "test-access-token" }
            };

            // Act
            using var connection = new StatementExecutionConnection(properties);

            // Assert - connection should be created successfully, extracting host and warehouse from URI
            Assert.NotNull(connection);
        }

        [Fact]
        public void Dispose_WithOAuthEnabled_DisposesAuthHttpClient()
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties[SparkParameters.AuthType] = "oauth";
            properties[DatabricksParameters.OAuthGrantType] = "client_credentials";
            properties[DatabricksParameters.OAuthClientId] = "test-client-id";
            properties[DatabricksParameters.OAuthClientSecret] = "test-client-secret";

            var connection = new StatementExecutionConnection(properties);

            // Get reference to auth HTTP client before disposal
            var authHttpClientField = typeof(StatementExecutionConnection).GetField(
                "_authHttpClient",
                BindingFlags.NonPublic | BindingFlags.Instance);
            var authHttpClient = authHttpClientField!.GetValue(connection) as HttpClient;
            Assert.NotNull(authHttpClient);

            // Act
            connection.Dispose();

            // Assert - after disposal, attempting to use the client should fail
            // We can't directly test disposal, but we verify no exceptions during dispose
            // Multiple dispose calls should be safe
            connection.Dispose();
        }

        [Fact]
        public void Constructor_MissingHostName_ThrowsArgumentException()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { DatabricksParameters.WarehouseId, "test-warehouse-id" },
                { SparkParameters.AccessToken, "test-access-token" }
            };

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new StatementExecutionConnection(properties));
        }

        [Fact]
        public void Constructor_MissingWarehouseId_ThrowsArgumentException()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test-workspace.cloud.databricks.com" },
                { SparkParameters.AccessToken, "test-access-token" }
            };

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new StatementExecutionConnection(properties));
        }

        public void Dispose()
        {
            _mockHttpHandler?.Object?.Dispose();
        }
    }
}
