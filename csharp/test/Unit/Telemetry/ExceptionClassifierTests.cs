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
using System.Security.Authentication;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for ExceptionClassifier to ensure correct classification of terminal vs retryable exceptions.
    /// </summary>
    public class ExceptionClassifierTests
    {
        // Terminal Exception Tests (should return true)

        [Fact]
        public void ExceptionClassifier_IsTerminalException_401_ReturnsTrue()
        {
            // Arrange
            var httpEx = CreateHttpRequestException(HttpStatusCode.Unauthorized);

            // Act
            var result = ExceptionClassifier.IsTerminalException(httpEx);

            // Assert
            Assert.True(result, "HTTP 401 Unauthorized should be classified as terminal");
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_403_ReturnsTrue()
        {
            // Arrange
            var httpEx = CreateHttpRequestException(HttpStatusCode.Forbidden);

            // Act
            var result = ExceptionClassifier.IsTerminalException(httpEx);

            // Assert
            Assert.True(result, "HTTP 403 Forbidden should be classified as terminal");
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_400_ReturnsTrue()
        {
            // Arrange
            var httpEx = CreateHttpRequestException(HttpStatusCode.BadRequest);

            // Act
            var result = ExceptionClassifier.IsTerminalException(httpEx);

            // Assert
            Assert.True(result, "HTTP 400 Bad Request should be classified as terminal");
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_404_ReturnsTrue()
        {
            // Arrange
            var httpEx = CreateHttpRequestException(HttpStatusCode.NotFound);

            // Act
            var result = ExceptionClassifier.IsTerminalException(httpEx);

            // Assert
            Assert.True(result, "HTTP 404 Not Found should be classified as terminal");
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_AuthException_ReturnsTrue()
        {
            // Arrange
            var authEx = new AuthenticationException("Authentication failed");

            // Act
            var result = ExceptionClassifier.IsTerminalException(authEx);

            // Assert
            Assert.True(result, "AuthenticationException should be classified as terminal");
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_UnauthorizedAccessException_ReturnsTrue()
        {
            // Arrange
            var unauthorizedEx = new UnauthorizedAccessException("Access denied");

            // Act
            var result = ExceptionClassifier.IsTerminalException(unauthorizedEx);

            // Assert
            Assert.True(result, "UnauthorizedAccessException should be classified as terminal");
        }

        // Retryable Exception Tests (should return false)

        [Fact]
        public void ExceptionClassifier_IsTerminalException_429_ReturnsFalse()
        {
            // Arrange
            var httpEx = CreateHttpRequestException((HttpStatusCode)429); // Too Many Requests

            // Act
            var result = ExceptionClassifier.IsTerminalException(httpEx);

            // Assert
            Assert.False(result, "HTTP 429 Too Many Requests should be classified as retryable");
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_503_ReturnsFalse()
        {
            // Arrange
            var httpEx = CreateHttpRequestException(HttpStatusCode.ServiceUnavailable);

            // Act
            var result = ExceptionClassifier.IsTerminalException(httpEx);

            // Assert
            Assert.False(result, "HTTP 503 Service Unavailable should be classified as retryable");
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_500_ReturnsFalse()
        {
            // Arrange
            var httpEx = CreateHttpRequestException(HttpStatusCode.InternalServerError);

            // Act
            var result = ExceptionClassifier.IsTerminalException(httpEx);

            // Assert
            Assert.False(result, "HTTP 500 Internal Server Error should be classified as retryable");
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_Timeout_ReturnsFalse()
        {
            // Arrange
            var timeoutEx = new TimeoutException("Request timed out");

            // Act
            var result = ExceptionClassifier.IsTerminalException(timeoutEx);

            // Assert
            Assert.False(result, "TimeoutException should be classified as retryable");
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_NetworkError_ReturnsFalse()
        {
            // Arrange - HttpRequestException without status code indicates network error
            var networkEx = new HttpRequestException("Network error occurred");

            // Act
            var result = ExceptionClassifier.IsTerminalException(networkEx);

            // Assert
            Assert.False(result, "Network errors (HttpRequestException without status code) should be classified as retryable");
        }

        // Edge Case Tests

        [Fact]
        public void ExceptionClassifier_IsTerminalException_NullException_ReturnsFalse()
        {
            // Arrange
            Exception? nullEx = null;

            // Act
            var result = ExceptionClassifier.IsTerminalException(nullEx!);

            // Assert
            Assert.False(result, "Null exception should return false");
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_GenericException_ReturnsFalse()
        {
            // Arrange
            var genericEx = new Exception("Some error");

            // Act
            var result = ExceptionClassifier.IsTerminalException(genericEx);

            // Assert
            Assert.False(result, "Generic exceptions should be treated as retryable by default");
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_WrappedHttpException_401_ReturnsTrue()
        {
            // Arrange
            var innerHttpEx = CreateHttpRequestException(HttpStatusCode.Unauthorized);
            var wrappedEx = new Exception("Wrapped error", innerHttpEx);

            // Act
            var result = ExceptionClassifier.IsTerminalException(wrappedEx);

            // Assert
            Assert.True(result, "Wrapped HTTP 401 exception should be classified as terminal");
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_WrappedHttpException_429_ReturnsFalse()
        {
            // Arrange
            var innerHttpEx = CreateHttpRequestException((HttpStatusCode)429);
            var wrappedEx = new Exception("Wrapped error", innerHttpEx);

            // Act
            var result = ExceptionClassifier.IsTerminalException(wrappedEx);

            // Assert
            Assert.False(result, "Wrapped HTTP 429 exception should be classified as retryable");
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_OtherHttpStatusCodes_ReturnsFalse()
        {
            // Test various other HTTP status codes that should be treated as retryable

            // Arrange & Act & Assert
            Assert.False(ExceptionClassifier.IsTerminalException(CreateHttpRequestException(HttpStatusCode.RequestTimeout)), "408 should be retryable");
            Assert.False(ExceptionClassifier.IsTerminalException(CreateHttpRequestException(HttpStatusCode.BadGateway)), "502 should be retryable");
            Assert.False(ExceptionClassifier.IsTerminalException(CreateHttpRequestException(HttpStatusCode.GatewayTimeout)), "504 should be retryable");
            Assert.False(ExceptionClassifier.IsTerminalException(CreateHttpRequestException(HttpStatusCode.OK)), "200 should be retryable (default)");
            Assert.False(ExceptionClassifier.IsTerminalException(CreateHttpRequestException(HttpStatusCode.Created)), "201 should be retryable (default)");
        }

        // Helper Methods

        /// <summary>
        /// Creates an HttpRequestException with the specified status code.
        /// Uses reflection to set the StatusCode property since HttpRequestException constructors
        /// with StatusCode are only available in .NET 5+.
        /// </summary>
        private static HttpRequestException CreateHttpRequestException(HttpStatusCode statusCode)
        {
            var message = $"HTTP {(int)statusCode} {statusCode}";

#if NET5_0_OR_GREATER
            // For .NET 5+, use the constructor with StatusCode
            return new HttpRequestException(message, null, statusCode);
#else
            // For older .NET versions, create exception and set StatusCode via reflection
            var exception = new HttpRequestException(message);
            var statusCodeProperty = typeof(HttpRequestException).GetProperty("StatusCode");
            if (statusCodeProperty != null && statusCodeProperty.CanWrite)
            {
                statusCodeProperty.SetValue(exception, statusCode);
            }
            return exception;
#endif
        }
    }
}
