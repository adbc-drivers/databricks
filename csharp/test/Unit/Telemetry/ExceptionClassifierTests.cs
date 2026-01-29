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
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Security.Authentication;
using AdbcDrivers.Databricks;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for ExceptionClassifier class.
    /// </summary>
    public class ExceptionClassifierTests
    {
        #region Terminal HTTP Status Code Tests

        [Fact]
        public void ExceptionClassifier_IsTerminalException_401_ReturnsTrue()
        {
            // Arrange
            var exception = new HttpExceptionWithStatusCode("Unauthorized", HttpStatusCode.Unauthorized);

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.True(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_403_ReturnsTrue()
        {
            // Arrange
            var exception = new HttpExceptionWithStatusCode("Forbidden", HttpStatusCode.Forbidden);

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.True(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_400_ReturnsTrue()
        {
            // Arrange
            var exception = new HttpExceptionWithStatusCode("Bad Request", HttpStatusCode.BadRequest);

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.True(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_404_ReturnsTrue()
        {
            // Arrange
            var exception = new HttpExceptionWithStatusCode("Not Found", HttpStatusCode.NotFound);

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.True(result);
        }

        #endregion

        #region Terminal Exception Type Tests

        [Fact]
        public void ExceptionClassifier_IsTerminalException_AuthException_ReturnsTrue()
        {
            // Arrange
            var exception = new AuthenticationException("Authentication failed");

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.True(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_UnauthorizedAccessException_ReturnsTrue()
        {
            // Arrange
            var exception = new UnauthorizedAccessException("Access denied");

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.True(result);
        }

        #endregion

        #region Retryable HTTP Status Code Tests

        [Fact]
        public void ExceptionClassifier_IsTerminalException_429_ReturnsFalse()
        {
            // Arrange
            var exception = new HttpExceptionWithStatusCode("Too Many Requests", (HttpStatusCode)429);

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_503_ReturnsFalse()
        {
            // Arrange
            var exception = new HttpExceptionWithStatusCode("Service Unavailable", HttpStatusCode.ServiceUnavailable);

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_500_ReturnsFalse()
        {
            // Arrange
            var exception = new HttpExceptionWithStatusCode("Internal Server Error", HttpStatusCode.InternalServerError);

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_502_ReturnsFalse()
        {
            // Arrange
            var exception = new HttpExceptionWithStatusCode("Bad Gateway", HttpStatusCode.BadGateway);

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_504_ReturnsFalse()
        {
            // Arrange
            var exception = new HttpExceptionWithStatusCode("Gateway Timeout", HttpStatusCode.GatewayTimeout);

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.False(result);
        }

        #endregion

        #region Retryable Exception Type Tests

        [Fact]
        public void ExceptionClassifier_IsTerminalException_Timeout_ReturnsFalse()
        {
            // Arrange
            var exception = new TimeoutException("Operation timed out");

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_NetworkError_ReturnsFalse()
        {
            // Arrange
            var exception = new SocketException((int)SocketError.NetworkDown);

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_ConnectionRefused_ReturnsFalse()
        {
            // Arrange
            var exception = new SocketException((int)SocketError.ConnectionRefused);

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_IOException_ReturnsFalse()
        {
            // Arrange
            var exception = new IOException("Network stream error");

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.False(result);
        }

        #endregion

        #region Wrapped Exception Tests

        [Fact]
        public void ExceptionClassifier_IsTerminalException_WrappedHttpException_ReturnsTrue()
        {
            // Arrange
            var innerException = new HttpExceptionWithStatusCode("Unauthorized", HttpStatusCode.Unauthorized);
            var wrappedException = new Exception("Wrapper exception", innerException);

            // Act
            var result = ExceptionClassifier.IsTerminalException(wrappedException);

            // Assert
            Assert.True(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_WrappedRetryableHttpException_ReturnsFalse()
        {
            // Arrange
            var innerException = new HttpExceptionWithStatusCode("Service Unavailable", HttpStatusCode.ServiceUnavailable);
            var wrappedException = new Exception("Wrapper exception", innerException);

            // Act
            var result = ExceptionClassifier.IsTerminalException(wrappedException);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_WrappedAuthException_ReturnsTrue()
        {
            // Arrange
            var innerException = new AuthenticationException("Auth failed");
            var wrappedException = new Exception("Wrapper exception", innerException);

            // Act
            var result = ExceptionClassifier.IsTerminalException(wrappedException);

            // Assert
            Assert.True(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_DeeplyNestedTerminalException_ReturnsTrue()
        {
            // Arrange
            var innermost = new HttpExceptionWithStatusCode("Forbidden", HttpStatusCode.Forbidden);
            var middle = new Exception("Middle wrapper", innermost);
            var outer = new Exception("Outer wrapper", middle);

            // Act
            var result = ExceptionClassifier.IsTerminalException(outer);

            // Assert
            Assert.True(result);
        }

        #endregion

        #region Safe Default Tests

        [Fact]
        public void ExceptionClassifier_IsTerminalException_Null_ReturnsFalse()
        {
            // Act
            var result = ExceptionClassifier.IsTerminalException(null);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_GenericException_ReturnsFalse()
        {
            // Arrange
            var exception = new Exception("Some generic error");

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_InvalidOperationException_ReturnsFalse()
        {
            // Arrange
            var exception = new InvalidOperationException("Invalid state");

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_ArgumentException_ReturnsFalse()
        {
            // Arrange
            var exception = new ArgumentException("Invalid argument");

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ExceptionClassifier_IsTerminalException_HttpRequestExceptionWithoutStatusCode_ReturnsFalse()
        {
            // Arrange - HttpRequestException without status code (network error)
            var exception = new HttpRequestException("A connection error occurred");

            // Act
            var result = ExceptionClassifier.IsTerminalException(exception);

            // Assert
            Assert.False(result);
        }

        #endregion
    }
}
