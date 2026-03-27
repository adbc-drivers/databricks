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
using AdbcDrivers.Databricks;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Tests for HttpExceptionWithStatusCode and HttpResponseExtensions.
    /// </summary>
    public class HttpExceptionWithStatusCodeTests
    {
        #region HttpExceptionWithStatusCode Tests

        [Fact]
        public void HttpExceptionWithStatusCode_Constructor_SetsStatusCode()
        {
            // Arrange & Act
            var exception = new HttpExceptionWithStatusCode("Test message", HttpStatusCode.NotFound);

            // Assert
            Assert.Equal(HttpStatusCode.NotFound, exception.StatusCode);
            Assert.Equal("Test message", exception.Message);
        }

        [Fact]
        public void HttpExceptionWithStatusCode_ConstructorWithInnerException_SetsStatusCodeAndInnerException()
        {
            // Arrange
            var innerException = new InvalidOperationException("Inner error");

            // Act
            var exception = new HttpExceptionWithStatusCode("Test message", HttpStatusCode.BadRequest, innerException);

            // Assert
            Assert.Equal(HttpStatusCode.BadRequest, exception.StatusCode);
            Assert.Equal("Test message", exception.Message);
            Assert.Same(innerException, exception.InnerException);
        }

        [Fact]
        public void HttpExceptionWithStatusCode_IsHttpRequestException()
        {
            // Arrange & Act
            var exception = new HttpExceptionWithStatusCode("Test", HttpStatusCode.InternalServerError);

            // Assert
            Assert.IsAssignableFrom<HttpRequestException>(exception);
        }

        [Theory]
        [InlineData(HttpStatusCode.BadRequest)]
        [InlineData(HttpStatusCode.Unauthorized)]
        [InlineData(HttpStatusCode.Forbidden)]
        [InlineData(HttpStatusCode.NotFound)]
        [InlineData(HttpStatusCode.InternalServerError)]
        [InlineData(HttpStatusCode.ServiceUnavailable)]
        public void HttpExceptionWithStatusCode_PreservesVariousStatusCodes(HttpStatusCode statusCode)
        {
            // Arrange & Act
            var exception = new HttpExceptionWithStatusCode($"Error {(int)statusCode}", statusCode);

            // Assert
            Assert.Equal(statusCode, exception.StatusCode);
        }

        #endregion

        #region HttpResponseExtensions Tests

        [Fact]
        public void EnsureSuccessOrThrow_SuccessStatusCode_DoesNotThrow()
        {
            // Arrange
            using var response = new HttpResponseMessage(HttpStatusCode.OK);

            // Act & Assert - should not throw
            response.EnsureSuccessOrThrow();
        }

        [Theory]
        [InlineData(HttpStatusCode.OK)]
        [InlineData(HttpStatusCode.Created)]
        [InlineData(HttpStatusCode.Accepted)]
        [InlineData(HttpStatusCode.NoContent)]
        public void EnsureSuccessOrThrow_AllSuccessStatusCodes_DoNotThrow(HttpStatusCode statusCode)
        {
            // Arrange
            using var response = new HttpResponseMessage(statusCode);

            // Act & Assert - should not throw
            response.EnsureSuccessOrThrow();
        }

        [Fact]
        public void EnsureSuccessOrThrow_BadRequest_ThrowsHttpExceptionWithStatusCode()
        {
            // Arrange
            using var response = new HttpResponseMessage(HttpStatusCode.BadRequest);

            // Act & Assert
            var exception = Assert.Throws<HttpExceptionWithStatusCode>(() => response.EnsureSuccessOrThrow());
            Assert.Equal(HttpStatusCode.BadRequest, exception.StatusCode);
            Assert.Contains("400", exception.Message);
        }

        [Fact]
        public void EnsureSuccessOrThrow_Unauthorized_ThrowsHttpExceptionWithStatusCode()
        {
            // Arrange
            using var response = new HttpResponseMessage(HttpStatusCode.Unauthorized);

            // Act & Assert
            var exception = Assert.Throws<HttpExceptionWithStatusCode>(() => response.EnsureSuccessOrThrow());
            Assert.Equal(HttpStatusCode.Unauthorized, exception.StatusCode);
            Assert.Contains("401", exception.Message);
        }

        [Fact]
        public void EnsureSuccessOrThrow_Forbidden_ThrowsHttpExceptionWithStatusCode()
        {
            // Arrange
            using var response = new HttpResponseMessage(HttpStatusCode.Forbidden);

            // Act & Assert
            var exception = Assert.Throws<HttpExceptionWithStatusCode>(() => response.EnsureSuccessOrThrow());
            Assert.Equal(HttpStatusCode.Forbidden, exception.StatusCode);
            Assert.Contains("403", exception.Message);
        }

        [Fact]
        public void EnsureSuccessOrThrow_NotFound_ThrowsHttpExceptionWithStatusCode()
        {
            // Arrange
            using var response = new HttpResponseMessage(HttpStatusCode.NotFound);

            // Act & Assert
            var exception = Assert.Throws<HttpExceptionWithStatusCode>(() => response.EnsureSuccessOrThrow());
            Assert.Equal(HttpStatusCode.NotFound, exception.StatusCode);
            Assert.Contains("404", exception.Message);
        }

        [Fact]
        public void EnsureSuccessOrThrow_InternalServerError_ThrowsHttpExceptionWithStatusCode()
        {
            // Arrange
            using var response = new HttpResponseMessage(HttpStatusCode.InternalServerError);

            // Act & Assert
            var exception = Assert.Throws<HttpExceptionWithStatusCode>(() => response.EnsureSuccessOrThrow());
            Assert.Equal(HttpStatusCode.InternalServerError, exception.StatusCode);
            Assert.Contains("500", exception.Message);
        }

        [Fact]
        public void EnsureSuccessOrThrow_ServiceUnavailable_ThrowsHttpExceptionWithStatusCode()
        {
            // Arrange
            using var response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);

            // Act & Assert
            var exception = Assert.Throws<HttpExceptionWithStatusCode>(() => response.EnsureSuccessOrThrow());
            Assert.Equal(HttpStatusCode.ServiceUnavailable, exception.StatusCode);
            Assert.Contains("503", exception.Message);
        }

        [Fact]
        public void EnsureSuccessOrThrow_TooManyRequests_ThrowsHttpExceptionWithStatusCode()
        {
            // Arrange
            using var response = new HttpResponseMessage((HttpStatusCode)429);

            // Act & Assert
            var exception = Assert.Throws<HttpExceptionWithStatusCode>(() => response.EnsureSuccessOrThrow());
            Assert.Equal((HttpStatusCode)429, exception.StatusCode);
            Assert.Contains("429", exception.Message);
        }

        [Fact]
        public void EnsureSuccessOrThrow_MessageContainsStatusCodeAndReasonPhrase()
        {
            // Arrange
            using var response = new HttpResponseMessage(HttpStatusCode.NotFound);

            // Act & Assert
            var exception = Assert.Throws<HttpExceptionWithStatusCode>(() => response.EnsureSuccessOrThrow());
            Assert.Contains("404", exception.Message);
            Assert.Contains("NotFound", exception.Message);
        }

        #endregion
    }
}
