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
using System.Security.Authentication;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Classifies exceptions as terminal or retryable for telemetry purposes.
    /// </summary>
    /// <remarks>
    /// Terminal exceptions are flushed immediately as they represent non-recoverable errors.
    /// Retryable exceptions are buffered until statement completes, as they may succeed on retry.
    ///
    /// Terminal exceptions include:
    /// - Authentication failures (HTTP 401, 403)
    /// - Invalid request format errors (HTTP 400)
    /// - Resource not found errors (HTTP 404)
    /// - AuthenticationException, UnauthorizedAccessException
    ///
    /// Retryable exceptions include:
    /// - Rate limiting (HTTP 429)
    /// - Server errors (HTTP 500, 502, 503, 504)
    /// - Network timeouts
    /// - Connection errors
    ///
    /// The safe default is to return false (retryable) for unknown exceptions.
    /// </remarks>
    internal static class ExceptionClassifier
    {
        /// <summary>
        /// Determines if an exception is terminal (should be flushed immediately).
        /// </summary>
        /// <param name="exception">The exception to classify.</param>
        /// <returns>True if the exception is terminal, false otherwise.</returns>
        /// <remarks>
        /// Returns false for null or unknown exceptions (safe default).
        /// This ensures that unexpected exceptions are buffered and only flushed
        /// if the statement ultimately fails.
        /// </remarks>
        public static bool IsTerminalException(Exception? exception)
        {
            if (exception == null)
            {
                return false;
            }

            // Check for terminal exception types
            if (IsTerminalExceptionType(exception))
            {
                return true;
            }

            // Check for terminal HTTP status codes via HttpExceptionWithStatusCode
            if (exception is HttpExceptionWithStatusCode httpException)
            {
                return IsTerminalHttpStatus(httpException.StatusCode);
            }

            // Check inner exception (handles wrapped exceptions)
            if (exception.InnerException != null)
            {
                return IsTerminalException(exception.InnerException);
            }

            // Safe default: treat unknown exceptions as retryable
            return false;
        }

        /// <summary>
        /// Determines if the exception type itself indicates a terminal error.
        /// </summary>
        private static bool IsTerminalExceptionType(Exception exception)
        {
            return exception is AuthenticationException
                || exception is UnauthorizedAccessException;
        }

        /// <summary>
        /// Determines if an HTTP status code is terminal.
        /// </summary>
        private static bool IsTerminalHttpStatus(HttpStatusCode statusCode)
        {
            int code = (int)statusCode;
            return code == 400  // BadRequest
                || code == 401  // Unauthorized
                || code == 403  // Forbidden
                || code == 404; // NotFound
        }
    }
}
