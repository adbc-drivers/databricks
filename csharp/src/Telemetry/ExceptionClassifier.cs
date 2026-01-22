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
using System.Net.Sockets;
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
        /// Terminal HTTP status codes that indicate non-recoverable errors.
        /// </summary>
        private static readonly HttpStatusCode[] TerminalStatusCodes = new[]
        {
            HttpStatusCode.BadRequest,       // 400
            HttpStatusCode.Unauthorized,     // 401
            HttpStatusCode.Forbidden,        // 403
            HttpStatusCode.NotFound          // 404
        };

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

            // Check for terminal HTTP status codes
            if (TryGetHttpStatusCode(exception, out HttpStatusCode statusCode))
            {
                return IsTerminalHttpStatus(statusCode);
            }

            // Check inner exception (handles wrapped HttpRequestException)
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
        /// Tries to extract HTTP status code from an exception.
        /// </summary>
        private static bool TryGetHttpStatusCode(Exception exception, out HttpStatusCode statusCode)
        {
            statusCode = default;

            if (exception is HttpRequestException httpException)
            {
#if NET5_0_OR_GREATER
                if (httpException.StatusCode.HasValue)
                {
                    statusCode = httpException.StatusCode.Value;
                    return true;
                }
#endif
                // For older .NET versions, try to parse from message
                // HttpRequestException doesn't expose StatusCode property in netstandard2.0
                return TryParseStatusCodeFromMessage(httpException.Message, out statusCode);
            }

            return false;
        }

        /// <summary>
        /// Tries to parse HTTP status code from exception message.
        /// This is a fallback for .NET versions where HttpRequestException doesn't expose StatusCode.
        /// </summary>
        private static bool TryParseStatusCodeFromMessage(string? message, out HttpStatusCode statusCode)
        {
            statusCode = default;

            if (string.IsNullOrEmpty(message))
            {
                return false;
            }

            // Common message patterns: "Response status code does not indicate success: 401 (Unauthorized)."
            // or "401 (Unauthorized)"
            foreach (var code in TerminalStatusCodes)
            {
                int codeValue = (int)code;
                if (message!.Contains(codeValue.ToString()))
                {
                    statusCode = code;
                    return true;
                }
            }

            // Check for retryable status codes as well for completeness
            int[] retryableCodes = { 429, 500, 502, 503, 504 };
            foreach (int code in retryableCodes)
            {
                if (message!.Contains(code.ToString()))
                {
                    statusCode = (HttpStatusCode)code;
                    return true;
                }
            }

            return false;
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
