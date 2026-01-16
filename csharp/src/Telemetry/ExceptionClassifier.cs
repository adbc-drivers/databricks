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

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Classifies exceptions as terminal (flush immediately) or retryable (buffer until statement completes).
    ///
    /// Terminal exceptions:
    /// - Authentication failures (401, 403)
    /// - Invalid request format (400)
    /// - Resource not found (404)
    /// - AuthenticationException types
    ///
    /// Retryable exceptions:
    /// - Rate limiting (429)
    /// - Service unavailable (503)
    /// - Internal server errors (500)
    /// - Network timeouts
    /// - Connection errors
    /// </summary>
    internal static class ExceptionClassifier
    {
        /// <summary>
        /// Determines if an exception is terminal (non-retryable) and should be flushed immediately.
        /// </summary>
        /// <param name="ex">The exception to classify.</param>
        /// <returns>True if the exception is terminal and should be flushed immediately; false if it should be buffered.</returns>
        public static bool IsTerminalException(Exception ex)
        {
            if (ex == null)
            {
                return false;
            }

            // Check for authentication exceptions
            if (ex is AuthenticationException || ex is UnauthorizedAccessException)
            {
                return true;
            }

            // Check for HTTP-based exceptions
            if (ex is HttpRequestException httpEx)
            {
                return IsTerminalHttpStatusCode(httpEx);
            }

            // Check inner exception for wrapped HTTP exceptions
            if (ex.InnerException is HttpRequestException innerHttpEx)
            {
                return IsTerminalHttpStatusCode(innerHttpEx);
            }

            // Default: treat as retryable (buffer until statement completes)
            return false;
        }

        /// <summary>
        /// Determines if an HTTP status code indicates a terminal exception.
        /// </summary>
        /// <param name="httpEx">The HttpRequestException to check.</param>
        /// <returns>True if the status code is terminal; false if it's retryable.</returns>
        private static bool IsTerminalHttpStatusCode(HttpRequestException httpEx)
        {
            // Extract status code from inner WebException if present
            if (!(httpEx.InnerException is WebException webEx) ||
                !(webEx.Response is HttpWebResponse httpResponse))
            {
                // No status code means network error (retryable)
                return false;
            }

            var statusCode = (int)httpResponse.StatusCode;

            // Terminal status codes (flush immediately)
            switch (statusCode)
            {
                case 400: // Bad Request - invalid request format
                case 401: // Unauthorized - authentication failure
                case 403: // Forbidden - permission denied
                case 404: // Not Found - resource not found
                    return true;

                // Retryable status codes (buffer until statement completes)
                case 429: // Too Many Requests - rate limiting
                case 500: // Internal Server Error
                case 503: // Service Unavailable
                    return false;

                default:
                    // Other status codes: treat as retryable by default
                    return false;
            }
        }
    }
}
