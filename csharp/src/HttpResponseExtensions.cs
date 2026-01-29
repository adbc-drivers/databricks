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

using System.Net.Http;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Extension methods for HttpResponseMessage.
    /// </summary>
    internal static class HttpResponseExtensions
    {
        /// <summary>
        /// Throws an <see cref="HttpExceptionWithStatusCode"/> if the HTTP response was not successful.
        /// This is similar to <see cref="HttpResponseMessage.EnsureSuccessStatusCode"/> but throws
        /// our custom exception that always contains the status code, solving .NET version compatibility issues.
        /// </summary>
        /// <param name="response">The HTTP response message to check.</param>
        /// <exception cref="HttpExceptionWithStatusCode">
        /// Thrown when the response status code indicates a failure (not in the 2xx range).
        /// </exception>
        public static void EnsureSuccessOrThrow(this HttpResponseMessage response)
        {
            if (!response.IsSuccessStatusCode)
            {
                string message = $"Response status code does not indicate success: {(int)response.StatusCode} ({response.StatusCode}).";
                throw new HttpExceptionWithStatusCode(message, response.StatusCode);
            }
        }
    }
}
