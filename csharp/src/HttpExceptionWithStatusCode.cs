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

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// An HTTP exception that always contains the HTTP status code.
    /// This class provides a consistent way to access the HTTP status code across all .NET versions.
    /// </summary>
    public class HttpExceptionWithStatusCode : HttpRequestException
    {
        /// <summary>
        /// Gets the HTTP status code associated with the exception.
        /// </summary>
        /// <remarks>
        /// The 'new' keyword is required for .NET 5.0+ where HttpRequestException has a StatusCode property.
        /// CS0109 warning is suppressed for netstandard2.0 where HttpRequestException does not have this property.
        /// </remarks>
#pragma warning disable CS0109 // Member does not hide an inherited member (netstandard2.0)
        public new HttpStatusCode StatusCode { get; }
#pragma warning restore CS0109

        /// <summary>
        /// Initializes a new instance of the <see cref="HttpExceptionWithStatusCode"/> class.
        /// </summary>
        /// <param name="message">The error message.</param>
        /// <param name="statusCode">The HTTP status code.</param>
        public HttpExceptionWithStatusCode(string message, HttpStatusCode statusCode)
            : base(message)
        {
            StatusCode = statusCode;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HttpExceptionWithStatusCode"/> class.
        /// </summary>
        /// <param name="message">The error message.</param>
        /// <param name="statusCode">The HTTP status code.</param>
        /// <param name="innerException">The inner exception.</param>
        public HttpExceptionWithStatusCode(string message, HttpStatusCode statusCode, Exception? innerException)
            : base(message, innerException)
        {
            StatusCode = statusCode;
        }
    }
}
