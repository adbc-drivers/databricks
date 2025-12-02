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
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Auth
{
    /// <summary>
    /// HTTP message handler that sets a static Bearer token on all requests.
    /// Used when OAuth is configured with an access token but without token refresh.
    /// </summary>
    internal class StaticBearerTokenHandler : DelegatingHandler
    {
        private readonly string _accessToken;

        /// <summary>
        /// Initializes a new instance of the <see cref="StaticBearerTokenHandler"/> class.
        /// </summary>
        /// <param name="innerHandler">The inner handler to delegate to.</param>
        /// <param name="accessToken">The access token to use for authentication.</param>
        public StaticBearerTokenHandler(HttpMessageHandler innerHandler, string accessToken)
            : base(innerHandler)
        {
            _accessToken = accessToken ?? throw new ArgumentNullException(nameof(accessToken));
        }

        /// <summary>
        /// Sends an HTTP request with the Bearer token.
        /// </summary>
        /// <param name="request">The HTTP request message to send.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The HTTP response message.</returns>
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
            return base.SendAsync(request, cancellationToken);
        }
    }
}
