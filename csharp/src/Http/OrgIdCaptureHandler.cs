/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Http
{
    /// <summary>
    /// HTTP handler that captures the x-databricks-org-id header from the first successful response.
    /// This org ID is used for telemetry workspace identification.
    /// </summary>
    internal class OrgIdCaptureHandler : DelegatingHandler
    {
        private string? _capturedOrgId;

        /// <summary>
        /// Gets the captured org ID from the response header, or null if not yet captured.
        /// </summary>
        public string? CapturedOrgId => _capturedOrgId;

        public OrgIdCaptureHandler(HttpMessageHandler innerHandler)
            : base(innerHandler)
        {
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            HttpResponseMessage response = await base.SendAsync(request, cancellationToken);

            if (_capturedOrgId == null &&
                response.IsSuccessStatusCode &&
                response.Headers.TryGetValues(DatabricksConstants.OrgIdHeader, out var headerValues))
            {
                _capturedOrgId = headerValues.FirstOrDefault();
            }

            return response;
        }
    }
}
