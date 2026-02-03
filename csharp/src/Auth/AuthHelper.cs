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

using System.Collections.Generic;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;

namespace AdbcDrivers.Databricks.Auth
{
    /// <summary>
    /// Helper methods for authentication operations.
    /// Provides shared functionality used by HttpHandlerFactory.
    /// </summary>
    internal static class AuthHelper
    {
        /// <summary>
        /// Gets the access token from connection properties.
        /// Tries access_token first, then falls back to token.
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <returns>The token, or null if not found.</returns>
        public static string? GetTokenFromProperties(IReadOnlyDictionary<string, string> properties)
        {
            if (properties.TryGetValue(SparkParameters.AccessToken, out string? accessToken) && !string.IsNullOrEmpty(accessToken))
            {
                return accessToken;
            }

            if (properties.TryGetValue(SparkParameters.Token, out string? token) && !string.IsNullOrEmpty(token))
            {
                return token;
            }

            return null;
        }
    }
}
