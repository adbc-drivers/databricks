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

namespace AdbcDrivers.Databricks.Http
{
    /// <summary>
    /// Helper methods for building User-Agent strings.
    /// </summary>
    internal static class UserAgentHelper
    {
        /// <summary>
        /// Builds the User-Agent string for ADBC driver HTTP requests (e.g., feature flag fetching).
        /// Format: AdbcDatabricksDriver/{version} [user_agent_entry]
        /// </summary>
        /// <param name="assemblyVersion">The driver version.</param>
        /// <param name="properties">Connection properties (optional, for user_agent_entry).</param>
        /// <returns>The User-Agent string.</returns>
        /// <remarks>
        /// This is used for internal driver HTTP requests like feature flag fetching.
        /// Statement execution uses a different User-Agent (DatabricksJDBCDriverOSS) for
        /// server-side feature compatibility.
        /// </remarks>
        public static string GetUserAgent(string assemblyVersion, IReadOnlyDictionary<string, string>? properties = null)
        {
            string baseUserAgent = $"AdbcDatabricksDriver/{assemblyVersion}";

            if (properties != null)
            {
                string userAgentEntry = PropertyHelper.GetStringProperty(properties, "adbc.spark.user_agent_entry", string.Empty);
                if (!string.IsNullOrWhiteSpace(userAgentEntry))
                {
                    return $"{baseUserAgent} {userAgentEntry}";
                }
            }

            return baseUserAgent;
        }
    }
}
