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
        /// Builds the User-Agent string used by every Databricks driver HTTP request — feature-flag
        /// fetches, and (via delegation) the SEA statement-execution path.
        /// Format: {DriverName}/{version} [product] [user_agent_entry],
        /// e.g. <c>ADBCDatabricksDriver/1.2.3 REST custom-entry</c>.
        /// </summary>
        /// <param name="assemblyVersion">The driver version.</param>
        /// <param name="properties">Connection properties (optional, for user_agent_entry).</param>
        /// <param name="product">Optional product/component token inserted after the version
        /// (e.g. "REST" for the SEA path). Thrift adds its own "Thrift/{v}" token in the base class.</param>
        /// <returns>The User-Agent string.</returns>
        /// <remarks>
        /// The prefix is derived from <see cref="DatabricksConnection.DatabricksDriverName"/> so it
        /// stays consistent with the Thrift transport user agent (which uses the same driver name).
        /// </remarks>
        public static string GetUserAgent(string assemblyVersion, IReadOnlyDictionary<string, string>? properties = null, string? product = null)
        {
            string baseUserAgent = $"{DatabricksConnection.DatabricksDriverName.Replace(" ", "")}/{assemblyVersion}";

            if (!string.IsNullOrEmpty(product))
            {
                baseUserAgent += $" {product}";
            }

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
