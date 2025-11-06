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
using System.Collections.Generic;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.E2E
{
    /// <summary>
    /// Helper methods for Databricks E2E tests.
    /// </summary>
    public static class DatabricksTestHelpers
    {
        /// <summary>
        /// Extracts the host name from test configuration.
        /// Supports both explicit HostName and Uri formats.
        /// </summary>
        /// <param name="config">The test configuration.</param>
        /// <returns>The extracted host name.</returns>
        /// <exception cref="ArgumentException">Thrown if neither HostName nor Uri is set, or if Uri format is invalid.</exception>
        public static string GetHostFromConfiguration(DatabricksTestConfiguration config)
        {
            if (!string.IsNullOrEmpty(config.HostName))
            {
                return config.HostName;
            }
            else if (!string.IsNullOrEmpty(config.Uri))
            {
                if (Uri.TryCreate(config.Uri, UriKind.Absolute, out Uri? parsedUri))
                {
                    return parsedUri.Host;
                }
                else
                {
                    throw new ArgumentException($"Invalid URI format: {config.Uri}");
                }
            }
            else
            {
                throw new ArgumentException(
                    "Either HostName or Uri must be set in the test configuration file");
            }
        }

        /// <summary>
        /// Extracts the path from test configuration.
        /// Supports both explicit Path and Uri formats.
        /// </summary>
        /// <param name="config">The test configuration.</param>
        /// <returns>The extracted path.</returns>
        /// <exception cref="ArgumentException">Thrown if neither Path nor Uri is set, or if Uri format is invalid.</exception>
        public static string GetPathFromConfiguration(DatabricksTestConfiguration config)
        {
            if (!string.IsNullOrEmpty(config.Path))
            {
                return config.Path;
            }
            else if (!string.IsNullOrEmpty(config.Uri))
            {
                if (Uri.TryCreate(config.Uri, UriKind.Absolute, out Uri? parsedUri))
                {
                    return parsedUri.AbsolutePath;
                }
                else
                {
                    throw new ArgumentException($"Invalid URI format: {config.Uri}");
                }
            }
            else
            {
                throw new ArgumentException(
                    "Either Path or Uri must be set in the test configuration file");
            }
        }

        /// <summary>
        /// Gets properties with Statement Execution API enabled.
        /// </summary>
        /// <param name="env">The test environment.</param>
        /// <param name="config">The test configuration.</param>
        /// <returns>Properties dictionary with Statement Execution API enabled.</returns>
        public static Dictionary<string, string> GetPropertiesWithStatementExecutionEnabled(
            DatabricksTestEnvironment env,
            DatabricksTestConfiguration config)
        {
            var properties = env.GetDriverParameters(config);

            // Enable Statement Execution API
            properties[DatabricksParameters.Protocol] = "rest";

            // Ensure host and path are set for REST API
            if (!properties.ContainsKey(SparkParameters.HostName) && properties.ContainsKey(AdbcOptions.Uri))
            {
                // Extract host from URI if not explicitly set
                var host = GetHostFromConfiguration(config);
                properties[SparkParameters.HostName] = host;
            }

            if (!properties.ContainsKey(SparkParameters.Path) && properties.ContainsKey(AdbcOptions.Uri))
            {
                // Extract path from URI if not explicitly set
                var path = GetPathFromConfiguration(config);
                properties[SparkParameters.Path] = path;
            }

            return properties;
        }
    }
}
