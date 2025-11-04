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

using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Databricks-specific implementation of <see cref="AdbcDatabase"/>
    /// </summary>
    public class DatabricksDatabase : AdbcDatabase
    {
        readonly IReadOnlyDictionary<string, string> properties;

        public DatabricksDatabase(IReadOnlyDictionary<string, string> properties)
        {
            this.properties = properties;
        }

        public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
        {
            try
            {
                IReadOnlyDictionary<string, string> mergedProperties = options == null
                    ? properties
                    : options
                        .Concat(properties.Where(x => !options.Keys.Contains(x.Key, StringComparer.OrdinalIgnoreCase)))
                        .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

                // Check protocol parameter to determine which connection type to create
                string protocol = "thrift"; // Default to Thrift for backward compatibility
                if (mergedProperties.TryGetValue(DatabricksParameters.Protocol, out string? protocolValue))
                {
                    protocol = protocolValue.ToLowerInvariant();
                }

                if (protocol == "rest")
                {
                    // Create REST API connection using Statement Execution API
                    return CreateRestConnection(mergedProperties);
                }
                else if (protocol == "thrift")
                {
                    // Create Thrift connection (existing behavior)
                    return CreateThriftConnection(mergedProperties);
                }
                else
                {
                    throw new ArgumentException(
                        $"Invalid protocol '{protocol}'. Supported values are 'thrift' and 'rest'.",
                        DatabricksParameters.Protocol);
                }
            }
            catch (AggregateException ae)
            {
                // Unwrap AggregateException to AdbcException if possible
                // to better conform to the ADBC standard
                if (ApacheUtility.ContainsException(ae, out AdbcException? adbcException) && adbcException != null)
                {
                    // keep the entire chain, but throw the AdbcException
                    throw new AdbcException(adbcException.Message, adbcException.Status, ae);
                }

                throw;
            }
        }

        /// <summary>
        /// Creates a Thrift-based connection (existing behavior).
        /// </summary>
        private AdbcConnection CreateThriftConnection(IReadOnlyDictionary<string, string> mergedProperties)
        {
            DatabricksConnection connection = new DatabricksConnection(mergedProperties);
            connection.OpenAsync().Wait();
            connection.ApplyServerSidePropertiesAsync().Wait();
            return connection;
        }

        /// <summary>
        /// Creates a REST API-based connection using Statement Execution API.
        /// </summary>
        private AdbcConnection CreateRestConnection(IReadOnlyDictionary<string, string> mergedProperties)
        {
            // Create HTTP client using DatabricksConnection's infrastructure
            var (httpClient, host) = DatabricksConnection.CreateHttpClientForRestApi(mergedProperties);

            // Create Statement Execution client
            var client = new StatementExecutionClient(httpClient, host);

            // Create and open connection
            var connection = new StatementExecutionConnection(client, mergedProperties);
            connection.OpenAsync().Wait();

            return connection;
        }
    }
}
