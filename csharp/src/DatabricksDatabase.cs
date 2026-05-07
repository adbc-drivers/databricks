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
using AdbcDrivers.Databricks.StatementExecution;
using Apache.Arrow.Adbc;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Spark;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Databricks-specific implementation of <see cref="AdbcDatabase"/>
    /// </summary>
    public class DatabricksDatabase : AdbcDatabase
    {
        /// <summary>
        /// The environment variable name that contains the path to the default Databricks configuration file.
        /// </summary>
        public const string DefaultConfigEnvironmentVariable = "DATABRICKS_CONFIG_FILE";

        internal static readonly string s_assemblyVersion = ApacheUtility.GetAssemblyVersion(typeof(DatabricksDatabase));

        readonly IReadOnlyDictionary<string, string> properties;

        /// <summary>
        /// RecyclableMemoryStreamManager for LZ4 decompression output streams.
        /// Shared across all connections from this database to enable memory pooling.
        /// This manager is instance-based to allow cleanup when the database is disposed.
        /// </summary>
        internal readonly Microsoft.IO.RecyclableMemoryStreamManager RecyclableMemoryStreamManager =
            new Microsoft.IO.RecyclableMemoryStreamManager();

        /// <summary>
        /// LZ4 buffer pool for decompression shared across all connections from this database.
        /// Sized for 4MB buffers (Databricks maxBlockSize) with capacity for 10 buffers.
        /// This pool is instance-based to allow cleanup when the database is disposed.
        /// </summary>
        internal readonly System.Buffers.ArrayPool<byte> Lz4BufferPool =
            System.Buffers.ArrayPool<byte>.Create(maxArrayLength: 4 * 1024 * 1024, maxArraysPerBucket: 10);

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

                // Merge with environment config (DATABRICKS_CONFIG_FILE) and feature flags from server
                mergedProperties = MergeWithEnvironmentConfigAndFeatureFlags(mergedProperties);

                // Check protocol selection
                string protocol = "thrift"; // default
                if (mergedProperties.TryGetValue(DatabricksParameters.Protocol, out var protocolValue))
                {
                    protocol = protocolValue.ToLowerInvariant();
                }

                AdbcConnection connection;

                if (protocol == "rest")
                {
                    // Use Statement Execution REST API
                    // The connection creates its own HTTP client with proper handler chain
                    // including TracingDelegatingHandler, RetryHttpHandler, and OAuth authentication
                    // handlers (OAuthDelegatingHandler, TokenRefreshDelegatingHandler,
                    // MandatoryTokenExchangeDelegatingHandler) when OAuth auth is configured
                    connection = new StatementExecutionConnection(
                        mergedProperties,
                        this.RecyclableMemoryStreamManager,
                        this.Lz4BufferPool);

                    // Open the connection to create session if needed
                    var statementConnection = (StatementExecutionConnection)connection;
                    statementConnection.OpenAsync().Wait();
                }
                else if (protocol == "thrift")
                {
                    // Use traditional Thrift/HiveServer2 protocol
                    connection = new DatabricksConnection(
                        mergedProperties,
                        this.RecyclableMemoryStreamManager,
                        this.Lz4BufferPool);

                    var databricksConnection = (DatabricksConnection)connection;
                    databricksConnection.OpenAsync().Wait();
                    databricksConnection.ApplyServerSidePropertiesAsync().Wait();
                }
                else
                {
                    throw new ArgumentException(
                        $"Unsupported protocol: '{protocol}'. Supported values are 'thrift' and 'rest'.",
                        nameof(mergedProperties));
                }

                return connection;
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
        /// Merges properties with environment config and feature flags from server.
        /// This is the single place where all property merging happens for both Thrift and REST connections.
        /// </summary>
        /// <param name="properties">Properties to merge.</param>
        /// <returns>Merged properties dictionary.</returns>
        private static IReadOnlyDictionary<string, string> MergeWithEnvironmentConfigAndFeatureFlags(IReadOnlyDictionary<string, string> properties)
        {
            // First, merge with environment config
            var mergedWithEnvConfig = MergeWithDefaultEnvironmentConfig(properties);

            // Then, merge with feature flags from server (cached per host)
            return FeatureFlagCache.GetInstance()
                .MergePropertiesWithFeatureFlags(mergedWithEnvConfig, s_assemblyVersion);
        }

        /// <summary>
        /// Automatically merges properties from the default DATABRICKS_CONFIG_FILE environment variable with passed-in properties.
        /// The merge priority is controlled by the "adbc.databricks.driver_config_take_precedence" property.
        /// If DATABRICKS_CONFIG_FILE is not set or invalid, only passed-in properties are used.
        /// </summary>
        /// <param name="properties">Properties passed to constructor.</param>
        /// <returns>Merged properties dictionary.</returns>
        private static IReadOnlyDictionary<string, string> MergeWithDefaultEnvironmentConfig(IReadOnlyDictionary<string, string> properties)
        {
            // Try to load configuration from the default environment variable
            var environmentConfig = DatabricksConfiguration.TryFromEnvironmentVariable(DefaultConfigEnvironmentVariable);

            if (environmentConfig != null)
            {
                // Determine precedence setting - check passed-in properties first, then environment config
                bool driverConfigTakesPrecedence = DetermineDriverConfigPrecedence(properties, environmentConfig.Properties);

                if (driverConfigTakesPrecedence)
                {
                    // Environment config properties override passed-in properties
                    return MergeProperties(properties, environmentConfig.Properties);
                }
                else
                {
                    // Passed-in properties override environment config properties (default behavior)
                    return MergeProperties(environmentConfig.Properties, properties);
                }
            }

            // No environment config available, use only passed-in properties
            return properties;
        }

        /// <summary>
        /// Determines whether driver configuration should take precedence based on the precedence property.
        /// Checks passed-in properties first, then environment properties, defaulting to false.
        /// </summary>
        /// <param name="passedInProperties">Properties passed to constructor.</param>
        /// <param name="environmentProperties">Properties loaded from environment configuration.</param>
        /// <returns>True if driver config should take precedence, false otherwise.</returns>
        private static bool DetermineDriverConfigPrecedence(IReadOnlyDictionary<string, string> passedInProperties, IReadOnlyDictionary<string, string> environmentProperties)
        {
            // Priority 1: Check passed-in properties for precedence setting
            if (passedInProperties.TryGetValue(DatabricksParameters.DriverConfigTakePrecedence, out string? passedInValue))
            {
                if (bool.TryParse(passedInValue, out bool passedInPrecedence))
                {
                    return passedInPrecedence;
                }
            }

            // Priority 2: Check environment config for precedence setting
            if (environmentProperties.TryGetValue(DatabricksParameters.DriverConfigTakePrecedence, out string? environmentValue))
            {
                if (bool.TryParse(environmentValue, out bool environmentPrecedence))
                {
                    return environmentPrecedence;
                }
            }

            // Default: Passed-in properties override environment config (current behavior)
            return false;
        }

        /// <summary>
        /// Merges two property dictionaries, with additional properties taking precedence.
        /// </summary>
        /// <param name="baseProperties">Base properties dictionary.</param>
        /// <param name="additionalProperties">Additional properties to merge. These take precedence over base properties.</param>
        /// <returns>Merged properties dictionary.</returns>
        private static IReadOnlyDictionary<string, string> MergeProperties(IReadOnlyDictionary<string, string> baseProperties, IReadOnlyDictionary<string, string>? additionalProperties)
        {
            if (additionalProperties == null || additionalProperties.Count == 0)
            {
                return baseProperties;
            }

            var merged = new Dictionary<string, string>();

            // Add base properties first
            foreach (var kvp in baseProperties)
            {
                merged[kvp.Key] = kvp.Value;
            }

            // Additional properties override base properties
            foreach (var kvp in additionalProperties)
            {
                merged[kvp.Key] = kvp.Value;
            }

            return merged;
        }
    }
}
