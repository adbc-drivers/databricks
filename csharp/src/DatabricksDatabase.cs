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
using System.Text.RegularExpressions;
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

                // Resolve protocol via explicit override -> httpPath auto-detection -> legacy default.
                // PECO-3055: matches JDBC's compute-resource-based protocol selection
                // (DatabricksConnectionContext.getClientTypeFromContext).
                string protocol = ResolveProtocol(mergedProperties);

                AdbcConnection connection;

                if (protocol == "rest")
                {
                    // Use Statement Execution REST API
                    // The connection creates its own HTTP client with proper handler chain
                    // including TracingDelegatingHandler, RetryHttpHandler, and OAuth authentication
                    // handlers (OAuthDelegatingHandler, TokenRefreshDelegatingHandler,
                    // MandatoryTokenExchangeDelegatingHandler) when OAuth auth is configured
                    bool fallbackToThrift = false;
                    try
                    {
                        connection = new StatementExecutionConnection(
                            mergedProperties,
                            this.RecyclableMemoryStreamManager,
                            this.Lz4BufferPool);

                        // Open the connection to create session if needed
                        var statementConnection = (StatementExecutionConnection)connection;
                        statementConnection.OpenAsync().Wait();
                    }
                    catch (Exception ex) when (!IsExplicitProtocolOverride(mergedProperties) && IsTemporaryRedirectError(ex))
                    {
                        // PECO-3055: SEA -> Thrift fallback on HTTP 307 from createSession.
                        // The redirect signals the workspace routes SEA traffic back to Thrift
                        // (e.g., region-locked SEA disable). Mirrors JDBC's
                        // DatabricksSession.open() handling of DatabricksTemporaryRedirectException.
                        // Only fall back when protocol was auto-detected — explicit "rest"
                        // selection from the user must surface the error.
                        fallbackToThrift = true;
                        connection = null!;
                    }

                    if (fallbackToThrift)
                    {
                        connection = OpenThriftConnection(mergedProperties);
                    }
                }
                else if (protocol == "thrift")
                {
                    connection = OpenThriftConnection(mergedProperties);
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

        // PECO-3055: regexes mirror the JDBC driver's path patterns
        // (DatabricksJdbcConstants.HTTP_WAREHOUSE_PATH_PATTERN / HTTP_ENDPOINT_PATH_PATTERN /
        // HTTP_CLUSTER_PATH_PATTERN). Kept here so the protocol decision logic is co-located
        // with the only consumer; the SEA-side warehouse-ID extraction in
        // StatementExecutionConnection uses its own stricter regex for /sql/1.0/(...)/{id}.
        private static readonly Regex s_warehousePathPattern =
            new Regex(@".*/warehouses/.+", RegexOptions.Compiled);
        private static readonly Regex s_endpointPathPattern =
            new Regex(@".*/endpoints/.+", RegexOptions.Compiled);
        private static readonly Regex s_clusterPathPattern =
            new Regex(@".*/o/.+/.+", RegexOptions.Compiled);

        /// <summary>
        /// PECO-3055: Resolves the protocol ("rest" for SEA, "thrift" for HiveServer2) using the same
        /// priority order as JDBC's <c>DatabricksConnectionContext.getClientTypeFromContext</c>:
        /// <list type="number">
        ///   <item>If <see cref="DatabricksParameters.Protocol"/> is set explicitly, honor it.</item>
        ///   <item>Otherwise inspect the httpPath:
        ///     <list type="bullet">
        ///       <item>Matches <c>.../o/.+/.+</c> (general-purpose cluster) → force <c>thrift</c>.</item>
        ///       <item>Matches <c>.../warehouses/.+</c> or <c>.../endpoints/.+</c> → default to <c>rest</c>.</item>
        ///     </list>
        ///   </item>
        ///   <item>Otherwise fall back to <c>thrift</c> (preserves pre-3055 behavior for
        ///     unparseable paths and unit-test fixtures that omit httpPath).</item>
        /// </list>
        /// </summary>
        internal static string ResolveProtocol(IReadOnlyDictionary<string, string> properties)
        {
            if (properties.TryGetValue(DatabricksParameters.Protocol, out var explicitProtocol)
                && !string.IsNullOrWhiteSpace(explicitProtocol))
            {
                return explicitProtocol.ToLowerInvariant();
            }

            string? httpPath = GetHttpPath(properties);
            if (!string.IsNullOrEmpty(httpPath))
            {
                // Strip query string before matching so /sql/1.0/warehouses/abc?o=123 still
                // resolves as a warehouse path.
                string pathOnly = httpPath!;
                int q = pathOnly.IndexOf('?');
                if (q >= 0) pathOnly = pathOnly.Substring(0, q);

                // GP cluster wins over warehouse/endpoint when both could match
                // (cluster paths never contain "/warehouses/" in practice, but JDBC checks
                // warehouse first then cluster; we keep the cluster check first to make the
                // forced-Thrift rule explicit per the PECO-3055 ticket description).
                if (s_clusterPathPattern.IsMatch(pathOnly))
                {
                    return "thrift";
                }
                if (s_warehousePathPattern.IsMatch(pathOnly) || s_endpointPathPattern.IsMatch(pathOnly))
                {
                    return "rest";
                }
            }

            // Fallback: legacy default. Preserves backwards compatibility for callers that
            // construct the driver without an httpPath (some integration test harnesses do this).
            return "thrift";
        }

        /// <summary>
        /// Returns true iff the caller passed <see cref="DatabricksParameters.Protocol"/> explicitly.
        /// Used to gate the SEA→Thrift 307 fallback: explicit "rest" must surface 307 errors so
        /// callers see a clear signal that SEA isn't available for their workspace.
        /// </summary>
        private static bool IsExplicitProtocolOverride(IReadOnlyDictionary<string, string> properties) =>
            properties.TryGetValue(DatabricksParameters.Protocol, out var v) && !string.IsNullOrWhiteSpace(v);

        /// <summary>
        /// Pulls the httpPath property in the same order the rest of the driver does
        /// (<see cref="SparkParameters.Path"/> first, falling back to <see cref="AdbcOptions.Uri"/>'s
        /// AbsolutePath). Mirrors <c>StatementExecutionConnection</c>'s ctor logic so protocol
        /// detection and SEA's warehouse-ID parsing always see the same path.
        /// </summary>
        private static string? GetHttpPath(IReadOnlyDictionary<string, string> properties)
        {
            if (properties.TryGetValue(SparkParameters.Path, out var path) && !string.IsNullOrEmpty(path))
            {
                return path;
            }
            if (properties.TryGetValue(AdbcOptions.Uri, out var uri) && !string.IsNullOrEmpty(uri)
                && Uri.TryCreate(uri, UriKind.Absolute, out var parsedUri))
            {
                return parsedUri.AbsolutePath;
            }
            return null;
        }

        /// <summary>
        /// Detects whether an exception chain originates from an HTTP 307 (TemporaryRedirect)
        /// response — the trigger for SEA → Thrift fallback per PECO-3055.
        /// </summary>
        /// <remarks>
        /// <para>
        /// SEA's <c>StatementExecutionClient.EnsureSuccessStatusCodeAsync</c> currently throws
        /// <see cref="DatabricksException"/> with a message containing the literal HTTP status
        /// code, so we match on <c>"status code 307"</c>. We also recognize the raw
        /// <see cref="System.Net.Http.HttpRequestException"/> with HTTP 307 in its message for
        /// the case where the redirect is surfaced before the SEA client wraps it.
        /// </para>
        /// <para>
        /// JDBC uses a dedicated <c>DatabricksTemporaryRedirectException</c> subclass for this;
        /// the C# driver doesn't carry HTTP status on its exception type yet, so we use
        /// message inspection here. A follow-up could promote this to a typed exception once
        /// SEA error mapping is refactored — kept out of scope for PECO-3055.
        /// </para>
        /// </remarks>
        private static bool IsTemporaryRedirectError(Exception ex)
        {
            for (Exception? current = ex; current != null; current = current.InnerException)
            {
                if (current.Message.IndexOf("status code 307", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return true;
                }
                if (current is System.Net.Http.HttpRequestException hre)
                {
#if NET5_0_OR_GREATER
                    if (hre.StatusCode == System.Net.HttpStatusCode.TemporaryRedirect)
                    {
                        return true;
                    }
#endif
                }
            }
            return false;
        }

        /// <summary>
        /// Constructs and opens a Thrift/HiveServer2 <see cref="DatabricksConnection"/>.
        /// Extracted so both the primary protocol="thrift" branch and the SEA→Thrift 307
        /// fallback path can share the same open sequence (open + ApplyServerSidePropertiesAsync).
        /// </summary>
        private DatabricksConnection OpenThriftConnection(IReadOnlyDictionary<string, string> mergedProperties)
        {
            var connection = new DatabricksConnection(
                mergedProperties,
                this.RecyclableMemoryStreamManager,
                this.Lz4BufferPool);

            connection.OpenAsync().Wait();
            connection.ApplyServerSidePropertiesAsync().Wait();
            return connection;
        }
    }
}
