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
using Apache.Arrow.Adbc.Drivers.Apache;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Databricks-specific implementation of <see cref="AdbcDatabase"/>
    /// </summary>
    public class DatabricksDatabase : AdbcDatabase
    {
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
    }
}
