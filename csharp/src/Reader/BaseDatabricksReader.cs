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
using System.Diagnostics;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Tracing;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader
{
    /// <summary>
    /// Base class for Databricks readers that handles common functionality of DatabricksReader and CloudFetchReader.
    /// Protocol-agnostic - works with both Thrift and REST implementations.
    /// </summary>
    internal abstract class BaseDatabricksReader : TracingReader
    {
        protected readonly Schema schema;
        protected readonly IResponse? response;  // Nullable for protocol-agnostic usage
        protected readonly bool isLz4Compressed;
        protected bool hasNoMoreRows = false;
        private bool isDisposed;

        /// <summary>
        /// Gets the statement for this reader. Subclasses can decide how to provide it.
        /// Used for Thrift operations in DatabricksReader. Not used in CloudFetchReader.
        /// </summary>
        protected abstract ITracingStatement Statement { get; }

        /// <summary>
        /// Protocol-agnostic constructor.
        /// </summary>
        /// <param name="statement">The tracing statement (both Thrift and REST implement ITracingStatement).</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="response">The query response (nullable for REST API).</param>
        /// <param name="isLz4Compressed">Whether results are LZ4 compressed.</param>
        protected BaseDatabricksReader(ITracingStatement statement, Schema schema, IResponse? response, bool isLz4Compressed)
            : base(statement)
        {
            this.schema = schema;
            this.response = response;
            this.isLz4Compressed = isLz4Compressed;
        }

        public override Schema Schema { get { return schema; } }

        protected override void Dispose(bool disposing)
        {
            if (!isDisposed)
            {
                base.Dispose(disposing);
                isDisposed = true;
            }
        }

        protected void ThrowIfDisposed()
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
        }

        public override string AssemblyName => DatabricksConnection.s_assemblyName;

        public override string AssemblyVersion => DatabricksConnection.s_assemblyVersion;
    }
}
