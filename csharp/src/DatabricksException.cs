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
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Databricks
{
    public class DatabricksException : AdbcException
    {
        private string? _sqlState;
        private int _nativeError;

        public DatabricksException()
        {
        }

        public DatabricksException(string message) : base(message)
        {
        }

        public DatabricksException(string message, AdbcStatusCode statusCode) : base(message, statusCode)
        {
        }

        public DatabricksException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public DatabricksException(string message, AdbcStatusCode statusCode, Exception innerException) : base(message, statusCode, innerException)
        {
        }

        public override string? SqlState
        {
            get { return _sqlState; }
        }

        public override int NativeError
        {
            get { return _nativeError; }
        }

        internal DatabricksException SetSqlState(string sqlState)
        {
            _sqlState = sqlState;
            return this;
        }

        internal DatabricksException SetNativeError(int nativeError)
        {
            _nativeError = nativeError;
            return this;
        }

        /// <summary>
        /// Returns true if this exception indicates a catalog, schema, or table was not found.
        /// Per JDBC spec, metadata methods should return empty result sets for non-existent objects
        /// rather than throwing. Checks both SQL state (42704) and error message content for
        /// not-found indicators, matching JDBC's isObjectNotFoundException.
        /// </summary>
        internal bool IsObjectNotFoundException()
        {
            const string ObjectNotFoundSqlState = "42704";

            if (SqlState == ObjectNotFoundSqlState)
                return true;

            var message = Message;
            if (string.IsNullOrEmpty(message)) return false;
            return message.IndexOf("NO_SUCH_CATALOG_EXCEPTION", StringComparison.OrdinalIgnoreCase) >= 0
                || message.IndexOf("TABLE_OR_VIEW_NOT_FOUND", StringComparison.OrdinalIgnoreCase) >= 0
                || message.IndexOf("SCHEMA_NOT_FOUND", StringComparison.OrdinalIgnoreCase) >= 0
                || message.IndexOf("INVALID_PARAMETER_VALUE", StringComparison.OrdinalIgnoreCase) >= 0;
        }
    }
}
