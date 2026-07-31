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
        /// Returns true if this exception indicates the server rejected
        /// <c>DESC TABLE EXTENDED ... AS JSON [STATIC ONLY]</c>: SQL state 42601 (parse/syntax
        /// error — e.g. STATIC ONLY on a runtime without PR #198486) or 20000 (internal error
        /// some DBRs return when a column type cannot be converted). The driver should fall back
        /// to the multi-call metadata path. Checks SqlState first, then the message, since the
        /// SEA execute path surfaces the SQL state only inside the error message (SqlState is null).
        /// </summary>
        internal bool IsDescTableExtendedUnsupportedException()
            => IsDescTableExtendedUnsupported(this);

        /// <summary>
        /// Static helper that accepts any <see cref="AdbcException"/> so the catch clause in
        /// <c>GetColumnsExtendedViaDescTableAsync</c> works for both <see cref="DatabricksException"/>
        /// (Thrift / legacy SEA path) and <see cref="AdbcDrivers.HiveServer2.Hive2.HiveServer2Exception"/>
        /// (SEA path after the HiveServer2Exception parity change).
        /// </summary>
        internal static bool IsDescTableExtendedUnsupported(AdbcException ex)
        {
            if (ex.SqlState == "42601" || ex.SqlState == "20000")
                return true;

            var message = ex.Message;
            if (string.IsNullOrEmpty(message)) return false;
            return message.IndexOf("SQLSTATE: 42601", StringComparison.OrdinalIgnoreCase) >= 0
                || message.IndexOf("SQLSTATE: 20000", StringComparison.OrdinalIgnoreCase) >= 0;
        }
    }
}
