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

namespace AdbcDrivers.Databricks.StatementExecution
{
    /// <summary>
    /// Validates the numeric statement options shared with the Thrift path
    /// (poll time, batch size, query timeout). Each method throws
    /// <see cref="ArgumentOutOfRangeException"/> for any invalid value — non-numeric,
    /// out of range, or overflowing — with the exact message text the Thrift driver uses
    /// (HiveServer2Statement / ApacheUtility), so SEA and Thrift behave identically.
    ///
    /// On SEA these options are read-only (set at connection level); the validators exist
    /// purely for API/behavior parity — they validate but the value is otherwise unused at
    /// the statement level.
    /// </summary>
    internal static class StatementOptionValidator
    {
        /// <summary>Poll-time in milliseconds: a numeric value greater than or equal to 0.</summary>
        public static int ValidatePollTime(string key, string value)
        {
            if (string.IsNullOrEmpty(value) || !int.TryParse(value, out int pollTimeMs) || pollTimeMs < 0)
            {
                throw new ArgumentOutOfRangeException(key, value, $"The value '{value}' for option '{key}' is invalid. Must be a numeric value greater than or equal to 0.");
            }
            return pollTimeMs;
        }

        /// <summary>Batch size: a numeric (long) value greater than zero.</summary>
        public static long ValidateBatchSize(string key, string value)
        {
            if (string.IsNullOrEmpty(value) || !long.TryParse(value, out long batchSize) || batchSize <= 0)
            {
                throw new ArgumentOutOfRangeException(key, value, $"The value '{value}' for option '{key}' is invalid. Must be a numeric value greater than zero.");
            }
            return batchSize;
        }

        /// <summary>Query timeout in seconds: a numeric value of 0 (infinite) or greater.</summary>
        public static int ValidateQueryTimeout(string key, string value)
        {
            if (string.IsNullOrEmpty(value) || !int.TryParse(value, out int queryTimeoutSecs) || queryTimeoutSecs < 0)
            {
                throw new ArgumentOutOfRangeException(key, value, $"The value '{value}' for option '{key}' is invalid. Must be a numeric value of 0 (infinite) or greater.");
            }
            return queryTimeoutSecs;
        }
    }
}
