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
using System.Text.RegularExpressions;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Provides mapping utilities for converting Databricks type names to JDBC/ODBC (XDBC) metadata.
    /// This ensures parity between Thrift HiveServer2 and Statement Execution API protocols
    /// by applying consistent type mappings regardless of the underlying protocol.
    ///
    /// Follows the same transformation strategy as the Databricks JDBC driver's MetadataResultSetBuilder.
    /// </summary>
    public static class DatabricksTypeMapper
    {
        /// <summary>
        /// Maps Databricks type names to XDBC (ODBC/JDBC) SQL type codes.
        /// Type codes follow the SQL/CLI specification (ISO/IEC 9075-3).
        /// </summary>
        private static readonly Dictionary<string, short> XdbcTypeCodes = new Dictionary<string, short>(StringComparer.OrdinalIgnoreCase)
        {
            // String types
            { "STRING", 12 },       // SQL_VARCHAR
            { "VARCHAR", 12 },      // SQL_VARCHAR
            { "CHAR", 1 },          // SQL_CHAR

            // Integer types
            { "TINYINT", -6 },      // SQL_TINYINT
            { "SMALLINT", 5 },      // SQL_SMALLINT
            { "INT", 4 },           // SQL_INTEGER
            { "INTEGER", 4 },       // SQL_INTEGER
            { "BIGINT", -5 },       // SQL_BIGINT

            // Floating-point types
            { "FLOAT", 6 },         // SQL_FLOAT
            { "REAL", 7 },          // SQL_REAL
            { "DOUBLE", 8 },        // SQL_DOUBLE

            // Exact numeric types
            { "DECIMAL", 3 },       // SQL_DECIMAL
            { "NUMERIC", 2 },       // SQL_NUMERIC

            // Boolean type
            { "BOOLEAN", 16 },      // SQL_BOOLEAN

            // Date/time types
            { "DATE", 91 },         // SQL_TYPE_DATE
            { "TIMESTAMP", 93 },    // SQL_TYPE_TIMESTAMP
            { "TIMESTAMP_NTZ", 93 },// SQL_TYPE_TIMESTAMP (no timezone)

            // Binary types
            { "BINARY", -2 },       // SQL_BINARY

            // Complex types
            { "ARRAY", 2003 },      // SQL_ARRAY
            { "STRUCT", 2002 },     // SQL_STRUCT
            { "MAP", 2000 },        // SQL_MAP

            // Databricks-specific types
            { "VARIANT", 1111 },    // SQL_OTHER (Databricks semi-structured type)
            { "VOID", 0 },          // SQL_NULL (represents absence of value)
            { "INTERVAL YEAR TO MONTH", 1111 },  // SQL_OTHER (year-month interval)
            { "INTERVAL DAY TO SECOND", 1111 },  // SQL_OTHER (day-time interval)

            // Null type
            { "NULL", 0 }           // SQL_NULL
        };

        /// <summary>
        /// Default column sizes for types when size is not specified.
        /// These values match the Thrift HiveServer2 protocol for ADBC parity.
        /// For numeric types, this represents the byte size of the type's binary representation.
        /// For complex types and special types, uses the value returned by Thrift HiveServer2.
        /// </summary>
        private static readonly Dictionary<string, int> DefaultColumnSizes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            { "TINYINT", 1 },       // Byte size (matches Thrift HiveServer2)
            { "SMALLINT", 2 },      // Byte size (matches Thrift HiveServer2)
            { "INT", 4 },           // Byte size (matches Thrift HiveServer2)
            { "INTEGER", 4 },
            { "BIGINT", 8 },        // Byte size (matches Thrift HiveServer2)
            { "FLOAT", 4 },         // Byte size (matches Thrift HiveServer2)
            { "REAL", 4 },
            { "DOUBLE", 8 },        // Byte size (matches Thrift HiveServer2)
            { "DECIMAL", 38 },      // Default precision
            { "NUMERIC", 38 },
            { "BOOLEAN", 1 },       // Byte size
            { "DATE", 4 },          // Byte size (matches Thrift HiveServer2)
            { "TIMESTAMP", 8 },     // Byte size (matches Thrift HiveServer2)
            { "TIMESTAMP_NTZ", 8 },
            { "STRING", 2147483647 },    // Max int32 (matches Thrift HiveServer2)
            { "VARCHAR", 2147483647 },
            { "CHAR", 2147483647 },
            { "BINARY", 0 },        // Thrift HiveServer2 returns 0 for unbounded binary
            { "ARRAY", 0 },         // Thrift HiveServer2 returns 0 for complex types
            { "MAP", 0 },           // Thrift HiveServer2 returns 0 for complex types
            { "STRUCT", 0 },        // Thrift HiveServer2 returns 0 for complex types
            { "VARIANT", 0 },       // Thrift HiveServer2 returns 0 for semi-structured types
            { "VOID", 1 }           // Thrift HiveServer2 returns 1 for VOID
        };

        /// <summary>
        /// Buffer lengths (in bytes) for binary representation of types.
        /// Used for the BUFFER_LENGTH JDBC metadata field.
        /// </summary>
        private static readonly Dictionary<string, int> BufferLengths = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            { "TINYINT", 1 },
            { "SMALLINT", 2 },
            { "INT", 4 },
            { "INTEGER", 4 },
            { "BIGINT", 8 },
            { "FLOAT", 4 },
            { "REAL", 4 },
            { "DOUBLE", 8 },
            { "BOOLEAN", 1 }
        };

        /// <summary>
        /// Numeric precision radix for numeric types.
        /// Used for the NUM_PREC_RADIX JDBC metadata field.
        /// All numeric types use base-10 to match Thrift HiveServer2 protocol.
        /// </summary>
        private const short DecimalRadix = 10; // Base-10 for all numeric types (matches Thrift HiveServer2)
        private const short BinaryRadix = 2;   // Base-2 (unused, kept for potential future use)

        /// <summary>
        /// Extracts the base type name from a parameterized Databricks type string.
        /// Applies XDBC normalization for compatibility with Thrift HiveServer2 protocol.
        /// Examples:
        ///   "INT" -> "INTEGER" (XDBC normalization)
        ///   "DECIMAL(10,2)" -> "DECIMAL"
        ///   "VARCHAR(50)" -> "VARCHAR"
        ///   "ARRAY<STRING>" -> "ARRAY"
        ///   "STRUCT<name: STRING, age: INT>" -> "STRUCT"
        ///   "MAP<STRING, INT>" -> "MAP"
        ///   "TIMESTAMP_NTZ" -> "TIMESTAMP" (XDBC normalization)
        ///   "INTERVAL YEAR TO MONTH" -> "INTERVAL" (XDBC normalization)
        /// </summary>
        /// <param name="typeName">The full Databricks type name</param>
        /// <returns>The base type name without parameters, with XDBC normalization applied</returns>
        public static string StripBaseTypeName(string? typeName)
        {
            if (string.IsNullOrEmpty(typeName))
                return string.Empty;

            // Check for angle brackets (complex types: ARRAY<T>, STRUCT<...>, MAP<K,V>)
            int angleIndex = typeName.IndexOf('<');
            if (angleIndex != -1)
            {
                return typeName.Substring(0, angleIndex).ToUpperInvariant();
            }

            // Check for parentheses (parameterized types: DECIMAL(p,s), VARCHAR(n), CHAR(n))
            int parenIndex = typeName.IndexOf('(');
            if (parenIndex != -1)
            {
                return typeName.Substring(0, parenIndex).ToUpperInvariant();
            }

            var baseType = typeName.ToUpperInvariant();

            // Apply XDBC normalization to match Thrift HiveServer2 behavior
            if (baseType == "TIMESTAMP_NTZ")
                return "TIMESTAMP";

            if (baseType.StartsWith("INTERVAL ", StringComparison.OrdinalIgnoreCase))
                return "INTERVAL";

            // Normalize INT to INTEGER to match Thrift HiveServer2
            if (baseType == "INT")
                return "INTEGER";

            return baseType;
        }

        /// <summary>
        /// Maps a Databricks type name to its XDBC (ODBC/JDBC) SQL type code.
        /// Returns null if the type is not recognized.
        /// </summary>
        /// <param name="typeName">The Databricks type name (e.g., "INT", "VARCHAR", "ARRAY")</param>
        /// <returns>The XDBC type code, or null if not found</returns>
        public static short? GetXdbcDataType(string typeName)
        {
            if (string.IsNullOrEmpty(typeName))
                return null;

            // For INTERVAL types, check the full type name first before stripping
            // This allows us to distinguish INTERVAL YEAR TO MONTH from INTERVAL DAY TO SECOND
            var upperTypeName = typeName.ToUpperInvariant();
            if (upperTypeName.StartsWith("INTERVAL ", StringComparison.OrdinalIgnoreCase))
            {
                // Try exact match first for full INTERVAL type names
                if (XdbcTypeCodes.TryGetValue(upperTypeName, out var intervalCode))
                {
                    return intervalCode;
                }
            }

            var baseType = StripBaseTypeName(typeName);
            if (XdbcTypeCodes.TryGetValue(baseType, out var code))
            {
                return code;
            }

            return null; // Unknown type
        }

        /// <summary>
        /// Calculates the COLUMN_SIZE JDBC metadata field for a Databricks type.
        /// For numeric types (INT, BIGINT, FLOAT, DOUBLE, etc.), returns the byte size of the binary representation.
        /// For DECIMAL/NUMERIC types, returns the precision.
        /// For character types, returns the maximum character length.
        /// This matches the Thrift HiveServer2 protocol behavior for ADBC parity.
        /// </summary>
        /// <param name="typeName">The full Databricks type name (e.g., "DECIMAL(10,2)", "VARCHAR(100)")</param>
        /// <returns>The column size, or null if not applicable</returns>
        public static int? GetColumnSize(string typeName)
        {
            if (string.IsNullOrEmpty(typeName))
                return null;

            // Special handling for INTERVAL types before normalization
            // Check original typeName to distinguish YEAR TO MONTH (4 bytes) from DAY TO SECOND (8 bytes)
            var upperTypeName = typeName.ToUpperInvariant();
            if (upperTypeName.StartsWith("INTERVAL ", StringComparison.OrdinalIgnoreCase))
            {
                if (upperTypeName.Contains("YEAR") || upperTypeName.Contains("MONTH"))
                    return 4; // INTERVAL YEAR TO MONTH
                if (upperTypeName.Contains("DAY") || upperTypeName.Contains("SECOND"))
                    return 8; // INTERVAL DAY TO SECOND
                return 4; // Default INTERVAL size
            }

            var baseType = StripBaseTypeName(typeName);

            // For DECIMAL(p,s), extract precision
            if (baseType == "DECIMAL" || baseType == "NUMERIC")
            {
                var precision = ExtractPrecision(typeName);
                if (precision.HasValue)
                    return precision.Value;

                if (DefaultColumnSizes.TryGetValue(baseType, out var decimalSize))
                    return decimalSize;
            }

            // For VARCHAR(n) or CHAR(n), extract length
            if (baseType == "VARCHAR" || baseType == "CHAR")
            {
                var length = ExtractLength(typeName);
                if (length.HasValue)
                    return length.Value;

                // Default length for string types
                if (DefaultColumnSizes.TryGetValue(baseType, out var stringSize))
                    return stringSize;
                return 65535;
            }

            // For other types, use default size
            if (DefaultColumnSizes.TryGetValue(baseType, out var size))
            {
                return size;
            }

            // For complex types (ARRAY, STRUCT, MAP) or unknown types, return a large default
            return 2147483647; // Max int32
        }

        /// <summary>
        /// Calculates the BUFFER_LENGTH JDBC metadata field for a Databricks type.
        /// This is the maximum number of bytes required to store the binary representation.
        /// Returns null for types where buffer length is not applicable (e.g., strings, complex types).
        /// </summary>
        /// <param name="typeName">The Databricks type name</param>
        /// <returns>The buffer length in bytes, or null if not applicable</returns>
        public static int? GetBufferLength(string typeName)
        {
            if (string.IsNullOrEmpty(typeName))
                return null;

            var baseType = StripBaseTypeName(typeName);

            if (BufferLengths.TryGetValue(baseType, out var length))
            {
                return length;
            }

            // For DECIMAL, buffer length is calculated based on precision
            if (baseType == "DECIMAL" || baseType == "NUMERIC")
            {
                var precision = ExtractPrecision(typeName);
                if (precision.HasValue)
                {
                    // Approximate: 5 bytes per 9 digits + 1 byte overhead
                    return ((precision.Value + 8) / 9) * 5 + 1;
                }
                return 20; // Default for DECIMAL without specified precision
            }

            // Buffer length not applicable for strings, dates, and complex types
            return null;
        }

        /// <summary>
        /// Calculates the CHAR_OCTET_LENGTH JDBC metadata field for character types.
        /// This is the maximum number of bytes in the character representation.
        /// Returns null for non-character types.
        /// </summary>
        /// <param name="typeName">The full Databricks type name (e.g., "VARCHAR(100)", "CHAR(50)")</param>
        /// <returns>The character octet length, or null if not applicable</returns>
        public static int? GetCharOctetLength(string typeName)
        {
            if (string.IsNullOrEmpty(typeName))
                return null;

            var baseType = StripBaseTypeName(typeName);

            // Only applicable for character types
            if (baseType != "STRING" && baseType != "VARCHAR" && baseType != "CHAR")
                return null;

            // Extract length from VARCHAR(n) or CHAR(n)
            var length = ExtractLength(typeName);
            if (length.HasValue)
                return length.Value;

            // Default for unbounded strings
            if (DefaultColumnSizes.TryGetValue(baseType, out var defaultSize))
                return defaultSize;
            return 65535;
        }

        /// <summary>
        /// Extracts the DECIMAL_DIGITS (scale) JDBC metadata field for numeric types.
        /// For DECIMAL(p,s), this returns s (the number of digits after the decimal point).
        /// For integer types, returns 0.
        /// For floating-point types, returns the fractional precision (matches Thrift HiveServer2).
        /// For TIMESTAMP types, returns 6 (microsecond precision).
        /// </summary>
        /// <param name="typeName">The full Databricks type name (e.g., "DECIMAL(10,2)")</param>
        /// <returns>The decimal digits (scale), or null if not applicable</returns>
        public static int? GetDecimalDigits(string typeName)
        {
            if (string.IsNullOrEmpty(typeName))
                return null;

            var baseType = StripBaseTypeName(typeName);

            // For DECIMAL(p,s), extract scale
            if (baseType == "DECIMAL" || baseType == "NUMERIC")
            {
                var scale = ExtractScale(typeName);
                return scale ?? 0; // Default scale is 0
            }

            // For integer types, scale is always 0
            if (baseType == "TINYINT" || baseType == "SMALLINT" ||
                baseType == "INT" || baseType == "INTEGER" || baseType == "BIGINT")
            {
                return 0;
            }

            // For floating-point types, return fractional precision (matches Thrift HiveServer2)
            if (baseType == "FLOAT" || baseType == "REAL")
            {
                return 7; // Single precision fractional digits
            }

            if (baseType == "DOUBLE")
            {
                return 15; // Double precision fractional digits
            }

            // For TIMESTAMP types, return fractional seconds precision (matches Thrift HiveServer2)
            if (baseType == "TIMESTAMP" || baseType == "TIMESTAMP_NTZ")
            {
                return 6; // Microsecond precision
            }

            // For DATE and STRING types, return 0 (matches Thrift HiveServer2)
            if (baseType == "DATE" || baseType == "STRING")
            {
                return 0;
            }

            // For all other types, return 0 for statement-based output compatibility (matches Thrift HiveServer2)
            // Thrift consistently returns 0 instead of null for types where decimal digits don't apply
            return 0;
        }

        /// <summary>
        /// Gets the NUM_PREC_RADIX JDBC metadata field for numeric types.
        /// Returns 10 for all numeric types to match Thrift HiveServer2 protocol.
        /// Returns null for non-numeric types.
        /// </summary>
        /// <param name="typeName">The Databricks type name</param>
        /// <returns>The numeric precision radix (10), or null if not applicable</returns>
        public static short? GetNumPrecRadix(string typeName)
        {
            if (string.IsNullOrEmpty(typeName))
                return null;

            var baseType = StripBaseTypeName(typeName);

            // All numeric types use base-10 (matches Thrift HiveServer2)
            if (baseType == "TINYINT" || baseType == "SMALLINT" ||
                baseType == "INT" || baseType == "INTEGER" || baseType == "BIGINT" ||
                baseType == "DECIMAL" || baseType == "NUMERIC" ||
                baseType == "FLOAT" || baseType == "REAL" || baseType == "DOUBLE")
            {
                return DecimalRadix;
            }

            return null; // Not applicable for non-numeric types
        }

        /// <summary>
        /// Extracts precision from DECIMAL(p,s) or NUMERIC(p,s) type string.
        /// Returns null if precision is not specified or cannot be parsed.
        /// </summary>
        /// <param name="typeName">The type string (e.g., "DECIMAL(10,2)")</param>
        /// <returns>The precision (p), or null if not found</returns>
        private static int? ExtractPrecision(string typeName)
        {
            // Pattern: DECIMAL(precision,scale) or DECIMAL(precision)
            var match = Regex.Match(typeName, @"\((\d+)(?:,\d+)?\)", RegexOptions.IgnoreCase);
            if (match.Success && int.TryParse(match.Groups[1].Value, out var precision))
            {
                return precision;
            }
            return null;
        }

        /// <summary>
        /// Extracts scale from DECIMAL(p,s) or NUMERIC(p,s) type string.
        /// Returns null if scale is not specified or cannot be parsed.
        /// </summary>
        /// <param name="typeName">The type string (e.g., "DECIMAL(10,2)")</param>
        /// <returns>The scale (s), or null if not found</returns>
        private static int? ExtractScale(string typeName)
        {
            // Pattern: DECIMAL(precision,scale)
            var match = Regex.Match(typeName, @"\(\d+,(\d+)\)", RegexOptions.IgnoreCase);
            if (match.Success && int.TryParse(match.Groups[1].Value, out var scale))
            {
                return scale;
            }
            return null;
        }

        /// <summary>
        /// Extracts length from VARCHAR(n) or CHAR(n) type string.
        /// Returns null if length is not specified or cannot be parsed.
        /// </summary>
        /// <param name="typeName">The type string (e.g., "VARCHAR(100)")</param>
        /// <returns>The length (n), or null if not found</returns>
        private static int? ExtractLength(string typeName)
        {
            // Pattern: VARCHAR(length) or CHAR(length)
            var match = Regex.Match(typeName, @"\((\d+)\)", RegexOptions.IgnoreCase);
            if (match.Success && int.TryParse(match.Groups[1].Value, out var length))
            {
                return length;
            }
            return null;
        }

        /// <summary>
        /// Gets the SQL data type category for SQL_DATA_TYPE JDBC metadata field.
        /// This follows the ODBC specification for SQL_DATA_TYPE values.
        /// For most types, this is the same as the XDBC type code.
        /// Matches Thrift HiveServer2 behavior: returns the specific type code (e.g., 91 for DATE, 93 for TIMESTAMP).
        /// </summary>
        /// <param name="typeName">The Databricks type name</param>
        /// <returns>The SQL data type code, or null if not applicable</returns>
        public static short? GetSqlDataType(string typeName)
        {
            // SQL_DATA_TYPE is the same as DATA_TYPE (XDBC type code) for all types
            // This matches Thrift HiveServer2 behavior
            return GetXdbcDataType(typeName);
        }

        /// <summary>
        /// Gets the SQL datetime subtype for SQL_DATETIME_SUB JDBC metadata field.
        /// Only applicable for date/time types.
        /// Returns null to match Thrift HiveServer2 behavior (uses specific type codes in SQL_DATA_TYPE instead).
        /// </summary>
        /// <param name="typeName">The Databricks type name</param>
        /// <returns>The datetime subtype code, or null if not a datetime type</returns>
        public static short? GetSqlDatetimeSub(string typeName)
        {
            // Thrift HiveServer2 returns null for SQL_DATETIME_SUB because it uses
            // specific type codes (91 for DATE, 93 for TIMESTAMP) in SQL_DATA_TYPE
            // instead of the generic SQL_DATETIME (9) approach
            return null;
        }
    }
}
