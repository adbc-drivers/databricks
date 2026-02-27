/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
using AdbcDrivers.HiveServer2.Hive2;
using static AdbcDrivers.HiveServer2.Hive2.HiveServer2Connection;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Computes column metadata (data type codes, column sizes, decimal digits,
    /// buffer lengths, etc.) from SQL type name strings. Provides defaults for
    /// metadata fields that are not returned directly by the server.
    /// </summary>
    internal static class ColumnMetadataHelper
    {
        private static readonly Dictionary<string, short> s_baseTypeToCodeMap = new(StringComparer.OrdinalIgnoreCase)
        {
            { "BOOLEAN", (short)ColumnTypeId.BOOLEAN },
            { "TINYINT", (short)ColumnTypeId.TINYINT },
            { "SMALLINT", (short)ColumnTypeId.SMALLINT },
            { "INTEGER", (short)ColumnTypeId.INTEGER },
            { "BIGINT", (short)ColumnTypeId.BIGINT },
            { "FLOAT", (short)ColumnTypeId.FLOAT },
            { "REAL", (short)ColumnTypeId.REAL },
            { "DOUBLE", (short)ColumnTypeId.DOUBLE },
            { "DECIMAL", (short)ColumnTypeId.DECIMAL },
            { "NUMERIC", (short)ColumnTypeId.NUMERIC },
            { "CHAR", (short)ColumnTypeId.CHAR },
            { "NCHAR", (short)ColumnTypeId.NCHAR },
            { "STRING", (short)ColumnTypeId.VARCHAR },
            { "VARCHAR", (short)ColumnTypeId.VARCHAR },
            { "NVARCHAR", (short)ColumnTypeId.NVARCHAR },
            { "LONGVARCHAR", (short)ColumnTypeId.LONGVARCHAR },
            { "LONGNVARCHAR", (short)ColumnTypeId.LONGNVARCHAR },
            { "BINARY", (short)ColumnTypeId.BINARY },
            { "VARBINARY", (short)ColumnTypeId.VARBINARY },
            { "LONGVARBINARY", (short)ColumnTypeId.LONGVARBINARY },
            { "DATE", (short)ColumnTypeId.DATE },
            { "TIMESTAMP", (short)ColumnTypeId.TIMESTAMP },
            { "ARRAY", (short)ColumnTypeId.ARRAY },
            { "MAP", (short)ColumnTypeId.JAVA_OBJECT },
            { "STRUCT", (short)ColumnTypeId.STRUCT },
            { "NULL", (short)ColumnTypeId.NULL },
            { "VOID", (short)ColumnTypeId.NULL },
            { "INTERVAL", (short)ColumnTypeId.OTHER },
            { "VARIANT", (short)ColumnTypeId.OTHER },
            { "OTHER", (short)ColumnTypeId.OTHER },
        };

        private static readonly Dictionary<string, string> s_aliasToBaseType = new(StringComparer.OrdinalIgnoreCase)
        {
            { "BYTE", "TINYINT" },
            { "SHORT", "SMALLINT" },
            { "LONG", "BIGINT" },
        };

        private static readonly HashSet<string> s_numericBaseTypes = new(StringComparer.OrdinalIgnoreCase)
        {
            "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
            "FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC"
        };

        private static readonly HashSet<string> s_charBaseTypes = new(StringComparer.OrdinalIgnoreCase)
        {
            "STRING", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR",
            "LONGVARCHAR", "LONGNVARCHAR"
        };

        internal static short GetDataTypeCode(string typeName)
        {
            string baseName = GetBaseTypeName(typeName);
            if (s_baseTypeToCodeMap.TryGetValue(baseName, out short code))
                return code;
            return (short)ColumnTypeId.OTHER;
        }

        internal static string GetBaseTypeName(string typeName)
        {
            if (SqlTypeNameParser<SqlTypeNameParserResult>.TryParse(typeName, out SqlTypeNameParserResult? result))
            {
                return result!.BaseTypeName;
            }
            string upper = typeName.Trim().ToUpperInvariant();
            if (s_aliasToBaseType.TryGetValue(upper, out string? canonical))
                return canonical;
            return upper;
        }

        internal static int? GetColumnSizeDefault(string typeName)
        {
            string baseName = GetBaseTypeName(typeName);
            switch (baseName)
            {
                case "BOOLEAN":
                case "TINYINT":
                    return 1;
                case "SMALLINT":
                    return 2;
                case "INTEGER":
                case "FLOAT":
                case "REAL":
                case "DATE":
                    return 4;
                case "BIGINT":
                case "DOUBLE":
                case "TIMESTAMP":
                    return 8;
                case "DECIMAL":
                case "NUMERIC":
                    return GetParsedPrecision(typeName) ?? SqlDecimalTypeParser.DecimalPrecisionDefault;
                case "VARCHAR":
                case "LONGVARCHAR":
                case "LONGNVARCHAR":
                case "NVARCHAR":
                    return GetParsedColumnSize(typeName) ?? SqlVarcharTypeParser.VarcharColumnSizeDefault;
                case "STRING":
                    return int.MaxValue;
                case "CHAR":
                case "NCHAR":
                    return GetParsedColumnSize(typeName) ?? 255;
                case "BINARY":
                case "VARBINARY":
                case "LONGVARBINARY":
                    return int.MaxValue;
                case "NULL":
                case "VOID":
                    return 1;
                case "INTERVAL":
                    return GetIntervalSize(typeName);
                default:
                    return 0;
            }
        }

        internal static int? GetDecimalDigitsDefault(string typeName)
        {
            string baseName = GetBaseTypeName(typeName);
            return baseName switch
            {
                "DECIMAL" or "NUMERIC" => GetParsedScale(typeName) ?? SqlDecimalTypeParser.DecimalScaleDefault,
                "FLOAT" or "REAL" => 7,
                "DOUBLE" => 15,
                "TIMESTAMP" => 6,
                _ => 0
            };
        }

        internal static int? GetBufferLength(string typeName)
        {
            string baseName = GetBaseTypeName(typeName);
            switch (baseName)
            {
                case "BOOLEAN":
                case "TINYINT":
                    return 1;
                case "SMALLINT":
                    return 2;
                case "INTEGER":
                case "FLOAT":
                case "REAL":
                    return 4;
                case "BIGINT":
                case "DOUBLE":
                case "TIMESTAMP":
                case "DATE":
                    return 8;
                case "DECIMAL":
                case "NUMERIC":
                    int precision = GetParsedPrecision(typeName) ?? SqlDecimalTypeParser.DecimalPrecisionDefault;
                    return ((precision + 8) / 9) * 5 + 1;
                default:
                    return null;
            }
        }

        internal static short? GetNumPrecRadix(string typeName)
        {
            string baseName = GetBaseTypeName(typeName);
            return s_numericBaseTypes.Contains(baseName) ? (short)10 : null;
        }

        internal static int? GetCharOctetLength(string typeName)
        {
            string baseName = GetBaseTypeName(typeName);
            return s_charBaseTypes.Contains(baseName) ? GetColumnSizeDefault(typeName) : null;
        }

        internal static short? GetSqlDatetimeSub(string typeName)
        {
            string baseName = GetBaseTypeName(typeName);
            return baseName switch
            {
                "DATE" => 1,
                "TIMESTAMP" => 3,
                _ => null
            };
        }

        internal static void PopulateTableInfoFromTypeName(
            TableInfo tableInfo,
            string columnName,
            string typeName,
            int ordinalPosition,
            bool isNullable = true,
            string? comment = null,
            string? columnDefault = null)
        {
            tableInfo.ColumnName.Add(columnName);
            tableInfo.TypeName.Add(typeName);
            tableInfo.ColType.Add(GetDataTypeCode(typeName));
            tableInfo.BaseTypeName.Add(GetBaseTypeName(typeName));
            tableInfo.Precision.Add(GetColumnSizeDefault(typeName));
            int? scale = GetDecimalDigitsDefault(typeName);
            tableInfo.Scale.Add(scale.HasValue ? (short)scale.Value : null);
            tableInfo.OrdinalPosition.Add(ordinalPosition);
            tableInfo.Nullable.Add(isNullable ? (short)1 : (short)0);
            tableInfo.IsNullable.Add(isNullable ? "YES" : "NO");
            tableInfo.IsAutoIncrement.Add(false);
            tableInfo.ColumnDefault.Add(columnDefault ?? "");
        }

        private static int? GetParsedPrecision(string typeName)
        {
            if (SqlTypeNameParser<SqlDecimalParserResult>.TryParse(typeName, out SqlTypeNameParserResult? result)
                && result is SqlDecimalParserResult decimalResult)
                return decimalResult.Precision;
            return null;
        }

        private static int? GetParsedScale(string typeName)
        {
            if (SqlTypeNameParser<SqlDecimalParserResult>.TryParse(typeName, out SqlTypeNameParserResult? result)
                && result is SqlDecimalParserResult decimalResult)
                return decimalResult.Scale;
            return null;
        }

        private static int? GetParsedColumnSize(string typeName)
        {
            if (SqlTypeNameParser<SqlCharVarcharParserResult>.TryParse(typeName, out SqlTypeNameParserResult? result)
                && result is SqlCharVarcharParserResult charResult)
                return charResult.ColumnSize;
            return null;
        }

        private static int GetIntervalSize(string typeName)
        {
            string upper = typeName.Trim().ToUpperInvariant();
            if (upper.Contains("YEAR") || upper.Contains("MONTH"))
                return 4;
            if (upper.Contains("DAY") || upper.Contains("HOUR") || upper.Contains("MINUTE") || upper.Contains("SECOND"))
                return 8;
            return 4;
        }
    }
}
