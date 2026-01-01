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
using System.Text;
using System.Text.RegularExpressions;
using Apache.Arrow;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Utility methods for metadata operations shared between Thrift and Statement Execution API protocols.
    /// Provides catalog name normalization, PK/FK validation, SQL pattern conversion, identifier quoting, and Arrow builder helpers.
    /// </summary>
    internal static class MetadataUtilities
    {
        #region Catalog Name Normalization

        /// <summary>
        /// Normalizes the "SPARK" catalog name to null for compatibility with Databricks legacy behavior.
        ///
        /// Why: In Databricks, the legacy "SPARK" catalog is used as a placeholder to represent the default catalog.
        /// When a client requests metadata for the "SPARK" catalog, the underlying API expects a null catalog name
        /// to trigger default catalog behavior. Passing "SPARK" directly would not return the expected results.
        ///
        /// This logic is required to maintain compatibility with legacy tools and standards that expect "SPARK" to act as a default catalog alias.
        /// </summary>
        /// <param name="catalogName">The catalog name to normalize</param>
        /// <returns>Null if catalog is "SPARK" (case-insensitive), otherwise returns the original catalog name</returns>
        public static string? NormalizeSparkCatalog(string? catalogName)
        {
            if (string.IsNullOrEmpty(catalogName))
                return catalogName;

            if (string.Equals(catalogName, "SPARK", StringComparison.OrdinalIgnoreCase))
                return null;

            return catalogName;
        }

        #endregion

        #region PK/FK Validation

        /// <summary>
        /// Determines whether a catalog supports primary/foreign key metadata queries.
        ///
        /// Why: For certain catalog names (null, empty, "SPARK", "hive_metastore"), Databricks does not support PK/FK metadata,
        /// or these are legacy/synthesized catalogs that should gracefully return empty results for compatibility.
        ///
        /// Returns true if the catalog is invalid for PK/FK operations (should return empty results).
        /// </summary>
        /// <param name="catalogName">The catalog name to validate</param>
        /// <returns>True if catalog is invalid for PK/FK queries (null, empty, SPARK, hive_metastore)</returns>
        public static bool IsInvalidPKFKCatalog(string? catalogName)
        {
            return string.IsNullOrEmpty(catalogName) ||
                   string.Equals(catalogName, "SPARK", StringComparison.OrdinalIgnoreCase) ||
                   string.Equals(catalogName, "hive_metastore", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Determines whether PK/FK metadata queries should return an empty result set based on catalog validity
        /// and the enablePKFK flag.
        ///
        /// This is used by GetPrimaryKeys, GetCrossReference, and GetImportedKeys operations.
        /// </summary>
        /// <param name="catalogName">Primary key table catalog name</param>
        /// <param name="foreignCatalogName">Foreign key table catalog name (optional, for cross-reference)</param>
        /// <param name="enablePKFK">Whether PK/FK feature is enabled globally</param>
        /// <returns>True if should return empty results without hitting the server</returns>
        public static bool ShouldReturnEmptyPKFKResult(string? catalogName, string? foreignCatalogName, bool enablePKFK)
        {
            if (!enablePKFK)
                return true;

            var catalogInvalid = IsInvalidPKFKCatalog(catalogName);
            var foreignCatalogInvalid = IsInvalidPKFKCatalog(foreignCatalogName);

            // Only when both catalog and foreignCatalog are invalid, we return empty results
            return catalogInvalid && foreignCatalogInvalid;
        }

        #endregion

        #region Table Name Building

        /// <summary>
        /// Builds a fully qualified table name with proper identifier quoting.
        /// Format: `catalog`.`schema`.`table` or `schema`.`table` or `table`
        ///
        /// Escapes backticks in identifiers by doubling them (`` -> ````).
        /// Excludes "SPARK" catalog (case-insensitive) from qualified name.
        /// Only includes catalog when schema is also defined.
        /// </summary>
        /// <param name="catalogName">Catalog name (optional)</param>
        /// <param name="schemaName">Schema name (optional)</param>
        /// <param name="tableName">Table name (required)</param>
        /// <returns>Fully qualified quoted table name, or null if tableName is null/empty</returns>
        public static string? BuildQualifiedTableName(string? catalogName, string? schemaName, string? tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                return tableName;

            var parts = new System.Collections.Generic.List<string>();

            if (!string.IsNullOrEmpty(schemaName))
            {
                // Only include CatalogName when SchemaName is defined
                if (!string.IsNullOrEmpty(catalogName) && !catalogName!.Equals("SPARK", StringComparison.OrdinalIgnoreCase))
                {
                    parts.Add($"`{catalogName.Replace("`", "``")}`");
                }
                parts.Add($"`{schemaName!.Replace("`", "``")}`");
            }

            // Escape if TableName contains backtick
            parts.Add($"`{tableName!.Replace("`", "``")}`");

            return string.Join(".", parts);
        }

        #endregion

        #region SQL Pattern Conversion and Identifier Quoting

        /// <summary>
        /// Quotes a Databricks identifier with backticks, escaping any existing backticks.
        /// Used for catalog, schema, table, and column names in SQL commands.
        /// </summary>
        /// <param name="identifier">The identifier to quote</param>
        /// <returns>Quoted identifier with escaped backticks (e.g., `my_table` or `my``table`)</returns>
        /// <example>
        /// QuoteIdentifier("my_table") => "`my_table`"
        /// QuoteIdentifier("my`table") => "`my``table`"
        /// </example>
        public static string QuoteIdentifier(string identifier)
        {
            if (string.IsNullOrEmpty(identifier))
                return identifier;

            return $"`{identifier.Replace("`", "``")}`";
        }

        /// <summary>
        /// Converts ADBC SQL patterns to Databricks glob patterns for use in LIKE clauses.
        ///
        /// Pattern conversions:
        /// - ADBC wildcard % (zero or more) → Databricks wildcard * (zero or more)
        /// - ADBC single char _ (exactly one) → Databricks single char . (exactly one)
        /// - ADBC escape \ (escape next) → handled for literals
        /// - SQL single quote ' → escaped as ''
        ///
        /// This conversion is used for SHOW SCHEMAS/TABLES/COLUMNS commands that support
        /// pattern matching with LIKE clauses. It should NOT be used for SHOW KEYS/FOREIGN KEYS
        /// commands which require exact identifiers.
        /// </summary>
        /// <param name="pattern">ADBC pattern string with % and _ wildcards</param>
        /// <returns>Databricks glob pattern escaped for SQL LIKE clause</returns>
        /// <example>
        /// ConvertAdbcPatternToDatabricksGlob("dev_%") => "dev_*"
        /// ConvertAdbcPatternToDatabricksGlob("test_schema") => "test_schema"
        /// ConvertAdbcPatternToDatabricksGlob("my'schema") => "my''schema"
        /// </example>
        public static string? ConvertAdbcPatternToDatabricksGlob(string? pattern)
        {
            if (string.IsNullOrEmpty(pattern))
                return pattern;

            var result = new StringBuilder(pattern.Length);
            bool escapeNext = false;

            for (int i = 0; i < pattern.Length; i++)
            {
                char c = pattern[i];

                if (c == '\\')
                {
                    // Check if it's an escaped backslash (\\)
                    if (i + 1 < pattern.Length && pattern[i + 1] == '\\')
                    {
                        result.Append("\\\\");  // Preserve both backslashes
                        i++; // Skip the next backslash
                    }
                    else
                    {
                        escapeNext = !escapeNext;  // Toggle escape state for next character
                    }
                }
                else if (escapeNext)
                {
                    // If the current character is escaped, add it directly (literal)
                    result.Append(c);
                    escapeNext = false;
                }
                else
                {
                    // Handle unescaped characters
                    switch (c)
                    {
                        case '%':
                            // ADBC wildcard % → Databricks wildcard *
                            result.Append('*');
                            break;
                        case '_':
                            // ADBC single char _ → Databricks single char .
                            result.Append('.');
                            break;
                        case '\'':
                            // SQL escape: ' → ''
                            result.Append("''");
                            break;
                        default:
                            result.Append(c);
                            break;
                    }
                }
            }

            return result.ToString();
        }

        /// <summary>
        /// Tests if a value matches an ADBC SQL pattern (% and _ wildcards).
        /// Used for client-side pattern matching when server-side filtering is not available.
        /// </summary>
        /// <param name="value">The value to test</param>
        /// <param name="pattern">ADBC pattern (% = any chars, _ = single char)</param>
        /// <returns>True if value matches the pattern (case-insensitive)</returns>
        /// <example>
        /// MatchesAdbcPattern("dev_schema", "dev_%") => true
        /// MatchesAdbcPattern("prod_schema", "dev_%") => false
        /// MatchesAdbcPattern("test", "t_st") => true
        /// </example>
        public static bool MatchesAdbcPattern(string value, string pattern)
        {
            if (string.IsNullOrEmpty(pattern))
                return string.IsNullOrEmpty(value);

            var regexPattern = "^" + Regex.Escape(pattern)
                .Replace("%", ".*")
                .Replace("_", ".") + "$";

            return Regex.IsMatch(value, regexPattern, RegexOptions.IgnoreCase);
        }

        #endregion
    }
}

