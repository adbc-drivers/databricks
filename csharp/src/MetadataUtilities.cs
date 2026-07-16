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

namespace AdbcDrivers.Databricks
{
    internal static class MetadataUtilities
    {
        internal static string? NormalizeSparkCatalog(string? catalogName)
        {
            if (string.IsNullOrEmpty(catalogName))
                return catalogName;

            if (string.Equals(catalogName, "SPARK", StringComparison.OrdinalIgnoreCase))
                return null;

            return catalogName;
        }

        /// <summary>
        /// Issue #524: an empty-string identifier argument (catalog="", schema="",
        /// table="", foreign_*="") is a degenerate filter that can never match a real
        /// object. The Thrift metadata RPCs reject it with a HiveServer2Exception,
        /// while the SEA path returns an empty result set. To keep the two protocols
        /// consistent (empty-string → empty result set, never an exception), callers
        /// short-circuit to an empty result when any relevant identifier is empty.
        ///
        /// Only a non-null, zero-length string counts; null means "no filter"
        /// (e.g. "all catalogs") and is left untouched.
        /// </summary>
        internal static bool HasEmptyStringIdentifier(params string?[] identifiers)
        {
            foreach (string? identifier in identifiers)
            {
                if (identifier != null && identifier.Length == 0)
                    return true;
            }
            return false;
        }

        internal static bool IsInvalidPKFKCatalog(string? catalogName)
        {
            return string.IsNullOrEmpty(catalogName) ||
                   string.Equals(catalogName, "SPARK", StringComparison.OrdinalIgnoreCase) ||
                   string.Equals(catalogName, "hive_metastore", StringComparison.OrdinalIgnoreCase);
        }

        internal static bool ShouldReturnEmptyPKFKResult(string? catalogName, string? foreignCatalogName, bool enablePKFK)
        {
            if (!enablePKFK)
                return true;

            return IsInvalidPKFKCatalog(catalogName) && IsInvalidPKFKCatalog(foreignCatalogName);
        }

        internal static string? BuildQualifiedTableName(string? catalogName, string? schemaName, string? tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                return tableName;

            var parts = new System.Collections.Generic.List<string>();

            if (!string.IsNullOrEmpty(schemaName))
            {
                if (!string.IsNullOrEmpty(catalogName) && !catalogName!.Equals("SPARK", StringComparison.OrdinalIgnoreCase))
                {
                    parts.Add($"`{catalogName.Replace("`", "``")}`");
                }
                parts.Add($"`{schemaName!.Replace("`", "``")}`");
            }

            parts.Add($"`{tableName!.Replace("`", "``")}`");

            return string.Join(".", parts);
        }
    }
}
