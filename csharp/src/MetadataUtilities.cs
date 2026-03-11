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
