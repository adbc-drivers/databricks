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

using System.Text;
using System.Text.RegularExpressions;

namespace AdbcDrivers.Databricks.StatementExecution.MetadataCommands
{
    internal abstract class MetadataCommandBase
    {
        protected const string InAllCatalogs = " IN ALL CATALOGS";
        protected const string LikeFormat = " LIKE '{0}'";
        protected const string SchemaLikeFormat = " SCHEMA LIKE '{0}'";
        protected const string TableLikeFormat = " TABLE LIKE '{0}'";
        protected const string InCatalogFormat = " IN CATALOG {0}";
        protected const string InSchemaFormat = " IN SCHEMA {0}";
        protected const string InTableFormat = " IN TABLE {0}";

        public abstract string Build();

        protected static string QuoteIdentifier(string identifier)
        {
            return $"`{identifier.Replace("`", "``")}`";
        }

        protected static string ConvertPattern(string? pattern)
        {
            if (pattern == null)
                return "*";

            var result = new StringBuilder(pattern!.Length);
            bool escapeNext = false;

            for (int i = 0; i < pattern.Length; i++)
            {
                char c = pattern[i];

                if (c == '\\')
                {
                    if (i + 1 < pattern.Length && pattern[i + 1] == '\\')
                    {
                        result.Append("\\\\");
                        i++;
                    }
                    else
                    {
                        escapeNext = !escapeNext;
                        if (!escapeNext)
                            result.Append('\\');
                    }
                }
                else if (escapeNext)
                {
                    result.Append(c);
                    escapeNext = false;
                }
                else if (c == '%')
                {
                    result.Append('*');
                }
                else if (c == '_')
                {
                    result.Append('.');
                }
                else if (c == '\'')
                {
                    result.Append("''");
                }
                else
                {
                    result.Append(c);
                }
            }

            if (escapeNext)
            {
                result.Append('\\');
            }

            return result.ToString();
        }

        protected static void AppendCatalogScope(StringBuilder sql, string? catalog)
        {
            if (catalog == null)
                sql.Append(InAllCatalogs);
            else
                sql.Append(string.Format(InCatalogFormat, QuoteIdentifier(catalog)));
        }

        /// <summary>
        /// Returns true when <paramref name="pattern"/> contains a SQL LIKE wildcard
        /// (% or _) that is NOT escaped by a preceding backslash. JDBC metadata APIs
        /// treat catalog/schema/table arguments as LIKE patterns, but SEA SHOW commands
        /// take literal identifiers, so callers must expand wildcards client-side.
        /// </summary>
        internal static bool ContainsUnescapedWildcard(string? pattern)
        {
            if (string.IsNullOrEmpty(pattern))
                return false;

            bool escapeNext = false;
            for (int i = 0; i < pattern!.Length; i++)
            {
                char c = pattern[i];
                if (c == '\\')
                {
                    // Two backslashes in a row are an escaped backslash literal, not an escape.
                    if (i + 1 < pattern.Length && pattern[i + 1] == '\\')
                    {
                        i++;
                        continue;
                    }
                    escapeNext = !escapeNext;
                }
                else if (escapeNext)
                {
                    escapeNext = false;
                }
                else if (c == '%' || c == '_')
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Returns true when <paramref name="pattern"/> is a pure "match anything"
        /// pattern: a single unescaped % (or *). These can be optimised to
        /// SHOW SCHEMAS IN ALL CATALOGS without enumerating catalogs.
        /// </summary>
        internal static bool IsMatchAnything(string? pattern)
        {
            return pattern == "%" || pattern == "*";
        }

        /// <summary>
        /// Compiles a JDBC LIKE pattern (with <c>%</c> / <c>_</c> wildcards and
        /// <c>\</c> escapes, where <c>\%</c> / <c>\_</c> / <c>\\</c> are literal)
        /// into a <see cref="Regex"/> for client-side filtering. Anchored at both
        /// ends; case-sensitive. Used when wildcard expansion has to happen on
        /// the driver side (e.g. catalog patterns on SEA — PECO-3035).
        /// </summary>
        internal static Regex JdbcLikeToRegex(string pattern)
        {
            var sb = new StringBuilder("^");
            bool escapeNext = false;
            for (int i = 0; i < pattern.Length; i++)
            {
                char c = pattern[i];
                if (c == '\\')
                {
                    // Two backslashes → literal backslash.
                    if (i + 1 < pattern.Length && pattern[i + 1] == '\\')
                    {
                        sb.Append("\\\\");
                        i++;
                        continue;
                    }
                    escapeNext = !escapeNext;
                    continue;
                }
                if (escapeNext)
                {
                    sb.Append(Regex.Escape(c.ToString()));
                    escapeNext = false;
                    continue;
                }
                switch (c)
                {
                    case '%': sb.Append(".*"); break;
                    case '_': sb.Append("."); break;
                    default: sb.Append(Regex.Escape(c.ToString())); break;
                }
            }
            sb.Append("$");
            return new Regex(sb.ToString());
        }
    }
}
