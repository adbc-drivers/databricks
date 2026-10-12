/*
 * Copyright (c) 2026 ADBC Drivers Contributors
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

namespace AdbcDrivers.Databricks.StatementExecution.Metadata
{
    internal readonly struct CatalogFilter
    {
        private readonly Regex? _pattern;

        private CatalogFilter(string? value, bool isPattern)
        {
            Value = value;
            IsPattern = isPattern;
            _pattern = isPattern && value != null ? ToRegex(value) : null;
        }

        internal string? Value { get; }
        internal bool IsPattern { get; }

        internal static CatalogFilter Exact(string? catalog) => new(catalog, false);
        internal static CatalogFilter Pattern(string? pattern) => new(pattern, true);

        internal bool Matches(string? actual)
            => Value == null || (actual != null && (_pattern?.IsMatch(actual)
                ?? string.Equals(Value, actual, StringComparison.OrdinalIgnoreCase)));

        // The catalog a LIKE pattern names, or null if it contains a wildcard.
        internal static string? Literal(string? pattern)
        {
            if (pattern == null) return null;
            var literal = new StringBuilder();
            bool escaped = false;
            foreach (char character in pattern)
            {
                if (!escaped)
                {
                    if (character is '%' or '_') return null;
                    if (character == '\\')
                    {
                        escaped = true;
                        continue;
                    }
                }
                literal.Append(character);
                escaped = false;
            }
            if (escaped) literal.Append('\\');
            return literal.ToString();
        }

        private static Regex ToRegex(string pattern)
        {
            var regex = new StringBuilder("^");
            bool escaped = false;
            foreach (char character in pattern)
            {
                if (!escaped && character == '\\')
                {
                    escaped = true;
                    continue;
                }
                regex.Append(!escaped && character == '%' ? ".*"
                    : !escaped && character == '_' ? "."
                    : Regex.Escape(character.ToString()));
                escaped = false;
            }
            if (escaped) regex.Append(Regex.Escape("\\"));
            regex.Append('$');
            return new Regex(regex.ToString(), RegexOptions.CultureInvariant | RegexOptions.IgnoreCase,
                TimeSpan.FromSeconds(1));
        }
    }
}
