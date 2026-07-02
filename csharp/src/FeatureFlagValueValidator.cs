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
using System.Reflection;
using AdbcDrivers.HiveServer2;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// How a strictly-typed connection parameter is parsed on the connect path. Mirrors the
    /// corresponding <c>PropertyHelper.Get*PropertyWithValidation</c> getter.
    /// </summary>
    internal enum FeatureFlagValueKind
    {
        Boolean,
        Int,
        PositiveInt,
        Long,
        PositiveLong,
    }

    /// <summary>
    /// Declares, at the parameter's definition site, how its value is parsed on the connect path.
    /// A parameter carrying this attribute is auto-registered with
    /// <see cref="FeatureFlagValueValidator"/>, so adding a new strictly-typed parameter also
    /// registers its server-flag validation — no separate list to keep in sync.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field, AllowMultiple = false, Inherited = false)]
    internal sealed class FeatureFlagTypeAttribute : Attribute
    {
        public FeatureFlagTypeAttribute(FeatureFlagValueKind kind)
        {
            Kind = kind;
        }

        public FeatureFlagValueKind Kind { get; }
    }

    /// <summary>
    /// Validates server-sourced feature-flag values before they are applied as connection
    /// properties.
    /// <para>
    /// Feature flags fetched from the connector-service are merged into the connection properties,
    /// where strictly-typed parameters are later read with the throwing
    /// <c>PropertyHelper.Get*PropertyWithValidation</c> helpers. A single malformed value pushed by
    /// the service (e.g. the literal <c>"null"</c>) would therefore throw and break connection setup
    /// for <em>every</em> client of that workspace. To keep server config fail-safe, an invalid
    /// value for a strictly-typed parameter is dropped here (so the driver falls back to its default)
    /// instead of reaching the connection. User-supplied local properties are unaffected and continue
    /// to fail fast.
    /// </para>
    /// <para>
    /// The typed-parameter registry is built by reflecting over the <see cref="FeatureFlagTypeAttribute"/>
    /// declared on each <see cref="DatabricksParameters"/> constant, so it stays in sync with the
    /// parameter definitions automatically. Base-class parameters that live in a separate assembly
    /// and cannot carry the attribute are registered explicitly. Keys not registered pass through
    /// unchanged (untyped/string parameters the connection never strictly parses).
    /// </para>
    /// </summary>
    internal static class FeatureFlagValueValidator
    {
        private static readonly IReadOnlyDictionary<string, FeatureFlagValueKind> s_typedParameters = BuildRegistry();

        private static IReadOnlyDictionary<string, FeatureFlagValueKind> BuildRegistry()
        {
            var map = new Dictionary<string, FeatureFlagValueKind>();

            // Auto-register every DatabricksParameters constant annotated with [FeatureFlagType].
            foreach (var field in typeof(DatabricksParameters).GetFields(BindingFlags.Public | BindingFlags.Static))
            {
                if (!field.IsLiteral || field.FieldType != typeof(string))
                {
                    continue;
                }

                var attr = field.GetCustomAttribute<FeatureFlagTypeAttribute>();
                if (attr == null)
                {
                    continue;
                }

                if (field.GetRawConstantValue() is string key && !string.IsNullOrEmpty(key))
                {
                    map[key] = attr.Kind;
                }
            }

            // Base-class (Apache) parameters live in a separate assembly and cannot carry the
            // attribute; register them explicitly. These mirror the Get*PropertyWithValidation
            // reads on the StatementExecution connect path.
            map[ApacheParameters.QueryTimeoutSeconds] = FeatureFlagValueKind.Int;
            map[ApacheParameters.PollTimeMilliseconds] = FeatureFlagValueKind.PositiveInt;

            return map;
        }

        /// <summary>
        /// Returns true if <paramref name="value"/> is acceptable for the given feature-flag
        /// <paramref name="key"/>. Unknown keys always pass through. For a strictly-typed parameter,
        /// returns true only when the value parses exactly as the connection's getter requires.
        /// </summary>
        public static bool IsAcceptable(string key, string? value)
        {
            if (!s_typedParameters.TryGetValue(key, out FeatureFlagValueKind kind))
            {
                // Not a strictly-typed parameter; the connection never throws parsing it.
                return true;
            }

            switch (kind)
            {
                case FeatureFlagValueKind.Boolean:
                    return bool.TryParse(value, out _);
                case FeatureFlagValueKind.Int:
                    return int.TryParse(value, out _);
                case FeatureFlagValueKind.PositiveInt:
                    return int.TryParse(value, out int i) && i > 0;
                case FeatureFlagValueKind.Long:
                    return long.TryParse(value, out _);
                case FeatureFlagValueKind.PositiveLong:
                    return long.TryParse(value, out long l) && l > 0;
                default:
                    return true;
            }
        }
    }
}
