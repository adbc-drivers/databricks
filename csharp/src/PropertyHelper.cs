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
using System.Diagnostics;
using System.Net;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Utility class for parsing connection properties.
    /// </summary>
    internal static class PropertyHelper
    {
        /// <summary>
        /// Records that a property value could not be parsed and was ignored in favor of the
        /// default. Emitted as an event on the current <see cref="Activity"/> (typically the
        /// enclosing connection-open span), matching the driver's ActivitySource-based
        /// diagnostics; a no-op when no activity is in scope.
        /// </summary>
        private static void LogInvalidValueIgnored(string key, string? value, object? defaultValue)
        {
            Activity.Current?.AddEvent(new ActivityEvent(
                "property.invalid_value_ignored",
                tags: new ActivityTagsCollection
                {
                    { "property.key", key },
                    { "property.value", value },
                    { "property.default", defaultValue?.ToString() }
                }));
        }

        /// <summary>
        /// Gets a string property value from the properties dictionary.
        /// </summary>
        /// <param name="properties">The properties dictionary.</param>
        /// <param name="key">The property key.</param>
        /// <param name="defaultValue">The default value if the property is not found.</param>
        /// <returns>The property value or the default value.</returns>
        public static string GetStringProperty(IReadOnlyDictionary<string, string> properties, string key, string defaultValue)
        {
            if (properties == null) throw new ArgumentNullException(nameof(properties));
            if (key == null) throw new ArgumentNullException(nameof(key));

            if (properties.TryGetValue(key, out string? value) && !string.IsNullOrEmpty(value))
            {
                return value;
            }
            return defaultValue;
        }

        /// <summary>
        /// Gets a required string property value from the properties dictionary.
        /// </summary>
        /// <param name="properties">The properties dictionary.</param>
        /// <param name="key">The property key.</param>
        /// <param name="errorMessage">The error message if the property is not found.</param>
        /// <returns>The property value.</returns>
        /// <exception cref="ArgumentException">Thrown if the property is not found or empty.</exception>
        public static string GetRequiredStringProperty(IReadOnlyDictionary<string, string> properties, string key, string? errorMessage = null)
        {
            if (properties == null) throw new ArgumentNullException(nameof(properties));
            if (key == null) throw new ArgumentNullException(nameof(key));

            if (properties.TryGetValue(key, out string? value) && !string.IsNullOrEmpty(value))
            {
                return value;
            }
            throw new ArgumentException(errorMessage ?? $"Required property '{key}' is missing or empty.");
        }

        /// <summary>
        /// Gets a boolean property value.
        /// Returns the default value if the property is not found, or if it exists but cannot be
        /// parsed as a boolean. Invalid values are ignored (logged, then defaulted) rather than
        /// throwing, so a malformed value from any source — including server feature flags merged
        /// into the connection properties — never fails connection setup.
        /// </summary>
        /// <param name="properties">The properties dictionary.</param>
        /// <param name="key">The property key.</param>
        /// <param name="defaultValue">The default value if the property is not found or invalid.</param>
        /// <returns>The parsed boolean value, or the default value when absent or invalid.</returns>
        public static bool GetBooleanPropertyWithValidation(IReadOnlyDictionary<string, string> properties, string key, bool defaultValue)
        {
            if (properties == null) throw new ArgumentNullException(nameof(properties));
            if (key == null) throw new ArgumentNullException(nameof(key));

            if (properties.TryGetValue(key, out string? value))
            {
                if (bool.TryParse(value, out bool result))
                {
                    return result;
                }
                LogInvalidValueIgnored(key, value, defaultValue);
            }
            return defaultValue;
        }

        /// <summary>
        /// Gets an integer property value.
        /// Returns the default value if the property is not found, or if it exists but cannot be
        /// parsed as an integer. Invalid values are ignored (logged, then defaulted) rather than
        /// throwing.
        /// </summary>
        /// <param name="properties">The properties dictionary.</param>
        /// <param name="key">The property key.</param>
        /// <param name="defaultValue">The default value if the property is not found or invalid.</param>
        /// <returns>The parsed integer value, or the default value when absent or invalid.</returns>
        public static int GetIntPropertyWithValidation(IReadOnlyDictionary<string, string> properties, string key, int defaultValue)
        {
            if (properties == null) throw new ArgumentNullException(nameof(properties));
            if (key == null) throw new ArgumentNullException(nameof(key));

            if (properties.TryGetValue(key, out string? value))
            {
                if (int.TryParse(value, out int result))
                {
                    return result;
                }
                LogInvalidValueIgnored(key, value, defaultValue);
            }
            return defaultValue;
        }

        /// <summary>
        /// Gets a positive integer property value.
        /// Returns the default value if the property is not found, or if it exists but cannot be
        /// parsed as an integer or is not positive. Invalid values are ignored (logged, then
        /// defaulted) rather than throwing.
        /// </summary>
        /// <param name="properties">The properties dictionary.</param>
        /// <param name="key">The property key.</param>
        /// <param name="defaultValue">The default value if the property is not found or invalid.</param>
        /// <returns>The parsed positive integer value, or the default value when absent or invalid.</returns>
        public static int GetPositiveIntPropertyWithValidation(IReadOnlyDictionary<string, string> properties, string key, int defaultValue)
        {
            if (properties == null) throw new ArgumentNullException(nameof(properties));
            if (key == null) throw new ArgumentNullException(nameof(key));

            if (properties.TryGetValue(key, out string? value))
            {
                if (int.TryParse(value, out int result) && result > 0)
                {
                    return result;
                }
                LogInvalidValueIgnored(key, value, defaultValue);
            }
            return defaultValue;
        }

        /// <summary>
        /// Gets a long property value.
        /// Returns the default value if the property is not found, or if it exists but cannot be
        /// parsed as a long. Invalid values are ignored (logged, then defaulted) rather than
        /// throwing.
        /// </summary>
        /// <param name="properties">The properties dictionary.</param>
        /// <param name="key">The property key.</param>
        /// <param name="defaultValue">The default value if the property is not found or invalid.</param>
        /// <returns>The parsed long value, or the default value when absent or invalid.</returns>
        public static long GetLongPropertyWithValidation(IReadOnlyDictionary<string, string> properties, string key, long defaultValue)
        {
            if (properties == null) throw new ArgumentNullException(nameof(properties));
            if (key == null) throw new ArgumentNullException(nameof(key));

            if (properties.TryGetValue(key, out string? value))
            {
                if (long.TryParse(value, out long result))
                {
                    return result;
                }
                LogInvalidValueIgnored(key, value, defaultValue);
            }
            return defaultValue;
        }

        /// <summary>
        /// Gets a positive long property value.
        /// Returns the default value if the property is not found, or if it exists but cannot be
        /// parsed as a long or is not positive. Invalid values are ignored (logged, then defaulted)
        /// rather than throwing.
        /// </summary>
        /// <param name="properties">The properties dictionary.</param>
        /// <param name="key">The property key.</param>
        /// <param name="defaultValue">The default value if the property is not found or invalid.</param>
        /// <returns>The parsed positive long value, or the default value when absent or invalid.</returns>
        public static long GetPositiveLongPropertyWithValidation(IReadOnlyDictionary<string, string> properties, string key, long defaultValue)
        {
            if (properties == null) throw new ArgumentNullException(nameof(properties));
            if (key == null) throw new ArgumentNullException(nameof(key));

            if (properties.TryGetValue(key, out string? value))
            {
                if (long.TryParse(value, out long result) && result > 0)
                {
                    return result;
                }
                LogInvalidValueIgnored(key, value, defaultValue);
            }
            return defaultValue;
        }

        /// <summary>
        /// Extracts the value of the 'o' parameter from a URL query string.
        /// </summary>
        /// <param name="queryString">Query string without leading '?'.</param>
        /// <returns>The org ID value, or null if not present or empty.</returns>
        public static string? ParseOrgIdFromQueryString(string queryString)
        {
            foreach (var part in queryString.Split('&'))
            {
                var kv = part.Split('=');
                if (kv.Length == 2 && kv[0] == "o" && !string.IsNullOrEmpty(kv[1]))
                    return Uri.UnescapeDataString(kv[1]);
            }
            return null;
        }

        /// <summary>
        /// Extracts the org ID from connection properties by inspecting the http path and URI query strings.
        /// Checks <see cref="SparkParameters.Path"/> first, then falls back to <see cref="AdbcOptions.Uri"/>.
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <returns>The org ID value, or null if not present.</returns>
        public static string? ParseOrgIdFromProperties(IReadOnlyDictionary<string, string>? properties)
        {
            if (properties == null) return null;

            if (properties.TryGetValue(SparkParameters.Path, out string? path) && !string.IsNullOrEmpty(path))
            {
                int q = path.IndexOf('?');
                if (q >= 0)
                {
                    string? orgId = ParseOrgIdFromQueryString(path.Substring(q + 1));
                    if (orgId != null) return orgId;
                }
            }

            if (properties.TryGetValue(AdbcOptions.Uri, out string? uri) && !string.IsNullOrEmpty(uri)
                && Uri.TryCreate(uri, UriKind.Absolute, out Uri? parsedUri)
                && !string.IsNullOrEmpty(parsedUri.Query))
            {
                return ParseOrgIdFromQueryString(parsedUri.Query.TrimStart('?'));
            }

            return null;
        }
    }
}
