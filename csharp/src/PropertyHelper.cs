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

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Utility class for parsing connection properties.
    /// </summary>
    internal static class PropertyHelper
    {
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
        /// Gets a boolean property value with strict validation.
        /// Returns the default value if the property is not found.
        /// Throws an exception if the property exists but cannot be parsed.
        /// </summary>
        /// <param name="properties">The properties dictionary.</param>
        /// <param name="key">The property key.</param>
        /// <param name="defaultValue">The default value if the property is not found.</param>
        /// <returns>The parsed boolean value or the default value.</returns>
        /// <exception cref="ArgumentException">Thrown if the property exists but cannot be parsed.</exception>
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
                throw new ArgumentException($"Parameter '{key}' value '{value}' could not be parsed. Valid values are 'true', 'false'.");
            }
            return defaultValue;
        }

        /// <summary>
        /// Gets an integer property value with strict validation.
        /// Returns the default value if the property is not found.
        /// Throws an exception if the property exists but cannot be parsed.
        /// </summary>
        /// <param name="properties">The properties dictionary.</param>
        /// <param name="key">The property key.</param>
        /// <param name="defaultValue">The default value if the property is not found.</param>
        /// <returns>The parsed integer value or the default value.</returns>
        /// <exception cref="ArgumentException">Thrown if the property exists but cannot be parsed.</exception>
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
                throw new ArgumentException($"Parameter '{key}' value '{value}' could not be parsed. Valid values are integers.");
            }
            return defaultValue;
        }

        /// <summary>
        /// Gets a positive integer property value with strict validation.
        /// Returns the default value if the property is not found.
        /// Throws an exception if the property exists but cannot be parsed or is not positive.
        /// </summary>
        /// <param name="properties">The properties dictionary.</param>
        /// <param name="key">The property key.</param>
        /// <param name="defaultValue">The default value if the property is not found.</param>
        /// <returns>The parsed integer value or the default value.</returns>
        /// <exception cref="ArgumentException">Thrown if the property exists but cannot be parsed or is not positive.</exception>
        public static int GetPositiveIntPropertyWithValidation(IReadOnlyDictionary<string, string> properties, string key, int defaultValue)
        {
            if (properties == null) throw new ArgumentNullException(nameof(properties));
            if (key == null) throw new ArgumentNullException(nameof(key));

            if (properties.TryGetValue(key, out string? value))
            {
                if (!int.TryParse(value, out int result))
                {
                    throw new ArgumentException($"Parameter '{key}' value '{value}' could not be parsed. Valid values are positive integers.");
                }
                if (result <= 0)
                {
                    throw new ArgumentOutOfRangeException(key, result, $"Parameter '{key}' value must be a positive integer.");
                }
                return result;
            }
            return defaultValue;
        }

        /// <summary>
        /// Gets a long property value with strict validation.
        /// Returns the default value if the property is not found.
        /// Throws an exception if the property exists but cannot be parsed.
        /// </summary>
        /// <param name="properties">The properties dictionary.</param>
        /// <param name="key">The property key.</param>
        /// <param name="defaultValue">The default value if the property is not found.</param>
        /// <returns>The parsed long value or the default value.</returns>
        /// <exception cref="ArgumentException">Thrown if the property exists but cannot be parsed.</exception>
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
                throw new ArgumentException($"Parameter '{key}' value '{value}' could not be parsed. Valid values are integers.");
            }
            return defaultValue;
        }

        /// <summary>
        /// Gets a positive long property value with strict validation.
        /// Returns the default value if the property is not found.
        /// Throws an exception if the property exists but cannot be parsed or is not positive.
        /// </summary>
        /// <param name="properties">The properties dictionary.</param>
        /// <param name="key">The property key.</param>
        /// <param name="defaultValue">The default value if the property is not found.</param>
        /// <returns>The parsed long value or the default value.</returns>
        /// <exception cref="ArgumentException">Thrown if the property exists but cannot be parsed or is not positive.</exception>
        public static long GetPositiveLongPropertyWithValidation(IReadOnlyDictionary<string, string> properties, string key, long defaultValue)
        {
            if (properties == null) throw new ArgumentNullException(nameof(properties));
            if (key == null) throw new ArgumentNullException(nameof(key));

            if (properties.TryGetValue(key, out string? value))
            {
                if (!long.TryParse(value, out long result))
                {
                    throw new ArgumentException($"Parameter '{key}' value '{value}' could not be parsed. Valid values are positive integers.");
                }
                if (result <= 0)
                {
                    throw new ArgumentOutOfRangeException(key, result, $"Parameter '{key}' value must be a positive integer.");
                }
                return result;
            }
            return defaultValue;
        }
    }
}
