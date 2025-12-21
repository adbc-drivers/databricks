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

using System.Collections.Generic;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using AdbcDrivers.Databricks;
using Xunit;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// Unit tests for DatabricksConnection class methods.
    /// </summary>
    public class DatabricksConnectionUnitTests
    {
        /// <summary>
        /// Creates a minimal DatabricksConnection for testing internal methods.
        /// </summary>
        private DatabricksConnection CreateMinimalConnection()
        {
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "test.databricks.com",
                [SparkParameters.Token] = "test-token"
            };
            return new DatabricksConnection(properties);
        }

        /// <summary>
        /// Tests that valid property names with standard Spark patterns are accepted.
        /// </summary>
        [Theory]
        [InlineData("spark.sql.adaptive.enabled")]
        [InlineData("spark.executor.instances")]
        [InlineData("spark.databricks.delta.optimizeWrite.enabled")]
        [InlineData("my_custom_property")]
        [InlineData("property123")]
        [InlineData("UPPERCASE_PROPERTY")]
        [InlineData("mixedCase.property_123")]
        [InlineData("a")]
        [InlineData("_underscore")]
        [InlineData("spark.sql.shuffle.partitions")]
        public void IsValidPropertyName_ValidNames_ReturnsTrue(string propertyName)
        {
            // Arrange
            using var connection = CreateMinimalConnection();

            // Act
            var result = connection.IsValidPropertyName(propertyName);

            // Assert
            Assert.True(result, $"Property name '{propertyName}' should be valid");
        }

        /// <summary>
        /// Tests that property names with invalid characters are rejected.
        /// </summary>
        [Theory]
        [InlineData("property-with-hyphen")]
        [InlineData("property with space")]
        [InlineData("property;with;semicolon")]
        [InlineData("property'with'quote")]
        [InlineData("property\"with\"doublequote")]
        [InlineData("property=with=equals")]
        [InlineData("property(with)parens")]
        [InlineData("property[with]brackets")]
        [InlineData("property{with}braces")]
        [InlineData("property/with/slash")]
        [InlineData("property\\with\\backslash")]
        [InlineData("property@with@at")]
        [InlineData("property#with#hash")]
        [InlineData("property$with$dollar")]
        [InlineData("property%with%percent")]
        [InlineData("property&with&ampersand")]
        [InlineData("property*with*asterisk")]
        [InlineData("property+with+plus")]
        [InlineData("property!with!exclamation")]
        [InlineData("property?with?question")]
        public void IsValidPropertyName_InvalidCharacters_ReturnsFalse(string propertyName)
        {
            // Arrange
            using var connection = CreateMinimalConnection();

            // Act
            var result = connection.IsValidPropertyName(propertyName);

            // Assert
            Assert.False(result, $"Property name '{propertyName}' should be invalid");
        }

        /// <summary>
        /// Tests that empty or whitespace property names are rejected.
        /// </summary>
        [Theory]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData("  ")]
        [InlineData("\t")]
        [InlineData("\n")]
        public void IsValidPropertyName_EmptyOrWhitespace_ReturnsFalse(string propertyName)
        {
            // Arrange
            using var connection = CreateMinimalConnection();

            // Act
            var result = connection.IsValidPropertyName(propertyName);

            // Assert
            Assert.False(result, $"Property name '{propertyName}' should be invalid");
        }

        /// <summary>
        /// Tests that property names starting with dots or ending with dots are handled correctly.
        /// </summary>
        [Theory]
        [InlineData(".property")] // Starts with dot - currently allowed
        [InlineData("property.")] // Ends with dot - currently allowed
        [InlineData(".")] // Just a dot - currently allowed
        [InlineData("..")] // Multiple dots - currently allowed
        public void IsValidPropertyName_DotEdgeCases_BehaviorDocumented(string propertyName)
        {
            // Arrange
            using var connection = CreateMinimalConnection();

            // Act
            var result = connection.IsValidPropertyName(propertyName);

            // Assert
            // Current regex pattern ^[a-zA-Z0-9_.]+$ allows dots anywhere
            // This test documents the current behavior
            Assert.True(result, $"Property name '{propertyName}' is currently accepted by the regex");
        }

        /// <summary>
        /// Tests property names that start with numbers.
        /// </summary>
        [Theory]
        [InlineData("123property")] // Starts with number - currently allowed
        [InlineData("1.property")] // Starts with number followed by dot - currently allowed
        public void IsValidPropertyName_StartsWithNumber_CurrentlyAllowed(string propertyName)
        {
            // Arrange
            using var connection = CreateMinimalConnection();

            // Act
            var result = connection.IsValidPropertyName(propertyName);

            // Assert
            // Current regex pattern ^[a-zA-Z0-9_.]+$ allows starting with numbers
            // This test documents the current behavior
            Assert.True(result, $"Property name '{propertyName}' is currently accepted by the regex");
        }
    }
}
