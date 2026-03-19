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
using Xunit;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// Unit tests for the PropertyHelper class.
    /// </summary>
    public class PropertyHelperTests
    {
        /// <summary>
        /// Test that GetStringProperty returns the correct value when the key exists.
        /// </summary>
        [Fact]
        public void GetStringProperty_ReturnsValueWhenKeyExists()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "key1", "value1" },
                { "key2", "value2" }
            };

            // Act
            var result = PropertyHelper.GetStringProperty(properties, "key1", "default");

            // Assert
            Assert.Equal("value1", result);
        }

        /// <summary>
        /// Test that GetStringProperty returns the default value when the key is missing.
        /// </summary>
        [Fact]
        public void GetStringProperty_ReturnsDefaultWhenKeyMissing()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "key1", "value1" },
                { "key2", "value2" }
            };

            // Act
            var result = PropertyHelper.GetStringProperty(properties, "missingKey", "defaultValue");

            // Assert
            Assert.Equal("defaultValue", result);
        }

        /// <summary>
        /// Test that GetStringProperty returns the default value when the value is an empty string.
        /// </summary>
        [Fact]
        public void GetStringProperty_ReturnsDefaultWhenValueIsEmptyString()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "key1", "" },
                { "key2", "value2" }
            };

            // Act
            var result = PropertyHelper.GetStringProperty(properties, "key1", "defaultValue");

            // Assert
            Assert.Equal("defaultValue", result);
        }

        /// <summary>
        /// Test that GetStringProperty throws ArgumentNullException when properties is null.
        /// </summary>
        [Fact]
        public void GetStringProperty_ThrowsArgumentNullExceptionWhenPropertiesIsNull()
        {
            // Arrange
            IReadOnlyDictionary<string, string>? properties = null;

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                PropertyHelper.GetStringProperty(properties, "key1", "defaultValue"));
        }

        /// <summary>
        /// Test that GetStringProperty throws ArgumentNullException when key is null.
        /// </summary>
        [Fact]
        public void GetStringProperty_ThrowsArgumentNullExceptionWhenKeyIsNull()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "key1", "value1" }
            };

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                PropertyHelper.GetStringProperty(properties, null!, "defaultValue"));
        }

        /// <summary>
        /// Test that GetRequiredStringProperty returns the correct value when the key exists.
        /// </summary>
        [Fact]
        public void GetRequiredStringProperty_ReturnsValueWhenKeyExists()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "key1", "value1" },
                { "key2", "value2" }
            };

            // Act
            var result = PropertyHelper.GetRequiredStringProperty(properties, "key1");

            // Assert
            Assert.Equal("value1", result);
        }

        /// <summary>
        /// Test that GetRequiredStringProperty throws ArgumentException when the key is missing.
        /// </summary>
        [Fact]
        public void GetRequiredStringProperty_ThrowsArgumentExceptionWhenKeyMissing()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "key1", "value1" },
                { "key2", "value2" }
            };

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                PropertyHelper.GetRequiredStringProperty(properties, "missingKey"));
        }

        /// <summary>
        /// Test that GetRequiredStringProperty throws ArgumentException when the value is empty.
        /// </summary>
        [Fact]
        public void GetRequiredStringProperty_ThrowsArgumentExceptionWhenValueIsEmpty()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "key1", "" },
                { "key2", "value2" }
            };

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                PropertyHelper.GetRequiredStringProperty(properties, "key1"));
        }

        /// <summary>
        /// Test that GetRequiredStringProperty uses custom error message when provided.
        /// </summary>
        [Fact]
        public void GetRequiredStringProperty_UsesCustomErrorMessageWhenProvided()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "key1", "value1" }
            };
            string customMessage = "Custom error: the property is required!";

            // Act & Assert - Test with missing key
            var exceptionMissing = Assert.Throws<ArgumentException>(() =>
                PropertyHelper.GetRequiredStringProperty(properties, "missingKey", customMessage));
            Assert.Equal(customMessage, exceptionMissing.Message);

            // Act & Assert - Test with empty value
            properties["emptyKey"] = "";
            var exceptionEmpty = Assert.Throws<ArgumentException>(() =>
                PropertyHelper.GetRequiredStringProperty(properties, "emptyKey", customMessage));
            Assert.Equal(customMessage, exceptionEmpty.Message);
        }

        /// <summary>
        /// Test that GetRequiredStringProperty uses default error message when not provided.
        /// </summary>
        [Fact]
        public void GetRequiredStringProperty_UsesDefaultErrorMessageWhenNotProvided()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "key1", "value1" }
            };

            // Act & Assert - Test with missing key
            var exceptionMissing = Assert.Throws<ArgumentException>(() =>
                PropertyHelper.GetRequiredStringProperty(properties, "missingKey"));
            Assert.Equal("Required property 'missingKey' is missing or empty.", exceptionMissing.Message);

            // Act & Assert - Test with empty value
            properties["emptyKey"] = "";
            var exceptionEmpty = Assert.Throws<ArgumentException>(() =>
                PropertyHelper.GetRequiredStringProperty(properties, "emptyKey"));
            Assert.Equal("Required property 'emptyKey' is missing or empty.", exceptionEmpty.Message);
        }

        /// <summary>
        /// Test that GetBooleanPropertyWithValidation returns true for "true".
        /// </summary>
        [Fact]
        public void GetBooleanPropertyWithValidation_ReturnsTrueForTrue()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "boolKey", "true" }
            };

            // Act
            var result = PropertyHelper.GetBooleanPropertyWithValidation(properties, "boolKey", false);

            // Assert
            Assert.True(result);
        }

        /// <summary>
        /// Test that GetBooleanPropertyWithValidation returns false for "false".
        /// </summary>
        [Fact]
        public void GetBooleanPropertyWithValidation_ReturnsFalseForFalse()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "boolKey", "false" }
            };

            // Act
            var result = PropertyHelper.GetBooleanPropertyWithValidation(properties, "boolKey", true);

            // Assert
            Assert.False(result);
        }

        /// <summary>
        /// Test that GetBooleanPropertyWithValidation returns the default value when the key is missing.
        /// </summary>
        [Fact]
        public void GetBooleanPropertyWithValidation_ReturnsDefaultWhenKeyMissing()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "otherKey", "value" }
            };

            // Act - Test with default value true
            var resultTrue = PropertyHelper.GetBooleanPropertyWithValidation(properties, "missingKey", true);

            // Act - Test with default value false
            var resultFalse = PropertyHelper.GetBooleanPropertyWithValidation(properties, "missingKey", false);

            // Assert
            Assert.True(resultTrue);
            Assert.False(resultFalse);
        }

        /// <summary>
        /// Test that GetBooleanPropertyWithValidation throws ArgumentException for invalid values.
        /// </summary>
        [Theory]
        [InlineData("yes")]
        [InlineData("1")]
        [InlineData("abc")]
        [InlineData("0")]
        [InlineData("")]
        public void GetBooleanPropertyWithValidation_ThrowsArgumentExceptionForInvalidValue(string invalidValue)
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "boolKey", invalidValue }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() =>
                PropertyHelper.GetBooleanPropertyWithValidation(properties, "boolKey", false));

            // Verify the error message contains the key and value
            Assert.Contains("boolKey", exception.Message);
            Assert.Contains(invalidValue, exception.Message);
        }

        /// <summary>
        /// Test that GetIntPropertyWithValidation returns the correctly parsed integer when given a valid integer string.
        /// </summary>
        [Fact]
        public void GetIntPropertyWithValidation_ReturnsParsedIntForValidString()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "intKey", "42" }
            };

            // Act
            var result = PropertyHelper.GetIntPropertyWithValidation(properties, "intKey", 0);

            // Assert
            Assert.Equal(42, result);
        }

        /// <summary>
        /// Test that GetIntPropertyWithValidation returns the default value when the key is missing.
        /// </summary>
        [Fact]
        public void GetIntPropertyWithValidation_ReturnsDefaultWhenKeyMissing()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "otherKey", "123" }
            };

            // Act
            var result = PropertyHelper.GetIntPropertyWithValidation(properties, "missingKey", 99);

            // Assert
            Assert.Equal(99, result);
        }

        /// <summary>
        /// Test that GetIntPropertyWithValidation throws ArgumentException for non-integer values.
        /// </summary>
        [Theory]
        [InlineData("abc")]
        [InlineData("1.5")]
        [InlineData("")]
        [InlineData("12.34")]
        [InlineData("not a number")]
        public void GetIntPropertyWithValidation_ThrowsArgumentExceptionForNonInteger(string invalidValue)
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "intKey", invalidValue }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() =>
                PropertyHelper.GetIntPropertyWithValidation(properties, "intKey", 0));

            // Verify the error message contains the key and value
            Assert.Contains("intKey", exception.Message);
            Assert.Contains(invalidValue, exception.Message);
        }

        /// <summary>
        /// Test that GetPositiveIntPropertyWithValidation returns the value when given a positive integer string.
        /// </summary>
        [Fact]
        public void GetPositiveIntPropertyWithValidation_ReturnsValueForPositiveInt()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "positiveIntKey", "42" }
            };

            // Act
            var result = PropertyHelper.GetPositiveIntPropertyWithValidation(properties, "positiveIntKey", 1);

            // Assert
            Assert.Equal(42, result);
        }

        /// <summary>
        /// Test that GetPositiveIntPropertyWithValidation throws ArgumentOutOfRangeException when the value is 0.
        /// </summary>
        [Fact]
        public void GetPositiveIntPropertyWithValidation_ThrowsArgumentOutOfRangeExceptionForZero()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "positiveIntKey", "0" }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
                PropertyHelper.GetPositiveIntPropertyWithValidation(properties, "positiveIntKey", 1));

            // Verify the exception contains the key name
            Assert.Contains("positiveIntKey", exception.Message);
        }

        /// <summary>
        /// Test that GetPositiveIntPropertyWithValidation throws ArgumentOutOfRangeException for negative integers.
        /// </summary>
        [Fact]
        public void GetPositiveIntPropertyWithValidation_ThrowsArgumentOutOfRangeExceptionForNegativeInt()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "positiveIntKey", "-5" }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
                PropertyHelper.GetPositiveIntPropertyWithValidation(properties, "positiveIntKey", 1));

            // Verify the exception contains the key name
            Assert.Contains("positiveIntKey", exception.Message);
        }

        /// <summary>
        /// Test that GetPositiveIntPropertyWithValidation throws ArgumentException for non-integer values.
        /// </summary>
        [Theory]
        [InlineData("abc")]
        [InlineData("1.5")]
        [InlineData("")]
        [InlineData("12.34")]
        [InlineData("not a number")]
        public void GetPositiveIntPropertyWithValidation_ThrowsArgumentExceptionForNonInteger(string invalidValue)
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "positiveIntKey", invalidValue }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() =>
                PropertyHelper.GetPositiveIntPropertyWithValidation(properties, "positiveIntKey", 1));

            // Verify the error message contains the key and value
            Assert.Contains("positiveIntKey", exception.Message);
            Assert.Contains(invalidValue, exception.Message);
        }

        /// <summary>
        /// Test that GetLongPropertyWithValidation returns the correctly parsed long when given a valid long string.
        /// </summary>
        [Fact]
        public void GetLongPropertyWithValidation_ReturnsParsedLongForValidString()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "longKey", "9223372036854775807" } // long.MaxValue
            };

            // Act
            var result = PropertyHelper.GetLongPropertyWithValidation(properties, "longKey", 0L);

            // Assert
            Assert.Equal(9223372036854775807L, result);
        }

        /// <summary>
        /// Test that GetLongPropertyWithValidation throws ArgumentException for non-long values.
        /// </summary>
        [Theory]
        [InlineData("abc")]
        [InlineData("1.5")]
        [InlineData("")]
        [InlineData("12.34")]
        [InlineData("not a number")]
        public void GetLongPropertyWithValidation_ThrowsArgumentExceptionForNonLong(string invalidValue)
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "longKey", invalidValue }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() =>
                PropertyHelper.GetLongPropertyWithValidation(properties, "longKey", 0L));

            // Verify the error message contains the key and value
            Assert.Contains("longKey", exception.Message);
            Assert.Contains(invalidValue, exception.Message);
        }

        /// <summary>
        /// Test that GetPositiveLongPropertyWithValidation returns the value when given a positive long string.
        /// </summary>
        [Fact]
        public void GetPositiveLongPropertyWithValidation_ReturnsValueForPositiveLong()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "positiveLongKey", "9223372036854775807" } // long.MaxValue
            };

            // Act
            var result = PropertyHelper.GetPositiveLongPropertyWithValidation(properties, "positiveLongKey", 1L);

            // Assert
            Assert.Equal(9223372036854775807L, result);
        }

        /// <summary>
        /// Test that GetPositiveLongPropertyWithValidation throws ArgumentOutOfRangeException when the value is 0.
        /// </summary>
        [Fact]
        public void GetPositiveLongPropertyWithValidation_ThrowsArgumentOutOfRangeExceptionForZero()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "positiveLongKey", "0" }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
                PropertyHelper.GetPositiveLongPropertyWithValidation(properties, "positiveLongKey", 1L));

            // Verify the exception contains the key name
            Assert.Contains("positiveLongKey", exception.Message);
        }

        /// <summary>
        /// Test that GetPositiveLongPropertyWithValidation throws ArgumentOutOfRangeException for negative longs.
        /// </summary>
        [Fact]
        public void GetPositiveLongPropertyWithValidation_ThrowsArgumentOutOfRangeExceptionForNegativeLong()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { "positiveLongKey", "-9223372036854775808" } // long.MinValue
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
                PropertyHelper.GetPositiveLongPropertyWithValidation(properties, "positiveLongKey", 1L));

            // Verify the exception contains the key name
            Assert.Contains("positiveLongKey", exception.Message);
        }

        /// <summary>
        /// Test that ParseOrgIdFromQueryString extracts the org ID from a query string containing 'o=12345'.
        /// </summary>
        [Fact]
        public void ParseOrgIdFromQueryString_ExtractsOrgIdFromSimpleQueryString()
        {
            // Arrange
            var queryString = "o=12345";

            // Act
            var result = PropertyHelper.ParseOrgIdFromQueryString(queryString);

            // Assert
            Assert.Equal("12345", result);
        }

        /// <summary>
        /// Test that ParseOrgIdFromQueryString returns null when the 'o' parameter is missing.
        /// </summary>
        [Fact]
        public void ParseOrgIdFromQueryString_ReturnsNullWhenOParamMissing()
        {
            // Arrange
            var queryString = "a=1&b=2&c=3";

            // Act
            var result = PropertyHelper.ParseOrgIdFromQueryString(queryString);

            // Assert
            Assert.Null(result);
        }

        /// <summary>
        /// Test that ParseOrgIdFromQueryString returns null when the 'o' parameter is present but has no value.
        /// </summary>
        [Fact]
        public void ParseOrgIdFromQueryString_ReturnsNullWhenOParamIsEmpty()
        {
            // Arrange
            var queryString = "o=";

            // Act
            var result = PropertyHelper.ParseOrgIdFromQueryString(queryString);

            // Assert
            Assert.Null(result);
        }

        /// <summary>
        /// Test that ParseOrgIdFromQueryString correctly handles URL-encoded organization ID values.
        /// </summary>
        [Fact]
        public void ParseOrgIdFromQueryString_HandlesUrlEncodedOrgIdValues()
        {
            // Arrange
            var queryString = "o=abc%2Bdef%3D123"; // URL-encoded "abc+def=123"

            // Act
            var result = PropertyHelper.ParseOrgIdFromQueryString(queryString);

            // Assert
            Assert.Equal("abc+def=123", result);
        }

        /// <summary>
        /// Test that ParseOrgIdFromQueryString correctly extracts org ID from query strings with multiple parameters.
        /// </summary>
        [Fact]
        public void ParseOrgIdFromQueryString_HandlesMultipleQueryParams()
        {
            // Arrange
            var queryString = "a=1&o=123&b=2";

            // Act
            var result = PropertyHelper.ParseOrgIdFromQueryString(queryString);

            // Assert
            Assert.Equal("123", result);
        }

        /// <summary>
        /// Test that ParseOrgIdFromProperties returns null when properties is null.
        /// </summary>
        [Fact]
        public void ParseOrgIdFromProperties_ReturnsNullWhenPropertiesIsNull()
        {
            // Arrange
            IReadOnlyDictionary<string, string>? properties = null;

            // Act
            var result = PropertyHelper.ParseOrgIdFromProperties(properties);

            // Assert
            Assert.Null(result);
        }
    }
}
