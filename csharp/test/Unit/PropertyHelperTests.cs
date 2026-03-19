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
    }
}
