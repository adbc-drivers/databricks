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
using AdbcDrivers.Databricks;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Tests for PropertyHelper. Focus: the *WithValidation getters ignore invalid values
    /// (fall back to the default) instead of throwing, so a malformed value from any source —
    /// including server feature flags merged into the connection properties — never fails
    /// connection setup.
    /// </summary>
    public class PropertyHelperTests
    {
        private const string Key = "adbc.databricks.some_flag";

        [Theory]
        [InlineData("true", true)]
        [InlineData("false", false)]
        [InlineData("TRUE", true)]
        public void GetBooleanPropertyWithValidation_ValidValue_IsParsed(string value, bool expected)
        {
            var props = new Dictionary<string, string> { [Key] = value };
            Assert.Equal(expected, PropertyHelper.GetBooleanPropertyWithValidation(props, Key, defaultValue: !expected));
        }

        [Theory]
        [InlineData("null")]   // the observed feature-flag placeholder value
        [InlineData("")]
        [InlineData("  ")]
        [InlineData("yes")]
        [InlineData("1")]
        public void GetBooleanPropertyWithValidation_InvalidValue_ReturnsDefault_NoThrow(string value)
        {
            var props = new Dictionary<string, string> { [Key] = value };
            // Invalid value present -> default is returned (not thrown), for both default values.
            Assert.True(PropertyHelper.GetBooleanPropertyWithValidation(props, Key, defaultValue: true));
            Assert.False(PropertyHelper.GetBooleanPropertyWithValidation(props, Key, defaultValue: false));
        }

        [Fact]
        public void GetBooleanPropertyWithValidation_Absent_ReturnsDefault()
        {
            var props = new Dictionary<string, string>();
            Assert.True(PropertyHelper.GetBooleanPropertyWithValidation(props, Key, defaultValue: true));
        }

        [Theory]
        [InlineData("null")]
        [InlineData("")]
        [InlineData("notanint")]
        public void GetIntPropertyWithValidation_InvalidValue_ReturnsDefault_NoThrow(string value)
        {
            var props = new Dictionary<string, string> { [Key] = value };
            Assert.Equal(42, PropertyHelper.GetIntPropertyWithValidation(props, Key, defaultValue: 42));
        }

        [Theory]
        [InlineData("null")]
        [InlineData("0")]     // not positive
        [InlineData("-5")]    // not positive
        [InlineData("abc")]
        public void GetPositiveIntPropertyWithValidation_InvalidOrNonPositive_ReturnsDefault_NoThrow(string value)
        {
            var props = new Dictionary<string, string> { [Key] = value };
            Assert.Equal(7, PropertyHelper.GetPositiveIntPropertyWithValidation(props, Key, defaultValue: 7));
        }

        [Theory]
        [InlineData("null")]
        [InlineData("0")]
        [InlineData("-1")]
        [InlineData("abc")]
        public void GetPositiveLongPropertyWithValidation_InvalidOrNonPositive_ReturnsDefault_NoThrow(string value)
        {
            var props = new Dictionary<string, string> { [Key] = value };
            Assert.Equal(99L, PropertyHelper.GetPositiveLongPropertyWithValidation(props, Key, defaultValue: 99L));
        }

        [Theory]
        [InlineData("null")]
        [InlineData("notalong")]
        public void GetLongPropertyWithValidation_InvalidValue_ReturnsDefault_NoThrow(string value)
        {
            var props = new Dictionary<string, string> { [Key] = value };
            Assert.Equal(123L, PropertyHelper.GetLongPropertyWithValidation(props, Key, defaultValue: 123L));
        }
    }
}
