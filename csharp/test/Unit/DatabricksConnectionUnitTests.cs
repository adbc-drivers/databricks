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
using Apache.Arrow;
using Apache.Arrow.Adbc;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
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

        /// <summary>
        /// Warehouse-style paths (/sql/1.0/warehouses/{id}, /sql/1.0/endpoints/{id}) — with or
        /// without query strings — must be classified as DBSQL warehouse so the fast-metadata
        /// flag can take effect on them.
        /// </summary>
        [Theory]
        [InlineData("/sql/1.0/warehouses/abc123")]
        [InlineData("/sql/1.0/warehouses/abc123/")]
        [InlineData("/sql/1.0/warehouses/abc123?o=987654")]
        [InlineData("/sql/1.0/endpoints/abc123")]
        [InlineData("/sql/1.0/endpoints/abc123?o=111&foo=bar")]
        public void IsWarehousePath_WarehousePaths_ReturnsTrue(string path)
        {
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "test.databricks.com",
                [SparkParameters.Token] = "test-token",
                [SparkParameters.Path] = path
            };
            using var connection = new DatabricksConnection(properties);
            Assert.True(connection.IsWarehousePath, $"Path '{path}' should be classified as a warehouse");
        }

        /// <summary>
        /// General-cluster paths and empty paths must NOT be classified as warehouses, so the
        /// fast-metadata flag is a no-op there even when enabled.
        /// </summary>
        [Theory]
        [InlineData("/sql/protocolv1/o/123456789/0123-456789-abcdef")]
        [InlineData("/sql/protocolv1/o/987/cluster-id")]
        [InlineData("/some/other/path")]
        [InlineData("")]
        public void IsWarehousePath_NonWarehousePaths_ReturnsFalse(string path)
        {
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "test.databricks.com",
                [SparkParameters.Token] = "test-token"
            };
            if (!string.IsNullOrEmpty(path))
            {
                properties[SparkParameters.Path] = path;
            }
            using var connection = new DatabricksConnection(properties);
            Assert.False(connection.IsWarehousePath, $"Path '{path}' should NOT be classified as a warehouse");
        }

        /// <summary>
        /// IsWarehousePath should also resolve a path embedded in the AdbcOptions.Uri property
        /// (the JDBC-style "uri" form) so users who configure that way still get the optimization.
        /// </summary>
        [Fact]
        public void IsWarehousePath_PathFromUri_ReturnsTrue()
        {
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.Token] = "test-token",
                [AdbcOptions.Uri] = "https://test.databricks.com/sql/1.0/warehouses/abc123?o=555"
            };
            using var connection = new DatabricksConnection(properties);
            Assert.True(connection.IsWarehousePath);
        }

        /// <summary>
        /// UseFastMetadataQuery requires BOTH the opt-in flag and a warehouse path. Verify the
        /// AND-gating: flag without warehouse path stays false, warehouse without flag stays false,
        /// only flag-and-warehouse-together returns true.
        /// </summary>
        [Theory]
        [InlineData("true", "/sql/1.0/warehouses/abc123", true)]
        [InlineData("true", "/sql/protocolv1/o/123/cluster", false)] // flag set but general cluster
        [InlineData("false", "/sql/1.0/warehouses/abc123", false)]    // warehouse but flag off
        [InlineData(null, "/sql/1.0/warehouses/abc123", false)]       // flag absent (default false)
        public void UseFastMetadataQuery_RequiresFlagAndWarehousePath(string? flagValue, string path, bool expected)
        {
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "test.databricks.com",
                [SparkParameters.Token] = "test-token",
                [SparkParameters.Path] = path
            };
            if (flagValue != null)
            {
                properties[DatabricksParameters.EnableFastMetadataQuery] = flagValue;
            }
            using var connection = new DatabricksConnection(properties);
            Assert.Equal(expected, connection.UseFastMetadataQuery);
        }

        /// <summary>
        /// Tests that GetInfo returns correct driver information for DriverName and DriverArrowVersion.
        /// Note: VendorName and VendorVersion require a server connection and should be tested in E2E tests.
        /// </summary>
        [Fact]
        public async System.Threading.Tasks.Task GetInfo_DriverInformation_ReturnsCorrectValues()
        {
            // Arrange
            using var connection = CreateMinimalConnection();
            var infoCodes = new List<AdbcInfoCode>
            {
                AdbcInfoCode.DriverName,
                AdbcInfoCode.DriverArrowVersion
            };

            // Act
            using IArrowArrayStream stream = connection.GetInfo(infoCodes);
            using RecordBatch recordBatch = await stream.ReadNextRecordBatchAsync();

            // Assert
            Assert.NotNull(recordBatch);
            Assert.Equal(2, recordBatch.Length);

            // Get the columns
            UInt32Array infoNameArray = (UInt32Array)recordBatch.Column("info_name");
            DenseUnionArray valueArray = (DenseUnionArray)recordBatch.Column("info_value");
            StringArray stringArray = (StringArray)valueArray.Fields[0];

            // Verify each info code
            for (int i = 0; i < infoNameArray.Length; i++)
            {
                AdbcInfoCode infoCode = (AdbcInfoCode)infoNameArray.GetValue(i)!.Value;
                string value = stringArray.GetString(i);

                switch (infoCode)
                {
                    case AdbcInfoCode.DriverName:
                        Assert.Equal("ADBC Databricks Driver", value);
                        break;
                    case AdbcInfoCode.DriverArrowVersion:
                        Assert.Equal("1.0.0", value);
                        break;
                    default:
                        Assert.Fail($"Unexpected info code: {infoCode}");
                        break;
                }
            }
        }
    }
}
