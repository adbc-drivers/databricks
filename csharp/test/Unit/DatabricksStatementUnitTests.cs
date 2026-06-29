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
using AdbcDrivers.HiveServer2.Spark;
using AdbcDrivers.Databricks;
using Apache.Arrow;
using Xunit;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Unit tests for DatabricksStatement class methods.
    /// </summary>
    public class DatabricksStatementTests
    {
        /// <summary>
        /// Creates a minimal DatabricksStatement for testing internal methods.
        /// </summary>
        private DatabricksStatement CreateStatement()
        {
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "test.databricks.com",
                [SparkParameters.Token] = "test-token"
            };

            // Create connection directly without opening database
            var connection = new DatabricksConnection(properties);
            return new DatabricksStatement(connection);
        }

        /// <summary>
        /// Helper method to access private confOverlay field using reflection.
        /// </summary>
        private Dictionary<string, string>? GetConfOverlay(DatabricksStatement statement)
        {
            var field = typeof(DatabricksStatement).GetField("confOverlay",
                BindingFlags.NonPublic | BindingFlags.Instance);
            return (Dictionary<string, string>?)field?.GetValue(statement);
        }

        /// <summary>
        /// Tests that query_tags parameter is captured and added to confOverlay.
        /// </summary>
        [Fact]
        public void SetOption_WithQueryTags_AddsToConfOverlay()
        {
            // Arrange
            using var statement = CreateStatement();

            // Act
            statement.SetOption(DatabricksParameters.QueryTags, "team:engineering,app:myapp");

            // Assert
            var confOverlay = GetConfOverlay(statement);
            Assert.NotNull(confOverlay);
            Assert.Single(confOverlay);
            Assert.Equal("team:engineering,app:myapp", confOverlay["query_tags"]);
        }

        /// <summary>
        /// Tests that parameters without query_tags don't get added to confOverlay.
        /// </summary>
        [Fact]
        public void SetOption_WithoutQueryTags_DoesNotAddToConfOverlay()
        {
            // Arrange
            using var statement = CreateStatement();

            // Act
            statement.SetOption(DatabricksParameters.UseCloudFetch, "true");

            // Assert
            var confOverlay = GetConfOverlay(statement);
            Assert.True(confOverlay == null || confOverlay.Count == 0);
        }

        /// <summary>
        /// Tests that query_tags works alongside regular parameters.
        /// </summary>
        [Fact]
        public void SetOption_MixedQueryTagsAndRegularParameters_BothWork()
        {
            // Arrange
            using var statement = CreateStatement();

            // Act
            statement.SetOption(DatabricksParameters.QueryTags, "k1:v1,k2:v2");
            statement.SetOption(DatabricksParameters.UseCloudFetch, "false");

            // Assert - Check conf overlay has query_tags
            var confOverlay = GetConfOverlay(statement);
            Assert.NotNull(confOverlay);
            Assert.Single(confOverlay);
            Assert.Equal("k1:v1,k2:v2", confOverlay["query_tags"]);

            // Assert - Regular parameter was set
            Assert.False(statement.UseCloudFetch);
        }

        /// <summary>
        /// Tests that confOverlay dictionary is initially null before any conf overlay parameters are set.
        /// </summary>
        [Fact]
        public void CreateStatement_ConfOverlayInitiallyNull()
        {
            // Arrange & Act
            using var statement = CreateStatement();

            // Assert
            var confOverlay = GetConfOverlay(statement);
            Assert.Null(confOverlay);
        }

        /// <summary>
        /// Tests that unrecognized options are silently dropped instead of throwing (PECO-2952).
        /// </summary>
        [Fact]
        public void SetOption_UnrecognizedKey_DoesNotThrow()
        {
            using var statement = CreateStatement();
            statement.SetOption("adbc.databricks.unknown_future_option", "some_value");
        }

        [Theory]
        [InlineData("getcatalogs", OperationType.ListCatalogs)]
        [InlineData("getschemas", OperationType.ListSchemas)]
        [InlineData("gettables", OperationType.ListTables)]
        [InlineData("getcolumns", OperationType.ListColumns)]
        [InlineData("getcolumnsextended", OperationType.ListColumns)]
        [InlineData("gettabletypes", OperationType.ListTableTypes)]
        [InlineData("getprimarykeys", OperationType.ListPrimaryKeys)]
        [InlineData("getcrossreference", OperationType.ListCrossReferences)]
        public void GetMetadataOperationType_ReturnsCorrectType(string command, OperationType expected)
        {
            Assert.Equal(expected, DatabricksStatement.GetMetadataOperationType(command));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("SELECT 1")]
        [InlineData("unknown_command")]
        public void GetMetadataOperationType_ReturnsNull_ForNonMetadataCommands(string? command)
        {
            Assert.Null(DatabricksStatement.GetMetadataOperationType(command));
        }

        [Theory]
        [InlineData("GETCATALOGS")]
        [InlineData("GetCatalogs")]
        [InlineData("GetTables")]
        public void GetMetadataOperationType_IsCaseInsensitive(string command)
        {
            Assert.NotNull(DatabricksStatement.GetMetadataOperationType(command));
        }

        /// <summary>
        /// Invokes the private static <c>NormalizeStringColumn</c> via reflection so the pure
        /// GetTables value-parity logic (issue #527) can be unit-tested without a live warehouse.
        /// </summary>
        private static StringArray InvokeNormalizeStringColumn(StringArray source, string defaultValue, bool normalizeUnknown)
        {
            var method = typeof(DatabricksStatement).GetMethod("NormalizeStringColumn",
                BindingFlags.NonPublic | BindingFlags.Static);
            Assert.NotNull(method);
            return (StringArray)method!.Invoke(null, new object[] { source, defaultValue, normalizeUnknown })!;
        }

        private static StringArray BuildStringArray(params string?[] values)
        {
            var builder = new StringArray.Builder();
            foreach (string? value in values)
            {
                if (value == null)
                {
                    builder.AppendNull();
                }
                else
                {
                    builder.Append(value);
                }
            }
            return builder.Build();
        }

        private static string?[] ToStrings(StringArray array)
        {
            var result = new string?[array.Length];
            for (int i = 0; i < array.Length; i++)
            {
                result[i] = array.IsNull(i) ? null : array.GetString(i);
            }
            return result;
        }

        /// <summary>
        /// REMARKS normalization (issue #527): null/empty and the legacy case-sensitive "UNKNOWN"
        /// placeholder default to "", while a genuine "Unknown"/"unknown" comment is preserved.
        /// </summary>
        [Fact]
        public void NormalizeStringColumn_Remarks_NormalizesNullEmptyAndUnknownCaseSensitively()
        {
            using var source = BuildStringArray(null, "", "UNKNOWN", "Unknown", "unknown", "a real comment");

            using var result = InvokeNormalizeStringColumn(source, string.Empty, normalizeUnknown: true);

            Assert.Equal(
                new string?[] { "", "", "", "Unknown", "unknown", "a real comment" },
                ToStrings(result));
        }

        /// <summary>
        /// TABLE_TYPE normalization (issue #527): null/empty default to "TABLE", but the "UNKNOWN"
        /// sentinel is NOT special-cased here (normalizeUnknown: false), matching the production call.
        /// </summary>
        [Fact]
        public void NormalizeStringColumn_TableType_DefaultsNullEmptyButLeavesUnknown()
        {
            using var source = BuildStringArray(null, "", "TABLE", "VIEW", "UNKNOWN");

            using var result = InvokeNormalizeStringColumn(source, "TABLE", normalizeUnknown: false);

            Assert.Equal(
                new string?[] { "TABLE", "TABLE", "TABLE", "VIEW", "UNKNOWN" },
                ToStrings(result));
        }
    }
}
