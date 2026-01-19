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
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using AdbcDrivers.Databricks;
using Xunit;

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
        /// Tests that conf overlay parameters with the correct prefix are captured.
        /// </summary>
        [Fact]
        public void SetOption_WithConfOverlayPrefix_AddsToConfOverlay()
        {
            // Arrange
            using var statement = CreateStatement();

            // Act
            statement.SetOption("adbc.databricks.conf_overlay_spark.sql.adaptive.enabled", "true");
            statement.SetOption("adbc.databricks.conf_overlay_query_tags", "team:engineering");

            // Assert
            var confOverlay = GetConfOverlay(statement);
            Assert.NotNull(confOverlay);
            Assert.Equal(2, confOverlay.Count);
            Assert.Equal("true", confOverlay["spark.sql.adaptive.enabled"]);
            Assert.Equal("team:engineering", confOverlay["query_tags"]);
        }

        /// <summary>
        /// Tests that setting an empty key after prefix removal throws ArgumentException.
        /// </summary>
        [Fact]
        public void SetOption_WithEmptyKeyAfterPrefix_ThrowsArgumentException()
        {
            // Arrange
            using var statement = CreateStatement();

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() =>
                statement.SetOption("adbc.databricks.conf_overlay_", "value"));

            Assert.Contains("Key cannot be empty after removing prefix", exception.Message);
        }

        /// <summary>
        /// Tests that parameters without conf overlay prefix don't get added to confOverlay.
        /// </summary>
        [Fact]
        public void SetOption_WithoutConfOverlayPrefix_DoesNotAddToConfOverlay()
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
        /// Tests that multiple conf overlay parameters accumulate in the dictionary.
        /// </summary>
        [Fact]
        public void SetOption_MultipleConfOverlayParameters_AccumulatesInDictionary()
        {
            // Arrange
            using var statement = CreateStatement();

            // Act
            statement.SetOption("adbc.databricks.conf_overlay_param1", "value1");
            statement.SetOption("adbc.databricks.conf_overlay_param2", "value2");
            statement.SetOption("adbc.databricks.conf_overlay_param3", "value3");

            // Assert
            var confOverlay = GetConfOverlay(statement);
            Assert.NotNull(confOverlay);
            Assert.Equal(3, confOverlay.Count);
            Assert.Equal("value1", confOverlay["param1"]);
            Assert.Equal("value2", confOverlay["param2"]);
            Assert.Equal("value3", confOverlay["param3"]);
        }

        /// <summary>
        /// Tests that conf overlay parameters work alongside regular parameters.
        /// </summary>
        [Fact]
        public void SetOption_MixedConfOverlayAndRegularParameters_BothWork()
        {
            // Arrange
            using var statement = CreateStatement();

            // Act - Set both conf overlay and regular parameters using correct parameter name
            statement.SetOption("adbc.databricks.conf_overlay_query_tags", "k1:v1,k2:v2");
            statement.SetOption(DatabricksParameters.UseCloudFetch, "false");
            statement.SetOption("adbc.databricks.conf_overlay_spark.sql.adaptive.enabled", "true");

            // Assert - Check conf overlay has only the conf_overlay_ parameters
            var confOverlay = GetConfOverlay(statement);
            Assert.NotNull(confOverlay);
            Assert.Equal(2, confOverlay.Count);
            Assert.Equal("k1:v1,k2:v2", confOverlay["query_tags"]);
            Assert.Equal("true", confOverlay["spark.sql.adaptive.enabled"]);

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
    }
}