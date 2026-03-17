/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    public class MetadataUtilitiesTests
    {
        // NormalizeSparkCatalog

        [Fact]
        public void NormalizeSparkCatalog_Spark_ReturnsNull()
        {
            Assert.Null(MetadataUtilities.NormalizeSparkCatalog("SPARK"));
        }

        [Fact]
        public void NormalizeSparkCatalog_SparkLowerCase_ReturnsNull()
        {
            Assert.Null(MetadataUtilities.NormalizeSparkCatalog("spark"));
        }

        [Fact]
        public void NormalizeSparkCatalog_SparkMixedCase_ReturnsNull()
        {
            Assert.Null(MetadataUtilities.NormalizeSparkCatalog("Spark"));
        }

        [Fact]
        public void NormalizeSparkCatalog_Null_ReturnsNull()
        {
            Assert.Null(MetadataUtilities.NormalizeSparkCatalog(null));
        }

        [Fact]
        public void NormalizeSparkCatalog_Empty_ReturnsEmpty()
        {
            Assert.Equal("", MetadataUtilities.NormalizeSparkCatalog(""));
        }

        [Fact]
        public void NormalizeSparkCatalog_Main_ReturnsMain()
        {
            Assert.Equal("main", MetadataUtilities.NormalizeSparkCatalog("main"));
        }

        // IsInvalidPKFKCatalog

        [Theory]
        [InlineData(null, true)]
        [InlineData("", true)]
        [InlineData("SPARK", true)]
        [InlineData("spark", true)]
        [InlineData("hive_metastore", true)]
        [InlineData("main", false)]
        [InlineData("samples", false)]
        public void IsInvalidPKFKCatalog(string? catalog, bool expected)
        {
            Assert.Equal(expected, MetadataUtilities.IsInvalidPKFKCatalog(catalog));
        }

        // ShouldReturnEmptyPKFKResult

        [Fact]
        public void ShouldReturnEmptyPKFKResult_PKFKDisabled_ReturnsTrue()
        {
            Assert.True(MetadataUtilities.ShouldReturnEmptyPKFKResult("main", "main", enablePKFK: false));
        }

        [Fact]
        public void ShouldReturnEmptyPKFKResult_BothInvalid_ReturnsTrue()
        {
            Assert.True(MetadataUtilities.ShouldReturnEmptyPKFKResult("SPARK", "hive_metastore", enablePKFK: true));
        }

        [Fact]
        public void ShouldReturnEmptyPKFKResult_OneValid_ReturnsFalse()
        {
            Assert.False(MetadataUtilities.ShouldReturnEmptyPKFKResult("main", "SPARK", enablePKFK: true));
        }

        [Fact]
        public void ShouldReturnEmptyPKFKResult_BothValid_ReturnsFalse()
        {
            Assert.False(MetadataUtilities.ShouldReturnEmptyPKFKResult("main", "main", enablePKFK: true));
        }

        // BuildQualifiedTableName

        [Fact]
        public void BuildQualifiedTableName_AllParts()
        {
            Assert.Equal("`main`.`default`.`users`", MetadataUtilities.BuildQualifiedTableName("main", "default", "users"));
        }

        [Fact]
        public void BuildQualifiedTableName_NoSchema()
        {
            Assert.Equal("`users`", MetadataUtilities.BuildQualifiedTableName("main", null, "users"));
        }

        [Fact]
        public void BuildQualifiedTableName_SparkCatalog_OmitsCatalog()
        {
            Assert.Equal("`default`.`users`", MetadataUtilities.BuildQualifiedTableName("SPARK", "default", "users"));
        }

        [Fact]
        public void BuildQualifiedTableName_NullTable_ReturnsNull()
        {
            Assert.Null(MetadataUtilities.BuildQualifiedTableName("main", "default", null));
        }

        [Fact]
        public void BuildQualifiedTableName_EmptyTable_ReturnsEmpty()
        {
            Assert.Equal("", MetadataUtilities.BuildQualifiedTableName("main", "default", ""));
        }

        [Fact]
        public void BuildQualifiedTableName_BackticksEscaped()
        {
            Assert.Equal("`main`.`my``schema`.`my``table`", MetadataUtilities.BuildQualifiedTableName("main", "my`schema", "my`table"));
        }
    }
}
