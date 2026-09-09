/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System.Collections.Generic;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Tests for the SPOG cluster-path fallback in <see cref="PropertyHelper.ParseOrgIdFromProperties"/>.
    /// Existing <c>?o=</c> extraction is covered indirectly through
    /// <see cref="StatementExecution.StatementExecutionConnectionOrgIdTests"/>.
    /// </summary>
    public class PropertyHelperTests
    {
        [Fact]
        public void ParseOrgIdFromProperties_ClusterPathWithoutQueryParam_ExtractsOrgIdFromPathSegment()
        {
            var props = new Dictionary<string, string>
            {
                { SparkParameters.Path, "sql/protocolv1/o/6051921418418893/0528-220959-uzmcn1qt" },
            };

            Assert.Equal("6051921418418893", PropertyHelper.ParseOrgIdFromProperties(props));
        }

        [Fact]
        public void ParseOrgIdFromProperties_ClusterPathWithLeadingSlash_ExtractsOrgIdFromPathSegment()
        {
            var props = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/protocolv1/o/6051921418418893/0528-220959-uzmcn1qt" },
            };

            Assert.Equal("6051921418418893", PropertyHelper.ParseOrgIdFromProperties(props));
        }

        [Fact]
        public void ParseOrgIdFromProperties_ClusterPathWithQueryParam_QueryParamWins()
        {
            // ?o= takes precedence over the path segment when both are present.
            var props = new Dictionary<string, string>
            {
                { SparkParameters.Path, "sql/protocolv1/o/111/0528-220959-uzmcn1qt?o=222" },
            };

            Assert.Equal("222", PropertyHelper.ParseOrgIdFromProperties(props));
        }

        [Fact]
        public void ParseOrgIdFromProperties_WarehousePathWithoutQueryParam_ReturnsNull()
        {
            // Regression guard: the cluster-path fallback must not match warehouse paths
            // (they never embed the workspace ID).
            var props = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
            };

            Assert.Null(PropertyHelper.ParseOrgIdFromProperties(props));
        }

        [Fact]
        public void ParseOrgIdFromProperties_UriWithClusterPath_ExtractsOrgIdFromPathSegment()
        {
            // When the path is supplied via AdbcOptions.Uri instead of SparkParameters.Path,
            // the cluster-segment fallback still applies.
            var props = new Dictionary<string, string>
            {
                {
                    AdbcOptions.Uri,
                    "https://host.databricks.com/sql/protocolv1/o/6051921418418893/0528-220959-uzmcn1qt"
                },
            };

            Assert.Equal("6051921418418893", PropertyHelper.ParseOrgIdFromProperties(props));
        }
    }
}
