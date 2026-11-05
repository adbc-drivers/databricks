/*
 * Copyright (c) 2026 ADBC Drivers Contributors
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
using System.Net.Http;
using AdbcDrivers.Databricks.StatementExecution;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Databricks.Tests
{
    internal sealed class MetadataRecordingConnection : StatementExecutionConnection
    {
        internal MetadataRecordingConnection(IReadOnlyDictionary<string, string> properties)
            : base(properties)
        {
        }

        internal MetadataRecordingConnection(IReadOnlyDictionary<string, string> properties, HttpClient http)
            : base(properties, http)
        {
        }

        internal List<StatementExecutionStatement> Statements { get; } = new();

        public override AdbcStatement CreateStatement()
        {
            var statement = (StatementExecutionStatement)base.CreateStatement();
            Statements.Add(statement);
            return statement;
        }

        internal void AssertNativeResponses(params string[] commands)
        {
            Assert.NotEmpty(Statements);
            foreach (string command in commands)
                Assert.Contains(Statements, statement =>
                    statement.SqlQuery?.StartsWith(command, StringComparison.Ordinal) == true);
            Assert.All(Statements, statement => Assert.True(statement.IsNativeMetadataResult,
                $"{statement.SqlQuery} fell back to SHOW; expected is_native_metadata_result: true."));
        }
    }
}
