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

using System.Text.Json;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Apache.Arrow.Ipc;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E
{
    /// <summary>
    /// E2E coverage for PECO-3032 (D3): when EnableComplexDatatypeSupport=false (default)
    /// and a MAP column contains values with double-quote characters, the returned JSON
    /// string must be valid. Server-emitted JSON (Thrift's flag=false path) leaks the
    /// inner quotes unescaped, producing malformed JSON like
    /// <c>{"key":"val with "quote""}</c>.
    ///
    /// The fix makes the C# driver serialize complex types client-side on both Thrift and
    /// SEA paths (matching JDBC), so escaping is handled by System.Text.Json regardless
    /// of what the server emits.
    /// </summary>
    public class ThriftMapJsonEscapingE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public ThriftMapJsonEscapingE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        private async Task<(AdbcConnection conn, IArrowArrayStream stream)> ExecuteAsync(string sql, string protocol)
        {
            var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
            // Some local configs carry BOTH 'uri' and 'hostName'/'path'; the driver rejects
            // this combination with "Conflicting server arguments". Resolve the conflict only
            // when both keys are present — CI configs typically have only one of them and
            // removing the wrong key would leave no host source.
            if (properties.ContainsKey(AdbcOptions.Uri) &&
                properties.ContainsKey(AdbcDrivers.HiveServer2.Spark.SparkParameters.HostName))
            {
                properties.Remove(AdbcOptions.Uri);
            }
            properties[DatabricksParameters.Protocol] = protocol;

            AdbcDriver driver = new DatabricksDriver();
            AdbcDatabase database = driver.Open(properties);
            AdbcConnection connection = database.Connect(properties);

            var statement = connection.CreateStatement();
            statement.SqlQuery = sql;
            QueryResult result = await statement.ExecuteQueryAsync();
            return (connection, result.Stream!);
        }

        [SkippableTheory]
        [InlineData("thrift")]
        [InlineData("rest")]
        public async Task MapValueContainingDoubleQuote_ReturnsValidJson(string protocol)
        {
            // The literal MAP value is: { "key": "val \"quote\"" }
            // — i.e. the value string contains two double-quote characters surrounding "quote".
            // Pre-fix, the Thrift path emits {"key":"val "quote""} (inner quotes unescaped),
            // which is invalid JSON.
            var (conn, stream) = await ExecuteAsync(
                "SELECT MAP('key', 'val \"quote\"')",
                protocol);
            using (conn)
            using (stream)
            {
                RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(1, batch!.Length);

                string raw = ((StringArray)batch.Column(0)).GetString(0);
                OutputHelper?.WriteLine($"[{protocol}] raw MAP column = {raw}");

                // The string must be valid JSON — parseable by System.Text.Json.
                using JsonDocument doc = JsonDocument.Parse(raw);
                Assert.Equal(JsonValueKind.Object, doc.RootElement.ValueKind);
                Assert.Equal("val \"quote\"", doc.RootElement.GetProperty("key").GetString());
            }
        }
    }
}
