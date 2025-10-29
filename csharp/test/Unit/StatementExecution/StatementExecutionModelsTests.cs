/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System.Collections.Generic;
using System.Text.Json;
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit.StatementExecution
{
    /// <summary>
    /// Tests for Statement Execution API model serialization and deserialization.
    /// </summary>
    public class StatementExecutionModelsTests
    {
        private readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        [Fact]
        public void TestCreateSessionRequestSerialization()
        {
            var request = new CreateSessionRequest
            {
                WarehouseId = "test-warehouse-id",
                Catalog = "main",
                Schema = "default",
                SessionConfigs = new Dictionary<string, string>
                {
                    { "spark.sql.ansi.enabled", "true" }
                }
            };

            string json = JsonSerializer.Serialize(request, _jsonOptions);
            var deserialized = JsonSerializer.Deserialize<CreateSessionRequest>(json, _jsonOptions);

            Assert.NotNull(deserialized);
            Assert.Equal("test-warehouse-id", deserialized.WarehouseId);
            Assert.Equal("main", deserialized.Catalog);
            Assert.Equal("default", deserialized.Schema);
            Assert.NotNull(deserialized.SessionConfigs);
            Assert.Equal("true", deserialized.SessionConfigs["spark.sql.ansi.enabled"]);
        }

        [Fact]
        public void TestCreateSessionResponseDeserialization()
        {
            string json = @"{""session_id"":""01234567-89ab-cdef-0123-456789abcdef""}";
            var response = JsonSerializer.Deserialize<CreateSessionResponse>(json, _jsonOptions);

            Assert.NotNull(response);
            Assert.Equal("01234567-89ab-cdef-0123-456789abcdef", response.SessionId);
        }

        [Fact]
        public void TestExecuteStatementRequestSerialization()
        {
            var request = new ExecuteStatementRequest
            {
                SessionId = "session-123",
                Statement = "SELECT * FROM table WHERE id = :id",
                Catalog = "main",
                Schema = "default",
                Parameters = new List<StatementParameter>
                {
                    new StatementParameter { Name = "id", Type = "INT", Value = "123" }
                },
                Disposition = "external_links",
                Format = "arrow_stream",
                ResultCompression = "lz4",
                WaitTimeout = "10s",
                RowLimit = 1000
            };

            string json = JsonSerializer.Serialize(request, _jsonOptions);
            var deserialized = JsonSerializer.Deserialize<ExecuteStatementRequest>(json, _jsonOptions);

            Assert.NotNull(deserialized);
            Assert.Equal("session-123", deserialized.SessionId);
            Assert.Equal("SELECT * FROM table WHERE id = :id", deserialized.Statement);
            Assert.Equal("external_links", deserialized.Disposition);
            Assert.Equal("arrow_stream", deserialized.Format);
            Assert.Equal("lz4", deserialized.ResultCompression);
            Assert.Equal("10s", deserialized.WaitTimeout);
            Assert.Equal(1000, deserialized.RowLimit);
            Assert.NotNull(deserialized.Parameters);
            Assert.Single(deserialized.Parameters);
            Assert.Equal("id", deserialized.Parameters[0].Name);
        }

        [Fact]
        public void TestExecuteStatementResponseDeserialization()
        {
            string json = @"{
                ""statement_id"": ""stmt-123"",
                ""status"": {
                    ""state"": ""SUCCEEDED"",
                    ""sql_state"": null
                },
                ""manifest"": {
                    ""format"": ""ARROW_STREAM"",
                    ""total_chunk_count"": 1,
                    ""total_row_count"": 100,
                    ""total_byte_count"": 1024,
                    ""schema"": {
                        ""column_count"": 2,
                        ""columns"": [
                            {
                                ""name"": ""id"",
                                ""position"": 0,
                                ""type_name"": ""BIGINT"",
                                ""type_text"": ""BIGINT""
                            },
                            {
                                ""name"": ""name"",
                                ""position"": 1,
                                ""type_name"": ""STRING"",
                                ""type_text"": ""STRING""
                            }
                        ]
                    }
                }
            }";

            var response = JsonSerializer.Deserialize<ExecuteStatementResponse>(json, _jsonOptions);

            Assert.NotNull(response);
            Assert.Equal("stmt-123", response.StatementId);
            Assert.NotNull(response.Status);
            Assert.Equal("SUCCEEDED", response.Status.State);
            Assert.NotNull(response.Manifest);
            Assert.Equal("ARROW_STREAM", response.Manifest.Format);
            Assert.Equal(1, response.Manifest.TotalChunkCount);
            Assert.Equal(100, response.Manifest.TotalRowCount);
            Assert.Equal(1024, response.Manifest.TotalByteCount);
            Assert.NotNull(response.Manifest.Schema);
            Assert.Equal(2, response.Manifest.Schema.ColumnCount);
            Assert.NotNull(response.Manifest.Schema.Columns);
            Assert.Equal(2, response.Manifest.Schema.Columns.Count);
            Assert.Equal("id", response.Manifest.Schema.Columns[0].Name);
            Assert.Equal("name", response.Manifest.Schema.Columns[1].Name);
        }

        [Fact]
        public void TestStatementStatusWithError()
        {
            string json = @"{
                ""state"": ""FAILED"",
                ""error"": {
                    ""error_code"": ""SYNTAX_ERROR"",
                    ""message"": ""Invalid SQL syntax""
                }
            }";

            var status = JsonSerializer.Deserialize<StatementStatus>(json, _jsonOptions);

            Assert.NotNull(status);
            Assert.Equal("FAILED", status.State);
            Assert.NotNull(status.Error);
            Assert.Equal("SYNTAX_ERROR", status.Error.ErrorCode);
            Assert.Equal("Invalid SQL syntax", status.Error.Message);
        }

        [Fact]
        public void TestResultManifestWithChunks()
        {
            string json = @"{
                ""format"": ""ARROW_STREAM"",
                ""schema"": {
                    ""column_count"": 1,
                    ""columns"": []
                },
                ""total_chunk_count"": 2,
                ""total_row_count"": 2000,
                ""total_byte_count"": 10485760,
                ""result_compression"": ""lz4"",
                ""truncated"": false,
                ""chunks"": [
                    {
                        ""chunk_index"": 0,
                        ""row_count"": 1000,
                        ""row_offset"": 0,
                        ""byte_count"": 5242880,
                        ""external_links"": [
                            {
                                ""external_link"": ""https://s3.amazonaws.com/bucket/file1.arrow"",
                                ""expiration"": ""2025-10-29T00:00:00Z"",
                                ""chunk_index"": 0,
                                ""row_count"": 1000,
                                ""row_offset"": 0,
                                ""byte_count"": 5242880
                            }
                        ]
                    }
                ]
            }";

            var manifest = JsonSerializer.Deserialize<ResultManifest>(json, _jsonOptions);

            Assert.NotNull(manifest);
            Assert.Equal("ARROW_STREAM", manifest.Format);
            Assert.Equal(2, manifest.TotalChunkCount);
            Assert.Equal(2000, manifest.TotalRowCount);
            Assert.Equal(10485760, manifest.TotalByteCount);
            Assert.Equal("lz4", manifest.ResultCompression);
            Assert.False(manifest.Truncated);
            Assert.NotNull(manifest.Chunks);
            Assert.Single(manifest.Chunks);

            var chunk = manifest.Chunks[0];
            Assert.Equal(0, chunk.ChunkIndex);
            Assert.Equal(1000, chunk.RowCount);
            Assert.Equal(0, chunk.RowOffset);
            Assert.Equal(5242880, chunk.ByteCount);
            Assert.NotNull(chunk.ExternalLinks);
            Assert.Single(chunk.ExternalLinks);

            var link = chunk.ExternalLinks[0];
            Assert.Equal("https://s3.amazonaws.com/bucket/file1.arrow", link.ExternalLinkUrl);
            Assert.Equal("2025-10-29T00:00:00Z", link.Expiration);
            Assert.Equal(0, link.ChunkIndex);
        }

        [Fact]
        public void TestExternalLinkWithHttpHeaders()
        {
            string json = @"{
                ""external_link"": ""https://storage.googleapis.com/bucket/file.arrow"",
                ""expiration"": ""2025-10-29T00:00:00Z"",
                ""chunk_index"": 0,
                ""row_count"": 500,
                ""row_offset"": 0,
                ""byte_count"": 2097152,
                ""http_headers"": {
                    ""X-Custom-Header"": ""value"",
                    ""Authorization"": ""Bearer token""
                },
                ""next_chunk_index"": 1
            }";

            var link = JsonSerializer.Deserialize<ExternalLink>(json, _jsonOptions);

            Assert.NotNull(link);
            Assert.Equal("https://storage.googleapis.com/bucket/file.arrow", link.ExternalLinkUrl);
            Assert.Equal(500, link.RowCount);
            Assert.Equal(2097152, link.ByteCount);
            Assert.NotNull(link.HttpHeaders);
            Assert.Equal(2, link.HttpHeaders.Count);
            Assert.Equal("value", link.HttpHeaders["X-Custom-Header"]);
            Assert.Equal("Bearer token", link.HttpHeaders["Authorization"]);
            Assert.Equal(1, link.NextChunkIndex);
        }

        [Fact]
        public void TestResultDataDeserialization()
        {
            string json = @"{
                ""chunk_index"": 0,
                ""row_count"": 2,
                ""row_offset"": 0,
                ""byte_count"": 128,
                ""data_array"": [
                    [""1"", ""Alice""],
                    [""2"", ""Bob""]
                ]
            }";

            var resultData = JsonSerializer.Deserialize<ResultData>(json, _jsonOptions);

            Assert.NotNull(resultData);
            Assert.Equal(0, resultData.ChunkIndex);
            Assert.Equal(2, resultData.RowCount);
            Assert.Equal(0, resultData.RowOffset);
            Assert.Equal(128, resultData.ByteCount);
            Assert.NotNull(resultData.DataArray);
            Assert.Equal(2, resultData.DataArray.Count);
            Assert.Equal(2, resultData.DataArray[0].Count);
            Assert.Equal("Alice", resultData.DataArray[0][1]);
        }

        [Fact]
        public void TestColumnInfoDeserialization()
        {
            string json = @"{
                ""name"": ""price"",
                ""position"": 0,
                ""type_name"": ""DECIMAL"",
                ""type_precision"": 18,
                ""type_scale"": 2,
                ""type_text"": ""DECIMAL(18,2)""
            }";

            var columnInfo = JsonSerializer.Deserialize<ColumnInfo>(json, _jsonOptions);

            Assert.NotNull(columnInfo);
            Assert.Equal("price", columnInfo.Name);
            Assert.Equal(0, columnInfo.Position);
            Assert.Equal("DECIMAL", columnInfo.TypeName);
            Assert.Equal(18, columnInfo.TypePrecision);
            Assert.Equal(2, columnInfo.TypeScale);
            Assert.Equal("DECIMAL(18,2)", columnInfo.TypeText);
        }

        [Fact]
        public void TestStatementParameterSerialization()
        {
            var param = new StatementParameter
            {
                Name = "date_param",
                Type = "DATE",
                Value = "2025-10-28"
            };

            string json = JsonSerializer.Serialize(param, _jsonOptions);
            var deserialized = JsonSerializer.Deserialize<StatementParameter>(json, _jsonOptions);

            Assert.NotNull(deserialized);
            Assert.Equal("date_param", deserialized.Name);
            Assert.Equal("DATE", deserialized.Type);
            Assert.Equal("2025-10-28", deserialized.Value?.ToString());
        }

        [Fact]
        public void TestGetStatementResponseDeserialization()
        {
            string json = @"{
                ""statement_id"": ""stmt-456"",
                ""status"": {
                    ""state"": ""RUNNING""
                }
            }";

            var response = JsonSerializer.Deserialize<GetStatementResponse>(json, _jsonOptions);

            Assert.NotNull(response);
            Assert.Equal("stmt-456", response.StatementId);
            Assert.NotNull(response.Status);
            Assert.Equal("RUNNING", response.Status.State);
        }
    }
}
