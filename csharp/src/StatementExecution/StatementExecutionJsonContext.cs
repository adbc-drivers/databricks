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

using System.Text.Json.Serialization;

namespace AdbcDrivers.Databricks.StatementExecution
{
    /// <summary>
    /// System.Text.Json source-generated metadata for the Statement Execution API (SEA)
    /// request and response models. Routing the (de)serialization calls through this context
    /// makes them trim- and NativeAOT-safe (no reflection-based serialization), which is why
    /// the driver builds cleanly under <c>IsAotCompatible</c> on net10.0.
    ///
    /// The options mirror the previously-used runtime <c>JsonSerializerOptions</c>: null
    /// properties are omitted on write. Nested model types (status, manifest, schema, chunks,
    /// links, parameters, …) are pulled in automatically by the generator.
    /// </summary>
    [JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonSerializable(typeof(CreateSessionRequest))]
    [JsonSerializable(typeof(CreateSessionResponse))]
    [JsonSerializable(typeof(ExecuteStatementRequest))]
    [JsonSerializable(typeof(ExecuteStatementResponse))]
    [JsonSerializable(typeof(GetStatementResponse))]
    [JsonSerializable(typeof(ResultData))]
    [JsonSerializable(typeof(ServiceError))]
    internal partial class StatementExecutionJsonContext : JsonSerializerContext
    {
    }
}
