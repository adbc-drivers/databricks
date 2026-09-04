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

using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// System.Text.Json source-generated metadata for the free-form configuration file, which is
    /// read as a flat <see cref="Dictionary{TKey,TValue}"/> of string-to-string. Using the
    /// generated context keeps the deserialization trim- and NativeAOT-safe. The options mirror
    /// the prior runtime options: case-insensitive property names, comments skipped, and trailing
    /// commas allowed.
    /// </summary>
    [JsonSourceGenerationOptions(
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true)]
    [JsonSerializable(typeof(Dictionary<string, string>))]
    internal partial class DatabricksConfigJsonContext : JsonSerializerContext
    {
    }
}
