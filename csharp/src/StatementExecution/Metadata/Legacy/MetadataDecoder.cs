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
using Apache.Arrow;

namespace AdbcDrivers.Databricks.StatementExecution.Metadata.Legacy
{
    // Decodes SHOW command results, which the server has already filtered.
    // Each operation lives in the file of the same name, mirroring Native/.
    internal sealed partial class MetadataDecoder : IMetadataDecoder
    {
        internal static readonly MetadataDecoder Instance = new();

        private MetadataDecoder() { }

        private static string? String(StringArray? values, int row)
            => values == null || values.IsNull(row) ? null : values.GetString(row);

        private static int? Integer(Int32Array? values, int row)
            => values == null || values.IsNull(row) ? null : values.GetValue(row);

        private static T? TryGetColumn<T>(RecordBatch batch, string name) where T : class, IArrowArray
        {
            try
            {
                return batch.Column(name) as T;
            }
            catch (ArgumentOutOfRangeException)
            {
                return null;
            }
        }
    }
}
