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

namespace AdbcDrivers.Databricks.StatementExecution.Metadata
{
    // Decodes one metadata response into format-independent rows. Use MetadataRowReader,
    // which picks the decoder and applies the rules shared by both formats.
    internal interface IMetadataDecoder
    {
        IEnumerable<string> Catalogs(MetadataBatches result);

        IEnumerable<SchemaRow> Schemas(MetadataBatches result);

        // Keeps rows whose defaulted table type is in tableTypes; null keeps all.
        IEnumerable<TableRow> Tables(MetadataBatches result, IReadOnlyCollection<string>? tableTypes);

        IEnumerable<ColumnRow> Columns(MetadataBatches result);
    }
}
