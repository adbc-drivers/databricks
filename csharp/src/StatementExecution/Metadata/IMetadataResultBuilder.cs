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
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Databricks.StatementExecution.Metadata
{
    // Builds the flat, Thrift-shaped result of each metadata operation.
    internal interface IMetadataResultBuilder
    {
        QueryResult Catalogs(MetadataBatches result);

        QueryResult Schemas(MetadataBatches result);

        QueryResult Tables(MetadataBatches result, IReadOnlyCollection<string>? tableTypes);

        // Native results pass the server columns through; legacy results are built from the rows.
        QueryResult Columns(IReadOnlyList<MetadataBatches> results, IReadOnlyList<ColumnRow> rows);

        // SHOW KEYS rows fall back to the requested table for identifiers they omit.
        QueryResult PrimaryKeys(MetadataBatches result, string catalog, string schema, string table);

        // GetCrossReference: keeps keys whose parent matches every non-null parent identifier.
        QueryResult ForeignKeys(MetadataBatches result,
            string? parentCatalog, string? parentSchema, string? parentTable,
            string catalog, string schema, string table);
    }
}
