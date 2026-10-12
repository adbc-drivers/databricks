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

namespace AdbcDrivers.Databricks.StatementExecution.Metadata
{
    internal readonly struct SchemaRow
    {
        internal SchemaRow(string catalog, string schema)
        {
            Catalog = catalog;
            Schema = schema;
        }

        internal string Catalog { get; }
        internal string Schema { get; }
    }

    internal readonly struct TableRow
    {
        internal TableRow(string catalog, string schema, string table, string tableType, string remarks)
        {
            Catalog = catalog;
            Schema = schema;
            Table = table;
            TableType = tableType;
            Remarks = remarks;
        }

        internal string Catalog { get; }
        internal string Schema { get; }
        internal string Table { get; }
        internal string TableType { get; }
        internal string Remarks { get; }
    }

    internal readonly struct ColumnRow
    {
        private readonly int? _ordinal;

        // A null ordinal is numbered by MetadataRowReader from the row's position in its table.
        internal ColumnRow(string? catalog, string? schema, string? table, string name,
            string typeName, bool nullable, int? ordinal, string? columnDefault, bool isAutoIncrement)
        {
            Catalog = catalog;
            Schema = schema;
            Table = table;
            Name = name;
            TypeName = typeName;
            Nullable = nullable;
            _ordinal = ordinal;
            Default = columnDefault;
            IsAutoIncrement = isAutoIncrement;
        }

        internal string? Catalog { get; }
        internal string? Schema { get; }
        internal string? Table { get; }
        internal string Name { get; }
        internal string TypeName { get; }
        internal bool Nullable { get; }
        // Zero-based; see MetadataRowReader.GetObjectsOrdinalOffset.
        internal int Ordinal => _ordinal ?? 0;
        internal bool HasOrdinal => _ordinal.HasValue;
        internal string? Default { get; }
        internal bool IsAutoIncrement { get; }

        internal ColumnRow WithOrdinal(int ordinal)
            => new(Catalog, Schema, Table, Name, TypeName, Nullable, ordinal, Default, IsAutoIncrement);
    }

    internal readonly struct PrimaryKeyRow
    {
        internal PrimaryKeyRow(string catalog, string schema, string table, string column, int sequence, string name)
        {
            Catalog = catalog;
            Schema = schema;
            Table = table;
            Column = column;
            Sequence = sequence;
            Name = name;
        }

        internal string Catalog { get; }
        internal string Schema { get; }
        internal string Table { get; }
        internal string Column { get; }
        internal int Sequence { get; }
        internal string Name { get; }
    }

    internal readonly struct ForeignKeyRow
    {
        internal ForeignKeyRow(string parentCatalog, string parentSchema, string parentTable, string parentColumn,
            string catalog, string schema, string table, string column, int sequence,
            int updateRule, int deleteRule, string name, string? parentName, int deferrability)
        {
            ParentCatalog = parentCatalog;
            ParentSchema = parentSchema;
            ParentTable = parentTable;
            ParentColumn = parentColumn;
            Catalog = catalog;
            Schema = schema;
            Table = table;
            Column = column;
            Sequence = sequence;
            UpdateRule = updateRule;
            DeleteRule = deleteRule;
            Name = name;
            ParentName = parentName;
            Deferrability = deferrability;
        }

        internal string ParentCatalog { get; }
        internal string ParentSchema { get; }
        internal string ParentTable { get; }
        internal string ParentColumn { get; }
        internal string Catalog { get; }
        internal string Schema { get; }
        internal string Table { get; }
        internal string Column { get; }
        internal int Sequence { get; }
        internal int UpdateRule { get; }
        internal int DeleteRule { get; }
        internal string Name { get; }
        internal string? ParentName { get; }
        internal int Deferrability { get; }
    }
}
