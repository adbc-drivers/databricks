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

using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;

namespace AdbcDrivers.Databricks.StatementExecution
{
    /// <summary>
    /// Maps a SEA metadata-command keyword (the ApacheParameters.IsMetadataCommand SqlQuery
    /// value, e.g. <c>"getcatalogs"</c>) to the corresponding telemetry-proto
    /// <see cref="OperationType"/> (e.g. <see cref="OperationType.ListCatalogs"/>).
    ///
    /// <para>
    /// Mirrors the Thrift-side helper <c>DatabricksStatement.GetMetadataOperationType</c>
    /// so SEA emits the same <c>StatementType.Metadata</c> + <c>OperationType.List*</c>
    /// pairs the Thrift path produces — see PECO-3022 gap B8. Used by
    /// <see cref="StatementExecutionStatement.ExecuteMetadataCommandAsync"/> when routing
    /// a metadata command into <c>StatementExecutionConnection.ExecuteMetadataSqlAsync</c>,
    /// and by the connection-level metadata data-provider methods that drive
    /// <c>GetObjects</c> / <c>GetTableTypes</c>.
    /// </para>
    ///
    /// <para>Pure-function; safe to call from any thread.</para>
    /// </summary>
    internal static class SeaMetadataOperationMapper
    {
        /// <summary>
        /// Returns the telemetry <see cref="OperationType"/> for a metadata command keyword,
        /// or <c>null</c> when <paramref name="sqlQuery"/> is not a recognized metadata
        /// command (in which case callers should fall back to the regular query operation
        /// type, e.g. <see cref="OperationType.ExecuteStatementAsync"/>).
        /// </summary>
        /// <param name="sqlQuery">The statement's SqlQuery when
        /// <c>ApacheParameters.IsMetadataCommand=true</c>; case-insensitive.</param>
        public static OperationType? Map(string? sqlQuery)
        {
            return sqlQuery?.ToLowerInvariant() switch
            {
                "getcatalogs" => OperationType.ListCatalogs,
                "getschemas" => OperationType.ListSchemas,
                "gettables" => OperationType.ListTables,
                "getcolumns" or "getcolumnsextended" => OperationType.ListColumns,
                "gettabletypes" => OperationType.ListTableTypes,
                "getprimarykeys" => OperationType.ListPrimaryKeys,
                "getcrossreference" => OperationType.ListCrossReferences,
                _ => null
            };
        }
    }
}
