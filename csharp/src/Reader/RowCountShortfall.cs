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

namespace AdbcDrivers.Databricks.Reader
{
    /// <summary>
    /// Shared handling for results that decode to fewer rows than the server declared
    /// (see <see cref="DatabricksParameters.FailOnRowCountMismatch"/>).
    /// </summary>
    internal static class RowCountShortfall
    {
        /// <summary>
        /// Whether a shortfall fails the read. Defaults to false; throws <see cref="System.ArgumentException"/>
        /// if the property is set to something other than true/false.
        /// </summary>
        internal static bool IsFailEnabled(IReadOnlyDictionary<string, string>? properties) =>
            properties != null &&
            PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.FailOnRowCountMismatch, false);

        /// <summary>The error thrown for a shortfall when <see cref="IsFailEnabled"/> is true.</summary>
        internal static DatabricksException CreateException(long expectedRows, long rowsRead) =>
            new DatabricksException(
                $"Result has fewer rows than the server declared: expected {expectedRows}, read {rowsRead}. " +
                $"Set {DatabricksParameters.FailOnRowCountMismatch}=false to return the partial result instead.",
                AdbcStatusCode.InvalidData);
    }
}
