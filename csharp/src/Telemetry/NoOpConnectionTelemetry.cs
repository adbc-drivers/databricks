/*
* Copyright (c) 2025 ADBC Drivers Contributors
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
using System.Diagnostics;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// No-op telemetry implementation used when telemetry is disabled or initialization fails.
    /// </summary>
    internal sealed class NoOpConnectionTelemetry : IConnectionTelemetry
    {
        public static readonly NoOpConnectionTelemetry Instance = new NoOpConnectionTelemetry();

        public TelemetrySessionContext? Session => null;

        public T ExecuteWithMetadataTelemetry<T>(
            Proto.Operation.Types.Type operationType,
            Func<T> operation,
            Activity? activity)
        {
            return operation();
        }

        public Task DisposeAsync() => Task.CompletedTask;
    }
}
