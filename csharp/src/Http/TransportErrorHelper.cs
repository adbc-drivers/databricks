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
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Http
{
    /// <summary>
    /// Shared utility for classifying transient transport-level errors.
    /// Used by both RetryHttpHandler and CloudFetchDownloader.
    /// </summary>
    internal static class TransportErrorHelper
    {
        /// <summary>
        /// Determines if an exception represents a transient transport-level error
        /// that should be retried (e.g., connection reset, DNS failure, TCP errors).
        /// Excludes user-initiated cancellations.
        /// </summary>
        public static bool IsTransientTransportException(Exception ex, CancellationToken cancellationToken)
        {
            // Never retry if the caller explicitly cancelled
            if (cancellationToken.IsCancellationRequested)
            {
                return false;
            }

            // HttpRequestException: connection refused, DNS failure, TCP reset, etc.
            if (ex is HttpRequestException)
            {
                return true;
            }

            // IOException: connection dropped mid-transfer
            if (ex is IOException)
            {
                return true;
            }

            // SocketException: low-level network errors (wrapped or standalone)
            if (ex is SocketException)
            {
                return true;
            }

            // TaskCanceledException NOT caused by the caller's token.
            // Only treat as transient if the associated token has actually been canceled.
            if (ex is TaskCanceledException tce
                && tce.CancellationToken != cancellationToken
                && tce.CancellationToken.CanBeCanceled
                && tce.CancellationToken.IsCancellationRequested)
            {
                return true;
            }

            // Check inner exceptions — transport errors are often wrapped
            if (ex.InnerException != null)
            {
                return IsTransientTransportException(ex.InnerException, cancellationToken);
            }

            return false;
        }
    }
}
