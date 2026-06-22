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
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Raised when the server-side session backing a connection has been closed or has
    /// expired (e.g. due to an inactivity timeout) and the connection can no longer be used.
    ///
    /// The server reports this as HTTP 400 with a Thrift error message containing
    /// "Invalid SessionHandle". Without this typed exception the condition surfaces as a
    /// generic, misleading "An unexpected error occurred while fetching results / Couldn't
    /// connect to server" error, which makes it impossible for callers to distinguish a
    /// recoverable "reconnect" situation from a genuine network/transport failure.
    ///
    /// Callers that catch this exception should dispose the connection and open a new one.
    /// </summary>
    public class DatabricksSessionExpiredException : DatabricksException
    {
        /// <summary>
        /// Substring present in the server's error message for a closed/expired session.
        /// Both the inactivity-timeout variant ("Invalid SessionHandle: Session [..] is closed")
        /// and the explicitly-closed variant ("Invalid SessionHandle: SessionHandle [..]")
        /// contain this phrase.
        /// </summary>
        internal const string ServerErrorSignature = "Invalid SessionHandle";

        /// <summary>
        /// Message used when failing fast on a connection already known to have an invalid session.
        /// </summary>
        internal const string FastFailMessage =
            "The Databricks session has expired or was closed by the server and is no longer usable. " +
            "Open a new connection to continue.";

        public DatabricksSessionExpiredException(string message)
            : base(message, AdbcStatusCode.InvalidState)
        {
        }

        public DatabricksSessionExpiredException(string message, Exception innerException)
            : base(message, AdbcStatusCode.InvalidState, innerException)
        {
        }

        /// <summary>
        /// Determines whether the given exception (or any exception in its inner / aggregate
        /// chain) represents a closed or expired server-side session.
        /// </summary>
        internal static bool IsSessionExpired(Exception? exception)
        {
            switch (exception)
            {
                case null:
                    return false;
                case DatabricksSessionExpiredException:
                    return true;
                case AggregateException aggregate:
                    foreach (Exception inner in aggregate.InnerExceptions)
                    {
                        if (IsSessionExpired(inner))
                        {
                            return true;
                        }
                    }
                    return false;
            }

            if (exception.Message != null &&
                exception.Message.IndexOf(ServerErrorSignature, StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return true;
            }

            return IsSessionExpired(exception.InnerException);
        }
    }
}
