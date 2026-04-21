/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow.Adbc.Tracing;
using Apache.Hive.Service.Rpc.Thrift;

namespace AdbcDrivers.Databricks.Reader
{
    /// <summary>
    /// Service that periodically polls the operation status of a Databricks warehouse query to keep it alive.
    /// This is used to maintain the command results and session when reading results takes a long time.
    /// </summary>
    internal class DatabricksOperationStatusPoller : IOperationStatusPoller
    {
        private readonly IHiveServer2Statement _statement;
        private readonly int _heartbeatIntervalSeconds;
        private readonly int _requestTimeoutSeconds;
        private readonly IResponse _response;
        private readonly IActivityTracer? _activityTracer;
        // internal cancellation token source - won't affect the external token
        private CancellationTokenSource? _internalCts;
        private Task? _operationStatusPollingTask;

        // Telemetry tracking
        private int _pollCount = 0;

        // Maximum number of consecutive poll failures before giving up.
        // At the default 60s heartbeat interval this allows ~10 minutes of transient errors
        // before the poller stops itself.
        private const int MaxConsecutiveFailures = 10;

        public DatabricksOperationStatusPoller(
            IHiveServer2Statement statement,
            IResponse response,
            int heartbeatIntervalSeconds = DatabricksConstants.DefaultOperationStatusPollingIntervalSeconds,
            int requestTimeoutSeconds = DatabricksConstants.DefaultOperationStatusRequestTimeoutSeconds,
            IActivityTracer? activityTracer = null)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _response = response;
            _heartbeatIntervalSeconds = heartbeatIntervalSeconds;
            _requestTimeoutSeconds = requestTimeoutSeconds;
            _activityTracer = activityTracer;
        }

        public bool IsStarted => _operationStatusPollingTask != null;

        /// <summary>
        /// Starts the operation status poller. Continues polling every minute until the operation is canceled/errored
        /// the token is canceled, or the operation status poller is disposed.
        /// </summary>
        /// <param name="externalToken">The external cancellation token.</param>
        public void Start(CancellationToken externalToken = default)
        {
            if (IsStarted)
            {
                throw new InvalidOperationException("Operation status poller already started");
            }
            _internalCts = new CancellationTokenSource();
            // create a linked token to the external token so that the external token can cancel the operation status polling task if needed
            var linkedToken = CancellationTokenSource.CreateLinkedTokenSource(_internalCts.Token, externalToken).Token;
            _operationStatusPollingTask = Task.Run(async () =>
            {
                await PollOperationStatus(linkedToken).ConfigureAwait(false);
            });
        }

        private async Task PollOperationStatus(CancellationToken cancellationToken)
        {
            int consecutiveFailures = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                (bool shouldBreak, int newConsecutiveFailures) result;
                if (_activityTracer != null)
                {
                    result = await _activityTracer.Trace.TraceActivityAsync(
                        activity => PollOnceAsync(cancellationToken, activity, consecutiveFailures),
                        activityName: "PollOperationStatus").ConfigureAwait(false);
                }
                else
                {
                    result = await PollOnceAsync(cancellationToken, null, consecutiveFailures).ConfigureAwait(false);
                }

                consecutiveFailures = result.newConsecutiveFailures;
                if (result.shouldBreak) break;

                // Wait before next poll.
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // Normal shutdown
                    break;
                }
            }
        }

        /// <summary>
        /// Performs a single GetOperationStatus poll.
        /// Returns (shouldBreak, updatedConsecutiveFailures).
        /// </summary>
        private async Task<(bool shouldBreak, int consecutiveFailures)> PollOnceAsync(CancellationToken cancellationToken, Activity? activity, int consecutiveFailures)
        {
            try
            {
                TOperationHandle? operationHandle = _response.OperationHandle;
                if (operationHandle == null) return (true, consecutiveFailures);

                using CancellationTokenSource timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(TimeSpan.FromSeconds(_requestTimeoutSeconds));

                TGetOperationStatusReq request = new TGetOperationStatusReq(operationHandle);
                TGetOperationStatusResp response = await _statement.Client.GetOperationStatus(request, timeoutCts.Token).ConfigureAwait(false);

                // Successful poll — reset failure counter
                consecutiveFailures = 0;

                // Track poll count for telemetry
                _pollCount++;

                activity?.SetTag("poll_count", _pollCount);
                activity?.SetTag("operation_state", response.OperationState.ToString());

                // end the heartbeat if the command has terminated
                if (response.OperationState == TOperationState.CANCELED_STATE ||
                    response.OperationState == TOperationState.ERROR_STATE ||
                    response.OperationState == TOperationState.CLOSED_STATE ||
                    response.OperationState == TOperationState.TIMEDOUT_STATE ||
                    response.OperationState == TOperationState.UKNOWN_STATE)
                {
                    return (true, consecutiveFailures);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Cancellation was requested - exit the polling loop gracefully
                return (true, consecutiveFailures);
            }
            catch (Exception ex)
            {
                consecutiveFailures++;

                // Log the error but continue polling. Transient errors (e.g. ObjectDisposedException
                // from TLS connection recycling) should not kill the heartbeat poller, as that would
                // cause the server-side command inactivity timeout to expire and terminate the query.
                activity?.SetTag("error.type", ex.GetType().Name);
                activity?.SetTag("error.message", ex.Message);
                activity?.SetTag("poll_count", _pollCount);
                activity?.SetTag("consecutive_failures", consecutiveFailures);
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);

                if (consecutiveFailures >= MaxConsecutiveFailures)
                {
                    activity?.SetTag("max_failures_reached", true);
                    return (true, consecutiveFailures);
                }
            }

            return (false, consecutiveFailures);
        }

        public void Stop()
        {
            _internalCts?.Cancel();
        }

        public void Dispose()
        {
            _internalCts?.Cancel();
            try
            {
                _operationStatusPollingTask?.GetAwaiter().GetResult();
            }
            catch (OperationCanceledException)
            {
                // Expected, no-op
            }

            _internalCts?.Dispose();
            _internalCts = null;
            _operationStatusPollingTask = null;
        }
    }
}
