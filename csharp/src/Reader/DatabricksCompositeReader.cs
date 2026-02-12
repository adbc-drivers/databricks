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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using Apache.Arrow;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow.Adbc.Tracing;
using Apache.Hive.Service.Rpc.Thrift;

namespace AdbcDrivers.Databricks.Reader
{
    /// <summary>
    /// A composite reader for Databricks that delegates to either CloudFetchReader or DatabricksReader
    /// based on CloudFetch configuration and result set characteristics. This was introduced because some
    /// older DBR do not accurately report the result set characteristics in the MetadataResponse
    /// </summary>
    internal class DatabricksCompositeReader : TracingReader
    {
        public override string AssemblyName => DatabricksConnection.s_assemblyName;

        public override string AssemblyVersion => DatabricksConnection.s_assemblyVersion;

        public override Schema Schema { get { return _schema; } }

        private BaseDatabricksReader? _activeReader;
        private readonly IHiveServer2Statement _statement;
        private readonly Schema _schema;
        private readonly IResponse _response;
        private readonly bool _isLz4Compressed;

        private IOperationStatusPoller? operationStatusPoller;
        private bool _disposed;
        private readonly HttpClient _httpClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabricksCompositeReader"/> class.
        /// </summary>
        /// <param name="statement">The Databricks statement.</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="isLz4Compressed">Whether the results are LZ4 compressed.</param>
        /// <param name="httpClient">The HTTP client for CloudFetch operations.</param>
        internal DatabricksCompositeReader(
            IHiveServer2Statement statement,
            Schema schema,
            IResponse response,
            bool isLz4Compressed,
            HttpClient httpClient,
            IOperationStatusPoller? operationPoller = null)
            : base(statement)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _response = response;
            _isLz4Compressed = isLz4Compressed;
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));

            // use direct results if available
            if (_statement.TryGetDirectResults(_response, out TSparkDirectResults? directResults)
                && directResults!.__isset.resultSet
                && directResults.ResultSet != null)
            {
                _activeReader = DetermineReader(directResults.ResultSet);
            }
            if (_response.DirectResults?.ResultSet?.HasMoreRows ?? true)
            {
                operationStatusPoller = operationPoller ?? new DatabricksOperationStatusPoller(_statement, response, GetHeartbeatIntervalFromConnection(), GetRequestTimeoutFromConnection());
                operationStatusPoller.Start();
            }
        }

        /// <summary>
        /// Determines whether CloudFetch should be used based on the fetch results.
        /// </summary>
        /// <param name="initialResults">The initial fetch results.</param>
        /// <returns>True if CloudFetch should be used, false otherwise.</returns>
        internal static bool ShouldUseCloudFetch(TFetchResultsResp initialResults)
        {
            return initialResults.__isset.results &&
                   initialResults.Results.__isset.resultLinks &&
                   initialResults.Results.ResultLinks?.Count > 0;
        }

        private BaseDatabricksReader DetermineReader(TFetchResultsResp initialResults, Activity? activity = null)
        {
            bool useCloudFetch = ShouldUseCloudFetch(initialResults);
            activity?.AddEvent("composite_reader.determine_reader", [
                new("use_cloudfetch", useCloudFetch),
                new("has_result_links", initialResults.__isset.results && initialResults.Results.__isset.resultLinks),
                new("result_links_count", initialResults.Results?.ResultLinks?.Count ?? 0)
            ]);

            if (useCloudFetch)
            {
                return CreateCloudFetchReader(initialResults);
            }
            else
            {
                return CreateDatabricksReader(initialResults);
            }
        }

        /// <summary>
        /// Reads the next record batch from the active reader.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <param name="activity">The activity for logging.</param>
        /// <returns>The next record batch, or null if there are no more batches.</returns>
        private async ValueTask<RecordBatch?> ReadNextRecordBatchInternalAsync(CancellationToken cancellationToken, Activity? activity)
        {
            // Initialize the active reader if not already done
            if (_activeReader == null)
            {
                activity?.AddEvent("composite_reader.initializing_reader");
                // if no reader, we did not have direct results
                // Make a FetchResults call to get the initial result set
                // and determine the reader based on the result set
                TFetchResultsReq request = new TFetchResultsReq(_response.OperationHandle!, TFetchOrientation.FETCH_NEXT, this._statement.BatchSize);

                // Set MaxBytes from DatabricksStatement
                if (this._statement is DatabricksStatement databricksStatement)
                {
                    request.MaxBytes = databricksStatement.MaxBytesPerFetchRequest;
                }

                TFetchResultsResp response = await this._statement.Client!.FetchResults(request, cancellationToken);
                _activeReader = DetermineReader(response, activity);
                activity?.AddEvent("composite_reader.reader_determined", [
                    new("reader_type", _activeReader.GetType().Name)
                ]);
            }

            return await _activeReader.ReadNextRecordBatchAsync(cancellationToken);
        }

        /// <summary>
        /// Creates a CloudFetchReader instance using the factory.
        /// Virtual to allow testing.
        /// </summary>
        /// <param name="initialResults">The initial fetch results.</param>
        /// <returns>A new CloudFetchReader instance.</returns>
        protected virtual BaseDatabricksReader CreateCloudFetchReader(TFetchResultsResp initialResults)
        {
            return CloudFetchReaderFactory.CreateThriftReader(
                _statement,
                _schema,
                _response,
                initialResults,
                _httpClient,
                _isLz4Compressed);
        }

        /// <summary>
        /// Creates a DatabricksReader instance. Virtual to allow testing.
        /// </summary>
        /// <param name="initialResults">The initial fetch results.</param>
        /// <returns>A new DatabricksReader instance.</returns>
        protected virtual BaseDatabricksReader CreateDatabricksReader(TFetchResultsResp initialResults)
        {
            return new DatabricksReader(_statement, _schema, _response, initialResults, _isLz4Compressed);
        }

        public override async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (_activeReader != null)
                {
                    activity?.SetTag("reader.active_reader_type", _activeReader.GetType().Name);
                }

                var result = await ReadNextRecordBatchInternalAsync(cancellationToken, activity);
                // Stop the poller when we've reached the end of results
                if (result == null)
                {
                    activity?.AddEvent("composite_reader.end_of_results");
                    StopOperationStatusPoller();
                }
                else
                {
                    activity?.AddEvent("composite_reader.batch_read", [
                        new("row_count", result.Length)
                    ]);
                }
                return result;
            });
        }

        protected override void Dispose(bool disposing)
        {
            this.TraceActivity(activity =>
            {
                if (_activeReader != null)
                {
                    activity?.SetTag("reader.active_reader_type", _activeReader.GetType().Name);
                }

                try
                {
                    if (!_disposed)
                    {
                        if (disposing)
                        {
                            activity?.AddEvent("composite_reader.disposing");
                            StopOperationStatusPoller();
                            if (_activeReader == null)
                            {
                                activity?.AddEvent("composite_reader.close_operation_no_reader");
                                _ = HiveServer2Reader.CloseOperationAsync(_statement, _response)
                                    .ConfigureAwait(false).GetAwaiter().GetResult();
                            }
                            else
                            {
                                activity?.AddEvent("composite_reader.disposing_active_reader", [
                                    new("reader_type", _activeReader.GetType().Name)
                                ]);
                                // Note: Have the contained reader close the operation to avoid duplicate calls.
                                _activeReader.Dispose();
                                _activeReader = null;
                            }
                            activity?.AddEvent("composite_reader.disposed");
                        }
                    }
                }
                finally
                {
                    base.Dispose(disposing);
                    _disposed = true;
                }
            }, activityName: nameof(DatabricksCompositeReader) + "." + nameof(Dispose));
        }

        private void StopOperationStatusPoller()
        {
            operationStatusPoller?.Stop();
            operationStatusPoller?.Dispose();
            operationStatusPoller = null;
        }

        /// <summary>
        /// Gets the heartbeat interval from the statement's connection.
        /// </summary>
        /// <returns>The heartbeat interval in seconds, or default if not available.</returns>
        private int GetHeartbeatIntervalFromConnection()
        {
            if (_statement is DatabricksStatement databricksStatement)
            {
                var connection = databricksStatement.Connection;
                if (connection is DatabricksConnection databricksConnection)
                {
                    return databricksConnection.FetchHeartbeatIntervalSeconds;
                }
            }

            return DatabricksConstants.DefaultOperationStatusPollingIntervalSeconds;
        }

        /// <summary>
        /// Gets the request timeout from the statement's connection.
        /// </summary>
        /// <returns>The request timeout in seconds, or default if not available.</returns>
        private int GetRequestTimeoutFromConnection()
        {
            if (_statement is DatabricksStatement databricksStatement)
            {
                var connection = databricksStatement.Connection;
                if (connection is DatabricksConnection databricksConnection)
                {
                    return databricksConnection.OperationStatusRequestTimeoutSeconds;
                }
            }

            return DatabricksConstants.DefaultOperationStatusRequestTimeoutSeconds;
        }
    }
}
