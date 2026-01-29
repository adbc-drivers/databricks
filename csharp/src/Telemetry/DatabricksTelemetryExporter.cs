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
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry.Models;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Exports telemetry events to the Databricks telemetry service.
    /// </summary>
    /// <remarks>
    /// This exporter:
    /// - Creates TelemetryRequest wrapper with uploadTime and protoLogs
    /// - Uses /telemetry-ext for authenticated requests
    /// - Uses /telemetry-unauth for unauthenticated requests
    /// - Implements retry logic for transient failures
    /// - Never throws exceptions (all swallowed and logged at TRACE level)
    ///
    /// JDBC Reference: TelemetryPushClient.java
    /// </remarks>
    internal sealed class DatabricksTelemetryExporter : ITelemetryExporter
    {
        /// <summary>
        /// Authenticated telemetry endpoint path.
        /// </summary>
        internal const string AuthenticatedEndpoint = "/telemetry-ext";

        /// <summary>
        /// Unauthenticated telemetry endpoint path.
        /// </summary>
        internal const string UnauthenticatedEndpoint = "/telemetry-unauth";

        private readonly HttpClient _httpClient;
        private readonly string _host;
        private readonly bool _isAuthenticated;
        private readonly TelemetryConfiguration _config;

        private static readonly JsonSerializerOptions s_jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        /// <summary>
        /// Gets the host URL for the telemetry endpoint.
        /// </summary>
        internal string Host => _host;

        /// <summary>
        /// Gets whether this exporter uses authenticated endpoints.
        /// </summary>
        internal bool IsAuthenticated => _isAuthenticated;

        /// <summary>
        /// Creates a new DatabricksTelemetryExporter.
        /// </summary>
        /// <param name="httpClient">The HTTP client to use for sending requests.</param>
        /// <param name="host">The Databricks host URL.</param>
        /// <param name="isAuthenticated">Whether to use authenticated endpoints.</param>
        /// <param name="config">The telemetry configuration.</param>
        /// <exception cref="ArgumentNullException">Thrown when httpClient, host, or config is null.</exception>
        /// <exception cref="ArgumentException">Thrown when host is empty or whitespace.</exception>
        public DatabricksTelemetryExporter(
            HttpClient httpClient,
            string host,
            bool isAuthenticated,
            TelemetryConfiguration config)
        {
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));

            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            _host = host;
            _isAuthenticated = isAuthenticated;
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        /// <summary>
        /// Export telemetry frontend logs to the Databricks telemetry service.
        /// </summary>
        /// <param name="logs">The list of telemetry frontend logs to export.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the asynchronous export operation.</returns>
        /// <remarks>
        /// This method never throws exceptions. All errors are caught and logged at TRACE level.
        /// </remarks>
        public async Task ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
        {
            if (logs == null || logs.Count == 0)
            {
                return;
            }

            try
            {
                var request = CreateTelemetryRequest(logs);
                var json = SerializeRequest(request);

                await SendWithRetryAsync(json, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Don't swallow cancellation - let it propagate
                throw;
            }
            catch (Exception ex)
            {
                // Swallow all other exceptions per telemetry requirement
                // Log at TRACE level to avoid customer anxiety
                Debug.WriteLine($"[TRACE] DatabricksTelemetryExporter: Error exporting telemetry: {ex.Message}");
            }
        }

        /// <summary>
        /// Creates a TelemetryRequest from a list of frontend logs.
        /// </summary>
        internal TelemetryRequest CreateTelemetryRequest(IReadOnlyList<TelemetryFrontendLog> logs)
        {
            var protoLogs = new List<string>(logs.Count);

            foreach (var log in logs)
            {
                var serializedLog = JsonSerializer.Serialize(log, s_jsonOptions);
                protoLogs.Add(serializedLog);
            }

            return new TelemetryRequest
            {
                UploadTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                ProtoLogs = protoLogs
            };
        }

        /// <summary>
        /// Serializes the telemetry request to JSON.
        /// </summary>
        internal string SerializeRequest(TelemetryRequest request)
        {
            return JsonSerializer.Serialize(request, s_jsonOptions);
        }

        /// <summary>
        /// Gets the telemetry endpoint URL based on authentication status.
        /// </summary>
        /// <remarks>
        /// Returns a full URL with https:// scheme. The host parameter may be
        /// just a hostname or a full URL with scheme.
        /// </remarks>
        internal string GetEndpointUrl()
        {
            var endpoint = _isAuthenticated ? AuthenticatedEndpoint : UnauthenticatedEndpoint;
            var host = _host.TrimEnd('/');

            // Ensure the host has a scheme
            if (!host.StartsWith("https://", StringComparison.OrdinalIgnoreCase) &&
                !host.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            {
                host = $"https://{host}";
            }

            return $"{host}{endpoint}";
        }

        /// <summary>
        /// Sends the telemetry request with retry logic.
        /// </summary>
        private async Task SendWithRetryAsync(string json, CancellationToken ct)
        {
            var endpointUrl = GetEndpointUrl();
            Exception? lastException = null;

            for (int attempt = 0; attempt <= _config.MaxRetries; attempt++)
            {
                try
                {
                    if (attempt > 0 && _config.RetryDelayMs > 0)
                    {
                        await Task.Delay(_config.RetryDelayMs, ct).ConfigureAwait(false);
                    }

                    await SendRequestAsync(endpointUrl, json, ct).ConfigureAwait(false);

                    Debug.WriteLine($"[TRACE] DatabricksTelemetryExporter: Successfully exported telemetry to {endpointUrl}");
                    return;
                }
                catch (OperationCanceledException)
                {
                    // Don't retry on cancellation
                    throw;
                }
                catch (HttpRequestException ex)
                {
                    lastException = ex;

                    // Check if this is a terminal error that shouldn't be retried
                    if (ExceptionClassifier.IsTerminalException(ex))
                    {
                        Debug.WriteLine($"[TRACE] DatabricksTelemetryExporter: Terminal error, not retrying: {ex.Message}");
                        return;
                    }

                    Debug.WriteLine($"[TRACE] DatabricksTelemetryExporter: Attempt {attempt + 1}/{_config.MaxRetries + 1} failed: {ex.Message}");
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    Debug.WriteLine($"[TRACE] DatabricksTelemetryExporter: Attempt {attempt + 1}/{_config.MaxRetries + 1} failed: {ex.Message}");
                }
            }

            if (lastException != null)
            {
                Debug.WriteLine($"[TRACE] DatabricksTelemetryExporter: All retry attempts exhausted: {lastException.Message}");
            }
        }

        /// <summary>
        /// Sends the HTTP request to the telemetry endpoint.
        /// </summary>
        private async Task SendRequestAsync(string endpointUrl, string json, CancellationToken ct)
        {
            using var content = new StringContent(json, Encoding.UTF8, "application/json");
            using var response = await _httpClient.PostAsync(endpointUrl, content, ct).ConfigureAwait(false);

            response.EnsureSuccessStatusCode();
        }
    }
}
