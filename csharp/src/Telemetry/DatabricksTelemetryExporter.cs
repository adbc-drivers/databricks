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

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Exports telemetry metrics to Databricks telemetry service via HTTP POST.
    /// Supports both authenticated (/telemetry-ext) and unauthenticated (/telemetry-unauth) endpoints.
    /// Implements retry logic for transient failures with configurable max retries and delay.
    /// All exceptions are swallowed and logged at TRACE level per design requirement.
    /// </summary>
    internal sealed class DatabricksTelemetryExporter : ITelemetryExporter
    {
        private const string AuthenticatedEndpoint = "/telemetry-ext";
        private const string UnauthenticatedEndpoint = "/telemetry-unauth";

        private readonly HttpClient _httpClient;
        private readonly string _host;
        private readonly bool _isAuthenticated;
        private readonly TelemetryConfiguration _config;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabricksTelemetryExporter"/> class.
        /// </summary>
        /// <param name="httpClient">HttpClient for making HTTP requests.</param>
        /// <param name="host">Databricks host (e.g., "workspace.cloud.databricks.com").</param>
        /// <param name="isAuthenticated">Whether the connection is authenticated.</param>
        /// <param name="config">Telemetry configuration.</param>
        public DatabricksTelemetryExporter(
            HttpClient httpClient,
            string host,
            bool isAuthenticated,
            TelemetryConfiguration config)
        {
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _host = host ?? throw new ArgumentNullException(nameof(host));
            _isAuthenticated = isAuthenticated;
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        /// <summary>
        /// Exports telemetry metrics to Databricks service.
        /// This method never throws exceptions. All errors are caught, logged at TRACE level, and swallowed.
        /// </summary>
        /// <param name="metrics">List of telemetry metrics to export.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>Task representing the async export operation.</returns>
        public async Task ExportAsync(IReadOnlyList<TelemetryMetric> metrics, CancellationToken ct = default)
        {
            if (metrics == null || metrics.Count == 0)
            {
                return;
            }

            try
            {
                await ExportWithRetryAsync(metrics, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per design requirement
                // Log at TRACE level only to avoid customer anxiety
                Debug.WriteLine($"[TRACE] Telemetry export error: {ex.Message}");
            }
        }

        /// <summary>
        /// Exports metrics with retry logic for transient failures.
        /// </summary>
        private async Task ExportWithRetryAsync(IReadOnlyList<TelemetryMetric> metrics, CancellationToken ct)
        {
            var endpoint = _isAuthenticated ? AuthenticatedEndpoint : UnauthenticatedEndpoint;
            var url = $"https://{_host}{endpoint}";

            // Serialize metrics to JSON
            var json = SerializeMetrics(metrics);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            int attempt = 0;
            int maxAttempts = _config.MaxRetries + 1; // MaxRetries is the number of retries, so total attempts = retries + 1

            while (attempt < maxAttempts)
            {
                try
                {
                    ct.ThrowIfCancellationRequested();

                    var response = await _httpClient.PostAsync(url, content, ct).ConfigureAwait(false);

                    if (response.IsSuccessStatusCode)
                    {
                        // Success - return immediately
                        return;
                    }

                    // Check if this is a transient failure that should be retried
                    var statusCode = (int)response.StatusCode;
                    bool isTransientFailure = IsTransientStatusCode(statusCode);

                    if (isTransientFailure && attempt < maxAttempts - 1)
                    {
                        // Transient failure - retry after delay
                        Debug.WriteLine($"[TRACE] Telemetry export transient failure (status {statusCode}), attempt {attempt + 1}/{maxAttempts}");
                        await Task.Delay(_config.RetryDelayMs, ct).ConfigureAwait(false);
                        attempt++;
                        continue;
                    }

                    // Non-transient failure or max retries reached - log and return
                    Debug.WriteLine($"[TRACE] Telemetry export failed with status {statusCode}, attempt {attempt + 1}/{maxAttempts}");
                    return;
                }
                catch (OperationCanceledException)
                {
                    // Cancellation requested - exit gracefully without logging
                    return;
                }
                catch (Exception ex) when (IsTransientException(ex))
                {
                    // Transient exception (timeout, network error, etc.) - retry if attempts remain
                    if (attempt < maxAttempts - 1)
                    {
                        Debug.WriteLine($"[TRACE] Telemetry export transient exception: {ex.Message}, attempt {attempt + 1}/{maxAttempts}");
                        await Task.Delay(_config.RetryDelayMs, ct).ConfigureAwait(false);
                        attempt++;
                        continue;
                    }

                    // Max retries reached
                    Debug.WriteLine($"[TRACE] Telemetry export max retries reached after exception: {ex.Message}");
                    return;
                }
                catch (Exception ex)
                {
                    // Non-transient exception - log and return without retry
                    Debug.WriteLine($"[TRACE] Telemetry export non-transient exception: {ex.Message}");
                    return;
                }
            }
        }

        /// <summary>
        /// Serializes metrics to JSON format expected by Databricks telemetry service.
        /// </summary>
        private string SerializeMetrics(IReadOnlyList<TelemetryMetric> metrics)
        {
            var options = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false
            };

            return JsonSerializer.Serialize(metrics, options);
        }

        /// <summary>
        /// Checks if an HTTP status code represents a transient failure that should be retried.
        /// </summary>
        /// <param name="statusCode">HTTP status code.</param>
        /// <returns>True if the status code represents a transient failure.</returns>
        private bool IsTransientStatusCode(int statusCode)
        {
            return statusCode switch
            {
                429 => true,  // Too Many Requests - rate limiting
                503 => true,  // Service Unavailable
                500 => true,  // Internal Server Error (may be transient)
                502 => true,  // Bad Gateway (proxy/gateway error)
                504 => true,  // Gateway Timeout
                _ => false
            };
        }

        /// <summary>
        /// Checks if an exception represents a transient failure that should be retried.
        /// </summary>
        /// <param name="ex">Exception to check.</param>
        /// <returns>True if the exception represents a transient failure.</returns>
        private bool IsTransientException(Exception ex)
        {
            // Network errors, timeouts are transient
            return ex is HttpRequestException ||
                   ex is TaskCanceledException ||
                   ex is TimeoutException;
        }
    }
}
