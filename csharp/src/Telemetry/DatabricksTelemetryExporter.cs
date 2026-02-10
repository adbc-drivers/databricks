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
using Polly;
using Polly.Retry;

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
    /// - Never throws exceptions (all swallowed and traced at Verbose level)
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

        /// <summary>
        /// Activity source for telemetry exporter tracing.
        /// </summary>
        private static readonly ActivitySource s_activitySource = new ActivitySource("AdbcDrivers.Databricks.TelemetryExporter");

        private readonly HttpClient _httpClient;
        private readonly string _host;
        private readonly bool _isAuthenticated;
        private readonly TelemetryConfiguration _config;
        private readonly ResiliencePipeline _retryPipeline;

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
            _retryPipeline = CreateRetryPipeline(config);
        }

        /// <summary>
        /// Creates the Polly retry pipeline with configured retry settings.
        /// </summary>
        private static ResiliencePipeline CreateRetryPipeline(TelemetryConfiguration config)
        {
            // If no retries configured, return an empty pipeline (no-op)
            if (config.MaxRetries <= 0)
            {
                return ResiliencePipeline.Empty;
            }

            var retryOptions = new RetryStrategyOptions
            {
                MaxRetryAttempts = config.MaxRetries,
                Delay = TimeSpan.FromMilliseconds(config.RetryDelayMs),
                BackoffType = DelayBackoffType.Constant,
                ShouldHandle = new PredicateBuilder()
                    .Handle<Exception>(ex =>
                        ex is not OperationCanceledException &&
                        !ExceptionClassifier.IsTerminalException(ex)),
                OnRetry = args =>
                {
                    Activity.Current?.AddEvent(new ActivityEvent("telemetry.export.retry",
                        tags: new ActivityTagsCollection
                        {
                            { "attempt", args.AttemptNumber + 1 },
                            { "max_attempts", config.MaxRetries + 1 },
                            { "error.message", args.Outcome.Exception?.Message ?? "Unknown" },
                            { "error.type", args.Outcome.Exception?.GetType().Name ?? "Unknown" }
                        }));
                    return default;
                }
            };

            return new ResiliencePipelineBuilder()
                .AddRetry(retryOptions)
                .Build();
        }

        /// <summary>
        /// Export telemetry frontend logs to the Databricks telemetry service.
        /// </summary>
        /// <param name="logs">The list of telemetry frontend logs to export.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>
        /// True if the export succeeded (HTTP 2xx response), false if it failed.
        /// Returns true for empty/null logs since there's nothing to export.
        /// </returns>
        /// <remarks>
        /// This method never throws exceptions. All errors are caught and traced using ActivitySource.
        /// </remarks>
        public async Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
        {
            if (logs == null || logs.Count == 0)
            {
                return true;
            }

            try
            {
                var request = CreateTelemetryRequest(logs);
                var json = SerializeRequest(request);

                return await SendWithRetryAsync(json, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Don't swallow cancellation - let it propagate
                throw;
            }
            catch (Exception ex)
            {
                // Swallow all other exceptions per telemetry requirement
                // Trace at Verbose level to avoid customer anxiety
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.export.error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));
                return false;
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
        internal string GetEndpointUrl()
        {
            var endpoint = _isAuthenticated ? AuthenticatedEndpoint : UnauthenticatedEndpoint;
            var host = _host.TrimEnd('/');
            return $"{host}{endpoint}";
        }

        /// <summary>
        /// Sends the telemetry request with retry logic using Polly.
        /// </summary>
        /// <returns>True if the request succeeded, false otherwise.</returns>
        private async Task<bool> SendWithRetryAsync(string json, CancellationToken ct)
        {
            var endpointUrl = GetEndpointUrl();

            try
            {
                await _retryPipeline.ExecuteAsync(async token =>
                {
                    await SendRequestAsync(endpointUrl, json, token).ConfigureAwait(false);
                }, ct).ConfigureAwait(false);

                Activity.Current?.AddEvent(new ActivityEvent("telemetry.export.success",
                    tags: new ActivityTagsCollection
                    {
                        { "endpoint", endpointUrl }
                    }));
                return true;
            }
            catch (OperationCanceledException)
            {
                // Don't swallow cancellation
                throw;
            }
            catch (HttpRequestException ex) when (ExceptionClassifier.IsTerminalException(ex))
            {
                // Terminal error - don't retry (this shouldn't be reached since ShouldHandle excludes it,
                // but kept as a safety net)
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.export.terminal_error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));
                return false;
            }
            catch (Exception ex)
            {
                // All retries exhausted
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.export.exhausted",
                    tags: new ActivityTagsCollection
                    {
                        { "total_attempts", _config.MaxRetries + 1 },
                        { "error.message", ex.Message },
                        { "error.type", ex.GetType().Name }
                    }));
                return false;
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
