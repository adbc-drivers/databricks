/*
* Copyright (c) 2025 ADBC Drivers Contributors
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
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ProxyControlApi.Api;
using ProxyControlApi.Client;

namespace AdbcDrivers.Databricks.Tests.ThriftProtocol
{
    /// <summary>
    /// Simplified wrapper around the OpenAPI-generated ProxyControlApi client.
    /// Provides a clean interface for test code without requiring full DI setup.
    ///
    /// This wrapper manually instantiates the generated client dependencies to avoid
    /// requiring Microsoft.Extensions.Hosting setup in test code.
    /// </summary>
    public class ProxyControlClient : IDisposable
    {
        private readonly DefaultApi _api;
        private readonly HttpClient _httpClient;
        private readonly ILoggerFactory _loggerFactory;
        private bool _disposed;

        public ProxyControlClient(int apiPort = 18081)
        {
            _httpClient = new HttpClient
            {
                BaseAddress = new Uri($"http://localhost:{apiPort}"),
                Timeout = TimeSpan.FromSeconds(5)
            };

            // Create minimal dependencies for the generated client
            _loggerFactory = NullLoggerFactory.Instance;
            var logger = _loggerFactory.CreateLogger<DefaultApi>();
            var jsonOptions = new System.Text.Json.JsonSerializerOptions();

            // Register custom JSON converters for proper serialization
            jsonOptions.Converters.Add(new ProxyControlApi.Model.ThriftVerificationRequestJsonConverter());
            jsonOptions.Converters.Add(new ProxyControlApi.Model.ThriftVerificationResultJsonConverter());
            jsonOptions.Converters.Add(new ProxyControlApi.Model.ThriftCallHistoryJsonConverter());

            var jsonProvider = new JsonSerializerOptionsProvider(jsonOptions);
            var events = new DefaultApiEvents();

            _api = new DefaultApi(logger, _loggerFactory, _httpClient, jsonProvider, events);
        }

        /// <summary>
        /// Lists all available failure scenarios and their current status.
        /// </summary>
        public async Task<List<FailureScenarioStatus>> ListScenariosAsync(CancellationToken cancellationToken = default)
        {
            var response = await _api.ListScenariosAsync(cancellationToken);

            if (!response.IsOk)
            {
                throw new InvalidOperationException($"Failed to list scenarios. Status: {response.StatusCode}");
            }

            var scenarioList = response.Ok();
            if (scenarioList?.Scenarios == null)
            {
                return new List<FailureScenarioStatus>();
            }

            return scenarioList.Scenarios.Select(s => new FailureScenarioStatus
            {
                Name = s.Name ?? string.Empty,
                Description = s.Description ?? string.Empty,
                Enabled = s.Enabled
            }).ToList();
        }

        /// <summary>
        /// Enables a failure scenario by name.
        /// </summary>
        public async Task<bool> EnableScenarioAsync(string scenarioName, CancellationToken cancellationToken = default)
        {
            var response = await _api.EnableScenarioAsync(scenarioName, cancellationToken);

            if (!response.IsOk)
            {
                throw new InvalidOperationException($"Failed to enable scenario '{scenarioName}'. Status: {response.StatusCode}");
            }

            var status = response.Ok();
            return status?.Enabled ?? false;
        }

        /// <summary>
        /// Disables a failure scenario by name.
        /// </summary>
        public async Task<bool> DisableScenarioAsync(string scenarioName, CancellationToken cancellationToken = default)
        {
            var response = await _api.DisableScenarioAsync(scenarioName, cancellationToken);

            if (!response.IsOk)
            {
                throw new InvalidOperationException($"Failed to disable scenario '{scenarioName}'. Status: {response.StatusCode}");
            }

            var status = response.Ok();
            return status?.Enabled ?? false;
        }

        /// <summary>
        /// Gets the status of a specific scenario by name.
        /// </summary>
        public async Task<FailureScenarioStatus?> GetScenarioStatusAsync(string scenarioName, CancellationToken cancellationToken = default)
        {
            var scenarios = await ListScenariosAsync(cancellationToken);
            return scenarios.Find(s => s.Name == scenarioName);
        }

        /// <summary>
        /// Disables all currently enabled scenarios.
        /// Useful for test cleanup.
        /// </summary>
        public async Task DisableAllScenariosAsync(CancellationToken cancellationToken = default)
        {
            var scenarios = await ListScenariosAsync(cancellationToken);
            foreach (var scenario in scenarios)
            {
                if (scenario.Enabled)
                {
                    await DisableScenarioAsync(scenario.Name, cancellationToken);
                }
            }
        }

        /// <summary>
        /// Gets the history of Thrift method calls tracked by the proxy.
        /// Returns method name, timestamp, message type, and sequence ID for each call.
        /// Note: Call history is automatically reset when a scenario is enabled.
        /// </summary>
        public async Task<ProxyControlApi.Model.ThriftCallHistory?> GetThriftCallsAsync(CancellationToken cancellationToken = default)
        {
            var response = await _api.GetThriftCallsAsync(cancellationToken);

            if (!response.IsOk)
            {
                throw new InvalidOperationException($"Failed to get Thrift calls. Status: {response.StatusCode}");
            }

            return response.Ok();
        }

        /// <summary>
        /// Manually resets the Thrift call history.
        /// Note: Call history is automatically reset when a scenario is enabled.
        /// </summary>
        public async Task ResetThriftCallsAsync(CancellationToken cancellationToken = default)
        {
            var response = await _api.ResetThriftCallsAsync(cancellationToken);

            if (!response.IsOk)
            {
                throw new InvalidOperationException($"Failed to reset Thrift calls. Status: {response.StatusCode}");
            }
        }

        /// <summary>
        /// Verifies that Thrift method calls match expected patterns.
        /// </summary>
        /// <param name="type">Verification type: exact_sequence, contains_sequence, method_count, method_exists</param>
        /// <param name="methods">Expected method sequence (for sequence verifications)</param>
        /// <param name="method">Method name (for method_count/method_exists)</param>
        /// <param name="count">Expected count (for method_count)</param>
        public async Task<ProxyControlApi.Model.ThriftVerificationResult?> VerifyThriftCallsAsync(
            string type,
            List<string>? methods = null,
            string? method = null,
            int? count = null,
            CancellationToken cancellationToken = default)
        {
            var typeEnum = ProxyControlApi.Model.ThriftVerificationRequest.TypeEnumFromString(type);
            var request = new ProxyControlApi.Model.ThriftVerificationRequest(typeEnum);

            // Only set optional properties if they're not null
            if (methods != null)
            {
                request.Methods = methods;
            }
            if (method != null)
            {
                request.Method = method;
            }
            if (count.HasValue)
            {
                request.Count = count.Value;
            }

            var response = await _api.VerifyThriftCallsAsync(request, cancellationToken);

            // TODO: Fix BadRequest deserialization issue
            // if (response.IsBadRequest)
            // {
            //     var error = response.BadRequest();
            //     throw new ArgumentException($"Verification request invalid: {error?.Error}");
            // }

            if (!response.IsOk)
            {
                throw new InvalidOperationException($"Failed to verify Thrift calls. Status: {response.StatusCode}, RawContent: {response.RawContent}");
            }

            return response.Ok();
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _httpClient?.Dispose();
                _loggerFactory?.Dispose();
                _disposed = true;
            }
        }
    }

    /// <summary>
    /// Represents the status of a failure scenario.
    /// </summary>
    public class FailureScenarioStatus
    {
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public bool Enabled { get; set; }
    }
}
