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
        /// Gets the history of Thrift method calls recorded by the proxy.
        /// Call history is automatically reset when a scenario is enabled.
        /// </summary>
        public async Task<ThriftCallHistory> GetThriftCallsAsync(CancellationToken cancellationToken = default)
        {
            var response = await _httpClient.GetAsync("/thrift/calls", cancellationToken);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            var options = new System.Text.Json.JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };
            var history = System.Text.Json.JsonSerializer.Deserialize<ThriftCallHistory>(json, options);
            return history ?? new ThriftCallHistory();
        }

        /// <summary>
        /// Counts how many times a specific Thrift method was called.
        /// </summary>
        public async Task<int> CountThriftMethodCallsAsync(string methodName, CancellationToken cancellationToken = default)
        {
            var history = await GetThriftCallsAsync(cancellationToken);
            return history.Calls?.Count(c => c.Type == "thrift" && c.Method == methodName) ?? 0;
        }

        /// <summary>
        /// Counts how many cloud download requests were made.
        /// </summary>
        public async Task<int> CountCloudDownloadsAsync(CancellationToken cancellationToken = default)
        {
            var history = await GetThriftCallsAsync(cancellationToken);
            return history.Calls?.Count(c => c.Type == "cloud_download") ?? 0;
        }

        /// <summary>
        /// Verifies that a Thrift method was called at least the specified number of times.
        /// </summary>
        public async Task AssertThriftMethodCalledAsync(string methodName, int minCalls, CancellationToken cancellationToken = default)
        {
            var actualCalls = await CountThriftMethodCallsAsync(methodName, cancellationToken);
            if (actualCalls < minCalls)
            {
                throw new Xunit.Sdk.XunitException(
                    $"Expected {methodName} to be called at least {minCalls} time(s), but was called {actualCalls} time(s)");
            }
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

    /// <summary>
    /// Represents the history of Thrift method calls recorded by the proxy.
    /// </summary>
    public class ThriftCallHistory
    {
        public List<ThriftCall> Calls { get; set; } = new List<ThriftCall>();
        public int Count { get; set; }
        public int MaxHistory { get; set; }
    }

    /// <summary>
    /// Represents a single tracked call (Thrift method or cloud download).
    /// </summary>
    public class ThriftCall
    {
        public double Timestamp { get; set; }
        public string Type { get; set; } = string.Empty; // "thrift" or "cloud_download"
        public string Method { get; set; } = string.Empty; // For Thrift calls
        public string MessageType { get; set; } = string.Empty; // For Thrift calls
        public int SequenceId { get; set; } // For Thrift calls
        public System.Text.Json.JsonElement? Fields { get; set; } // For Thrift calls
        public string Url { get; set; } = string.Empty; // For cloud downloads
    }
}
