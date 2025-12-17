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
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Tests.ThriftProtocol
{
    /// <summary>
    /// Client for interacting with the Proxy Server Control API.
    /// Provides methods to enable/disable failure scenarios and query their status.
    /// </summary>
    public class ProxyControlClient : IDisposable
    {
        private readonly HttpClient _httpClient;
        private readonly string _baseUrl;
        private bool _disposed;

        public ProxyControlClient(int apiPort = 18081)
        {
            _baseUrl = $"http://localhost:{apiPort}";
            _httpClient = new HttpClient
            {
                BaseAddress = new Uri(_baseUrl),
                Timeout = TimeSpan.FromSeconds(5)
            };
        }

        /// <summary>
        /// Lists all available failure scenarios and their current status.
        /// </summary>
        public async Task<List<FailureScenarioStatus>> ListScenariosAsync(CancellationToken cancellationToken = default)
        {
            var response = await _httpClient.GetAsync("/scenarios", cancellationToken);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync(cancellationToken);
            var result = JsonSerializer.Deserialize<ScenarioListResponse>(json, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });

            return result?.Scenarios ?? new List<FailureScenarioStatus>();
        }

        /// <summary>
        /// Enables a failure scenario by name.
        /// </summary>
        public async Task<bool> EnableScenarioAsync(string scenarioName, CancellationToken cancellationToken = default)
        {
            var response = await _httpClient.PostAsync($"/scenarios/{scenarioName}/enable", null, cancellationToken);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync(cancellationToken);
            var result = JsonSerializer.Deserialize<ScenarioActionResponse>(json, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });

            return result?.Enabled ?? false;
        }

        /// <summary>
        /// Disables a failure scenario by name.
        /// </summary>
        public async Task<bool> DisableScenarioAsync(string scenarioName, CancellationToken cancellationToken = default)
        {
            var response = await _httpClient.PostAsync($"/scenarios/{scenarioName}/disable", null, cancellationToken);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync(cancellationToken);
            var result = JsonSerializer.Deserialize<ScenarioActionResponse>(json, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });

            return result?.Enabled ?? false;
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

        public void Dispose()
        {
            if (!_disposed)
            {
                _httpClient?.Dispose();
                _disposed = true;
            }
        }

        #region Response Models

        private class ScenarioListResponse
        {
            public List<FailureScenarioStatus> Scenarios { get; set; } = new();
        }

        private class ScenarioActionResponse
        {
            public string Scenario { get; set; } = string.Empty;
            public bool Enabled { get; set; }
        }

        #endregion
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
