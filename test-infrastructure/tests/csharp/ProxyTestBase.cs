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
using System.IO;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.ThriftProtocol
{
    /// <summary>
    /// Base class for tests that need Thrift protocol failure injection via the proxy server.
    /// Automatically manages proxy server lifecycle and provides utilities for scenario control.
    /// </summary>
    public abstract class ProxyTestBase : IAsyncLifetime
    {
        private ProxyServerManager? _proxyManager;
        private ProxyControlClient? _controlClient;
        private DatabricksTestConfiguration? _testConfig;
        private bool _proxyStarted;

        protected ProxyServerManager ProxyManager => _proxyManager
            ?? throw new InvalidOperationException("Proxy server not initialized. Call InitializeAsync first.");

        protected ProxyControlClient ControlClient => _controlClient
            ?? throw new InvalidOperationException("Control client not initialized. Call InitializeAsync first.");

        protected DatabricksTestConfiguration TestConfig => _testConfig
            ?? throw new InvalidOperationException("Test configuration not initialized. Call InitializeAsync first.");

        /// <summary>
        /// xUnit lifecycle method: Initialize proxy server before each test.
        /// </summary>
        public virtual async Task InitializeAsync()
        {
            // Load test configuration
            _testConfig = await LoadTestConfigurationAsync();

            // Initialize proxy server
            _proxyManager = new ProxyServerManager();
            _controlClient = new ProxyControlClient(_proxyManager.ApiPort);

            try
            {
                await _proxyManager.StartAsync();
                _proxyStarted = true;

                // Set environment variables so CloudFetch HttpClient routes through proxy
                // This enables mitmproxy to intercept HTTPS requests to cloud storage
                var proxyUrl = $"http://localhost:{_proxyManager.ProxyPort}";
                Environment.SetEnvironmentVariable("HTTP_PROXY", proxyUrl);
                Environment.SetEnvironmentVariable("HTTPS_PROXY", proxyUrl);

                // Ensure all scenarios are disabled at the start of each test
                await _controlClient.DisableAllScenariosAsync();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    "Failed to start proxy server. " +
                    "Ensure mitmproxy is installed: pip install mitmproxy flask",
                    ex);
            }
        }

        /// <summary>
        /// xUnit lifecycle method: Cleanup after each test.
        /// </summary>
        public virtual async Task DisposeAsync()
        {
            if (_proxyStarted && _controlClient != null)
            {
                try
                {
                    // Clean up: disable all scenarios
                    await _controlClient.DisableAllScenariosAsync();
                }
                catch (Exception)
                {
                    // Ignore cleanup errors
                }
            }

            // Clear proxy environment variables
            Environment.SetEnvironmentVariable("HTTP_PROXY", null);
            Environment.SetEnvironmentVariable("HTTPS_PROXY", null);

            _controlClient?.Dispose();
            _proxyManager?.Dispose();
        }

        /// <summary>
        /// Creates a driver connection that routes through the proxy server.
        /// </summary>
        protected AdbcConnection CreateProxiedConnection()
        {
            if (!_proxyStarted)
            {
                throw new InvalidOperationException("Proxy server not started. Call InitializeAsync first.");
            }

            var parameters = BuildProxiedConnectionParameters();
            var driver = new DatabricksDriver();
            var database = driver.Open(parameters);
            return database.Connect(parameters);
        }

        /// <summary>
        /// Builds connection parameters that route through the mitmproxy server.
        /// Uses proper proxy settings with TLS certificate handling for HTTPS interception.
        /// Override this method to customize connection configuration.
        /// </summary>
        protected virtual Dictionary<string, string> BuildProxiedConnectionParameters()
        {
            var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Driver config takes precedence over environment variables
            parameters["adbc.databricks.driver_config_take_precedence"] = "true";

            // Configure proxy settings to route through mitmproxy
            parameters["adbc.proxy_options.use_proxy"] = "true";
            parameters["adbc.proxy_options.proxy_host"] = "localhost";
            parameters["adbc.proxy_options.proxy_port"] = ProxyManager.ProxyPort.ToString();

            // Enable TLS but allow mitmproxy's self-signed certificate
            parameters["adbc.http_options.tls.enabled"] = "true";
            parameters["adbc.http_options.tls.allow_self_signed"] = "true";
            parameters["adbc.http_options.tls.disable_server_certificate_validation"] = "true";
            parameters["adbc.http_options.tls.allow_hostname_mismatch"] = "true";

            // Point to mitmproxy's CA certificate
            var mitmproxyCertPath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                ".mitmproxy",
                "mitmproxy-ca-cert.pem");
            if (File.Exists(mitmproxyCertPath))
            {
                parameters["adbc.http_options.tls.trusted_certificate_path"] = mitmproxyCertPath;
            }

            // Copy connection details from test config
            // Support both uri format and individual hostname/port/path format
            if (!string.IsNullOrEmpty(TestConfig.Uri))
            {
                // Use uri format (preferred)
                parameters["uri"] = TestConfig.Uri;
            }
            else
            {
                // Fall back to individual components
                if (!string.IsNullOrEmpty(TestConfig.HostName))
                {
                    parameters[SparkParameters.HostName] = TestConfig.HostName;
                }
                if (!string.IsNullOrEmpty(TestConfig.Port))
                {
                    parameters[SparkParameters.Port] = TestConfig.Port;
                }
                if (!string.IsNullOrEmpty(TestConfig.Path))
                {
                    parameters[SparkParameters.Path] = TestConfig.Path;
                }
            }

            if (!string.IsNullOrEmpty(TestConfig.Token))
            {
                parameters[SparkParameters.Token] = TestConfig.Token;
            }
            if (!string.IsNullOrEmpty(TestConfig.AccessToken))
            {
                parameters[SparkParameters.AccessToken] = TestConfig.AccessToken;
            }
            if (!string.IsNullOrEmpty(TestConfig.AuthType))
            {
                parameters[SparkParameters.AuthType] = TestConfig.AuthType;
            }

            return parameters;
        }

        /// <summary>
        /// Loads the test configuration from the environment variable.
        /// </summary>
        private async Task<DatabricksTestConfiguration> LoadTestConfigurationAsync()
        {
            var configPath = Environment.GetEnvironmentVariable("DATABRICKS_TEST_CONFIG_FILE");
            if (string.IsNullOrEmpty(configPath))
            {
                throw new InvalidOperationException(
                    "DATABRICKS_TEST_CONFIG_FILE environment variable not set. " +
                    "Set it to the path of your databricks-test-config.json file.");
            }

            if (!System.IO.File.Exists(configPath))
            {
                throw new System.IO.FileNotFoundException($"Test config file not found: {configPath}");
            }

            var json = await System.IO.File.ReadAllTextAsync(configPath);
            var config = System.Text.Json.JsonSerializer.Deserialize<DatabricksTestConfiguration>(json);

            if (config == null)
            {
                throw new InvalidOperationException($"Failed to deserialize test config from {configPath}");
            }

            return config;
        }
    }
}
