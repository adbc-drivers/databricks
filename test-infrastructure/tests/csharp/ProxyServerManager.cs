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
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Tests.ThriftProtocol
{
    /// <summary>
    /// Manages the lifecycle of the Thrift Protocol Test Proxy Server (Go binary).
    /// Starts the proxy server process and ensures it's running before tests execute.
    /// </summary>
    public class ProxyServerManager : IDisposable
    {
        private Process? _proxyProcess;
        private readonly string _proxyBinaryPath;
        private readonly string _configPath;
        private readonly int _proxyPort;
        private readonly int _apiPort;
        private bool _disposed;

        public int ProxyPort => _proxyPort;
        public int ApiPort => _apiPort;
        public bool IsRunning => _proxyProcess != null && !_proxyProcess.HasExited;

        /// <summary>
        /// Creates a new ProxyServerManager.
        /// </summary>
        /// <param name="proxyBinaryPath">Path to the compiled Go proxy binary (default: auto-detect)</param>
        /// <param name="configPath">Path to proxy-config.yaml (default: auto-detect)</param>
        /// <param name="proxyPort">Port for proxy server (default: 18080 for testing)</param>
        /// <param name="apiPort">Port for control API (default: 18081 for testing)</param>
        public ProxyServerManager(
            string? proxyBinaryPath = null,
            string? configPath = null,
            int proxyPort = 18080,
            int apiPort = 18081)
        {
            _proxyPort = proxyPort;
            _apiPort = apiPort;

            // Auto-detect paths relative to the test project (test-infrastructure/tests/csharp/)
            var testProjectRoot = FindTestProjectRoot();
            _proxyBinaryPath = proxyBinaryPath ?? Path.Combine(testProjectRoot, "..", "..", "proxy-server", "proxy-server");
            _configPath = configPath ?? Path.Combine(testProjectRoot, "..", "..", "proxy-server", "proxy-config.yaml");

            // Ensure binary exists
            if (!File.Exists(_proxyBinaryPath))
            {
                throw new FileNotFoundException(
                    $"Proxy binary not found at {_proxyBinaryPath}. " +
                    "Build it first with: cd test-infrastructure/proxy-server && go build -o proxy-server",
                    _proxyBinaryPath);
            }

            // Ensure config exists
            if (!File.Exists(_configPath))
            {
                throw new FileNotFoundException($"Proxy config not found at {_configPath}", _configPath);
            }
        }

        /// <summary>
        /// Starts the proxy server process and waits until it's ready to accept connections.
        /// </summary>
        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (IsRunning)
            {
                return; // Already running
            }

            // Start the Go proxy process
            _proxyProcess = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = _proxyBinaryPath,
                    Arguments = $"--config {_configPath}",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true,
                    WorkingDirectory = Path.GetDirectoryName(_proxyBinaryPath)!
                }
            };

            // Capture output for debugging
            _proxyProcess.OutputDataReceived += (sender, args) =>
            {
                if (!string.IsNullOrEmpty(args.Data))
                {
                    Debug.WriteLine($"[Proxy] {args.Data}");
                }
            };

            _proxyProcess.ErrorDataReceived += (sender, args) =>
            {
                if (!string.IsNullOrEmpty(args.Data))
                {
                    Debug.WriteLine($"[Proxy Error] {args.Data}");
                }
            };

            _proxyProcess.Start();
            _proxyProcess.BeginOutputReadLine();
            _proxyProcess.BeginErrorReadLine();

            // Wait for the API server to be ready
            await WaitForApiReadyAsync(cancellationToken);
        }

        /// <summary>
        /// Stops the proxy server process.
        /// </summary>
        public void Stop()
        {
            if (_proxyProcess != null && !_proxyProcess.HasExited)
            {
                try
                {
                    _proxyProcess.Kill(entireProcessTree: true);
                    _proxyProcess.WaitForExit(5000); // Wait up to 5 seconds for graceful shutdown
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"[Proxy] Error stopping proxy: {ex.Message}");
                }
                finally
                {
                    _proxyProcess?.Dispose();
                    _proxyProcess = null;
                }
            }
        }

        /// <summary>
        /// Waits until the Control API is ready to accept connections.
        /// </summary>
        private async Task WaitForApiReadyAsync(CancellationToken cancellationToken)
        {
            using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(1) };
            var apiUrl = $"http://localhost:{_apiPort}/scenarios";

            for (int i = 0; i < 30; i++) // Try for up to 3 seconds
            {
                try
                {
                    var response = await httpClient.GetAsync(apiUrl, cancellationToken);
                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        Debug.WriteLine($"[Proxy] Control API ready at {apiUrl}");
                        return;
                    }
                }
                catch (Exception)
                {
                    // Expected during startup, continue waiting
                }

                await Task.Delay(100, cancellationToken);

                // Check if process has already exited
                if (_proxyProcess?.HasExited == true)
                {
                    throw new InvalidOperationException(
                        $"Proxy process exited unexpectedly with code {_proxyProcess.ExitCode}");
                }
            }

            throw new TimeoutException($"Proxy Control API at {apiUrl} did not become ready within 3 seconds");
        }

        /// <summary>
        /// Finds the test project root directory by searching for the .csproj file.
        /// </summary>
        private static string FindTestProjectRoot()
        {
            var currentDir = AppContext.BaseDirectory;
            while (currentDir != null && !File.Exists(Path.Combine(currentDir, "ProxyTests.csproj")))
            {
                currentDir = Directory.GetParent(currentDir)?.FullName;
            }

            if (currentDir == null)
            {
                throw new InvalidOperationException("Could not find test project root directory (ProxyTests.csproj)");
            }

            return currentDir;
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                Stop();
                _disposed = true;
            }
        }
    }
}
