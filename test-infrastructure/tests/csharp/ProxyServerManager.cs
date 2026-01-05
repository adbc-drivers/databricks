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
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Tests.ThriftProtocol
{
    /// <summary>
    /// Manages the lifecycle of the mitmproxy-based test proxy server.
    /// Starts mitmproxy with the failure injection addon and ensures it's running before tests execute.
    /// </summary>
    public class ProxyServerManager : IDisposable
    {
        private Process? _proxyProcess;
        private readonly string _addonScriptPath;
        private readonly int _proxyPort;
        private readonly int _apiPort;
        private bool _disposed;

        public int ProxyPort => _proxyPort;
        public int ApiPort => _apiPort;
        public bool IsRunning => _proxyProcess != null && !_proxyProcess.HasExited;

        /// <summary>
        /// Creates a new ProxyServerManager.
        /// </summary>
        /// <param name="addonScriptPath">Path to mitmproxy addon Python script (default: auto-detect)</param>
        /// <param name="proxyPort">Port for proxy server (default: 18080 for testing)</param>
        /// <param name="apiPort">Port for control API (default: 18081 for testing)</param>
        public ProxyServerManager(
            string? addonScriptPath = null,
            int proxyPort = 18080,
            int apiPort = 18081)
        {
            _proxyPort = proxyPort;
            _apiPort = apiPort;

            // Auto-detect paths relative to the test project (test-infrastructure/tests/csharp/)
            var testProjectRoot = FindTestProjectRoot();
            _addonScriptPath = addonScriptPath ?? Path.Combine(testProjectRoot, "..", "..", "proxy-server", "mitmproxy_addon.py");

            // Ensure addon script exists
            if (!File.Exists(_addonScriptPath))
            {
                throw new FileNotFoundException(
                    $"mitmproxy addon script not found at {_addonScriptPath}. " +
                    "Ensure mitmproxy_addon.py exists in test-infrastructure/proxy-server/",
                    _addonScriptPath);
            }

            // Check if mitmproxy is installed
            if (!IsMitmproxyInstalled())
            {
                throw new InvalidOperationException(
                    "mitmproxy not found. Install it with: pip install mitmproxy flask\n" +
                    "Or install from requirements.txt: pip install -r test-infrastructure/proxy-server/requirements.txt");
            }
        }

        /// <summary>
        /// Starts the mitmproxy server process and waits until it's ready to accept connections.
        /// </summary>
        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (IsRunning)
            {
                return; // Already running
            }

            // Start mitmproxy with our addon
            // mitmdump: headless version of mitmproxy (no UI)
            // -s: load addon script
            // --listen-port: proxy port
            // --set confdir: certificate directory (expand ~ to actual home directory)
            var homeDirectory = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            var mitmproxyConfigDir = Path.Combine(homeDirectory, ".mitmproxy");

            _proxyProcess = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "mitmdump",
                    Arguments = $"-s \"{_addonScriptPath}\" --listen-port {_proxyPort} --set confdir=\"{mitmproxyConfigDir}\"",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true,
                    WorkingDirectory = Path.GetDirectoryName(_addonScriptPath)!
                }
            };

            // Capture output for debugging
            var outputLines = new System.Collections.Concurrent.ConcurrentBag<string>();
            var errorLines = new System.Collections.Concurrent.ConcurrentBag<string>();

            _proxyProcess.OutputDataReceived += (sender, args) =>
            {
                if (!string.IsNullOrEmpty(args.Data))
                {
                    outputLines.Add(args.Data);
                    Console.WriteLine($"[mitmproxy] {args.Data}");
                }
            };

            _proxyProcess.ErrorDataReceived += (sender, args) =>
            {
                if (!string.IsNullOrEmpty(args.Data))
                {
                    errorLines.Add(args.Data);
                    Console.WriteLine($"[mitmproxy ERROR] {args.Data}");
                }
            };

            // Log the command being executed
            Console.WriteLine($"[Proxy] Starting mitmdump");
            Console.WriteLine($"[Proxy] Command: {_proxyProcess.StartInfo.FileName} {_proxyProcess.StartInfo.Arguments}");
            Console.WriteLine($"[Proxy] Working Directory: {_proxyProcess.StartInfo.WorkingDirectory}");

            _proxyProcess.Start();
            _proxyProcess.BeginOutputReadLine();
            _proxyProcess.BeginErrorReadLine();

            // Wait for the API server to be ready
            try
            {
                await WaitForApiReadyAsync(cancellationToken);
            }
            catch
            {
                // If proxy failed to start, show captured output/errors
                if (errorLines.Any())
                {
                    Console.WriteLine("[Proxy] Captured stderr output:");
                    foreach (var line in errorLines)
                    {
                        Console.WriteLine($"  {line}");
                    }
                }
                if (outputLines.Any())
                {
                    Console.WriteLine("[Proxy] Captured stdout output:");
                    foreach (var line in outputLines)
                    {
                        Console.WriteLine($"  {line}");
                    }
                }
                throw;
            }
        }

        /// <summary>
        /// Checks if mitmproxy (mitmdump) is installed and available on PATH.
        /// </summary>
        private static bool IsMitmproxyInstalled()
        {
            try
            {
                var process = new Process
                {
                    StartInfo = new ProcessStartInfo
                    {
                        FileName = "mitmdump",
                        Arguments = "--version",
                        UseShellExecute = false,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        CreateNoWindow = true
                    }
                };
                process.Start();
                process.WaitForExit(5000);
                return process.ExitCode == 0;
            }
            catch
            {
                return false;
            }
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
        /// Waits until both the proxy and Control API are ready to accept connections.
        /// </summary>
        private async Task WaitForApiReadyAsync(CancellationToken cancellationToken)
        {
            using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(1) };
            var apiUrl = $"http://localhost:{_apiPort}/scenarios";

            bool apiReady = false;
            bool proxyReady = false;

            // Flask with use_reloader=False should start quickly on all platforms
            // But keep conservative timeout to avoid flakiness in CI
            var maxAttempts = 300; // 30s timeout for all platforms
            Console.WriteLine($"[Proxy] Waiting up to {maxAttempts / 10}s for proxy to become ready");

            for (int i = 0; i < maxAttempts; i++)
            {
                // Check Control API
                if (!apiReady)
                {
                    try
                    {
                        var response = await httpClient.GetAsync(apiUrl, cancellationToken);
                        if (response.StatusCode == HttpStatusCode.OK)
                        {
                            Console.WriteLine($"[Proxy] Control API ready at {apiUrl} after {i * 100}ms");
                            apiReady = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        // Log every 10 seconds to show progress
                        if (i > 0 && i % 100 == 0)
                        {
                            Console.WriteLine($"[Proxy] Still waiting for API... ({i / 10}s elapsed, last error: {ex.GetType().Name}: {ex.Message})");
                        }
                    }
                }

                // Check proxy port is listening
                if (!proxyReady && apiReady)
                {
                    try
                    {
                        using var tcpClient = new System.Net.Sockets.TcpClient();
                        await tcpClient.ConnectAsync("localhost", _proxyPort, cancellationToken);
                        Debug.WriteLine($"[Proxy] Proxy port {_proxyPort} is listening");
                        proxyReady = true;
                    }
                    catch (Exception)
                    {
                        // Expected during startup, continue waiting
                    }
                }

                if (apiReady && proxyReady)
                {
                    // Give it a bit more time to fully initialize
                    await Task.Delay(100, cancellationToken);
                    return;
                }

                await Task.Delay(100, cancellationToken);

                // Check if process has already exited
                if (_proxyProcess?.HasExited == true)
                {
                    throw new InvalidOperationException(
                        $"Proxy process exited unexpectedly with code {_proxyProcess.ExitCode}");
                }
            }

            var statusMsg = $"API Ready: {apiReady}, Proxy Ready: {proxyReady}";
            var timeoutSeconds = maxAttempts / 10;
            throw new TimeoutException($"Proxy did not become fully ready within {timeoutSeconds} seconds. {statusMsg}");
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
