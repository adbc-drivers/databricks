// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"
)

var (
	configPath = flag.String("config", "proxy-config.yaml", "Path to proxy configuration file")
	config     *Config
	scenarios  = make(map[string]*FailureScenario)
	mu         sync.RWMutex // Protects scenarios map
)

func main() {
	flag.Parse()

	// Load configuration
	var err error
	config, err = LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Index scenarios by name for quick lookup
	for i := range config.FailureScenarios {
		scenarios[config.FailureScenarios[i].Name] = &config.FailureScenarios[i]
	}

	log.Printf("Loaded %d failure scenarios", len(scenarios))
	log.Printf("Target server: %s", config.Proxy.TargetServer)
	log.Printf("Listen port: %d (proxy)", config.Proxy.ListenPort)
	log.Printf("API port: %d (control)", config.Proxy.APIPort)

	// Start control API server
	go startControlAPI()

	// Start proxy server
	startProxy()
}

// startProxy starts the main proxy server that intercepts Thrift/HTTP traffic
func startProxy() {
	targetURL, err := url.Parse(config.Proxy.TargetServer)
	if err != nil {
		log.Fatalf("Failed to parse target server URL: %v", err)
	}

	// Create reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	// Customize the Director to ensure proper header forwarding
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		// Ensure the Host header is set to the target server
		req.Host = targetURL.Host
		req.Header.Set("X-Forwarded-Host", req.Header.Get("Host"))

		if config.Proxy.LogRequests {
			log.Printf("[PROXY] Forwarding to: %s%s", targetURL.String(), req.URL.Path)
		}
	}

	// Wrap proxy with failure injection handler
	handler := proxyHandler(proxy)

	addr := fmt.Sprintf(":%d", config.Proxy.ListenPort)
	log.Printf("Starting proxy server on %s", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatalf("Proxy server failed: %v", err)
	}
}

// startControlAPI starts the control API server for enabling/disabling scenarios
func startControlAPI() {
	mux := http.NewServeMux()

	// GET /scenarios - List all available scenarios
	mux.HandleFunc("/scenarios", handleListScenarios)

	// POST /scenarios/{name}/enable - Enable a scenario
	mux.HandleFunc("/scenarios/", handleScenarioAction)

	addr := fmt.Sprintf(":%d", config.Proxy.APIPort)
	log.Printf("Starting control API on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Control API failed: %v", err)
	}
}

// handleListScenarios returns list of all scenarios with their status
func handleListScenarios(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	mu.RLock()
	defer mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Write JSON response manually (avoiding json package for simplicity)
	fmt.Fprintf(w, "{\"scenarios\":[")
	first := true
	for name, scenario := range scenarios {
		if !first {
			fmt.Fprintf(w, ",")
		}
		first = false
		fmt.Fprintf(w, "{\"name\":\"%s\",\"description\":\"%s\",\"enabled\":%t}",
			name, scenario.Description, scenario.Enabled)
	}
	fmt.Fprintf(w, "]}")
}

// handleScenarioAction handles enable/disable requests for scenarios
func handleScenarioAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse path: /scenarios/{name}/enable or /scenarios/{name}/disable
	path := r.URL.Path[len("/scenarios/"):]

	// Find action (enable/disable)
	var scenarioName, action string
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			scenarioName = path[:i]
			action = path[i+1:]
			break
		}
	}

	if scenarioName == "" || (action != "enable" && action != "disable") {
		http.Error(w, "Invalid path. Use /scenarios/{name}/enable or /scenarios/{name}/disable",
			http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	scenario, exists := scenarios[scenarioName]
	if !exists {
		http.Error(w, fmt.Sprintf("Scenario not found: %s", scenarioName),
			http.StatusNotFound)
		return
	}

	if action == "enable" {
		scenario.Enabled = true
		log.Printf("[API] Enabled scenario: %s", scenarioName)
	} else {
		scenario.Enabled = false
		log.Printf("[API] Disabled scenario: %s", scenarioName)
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{\"scenario\":\"%s\",\"enabled\":%t}", scenarioName, scenario.Enabled)
}

// proxyHandler wraps the reverse proxy to inject CloudFetch failures
func proxyHandler(proxy *httputil.ReverseProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Detect if this is a CloudFetch download
		if isCloudFetchDownload(r) {
			// Check for enabled CloudFetch scenarios
			scenario := getEnabledCloudFetchScenario()
			if scenario != nil {
				if handleCloudFetchFailure(w, r, scenario) {
					return // Failure was injected, don't proxy
				}
			}
		} else if isThriftRequest(r) {
			// Check for enabled Thrift operation scenarios
			scenario := getEnabledThriftScenario()
			if scenario != nil {
				if handleThriftFailure(w, r, scenario) {
					return // Failure was injected, don't proxy
				}
			}
		}

		// Normal proxying
		if config.Proxy.LogRequests {
			log.Printf("[PROXY] %s %s", r.Method, r.URL.Path)
		}
		proxy.ServeHTTP(w, r)
	}
}

// isCloudFetchDownload detects CloudFetch downloads (HTTP GET to cloud storage)
func isCloudFetchDownload(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	host := strings.ToLower(r.Host)
	return strings.Contains(host, "blob.core.windows.net") ||
		strings.Contains(host, "s3.amazonaws.com") ||
		strings.Contains(host, "storage.googleapis.com")
}

// isThriftRequest detects Thrift/HTTP requests to SQL warehouse
func isThriftRequest(r *http.Request) bool {
	// Thrift requests are POST to /sql/1.0/warehouses/{warehouse_id}
	return r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/sql/")
}

// getEnabledCloudFetchScenario finds an enabled CloudFetch scenario
func getEnabledCloudFetchScenario() *FailureScenario {
	mu.RLock()
	defer mu.RUnlock()

	for _, scenario := range scenarios {
		if scenario.Enabled && scenario.Operation == "CloudFetchDownload" {
			return scenario
		}
	}
	return nil
}

// getEnabledThriftScenario finds an enabled Thrift operation scenario
func getEnabledThriftScenario() *FailureScenario {
	mu.RLock()
	defer mu.RUnlock()

	for _, scenario := range scenarios {
		// For now, match any enabled scenario with a Thrift operation
		// TODO: Parse Thrift binary protocol to match specific operations
		if scenario.Enabled && scenario.Operation != "" && scenario.Operation != "CloudFetchDownload" {
			return scenario
		}
	}
	return nil
}

// handleThriftFailure injects Thrift operation failures
func handleThriftFailure(w http.ResponseWriter, r *http.Request, scenario *FailureScenario) bool {
	log.Printf("[INJECT] Triggering scenario: %s (operation: %s)", scenario.Name, scenario.Operation)

	switch scenario.Action {
	case "return_error":
		// Return HTTP error with specified code and message
		code := scenario.ErrorCode
		if code == 0 {
			code = http.StatusInternalServerError
		}
		http.Error(w, scenario.ErrorMessage, code)
		disableScenario(scenario.Name)
		return true

	case "delay":
		// Inject delay then continue with normal request
		duration, err := time.ParseDuration(scenario.Duration)
		if err != nil {
			log.Printf("[ERROR] Invalid duration for scenario %s: %v", scenario.Name, err)
			return false
		}
		log.Printf("[INJECT] Delaying %s for scenario: %s", duration, scenario.Name)
		time.Sleep(duration)
		disableScenario(scenario.Name)
		return false // Continue with request after delay

	case "close_connection":
		// Close the connection abruptly to simulate connection reset
		if hijacker, ok := w.(http.Hijacker); ok {
			conn, _, err := hijacker.Hijack()
			if err != nil {
				log.Printf("[ERROR] Failed to hijack connection for scenario %s: %v", scenario.Name, err)
				return false
			}
			log.Printf("[INJECT] Closing connection for scenario: %s", scenario.Name)
			conn.Close()
			disableScenario(scenario.Name)
			return true
		}
		log.Printf("[ERROR] ResponseWriter does not support hijacking for scenario: %s", scenario.Name)
		return false
	}

	return false
}

// handleCloudFetchFailure injects CloudFetch failures
func handleCloudFetchFailure(w http.ResponseWriter, r *http.Request, scenario *FailureScenario) bool {
	log.Printf("[INJECT] Triggering scenario: %s", scenario.Name)

	switch scenario.Action {
	case "expire_cloud_link":
		// Return 403 with Azure expired signature error
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("AuthorizationQueryParametersError: Query Parameters are not supported for this operation"))
		disableScenario(scenario.Name)
		return true

	case "return_error":
		// Return HTTP error with specified code and message
		code := scenario.ErrorCode
		if code == 0 {
			code = http.StatusInternalServerError
		}
		http.Error(w, scenario.ErrorMessage, code)
		disableScenario(scenario.Name)
		return true

	case "delay":
		// Inject delay then continue with normal request
		duration, err := time.ParseDuration(scenario.Duration)
		if err != nil {
			log.Printf("[ERROR] Invalid duration for scenario %s: %v", scenario.Name, err)
			return false
		}
		log.Printf("[INJECT] Delaying %s for scenario: %s", duration, scenario.Name)
		time.Sleep(duration)
		disableScenario(scenario.Name)
		return false // Continue with request after delay

	case "close_connection":
		// Close the connection abruptly to simulate connection reset
		if hijacker, ok := w.(http.Hijacker); ok {
			conn, _, err := hijacker.Hijack()
			if err != nil {
				log.Printf("[ERROR] Failed to hijack connection for scenario %s: %v", scenario.Name, err)
				return false
			}
			log.Printf("[INJECT] Closing connection for scenario: %s", scenario.Name)
			conn.Close()
			disableScenario(scenario.Name)
			return true
		}
		log.Printf("[ERROR] ResponseWriter does not support hijacking for scenario: %s", scenario.Name)
		return false
	}

	return false
}

// disableScenario disables a scenario after injection (one-shot behavior)
func disableScenario(name string) {
	mu.Lock()
	defer mu.Unlock()

	if scenario, exists := scenarios[name]; exists {
		scenario.Enabled = false
		log.Printf("[INJECT] Auto-disabled scenario: %s", name)
	}
}
