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
	"sync"
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

	// Customize the proxy director to handle Thrift protocol
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)

		// TODO: Check if any scenarios should be injected here
		// For now, just pass through

		if config.Proxy.LogRequests {
			log.Printf("[PROXY] %s %s", req.Method, req.URL.Path)
		}
	}

	// TODO: Add response modifier for failure injection
	// proxy.ModifyResponse = func(resp *http.Response) error { ... }

	addr := fmt.Sprintf(":%d", config.Proxy.ListenPort)
	log.Printf("Starting proxy server on %s", addr)
	if err := http.ListenAndServe(addr, proxy); err != nil {
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
