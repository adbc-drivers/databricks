package main

import (
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

// TestProxyServer runs end-to-end tests by starting the actual proxy server
func TestProxyServer(t *testing.T) {
	// Create test config file
	testConfig := `proxy:
  listen_port: 18080
  target_server: "https://httpbin.org"
  api_port: 18081
  log_requests: false

failure_scenarios:
  - name: "test_azure_403"
    description: "Test Azure 403 error"
    operation: "CloudFetchDownload"
    action: "return_error"
    error_code: 403
    error_message: "[TEST_ERROR]"
`

	tmpfile, err := os.CreateTemp("", "test-config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp config: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(testConfig); err != nil {
		t.Fatalf("Failed to write temp config: %v", err)
	}
	tmpfile.Close()

	// Load config
	var loadErr error
	config, loadErr = LoadConfig(tmpfile.Name())
	if loadErr != nil {
		t.Fatalf("Failed to load config: %v", loadErr)
	}

	// Index scenarios
	scenarios = make(map[string]*FailureScenario)
	for i := range config.FailureScenarios {
		scenarios[config.FailureScenarios[i].Name] = &config.FailureScenarios[i]
	}

	// Start servers in background
	go startControlAPI()
	go startProxy()

	// Wait for servers to start
	time.Sleep(500 * time.Millisecond)

	// Run tests
	t.Run("ControlAPI_ListScenarios", testListScenarios)
	t.Run("ControlAPI_EnableScenario", testEnableScenario)
	t.Run("CloudFetch_InjectionWorks", testCloudFetchInjection)
}

func testListScenarios(t *testing.T) {
	resp, err := http.Get("http://localhost:18081/scenarios")
	if err != nil {
		t.Fatalf("Failed to list scenarios: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// Verify response contains our test scenario
	if len(bodyStr) < 10 {
		t.Errorf("Response too short: %s", bodyStr)
	}
	if !contains(bodyStr, "test_azure_403") {
		t.Errorf("Response missing test scenario: %s", bodyStr)
	}
}

func testEnableScenario(t *testing.T) {
	// Enable scenario
	resp, err := http.Post("http://localhost:18081/scenarios/test_azure_403/enable", "", nil)
	if err != nil {
		t.Fatalf("Failed to enable scenario: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !contains(bodyStr, "true") {
		t.Errorf("Expected enabled=true in response: %s", bodyStr)
	}
}

func testCloudFetchInjection(t *testing.T) {
	// Enable scenario first
	http.Post("http://localhost:18081/scenarios/test_azure_403/enable", "", nil)

	// Make request that should trigger injection
	req, _ := http.NewRequest("GET", "http://localhost:18080/test-file", nil)
	req.Host = "test.blob.core.windows.net" // Simulate Azure CloudFetch

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to make CloudFetch request: %v", err)
	}
	defer resp.Body.Close()

	// Verify injection happened
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("Expected status 403 (injection), got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !contains(bodyStr, "[TEST_ERROR]") {
		t.Errorf("Expected error message '[TEST_ERROR]', got: %s", bodyStr)
	}

	// Verify scenario was auto-disabled (one-shot)
	time.Sleep(100 * time.Millisecond)

	// Second request should NOT trigger injection
	req2, _ := http.NewRequest("GET", "http://localhost:18080/test-file", nil)
	req2.Host = "test.blob.core.windows.net"

	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("Failed to make second request: %v", err)
	}
	defer resp2.Body.Close()

	// Should be forwarded to httpbin.org (404 or 200, not 403)
	if resp2.StatusCode == http.StatusForbidden {
		t.Error("Scenario should have been auto-disabled, but injection still occurred")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && containsSubstring(s, substr)
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestConfigLoading tests config validation without starting the server
func TestConfigLoading(t *testing.T) {
	t.Run("ValidConfig", func(t *testing.T) {
		config, err := LoadConfig("proxy-config.yaml")
		if err != nil {
			t.Fatalf("Failed to load valid config: %v", err)
		}
		if config.Proxy.ListenPort != 8080 {
			t.Errorf("Expected port 8080, got %d", config.Proxy.ListenPort)
		}
	})

	t.Run("MissingFile", func(t *testing.T) {
		_, err := LoadConfig("nonexistent.yaml")
		if err == nil {
			t.Error("Expected error for missing file")
		}
	})

	t.Run("MissingRequiredFields", func(t *testing.T) {
		tmpfile, _ := os.CreateTemp("", "test-*.yaml")
		defer os.Remove(tmpfile.Name())

		tmpfile.WriteString("proxy:\n  listen_port: 8080\n")
		tmpfile.Close()

		_, err := LoadConfig(tmpfile.Name())
		if err == nil {
			t.Error("Expected error for missing target_server")
		}
	})
}
