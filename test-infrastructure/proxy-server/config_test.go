package main

import (
	"os"
	"testing"
)

func TestLoadConfig_ValidConfig(t *testing.T) {
	// Use the existing proxy-config.yaml
	config, err := LoadConfig("proxy-config.yaml")
	if err != nil {
		t.Fatalf("Failed to load valid config: %v", err)
	}

	// Verify proxy settings
	if config.Proxy.ListenPort != 8080 {
		t.Errorf("Expected listen port 8080, got %d", config.Proxy.ListenPort)
	}
	if config.Proxy.TargetServer != "https://workspace.databricks.com" {
		t.Errorf("Expected target server https://workspace.databricks.com, got %s", config.Proxy.TargetServer)
	}
	if config.Proxy.APIPort != 8081 {
		t.Errorf("Expected API port 8081, got %d", config.Proxy.APIPort)
	}

	// Verify scenarios loaded
	if len(config.FailureScenarios) != 10 {
		t.Errorf("Expected 10 scenarios, got %d", len(config.FailureScenarios))
	}

	// Verify specific scenario exists
	found := false
	for _, scenario := range config.FailureScenarios {
		if scenario.Name == "cloudfetch_expired_link" {
			found = true
			if scenario.Action != "expire_cloud_link" {
				t.Errorf("Expected action expire_cloud_link, got %s", scenario.Action)
			}
			if scenario.Operation != "CloudFetchDownload" {
				t.Errorf("Expected operation CloudFetchDownload, got %s", scenario.Operation)
			}
		}
	}
	if !found {
		t.Error("Expected scenario cloudfetch_expired_link not found")
	}
}

func TestLoadConfig_MissingFile(t *testing.T) {
	_, err := LoadConfig("nonexistent.yaml")
	if err == nil {
		t.Error("Expected error for missing file, got nil")
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	// Create temp invalid YAML file
	tmpfile, err := os.CreateTemp("", "invalid-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	tmpfile.WriteString("invalid: yaml: content: [")
	tmpfile.Close()

	_, err = LoadConfig(tmpfile.Name())
	if err == nil {
		t.Error("Expected error for invalid YAML, got nil")
	}
}

func TestLoadConfig_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			name:    "missing listen_port",
			content: "proxy:\n  target_server: \"https://example.com\"\n",
			wantErr: "listen_port is required",
		},
		{
			name:    "missing target_server",
			content: "proxy:\n  listen_port: 8080\n",
			wantErr: "target_server is required",
		},
		{
			name: "scenario missing name",
			content: `proxy:
  listen_port: 8080
  target_server: "https://example.com"
failure_scenarios:
  - description: "test"
    action: "delay"
`,
			wantErr: "missing required field: name",
		},
		{
			name: "scenario missing action",
			content: `proxy:
  listen_port: 8080
  target_server: "https://example.com"
failure_scenarios:
  - name: "test"
    description: "test"
`,
			wantErr: "missing required field: action",
		},
		{
			name: "duplicate scenario names",
			content: `proxy:
  listen_port: 8080
  target_server: "https://example.com"
failure_scenarios:
  - name: "test"
    description: "test1"
    action: "delay"
  - name: "test"
    description: "test2"
    action: "delay"
`,
			wantErr: "duplicate scenario name: test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpfile, err := os.CreateTemp("", "test-*.yaml")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tmpfile.Name())

			tmpfile.WriteString(tt.content)
			tmpfile.Close()

			_, err = LoadConfig(tmpfile.Name())
			if err == nil {
				t.Errorf("Expected error containing %q, got nil", tt.wantErr)
			} else if err.Error() != tt.wantErr && !contains(err.Error(), tt.wantErr) {
				t.Errorf("Expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	// Config with minimal fields
	content := `proxy:
  listen_port: 8080
  target_server: "https://example.com"
`
	tmpfile, err := os.CreateTemp("", "test-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	tmpfile.WriteString(content)
	tmpfile.Close()

	config, err := LoadConfig(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Check defaults
	if config.Proxy.APIPort != 8081 {
		t.Errorf("Expected default API port 8081, got %d", config.Proxy.APIPort)
	}
	if config.Proxy.LogLevel != "info" {
		t.Errorf("Expected default log level 'info', got %s", config.Proxy.LogLevel)
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
