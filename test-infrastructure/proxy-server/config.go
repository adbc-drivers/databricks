package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the top-level proxy configuration
type Config struct {
	Proxy            ProxyConfig        `yaml:"proxy"`
	FailureScenarios []FailureScenario  `yaml:"failure_scenarios"`
}

// ProxyConfig represents proxy server settings
type ProxyConfig struct {
	ListenPort   int    `yaml:"listen_port"`
	TargetServer string `yaml:"target_server"`
	APIPort      int    `yaml:"api_port"`
	LogRequests  bool   `yaml:"log_requests"`
	LogLevel     string `yaml:"log_level"`
}

// FailureScenario represents a single failure injection scenario
type FailureScenario struct {
	Name         string `yaml:"name"`
	Description  string `yaml:"description"`
	JIRA         string `yaml:"jira,omitempty"`
	Operation    string `yaml:"operation,omitempty"` // Optional: which Thrift operation to match
	Action       string `yaml:"action"`

	// Action-specific parameters
	ErrorCode    int    `yaml:"error_code,omitempty"`
	ErrorMessage string `yaml:"error_message,omitempty"`
	Retryable    bool   `yaml:"retryable,omitempty"`
	Duration     string `yaml:"duration,omitempty"`
	AtByte       int64  `yaml:"at_byte,omitempty"`
	ErrorType    string `yaml:"error_type,omitempty"`
	Field        string `yaml:"field,omitempty"`
	Modification string `yaml:"modification,omitempty"`

	// Runtime state (not from YAML)
	Enabled bool `yaml:"-"` // Whether this scenario is currently active
}

// LoadConfig loads proxy configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults
	if config.Proxy.APIPort == 0 {
		config.Proxy.APIPort = 8081
	}
	if config.Proxy.LogLevel == "" {
		config.Proxy.LogLevel = "info"
	}

	// Validate required fields
	if config.Proxy.ListenPort == 0 {
		return nil, fmt.Errorf("proxy.listen_port is required")
	}
	if config.Proxy.TargetServer == "" {
		return nil, fmt.Errorf("proxy.target_server is required")
	}

	// Validate scenarios
	scenarioNames := make(map[string]bool)
	for _, scenario := range config.FailureScenarios {
		if scenario.Name == "" {
			return nil, fmt.Errorf("failure scenario missing required field: name")
		}
		if scenarioNames[scenario.Name] {
			return nil, fmt.Errorf("duplicate scenario name: %s", scenario.Name)
		}
		scenarioNames[scenario.Name] = true

		if scenario.Action == "" {
			return nil, fmt.Errorf("scenario %s missing required field: action", scenario.Name)
		}
	}

	return &config, nil
}
