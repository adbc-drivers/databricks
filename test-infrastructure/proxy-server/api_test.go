package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleListScenarios(t *testing.T) {
	// Setup test config
	config = &Config{
		Proxy: ProxyConfig{
			ListenPort:   8080,
			TargetServer: "https://example.com",
			APIPort:      8081,
		},
		FailureScenarios: []FailureScenario{
			{Name: "test1", Description: "Test scenario 1", Action: "delay"},
			{Name: "test2", Description: "Test scenario 2", Action: "return_error"},
		},
	}
	scenarios = make(map[string]*FailureScenario)
	for i := range config.FailureScenarios {
		scenarios[config.FailureScenarios[i].Name] = &config.FailureScenarios[i]
	}

	req := httptest.NewRequest(http.MethodGet, "/scenarios", nil)
	w := httptest.NewRecorder()

	handleListScenarios(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", contentType)
	}

	// Parse response
	var response struct {
		Scenarios []struct {
			Name        string `json:"name"`
			Description string `json:"description"`
			Enabled     bool   `json:"enabled"`
		} `json:"scenarios"`
	}

	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if len(response.Scenarios) != 2 {
		t.Errorf("Expected 2 scenarios, got %d", len(response.Scenarios))
	}
}

func TestHandleScenarioAction_Enable(t *testing.T) {
	// Setup test config
	config = &Config{}
	scenarios = map[string]*FailureScenario{
		"test1": {Name: "test1", Description: "Test", Action: "delay", Enabled: false},
	}

	req := httptest.NewRequest(http.MethodPost, "/scenarios/test1/enable", nil)
	w := httptest.NewRecorder()

	handleScenarioAction(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Verify scenario was enabled
	if !scenarios["test1"].Enabled {
		t.Error("Expected scenario to be enabled")
	}

	// Parse response
	var response struct {
		Scenario string `json:"scenario"`
		Enabled  bool   `json:"enabled"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if response.Scenario != "test1" {
		t.Errorf("Expected scenario test1, got %s", response.Scenario)
	}
	if !response.Enabled {
		t.Error("Expected enabled=true in response")
	}
}

func TestHandleScenarioAction_Disable(t *testing.T) {
	// Setup test config
	config = &Config{}
	scenarios = map[string]*FailureScenario{
		"test1": {Name: "test1", Description: "Test", Action: "delay", Enabled: true},
	}

	req := httptest.NewRequest(http.MethodPost, "/scenarios/test1/disable", nil)
	w := httptest.NewRecorder()

	handleScenarioAction(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Verify scenario was disabled
	if scenarios["test1"].Enabled {
		t.Error("Expected scenario to be disabled")
	}
}

func TestHandleScenarioAction_NotFound(t *testing.T) {
	config = &Config{}
	scenarios = make(map[string]*FailureScenario)

	req := httptest.NewRequest(http.MethodPost, "/scenarios/nonexistent/enable", nil)
	w := httptest.NewRecorder()

	handleScenarioAction(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}
}
