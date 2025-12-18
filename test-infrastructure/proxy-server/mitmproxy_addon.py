#!/usr/bin/env python3
"""
mitmproxy addon for Databricks ADBC driver testing.
Implements failure injection for CloudFetch and Thrift protocol testing.

Control API runs on port 18081 (compatible with existing test infrastructure).
Proxy listens on port 18080.
"""

import json
import threading
import time
from pathlib import Path
from typing import Dict, Any
from flask import Flask, jsonify, request
from mitmproxy import http, ctx

# Flask app for control API
app = Flask(__name__)

# Global state for enabled scenarios (thread-safe with lock)
state_lock = threading.Lock()
enabled_scenarios: Dict[str, bool] = {}

# Load scenario definitions from YAML (we'll parse the existing config)
SCENARIOS = {
    "cloudfetch_expired_link": {
        "description": "CloudFetch link expires, driver should retry via FetchResults",
        "operation": "CloudFetchDownload",
        "action": "expire_cloud_link",
    },
    "cloudfetch_azure_403": {
        "description": "Azure returns 403 Forbidden during CloudFetch download",
        "operation": "CloudFetchDownload",
        "action": "return_error",
        "error_code": 403,
        "error_message": "[FILES_API_AZURE_FORBIDDEN]",
    },
    "cloudfetch_timeout": {
        "description": "CloudFetch download times out (exceeds 60s)",
        "operation": "CloudFetchDownload",
        "action": "delay",
        "duration": "65s",
    },
    "cloudfetch_connection_reset": {
        "description": "Connection reset during CloudFetch download",
        "operation": "CloudFetchDownload",
        "action": "close_connection",
    },
}


# ===== Control API Endpoints =====

@app.route('/scenarios', methods=['GET'])
def list_scenarios():
    """List all available scenarios with their status."""
    with state_lock:
        scenarios_list = [
            {
                "name": name,
                "description": config["description"],
                "enabled": enabled_scenarios.get(name, False)
            }
            for name, config in SCENARIOS.items()
        ]
    return jsonify({"scenarios": scenarios_list})


@app.route('/scenarios/<scenario_name>/enable', methods=['POST'])
def enable_scenario(scenario_name):
    """Enable a failure scenario."""
    if scenario_name not in SCENARIOS:
        return jsonify({"error": f"Scenario not found: {scenario_name}"}), 404

    with state_lock:
        enabled_scenarios[scenario_name] = True

    ctx.log.info(f"[API] Enabled scenario: {scenario_name}")
    return jsonify({"scenario": scenario_name, "enabled": True})


@app.route('/scenarios/<scenario_name>/disable', methods=['POST'])
def disable_scenario(scenario_name):
    """Disable a failure scenario."""
    if scenario_name not in SCENARIOS:
        return jsonify({"error": f"Scenario not found: {scenario_name}"}), 404

    with state_lock:
        enabled_scenarios[scenario_name] = False

    ctx.log.info(f"[API] Disabled scenario: {scenario_name}")
    return jsonify({"scenario": scenario_name, "enabled": False})


@app.route('/scenarios/<scenario_name>/status', methods=['GET'])
def get_scenario_status(scenario_name):
    """Get status of a specific scenario."""
    if scenario_name not in SCENARIOS:
        return jsonify({"error": f"Scenario not found: {scenario_name}"}), 404

    with state_lock:
        enabled = enabled_scenarios.get(scenario_name, False)

    return jsonify({
        "name": scenario_name,
        "description": SCENARIOS[scenario_name]["description"],
        "enabled": enabled
    })


# ===== mitmproxy Addon Class =====

class FailureInjectionAddon:
    """mitmproxy addon that injects failures based on enabled scenarios."""

    def __init__(self):
        """Initialize addon and start control API server."""
        ctx.log.info("Starting FailureInjectionAddon")

        # Start Flask control API in background thread
        def run_api():
            app.run(host='0.0.0.0', port=18081, threaded=True)

        api_thread = threading.Thread(target=run_api, daemon=True, name="ControlAPI")
        api_thread.start()
        ctx.log.info("Control API started on http://0.0.0.0:18081")

    def request(self, flow: http.HTTPFlow) -> None:
        """
        Intercept requests and inject failures based on enabled scenarios.
        Called by mitmproxy for each HTTP request.
        """
        # Detect request type
        if self._is_cloudfetch_download(flow.request):
            self._handle_cloudfetch_request(flow)
        elif self._is_thrift_request(flow.request):
            self._handle_thrift_request(flow)

    def _is_cloudfetch_download(self, request: http.Request) -> bool:
        """Detect if this is a CloudFetch download to cloud storage."""
        if request.method != "GET":
            return False

        host = request.pretty_host.lower()
        return (
            "blob.core.windows.net" in host or
            "s3.amazonaws.com" in host or
            "storage.googleapis.com" in host
        )

    def _is_thrift_request(self, request: http.Request) -> bool:
        """Detect if this is a Thrift request to Databricks SQL warehouse."""
        return request.method == "POST" and request.path.startswith("/sql/")

    def _handle_cloudfetch_request(self, flow: http.HTTPFlow) -> None:
        """Handle CloudFetch requests and inject failures if scenario is enabled."""
        with state_lock:
            # Find first enabled CloudFetch scenario
            enabled_scenario = None
            for name, config in SCENARIOS.items():
                if config["operation"] == "CloudFetchDownload" and enabled_scenarios.get(name):
                    enabled_scenario = (name, config)
                    break

        if not enabled_scenario:
            return  # No scenario enabled, let request proceed normally

        scenario_name, scenario_config = enabled_scenario
        ctx.log.info(f"[INJECT] Triggering scenario: {scenario_name} for {flow.request.pretty_url}")

        # Inject failure based on action
        action = scenario_config["action"]

        if action == "expire_cloud_link":
            # Return 403 with Azure expired signature error
            flow.response = http.Response.make(
                403,
                b"AuthorizationQueryParametersError: Query Parameters are not supported for this operation",
                {"Content-Type": "text/plain"}
            )
            self._disable_scenario(scenario_name)

        elif action == "return_error":
            # Return HTTP error with specified code and message
            error_code = scenario_config.get("error_code", 500)
            error_message = scenario_config.get("error_message", "Internal Server Error")
            flow.response = http.Response.make(
                error_code,
                error_message.encode('utf-8'),
                {"Content-Type": "text/plain"}
            )
            self._disable_scenario(scenario_name)

        elif action == "delay":
            # Inject delay (simulates timeout)
            duration_str = scenario_config.get("duration", "5s")
            seconds = int(duration_str.rstrip('s'))
            ctx.log.info(f"[INJECT] Delaying {seconds}s for scenario: {scenario_name}")
            time.sleep(seconds)
            self._disable_scenario(scenario_name)
            # Let request continue after delay

        elif action == "close_connection":
            # Kill the connection abruptly
            flow.response = http.Response.make(
                500,
                b"Connection reset by peer",
                {"Content-Type": "text/plain"}
            )
            flow.kill()
            self._disable_scenario(scenario_name)

    def _handle_thrift_request(self, flow: http.HTTPFlow) -> None:
        """Handle Thrift requests (future implementation)."""
        # TODO: Implement Thrift operation parsing and failure injection
        pass

    def _disable_scenario(self, scenario_name: str) -> None:
        """Disable a scenario after one-shot injection."""
        with state_lock:
            enabled_scenarios[scenario_name] = False
        ctx.log.info(f"[INJECT] Auto-disabled scenario: {scenario_name}")


# Register addon with mitmproxy
addons = [FailureInjectionAddon()]
