#!/usr/bin/env python3
# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
from typing import Dict, Any, Optional, List
from flask import Flask, jsonify, request
from mitmproxy import http, ctx
from thrift_decoder import decode_thrift_message, format_thrift_message

# Flask app for control API
app = Flask(__name__)

# Global state for enabled scenarios (thread-safe with lock)
state_lock = threading.Lock()
enabled_scenarios: Dict[str, bool] = {}

# Call tracking state (thread-safe with lock)
MAX_CALL_HISTORY = 1000
call_history: List[Dict[str, Any]] = []

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
    """Enable a failure scenario and auto-reset call history."""
    if scenario_name not in SCENARIOS:
        return jsonify({"error": f"Scenario not found: {scenario_name}"}), 404

    with state_lock:
        enabled_scenarios[scenario_name] = True
        # Auto-reset call history when scenario is enabled (new test scenario)
        call_history.clear()

    ctx.log.info(f"[API] Enabled scenario: {scenario_name}, reset call history")
    return jsonify({"scenario": scenario_name, "enabled": True, "call_history_reset": True})


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


@app.route('/scenarios/disable-all', methods=['POST'])
def disable_all_scenarios():
    """Disable all failure scenarios."""
    with state_lock:
        for scenario_name in SCENARIOS.keys():
            enabled_scenarios[scenario_name] = False

    ctx.log.info("[API] Disabled all scenarios")
    return jsonify({"message": "All scenarios disabled"})


@app.route('/thrift/calls', methods=['GET'])
def get_thrift_calls():
    """Get history of Thrift method calls."""
    with state_lock:
        return jsonify({
            "calls": call_history.copy(),
            "count": len(call_history),
            "max_history": MAX_CALL_HISTORY
        })


@app.route('/thrift/calls/reset', methods=['POST'])
def reset_thrift_calls():
    """Reset Thrift call history."""
    with state_lock:
        call_history.clear()

    ctx.log.info("[API] Reset Thrift call history")
    return jsonify({"message": "Call history reset", "count": 0})


@app.route('/thrift/calls/verify', methods=['POST'])
def verify_thrift_calls():
    """
    Verify that Thrift calls match expected patterns.

    Request body examples:

    1. Exact sequence match:
    {
        "type": "exact_sequence",
        "methods": ["ExecuteStatement", "FetchResults", "CloseOperation"]
    }

    2. Contains sequence (in order):
    {
        "type": "contains_sequence",
        "methods": ["ExecuteStatement", "FetchResults"]
    }

    3. Method count:
    {
        "type": "method_count",
        "method": "FetchResults",
        "count": 2
    }

    4. Method exists:
    {
        "type": "method_exists",
        "method": "ExecuteStatement"
    }
    """
    try:
        data = request.get_json(force=True, silent=True)
    except Exception:
        data = None

    if not data:
        return jsonify({"error": "Request body required"}), 400

    verification_type = data.get("type")
    if not verification_type:
        return jsonify({"error": "Verification type required"}), 400

    with state_lock:
        methods = [call["method"] for call in call_history]

    try:
        if verification_type == "exact_sequence":
            expected = data.get("methods", [])
            if methods == expected:
                return jsonify({"verified": True, "actual": methods, "expected": expected})
            else:
                return jsonify({"verified": False, "actual": methods, "expected": expected})

        elif verification_type == "contains_sequence":
            expected = data.get("methods", [])
            # Check if expected sequence appears in order (but not necessarily consecutive)
            idx = 0
            for method in methods:
                if idx < len(expected) and method == expected[idx]:
                    idx += 1
            verified = (idx == len(expected))
            return jsonify({"verified": verified, "actual": methods, "expected": expected})

        elif verification_type == "method_count":
            method_name = data.get("method")
            expected_count = data.get("count")
            if not method_name or expected_count is None:
                return jsonify({"error": "method and count required"}), 400
            actual_count = methods.count(method_name)
            verified = (actual_count == expected_count)
            return jsonify({
                "verified": verified,
                "method": method_name,
                "actual_count": actual_count,
                "expected_count": expected_count
            })

        elif verification_type == "method_exists":
            method_name = data.get("method")
            if not method_name:
                return jsonify({"error": "method required"}), 400
            verified = method_name in methods
            return jsonify({"verified": verified, "method": method_name, "actual": methods})

        else:
            return jsonify({"error": f"Unknown verification type: {verification_type}"}), 400

    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
        """Handle Thrift requests, log decoded messages, and track call history."""
        # Decode and log Thrift request
        if flow.request.content:
            decoded = decode_thrift_message(flow.request.content)
            if decoded and "error" not in decoded:
                formatted = format_thrift_message(decoded, max_field_length=100)
                ctx.log.info(f"[THRIFT REQUEST]\n{formatted}")

                # Track call in history
                with state_lock:
                    call_record = {
                        "timestamp": time.time(),
                        "method": decoded.get("method", "unknown"),
                        "message_type": decoded.get("message_type", "unknown"),
                        "sequence_id": decoded.get("sequence_id", 0),
                    }
                    call_history.append(call_record)

                    # Enforce max history limit
                    if len(call_history) > MAX_CALL_HISTORY:
                        # Remove oldest calls to stay within limit
                        del call_history[:len(call_history) - MAX_CALL_HISTORY]

            elif decoded:
                ctx.log.warn(f"[THRIFT REQUEST] Decode error: {decoded.get('error')}")

    def response(self, flow: http.HTTPFlow) -> None:
        """
        Intercept responses to log Thrift messages.
        Called by mitmproxy for each HTTP response.
        """
        if self._is_thrift_request(flow.request) and flow.response:
            if flow.response.content:
                decoded = decode_thrift_message(flow.response.content)
                if decoded and "error" not in decoded:
                    formatted = format_thrift_message(decoded, max_field_length=100)
                    ctx.log.info(f"[THRIFT RESPONSE]\n{formatted}")
                elif decoded:
                    ctx.log.warn(f"[THRIFT RESPONSE] Decode error: {decoded.get('error')}")

    def _disable_scenario(self, scenario_name: str) -> None:
        """Disable a scenario after one-shot injection."""
        with state_lock:
            enabled_scenarios[scenario_name] = False
        ctx.log.info(f"[INJECT] Auto-disabled scenario: {scenario_name}")


# Register addon with mitmproxy
addons = [FailureInjectionAddon()]
