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

import threading
import time
from typing import Any, Dict, List

from flask import Flask, jsonify, request
from mitmproxy import ctx, http
from thrift_decoder import decode_thrift_message, format_thrift_message

# Flask app for control API
app = Flask(__name__)

# Global state for enabled scenarios (thread-safe with lock)
state_lock = threading.Lock()
enabled_scenarios: Dict[str, bool] = {}

# Call count tracking for trigger_after_count scenarios
scenario_call_counts: Dict[str, int] = {}

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
    "cloudfetch_400": {
        "description": "CloudFetch returns 400 Bad Request (malformed request or missing parameters)",
        "operation": "CloudFetchDownload",
        "action": "return_error",
        "error_code": 400,
        "error_message": "Bad Request",
    },
    "cloudfetch_403": {
        "description": "CloudFetch returns 403 Forbidden (expired link or insufficient permissions)",
        "operation": "CloudFetchDownload",
        "action": "return_error",
        "error_code": 403,
        "error_message": "Forbidden",
    },
    "cloudfetch_404": {
        "description": "CloudFetch returns 404 Not Found (object does not exist)",
        "operation": "CloudFetchDownload",
        "action": "return_error",
        "error_code": 404,
        "error_message": "Not Found",
    },
    "cloudfetch_405": {
        "description": "CloudFetch returns 405 Method Not Allowed (incorrect HTTP method)",
        "operation": "CloudFetchDownload",
        "action": "return_error",
        "error_code": 405,
        "error_message": "Method Not Allowed",
    },
    "cloudfetch_412": {
        "description": "CloudFetch returns 412 Precondition Failed (condition not met)",
        "operation": "CloudFetchDownload",
        "action": "return_error",
        "error_code": 412,
        "error_message": "Precondition Failed",
    },
    "cloudfetch_500": {
        "description": "CloudFetch returns 500 Internal Server Error (server-side error)",
        "operation": "CloudFetchDownload",
        "action": "return_error",
        "error_code": 500,
        "error_message": "Internal Server Error",
    },
    "cloudfetch_503": {
        "description": "CloudFetch returns 503 Service Unavailable (rate limiting or temporary failure)",
        "operation": "CloudFetchDownload",
        "action": "return_error",
        "error_code": 503,
        "error_message": "Service Unavailable",
    },
    "cloudfetch_timeout": {
        "description": "CloudFetch download times out (exceeds 60s) - configurable delay",
        "operation": "CloudFetchDownload",
        "action": "delay",
        "duration_seconds": 65,  # Changed from "65s" string to integer
    },
    "cloudfetch_connection_reset": {
        "description": "Connection reset during CloudFetch download",
        "operation": "CloudFetchDownload",
        "action": "close_connection",
    },
    # Session Lifecycle Scenarios
    "invalid_session_handle": {
        "description": "Server returns invalid session handle error on next ExecuteStatement",
        "operation": "ExecuteStatement",
        "action": "return_thrift_error",
        "error_type": "INVALID_HANDLE",
        "error_message": "Invalid or expired session handle",
        # No trigger_after_count needed - test enables scenario after first statement
    },
    "session_timeout_premature": {
        "description": "Session expires before idle timeout",
        "operation": "ThriftOperation",
        "action": "return_thrift_error",
        "error_type": "SESSION_EXPIRED",
        "error_message": "Session timed out due to inactivity",
    },
    "expired_credentials": {
        "description": "Authentication fails due to expired credentials",
        "operation": "OpenSession",
        "action": "return_auth_error",
        "error_type": "INVALID_AUTHORIZATION_SPECIFICATION",
        "error_message": "Token has expired",
    },
    "network_timeout_open_session": {
        "description": "Network timeout during OpenSession request",
        "operation": "OpenSession",
        "action": "delay",
        "duration_seconds": 35,
    },
    "network_failure_close_session": {
        "description": "Network failure during CloseSession request",
        "operation": "CloseSession",
        "action": "close_connection",
    },
    "service_unavailable_503_open_session": {
        "description": "OpenSession returns 503 Service Unavailable (transient error, driver should retry)",
        "operation": "OpenSession",
        "action": "return_error",
        "error_code": 503,
        "error_message": "Service Unavailable",
    },
    "request_timeout_408_open_session": {
        "description": "OpenSession returns 408 Request Timeout (transient error, driver should retry)",
        "operation": "OpenSession",
        "action": "return_error",
        "error_code": 408,
        "error_message": "Request Timeout",
    },
    "bad_gateway_502_open_session": {
        "description": "OpenSession returns 502 Bad Gateway (transient error, driver should retry)",
        "operation": "OpenSession",
        "action": "return_error",
        "error_code": 502,
        "error_message": "Bad Gateway",
    },
    "gateway_timeout_504_open_session": {
        "description": "OpenSession returns 504 Gateway Timeout (transient error, driver should retry)",
        "operation": "OpenSession",
        "action": "return_error",
        "error_code": 504,
        "error_message": "Gateway Timeout",
    },
    "too_many_requests_429_open_session": {
        "description": "OpenSession returns 429 Too Many Requests (rate limit, driver should retry with separate timeout)",
        "operation": "OpenSession",
        "action": "return_error",
        "error_code": 429,
        "error_message": "Too Many Requests",
    },
}


# ===== Control API Endpoints =====


@app.route("/scenarios", methods=["GET"])
def list_scenarios():
    """List all available scenarios with their status."""
    with state_lock:
        scenarios_list = [
            {
                "name": name,
                "description": config["description"],
                "enabled": name in enabled_scenarios
                and enabled_scenarios[name] is not False,
            }
            for name, config in SCENARIOS.items()
        ]
    return jsonify({"scenarios": scenarios_list})


@app.route("/scenarios/<scenario_name>/enable", methods=["POST"])
def enable_scenario(scenario_name):
    """
    Enable a failure scenario and auto-reset call history.

    Optional request body for configurable scenarios:
    {
        "duration_seconds": 30  // For delay scenarios (overrides default)
    }
    """
    if scenario_name not in SCENARIOS:
        return jsonify({"error": f"Scenario not found: {scenario_name}"}), 404

    # Check for runtime configuration
    try:
        data = request.get_json(force=True, silent=True)
    except Exception:
        data = None

    scenario_config = SCENARIOS[scenario_name].copy()

    # Apply runtime overrides for configurable parameters
    if data:
        if "duration_seconds" in data and scenario_config.get("action") == "delay":
            scenario_config["duration_seconds"] = int(data["duration_seconds"])
            ctx.log.info(f"[API] Override delay duration: {data['duration_seconds']}s")

    with state_lock:
        # Store the potentially modified config
        enabled_scenarios[scenario_name] = scenario_config
        # Auto-reset call history when scenario is enabled (new test scenario)
        call_history.clear()
        # Reset call counts for trigger_after_count scenarios
        scenario_call_counts.clear()

    ctx.log.info(f"[API] Enabled scenario: {scenario_name}, reset call history")
    return jsonify(
        {
            "scenario": scenario_name,
            "enabled": True,
            "config": scenario_config,
            "call_history_reset": True,
        }
    )


@app.route("/scenarios/<scenario_name>/disable", methods=["POST"])
def disable_scenario(scenario_name):
    """Disable a failure scenario."""
    if scenario_name not in SCENARIOS:
        return jsonify({"error": f"Scenario not found: {scenario_name}"}), 404

    with state_lock:
        enabled_scenarios[scenario_name] = False

    ctx.log.info(f"[API] Disabled scenario: {scenario_name}")
    return jsonify({"scenario": scenario_name, "enabled": False})


@app.route("/scenarios/<scenario_name>/status", methods=["GET"])
def get_scenario_status(scenario_name):
    """Get status of a specific scenario."""
    if scenario_name not in SCENARIOS:
        return jsonify({"error": f"Scenario not found: {scenario_name}"}), 404

    with state_lock:
        enabled_config = enabled_scenarios.get(scenario_name, False)
        enabled = enabled_config is not False

    return jsonify(
        {
            "name": scenario_name,
            "description": SCENARIOS[scenario_name]["description"],
            "enabled": enabled,
            "config": enabled_config if enabled else None,
        }
    )


@app.route("/scenarios/disable-all", methods=["POST"])
def disable_all_scenarios():
    """Disable all failure scenarios."""
    with state_lock:
        for scenario_name in SCENARIOS.keys():
            enabled_scenarios[scenario_name] = False

    ctx.log.info("[API] Disabled all scenarios")
    return jsonify({"message": "All scenarios disabled"})


@app.route("/thrift/calls", methods=["GET"])
def get_thrift_calls():
    """Get history of Thrift method calls."""
    with state_lock:
        return jsonify(
            {
                "calls": call_history.copy(),
                "count": len(call_history),
                "max_history": MAX_CALL_HISTORY,
            }
        )


@app.route("/thrift/calls/reset", methods=["POST"])
def reset_thrift_calls():
    """Reset Thrift call history."""
    with state_lock:
        call_history.clear()

    ctx.log.info("[API] Reset Thrift call history")
    return jsonify({"message": "Call history reset", "count": 0})


@app.route("/thrift/calls/verify", methods=["POST"])
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
                return jsonify(
                    {"verified": True, "actual": methods, "expected": expected}
                )
            else:
                return jsonify(
                    {"verified": False, "actual": methods, "expected": expected}
                )

        elif verification_type == "contains_sequence":
            expected = data.get("methods", [])
            # Check if expected sequence appears in order (but not necessarily consecutive)
            idx = 0
            for method in methods:
                if idx < len(expected) and method == expected[idx]:
                    idx += 1
            verified = idx == len(expected)
            return jsonify(
                {"verified": verified, "actual": methods, "expected": expected}
            )

        elif verification_type == "method_count":
            method_name = data.get("method")
            expected_count = data.get("count")
            if not method_name or expected_count is None:
                return jsonify({"error": "method and count required"}), 400
            actual_count = methods.count(method_name)
            verified = actual_count == expected_count
            return jsonify(
                {
                    "verified": verified,
                    "method": method_name,
                    "actual_count": actual_count,
                    "expected_count": expected_count,
                }
            )

        elif verification_type == "method_exists":
            method_name = data.get("method")
            if not method_name:
                return jsonify({"error": "method required"}), 400
            verified = method_name in methods
            return jsonify(
                {"verified": verified, "method": method_name, "actual": methods}
            )

        else:
            return jsonify(
                {"error": f"Unknown verification type: {verification_type}"}
            ), 400

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
            # Disable reloader and use production mode for faster startup
            app.run(
                host="0.0.0.0",
                port=18081,
                threaded=True,
                use_reloader=False,
                debug=False,
            )

        api_thread = threading.Thread(target=run_api, daemon=True, name="ControlAPI")
        api_thread.start()
        ctx.log.info("Control API started on http://0.0.0.0:18081")

    async def request(self, flow: http.HTTPFlow) -> None:
        """
        Intercept requests and inject failures based on enabled scenarios.
        Called by mitmproxy for each HTTP request.
        Made async to support non-blocking delays.
        """
        # Detect request type
        if self._is_cloudfetch_download(flow.request):
            # Track cloud fetch download
            with state_lock:
                call_record = {
                    "timestamp": time.time(),
                    "type": "cloud_download",
                    "url": flow.request.pretty_url,
                }
                call_history.append(call_record)

                # Enforce max history limit
                if len(call_history) > MAX_CALL_HISTORY:
                    del call_history[: len(call_history) - MAX_CALL_HISTORY]

            await self._handle_cloudfetch_request(flow)
        elif self._is_thrift_request(flow.request):
            self._handle_thrift_request(flow)
            # Check for session-related failure scenarios
            await self._handle_thrift_session_scenarios(flow)

    def _is_cloudfetch_download(self, request: http.Request) -> bool:
        """Detect if this is a CloudFetch download to cloud storage."""
        if request.method != "GET":
            return False

        host = request.pretty_host.lower()
        return (
            "blob.core.windows.net" in host
            or "s3.amazonaws.com" in host
            or "storage.googleapis.com" in host
        )

    def _is_thrift_request(self, request: http.Request) -> bool:
        """
        Detect if this is a Thrift request to Databricks SQL warehouse.

        Distinguishes Thrift API from SEA (SQL Execution API):
        - Thrift: POST /sql/1.0/warehouses/{warehouse_id} or POST /sql/1.0/endpoints/{endpoint_id}
        - SEA: POST /api/2.0/sql/statements
        """
        if request.method != "POST":
            return False

        # Thrift requests use /sql/1.0/warehouses/ or /sql/1.0/endpoints/ paths
        # SEA requests use /api/2.0/sql/statements path
        return (
            "/sql/1.0/warehouses/" in request.path
            or "/sql/1.0/endpoints/" in request.path
        )

    async def _handle_cloudfetch_request(self, flow: http.HTTPFlow) -> None:
        """Handle CloudFetch requests and inject failures if scenario is enabled."""
        with state_lock:
            # Find first enabled CloudFetch scenario
            enabled_scenario = None
            for name in enabled_scenarios:
                scenario_config = enabled_scenarios[name]
                if scenario_config is not False:
                    # scenario_config is now the full config dict
                    base_config = SCENARIOS.get(name, {})
                    if base_config.get("operation") == "CloudFetchDownload":
                        enabled_scenario = (name, scenario_config)
                        break

        if not enabled_scenario:
            return  # No scenario enabled, let request proceed normally

        scenario_name, scenario_config = enabled_scenario
        ctx.log.info(
            f"[INJECT] Triggering scenario: {scenario_name} for {flow.request.pretty_url}"
        )

        # Inject failure based on action
        action = scenario_config["action"]

        if action == "expire_cloud_link":
            # Return 403 with Azure expired signature error
            flow.response = http.Response.make(
                403,
                b"AuthorizationQueryParametersError: Query Parameters are not supported for this operation",
                {"Content-Type": "text/plain"},
            )
            self._disable_scenario(scenario_name)

        elif action == "return_error":
            # Return HTTP error with specified code and message
            error_code = scenario_config.get("error_code", 500)
            error_message = scenario_config.get(
                "error_message", "Internal Server Error"
            )
            flow.response = http.Response.make(
                error_code,
                error_message.encode("utf-8"),
                {"Content-Type": "text/plain"},
            )
            self._disable_scenario(scenario_name)

        elif action == "delay":
            # Inject delay using asyncio.sleep() to avoid blocking the event loop
            import asyncio

            duration_seconds = scenario_config.get("duration_seconds", 5)
            ctx.log.info(
                f"[INJECT] Delaying {duration_seconds}s for scenario: {scenario_name}"
            )
            # Disable BEFORE the delay so new requests don't trigger this scenario
            self._disable_scenario(scenario_name)
            await asyncio.sleep(duration_seconds)
            ctx.log.info(f"[INJECT] Delay complete for scenario: {scenario_name}")
            # Let request continue after delay

        elif action == "close_connection":
            # Kill the connection abruptly
            flow.response = http.Response.make(
                500, b"Connection reset by peer", {"Content-Type": "text/plain"}
            )
            flow.kill()
            self._disable_scenario(scenario_name)

    async def _handle_thrift_session_scenarios(self, flow: http.HTTPFlow) -> None:
        """Handle Thrift session-related failure scenarios."""
        # Decode the Thrift request to determine the operation type
        if not flow.request.content:
            return

        decoded = decode_thrift_message(flow.request.content)
        if not decoded or "error" in decoded:
            return

        method_name = decoded.get("method", "")

        # Find enabled scenario that matches this Thrift operation
        with state_lock:
            enabled_scenario = None
            for name in enabled_scenarios:
                scenario_config = enabled_scenarios[name]
                if scenario_config is not False:
                    base_config = SCENARIOS.get(name, {})
                    operation = base_config.get("operation", "")

                    # Check if this scenario matches the operation
                    if operation == "ThriftOperation" or operation == method_name:
                        # Check trigger_after_count if specified
                        trigger_after = base_config.get("trigger_after_count", 0)
                        if trigger_after > 0:
                            # Track call count for this scenario + method combination
                            key = f"{name}:{method_name}"
                            current_count = scenario_call_counts.get(key, 0)
                            scenario_call_counts[key] = current_count + 1

                            # Only trigger if we've reached the threshold
                            if scenario_call_counts[key] <= trigger_after:
                                continue  # Skip this scenario for now

                        enabled_scenario = (name, scenario_config, base_config)
                        break

        if not enabled_scenario:
            return  # No matching scenario enabled

        scenario_name, scenario_config, base_config = enabled_scenario
        action = base_config.get("action", "")

        ctx.log.info(
            f"[INJECT] Triggering Thrift scenario: {scenario_name} for method: {method_name}"
        )

        if action == "return_thrift_error":
            # Create a Thrift error response
            # For simplicity, return HTTP 500 with error message
            # A full implementation would construct proper Thrift error response
            error_message = base_config.get("error_message", "Thrift operation failed")
            error_type = base_config.get("error_type", "UNKNOWN_ERROR")

            flow.response = http.Response.make(
                500,
                f"Thrift Error [{error_type}]: {error_message}".encode("utf-8"),
                {"Content-Type": "application/x-thrift"},
            )
            self._disable_scenario(scenario_name)

        elif action == "return_auth_error":
            # Return HTTP 401 for authentication failures (non-retryable)
            error_message = base_config.get("error_message", "Authentication failed")
            error_type = base_config.get("error_type", "UNAUTHORIZED")

            flow.response = http.Response.make(
                401,
                f"Authentication Error [{error_type}]: {error_message}".encode("utf-8"),
                {"Content-Type": "application/x-thrift"},
            )
            self._disable_scenario(scenario_name)

        elif action == "delay":
            # Inject delay for slow operations
            import asyncio

            duration_seconds = base_config.get("duration_seconds", 5)
            ctx.log.info(
                f"[INJECT] Delaying {duration_seconds}s for Thrift scenario: {scenario_name}"
            )
            self._disable_scenario(scenario_name)
            await asyncio.sleep(duration_seconds)
            ctx.log.info(
                f"[INJECT] Delay complete for Thrift scenario: {scenario_name}"
            )

        elif action == "return_error":
            # Return HTTP error with specified code and message
            error_code = base_config.get("error_code", 500)
            error_message = base_config.get(
                "error_message", "Internal Server Error"
            )
            flow.response = http.Response.make(
                error_code,
                error_message.encode("utf-8"),
                {"Content-Type": "text/plain"},
            )
            self._disable_scenario(scenario_name)

        elif action == "close_connection":
            # Kill the connection abruptly
            flow.response = http.Response.make(
                500, b"Connection reset by peer", {"Content-Type": "text/plain"}
            )
            flow.kill()
            self._disable_scenario(scenario_name)

        elif action == "track_active_operations":
            # For CloseSession with active operations
            # This is a passive tracking scenario - log but don't block
            ctx.log.info(
                f"[INJECT] Tracking scenario: {scenario_name} - CloseSession called"
            )
            # Don't disable - let the request proceed normally
            # Tests can verify behavior via call tracking

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
                        "type": "thrift",
                        "method": decoded.get("method", "unknown"),
                        "message_type": decoded.get("message_type", "unknown"),
                        "sequence_id": decoded.get("sequence_id", 0),
                        "fields": decoded.get("fields", {}),
                    }
                    call_history.append(call_record)

                    # Enforce max history limit
                    if len(call_history) > MAX_CALL_HISTORY:
                        # Remove oldest calls to stay within limit
                        del call_history[: len(call_history) - MAX_CALL_HISTORY]

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
                    ctx.log.warn(
                        f"[THRIFT RESPONSE] Decode error: {decoded.get('error')}"
                    )

    def _disable_scenario(self, scenario_name: str) -> None:
        """Disable a scenario after one-shot injection."""
        with state_lock:
            enabled_scenarios[scenario_name] = False
        ctx.log.info(f"[INJECT] Auto-disabled scenario: {scenario_name}")


# Register addon with mitmproxy
addons = [FailureInjectionAddon()]
