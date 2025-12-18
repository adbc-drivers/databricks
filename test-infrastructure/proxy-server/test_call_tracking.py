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
Unit tests for call tracking and verification functionality.
Tests the API endpoints for tracking Thrift method calls.
"""

import json
import urllib.request
import urllib.parse
import urllib.error
from typing import List

# Control API base URL
API_BASE = "http://localhost:18081"


def http_request(url: str, method: str = "GET", data: dict = None) -> dict:
    """Make HTTP request and return JSON response."""
    req_data = None
    headers = {}

    # For POST requests, always set Content-Type to application/json
    if method == "POST":
        headers['Content-Type'] = 'application/json'
        if data is not None:
            req_data = json.dumps(data).encode('utf-8')

    req = urllib.request.Request(url, data=req_data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        # Return error response for validation testing
        error_body = e.read().decode('utf-8')
        try:
            return json.loads(error_body)
        except json.JSONDecodeError:
            raise Exception(f"HTTP {e.code}: {error_body}")


def get_calls() -> dict:
    """Get current call history."""
    return http_request(f"{API_BASE}/thrift/calls")


def reset_calls() -> dict:
    """Reset call history."""
    return http_request(f"{API_BASE}/thrift/calls/reset", method="POST")


def verify_calls(verification_type: str, **kwargs) -> dict:
    """Verify call sequence."""
    payload = {"type": verification_type, **kwargs}
    return http_request(f"{API_BASE}/thrift/calls/verify", method="POST", data=payload)


def test_call_tracking_basics():
    """Test that call tracking endpoints work correctly."""
    print("Testing call tracking basics...")

    # Reset to start fresh
    result = reset_calls()
    print(f"✓ Reset calls: {result}")
    assert result["count"] == 0

    # Get empty history
    history = get_calls()
    print(f"✓ Empty history: {history}")
    assert history["count"] == 0
    assert len(history["calls"]) == 0
    assert history["max_history"] == 1000

    print("✓ Call tracking basics work correctly\n")


def test_verification_types():
    """Test different verification types with mock data."""
    print("Testing verification types...")

    # For these tests, we'll manually construct verification requests
    # Since we can't easily inject mock Thrift calls without starting the full proxy

    # Test error cases
    # 1. Empty body
    error = http_request(f"{API_BASE}/thrift/calls/verify", method="POST", data=None)
    print(f"  Empty body error: {error}")
    assert "error" in error and "body required" in error["error"].lower()
    print("✓ Empty body returns error")

    # 2. Missing type field
    error = http_request(f"{API_BASE}/thrift/calls/verify", method="POST", data={"foo": "bar"})
    print(f"  Missing type error: {error}")
    assert "error" in error and "type" in error["error"].lower()
    print("✓ Missing type returns error")

    # 3. Unknown type
    error = http_request(
        f"{API_BASE}/thrift/calls/verify",
        method="POST",
        data={"type": "unknown_type"}
    )
    assert "error" in error and "unknown" in error["error"].lower()
    print("✓ Unknown type returns error")

    # 4. Method count missing parameters
    error = http_request(
        f"{API_BASE}/thrift/calls/verify",
        method="POST",
        data={"type": "method_count"}
    )
    assert "error" in error
    print("✓ Method count without params returns error")

    # 5. Method exists missing parameters
    error = http_request(
        f"{API_BASE}/thrift/calls/verify",
        method="POST",
        data={"type": "method_exists"}
    )
    assert "error" in error
    print("✓ Method exists without method returns error")

    print("✓ All verification types validated\n")


def test_auto_reset_on_scenario_enable():
    """Test that call history auto-resets when a scenario is enabled."""
    print("Testing auto-reset on scenario enable...")

    # First, list available scenarios
    scenarios = http_request(f"{API_BASE}/scenarios")
    print(f"Available scenarios: {len(scenarios['scenarios'])}")

    # Pick first scenario
    if scenarios["scenarios"]:
        scenario_name = scenarios["scenarios"][0]["name"]
        print(f"Using scenario: {scenario_name}")

        # Enable scenario (should auto-reset call history)
        result = http_request(f"{API_BASE}/scenarios/{scenario_name}/enable", method="POST")
        print(f"✓ Enabled scenario: {result}")
        assert result["call_history_reset"] is True

        # Verify call history is empty
        history = get_calls()
        print(f"✓ Call history after scenario enable: {history}")
        assert history["count"] == 0

        # Disable scenario
        http_request(f"{API_BASE}/scenarios/{scenario_name}/disable", method="POST")
        print("✓ Disabled scenario")

    print("✓ Auto-reset on scenario enable works correctly\n")


def main():
    """Run all tests."""
    print("=" * 60)
    print("Call Tracking API Tests")
    print("=" * 60)
    print()

    try:
        # Check if proxy is running
        http_request(f"{API_BASE}/scenarios")
    except Exception as e:
        print(f"❌ Error: Proxy not running on {API_BASE}")
        print(f"   Please start the proxy first: make start-proxy")
        print(f"   Error: {e}")
        return 1

    try:
        test_call_tracking_basics()
        test_verification_types()
        test_auto_reset_on_scenario_enable()

        print("=" * 60)
        print("✅ All tests passed!")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
