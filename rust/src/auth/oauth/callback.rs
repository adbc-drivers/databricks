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

//! OAuth callback server for capturing authorization codes during U2M flow.
//!
//! This module implements a lightweight HTTP server that listens on localhost
//! for OAuth redirect callbacks from the browser. The server:
//! - Binds to a configurable port (default 8020)
//! - Validates the state parameter for CSRF protection
//! - Extracts the authorization code from query parameters
//! - Returns a simple HTML page to the browser
//! - Times out after a configurable duration (default 120s)

use crate::error::{DatabricksErrorHelper, Result};
use driverbase::error::ErrorHelper;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// HTML response sent to the browser after successful callback.
const SUCCESS_HTML: &str = r#"<!DOCTYPE html>
<html>
<head><title>Authentication Successful</title></head>
<body>
<h1>Authentication Successful</h1>
<p>You can close this tab and return to your application.</p>
</body>
</html>"#;

/// HTML response sent to the browser when an error occurs.
const ERROR_HTML: &str = r#"<!DOCTYPE html>
<html>
<head><title>Authentication Error</title></head>
<body>
<h1>Authentication Error</h1>
<p>An error occurred during authentication. You can close this tab and try again.</p>
</body>
</html>"#;

/// Result of an OAuth callback.
#[derive(Debug)]
enum CallbackResult {
    /// Successfully received authorization code with state.
    Success { code: String, state: String },
    /// Authorization server returned an error.
    AuthError {
        error: String,
        description: Option<String>,
    },
}

/// OAuth callback server for capturing authorization codes.
///
/// This server listens on localhost for OAuth redirect callbacks during the
/// U2M (authorization code + PKCE) flow. It validates the state parameter,
/// extracts the authorization code, and returns a simple HTML response to the browser.
///
/// # Example
///
/// ```no_run
/// use databricks_adbc::auth::oauth::callback::CallbackServer;
/// use std::time::Duration;
///
/// # async fn example() -> databricks_adbc::error::Result<()> {
/// // Start callback server on port 8020
/// let server = CallbackServer::new(8020).await?;
/// let redirect_uri = server.redirect_uri();
/// println!("Redirect URI: {}", redirect_uri);
///
/// // Open browser with authorization URL...
///
/// // Wait for callback (120 second timeout)
/// let code = server.wait_for_code("expected-state-value", Duration::from_secs(120)).await?;
/// println!("Received authorization code: {}", code);
/// # Ok(())
/// # }
/// ```
pub struct CallbackServer {
    /// Port the server is listening on.
    port: u16,
    /// Receiver for callback results.
    result_rx: Option<oneshot::Receiver<CallbackResult>>,
    /// Handle to the server task.
    server_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for CallbackServer {
    fn drop(&mut self) {
        // Abort the server task if it's still running (e.g., wait_for_code was never called)
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }
    }
}

impl CallbackServer {
    /// Creates a new callback server listening on the specified port.
    ///
    /// The server binds to `127.0.0.1:{port}` and is ready to receive one callback.
    ///
    /// # Arguments
    ///
    /// * `port` - Port number to listen on (typically 8020)
    ///
    /// # Returns
    ///
    /// A configured server ready to wait for callbacks, or an error if:
    /// - The port is already in use
    /// - Permission denied to bind to the port
    /// - Invalid port number
    ///
    /// # Errors
    ///
    /// Returns `io()` error if the server cannot bind to the specified port.
    pub async fn new(port: u16) -> Result<Self> {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().map_err(|e| {
            DatabricksErrorHelper::invalid_argument()
                .message(format!("Invalid port number {}: {}", port, e))
        })?;

        let listener = TcpListener::bind(addr).await.map_err(|e| {
            DatabricksErrorHelper::io()
                .message(format!("Failed to bind callback server to {}: {}", addr, e))
                .context("OAuth callback server")
        })?;

        // Get the actual port (in case port 0 was specified for automatic assignment)
        let actual_port = listener
            .local_addr()
            .map_err(|e| {
                DatabricksErrorHelper::io().message(format!("Failed to get local address: {}", e))
            })?
            .port();

        // Create channel for callback result
        let (result_tx, result_rx) = oneshot::channel();

        // Spawn the server task
        let server_handle = tokio::spawn(Self::run_server(listener, result_tx));

        Ok(Self {
            port: actual_port,
            result_rx: Some(result_rx),
            server_handle: Some(server_handle),
        })
    }

    /// Returns the redirect URI for this callback server.
    ///
    /// This URI should be used when constructing the OAuth authorization URL.
    /// The format is `http://localhost:{port}/callback`.
    pub fn redirect_uri(&self) -> String {
        format!("http://localhost:{}/callback", self.port)
    }

    /// Waits for the OAuth callback with the authorization code.
    ///
    /// This method blocks until:
    /// - A callback is received with a valid code and matching state
    /// - The timeout expires
    /// - The state parameter doesn't match (CSRF protection)
    ///
    /// # Arguments
    ///
    /// * `expected_state` - The state value to validate against (CSRF protection)
    /// * `timeout` - Maximum time to wait for callback
    ///
    /// # Returns
    ///
    /// The authorization code extracted from the callback, or an error if:
    /// - Timeout expires without receiving a callback
    /// - State parameter doesn't match the expected value
    /// - Required parameters are missing from the callback
    /// - The authorization server returned an error
    ///
    /// # Errors
    ///
    /// Returns `io()` error for timeout, network errors, or protocol violations.
    pub async fn wait_for_code(
        mut self,
        expected_state: &str,
        timeout: Duration,
    ) -> Result<String> {
        let result_rx = self.result_rx.take().ok_or_else(|| {
            DatabricksErrorHelper::io().message("Callback receiver already consumed")
        })?;

        // Wait for callback or timeout
        let callback_result = tokio::time::timeout(timeout, result_rx)
            .await
            .map_err(|_| {
                DatabricksErrorHelper::io()
                    .message(format!(
                        "OAuth callback timeout after {} seconds. No response received from browser.",
                        timeout.as_secs()
                    ))
                    .context("OAuth callback server")
            })?
            .map_err(|_| {
                DatabricksErrorHelper::io()
                    .message("Callback channel closed unexpectedly")
            })?;

        // Shutdown the server
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }

        // Process the callback result
        match callback_result {
            CallbackResult::Success { code, state } => {
                // Validate state parameter (CSRF protection)
                if state != expected_state {
                    return Err(DatabricksErrorHelper::io()
                        .message(format!(
                            "OAuth state mismatch (CSRF protection). Expected '{}', received '{}'",
                            expected_state, state
                        ))
                        .context("OAuth callback validation"));
                }
                Ok(code)
            }
            CallbackResult::AuthError { error, description } => {
                let msg = if let Some(desc) = description {
                    format!("OAuth authorization failed: {} - {}", error, desc)
                } else {
                    format!("OAuth authorization failed: {}", error)
                };
                Err(DatabricksErrorHelper::io()
                    .message(msg)
                    .context("OAuth authorization"))
            }
        }
    }

    /// Runs the HTTP server to handle callback requests.
    async fn run_server(listener: TcpListener, result_tx: oneshot::Sender<CallbackResult>) {
        // We only expect one callback, so we'll accept one connection and process it
        loop {
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    // Read the HTTP request
                    match Self::handle_request(&mut stream).await {
                        Ok(result) => {
                            // Send the result through the channel
                            let _ = result_tx.send(result);
                            break;
                        }
                        Err(e) => {
                            tracing::warn!("Failed to handle callback request: {}", e);
                            // Continue listening for another request
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to accept connection: {}", e);
                    break;
                }
            }
        }
    }

    /// Handles a single HTTP request and returns the callback result.
    async fn handle_request(stream: &mut tokio::net::TcpStream) -> Result<CallbackResult> {
        use tokio::io::AsyncReadExt;

        // Read the HTTP request (up to 8KB)
        let mut buffer = vec![0u8; 8192];
        let n = stream.read(&mut buffer).await.map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to read callback request: {}", e))
        })?;

        let request = String::from_utf8_lossy(&buffer[..n]);

        // Parse the request line (e.g., "GET /callback?code=...&state=... HTTP/1.1")
        let first_line = request.lines().next().unwrap_or("");
        let parts: Vec<&str> = first_line.split_whitespace().collect();

        if parts.len() < 2 {
            Self::send_response(stream, 400, ERROR_HTML).await?;
            return Err(DatabricksErrorHelper::io().message("Invalid HTTP request format"));
        }

        let path_and_query = parts[1];

        // Extract query parameters
        let query_params = if let Some(query_start) = path_and_query.find('?') {
            Self::parse_query(&path_and_query[query_start + 1..])
        } else {
            HashMap::new()
        };

        // Check for authorization server errors
        if let Some(error) = query_params.get("error") {
            let description = query_params.get("error_description").map(|s| s.to_string());
            let result = CallbackResult::AuthError {
                error: error.to_string(),
                description,
            };
            Self::send_response(stream, 400, ERROR_HTML).await?;
            return Ok(result);
        }

        // Extract code and state
        let code = query_params.get("code").ok_or_else(|| {
            DatabricksErrorHelper::io().message("Missing 'code' parameter in OAuth callback")
        })?;

        let state = query_params.get("state").ok_or_else(|| {
            DatabricksErrorHelper::io().message("Missing 'state' parameter in OAuth callback")
        })?;

        // Send success response to browser
        Self::send_response(stream, 200, SUCCESS_HTML).await?;

        // Return success result with code and state
        // Note: State validation happens in wait_for_code()
        Ok(CallbackResult::Success {
            code: code.to_string(),
            state: state.to_string(),
        })
    }

    /// Sends an HTTP response to the client.
    async fn send_response(
        stream: &mut tokio::net::TcpStream,
        status: u16,
        body: &str,
    ) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let status_text = match status {
            200 => "OK",
            400 => "Bad Request",
            _ => "Unknown",
        };

        let response = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            status, status_text, body.len(), body
        );

        stream.write_all(response.as_bytes()).await.map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to send HTTP response: {}", e))
        })?;

        stream.flush().await.map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to flush response: {}", e))
        })?;

        Ok(())
    }

    /// Parses URL query parameters from a query string.
    fn parse_query(query: &str) -> HashMap<String, String> {
        query
            .split('&')
            .filter_map(|part| {
                let mut split = part.splitn(2, '=');
                let key = split.next()?;
                let value = split.next().unwrap_or("");
                Some((
                    Self::url_decode(key).unwrap_or_else(|| key.to_string()),
                    Self::url_decode(value).unwrap_or_else(|| value.to_string()),
                ))
            })
            .collect()
    }

    /// URL-decodes a string.
    fn url_decode(s: &str) -> Option<String> {
        let mut result = String::new();
        let mut chars = s.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '%' => {
                    let hex: String = chars.by_ref().take(2).collect();
                    if hex.len() == 2 {
                        if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                            result.push(byte as char);
                        } else {
                            return None;
                        }
                    } else {
                        return None;
                    }
                }
                '+' => result.push(' '),
                _ => result.push(ch),
            }
        }

        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_callback_captures_code() {
        // Start callback server
        let server = CallbackServer::new(0)
            .await
            .expect("Failed to create server");
        let port = server.port;
        let expected_state = "test-state-12345";

        // Spawn task to simulate browser callback
        tokio::spawn(async move {
            // Give server time to start listening
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            // Simulate HTTP GET request with code and state
            let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .expect("Failed to connect to callback server");

            let request = format!(
                "GET /callback?code=test-auth-code-123&state={} HTTP/1.1\r\nHost: localhost:{}\r\n\r\n",
                expected_state, port
            );

            stream
                .write_all(request.as_bytes())
                .await
                .expect("Failed to write request");
            stream.flush().await.expect("Failed to flush");

            // Read the response
            let mut response = vec![0u8; 4096];
            let _ = stream.read(&mut response).await;
        });

        // Wait for callback
        let code = server
            .wait_for_code(expected_state, Duration::from_secs(5))
            .await
            .expect("Failed to receive callback");

        assert_eq!(code, "test-auth-code-123");
    }

    #[tokio::test]
    async fn test_callback_validates_state() {
        let server = CallbackServer::new(0)
            .await
            .expect("Failed to create server");
        let port = server.port;
        let expected_state = "expected-state-value";
        let wrong_state = "wrong-state-value";

        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .expect("Failed to connect");

            let request = format!(
                "GET /callback?code=auth-code&state={} HTTP/1.1\r\nHost: localhost:{}\r\n\r\n",
                wrong_state, port
            );

            stream.write_all(request.as_bytes()).await.ok();
            stream.flush().await.ok();

            let mut response = vec![0u8; 4096];
            let _ = stream.read(&mut response).await;
        });

        // Wait for callback - should fail due to state mismatch
        let result = server
            .wait_for_code(expected_state, Duration::from_secs(5))
            .await;

        assert!(
            result.is_err(),
            "Should reject callback with mismatched state"
        );
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(
            error_msg.contains("state mismatch") || error_msg.contains("State mismatch"),
            "Error should mention state mismatch: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_callback_timeout() {
        let server = CallbackServer::new(0)
            .await
            .expect("Failed to create server");

        // Don't send any callback - just wait for timeout
        let result = server
            .wait_for_code("any-state", Duration::from_secs(1))
            .await;

        assert!(result.is_err(), "Should timeout without callback");
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(
            error_msg.contains("timeout") || error_msg.contains("Timeout"),
            "Error should mention timeout: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_callback_missing_code() {
        let server = CallbackServer::new(0)
            .await
            .expect("Failed to create server");
        let port = server.port;

        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .expect("Failed to connect");

            // Send callback without 'code' parameter
            let request = format!(
                "GET /callback?state=test-state HTTP/1.1\r\nHost: localhost:{}\r\n\r\n",
                port
            );

            stream.write_all(request.as_bytes()).await.ok();
            stream.flush().await.ok();
        });

        let result = server
            .wait_for_code("test-state", Duration::from_secs(5))
            .await;

        assert!(result.is_err(), "Should fail without code parameter");
    }

    #[tokio::test]
    async fn test_callback_auth_error() {
        let server = CallbackServer::new(0)
            .await
            .expect("Failed to create server");
        let port = server.port;

        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .expect("Failed to connect");

            // Simulate authorization server error response
            let request = format!(
                "GET /callback?error=access_denied&error_description=User%20denied%20consent HTTP/1.1\r\nHost: localhost:{}\r\n\r\n",
                port
            );

            stream.write_all(request.as_bytes()).await.ok();
            stream.flush().await.ok();
        });

        let result = server
            .wait_for_code("test-state", Duration::from_secs(5))
            .await;

        assert!(result.is_err(), "Should fail with auth error");
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(
            error_msg.contains("access_denied"),
            "Error should contain error code: {}",
            error_msg
        );
    }

    #[test]
    fn test_url_decode() {
        assert_eq!(
            CallbackServer::url_decode("hello%20world").unwrap(),
            "hello world"
        );
        assert_eq!(
            CallbackServer::url_decode("test+string").unwrap(),
            "test string"
        );
        assert_eq!(CallbackServer::url_decode("simple").unwrap(), "simple");
    }

    #[test]
    fn test_parse_query() {
        let params = CallbackServer::parse_query("code=abc123&state=xyz789");
        assert_eq!(params.get("code").unwrap(), "abc123");
        assert_eq!(params.get("state").unwrap(), "xyz789");

        let params2 =
            CallbackServer::parse_query("error=access_denied&error_description=User%20denied");
        assert_eq!(params2.get("error").unwrap(), "access_denied");
        assert_eq!(params2.get("error_description").unwrap(), "User denied");
    }

    #[tokio::test]
    async fn test_redirect_uri_format() {
        // Use port 0 to let the OS assign an available port, avoiding conflicts
        let server = CallbackServer::new(0)
            .await
            .expect("Failed to create server");
        let uri = server.redirect_uri();
        assert!(
            uri.starts_with("http://localhost:"),
            "Expected URI starting with http://localhost:, got: {}",
            uri
        );
        // Verify the port is a valid non-zero number
        let port: u16 = uri
            .strip_prefix("http://localhost:")
            .unwrap()
            .strip_suffix("/callback")
            .unwrap_or_else(|| uri.strip_prefix("http://localhost:").unwrap())
            .parse()
            .unwrap();
        assert!(port > 0, "Expected a non-zero port, got: {}", port);
    }
}
