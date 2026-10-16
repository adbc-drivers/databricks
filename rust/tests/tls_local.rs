// Copyright (c) 2026 ADBC Drivers Contributors
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

//! Exercise TLS options against a local HTTPS server without workspace credentials.

use databricks_adbc::client::http::TlsConfig;
use databricks_adbc::{DatabricksHttpClient, HttpClientConfig};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::{rustls, TlsAcceptor};

async fn check_tls(
    certificate_name: &str,
    mut tls: TlsConfig,
    trust_certificate: bool,
    expected_success: bool,
) {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec![certificate_name.to_string()]).unwrap();
    let certificate_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(certificate_file.path(), cert.pem()).unwrap();
    if trust_certificate {
        tls.trusted_certificate_path = Some(certificate_file.path().display().to_string());
    }

    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into(),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        // A failed handshake is expected when the client rejects the certificate.
        if let Ok(mut stream) = acceptor.accept(socket).await {
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
            stream.shutdown().await.unwrap();
        }
    });

    let mut config = HttpClientConfig {
        tls,
        connect_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    // Keep these local tests independent of proxy environment variables.
    config.proxy.url = Some("http://127.0.0.1:9".to_string());
    config.proxy.bypass_hosts = Some("127.0.0.1".to_string());
    let client = DatabricksHttpClient::with_default_retry(config).unwrap();
    let result = client
        .inner()
        .get(format!("https://{address}/"))
        .send()
        .await;
    if expected_success {
        let response = result.expect("trusted TLS connection should succeed");
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.unwrap(), "ok");
    } else {
        let error = result.expect_err("invalid certificate should be rejected");
        assert!(error.is_connect(), "{error:?}");
        assert!(
            format!("{error:?}").contains("InvalidCertificate"),
            "{error:?}"
        );
    }
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("TLS server should finish")
        .unwrap();
}

#[tokio::test]
async fn rejects_untrusted_certificate() {
    check_tls("127.0.0.1", TlsConfig::default(), false, false).await;
}

#[tokio::test]
async fn accepts_custom_certificate() {
    check_tls("127.0.0.1", TlsConfig::default(), true, true).await;
}

#[tokio::test]
async fn accepts_self_signed_when_enabled() {
    let tls = TlsConfig {
        allow_self_signed: Some(true),
        ..Default::default()
    };
    check_tls("127.0.0.1", tls, false, true).await;
}

#[tokio::test]
async fn rejects_hostname_mismatch() {
    check_tls("different.example", TlsConfig::default(), true, false).await;
}

#[tokio::test]
async fn accepts_hostname_mismatch_when_enabled() {
    let tls = TlsConfig {
        allow_hostname_mismatch: Some(true),
        ..Default::default()
    };
    check_tls("different.example", tls, true, true).await;
}

#[tokio::test]
async fn allowing_hostname_mismatch_still_requires_trusted_certificate() {
    let tls = TlsConfig {
        allow_hostname_mismatch: Some(true),
        ..Default::default()
    };
    check_tls("different.example", tls, false, false).await;
}
