//! Fixed upload formats and authentication retries use an owned loopback peer.
use super::*;
use crate::core::live_sync::{CloudEvent, EventKind, EventPriority, LatencyTrace};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) async fn request(listener: &tokio::net::TcpListener) -> (tokio::net::TcpStream, String, Vec<u8>) {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut input = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        input.extend_from_slice(&buffer[..count]);
        assert!(input.len() < 256 * 1024);
        if let Some(end) = input.windows(4).position(|s| s == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&input[..end]).to_lowercase();
            let length: usize = headers
                .lines()
                .find_map(|s| s.strip_prefix("content-length:"))
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            if input.len() >= end + 4 + length {
                return (socket, headers, input[end + 4..end + 4 + length].to_vec());
            }
        }
    }
}
pub(super) async fn reply(mut socket: tokio::net::TcpStream, status: u16, body: serde_json::Value) {
    let body = body.to_string();
    socket.write_all(format!("HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
}

async fn case(events: usize, statuses: &[u16], expected: &[u16]) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let at = chrono::DateTime::parse_from_rfc3339("2026-10-26T12:34:56.123456789Z")
        .unwrap()
        .to_utc();
    let batch = LiveSyncBatch::from_events(
        (0..events)
            .map(|n| CloudEvent {
                protocol_version: 2,
                event_id: uuid::Uuid::from_u128(n as u128 + 1),
                runtime_id: "runtime".into(),
                strategy_id: "strategy".into(),
                live_session_id: "session".into(),
                run_id: Some("run".into()),
                entity_id: format!("entity:123:{n}"),
                entity_revision: 1,
                critical_sequence: Some(n as u64 + 1),
                priority: EventPriority::Critical,
                kind: EventKind::InsightProjection,
                payload: json!({"integer":u64::MAX,"float":-0.0,"text":"العربية 中文 한국어"}),
                latency: LatencyTrace::default(),
                created_at: at,
            })
            .collect(),
    );
    let canonical = serde_json::to_vec(&batch).unwrap();
    let protobuf = events <= 25;
    let expected_wire = if protobuf {
        aqe_live_protocol::protobuf::encode(&batch).unwrap()
    } else {
        canonical.clone()
    };
    let statuses = statuses.to_vec();
    let server = tokio::spawn(async move {
        let mut token_number = 0;
        for (n, &status) in statuses.iter().enumerate() {
            if n == 0 || statuses[n - 1] == 401 {
                let (socket, headers, _) = request(&listener).await;
                assert!(headers.starts_with("post /v1/register "));
                token_number += 1;
                // No format negotiation metadata is required or sent.
                reply(
                    socket,
                    200,
                    json!({"token":format!("owned-token-{token_number}"),
                    "expiresAtUs":now_epoch_micros()+60_000_000}),
                )
                .await;
            }
            let (socket, headers, bytes) = request(&listener).await;
            assert!(headers.starts_with("post /v1/batch "));
            assert!(headers.contains(&format!("authorization: bearer owned-token-{token_number}")));
            assert_eq!(
                headers.contains(aqe_live_protocol::protobuf::MIME),
                protobuf
            );
            assert_eq!(
                bytes, expected_wire,
                "retries/caller resubmission must keep exact wire format"
            );
            let restored: LiveSyncBatch = if protobuf {
                aqe_live_protocol::protobuf::decode(&bytes).unwrap()
            } else {
                serde_json::from_slice(&bytes).unwrap()
            };
            assert_eq!(serde_json::to_vec(&restored).unwrap(), canonical);
            reply(socket, status, if status == 202 {
                json!({"persistedCriticalThrough":0,"acceptedCriticalThrough":events,"durability":"cloud_stream"})
            } else { json!({"error":"owned rejection"}) }).await;
        }
    });
    let connection = GatewayConnection::new(
        &format!("http://{address}"),
        HttpBatchSinkConfig {
            batch_url: format!("http://{address}/unused"),
            actions_url: None,
            bearer_token: String::new(),
            session_secret: Some("owned-secret".into()),
            signin_url: None,
            namespace: "fixture".into(),
            database: "fixture".into(),
            access_method: "aqe_live".into(),
            user_id: "users:fixture".into(),
            strategy_id: "strategy".into(),
            live_session_id: "session".into(),
            runtime_id: "runtime".into(),
            run_id: "run".into(),
            server_node_id: "local".into(),
            request_timeout: Duration::from_secs(2),
        },
    )
    .unwrap();
    for &status in expected {
        let result = connection.publish(&batch).await;
        if status == 202 {
            let result = result.unwrap();
            assert_eq!(result.accepted_critical_through, Some(events as u64));
            assert_eq!(result.persisted_critical_through, 0);
        } else {
            let error = result.unwrap_err();
            assert!(error.message.contains(&format!("HTTP {status}")));
            if matches!(status, 408 | 429) {
                assert_eq!(error.kind, RemoteSyncErrorKind::Transient,
                    "upload deadlines and busy admission must not quarantine retained work");
            }
        }
    }
    server.await.unwrap();
}

#[tokio::test]
async fn publisher_uses_protobuf_without_format_negotiation() {
    crate::core::live_sync::install_default_rustls_provider();
    tokio::time::timeout(Duration::from_secs(10), case(1, &[202], &[202]))
        .await
        .unwrap();
}
#[tokio::test]
async fn binary_rejection_does_not_retry_as_json_or_downgrade_the_next_publication() {
    crate::core::live_sync::install_default_rustls_provider();
    for status in [400, 415] {
        tokio::time::timeout(
            Duration::from_secs(10),
            case(1, &[status, status], &[status, status]),
        )
        .await
        .unwrap();
    }
}
#[tokio::test]
async fn credential_refresh_keeps_the_same_protobuf_format_and_retry_bound() {
    crate::core::live_sync::install_default_rustls_provider();
    tokio::time::timeout(Duration::from_secs(10), async {
        case(1, &[401, 202], &[202]).await;
        case(1, &[401, 401], &[401]).await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn bulk_json_is_a_selected_format_without_protobuf_attempts() {
    crate::core::live_sync::install_default_rustls_provider();
    tokio::time::timeout(Duration::from_secs(10), case(26, &[202], &[202]))
        .await
        .unwrap();
}

#[tokio::test]
async fn upload_deadlines_and_busy_admission_preserve_retryable_exact_publication() {
    crate::core::live_sync::install_default_rustls_provider();
    tokio::time::timeout(Duration::from_secs(10), async {
        for events in [1, 26] {
            case(events, &[408, 429, 202], &[408, 429, 202]).await;
        }
    }).await.unwrap();
}
