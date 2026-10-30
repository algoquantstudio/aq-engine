use super::*;

fn credential_fixture_config(url: &str) -> HttpBatchSinkConfig {
    HttpBatchSinkConfig {
        batch_url: format!("{url}/unused"),
        actions_url: None,
        bearer_token: String::new(),
        session_secret: Some("owned-fixture-session-secret".into()),
        signin_url: Some(format!("{url}/signin")),
        namespace: "fixture".into(),
        database: "fixture".into(),
        access_method: "aqe_live".into(),
        user_id: "users:fixture".into(),
        strategy_id: "strategy".into(),
        live_session_id: "session".into(),
        runtime_id: "runtime".into(),
        run_id: "run".into(),
        server_node_id: "local".into(),
        request_timeout: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn credential_posts_cannot_follow_redirects_to_another_origin() {
    use crate::core::live_sync::{HttpBatchSink, RemoteBatchSink};
    use tokio::io::AsyncWriteExt;
    for status in [307, 308] {
        for registration in [true, false] {
            let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let destination = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", origin.local_addr().unwrap());
            let location = format!("http://{}/stolen", destination.local_addr().unwrap());
            let (completed, done) = tokio::sync::oneshot::channel::<()>();
            let peer = tokio::spawn(async move {
                let (mut socket, headers, body) = super::protobuf_tests::request(&origin).await;
                assert!(headers.starts_with(if registration {
                    "post /v1/safety/register "
                } else {
                    "post /signin "
                }));
                let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(
                    body[if registration {
                        "sessionSecret"
                    } else {
                        "secret"
                    }],
                    "owned-fixture-session-secret"
                );
                let response = format!(
                    "HTTP/1.1 {status} Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                drop(socket);
                // Keep the canary origin alive until the credential operation
                // finishes; a short timing window could hide a slow redirect.
                tokio::select! {
                    biased;
                    _ = destination.accept() => panic!("credential-bearing redirect reached a second origin"),
                    _ = done => {},
                }
            });
            let config = credential_fixture_config(&url);
            if registration {
                let gateway = GatewayConnection::new(&url, config).unwrap();
                assert!(
                    gateway
                        .control_token()
                        .await
                        .unwrap_err()
                        .message
                        .contains(&format!("HTTP {status}"))
                );
            } else {
                let sink = HttpBatchSink::new(config, &url).unwrap();
                assert!(
                    sink.refresh_authentication()
                        .await
                        .unwrap_err()
                        .message
                        .contains(&status.to_string())
                );
            }
            let _ = completed.send(());
            peer.await.unwrap();
        }
    }
}

#[tokio::test]
async fn authentication_errors_do_not_expose_response_bodies() {
    use crate::core::live_sync::{HttpBatchSink, RemoteBatchSink};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (socket, _, _) = super::protobuf_tests::request(&listener).await;
        super::protobuf_tests::reply(
            socket,
            401,
            serde_json::json!({
                "secret": "owned-fixture-session-secret", "echo": "private-response-data",
            }),
        )
        .await;
    });
    let sink = HttpBatchSink::new(credential_fixture_config(&url), &url).unwrap();
    let error = sink.refresh_authentication().await.unwrap_err();
    assert_eq!(error.kind, RemoteSyncErrorKind::Authentication);
    assert!(error.message.contains("401"));
    assert!(!error.message.contains("owned-fixture-session-secret"));
    assert!(!error.message.contains("private-response-data"));
    peer.await.unwrap();
}

#[tokio::test]
async fn diagnostic_refresh_does_not_own_the_stop_credential_lock() {
    let connection = GatewayConnection::new(
        "http://127.0.0.1:9080",
        HttpBatchSinkConfig {
            batch_url: "http://127.0.0.1:9081/batch".into(),
            actions_url: None,
            bearer_token: String::new(),
            session_secret: Some("fixture-not-a-real-secret".into()),
            signin_url: None,
            namespace: "test".into(),
            database: "test".into(),
            access_method: "aqe_live".into(),
            user_id: "users:test".into(),
            strategy_id: "strategy".into(),
            live_session_id: "session".into(),
            runtime_id: "runtime".into(),
            run_id: "run".into(),
            server_node_id: "local".into(),
            request_timeout: Duration::from_secs(1),
        },
    )
    .unwrap();
    *connection.control_access.lock().await = Some(Access {
        token: "safety-token".into(),
        expires_at_us: now_epoch_micros() + 60_000_000,
        run_id: "run".into(),
    });
    let _diagnostic_refresh = connection.capture_access.lock().await;
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), connection.control_token())
            .await
            .unwrap()
            .unwrap(),
        "safety-token"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            connection.operational_token(&connection.capture_access)
        )
        .await
        .is_err()
    );
    assert!(connection.safety_registered().await);
}

#[tokio::test]
async fn missing_safety_endpoint_never_requests_an_ingestion_token() {
    use super::protobuf_tests::{reply, request};
    for status in [404, 405, 401, 403] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (socket, headers, _) = request(&listener).await;
            assert!(headers.starts_with("post /v1/safety/register "));
            reply(
                socket,
                status,
                serde_json::json!({"error":"owned rejection"}),
            )
            .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err(),
                "safety errors must never invoke ingestion registration"
            );
        });
        let connection = GatewayConnection::new(
            &url,
            HttpBatchSinkConfig {
                batch_url: format!("{url}/unused"),
                actions_url: None,
                bearer_token: String::new(),
                session_secret: Some("fixture-not-a-real-secret".into()),
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
                request_timeout: Duration::from_secs(1),
            },
        )
        .unwrap();
        let error = connection.control_token().await.unwrap_err();
        assert!(error.message.contains(&format!("HTTP {status}")));
        assert!(!connection.safety_registered().await);
        peer.await.unwrap();
    }
}
