use super::*;

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
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        let peer=tokio::spawn(async move {
            let (socket,headers,_)=request(&listener).await;
            assert!(headers.starts_with("post /v1/safety/register "));
            reply(socket,status,serde_json::json!({"error":"owned rejection"})).await;
            assert!(tokio::time::timeout(Duration::from_millis(100),listener.accept()).await.is_err(),
                "safety errors must never invoke ingestion registration");
        });
        let connection=GatewayConnection::new(&url,HttpBatchSinkConfig {
            batch_url:format!("{url}/unused"),actions_url:None,bearer_token:String::new(),
            session_secret:Some("fixture-not-a-real-secret".into()),signin_url:None,
            namespace:"fixture".into(),database:"fixture".into(),access_method:"aqe_live".into(),
            user_id:"users:fixture".into(),strategy_id:"strategy".into(),live_session_id:"session".into(),
            runtime_id:"runtime".into(),run_id:"run".into(),server_node_id:"local".into(),
            request_timeout:Duration::from_secs(1),
        }).unwrap();
        let error=connection.control_token().await.unwrap_err();
        assert!(error.message.contains(&format!("HTTP {status}")));
        assert!(!connection.safety_registered().await);
        peer.await.unwrap();
    }
}
