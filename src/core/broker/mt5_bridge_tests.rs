use super::*;
use crate::core::broker::types::{
    OrderClass, OrderLeg, OrderLegs, OrderSide, OrderType, TimeInForce,
};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

fn test_bridge(token: &str) -> Mt5Bridge {
    Mt5Bridge::new(Mt5BridgeConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        token: token.to_string(),
        request_timeout: Duration::from_millis(5_000),
        connect_timeout: Duration::from_millis(5_000),
        poll_interval: Duration::from_millis(250),
        symbol_map: HashMap::new(),
    })
}

#[actix_web::test]
async fn unauthorized_http_json_is_rejected_without_polling_the_payload() {
    use actix_web::FromRequest;
    use futures::stream::LocalBoxStream;
    for token in [None, Some("wrong-token")] {
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let stream: LocalBoxStream<'static, Result<web::Bytes, actix_web::error::PayloadError>> =
            Box::pin(futures::stream::poll_fn(move |_| {
                observed.fetch_add(1, AtomicOrdering::SeqCst);
                std::task::Poll::Pending
            }));
        let mut payload = actix_web::dev::Payload::from(stream);
        let data = web::Data::new(test_bridge("private-http-token"));
        let mut request = actix_web::test::TestRequest::post().uri("/v1/snapshot")
            .app_data(data).insert_header(("content-type", "application/json"));
        if let Some(token) = token {
            request = request.insert_header(("x-aqe-mt5-token", token));
        }
        let request = request.to_http_request();
        let result = tokio::time::timeout(Duration::from_millis(100),
            AuthenticatedJson::<Value>::from_request(&request, &mut payload)).await.unwrap();
        assert_eq!(result.err().unwrap().as_response_error().status_code(), StatusCode::UNAUTHORIZED);
        assert_eq!(polls.load(AtomicOrdering::SeqCst), 0);
    }
}

#[actix_web::test]
async fn all_http_producer_routes_authorize_before_json_parsing() {
    let data = web::Data::new(test_bridge("private-http-token"));
    let app = actix_web::test::init_service(App::new().app_data(data.clone())
        .route("/v1/heartbeat", web::post().to(heartbeat))
        .route("/v1/snapshot", web::post().to(snapshot))
        .route("/v1/market-data", web::post().to(market_data))
        .route("/v1/trade-event", web::post().to(trade_event))
        .route("/v1/trade-events", web::post().to(trade_events))
        .route("/v1/execution-fill-revisions", web::post().to(fill_revisions))
        .route("/v1/rpc/poll", web::post().to(poll_rpc))
        .route("/v1/rpc/response", web::post().to(rpc_response))).await;
    for path in ["/v1/heartbeat", "/v1/snapshot", "/v1/market-data", "/v1/trade-event",
        "/v1/trade-events", "/v1/execution-fill-revisions", "/v1/rpc/poll", "/v1/rpc/response"] {
        for token in [None, Some("wrong-token")] {
            let mut request = actix_web::test::TestRequest::post().uri(path)
                .insert_header(("content-type", "application/json")).set_payload("not JSON");
            if let Some(token) = token {
                request = request.insert_header(("x-aqe-mt5-token", token));
            }
            let response = actix_web::test::call_service(&app, request.to_request()).await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
            let body: Mt5BridgeResponse<Value> = actix_web::test::read_body_json(response).await;
            assert!(!body.ok);
        }
    }
    assert!(data.diagnostics().last_poll_attempt_at.is_some());
    assert!(data.diagnostics().last_poll_auth_error.is_some());
    // Authorized EAs still reach the existing JSON decoder and its error policy.
    let request = actix_web::test::TestRequest::post().uri("/v1/heartbeat")
        .insert_header(("x-aqe-mt5-token", "private-http-token"))
        .insert_header(("content-type", "application/json")).set_payload("not JSON").to_request();
    assert_eq!(actix_web::test::call_service(&app, request).await.status(), StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn unauthorized_http_headers_are_rejected_before_a_stalled_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let data = web::Data::new(test_bridge("private-http-token"));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = HttpServer::new(move || App::new().app_data(data.clone())
        .app_data(web::JsonConfig::default().limit(16 * 1024 * 1024))
        .route("/v1/snapshot", web::post().to(snapshot)))
        .workers(1).listen(listener).unwrap().run();
    let handle = server.handle();
    actix_web::rt::spawn(server);
    let mut outcomes = Vec::new();
    for token_header in ["", "x-aqe-mt5-token: wrong-token\r\n"] {
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        let headers = format!("POST /v1/snapshot HTTP/1.1\r\nHost: {address}\r\n{token_header}Content-Type: application/json\r\nContent-Length: 16777216\r\nConnection: close\r\n\r\n");
        socket.write_all(headers.as_bytes()).await.unwrap();
        // Deliberately send no body. Admission cannot wait for any of the
        // advertised 16 MiB, even when the producer stalls after its headers.
        let mut response = [0; 1024];
        outcomes.push(tokio::time::timeout(Duration::from_secs(3), socket.read(&mut response)).await
            .map(|read| String::from_utf8_lossy(&response[..read.unwrap()]).into_owned()));
    }
    handle.stop(false).await;
    for response in outcomes {
        assert!(response.expect("authorization waited for the HTTP body").starts_with("HTTP/1.1 401"));
    }
}

#[actix_web::test]
async fn admitted_http_json_retains_body_limits_and_decodes_values() {
    use actix_web::FromRequest;
    let data = web::Data::new(test_bridge("private-http-token"));
    let (request, mut payload) = actix_web::test::TestRequest::post()
        .app_data(data.clone()).app_data(web::JsonConfig::default().limit(32))
        .insert_header(("x-aqe-mt5-token", "private-http-token"))
        .insert_header(("content-type", "application/json"))
        .set_payload("{\"ok\":true}").to_http_parts();
    let value = AuthenticatedJson::<Value>::from_request(&request, &mut payload).await.unwrap().into_inner();
    assert_eq!(value, serde_json::json!({"ok": true}));
    let (request, mut payload) = actix_web::test::TestRequest::post()
        .app_data(data).app_data(web::JsonConfig::default().limit(4))
        .insert_header(("x-aqe-mt5-token", "private-http-token"))
        .insert_header(("content-type", "application/json"))
        .set_payload("{\"ok\":true}").to_http_parts();
    let error = AuthenticatedJson::<Value>::from_request(&request, &mut payload).await.err().unwrap();
    assert_eq!(error.as_response_error().status_code(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[actix_web::test]
async fn unauthenticated_v2_connections_cannot_fence_the_active_ea() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let bridge = Arc::new(test_bridge("transport-secret"));
    let data: web::Data<Mt5Bridge> = web::Data::from(bridge.clone());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = HttpServer::new(move || App::new().app_data(data.clone())
        .route("/v2/bridge", web::get().to(bridge_v2)))
        .workers(1).listen(listener).unwrap().run();
    let handle = server.handle();
    actix_web::rt::spawn(server);
    let url = format!("ws://{address}/v2/bridge");
    let (mut ea, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let hello: Value = serde_json::from_str(ea.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(bridge.active_transport_generation.load(Ordering::SeqCst), 0);
    ea.send(Message::Text(serde_json::json!({"type":"Authenticate", "nonce":hello["nonce"],
        "token":"transport-secret", "sessionId":"authenticated-ea"}).to_string().into())).await.unwrap();
    let authenticated: Value = serde_json::from_str(ea.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    let generation = authenticated["transportGeneration"].as_u64().unwrap();
    assert_eq!(generation, 1);

    let (mut unauthenticated, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let hello: Value = serde_json::from_str(unauthenticated.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(bridge.active_transport_generation.load(Ordering::SeqCst), generation);
    unauthenticated.send(Message::Text(serde_json::json!({"type":"Authenticate", "nonce":hello["nonce"],
        "token":"wrong-secret", "sessionId":"attacker"}).to_string().into())).await.unwrap();
    let rejected: Value = serde_json::from_str(unauthenticated.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(rejected["type"], "Rejected");
    assert_eq!(bridge.active_transport_generation.load(Ordering::SeqCst), generation);
    assert!(!bridge.state.read().active_sessions.contains_key("attacker"));

    // Exercise actual command delivery, not just the generation counter.
    let requester = bridge.clone();
    let request = tokio::spawn(async move { requester.request_rpc_with_timeout(
        Mt5RpcAction::GetHistory, serde_json::json!({}), Duration::from_secs(2)).await });
    let command = tokio::time::timeout(Duration::from_secs(2), ea.next()).await.unwrap().unwrap().unwrap();
    let command: Value = serde_json::from_str(command.to_text().unwrap()).unwrap();
    assert_eq!(command["type"], "BrokerCommand");
    ea.send(Message::Text(serde_json::json!({"type":"BrokerCommandResult", "payload":{
        "requestId":command["request"]["requestId"], "ok":true, "payload":[]}}).to_string().into())).await.unwrap();
    assert_eq!(request.await.unwrap().unwrap(), serde_json::json!([]));

    // A genuine authenticated reconnect still claims a new generation.
    let (mut replacement, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let hello: Value = serde_json::from_str(replacement.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    replacement.send(Message::Text(serde_json::json!({"type":"Authenticate", "nonce":hello["nonce"],
        "token":"transport-secret", "sessionId":"authenticated-replacement"}).to_string().into())).await.unwrap();
    let response: Value = serde_json::from_str(replacement.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(response["transportGeneration"], generation + 1);
    let _ = ea.close(None).await;
    let _ = replacement.close(None).await;
    handle.stop(false).await;
    bridge.shutdown().await;
}

#[actix_web::test]
async fn unauthenticated_v2_sockets_are_bounded_and_expire() {
    use futures::StreamExt;
    let bridge = Arc::new(test_bridge("handshake-secret"));
    let data: web::Data<Mt5Bridge> = web::Data::from(bridge.clone());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = HttpServer::new(move || App::new().app_data(data.clone())
        .route("/v2/bridge", web::get().to(bridge_v2)))
        .workers(1).listen(listener).unwrap().run();
    let handle = server.handle();
    actix_web::rt::spawn(server);
    let url = format!("ws://{address}/v2/bridge");
    let mut sockets = Vec::new();
    for _ in 0..MT5_V2_MAX_AUTH_CONNECTIONS {
        let (mut socket, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        socket.next().await.unwrap().unwrap();
        sockets.push(socket);
    }
    let rejected = tokio_tungstenite::connect_async(&url).await.unwrap_err();
    match rejected {
        tokio_tungstenite::tungstenite::Error::Http(response) => assert_eq!(response.status(), 429),
        other => panic!("Expected bounded handshake rejection: {other}"),
    }
    tokio::time::timeout(MT5_V2_AUTH_TIMEOUT + Duration::from_secs(2), async {
        while bridge.v2_auth_slots.available_permits() != MT5_V2_MAX_AUTH_CONNECTIONS {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.unwrap();
    assert_eq!(bridge.active_transport_generation.load(Ordering::SeqCst), 0);
    drop(sockets);
    handle.stop(false).await;
    bridge.shutdown().await;
}

#[test]
fn position_payload_keeps_order_ids_and_all_same_symbol_tickets() {
    let bridge=test_bridge("position-identities");
    let position=|id:&str,pnl| Position {
        account_id:"account".into(),order_id:Some(id.into()),asset:default_mt5_asset("BTCUSD"),
        avg_entry_price:100.,qty:1.,side:OrderSide::Sell,market_value:99.,cost_basis:100.,current_price:99.,
        unrealized_pnl:pnl,realized_pnl:0.,entry_commission:0.,margin_required:None,
    };
    let payload=|positions| Mt5SnapshotPayload {account:None,assets:vec![],orders:vec![],positions};
    bridge.apply_snapshot("session",payload(vec![position("a",5.),position("b",12.)]));
    assert_eq!(bridge.positions().len(),2);
    assert_eq!(bridge.positions().iter().map(|p|p.unrealized_pnl).sum::<f64>(),17.);
    assert!(bridge.position("BTCUSD").is_err()); // Ambiguous symbol, never choose an arbitrary split.
    bridge.apply_snapshot("session",payload(vec![position("b",14.)]));
    assert_eq!(bridge.positions().len(),1);
    assert_eq!(bridge.position("BTCUSD").unwrap().order_id.as_deref(),Some("b"));
    let mut legacy=serde_json::to_value(position("b",14.)).unwrap();
    assert!(legacy.get("symbol").is_none());
    assert_eq!(legacy["asset"]["symbol"],"BTCUSD");
    // Old EAs may still send the redundant field. The asset is authoritative.
    legacy["symbol"]=serde_json::json!("obsolete-duplicate");
    let decoded:Position=serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(decoded.asset.symbol,"BTCUSD");
    assert_eq!(decoded.order_id.as_deref(),Some("b"));
    legacy.as_object_mut().unwrap().remove("order_id");
    assert!(serde_json::from_value::<Position>(legacy).unwrap().order_id.is_none());
}

#[tokio::test]
async fn automatic_recovery_shutdown_cancels_pending_reads_and_fences_rescheduling() {
    let bridge = Arc::new(test_bridge("owned-recovery-test"));
    // Exercise real recovery/RPC ownership without opening a server or using MT5.
    assert!(bridge.recovery_task.enable());
    bridge.state.write().last_rpc_poll = Some(Utc::now());
    bridge.schedule_runtime_recovery("test transport");
    bridge.schedule_runtime_recovery("duplicate test transport");
    tokio::time::timeout(Duration::from_secs(1), async {
        while bridge.pending_rpc.lock().len() != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(bridge.rpc_queue.lock().len(), 3);
    bridge.shutdown().await;
    assert!(bridge.pending_rpc.lock().is_empty());
    assert!(bridge.rpc_queue.lock().is_empty());
    assert_eq!(Arc::strong_count(&bridge), 1);
    bridge.schedule_runtime_recovery("late transport callback");
    tokio::task::yield_now().await;
    assert_eq!(Arc::strong_count(&bridge), 1);
    assert!(bridge.pending_rpc.lock().is_empty());
}

#[test]
fn broker_capacity_limits_round_trip_and_legacy_account_falls_back() {
    use crate::core::broker::{BrokerFeature, mt5_broker::Mt5Broker, traits::Broker};
    let bridge = Arc::new(test_bridge("capacity-test"));
    let broker = Mt5Broker::new(bridge.clone());
    assert!(
        broker
            .get_features()
            .contains(&BrokerFeature::MaxTotalOrders)
    );
    assert!(broker.get_features().contains(&BrokerFeature::MaxTotalSize));
    let mut account: Account = serde_json::from_value(serde_json::json!({
        "account_id": "123", "account_type": "Live", "equity": 100000.0,
        "cash": 100000.0, "currency": "USD", "buying_power": 90000.0,
        "shorting_enabled": true, "leverage": 100
    }))
    .unwrap();
    assert_eq!(account.max_total_orders, None);
    bridge.apply_snapshot(
        "capacity-test",
        Mt5SnapshotPayload {
            account: Some(account.clone()),
            ..Default::default()
        },
    );
    assert_eq!(bridge.account().unwrap().max_total_orders, Some(4096));
    account.max_total_orders = Some(123);
    let mut asset = crate::core::utils::quantity_sizing::tests::sample_asset();
    asset.symbol = "SOLUSD".into();
    asset.max_order_size = Some(100.0);
    asset.max_total_size = Some(1000.0);
    let asset: Asset = serde_json::from_value(serde_json::to_value(asset).unwrap()).unwrap();
    bridge.apply_snapshot(
        "capacity-test",
        Mt5SnapshotPayload {
            account: Some(account),
            assets: vec![asset],
            ..Default::default()
        },
    );
    assert_eq!(bridge.account().unwrap().max_total_orders, Some(123));
    let state = bridge.state.read();
    assert_eq!(state.assets["SOLUSD"].max_total_size, Some(1000.0));
    assert_eq!(state.assets["SOLUSD"].max_order_size, Some(100.0));
}

#[actix_web::test]
async fn quote_bridge_reassembles_large_history_without_disconnecting() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::{
        Message,
        protocol::frame::{
            Frame,
            coding::{Data, OpCode},
        },
    };
    let bridge = Arc::new(test_bridge("fragment-test"));
    bridge.start_ingress_workers();
    let evidence = Arc::new(Mutex::new(Vec::new()));
    let capture = evidence.clone();
    bridge.subscribe_broker_event_stream(Arc::new(move |event| capture.lock().push(event)));
    let data: web::Data<Mt5Bridge> = web::Data::from(bridge.clone());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .route("/v2/bridge", web::get().to(bridge_v2))
    })
    .workers(1)
    .listen(listener)
    .unwrap()
    .run();
    let handle = server.handle();
    actix_web::rt::spawn(server);
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/v2/bridge"))
        .await
        .unwrap();
    let hello: Value =
        serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(hello["executionFillRevisionVersion"], 1);
    socket
        .send(Message::Text(
            serde_json::json!({"type":"Authenticate", "nonce":hello["nonce"],
        "token":"fragment-test", "sessionId":hello["bridgeSessionId"]})
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let authenticated: Value =
        serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(authenticated["type"], "Authenticated");
    for count in [1000usize, 1] {
        let requester = bridge.clone();
        let task = tokio::spawn(async move {
            requester
                .request_rpc_with_timeout(
                    Mt5RpcAction::GetHistory,
                    serde_json::json!({}),
                    Duration::from_secs(5),
                )
                .await
        });
        let command = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let command: Value = serde_json::from_str(command.to_text().unwrap()).unwrap();
        let bars = vec![
            serde_json::json!({"symbol":"EURUSD", "open":1.1, "high":1.2,
            "low":1.0,"close":1.15,"volume":1200,"timestamp":"2026-01-01T00:00:00Z"});
            count
        ];
        let response = serde_json::json!({"type":"BrokerCommandResult", "payload":{
            "requestId":command["request"]["requestId"], "ok":true, "payload":bars}})
        .to_string();
        if count == 1000 {
            assert!(response.len() > 65536);
        }
        let bytes = response.as_bytes();
        let chunks = bytes.chunks(32768).collect::<Vec<_>>();
        for (index, chunk) in chunks.iter().enumerate() {
            let opcode = if index == 0 {
                Data::Text
            } else {
                Data::Continue
            };
            socket
                .send(Message::Frame(Frame::message(
                    chunk.to_vec(),
                    OpCode::Data(opcode),
                    index + 1 == chunks.len(),
                )))
                .await
                .unwrap();
        }
        let result = task.await.unwrap().unwrap();
        assert_eq!(result.as_array().unwrap().len(), count);
    }
    // The distinct frame kind forces isolation even when the payload carries
    // the legacy default flag. An older receiver rejects this unknown kind.
    socket.send(Message::Text(serde_json::json!({
        "type":"ExecutionFillRevisionBatch","sequence":1,"events":[{
            "analyticsOnly":false,"event":"Closed","eventSeq":1,"nativeEventId":"revision-1",
            "order":trade_event_order("position", "owner", TradeUpdateEvent::Closed),
            "executionFill":{"version":1,"provider":"mt5-deal-v1","fill_id":"deal",
                "order_id":"order","position_id":"position","symbol":"AAPL","side":"buy",
                "effect":"exit","quantity":2.,"price":100.,"occurred_at_us":100,"as_of_us":200,
                "cash":{"currency":"USD","gross_pnl":10.,"commission":1.,"carry":0.},
                "account_notional":null}
        }]
    }).to_string().into())).await.unwrap();
    let ack = tokio::time::timeout(Duration::from_secs(1), socket.next()).await.unwrap().unwrap().unwrap();
    let ack: Value = serde_json::from_str(ack.to_text().unwrap()).unwrap();
    assert_eq!(ack["type"], "BatchAcknowledgement");
    tokio::time::timeout(Duration::from_secs(1), async {
        while evidence.lock().is_empty() { tokio::task::yield_now().await; }
    }).await.unwrap();
    assert!(evidence.lock()[0].metadata.analytics_only);
    assert!(bridge.order("position").is_err());
    assert!(bridge.drain_trade_events().is_empty());
    socket.close(None).await.unwrap();
    handle.stop(false).await;
    bridge.stop_ingress_workers();
}

fn headers(session_id: &str, token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-aqe-mt5-session".parse().unwrap(),
        session_id.parse().unwrap(),
    );
    headers.insert("x-aqe-mt5-token".parse().unwrap(), token.parse().unwrap());
    headers
}

#[test]
fn quote_leases_survive_bar_unsubscribe_and_deliver_only_matching_fresh_quotes() {
    let bridge = test_bridge("test-token");
    let session = bridge.session_id();
    let count = Arc::new(AtomicUsize::new(0));
    for (id, symbol, owner_session) in [
        ("selected", "AAPL", session.as_str()),
        ("different-symbol", "MSFT", session.as_str()),
        ("old-session", "AAPL", "old"),
    ] {
        let count = count.clone();
        bridge.quote_subscribers.lock().insert(
            id.into(),
            Mt5QuoteSubscription {
                session_id: owner_session.into(),
                symbol: symbol.into(),
                callback: Arc::new(move |_| {
                    count.fetch_add(1, AtomicOrdering::Relaxed);
                }),
            },
        );
    }
    let tf = TimeFrame::new(1, TimeFrameUnit::Minute);
    bridge
        .subscribe_bars(
            vec!["AAPL".into()],
            tf,
            DataStreamMode::Intrabar,
            Arc::new(|_| {}),
        )
        .unwrap();
    assert_eq!(bridge.current_subscription_specs().len(), 2);
    bridge
        .unsubscribe_bars(vec!["AAPL".into()], tf, DataStreamMode::Intrabar)
        .unwrap();
    assert_eq!(bridge.current_subscription_specs().len(), 2);
    let quote = Quote {
        profit_model: None,
        symbol: "AAPL".into(),
        bid: 100.0,
        ask: 100.2,
        bid_size: 0.0,
        ask_size: 0.0,
        last: None,
        last_size: None,
        timestamp: Utc::now(),
    };
    for _ in 0..2 {
        bridge.apply_market_data(
            &session,
            Mt5MarketDataPayload {
                quotes: vec![quote.clone()],
                ..Default::default()
            },
        );
    }
    assert_eq!(count.load(AtomicOrdering::Relaxed), 1);
    let mut fx_quote=quote.clone();
    fx_quote.profit_model=Some(aqe_live_protocol::BrokerQuoteProfit {
        version:1,account_id:"account".into(),currency:"GBP".into(),symbol:"AAPL".into(),as_of_us:1,
        buy_gain:0.75,buy_loss:0.76,sell_gain:0.75,sell_loss:0.76,
    });
    bridge.apply_market_data(&session,Mt5MarketDataPayload{quotes:vec![fx_quote.clone()],..Default::default()});
    assert_eq!(bridge.latest_quote("AAPL").unwrap().profit_model,fx_quote.profit_model);
    assert_eq!(count.load(AtomicOrdering::Relaxed),1,"FX metadata alone must not replay execution quotes");
    fx_quote.set_mapped_symbol("Alias".into());
    assert_eq!(fx_quote.profit_model.as_ref().unwrap().symbol,"Alias");
    fx_quote.profit_model.as_mut().unwrap().symbol="wrong-instrument".into();
    fx_quote.set_mapped_symbol("AnotherAlias".into());
    assert!(fx_quote.profit_model.is_none());
    bridge.quote_subscribers.lock().remove("selected");
    let specs = bridge.current_subscription_specs();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].symbol, "MSFT");
    bridge.clear_session_runtime_state();
    assert!(bridge.quote_subscribers.lock().is_empty());
}

fn trade_event_order(order_id: &str, insight_id: &str, status: TradeUpdateEvent) -> Order {
    Order {
        order_id: order_id.to_string(),
        insight_id: Some(insight_id.to_string()),
        strategy_type: Some("Testing".to_string()),
        asset: default_mt5_asset("AAPL"),
        qty: 1.0,
        filled_qty: 1.0,
        limit_price: None,
        filled_price: Some(100.0),
        stop_price: None,
        side: OrderSide::Buy,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::GTC,
        status,
        order_class: OrderClass::Simple,
        created_at: 0,
        updated_at: 0,
        submitted_at: 0,
        filled_at: Some(0),
        realized_pnl: None,
        position_accounting: None,
        commission: None,
        swap: None,
        rejection_reason: None,
        legs: None,
    }
}

async fn next_test_rpc(bridge: &Mt5Bridge) -> Mt5RpcRequest {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(request) = bridge.drain_rpc_requests(1).0.pop() {
                return request;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("broker should issue the expected RPC")
}

#[tokio::test]
async fn owned_fill_history_rpc_validates_scope_maps_symbols_and_never_emits_orders() {
    use crate::core::broker::{fill_history::ExecutionFillHistoryReader, mt5_broker::Mt5Broker};
    let bridge = Arc::new(Mt5Bridge::new(Mt5BridgeConfig {
        symbol_map: HashMap::from([("GENERIC".into(), "GENERIC.native".into())]),
        ..test_bridge("history-test").config().clone()
    }));
    let broker = Mt5Broker::new(bridge.clone());
    for (account, position_snapshot) in [("account", true), ("account", false), ("foreign", true)] {
        let broker = broker.clone();
        let read = tokio::spawn(async move { broker.read("account","entry-order","GENERIC").await });
        let request = next_test_rpc(&bridge).await;
        assert!(matches!(request.action,Mt5RpcAction::GetExecutionFillHistory));
        assert_eq!(request.payload,serde_json::json!({"accountId":"account","entryOrderId":"entry-order","symbol":"GENERIC.native","invalidationVersion":1,"positionSnapshotVersion":1}));
        bridge.apply_rpc_response(Mt5RpcResponsePayload { request_id:request.request_id, ok:true, message:None,
            payload:Some(serde_json::json!({"account_id":account,"entry_order_id":"entry-order","position_id":"position",
                "symbol":"GENERIC.native","as_of_us":200,"clock":{"basis":"utc"},
                "position_snapshot":if position_snapshot {serde_json::json!({"position_id":"position","symbol":"GENERIC.native","quantity":2.,"side":"buy","as_of_us":200})} else {Value::Null},"fills":[{
                "version":1,"provider":"mt5-deal-v1","fill_id":"deal","order_id":"entry-order","position_id":"position",
                "symbol":"GENERIC.native","side":"buy","effect":"entry","quantity":2.,"price":100.,
                "occurred_at_us":100,"as_of_us":200,"cash":{"currency":"GBP","gross_pnl":0.,"commission":1.,"carry":0.},"account_notional":null}],
                "invalidations":[{"provider":"mt5-deal-v1","fill_id":"cancelled-exit","order_id":"exit-order","position_id":"position",
                "symbol":"GENERIC.native","side":"sell","effect":"exit","occurred_at_us":100,"as_of_us":200,"reason":"broker_cancelled"}]})) });
        let result = read.await.unwrap();
        if account == "account" {
            let history = result.unwrap().unwrap();
            assert_eq!(history.symbol,"GENERIC"); assert_eq!(history.fills[0].symbol,"GENERIC");
            assert_eq!(history.invalidations[0].symbol,"GENERIC");
            if position_snapshot {
                assert_eq!(history.position_snapshot.as_ref().unwrap().symbol,"GENERIC");
                assert!(history.position_check().unwrap().matched);
            } else { assert!(history.position_check().is_none()); }
        } else { assert!(result.is_err()); }
        assert!(bridge.state.read().orders.is_empty());
        assert!(bridge.state.read().positions.is_empty());
        assert!(bridge.event_queue.lock().is_empty());
        assert!(bridge.pending_rpc.lock().is_empty());
    }
}

#[tokio::test]
async fn position_valuation_rpc_is_exact_bounded_read_only_and_symbol_mapped() {
    use crate::core::broker::{position_valuation::PositionValuationReader, mt5_broker::Mt5Broker};
    let bridge = Arc::new(Mt5Bridge::new(Mt5BridgeConfig {
        symbol_map: HashMap::from([("GENERIC".into(), "GENERIC.native".into())]),
        ..test_bridge("valuation-test").config().clone()
    }));
    let broker = Mt5Broker::new(bridge.clone());
    for scenario in ["valid", "null", "foreign_account", "foreign_symbol", "foreign_position", "missing", "duplicate", "old_ea", "unknown_provider"] {
        let reader = broker.clone();
        let read = tokio::spawn(async move { reader.read_positions("account","GENERIC",&["101".into(),"102".into()]).await });
        let request = next_test_rpc(&bridge).await;
        assert!(matches!(request.action,Mt5RpcAction::GetPositionValuations));
        assert_eq!(request.payload,serde_json::json!({"accountId":"account","symbol":"GENERIC.native","positionIds":["101","102"],"valuationVersion":1}));
        let mut payload = serde_json::json!({"version":1,"provider":"mt5-position-v1","account_id":"account","account_currency":"GBP",
            "symbol":"GENERIC.native","as_of_us":2000000,"positions":[
                {"position_id":"101","status":"open","quantity":2.,"side":"sell","average_entry_price":100.,"current_price":99.,
                 "gross_unrealized_pnl":2.,"carry":-0.2,"quote_time":null,"notional":{"basis":"unavailable","reason":"currency_conversion_unavailable"}},
                {"position_id":"102","status":"not_open"}]});
        match scenario {
            "null"=>payload=Value::Null,
            "foreign_account"=>payload["account_id"]="foreign".into(),
            "foreign_symbol"=>payload["symbol"]="foreign".into(),
            "foreign_position"=>payload["positions"][0]["position_id"]="999".into(),
            "missing"=>{payload["positions"].as_array_mut().unwrap().pop();},
            "duplicate"=>payload["positions"][1]=payload["positions"][0].clone(),
            "unknown_provider"=>payload["provider"]="foreign".into(),
            _=>{}
        }
        bridge.apply_rpc_response(Mt5RpcResponsePayload{request_id:request.request_id,ok:scenario!="old_ea",
            message:(scenario=="old_ea").then(||"unsupported RPC".into()),payload:Some(payload)});
        let result=read.await.unwrap();
        match scenario {
            "valid"=>{let p=result.unwrap().unwrap();assert_eq!(p.symbol,"GENERIC");assert_eq!(p.positions.len(),2);},
            "null"=>assert!(result.unwrap().is_none()),
            _=>assert!(result.is_err(),"{scenario}"),
        }
        assert!(bridge.state.read().orders.is_empty());
        assert!(bridge.state.read().positions.is_empty());
        assert!(bridge.event_queue.lock().is_empty());
        assert!(bridge.pending_rpc.lock().is_empty());
        assert!(bridge.drain_rpc_requests(1).0.is_empty());
    }
    for ids in [vec![],vec!["101".into(),"101".into()],vec!["001".into()],vec!["-1".into()],
        vec!["1,2".into()],(1..34).map(|x|x.to_string()).collect()] {
        assert!(broker.read_positions("account","GENERIC",&ids).await.is_err());
        assert!(bridge.drain_rpc_requests(1).0.is_empty(),"invalid input must not reach MT5");
    }
}

#[tokio::test]
async fn already_closed_close_rpc_reconciles_without_repeating_close() {
    use crate::core::broker::{mt5_broker::Mt5Broker, traits::OrderManagementProvider};
    for code in ["10029", "10036"] {
        let bridge = Arc::new(test_bridge("closed-race"));
    bridge.upsert_order(trade_event_order("closed-ticket", "insight-closed", TradeUpdateEvent::Filled));
    let broker = Mt5Broker::new(bridge.clone());
    let task = tokio::spawn(async move { broker.close_position("closed-ticket", 1.0, None).await });
    let close = next_test_rpc(&bridge).await;
    assert!(matches!(close.action, Mt5RpcAction::ClosePosition));
    bridge.apply_rpc_response(Mt5RpcResponsePayload {
        request_id: close.request_id, ok: false, message: Some(code.into()), payload: None,
    });
    // History can lag the rejection. The first read must not fabricate a close.
    let first = next_test_rpc(&bridge).await;
    assert!(matches!(first.action, Mt5RpcAction::ReconcileOrders));
    assert_eq!(first.payload["orderIds"], serde_json::json!(["closed-ticket"]));
    bridge.apply_rpc_response(Mt5RpcResponsePayload {
        request_id: first.request_id, ok: true, message: None,
        payload: Some(serde_json::json!([{"orderId": "closed-ticket", "status": "Unknown"}])),
    });
    assert!(bridge.drain_trade_events().is_empty());
    let second = next_test_rpc(&bridge).await;
    assert!(matches!(second.action, Mt5RpcAction::ReconcileOrders));
    bridge.apply_rpc_response(Mt5RpcResponsePayload {
        request_id: second.request_id, ok: true, message: None,
        payload: Some(serde_json::json!([{"orderId": "closed-ticket", "status": "Closed", "symbol": "AAPL",
            "closeDetails": {"closePrice": 98.5, "closedAt": "2026-10-01T09:29:55Z",
                "realizedPnl": -1.5, "commission": 0.2, "swap": -0.1}}])),
    });
    assert!(task.await.unwrap().unwrap());
    let events = bridge.drain_trade_events();
    assert_eq!(events.len(), 1);
    let (order, event) = &events[0];
    assert_eq!(*event, TradeUpdateEvent::Closed);
    assert_eq!(order.order_id, "closed-ticket");
    assert_eq!(order.insight_id.as_deref(), Some("insight-closed"));
    assert_eq!(order.filled_price, Some(98.5));
    assert_eq!(order.realized_pnl, Some(-1.5));
    assert_eq!(order.commission, Some(0.2));
    assert_eq!(order.swap, Some(-0.1));
    assert!(bridge.drain_rpc_requests(1).0.is_empty());
    }
}

fn cancellation_recovery_bridge() -> Arc<Mt5Bridge> {
    let bridge=Arc::new(test_bridge("cancel-recovery"));
    let account:Account=serde_json::from_value(serde_json::json!({
        "account_id":"account","account_type":"Live","equity":100000.,
        "cash":100000.,"currency":"USD","buying_power":90000.,
        "shorting_enabled":true,"leverage":100
    })).unwrap();
    bridge.apply_snapshot("cancel-recovery",Mt5SnapshotPayload { account:Some(account),..Default::default() });
    bridge.upsert_order(trade_event_order("2025202590","saved-owner",TradeUpdateEvent::New));
    bridge
}

fn reply_cancel_history(bridge:&Mt5Bridge,request:Mt5RpcRequest,status:&str) {
    assert!(matches!(request.action,Mt5RpcAction::ReconcileOrders));
    assert_eq!(request.payload["orderIds"],serde_json::json!(["2025202590"]));
    bridge.apply_rpc_response(Mt5RpcResponsePayload { request_id:request.request_id,ok:true,message:None,
        payload:Some(serde_json::json!([{"orderId":"2025202590","status":status,"symbol":"AAPL"}])) });
}

#[tokio::test]
async fn cancellation_recovery_requires_pending_proof_then_terminal_confirmation_not_ack() {
    use crate::core::broker::{mt5_broker::Mt5Broker,traits::OrderManagementProvider};
    for (rpc_ok,after,confirmed) in [(true,"Cancelled",true),(false,"Cancelled",true),
        (true,"Pending",false),(true,"Unknown",false),(true,"PositionOpen",false)] {
        let bridge=cancellation_recovery_bridge();
        let broker=Mt5Broker::new(bridge.clone());
        let task=tokio::spawn(async move {broker.retry_cancellation("account","2025202590").await});
        reply_cancel_history(&bridge,next_test_rpc(&bridge).await,"Pending");
        let cancel=next_test_rpc(&bridge).await;
        assert!(matches!(cancel.action,Mt5RpcAction::CancelOrder));
        assert_eq!(cancel.payload["orderId"],"2025202590");
        bridge.apply_rpc_response(Mt5RpcResponsePayload {request_id:cancel.request_id,ok:rpc_ok,
            message:(!rpc_ok).then(||"old request may already have cancelled".into()),payload:Some(serde_json::json!({"cancelled":true}))});
        reply_cancel_history(&bridge,next_test_rpc(&bridge).await,after);
        assert_eq!(task.await.unwrap().unwrap(),confirmed,"{rpc_ok}/{after}");
        let events=bridge.drain_trade_events();
        assert_eq!(events.len(),usize::from(confirmed),"no Cancelled event on an ACK alone");
        if confirmed {assert_eq!(events[0].1,TradeUpdateEvent::Cancelled);assert_eq!(events[0].0.order_id,"2025202590");}
        assert!(bridge.drain_rpc_requests(10).0.is_empty(),"never submit, close, or retry blindly");
    }
}

#[tokio::test]
async fn cancellation_recovery_never_mutates_unknown_filled_or_foreign_tickets() {
    use crate::core::broker::{mt5_broker::Mt5Broker,traits::OrderManagementProvider};
    for status in ["Unknown","Filled","PositionOpen","Closed","Cancelled"] {
        let bridge=cancellation_recovery_bridge();
        let broker=Mt5Broker::new(bridge.clone());
        assert!(broker.pending_cancellation_recovery("foreign-account","2025202590").await.is_err());
        assert!(bridge.drain_rpc_requests(1).0.is_empty());
        let task=tokio::spawn(async move {broker.retry_cancellation("account","2025202590").await});
        reply_cancel_history(&bridge,next_test_rpc(&bridge).await,status);
        assert!(!task.await.unwrap().unwrap());
        assert!(bridge.drain_rpc_requests(1).0.is_empty(),"{status}: must not cancel/close/submit");
    }
    for payload in [serde_json::json!([]),serde_json::json!([{"orderId":"other","status":"Pending","symbol":"AAPL"}]),
        serde_json::json!([{"orderId":"2025202590","status":"Pending","symbol":"WRONG"}]),
        serde_json::json!([{"orderId":"2025202590","status":"Pending","symbol":"AAPL","positionId":"partially-filled"}])] {
        let bridge=cancellation_recovery_bridge();
        let broker=Mt5Broker::new(bridge.clone());
        let task=tokio::spawn(async move {broker.pending_cancellation_recovery("account","2025202590").await});
        let request=next_test_rpc(&bridge).await;
        bridge.apply_rpc_response(Mt5RpcResponsePayload {request_id:request.request_id,ok:true,message:None,payload:Some(payload)});
        assert!(task.await.unwrap().unwrap().is_none());
        assert!(bridge.drain_rpc_requests(1).0.is_empty());
    }
}

#[tokio::test]
async fn already_closed_native_event_settles_close_when_history_read_fails() {
    use crate::core::broker::{mt5_broker::Mt5Broker, traits::OrderManagementProvider};
    for code in ["10029", "10036"] {
        let bridge = Arc::new(test_bridge("native-close-race"));
    let original = trade_event_order("native-closed", "insight-native", TradeUpdateEvent::Filled);
    bridge.upsert_order(original.clone());
    let broker = Mt5Broker::new(bridge.clone());
    let task = tokio::spawn(async move { broker.close_position("native-closed", 1.0, None).await });
    let close = next_test_rpc(&bridge).await;
    bridge.apply_rpc_response(Mt5RpcResponsePayload {
        request_id: close.request_id, ok: false, message: Some(code.into()), payload: None,
    });
    let read = next_test_rpc(&bridge).await;
    assert!(matches!(read.action, Mt5RpcAction::ReconcileOrders));
    let mut terminal = original;
    terminal.status = TradeUpdateEvent::Closed;
    terminal.filled_price = Some(98.5);
    terminal.realized_pnl = Some(-1.5);
    terminal.commission = Some(0.2);
    terminal.swap = Some(-0.1);
    bridge.apply_trade_event("native-close-race", None, Mt5TradeEventPayload { analytics_only: false,
        execution_fill: None,
        native_event_id: Some("native-close-confirmation".into()),
        event_seq: None, source_occurred_at_us: None, adapter_queued_at_us: None,
        event: TradeUpdateEvent::Closed, order: terminal,
    });
    bridge.apply_rpc_response(Mt5RpcResponsePayload {
        request_id: read.request_id, ok: false, message: Some("history unavailable".into()), payload: None,
    });
    assert!(task.await.unwrap().unwrap());
    // Keep the one actual broker event; do not duplicate it or resend a close.
    let events = bridge.drain_trade_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].1, TradeUpdateEvent::Closed);
    assert_eq!(events[0].0.realized_pnl, Some(-1.5));
    assert!(bridge.drain_rpc_requests(1).0.is_empty());
    }
}

#[tokio::test]
async fn already_closed_without_terminal_proof_retains_execution_error() {
    use crate::core::broker::{mt5_broker::Mt5Broker, traits::OrderManagementProvider};
    for code in ["10029", "10036"] {
        let bridge = Arc::new(test_bridge("unconfirmed-close"));
    bridge.upsert_order(trade_event_order("unknown-ticket", "insight-unknown", TradeUpdateEvent::Filled));
    let broker = Mt5Broker::new(bridge.clone());
    let task = tokio::spawn(async move { broker.close_position("unknown-ticket", 1.0, None).await });
    let close = next_test_rpc(&bridge).await;
    bridge.apply_rpc_response(Mt5RpcResponsePayload {
        request_id: close.request_id, ok: false, message: Some(code.into()), payload: None,
    });
    for _ in 0..3 {
        let read = next_test_rpc(&bridge).await;
        assert!(matches!(read.action, Mt5RpcAction::ReconcileOrders));
        bridge.apply_rpc_response(Mt5RpcResponsePayload {
            request_id: read.request_id, ok: true, message: None,
            payload: Some(serde_json::json!([{"orderId": "unknown-ticket", "status": "PositionOpen"}])),
        });
    }
    assert!(matches!(task.await.unwrap(), Err(BrokerError::TradeError(message)) if message == code));
    assert!(bridge.drain_trade_events().is_empty());
    assert!(bridge.drain_rpc_requests(1).0.is_empty());
    }
}

#[test]
fn session_token_auth_accepts_stale_session_with_valid_token() {
    let bridge = test_bridge("test-token");
    bridge.set_session_id("current-session");
    let headers = headers("old-session", "test-token");

    assert!(authorize_session_token(&bridge, &headers, "old-session").is_ok());
}

#[test]
fn multi_symbol_split_cancellations_preserve_ticket_asset() {
    let bridge = test_bridge("test-token");
    // One EA on BTCUSD services both ETH and SOL, including nested split tickets.
    for (index, symbol) in ["ETHUSD", "SOLUSD"].into_iter().enumerate() {
        for split in 0..91 {
            let ticket = format!("{index}-{split}");
            let insight = format!("insight-{ticket}");
            let mut original = trade_event_order(&ticket, &insight, TradeUpdateEvent::Accepted);
            original.asset = default_mt5_asset(symbol);
            bridge.upsert_order(original.clone());
            let mut malformed = original;
            malformed.asset = default_mt5_asset("BTCUSD");
            malformed.insight_id = None;
            malformed.status = TradeUpdateEvent::Cancelled;
            bridge.apply_trade_event(
                "session",
                None,
                Mt5TradeEventPayload { analytics_only: false,
                    execution_fill: None,
                    native_event_id: Some(format!("cancel-{ticket}")),
                    event_seq: None,
                    source_occurred_at_us: None,
                    adapter_queued_at_us: None,
                    event: TradeUpdateEvent::Cancelled,
                    order: malformed,
                },
            );
            let cached = bridge.order(&ticket).unwrap();
            assert_eq!(cached.asset.symbol, symbol);
            assert_eq!(cached.insight_id.as_deref(), Some(insight.as_str()));
            // The RPC acknowledgement uses the cache, so it must also stay on ETH/SOL.
            bridge.emit_order_event(cached, TradeUpdateEvent::Cancelled);
            let events = bridge.drain_trade_events();
            assert_eq!(events.len(), 2);
            for (order, event) in events {
                assert_eq!(order.order_id, ticket);
                assert_eq!(order.asset.symbol, symbol);
                assert_eq!(event, TradeUpdateEvent::Cancelled);
            }
        }
    }
}

#[test]
fn ea_order_events_never_use_chart_or_request_symbol() {
    let source = include_str!("../../../integrations/mt5/AqeMt5BridgeEA.mq5");
    let handler = source
        .split("void ProcessTradeTransaction(")
        .nth(1)
        .unwrap()
        .split("void OnTradeTransaction(")
        .next()
        .unwrap();
    assert!(!handler.contains("_Symbol"));
    assert!(!handler.contains("request.symbol"));
    assert!(!handler.contains("request.comment"));
    assert!(!handler.contains("result.retcode"));
    assert!(handler.contains("string symbol = trans.symbol"));
    assert!(handler.contains("HistoryOrderGetString(trans.order, ORDER_SYMBOL)"));
    assert!(handler.contains("OrderGetString(ORDER_SYMBOL)"));
    assert!(handler.contains("HistoryDealGetString(trans.deal, DEAL_SYMBOL)"));
}

#[test]
fn late_acceptance_does_not_resurrect_cancelled_cached_order() {
    let bridge = test_bridge("test-token");
    let cancelled = trade_event_order("ticket", "insight", TradeUpdateEvent::Cancelled);
    bridge.upsert_order(cancelled.clone());
    let mut accepted = cancelled.clone();
    accepted.status = TradeUpdateEvent::Accepted;
    bridge.upsert_order(accepted);
    assert_eq!(
        bridge.order("ticket").unwrap().status,
        TradeUpdateEvent::Cancelled
    );
    let mut filled = cancelled;
    filled.status = TradeUpdateEvent::Filled;
    bridge.upsert_order(filled);
    assert_eq!(
        bridge.order("ticket").unwrap().status,
        TradeUpdateEvent::Filled
    );
}

#[test]
fn reconcile_orders_action_uses_the_expected_ea_wire_name() {
    assert_eq!(
        serde_json::to_value(Mt5RpcAction::ReconcileOrders).unwrap(),
        serde_json::json!("RECONCILE_ORDERS")
    );
}

#[tokio::test]
async fn submission_lookup_is_read_only_and_requires_exact_receipt_identity() {
    use crate::core::broker::{
        commands::{BrokerOrderRequest, SubmissionResolution},
        mt5_broker::Mt5Broker,
        traits::OrderManagementProvider,
    };
    let bridge = Arc::new(test_bridge("submission-lookup"));
    let account: Account = serde_json::from_value(serde_json::json!({
        "account_id": "123", "account_type": "Live", "equity": 100000.0,
        "cash": 100000.0, "currency": "USD", "buying_power": 90000.0,
        "shorting_enabled": true, "leverage": 100
    }))
    .unwrap();
    bridge.apply_snapshot(
        "lookup",
        Mt5SnapshotPayload {
            account: Some(account),
            ..Default::default()
        },
    );
    let broker = Mt5Broker::new(bridge.clone());
    let mut request = BrokerOrderRequest::from(&crate::core::insight::Insight::default());
    request.symbol = "AAPL".into();
    request.side = OrderSide::Buy;
    for case in 0..4 {
        let lookup_broker = broker.clone();
        let lookup_request = request.clone();
        let task = tokio::spawn(async move {
            lookup_broker
                .reconcile_submission("123", &lookup_request)
                .await
        });
        let rpc = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let (mut requests, _) = bridge.drain_rpc_requests(1);
                if let Some(request) = requests.pop() {
                    break request;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(rpc.action, Mt5RpcAction::GetSubmission));
        assert_eq!(rpc.payload["submissionAccount"], "123");
        let mut order = trade_event_order(
            "broker-ticket",
            &request.insight_id,
            TradeUpdateEvent::Accepted,
        );
        if case == 3 {
            order.asset.symbol = "OTHER".into();
        }
        let fingerprint = if case == 2 {
            serde_json::json!("wrong")
        } else {
            rpc.payload["submissionFingerprint"].clone()
        };
        bridge.apply_rpc_response(Mt5RpcResponsePayload { request_id: rpc.request_id, ok: true,
            message: None, payload: Some(serde_json::json!({ "fingerprint": fingerprint,
                "order": if case == 0 { Value::Null } else { serde_json::to_value(order).unwrap() } })) });
        let result = task.await.unwrap();
        match case {
            0 => assert!(matches!(result.unwrap(), SubmissionResolution::Unknown)),
            1 => assert!(
                matches!(result.unwrap(), SubmissionResolution::Confirmed(order) if order.order_id == "broker-ticket")
            ),
            _ => assert!(result.is_err()),
        }
    }
    assert!(
        broker
            .reconcile_submission("other-account", &request)
            .await
            .is_err()
    );
    assert!(bridge.drain_rpc_requests(1).0.is_empty());
}

#[test]
fn v2_authenticate_decodes_the_ea_camel_case_wire_frame() {
    let frame = serde_json::from_value::<Mt5BridgeV2Inbound>(serde_json::json!({
        "type": "Authenticate",
        "nonce": "nonce-1",
        "token": "test",
        "sessionId": "session-1",
        "lastAcknowledgedSequence": 41
    }))
    .unwrap();

    match frame {
        Mt5BridgeV2Inbound::Authenticate {
            nonce,
            token,
            session_id,
            last_acknowledged_sequence,
        } => {
            assert_eq!(nonce, "nonce-1");
            assert_eq!(token, "test");
            assert_eq!(session_id, "session-1");
            assert_eq!(last_acknowledged_sequence, Some(41));
        }
        _ => panic!("expected Authenticate frame"),
    }
}

#[test]
fn v2_post_authentication_frames_decode_camel_case_fields() {
    let trade_batch = serde_json::from_value::<Mt5BridgeV2Inbound>(serde_json::json!({
        "type": "TradeEventBatch",
        "sequence": 42,
        "events": [],
        "droppedCount": 3
    }))
    .unwrap();
    match trade_batch {
        Mt5BridgeV2Inbound::TradeEventBatch {
            sequence,
            events,
            dropped_count,
        } => {
            assert_eq!(sequence, Some(42));
            assert!(events.is_empty());
            assert_eq!(dropped_count, Some(3));
        }
        _ => panic!("expected TradeEventBatch frame"),
    }

    let acknowledgement = serde_json::from_value::<Mt5BridgeV2Inbound>(serde_json::json!({
        "type": "BatchAcknowledgement",
        "brokerEventSequence": 42,
        "correlationId": "market:42"
    }))
    .unwrap();
    match acknowledgement {
        Mt5BridgeV2Inbound::BatchAcknowledgement {
            broker_event_sequence,
            correlation_id,
        } => {
            assert_eq!(broker_event_sequence, Some(42));
            assert_eq!(correlation_id.as_deref(), Some("market:42"));
        }
        _ => panic!("expected BatchAcknowledgement frame"),
    }
}

#[test]
fn token_auth_rejects_empty_configured_secrets() {
    for token in ["", "   "] {
        let bridge = test_bridge(token);
        let headers = headers("session", token);
        assert!(authorize_session_token(&bridge, &headers, "session").is_err());
        assert!(authorize_token(&bridge, &headers).is_err());
    }
}

#[test]
fn session_token_auth_rejects_invalid_token() {
    let bridge = test_bridge("test-token");
    let headers = headers("old-session", "wrong-token");

    assert_eq!(
        authorize_session_token(&bridge, &headers, "old-session"),
        Err("invalid MT5 bridge token".to_string())
    );
}

#[test]
fn session_token_auth_rejects_mismatched_session_header() {
    let bridge = test_bridge("test-token");
    let headers = headers("other-session", "test-token");

    assert_eq!(
        authorize_session_token(&bridge, &headers, "old-session"),
        Err("invalid MT5 bridge session".to_string())
    );
}

#[test]
fn pending_rpc_guard_cleans_abandoned_request() {
    let bridge = test_bridge("test-token");
    let request_id = "request-1".to_string();
    let (tx, _rx) = oneshot::channel();
    bridge.pending_rpc.lock().insert(request_id.clone(), tx);
    bridge.rpc_queue.lock().push_back(Mt5RpcRequest {
        session_id: String::new(),
        request_id: request_id.clone(),
        action: Mt5RpcAction::GetAccount,
        payload: serde_json::json!({}),
    });

    {
        let _guard = PendingMt5RpcRequest::new(
            request_id.clone(),
            bridge.rpc_queue.clone(),
            bridge.pending_rpc.clone(),
        );
    }

    assert!(!bridge.pending_rpc.lock().contains_key(&request_id));
    assert!(
        !bridge
            .rpc_queue
            .lock()
            .iter()
            .any(|request| request.request_id == request_id)
    );
}

#[test]
fn bounded_trade_events_preserve_order_metadata_and_unconsumed_tail() {
    let bridge = test_bridge("bounded-test");
    let insight_id = uuid::Uuid::new_v4().to_string();
    for sequence in [4, 2, 3, 1, 5] {
        bridge.enqueue_trade_event(
            sequence,
            uuid::Uuid::new_v4(),
            trade_event_order(
                &format!("order-{sequence}"),
                &insight_id,
                TradeUpdateEvent::Accepted,
            ),
            TradeUpdateEvent::Accepted,
            BrokerEventMetadata { analytics_only: false,
                execution_fill: None,
                source_occurred_at_us: Some(sequence as i64),
                adapter_queued_at_us: None,
                source_clock: BrokerClockDomain::RuntimeHost,
            },
        );
    }
    assert!(bridge.drain_trade_events_bounded(0).is_empty());
    assert!(bridge.has_pending_trade_events());
    let first = bridge.drain_trade_events_bounded(2);
    assert_eq!(
        first
            .iter()
            .map(|(order, _)| order.order_id.as_str())
            .collect::<Vec<_>>(),
        ["order-1", "order-2"]
    );
    assert_eq!(bridge.event_queue.lock().len(), 3);
    let rest = bridge.drain_broker_events();
    assert_eq!(
        rest.iter()
            .map(|event| event.source_sequence.unwrap())
            .collect::<Vec<_>>(),
        [3, 4, 5]
    );
    assert_eq!(
        rest.iter()
            .map(|event| event.metadata.source_occurred_at_us.unwrap())
            .collect::<Vec<_>>(),
        [3, 4, 5]
    );
    assert!(!bridge.has_pending_trade_events());
}

#[tokio::test]
async fn exact_fill_metadata_reaches_durable_ledger_without_cumulative_order_double_counting() {
    use crate::core::broker::command_journal::{BrokerCommandJournal,ExecutionFillBatch};
    use aqe_live_protocol::{ExecutionFill,ExecutionFillInput};
    let mut config=test_bridge("fill-source").config().clone();
    config.symbol_map.insert("CUSTOM-FUTURE".into(),"AAPL".into());
    let bridge=Mt5Bridge::new(config);
    let fill:ExecutionFill=serde_json::from_value(serde_json::json!({
        "version":1,"provider":"mt5-deal-v1","fill_id":"1234","order_id":"original-order","position_id":"position",
        "symbol":"AAPL","side":"buy","effect":"entry","quantity":2.0,"price":100.25,
        "occurred_at_us":100,"as_of_us":200,"cash":{"currency":"GBP","gross_pnl":0.,"commission":-0.25,"carry":0.},
        "account_notional":null
    })).unwrap();
    let mut order=trade_event_order("position","owner",TradeUpdateEvent::PartialFilled);
    order.qty=10.;order.filled_qty=6.;
    let payload=Mt5TradeEventPayload { analytics_only: false,execution_fill:Some(Box::new(fill)),native_event_id:Some("deal:1234".into()),event_seq:Some(1),
        source_occurred_at_us:Some(100),adapter_queued_at_us:Some(201),event:TradeUpdateEvent::PartialFilled,order};
    let mut legacy=serde_json::to_value(&payload).unwrap();legacy.as_object_mut().unwrap().remove("executionFill");
    assert!(serde_json::from_value::<Mt5TradeEventPayload>(legacy).unwrap().execution_fill.is_none());
    bridge.apply_trade_event("before-reconnect",None,payload.clone());
    bridge.apply_trade_event("after-reconnect",None,payload);
    let events=bridge.drain_broker_events();assert_eq!(events.len(),2);
    assert_ne!(events[0].event_id,events[1].event_id);
    let inputs=events.into_iter().map(|e| {
        assert_eq!(e.event.0.filled_qty,6.,"analytics must not rewrite the order reducer's quantity");
        let fill=e.metadata.execution_fill.unwrap();assert_eq!(fill.symbol,"CUSTOM-FUTURE");assert_eq!(fill.quantity,2.);
        ExecutionFillInput {insight_id:e.event.0.insight_id,fill:*fill}
    }).collect();
    let dir=std::env::temp_dir().join(format!("aqe-bridge-fills-{}",uuid::Uuid::new_v4()));
    let mut journal=BrokerCommandJournal::open(dir.join("commands.db"),"strategy","account").await.unwrap();
    journal.record_execution_fills(&ExecutionFillBatch::new(inputs).unwrap()).await.unwrap();
    let page=journal.execution_fill_page(0,None,128).await.unwrap();assert_eq!(page.records.len(),1);
    assert_eq!(page.records[0].observation.fill.cash.as_ref().unwrap().net(),Some(0.25));
    assert_eq!(page.records[0].observation.insight_id.as_deref(),Some("owner"));
    drop(journal);std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn historical_fill_revisions_update_evidence_without_reapplying_order_lifecycle() {
    use crate::core::broker::command_journal::{BrokerCommandJournal, ExecutionFillBatch};
    use aqe_live_protocol::{ExecutionFill, ExecutionFillInput};
    let bridge = test_bridge("correction-source");
    let broker_events = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&broker_events);
    bridge.subscribe_broker_event_stream(Arc::new(move |event| observed.lock().push(event)));
    let trade_events = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&trade_events);
    bridge.subscribe_trade_stream(Arc::new(move |event| observed.lock().push(event)));
    let fill: ExecutionFill = serde_json::from_value(serde_json::json!({
        "version":1,"provider":"mt5-deal-v1","fill_id":"deal","order_id":"original-order",
        "position_id":"position","symbol":"AAPL","side":"buy","effect":"exit",
        "quantity":2.,"price":100.,"occurred_at_us":100,"as_of_us":200,
        "cash":{"currency":"USD","gross_pnl":10.,"commission":1.,"carry":0.},"account_notional":null
    })).unwrap();
    let original = Mt5TradeEventPayload {
        analytics_only: false, execution_fill: Some(Box::new(fill)),
        native_event_id: Some("deal:add".into()), event_seq: Some(1),
        source_occurred_at_us: Some(100), adapter_queued_at_us: Some(200),
        event: TradeUpdateEvent::Closed,
        order: trade_event_order("position", "owner", TradeUpdateEvent::Closed),
    };
    let mut legacy = serde_json::to_value(&original).unwrap();
    legacy.as_object_mut().unwrap().remove("analyticsOnly");
    assert!(!serde_json::from_value::<Mt5TradeEventPayload>(legacy).unwrap().analytics_only);
    bridge.apply_trade_event("session", None, original.clone());
    assert_eq!(bridge.drain_trade_events().len(), 1);
    let cached_before = serde_json::to_value(bridge.order("position").unwrap()).unwrap();
    for revision in 1..=2 {
        let mut corrected = original.clone();
        corrected.analytics_only = true;
        corrected.event_seq = Some(revision + 1);
        corrected.native_event_id = Some(format!("deal:revision:{revision}"));
        // Thin history routing metadata must not erase the established owner.
        corrected.order.insight_id = None;
        corrected.order.qty = 999.;
        corrected.order.filled_qty = 999.;
        let fill = corrected.execution_fill.as_mut().unwrap();
        fill.as_of_us = 200 + revision as i64;
        fill.cash.as_mut().unwrap().commission = Some(1. + revision as f64);
        bridge.apply_trade_event("session", None, corrected.clone());
        bridge.apply_trade_event("session", None, corrected); // transport retry
    }
    let mut invalid = original.clone();
    invalid.analytics_only = true;
    invalid.execution_fill = None;
    bridge.apply_trade_event("session", None, invalid);
    assert_eq!(trade_events.lock().len(), 1);
    assert!(bridge.drain_trade_events().is_empty());
    assert!(!bridge.has_pending_trade_events());
    assert_eq!(serde_json::to_value(bridge.order("position").unwrap()).unwrap(), cached_before);
    assert_eq!(broker_events.lock().len(), 3);
    // A stale redelivery in a new bridge session is still economically deduped.
    bridge.apply_trade_event("reconnected", None, Mt5TradeEventPayload {
        analytics_only: true, ..original
    });
    let inputs = broker_events.lock().iter().map(|event| ExecutionFillInput {
        insight_id: event.event.0.insight_id.clone(),
        fill: *event.metadata.execution_fill.clone().unwrap(),
    }).collect();
    let dir = std::env::temp_dir().join(format!("aqe-bridge-revisions-{}", uuid::Uuid::new_v4()));
    let mut journal = BrokerCommandJournal::open(dir.join("commands.db"), "strategy", "account").await.unwrap();
    journal.record_execution_fills(&ExecutionFillBatch::new(inputs).unwrap()).await.unwrap();
    drop(journal);
    let journal = BrokerCommandJournal::open(dir.join("commands.db"), "strategy", "account").await.unwrap();
    let page = journal.execution_fill_page(0, None, 32).await.unwrap();
    assert_eq!(page.records.len(), 3);
    assert_eq!(page.records.iter().map(|r| r.observation.fill.cash.as_ref().unwrap().net().unwrap()).collect::<Vec<_>>(), [9.,8.,7.]);
    assert!(page.records.iter().all(|r| r.observation.insight_id.as_deref()==Some("owner")));
    assert!(journal.recovery_page(0,32).await.unwrap().is_empty());
    drop(journal);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn trade_events_drain_in_sequence_order() {
    let bridge = test_bridge("test-token");
    bridge.apply_trade_event(
        "session-1",
        None,
        Mt5TradeEventPayload { analytics_only: false,
            execution_fill: None,
            native_event_id: Some("event-2".to_string()),
            event_seq: Some(2),
            source_occurred_at_us: None,
            adapter_queued_at_us: None,
            event: TradeUpdateEvent::Closed,
            order: trade_event_order("order-2", "insight-1", TradeUpdateEvent::Closed),
        },
    );
    bridge.apply_trade_event(
        "session-1",
        None,
        Mt5TradeEventPayload { analytics_only: false,
            execution_fill: None,
            native_event_id: Some("event-1".to_string()),
            event_seq: Some(1),
            source_occurred_at_us: None,
            adapter_queued_at_us: None,
            event: TradeUpdateEvent::Filled,
            order: trade_event_order("order-1", "insight-1", TradeUpdateEvent::Filled),
        },
    );

    let events = bridge.drain_trade_events();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].0.order_id, "order-1");
    assert_eq!(events[0].1, TradeUpdateEvent::Filled);
    assert_eq!(events[1].0.order_id, "order-2");
    assert_eq!(events[1].1, TradeUpdateEvent::Closed);
}

#[actix_web::test]
async fn revision_endpoint_requires_auth_and_forces_analytics_only() {
    let bridge = Arc::new(test_bridge("revision-token"));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let capture = observed.clone();
    bridge.subscribe_broker_event_stream(Arc::new(move |event| capture.lock().push(event)));
    bridge.start_ingress_workers();
    let app = actix_web::test::init_service(App::new()
        .app_data(web::Data::from(bridge.clone()))
        .route("/v1/execution-fill-revisions", web::post().to(fill_revisions))).await;
    let payload = serde_json::json!({
        "protocolVersion":1,"sessionId":bridge.session_id(), "requestId":"revision", "payload":{"events":[{
            "event":"Closed", "nativeEventId":"revision-1", "eventSeq":1,
            "order":trade_event_order("position", "owner", TradeUpdateEvent::Closed),
            "executionFill":{"version":1,"provider":"mt5-deal-v1","fill_id":"deal",
                "order_id":"order","position_id":"position","symbol":"AAPL","side":"buy",
                "effect":"exit","quantity":2.,"price":100.,"occurred_at_us":100,"as_of_us":200,
                "cash":{"currency":"USD","gross_pnl":10.,"commission":1.,"carry":0.},
                "account_notional":null}
        }]}
    });
    // Missing analyticsOnly is intentional: the dedicated route is authoritative.
    let request = actix_web::test::TestRequest::post().uri("/v1/execution-fill-revisions")
        .set_json(&payload).to_request();
    assert_eq!(actix_web::test::call_service(&app, request).await.status(), StatusCode::UNAUTHORIZED);
    assert!(observed.lock().is_empty());
    let request = actix_web::test::TestRequest::post().uri("/v1/execution-fill-revisions")
        .insert_header(("x-aqe-mt5-token", "revision-token"))
        .insert_header(("x-aqe-mt5-session", bridge.session_id()))
        .set_json(&payload).to_request();
    assert_eq!(actix_web::test::call_service(&app, request).await.status(), StatusCode::OK);
    tokio::time::timeout(Duration::from_secs(1), async {
        while observed.lock().is_empty() { tokio::task::yield_now().await; }
    }).await.unwrap();
    assert!(observed.lock()[0].metadata.analytics_only);
    assert!(bridge.order("position").is_err());
    assert!(bridge.drain_trade_events().is_empty());
    bridge.stop_ingress_workers();
}

#[tokio::test]
#[ignore = "read-only: requires an explicitly configured running MT5 EA; never starts a strategy"]
async fn live_mt5_read_only_account_quote_history_probe() {
    // Deliberately use the bridge directly: a StrategyState can reconcile orphan
    // orders at startup/teardown and is not appropriate for a read-only probe.
    let mut config = Mt5BridgeConfig::from_env().expect("configure the authorized MT5 token/bind address");
    config.connect_timeout = Duration::from_secs(30);
    config.request_timeout = Duration::from_secs(10);
    let bridge = Arc::new(Mt5Bridge::new(config));
    bridge.start().await.expect("start the temporary read-only listener");
    let result = tokio::time::timeout(Duration::from_secs(55), async {
        bridge.wait_for_rpc_poll().await?;
        let account = bridge.request_account().await?;
        let asset = bridge.request_asset("BTCUSD").await?;
        let quote = bridge.request_latest_quote("BTCUSD").await?;
        let orders = bridge.request_orders().await?;
        let positions = bridge.request_positions().await?;
        let bar = bridge.request_latest_bar("BTCUSD").await?;
        let end = Utc::now();
        let history = bridge.request_history("BTCUSD", end - chrono::Duration::minutes(15), end,
            TimeFrame::new(1, TimeFrameUnit::Minute)).await?;
        assert!(!account.currency.is_empty());
        assert_eq!(asset.symbol, "BTCUSD");
        assert!(quote.bid.is_finite() && quote.bid > 0.);
        assert!(quote.ask.is_finite() && quote.ask >= quote.bid);
        assert!(history.height() > 0, "recent BTCUSD minute history must be available");
        println!("Read-only MT5 probe: account currency={}, ticker={}, quote received, latest bar received, {} historical bars, {} open orders, {} positions; no trading commands sent.",
            account.currency, asset.symbol, history.height(), orders.len(), positions.len());
        assert!(serde_json::to_value(bar).unwrap().is_object());
        Ok::<(), BrokerError>(())
    }).await;
    bridge.shutdown().await;
    result.expect("read-only MT5 probe timed out").expect("read-only MT5 RPC failed");
}

#[tokio::test]
#[ignore = "read-only: requires EA 1.424, automatically observed clock coverage and an explicit entry order/symbol"]
async fn live_mt5_read_only_owned_history_position_probe() {
    use crate::core::broker::{fill_history::ExecutionFillHistoryReader, mt5_broker::Mt5Broker};
    let entry = std::env::var("AQE_MT5_HISTORY_ENTRY_ORDER_ID").expect("select an authorized historical entry order");
    let symbol = std::env::var("AQE_MT5_HISTORY_SYMBOL").expect("select its exact symbol");
    assert!(!entry.trim().is_empty() && !symbol.trim().is_empty());
    let mut config = Mt5BridgeConfig::from_env().expect("configure the authorized MT5 token/bind address");
    config.connect_timeout = Duration::from_secs(30);
    let bridge = Arc::new(Mt5Bridge::new(config));
    bridge.start().await.expect("start temporary read-only listener");
    let result = tokio::time::timeout(Duration::from_secs(50), async {
        bridge.wait_for_rpc_poll().await?;
        let account = bridge.request_account().await?;
        let broker = Mt5Broker::new(bridge.clone());
        let history = broker.read(&account.account_id, &entry, &symbol).await?
            .ok_or_else(||BrokerError::TradeError("history unavailable; scope must be a bounded single-entry position within the EA's automatically observed clock window".into()))?;
        let check = history.position_check().ok_or_else(||BrokerError::TradeError("position snapshot unavailable; verify the running compiled EA version".into()))?;
        println!("Read-only owned-history probe: {} fills, {} invalidations, position quantity matched={}; no orders or strategy state changed.",history.fills.len(),history.invalidations.len(),check.matched);
        if !check.matched { return Err(BrokerError::TradeError("fill prefix and broker position quantity disagree".into())); }
        Ok::<(), BrokerError>(())
    }).await;
    bridge.shutdown().await;
    result.expect("owned-history probe timed out").expect("owned-history validation failed");
}

#[tokio::test]
#[ignore = "read-only: requires EA with GET_POSITION_VALUATIONS; defaults to an absent-ID capability canary"]
async fn live_mt5_read_only_position_valuation_probe() {
    use crate::core::broker::{position_valuation::PositionValuationReader,mt5_broker::Mt5Broker};
    let symbol=std::env::var("AQE_MT5_VALUATION_SYMBOL").unwrap_or_else(|_|"BTCUSD".into());
    // No discovery by symbol or guessing ownership. An explicit position ID
    // exercises an actual mark; the sentinel checks RPC compatibility only.
    let selected=std::env::var("AQE_MT5_VALUATION_POSITION_ID").ok();
    let id=selected.clone().unwrap_or_else(||i64::MAX.to_string());
    let mut config=Mt5BridgeConfig::from_env().expect("configure authorized MT5 token/bind address");
    config.connect_timeout=Duration::from_secs(30);
    let bridge=Arc::new(Mt5Bridge::new(config));
    bridge.start().await.expect("start temporary read-only listener");
    let result=tokio::time::timeout(Duration::from_secs(50),async {
        bridge.wait_for_rpc_poll().await?;
        let account=bridge.request_account().await?;
        let values=Mt5Broker::new(bridge.clone()).read_positions(&account.account_id,&symbol,&[id]).await?
            .ok_or_else(||BrokerError::TradeError("position valuation unavailable or unsupported by running EA".into()))?;
        println!("Read-only MT5 valuation probe: {} scoped observations; explicit position selected={}; no orders or strategy state changed.",values.positions.len(),selected.is_some());
        if selected.is_none() {
            assert!(matches!(values.positions[0].state,aqe_live_protocol::PositionValuationState::NotOpen));
            println!("Absent-ID capability check only: this does not validate a live mark or clock coverage.");
        }
        Ok::<(),BrokerError>(())
    }).await;
    bridge.shutdown().await;
    result.expect("valuation probe timed out").expect("read-only valuation validation failed");
}

#[test]
fn trade_event_batches_notify_subscribers_in_sequence_order() {
    let bridge = test_bridge("test-token");
    let callback_order = Arc::new(Mutex::new(Vec::new()));
    let callback_order_clone = callback_order.clone();
    bridge.subscribe_trade_stream(Arc::new(move |(order, _event)| {
        callback_order_clone.lock().push(order.order_id);
    }));

    bridge.apply_trade_event_batch(
        "session-1",
        None,
        vec![
            Mt5TradeEventPayload { analytics_only: false,
                execution_fill: None,
                native_event_id: Some("event-2".to_string()),
                event_seq: Some(2),
                source_occurred_at_us: None,
                adapter_queued_at_us: None,
                event: TradeUpdateEvent::Closed,
                order: trade_event_order("order-2", "insight-1", TradeUpdateEvent::Closed),
            },
            Mt5TradeEventPayload { analytics_only: false,
                execution_fill: None,
                native_event_id: Some("event-1".to_string()),
                event_seq: Some(1),
                source_occurred_at_us: None,
                adapter_queued_at_us: None,
                event: TradeUpdateEvent::Filled,
                order: trade_event_order("order-1", "insight-1", TradeUpdateEvent::Filled),
            },
        ],
    );

    assert_eq!(
        callback_order.lock().as_slice(),
        ["order-1".to_string(), "order-2".to_string()]
    );
}

#[tokio::test]
async fn ordered_trade_ingress_acknowledges_before_dispatch_and_preserves_sequence() {
    let bridge = Arc::new(test_bridge("test-token"));
    let callback_order = Arc::new(Mutex::new(Vec::new()));
    let callback_order_clone = Arc::clone(&callback_order);
    bridge.subscribe_trade_stream(Arc::new(move |(order, _event)| {
        callback_order_clone.lock().push(order.order_id);
    }));
    bridge.start_ingress_workers();

    let accepted = bridge
        .enqueue_trade_ingress(TradeIngress {
            session_id: "session-1".to_string(),
            envelope_sequence: None,
            events: vec![
                Mt5TradeEventPayload { analytics_only: false,
                    execution_fill: None,
                    native_event_id: Some("event-2".to_string()),
                    event_seq: Some(2),
                    source_occurred_at_us: None,
                    adapter_queued_at_us: None,
                    event: TradeUpdateEvent::Closed,
                    order: trade_event_order("order-2", "insight-1", TradeUpdateEvent::Closed),
                },
                Mt5TradeEventPayload { analytics_only: false,
                    execution_fill: None,
                    native_event_id: Some("event-1".to_string()),
                    event_seq: Some(1),
                    source_occurred_at_us: None,
                    adapter_queued_at_us: None,
                    event: TradeUpdateEvent::Filled,
                    order: trade_event_order("order-1", "insight-1", TradeUpdateEvent::Filled),
                },
            ],
            dropped_count: 0,
        })
        .unwrap();

    assert_eq!(accepted, 2);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if callback_order.lock().len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ordered ingress worker should dispatch promptly");
    assert_eq!(
        callback_order.lock().as_slice(),
        ["order-1".to_string(), "order-2".to_string()]
    );
    bridge.stop_ingress_workers();
}

#[test]
fn market_data_ingress_applies_bounded_backpressure() {
    let bridge = test_bridge("test-token");
    let (sender, _receiver) = mpsc::channel::<MarketDataIngress>(2);
    *bridge.market_data_ingress_tx.lock() = Some(sender);

    for _ in 0..2 {
        bridge
            .enqueue_market_data_ingress(MarketDataIngress {
                session_id: "session-1".to_string(),
                payload: Mt5MarketDataPayload::default(),
            })
            .unwrap();
    }
    let error = bridge
        .enqueue_market_data_ingress(MarketDataIngress {
            session_id: "session-1".to_string(),
            payload: Mt5MarketDataPayload::default(),
        })
        .unwrap_err();

    assert!(error.contains("queue is full"));
    assert_eq!(bridge.market_data_ingress_depth.load(Ordering::Relaxed), 2);
}

#[actix_web::test]
async fn actix_health_endpoint_preserves_bridge_response_shape() {
    let bridge = Arc::new(test_bridge("test-token"));
    let bridge_data: web::Data<Mt5Bridge> = web::Data::from(bridge);
    let app = actix_web::test::init_service(
        App::new()
            .app_data(bridge_data)
            .route("/health", web::get().to(health)),
    )
    .await;

    let request = actix_web::test::TestRequest::get()
        .uri("/health")
        .to_request();
    let response: Mt5BridgeResponse<Value> =
        actix_web::test::call_and_read_body_json(&app, request).await;

    assert!(response.ok);
    assert_eq!(response.protocol_version, PROTOCOL_VERSION);
    assert_eq!(response.execution_fill_revision_version, 1);
    assert!(response.payload.is_some());
    assert!(response.payload.as_ref().unwrap().get("diagnostics").is_none());
    let authenticated = actix_web::test::TestRequest::get().uri("/health")
        .insert_header(("x-aqe-mt5-token", "test-token")).to_request();
    let full: Mt5BridgeResponse<Value> = actix_web::test::call_and_read_body_json(&app, authenticated).await;
    assert!(full.payload.unwrap().get("diagnostics").is_some());
    let invalid = actix_web::test::TestRequest::get().uri("/health")
        .insert_header(("x-aqe-mt5-token", "wrong-token")).to_request();
    assert_eq!(actix_web::test::call_service(&app, invalid).await.status(), StatusCode::UNAUTHORIZED);
    let mut legacy = serde_json::to_value(&response).unwrap();
    legacy.as_object_mut().unwrap().remove("executionFillRevisionVersion");
    let legacy: Mt5BridgeResponse<Value> = serde_json::from_value(legacy).unwrap();
    assert_eq!(legacy.execution_fill_revision_version, 0);
}

#[tokio::test]
async fn actix_bridge_server_starts_and_stops_on_tokio_runtime() {
    let bridge = Arc::new(test_bridge("test-token"));

    assert!(bridge.start().await.unwrap());
    assert!(bridge.server_started.load(Ordering::SeqCst));
    assert!(bridge.server_control.lock().is_some());

    bridge.shutdown().await;
    assert!(!bridge.server_started.load(Ordering::SeqCst));
    assert!(bridge.server_control.lock().is_none());
}

#[test]
fn trade_events_dedupe_by_native_event_id_not_order_status() {
    let bridge = test_bridge("test-token");
    for (native_id, sequence) in [("fill-1", 1), ("fill-2", 2), ("fill-1", 3)] {
        bridge.apply_trade_event(
            "session-1",
            None,
            Mt5TradeEventPayload { analytics_only: false,
                execution_fill: None,
                native_event_id: Some(native_id.to_string()),
                event_seq: Some(sequence),
                source_occurred_at_us: None,
                adapter_queued_at_us: None,
                event: TradeUpdateEvent::Filled,
                order: trade_event_order("same-order", "insight-1", TradeUpdateEvent::Filled),
            },
        );
    }

    let events = bridge.drain_trade_events();

    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .all(|(_, event)| *event == TradeUpdateEvent::Filled)
    );
}

#[test]
fn trade_event_dedupe_evicts_oldest_ids_at_capacity() {
    let bridge = test_bridge("test-token");
    for index in 0..=MAX_SEEN_EVENT_IDS {
        assert!(bridge.mark_event_seen(format!("event-{index}")));
    }

    let state = bridge.state.read();
    assert_eq!(state.seen_event_ids.len(), MAX_SEEN_EVENT_IDS);
    assert_eq!(state.seen_event_order.len(), MAX_SEEN_EVENT_IDS);
    assert!(!state.seen_event_ids.contains("event-0"));
    assert!(
        state
            .seen_event_ids
            .contains(&format!("event-{MAX_SEEN_EVENT_IDS}"))
    );
}

#[test]
fn reverse_symbol_mapping_uses_precomputed_index() {
    let mut config = test_bridge("test-token").config().clone();
    config
        .symbol_map
        .insert("XAUUSD".to_string(), "GOLD.cash".to_string());
    let bridge = Mt5Bridge::new(config);

    assert_eq!(bridge.aqe_symbol("GOLD.cash"), "XAUUSD");
    assert_eq!(bridge.aqe_symbol("EURUSD"), "EURUSD");
}

#[test]
fn terminal_disconnect_is_distinct_from_bridge_transport() {
    let bridge = test_bridge("test-token");
    {
        let mut state = bridge.state.write();
        state.last_rpc_poll = Some(Utc::now());
        state.terminal_connected = Some(false);
    }

    assert!(bridge.is_transport_connected());
    assert!(!bridge.is_broker_connected());
    assert!(!bridge.is_datafeed_connected());

    bridge.state.write().terminal_connected = Some(true);
    assert!(bridge.is_broker_connected());
}

#[test]
fn account_observations_distinguish_missing_values_from_confirmed_zero_equity() {
    let mut account: Account = serde_json::from_value(serde_json::json!({
        "account_id":"123", "account_type":"Live", "currency":"GBP", "equity":0., "cash":0.,
        "buying_power":0., "shorting_enabled":true, "leverage":100
    })).unwrap();
    assert!(Mt5Bridge::validate_account_observation(&account, None).is_err());
    assert!(Mt5Bridge::validate_account_observation(&account, Some(false)).is_err());
    assert!(Mt5Bridge::validate_account_observation(&account, Some(true)).is_ok());
    account.equity = -10.;
    assert!(Mt5Bridge::validate_account_observation(&account, Some(true)).is_ok());
    account.equity = 100.; account.cash = 100.;
    assert!(Mt5Bridge::validate_account_observation(&account, None).is_ok());
    for invalid in [f64::NAN, f64::INFINITY] {
        account.equity = invalid;
        assert!(Mt5Bridge::validate_account_observation(&account, Some(true)).is_err());
    }
    account.equity = 100.; account.account_id = "0".into();
    assert!(Mt5Bridge::validate_account_observation(&account, Some(true)).is_err());
}

#[tokio::test]
async fn disconnected_terminal_cannot_refresh_or_publish_account_placeholders() {
    let bridge = test_bridge("account-proof");
    let account: Account = serde_json::from_value(serde_json::json!({
        "account_id":"123", "account_type":"Live", "currency":"GBP", "equity":100., "cash":100.,
        "buying_power":90., "shorting_enabled":true, "leverage":100
    })).unwrap();
    bridge.apply_snapshot("account-proof", Mt5SnapshotPayload { account:Some(account.clone()), ..Default::default() });
    assert_eq!(bridge.account().unwrap().equity,100.);
    bridge.state.write().terminal_connected=Some(false);
    let mut placeholder=account; placeholder.equity=0.;placeholder.cash=0.;placeholder.buying_power=0.;
    bridge.apply_snapshot("account-proof", Mt5SnapshotPayload { account:Some(placeholder), ..Default::default() });
    assert!(bridge.account().is_err());
    assert!(bridge.request_account().await.is_err());
    assert!(bridge.rpc_queue.lock().is_empty(),"No disconnected RPC is dispatched");
    assert_eq!(bridge.state.read().account.as_ref().unwrap().equity,100.);
}

#[test]
fn update_order_no_changes_response_is_idempotent_success() {
    assert!(Mt5Bridge::is_idempotent_update_order_response(
        Mt5RpcAction::UpdateOrder,
        Some("10025")
    ));
    assert!(!Mt5Bridge::is_idempotent_update_order_response(
        Mt5RpcAction::SubmitOrder,
        Some("10025")
    ));
    assert!(!Mt5Bridge::is_idempotent_update_order_response(
        Mt5RpcAction::UpdateOrder,
        Some("10016")
    ));
}

#[test]
fn only_proven_pre_submit_failures_are_definitive_rejections() {
    for message in [
        "invalid submission correlation/account",
        "invalid submit order request",
        "submission intent receipt could not be flushed",
    ] {
        assert!(Mt5Bridge::is_definitive_pre_submit_rejection(
            Mt5RpcAction::SubmitOrder,
            message
        ));
    }
    assert!(!Mt5Bridge::is_definitive_pre_submit_rejection(
        Mt5RpcAction::SubmitOrder,
        "submission outcome unknown; reconciliation required"
    ));
    assert!(!Mt5Bridge::is_definitive_pre_submit_rejection(
        Mt5RpcAction::UpdateOrder,
        "invalid submit order request"
    ));
}

#[test]
fn datafeed_connection_requires_fresh_market_data_for_active_subscriptions() {
    let bridge = test_bridge("test-token");
    bridge
        .subscribe_bars(
            vec!["AAPL".to_string()],
            TimeFrame::new(1, TimeFrameUnit::Minute),
            DataStreamMode::CompletedBar,
            Arc::new(|_bar| {}),
        )
        .unwrap();
    {
        let mut state = bridge.state.write();
        state.last_rpc_poll = Some(Utc::now());
        state.terminal_connected = Some(true);
        state.last_subscription_sync = Some(Utc::now());
    }

    assert!(bridge.is_datafeed_connected());

    bridge.state.write().last_subscription_sync =
        Some(Utc::now() - chrono::Duration::seconds(MT5_MARKET_DATA_FRESH_SECS + 1));
    assert!(!bridge.is_datafeed_connected());

    bridge.state.write().last_market_data = Some(Utc::now());
    assert!(bridge.is_datafeed_connected());
}

#[tokio::test]
async fn datafeed_recovery_replays_active_bar_subscriptions() {
    let bridge = Arc::new(test_bridge("test-token"));
    bridge
        .subscribe_bars(
            vec!["AAPL".to_string()],
            TimeFrame::new(1, TimeFrameUnit::Minute),
            DataStreamMode::CompletedBar,
            Arc::new(|_bar| {}),
        )
        .unwrap();
    {
        let mut state = bridge.state.write();
        state.last_rpc_poll = Some(Utc::now());
        state.terminal_connected = Some(true);
    }

    let recovering_bridge = Arc::clone(&bridge);
    let recovery = tokio::spawn(async move { recovering_bridge.recover_datafeed_state().await });
    let request = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(request) = bridge.rpc_queue.lock().pop_front() {
                return request;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("subscription replay should queue an RPC request");

    assert!(matches!(request.action, Mt5RpcAction::SubscribeBars));
    bridge.apply_rpc_response(Mt5RpcResponsePayload {
        request_id: request.request_id,
        ok: true,
        message: None,
        payload: Some(serde_json::json!({})),
    });

    recovery.await.unwrap().unwrap();
    assert!(bridge.state.read().last_subscription_sync.is_some());
    assert!(bridge.is_datafeed_connected());
}

#[test]
fn older_ea_heartbeat_payloads_remain_compatible() {
    let payload: Mt5HeartbeatPayload = serde_json::from_value(serde_json::json!({
        "terminalName": "MetaTrader 5",
        "accountId": "12345",
        "serverTime": "2026-08-27T10:00:00Z"
    }))
    .unwrap();

    assert_eq!(payload.terminal_connected, None);
    assert_eq!(payload.terminal_trade_allowed, None);
}

#[test]
fn trade_callbacks_can_clear_subscribers_reentrantly() {
    let bridge = Arc::new(test_bridge("test-token"));
    let callback_bridge = bridge.clone();
    let callback_count = Arc::new(AtomicUsize::new(0));
    let callback_count_clone = callback_count.clone();
    bridge.subscribe_trade_stream(Arc::new(move |_event| {
        callback_count_clone.fetch_add(1, AtomicOrdering::Relaxed);
        callback_bridge.clear_trade_subscribers();
    }));

    bridge.emit_order_event(
        trade_event_order("order-1", "insight-1", TradeUpdateEvent::Filled),
        TradeUpdateEvent::Filled,
    );

    assert_eq!(callback_count.load(AtomicOrdering::Relaxed), 1);
    assert!(bridge.trade_subscribers.lock().is_empty());
}

#[test]
fn bar_callbacks_can_unsubscribe_reentrantly() {
    let bridge = Arc::new(test_bridge("test-token"));
    let session_id = bridge.session_id();
    let timeframe = TimeFrame::new(1, TimeFrameUnit::Minute);
    let callback_bridge = bridge.clone();
    let callback_count = Arc::new(AtomicUsize::new(0));
    let callback_count_clone = callback_count.clone();
    bridge
        .subscribe_bars(
            vec!["AAPL".to_string()],
            timeframe,
            DataStreamMode::CompletedBar,
            Arc::new(move |_bar| {
                callback_count_clone.fetch_add(1, AtomicOrdering::Relaxed);
                callback_bridge
                    .unsubscribe_bars(
                        vec!["AAPL".to_string()],
                        timeframe,
                        DataStreamMode::CompletedBar,
                    )
                    .unwrap();
            }),
        )
        .unwrap();

    bridge.apply_market_data(
        &session_id,
        Mt5MarketDataPayload {
            bars: vec![Mt5StreamBar {
                timeframe: Some("PERIOD_M1".to_string()),
                bar: Bar {
                    symbol: "AAPL".to_string(),
                    open: 100.0,
                    high: 101.0,
                    low: 99.0,
                    close: 100.5,
                    volume: 10.0,
                    timestamp: Utc::now(),
                },
            }],
            ..Default::default()
        },
    );

    assert_eq!(callback_count.load(AtomicOrdering::Relaxed), 1);
    assert!(bridge.bar_subscribers.lock().is_empty());
}

#[test]
fn retried_completed_bars_are_emitted_once_per_stream() {
    let bridge = test_bridge("test-token");
    let session_id = bridge.session_id();
    let timeframe = TimeFrame::new(1, TimeFrameUnit::Minute);
    let callback_count = Arc::new(AtomicUsize::new(0));
    let callback_count_clone = callback_count.clone();
    bridge
        .subscribe_bars(
            vec!["AAPL".to_string()],
            timeframe,
            DataStreamMode::CompletedBar,
            Arc::new(move |_bar| {
                callback_count_clone.fetch_add(1, AtomicOrdering::Relaxed);
            }),
        )
        .unwrap();

    let completed_bar = Mt5StreamBar {
        timeframe: Some("PERIOD_M1".to_string()),
        bar: Bar {
            symbol: "AAPL".to_string(),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 10.0,
            timestamp: Utc::now(),
        },
    };
    for _ in 0..2 {
        bridge.apply_market_data(
            &session_id,
            Mt5MarketDataPayload {
                bars: vec![completed_bar.clone()],
                ..Default::default()
            },
        );
    }

    assert_eq!(callback_count.load(AtomicOrdering::Relaxed), 1);
}

#[test]
fn unchanged_intrabar_ticks_are_not_emitted_twice() {
    let bridge = test_bridge("test-token");
    let session_id = bridge.session_id();
    let timeframe = TimeFrame::new(1, TimeFrameUnit::Minute);
    let callback_count = Arc::new(AtomicUsize::new(0));
    let callback_count_clone = callback_count.clone();
    bridge
        .subscribe_bars(
            vec!["AAPL".to_string()],
            timeframe,
            DataStreamMode::Intrabar,
            Arc::new(move |_bar| {
                callback_count_clone.fetch_add(1, AtomicOrdering::Relaxed);
            }),
        )
        .unwrap();

    let quote = Quote {
        profit_model: None,
        symbol: "AAPL".to_string(),
        bid: 100.0,
        ask: 100.2,
        bid_size: 0.0,
        ask_size: 0.0,
        last: Some(100.1),
        last_size: None,
        timestamp: Utc::now(),
    };
    for _ in 0..2 {
        bridge.apply_market_data(
            &session_id,
            Mt5MarketDataPayload {
                quotes: vec![quote.clone()],
                ..Default::default()
            },
        );
    }

    assert_eq!(callback_count.load(AtomicOrdering::Relaxed), 1);
}

#[test]
fn quote_and_authoritative_intrabar_volume_remain_independent() {
    let bridge = test_bridge("test-token");
    let session = bridge.session_id();
    let bars = Arc::new(Mutex::new(Vec::<Bar>::new()));
    let received = bars.clone();
    bridge
        .subscribe_bars(
            vec!["AAPL".into()],
            TimeFrame::new(1, TimeFrameUnit::Minute),
            DataStreamMode::Intrabar,
            Arc::new(move |bar| received.lock().push(bar)),
        )
        .unwrap();
    let time = Utc::now();
    let start = TimeFrame::new(1, TimeFrameUnit::Minute).get_current_time_increment(time);
    let candle = Mt5StreamBar {
        timeframe: Some("PERIOD_M1".into()),
        bar: Bar {
            symbol: "AAPL".into(),
            open: 100.0,
            high: 105.0,
            low: 99.0,
            close: 101.0,
            volume: 40.0,
            timestamp: start,
        },
    };
    let quote = Quote {
        profit_model: None,
        symbol: "AAPL".into(),
        bid: 101.0,
        ask: 102.0,
        bid_size: 0.0,
        ask_size: 0.0,
        last: Some(101.5),
        last_size: None,
        timestamp: time,
    };
    let send = |candle: Mt5StreamBar| {
        bridge.apply_market_data(
            &session,
            Mt5MarketDataPayload {
                quotes: vec![quote.clone()],
                intrabar_bars: vec![candle],
                ..Default::default()
            },
        )
    };
    send(candle.clone());
    send(candle.clone()); // Duplicate delivery must neither emit nor add volume.
    let mut next = candle.clone();
    next.bar.volume = 43.0;
    send(next); // Volume changed but quote/close did not: still emit the candle.
    send(candle.clone()); // Out-of-order cumulative snapshot must not regress.
    let mut next_bucket = candle.clone();
    next_bucket.bar.timestamp += chrono::Duration::minutes(1);
    next_bucket.bar.volume = 1.0;
    send(next_bucket);
    let received = bars.lock();
    assert_eq!(
        received.iter().map(|b| b.volume).collect::<Vec<_>>(),
        vec![40.0, 43.0, 1.0]
    );
    assert!(received.iter().all(|b| b.close == 101.0 && b.high == 105.0));
}

#[test]
fn legacy_quote_payload_remains_readable() {
    let payload: Mt5MarketDataPayload = serde_json::from_value(serde_json::json!({
        "quotes": [], "bars": [], "history": []
    }))
    .unwrap();
    assert!(payload.intrabar_bars.is_empty());
}

#[test]
fn quote_history_transport_retries_do_not_replay_trade_mutations() {
    for action in [
        Mt5RpcAction::GetHistory,
        Mt5RpcAction::GetExecutionFillHistory,
        Mt5RpcAction::GetPositionValuations,
        Mt5RpcAction::GetLatestQuote,
        Mt5RpcAction::SubscribeBars,
        Mt5RpcAction::ReconcileOrders,
        Mt5RpcAction::EstimateMargin,
    ] {
        assert!(action.can_retry_after_transport_failure());
    }
    for action in [
        Mt5RpcAction::SubmitOrder,
        Mt5RpcAction::ClosePosition,
        Mt5RpcAction::CloseAllPositions,
        Mt5RpcAction::CancelOrder,
        Mt5RpcAction::UpdateOrder,
    ] {
        assert!(!action.can_retry_after_transport_failure());
    }
}

#[test]
fn authoritative_margin_rpc_is_additive_and_uses_the_stable_wire_name() {
    assert_eq!(
        serde_json::to_value(Mt5RpcAction::EstimateMargin).unwrap(),
        serde_json::Value::String("ESTIMATE_MARGIN".into())
    );
    let ea = include_str!("../../../integrations/mt5/AqeMt5BridgeEA.mq5");
    assert!(ea.contains("if(action == \"ESTIMATE_MARGIN\")"));
    assert!(ea.contains("OrderCalcMargin("));
}

#[test]
fn trade_events_preserve_existing_order_route_and_legs_for_thin_mt5_events() {
    let bridge = test_bridge("test-token");
    let mut existing = trade_event_order("order-1", "insight-1", TradeUpdateEvent::Filled);
    existing.legs = Some(OrderLegs {
        take_profit: Some(OrderLeg {
            order_id: Some("tp-1".to_string()),
            limit_price: Some(110.0),
            trail_price: None,
            side: OrderSide::Sell,
            filled_price: None,
            order_type: OrderType::Limit,
            status: TradeUpdateEvent::Pending,
            order_class: OrderClass::Bracket,
            created_at: 0,
            updated_at: 0,
            submitted_at: 0,
            filled_at: None,
        }),
        stop_loss: Some(OrderLeg {
            order_id: Some("sl-1".to_string()),
            limit_price: Some(95.0),
            trail_price: None,
            side: OrderSide::Sell,
            filled_price: None,
            order_type: OrderType::Stop,
            status: TradeUpdateEvent::Pending,
            order_class: OrderClass::Bracket,
            created_at: 0,
            updated_at: 0,
            submitted_at: 0,
            filled_at: None,
        }),
        trailing_stop: None,
    });
    bridge.upsert_order(existing);

    let mut thin_close = trade_event_order("order-1", "", TradeUpdateEvent::Closed);
    thin_close.insight_id = None;
    thin_close.strategy_type = None;
    thin_close.legs = None;
    bridge.apply_trade_event(
        "session-1",
        None,
        Mt5TradeEventPayload { analytics_only: false,
            execution_fill: None,
            native_event_id: Some("close-1".to_string()),
            event_seq: Some(1),
            source_occurred_at_us: None,
            adapter_queued_at_us: None,
            event: TradeUpdateEvent::Closed,
            order: thin_close,
        },
    );

    let events = bridge.drain_trade_events();

    assert_eq!(events.len(), 1);
    let order = &events[0].0;
    assert_eq!(order.insight_id.as_deref(), Some("insight-1"));
    assert_eq!(order.strategy_type.as_deref(), Some("Testing"));
    assert_eq!(
        order
            .legs
            .as_ref()
            .and_then(|legs| legs.take_profit.as_ref())
            .and_then(|leg| leg.order_id.as_deref()),
        Some("tp-1")
    );
}
