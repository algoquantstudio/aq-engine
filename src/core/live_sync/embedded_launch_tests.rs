//! Owned loopback gateway contract; no broker, strategy process, production
//! gateway, NATS cluster or existing database is used by this test.
use super::*;
use crate::launch_config::{CloudConfig, DeliveryConfig, LaunchConfig, VERSION};
use actix_web::{App, HttpRequest, HttpResponse, HttpServer, web};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[tokio::test]
async fn protected_copied_launch_delivers_in_background_and_acknowledges_its_outbox() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let registrations = Arc::new(AtomicUsize::new(0));
    let allow_registration = Arc::new(AtomicBool::new(true));
    let batches = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
    let regs = registrations.clone();
    let allowed = allow_registration.clone();
    let delivered = batches.clone();
    let events = observed.clone();
    let server = HttpServer::new(move || {
        let regs = regs.clone(); let delivered = delivered.clone(); let events = events.clone(); let allowed = allowed.clone();
        App::new()
            .route("/v1/register", web::post().to(move |body: web::Json<serde_json::Value>| {
                let regs = regs.clone(); let allowed = allowed.clone(); async move {
                    if !allowed.load(Ordering::SeqCst) { return HttpResponse::Unauthorized().finish(); }
                    assert_eq!(body["sessionSecret"], "owned-fixture-secret");
                    assert_eq!(body["strategyId"], "copied-fixture");
                    assert_eq!(body["serverNodeId"], "local");
                    regs.fetch_add(1, Ordering::SeqCst);
                    HttpResponse::Ok().json(serde_json::json!({"token":"owned-fixture-grant","expiresAtUs":super::super::now_epoch_micros()+3_600_000_000i64}))
                }
            }))
            .route("/v1/batch", web::post().to(move |request: HttpRequest, bytes: web::Bytes| {
                let delivered = delivered.clone(); let events = events.clone(); async move {
                    // Small batches now use the production Protobuf transport.
                    // Exercise it rather than silently rejecting it in a stale
                    // JSON-only fixture or changing the client's real format.
                    let batch: super::super::LiveSyncBatch = match request.headers().get("content-type").and_then(|value| value.to_str().ok()) {
                        Some(aqe_live_protocol::protobuf::MIME) => aqe_live_protocol::protobuf::decode(&bytes).unwrap(),
                        Some("application/json") => serde_json::from_slice(&bytes).unwrap(),
                        other => panic!("Unexpected copied-launch batch format: {other:?}"),
                    };
                    assert_eq!(request.headers().get("authorization").unwrap(), "Bearer owned-fixture-grant");
                    assert_eq!(batch.strategy_id, "copied-fixture");
                    assert!(batch.events().all(|e| e.run_id.is_some()));
                    for event in batch.events() { events.lock().push(event.entity_id.clone()); }
                    delivered.fetch_add(1, Ordering::SeqCst);
                    HttpResponse::Ok().json(super::super::LiveSyncBatchResult {
                        persisted_critical_through: batch.last_critical_sequence.unwrap_or(0),
                        applied: batch.events().count(), ..Default::default()
                    })
                }
            }))
            .route("/v1/runs/seal", web::post().to(|| async { HttpResponse::Ok().json(serde_json::json!({})) }))
    }).workers(1).listen(listener).unwrap().run();
    let handle = server.handle();
    let server_task = tokio::spawn(server);
    let directory =
        std::env::temp_dir().join(format!("aqe-copied-launch-{}", uuid::Uuid::new_v4()));
    let launch_path = directory.join("private").join("launch.json");
    let config = LaunchConfig {
        version: VERSION,
        run_id: uuid::Uuid::new_v4().to_string(),
        cloud: Some(CloudConfig {
            url: "http://127.0.0.1:9".into(),
            session_id: uuid::Uuid::new_v4().to_string(),
            session_secret: "owned-fixture-secret".into(),
            strategy_id: "copied-fixture".into(),
            user_id: "users:fixture".into(),
            node_id: "local".into(),
            access_method: "aqe_live".into(),
        }),
        mt5: None,
        delivery: Some(DeliveryConfig::EmbeddedGateway {
            url: endpoint,
            outbox_directory: directory.join("outbox").to_string_lossy().into_owned(),
            account_coordinator_dir: directory
                .join("account-locks")
                .to_string_lossy()
                .into_owned(),
        }),
        fill_export: true,
        capture:None,
    };
    crate::launch_config::write_private(&launch_path, &config).unwrap();
    let config = crate::launch_config::read_private(&launch_path).unwrap();
    let auth = AqsAuth::from_launch(config.cloud.as_ref().unwrap());
    assert_eq!(auth.live_session_id, None);
    let Some(DeliveryConfig::EmbeddedGateway {
        url,
        outbox_directory,
        ..
    }) = config.delivery
    else {
        panic!("copied delivery must be embedded")
    };
    let hosted = HostedSyncConfig::new(&url, outbox_directory)
        .unwrap()
        .with_launch_run(&config.run_id)
        .unwrap();
    let mut sync = EmbeddedHostedSync::start(&hosted, &auth).await.unwrap();
    assert_eq!(
        sync.run_id, config.run_id,
        "the database lifecycle fence must match the prepared AQS run"
    );
    let mut event = super::super::EventEnvelope::new(
        &sync.runtime_id,
        &auth.strategy_id,
        &auth.session_id,
        "copied-lifecycle",
        1,
        super::super::EventPriority::Critical,
        super::super::EventKind::StrategyLifecycle,
        serde_json::json!({"state":"running"}),
    );
    event.run_id = Some(sync.run_id.clone());
    sync.publisher
        .publish_critical(
            super::super::EventSubject::for_priority(
                &sync.runtime_id,
                &auth.strategy_id,
                super::super::EventPriority::Critical,
            ),
            event,
        )
        .unwrap();
    // The caller only submits to its bounded local publisher. A separate worker
    // registers, uploads and acknowledges; it needs no desktop/runtime host.
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if batches.load(Ordering::SeqCst) > 0
                && sync.retirement.1.health().await.unwrap().pending_critical == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background copied delivery must reach the gateway and drain its durable outbox");
    assert!(registrations.load(Ordering::SeqCst) >= 1);
    assert_eq!(observed.lock().as_slice(), ["copied-lifecycle"]);
    sync.shutdown().await;
    drop(sync);
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let error = EmbeddedHostedSync::start(&hosted, &auth)
                .await
                .err()
                .expect("a copied launch cannot run twice");
            if error.contains("already been attempted") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the durable single-use claim must survive worker shutdown");
    allow_registration.store(false, Ordering::SeqCst);
    let rejected = hosted
        .clone()
        .with_launch_run(&uuid::Uuid::new_v4().to_string())
        .unwrap();
    let error = EmbeddedHostedSync::start(&rejected, &auth)
        .await
        .err()
        .expect("rejected credentials must prevent initial copied startup");
    assert!(error.contains("not admitted"));
    assert_eq!(
        observed.lock().as_slice(),
        ["copied-lifecycle"],
        "a denied startup must not publish a running lifecycle"
    );
    handle.stop(true).await;
    server_task.await.unwrap().unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
