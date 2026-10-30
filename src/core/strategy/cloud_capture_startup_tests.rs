// Actual run_live journey with an in-memory paper broker, fixture feed and
// owned loopback gateway. Never connects to MT5, application DB/NATS or Stripe.
mod cloud_capture_startup_regressions {
    use super::*;
    use crate::core::live_sync::{HostedSyncConfig, OutboxStore};
    use crate::core::strategy::{AqsAuth, StrategyStatus};
    use actix_web::{App, HttpResponse, HttpServer, web};
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    #[tokio::test]
    async fn normal_live_cloud_capture_pause_preserves_local_start_and_graceful_stop() {
        run_paused_start_fixture(false,false,None).await;
    }

    #[tokio::test]
    async fn early_stop_with_paused_cloud_capture_still_settles_local_execution() {
        run_paused_start_fixture(true,false,None).await;
    }

    #[tokio::test]
    async fn physical_cloud_capture_pressure_preserves_local_start_and_graceful_stop() {
        run_paused_start_fixture(false,true,None).await;
    }

    #[tokio::test]
    async fn early_stop_under_physical_cloud_pressure_still_settles_local_execution() {
        run_paused_start_fixture(true,true,None).await;
    }

    #[tokio::test]
    async fn live_source_monitor_clean_stop_retains_final_publications_without_a_false_gap(){
        run_paused_start_fixture(false,false,Some(false)).await;
    }

    #[tokio::test]
    async fn live_source_monitor_early_stop_does_not_leave_a_journal_owner_or_false_gap(){
        run_paused_start_fixture(true,false,Some(false)).await;
    }

    #[tokio::test]
    async fn live_source_pressure_does_not_block_local_entry_cleanup_or_stop(){
        run_paused_start_fixture(false,false,Some(true)).await;
    }

    #[tokio::test]
    async fn live_source_monitor_transfers_to_the_recovered_execution_epoch(){
        run_source_start_fixture(false,false,Some(false),true,false).await;
    }

    #[tokio::test]
    async fn failed_first_source_witness_does_not_block_live_paper_execution_or_stop(){
        run_source_start_fixture(false,false,Some(false),false,true).await;
    }

    #[tokio::test]
    async fn failed_first_source_witness_preserves_early_stop_safety(){
        run_source_start_fixture(true,false,Some(false),false,true).await;
    }

    #[tokio::test]
    async fn failed_source_witness_does_not_block_the_recovered_live_execution_epoch(){
        run_source_start_fixture(false,false,Some(false),true,true).await;
    }

    async fn run_paused_start_fixture(pre_stopped: bool,physical:bool,source:Option<bool>) {
        run_source_start_fixture(pre_stopped,physical,source,false,false).await;
    }

    async fn run_source_start_fixture(pre_stopped:bool,physical:bool,source:Option<bool>,recovering_stop:bool,fail_witness:bool){
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let safety = Arc::new(AtomicUsize::new(0));
        let observed = safety.clone();
        let server=HttpServer::new(move||{
            let observed=observed.clone();
            App::new()
                .route("/v1/register",web::post().to(||async{HttpResponse::Ok().json(json!({"token":"fixture","expiresAtUs":crate::core::live_sync::now_epoch_micros()+3_600_000_000i64}))}))
                .route("/v1/heartbeat",web::post().to(||async{HttpResponse::TooManyRequests().json(json!({"version":1,"code":"cloud_quota","cloudPaused":true}))}))
                .route("/v1/batch",web::post().to(||async{HttpResponse::TooManyRequests().json(json!({"version":1,"code":"cloud_quota","cloudPaused":true}))}))
                .route("/v1/safety/register",web::post().to(||async{HttpResponse::Ok().json(json!({"token":"control-fixture","expiresAtUs":crate::core::live_sync::now_epoch_micros()+3_600_000_000i64}))}))
                .route("/v1/safety/observe",web::post().to(move|body:web::Json<aqe_live_protocol::SafetyObservation>|{
                    let observed=observed.clone();async move{body.validate().unwrap();if body.state=="stopped"{observed.fetch_add(1,Ordering::SeqCst);}HttpResponse::Ok().json(json!({"valid":true}))}
                }))
                .route("/v1/safety/actions",web::get().to(||async{HttpResponse::Ok().json(json!({"valid":true,"actions":[]}))}))
                .route("/v1/runs/seal",web::post().to(||async{HttpResponse::Ok().json(json!({}))}))
        }).workers(1).listen(listener).unwrap().run();
        let handle = server.handle();
        let server_task = tokio::spawn(server);
        let directory =
            std::env::temp_dir().join(format!("cloud-paused-start-{}", uuid::Uuid::new_v4()));
        let calls = Arc::new(AtomicUsize::new(0));
        let hosted=HostedSyncConfig::new(&endpoint,directory.join("cloud")).unwrap();
        let source_limit=source.map(|pressure|crate::core::live_sync::PhysicalCaptureLimit {
            physical_limit_bytes:if pressure {crate::core::live_sync::MIN_OPERATIONAL_HEADROOM_BYTES+1}else{32*1024*1024},
            operational_headroom_bytes:crate::core::live_sync::MIN_OPERATIONAL_HEADROOM_BYTES,
        });
        let hosted=if source.is_some(){
            hosted.with_physical_capture_budget(crate::core::live_sync::PhysicalCaptureBudget {
                payload_limit_bytes:4*1024*1024,physical_limit_bytes:64*1024*1024,
                operational_headroom_bytes:crate::core::live_sync::MIN_OPERATIONAL_HEADROOM_BYTES,
            }).unwrap().with_source_journal_capture_limit(source_limit.unwrap()).unwrap()
        }else if physical {
            hosted.with_physical_capture_budget(crate::core::live_sync::PhysicalCaptureBudget {
                payload_limit_bytes:4*1024*1024,
                physical_limit_bytes:crate::core::live_sync::MIN_OPERATIONAL_HEADROOM_BYTES+1,
                operational_headroom_bytes:crate::core::live_sync::MIN_OPERATIONAL_HEADROOM_BYTES,
            }).unwrap()
        } else {hosted.with_capture_payload_limit_bytes(1).unwrap()};
        let mut state = StrategyState::new(
            "PausedCloudFixture".into(),
            "1".into(),
            LiveCommandLifecycleFixture {
                teardown_calls: calls.clone(),
                cleanup: true,
            },
            UnifiedBroker::new(
                PaperBroker::new(AccountType::Paper, 100_000.0, 1),
                FixedScaleOutDataFeed::new(),
            ),
            StrategyMode::Live,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        )
        .with_artifact_root(&directory)
        .with_hosted_sync(hosted);
        let mut insight = Insight::default();
        insight.symbol = "AAPL".into();
        insight.set_quantity(Some(1.0));
        insight.order_type = OrderType::Limit;
        insight.set_limit_price(Some(100.0));
        let insight_id = insight.insight_id;
        state.insights.add_insight(insight);
        let auth = AqsAuth {
            access_method: "aqe_live".into(),
            session_id: "session".into(),
            session_secret: "fixture".into(),
            strategy_id: "strategy".into(),
            user_id: "users:fixture".into(),
            node_id: None,
            live_session_id: None,
            url: Some("ws://127.0.0.1:9".into()),
        };
        let broker = state.broker.execution.clone();
        if fail_witness {
            // Own the same hosted publisher file before run_live creates any
            // worker. A deterministic SQLite fault, not a timing race or a
            // production test switch. Only optional witness INSERTs fail.
            let scope=uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL,
                &serde_json::to_vec(&(&auth.user_id,&auth.strategy_id,"session")).unwrap()).simple().to_string();
            let path=directory.join("cloud").join(scope).join("publisher.db");
            let store=OutboxStore::open(&path).await.unwrap();
            store.latest_capture_pause().await.unwrap();
            let db=turso::Builder::new_local(path.to_string_lossy().as_ref()).build().await.unwrap();
            let writer=db.connect().unwrap();
            writer.execute_batch("CREATE TRIGGER refuse_live_initial_witness BEFORE INSERT ON capture_source_observation BEGIN SELECT RAISE(ABORT,'owned live startup fixture'); END;
                CREATE TRIGGER refuse_live_observation_suspension BEFORE INSERT ON capture_pauses WHEN NEW.source_observation_pause_id IS NOT NULL BEGIN SELECT RAISE(ABORT,'owned late observation fixture'); END;").await.unwrap();
        }
        let account_id=state.broker.execution.get_account().await.unwrap().account_id;
        let journal_path=state.live_execution_journal_path("strategy",&account_id);
        if recovering_stop {
            let mut journal=crate::core::broker::command_journal::BrokerCommandJournal::open_with_state_history(&journal_path,"strategy",&account_id,false).await.unwrap()
                .with_analytics_capture_budget(source_limit.unwrap()).await.unwrap();
            journal.request_execution_stop().await.unwrap();
        }
        if source==Some(true){
            use crate::core::broker::{command_journal::{BrokerCommandJournal,ExecutionFillBatch},commands::BrokerCommandError};
            let mut journal=BrokerCommandJournal::open_with_state_history(&journal_path,"strategy",&account_id,false).await.unwrap()
                .with_analytics_capture_budget(source_limit.unwrap()).await.unwrap();
            journal.begin_execution_epoch().await.unwrap();
            let input=serde_json::from_value(json!({"insight_id":"fixture","fill":{
                "version":1,"provider":"fixture","fill_id":"pressure","order_id":"order","position_id":null,"symbol":"ANY",
                "side":"buy","effect":"entry","quantity":1.0,"price":1.0,"occurred_at_us":100,"as_of_us":200,"cash":null,"account_notional":null
            }})).unwrap();
            assert!(matches!(journal.record_execution_fills(&ExecutionFillBatch::new(vec![input]).unwrap()).await,Err(BrokerCommandError::CapturePaused)));
        }
        let stop = state.shutdown_tx.clone();
        if pre_stopped {
            stop.send_replace(true);
        }
        let telemetry=state.runtime_telemetry.snapshot_handle();
        let mut watcher = (!pre_stopped).then(|| {
            tokio::spawn(async move {
                let admitted = tokio::time::timeout(Duration::from_secs(8), async {
                    loop {
                        let entered=if recovering_stop {
                            telemetry.read().unwrap().events.iter().any(|event|event.message=="Live strategy started after recovery")
                        }else{!broker.get_orders().await.unwrap().is_empty()};
                        if entered {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .is_ok();
                stop.send_replace(true);
                admitted
            })
        });
        let result =
            tokio::time::timeout(Duration::from_secs(15), state.run_live(Some(auth))).await;
        let succeeded = result.as_ref().is_ok_and(|result| result.is_ok());
        if !succeeded {
            if let Some(watcher) = watcher.take() {
                watcher.abort();
                let _ = watcher.await;
            }
            state.shutdown();
            let _ = state.stop_live_execution_worker().await;
            let _ = state.broker.disconnect().await;
        } else if let Some(watcher) = watcher.take() {
            assert!(
                watcher.await.unwrap(),
                "cloud-only pause must not prevent local pending-entry dispatch"
            );
        }
        handle.stop(false).await;
        let _ = server_task.await;
        result
            .expect("owned paused-cloud strategy must not hang")
            .expect("a cloud-only capture pause must not fail live startup or broker settlement");
        assert_eq!(state.status, StrategyStatus::Stopped);
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(!pre_stopped));
        assert_eq!(state.insights.active_insight_count(), 0);
        if pre_stopped {
            assert!(state.broker.execution.get_orders().await.unwrap().is_empty(),"early stop must never submit the entry");
            if let Some(row)=state.insights.get(&insight_id){
                assert_eq!(row.state,InsightState::Cancelled,"an undispatched entry must be cancelled, never fabricated as closed");
            }else{
                assert_eq!(source,Some(false),"a paused cloud must retain its dirty terminal row; a healthy projection may prune it after acceptance");
            }
        }
        assert!(state.command_driver.is_none());
        assert!(!state.broker.is_connected());
        let scope = std::fs::read_dir(directory.join("cloud"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let store = OutboxStore::open(scope.join("publisher.db")).await.unwrap();
        if source==Some(false) && !fail_witness {
            assert!(store.latest_capture_pause().await.unwrap().is_none(),"a clean stopped journal must not invent an observation gap");
            // The final read-only guard and monitor must both release their
            // ownership when run_live returns, even for an early stop.
            let journal=crate::core::broker::command_journal::BrokerCommandJournal::open(&journal_path,"strategy",&account_id).await.unwrap()
                .with_analytics_capture_budget(source_limit.unwrap()).await.unwrap();
            assert!(journal.analytics_capture_pause().await.unwrap().is_none());
            drop(journal);drop(store);drop(state);
            std::fs::remove_dir_all(directory).unwrap();
            return;
        }
        if source.is_none(){assert_eq!(store.capture_retained_bytes().await.unwrap(), 0);}
        assert!(
            store
                .has_unresolved_capture_pause_for_scope("strategy", "session")
                .await
                .unwrap(),
            "the missing cloud history must remain explicit for recovery"
        );
        let database =
            turso::Builder::new_local(scope.join("publisher.db").to_string_lossy().as_ref())
                .build()
                .await
                .unwrap();
        let reader = database.connect().unwrap();
        let mut rows = reader
            .query("SELECT observation FROM safety_observations LIMIT 258", ())
            .await
            .unwrap();
        let mut stopped = false;
        let mut count = 0;
        while let Some(row) = rows.next().await.unwrap() {
            count += 1;
            let fact: aqe_live_protocol::SafetyObservation =
                serde_json::from_str(&row.get::<String>(0).unwrap()).unwrap();
            fact.validate().unwrap();
            stopped |= fact.strategy_id == "strategy"
                && fact.live_session_id == "session"
                && fact.state == "stopped";
        }
        assert!(
            stopped && count <= 257,
            "the exact compact stopped fact must be durable at process exit"
        );
        drop(rows);
        drop(reader);
        drop(database);
        assert!(
            safety.load(Ordering::SeqCst) > 0
                || store
                    .retirement_uncommitted_for_scope("strategy", "session")
                    .await
                    .unwrap()
                    > 0,
            "stop must be delivered or durably retained, never silently discarded"
        );
        drop(store);
        drop(state);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
