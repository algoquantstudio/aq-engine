mod stop_burst {
    use super::*;
    use crate::core::broker::command_journal::{BrokerCommandJournal, lifecycle::ExecutionPhase};
    use crate::core::insight::InsightMutationState;
    use crate::core::strategy::StrategyStatus;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    struct FixtureDirectory(std::path::PathBuf);
    impl Drop for FixtureDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Default)]
    struct Progress {
        queued: AtomicUsize,
        all_queued: tokio::sync::Notify,
        teardown_calls: AtomicUsize,
        teardown_started: tokio::sync::Notify,
        finished: AtomicBool,
    }

    struct BurstSubmit(Arc<Progress>);
    impl InsightPipe for BurstSubmit {
        fn name(&self) -> &str {
            "BurstSubmit"
        }
        fn version(&self) -> &str {
            "1"
        }
        fn run(
            &mut self,
            ctx: &mut dyn StrategyContext,
            insight: &mut Insight,
        ) -> InsightPipeResult {
            // Counting a receipt does not imply dispatch or broker acceptance.
            let pending = insight.has_pending_submit();
            let result = insight.submit(ctx);
            if !pending
                && matches!(&result, Ok(InsightMutationState::Pending(_)))
                && self.0.queued.fetch_add(1, Ordering::SeqCst) + 1 == 1000
            {
                self.0.all_queued.notify_one();
            }
            InsightPipeResult::from_mutation(result, self.name())
        }
    }

    struct BurstStrategy(Arc<Progress>);
    impl Strategy for BurstStrategy {
        fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
            ctx.add_pipe(
                WrappedInsightPipe::builder(Box::new(BurstSubmit(self.0.clone()))).build(),
            );
        }
        fn init(&mut self, _: &mut dyn StrategyContext, _: &Asset) {}
        fn universe(&self, _: &mut dyn StrategyContext) -> HashSet<String> {
            HashSet::from(["AAPL".into()])
        }
        fn on_bar(&mut self, _: &mut dyn StrategyContext, _: &str, _: &BarData) {}
        fn generate_insights(&mut self, _: &mut dyn StrategyContext, _: &str) {}
        fn insight_pipeline(&mut self, _: &mut dyn StrategyContext, _: &Insight) {}
        fn on_teardown(&mut self, ctx: &mut dyn StrategyContext) {
            self.0.teardown_calls.fetch_add(1, Ordering::SeqCst);
            self.0.teardown_started.notify_one();
            ctx.cleanup_active_insights_for_teardown();
        }
    }

    fn limit(price: f64) -> Insight {
        let mut insight = Insight::default();
        insight.symbol = "AAPL".into();
        insight.side = OrderSide::Buy;
        insight.set_quantity(Some(1.0));
        insight.order_type = OrderType::Limit;
        insight.set_limit_price(Some(price));
        insight
    }

    /// The actual run_live/stop-watch/teardown path, using only a fake broker,
    /// static feed and an owned local journal. No external sockets or orders.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_thousand_entry_stop_settles_late_fill_before_finishing() {
        run_stop_burst(true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_thousand_entry_stop_closes_fill_after_cancel_was_requested() {
        run_stop_burst(false).await;
    }

    async fn run_stop_burst(fill_before_response: bool) {
        let directory = FixtureDirectory(
            std::env::temp_dir().join(format!("aqe-stop-burst-{}", uuid::Uuid::new_v4())),
        );
        let progress = Arc::new(Progress::default());
        let gate = Arc::new(TestSubmitGate::default());
        let mut execution = TestExecutionBroker::with_features(&[]);
        execution.submit_gate = Some(gate.clone());
        let mut state = StrategyState::new(
            "StopBurst".into(),
            "1".into(),
            BurstStrategy(progress.clone()),
            UnifiedBroker::new(execution.clone(), FixedScaleOutDataFeed::new()),
            StrategyMode::Live,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        )
        .with_artifact_root(&directory.0);
        let strategy = state.strategy_id.to_string();
        let journal_path = state.live_execution_journal_path(&strategy, "test");
        let mut dormant = HashSet::new();
        let mut roots = HashSet::new();
        for _ in 0..1000 {
            let mut parent = limit(100.0);
            roots.insert(parent.insight_id);
            let child = limit(90.0);
            dormant.insert(child.insight_id);
            parent.add_child_insight(child, None, &mut state);
            state.insights.add_insight(parent);
        }

        let stop = state.shutdown_tx.clone();
        let probe = execution.clone();
        let observer = progress.clone();
        let watcher = async move {
            gate.entered.notified().await;
            observer.all_queued.notified().await;
            assert_eq!(observer.queued.load(Ordering::SeqCst), 1000);
            assert_eq!(probe.submit_requests.lock().unwrap().len(), 1);
            let sent = Instant::now();
            stop.send_replace(true);
            observer.teardown_started.notified().await;
            let stop_latency = sent.elapsed();
            assert!(
                stop_latency < Duration::from_millis(100),
                "stop-watch to teardown hook took {stop_latency:?}"
            );
            let request = probe.submit_requests.lock().unwrap()[0].clone();
            let order_id = format!("submitted-{}", request.insight_id);
            // Broker fill races the outstanding submit acknowledgement AFTER stop.
            let fill = test_request_order(
                &order_id,
                &request,
                TradeUpdateEvent::Filled,
                OrderSide::Buy,
                Some(100.0),
            );
            if fill_before_response {
                probe.emit(fill.clone(), TradeUpdateEvent::Filled);
                gate.release.notify_one();
            } else {
                gate.release.notify_one();
                // Accepted submit -> teardown cancel -> fill while cancellation
                // awaits terminal evidence. The fill must still be closed.
                loop {
                    if !probe.cancel_requests.lock().unwrap().is_empty() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                probe.emit(fill.clone(), TradeUpdateEvent::Filled);
            }
            loop {
                if !probe.close_requests.lock().unwrap().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            // A successful close RPC alone is not terminal trade evidence.
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(!observer.finished.load(Ordering::SeqCst));
            assert!(*probe.connected.lock().unwrap());
            assert_eq!(probe.close_quantities.lock().unwrap().as_slice(), [1.0]);
            assert_eq!(probe.submit_requests.lock().unwrap().len(), 1);
            probe.emit(fill, TradeUpdateEvent::Filled); // duplicate must not resubmit cleanup
            let close = test_request_order(
                &order_id,
                &request,
                TradeUpdateEvent::Closed,
                OrderSide::Sell,
                Some(101.0),
            );
            probe.emit(close.clone(), TradeUpdateEvent::Closed);
            probe.emit(close, TradeUpdateEvent::Closed);
            stop_latency
        };
        let run_progress = progress.clone();
        let run = async {
            let result = state.run_live(None).await;
            run_progress.finished.store(true, Ordering::SeqCst);
            result
        };
        let outcome = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(run, watcher)
        })
        .await;
        if outcome.is_err() {
            eprintln!("burst timeout: queued={} teardown={} finished={} submits={} cancels={} closes={} active={} pending={} failure={:?}",
                progress.queued.load(Ordering::SeqCst), progress.teardown_calls.load(Ordering::SeqCst),
                progress.finished.load(Ordering::SeqCst), execution.submit_requests.lock().unwrap().len(),
                execution.cancel_requests.lock().unwrap().len(), execution.close_requests.lock().unwrap().len(),
                state.insights.active_insight_count(), state.command_driver.as_ref().map_or(0, |driver| driver.pending_count()),
                state.live_execution_failure());
            if let Some(request) = execution.submit_requests.lock().unwrap().first() {
                if let Ok(id) = uuid::Uuid::parse_str(&request.insight_id) {
                    if let Some(row) = state.insights.get(&id) {
                        eprintln!("burst dispatched row: state={:?} closing={} cancelling={} pending={:?}",
                            row.state, row.closing, row.cancelling, row.pending_mutations);
                    }
                }
            }
            state.shutdown();
            execution.submit_gate.as_ref().unwrap().release.notify_one();
            let _ = state.stop_live_execution_worker().await;
            let _ = state.broker.disconnect().await;
        }
        let (result, latency) = outcome.expect("1,000-entry teardown fixture must settle");
        result.unwrap();
        assert_eq!(progress.teardown_calls.load(Ordering::SeqCst), 1);
        assert_eq!(state.status, StrategyStatus::Stopped);
        assert_eq!(state.insights.active_insight_count(), 0);
        let mut closed = 0;
        for id in roots {
            let insight = state
                .insights
                .get(&id)
                .expect("root must remain inspectable");
            match &insight.state {
                InsightState::Closed => closed += 1,
                InsightState::Cancelled => {}
                state => panic!("queued root ended in unexpected state {state:?}"),
            }
        }
        assert_eq!(closed, 1, "only the broker-confirmed fill may be closed");
        assert_eq!(execution.submit_requests.lock().unwrap().len(), 1);
        assert_eq!(execution.close_requests.lock().unwrap().len(), 1);
        assert!(dormant.iter().all(|id| {
            state
                .insights
                .get(id)
                .is_none_or(|child| child.order_id.is_none() && child.state != InsightState::Filled)
        }));
        assert!(state.command_driver.is_none());
        let journal = BrokerCommandJournal::open(&journal_path, &strategy, "test")
            .await
            .unwrap();
        assert_eq!(journal.execution_lifecycle().phase, ExecutionPhase::Stopped);
        assert!(journal.recovery_page(0, 256).await.unwrap().is_empty());
        eprintln!(
            "1,000 entry intents (fill-before-response={fill_before_response}): stop-watch -> teardown hook {latency:?}; one late fill closed; no post-stop submits"
        );
    }
}
