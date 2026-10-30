mod lifecycle_reconciliation_regressions {
    use super::*;
    use crate::core::broker::types::{
        OrderLifecycleCloseDetails, OrderLifecycleSnapshot, OrderLifecycleStatus,
    };

    struct CancelHistory(Arc<Mutex<Vec<String>>>);
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn stuck_cancel_startup_recovers_same_journal_then_opens_exactly_one_new_epoch() {
        use crate::core::{broker::{command_journal::{BrokerCommandJournal,ExecutionCheckpointBatch,ExecutionIntent,lifecycle::ExecutionPhase},
            commands::{BrokerCommand,BrokerCommandAction,BrokerCommandCompletion,BrokerCommandOutcome}},
            insight::{execution_checkpoint::InsightExecutionCheckpoint,operations::{PendingInsightMutation,MutationKind,MutationEffect}}};
        let directory=std::env::temp_dir().join(format!("aqe-stuck-cancel-startup-{}",uuid::Uuid::new_v4()));
        let mut broker=TestExecutionBroker::with_features(&[]);
        broker.recover_pending_cancel=true;
        let teardown=Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut state=StrategyState::new("CancelRecovery".into(),"1".into(),
            LiveCommandLifecycleFixture {teardown_calls:teardown.clone(),cleanup:true},
            UnifiedBroker::new(broker.clone(),FixedScaleOutDataFeed::new()),StrategyMode::Live,
            TimeFrame::new(1,TimeFrameUnit::Minute)).with_artifact_root(&directory);
        let key=state.live_metrics_strategy_key(None);
        let path=state.live_execution_journal_path(&key,"test");
        let mut saved=row("2025202590",false);
        saved.cancelling=true;
        broker.acknowledge_cancel(test_request_order("2025202590",&(&saved).into(),
            TradeUpdateEvent::Cancelled,saved.side.clone(),None));
        let command=BrokerCommand::new(&key,"test",&saved.insight_id.to_string(),
            BrokerCommandAction::Cancel {order_id:"2025202590".into()},None);
        saved.pending_mutations.push(PendingInsightMutation {kind:MutationKind::Cancel,receipt:command.receipt(),effect:MutationEffect::Cancel});
        let mut journal=BrokerCommandJournal::open(&path,&key,"test").await.unwrap();
        journal.persist_execution_batch(&ExecutionCheckpointBatch::with_intents(
            vec![InsightExecutionCheckpoint::capture(&saved,1,true,None)],vec![],
            vec![ExecutionIntent {command:command.clone(),admission_order:1,queued_at_us:1}]).unwrap()).await.unwrap();
        journal.mark_dispatched(&command.id,2).await.unwrap();
        journal.complete(&BrokerCommandCompletion {receipt:command.receipt(),queued_at_us:1,dispatched_at_us:Some(2),
            completed_at_us:3,outcome:BrokerCommandOutcome::Ambiguous {reason:"old MT5 CancelOrder timeout".into()}}).await.unwrap();
        journal.request_execution_stop().await.unwrap();
        drop(journal);
        let shutdown=state.shutdown_tx.clone();
        let stop=async {
            while broker.cancel_requests.lock().unwrap().is_empty(){tokio::task::yield_now().await;}
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            assert_eq!(teardown.load(std::sync::atomic::Ordering::SeqCst),0,"recovery must not invoke user teardown");
            shutdown.send_replace(true);
        };
        tokio::time::timeout(std::time::Duration::from_secs(15),async {
            let(result,())=tokio::join!(state.run_live(None),stop);
            result.unwrap();
        }).await.expect("confirmed pending cancellation must not strand startup");
        assert_eq!(broker.cancel_requests.lock().unwrap().as_slice(),&["2025202590"]);
        assert!(broker.submit_requests.lock().unwrap().is_empty(),"never replay entries while recovering");
        assert!(broker.close_requests.lock().unwrap().is_empty(),"never turn a cancellation into a position close");
        let journal=BrokerCommandJournal::open(&path,&key,"test").await.unwrap();
        assert_eq!(journal.execution_lifecycle().epoch,2);
        assert_eq!(journal.execution_lifecycle().phase,ExecutionPhase::Stopped);
        assert!(journal.recovery_page(0,256).await.unwrap().is_empty());
        assert_eq!(journal.get(&command.id).await.unwrap().unwrap().state,
            crate::core::broker::command_journal::JournalCommandState::Applied);
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn normal_teardown_without_broker_confirmation_has_unconditional_deadline() {
        use crate::core::broker::command_journal::{BrokerCommandJournal, lifecycle::ExecutionPhase};
        let directory = std::env::temp_dir().join(format!("aqe-stop-deadline-{}", uuid::Uuid::new_v4()));
        // Close returns accepted, but deliberately never emits a Closed event.
        // This is not an execution error and previously waited indefinitely.
        let broker = TestExecutionBroker::with_features(&[]);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut state = StrategyState::new("UnconfirmedStop".into(), "1".into(),
            LiveCommandLifecycleFixture { teardown_calls: calls.clone(), cleanup: true },
            UnifiedBroker::new(broker.clone(), FixedScaleOutDataFeed::new()),
            StrategyMode::Live, TimeFrame::new(1, TimeFrameUnit::Minute),
        ).with_artifact_root(&directory);
        state.insights.add_insight(row("unconfirmed-close", true));
        let shutdown = state.shutdown_tx.clone();
        let stop = async {
            while !*broker.connected.lock().unwrap() { tokio::task::yield_now().await; }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            shutdown.send_replace(true);
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let (result, ()) = tokio::join!(state.run_live_with_teardown_timeout(
                None, std::time::Duration::from_millis(350)), stop);
            assert!(result.unwrap_err().to_string().contains("settlement deadline"));
        }).await.expect("ordinary teardown must not wait forever for an absent callback");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(state.status, crate::core::strategy::StrategyStatus::Stopping);
        assert_eq!(state.insights.active_insight_count(), 1);
        assert!(!*broker.connected.lock().unwrap());
        let account = broker.get_account().await.unwrap();
        let key = state.live_metrics_strategy_key(None);
        let journal = BrokerCommandJournal::open(
            state.live_execution_journal_path(&key, &account.account_id), &key, &account.account_id,
        ).await.unwrap();
        assert_eq!(journal.execution_lifecycle().phase, ExecutionPhase::Stopping);
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unfinished_stop_recovers_then_runs_without_extra_teardown_callback() {
        use crate::core::broker::command_journal::{BrokerCommandJournal, lifecycle::ExecutionPhase};
        let directory = std::env::temp_dir().join(format!("aqe-stop-continue-{}", uuid::Uuid::new_v4()));
        let broker = TestExecutionBroker::with_features(&[]);
        let teardown_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut state = StrategyState::new("ContinueRecovery".into(), "1".into(),
            LiveCommandLifecycleFixture { teardown_calls: teardown_calls.clone(), cleanup: true },
            UnifiedBroker::new(broker.clone(), FixedScaleOutDataFeed::new()),
            StrategyMode::Live, TimeFrame::new(1, TimeFrameUnit::Minute),
        ).with_artifact_root(&directory);
        let account = broker.get_account().await.unwrap();
        let key = state.live_metrics_strategy_key(None);
        let path = state.live_execution_journal_path(&key, &account.account_id);
        let mut journal = BrokerCommandJournal::open(&path, &key, &account.account_id).await.unwrap();
        journal.request_execution_stop().await.unwrap();
        drop(journal);
        let shutdown = state.shutdown_tx.clone();
        let calls = teardown_calls.clone();
        let stop = async {
            while !*broker.connected.lock().unwrap() { tokio::task::yield_now().await; }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0, "recovery invoked user teardown");
            assert!(!*shutdown.borrow(), "old stop leaked into new launch control");
            shutdown.send_replace(true);
        };
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let (result, ()) = tokio::join!(state.run_live(None), stop);
            result.unwrap();
        }).await.unwrap();
        assert_eq!(teardown_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let journal = BrokerCommandJournal::open(&path, &key, &account.account_id).await.unwrap();
        assert_eq!(journal.execution_lifecycle().epoch, 2);
        assert_eq!(journal.execution_lifecycle().phase, ExecutionPhase::Stopped);
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }

    struct CloseHistory(Arc<Mutex<Vec<String>>>);
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stop_during_unresolved_recovery_skips_user_teardown_and_retains_journal() {
        use crate::core::broker::command_journal::{BrokerCommandJournal, ExecutionCheckpointBatch, lifecycle::ExecutionPhase};
        use crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint;
        let directory = std::env::temp_dir().join(format!("aqe-recovery-abort-{}", uuid::Uuid::new_v4()));
        let mut broker = TestExecutionBroker::with_features(&[]);
        broker.fail_close = true;
        let teardown_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut state = StrategyState::new("AbortRecovery".into(), "1".into(),
            LiveCommandLifecycleFixture { teardown_calls: teardown_calls.clone(), cleanup: true },
            UnifiedBroker::new(broker.clone(), FixedScaleOutDataFeed::new()), StrategyMode::Live,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        ).with_artifact_root(&directory);
        let account = broker.get_account().await.unwrap();
        let key = state.live_metrics_strategy_key(None);
        let path = state.live_execution_journal_path(&key, &account.account_id);
        let mut journal = BrokerCommandJournal::open(&path, &key, &account.account_id).await.unwrap();
        journal.persist_execution_batch(&ExecutionCheckpointBatch::new(
            vec![InsightExecutionCheckpoint::capture(&row("unresolved-order", true), 1, true, None)], vec![],
        ).unwrap()).await.unwrap();
        journal.request_execution_stop().await.unwrap();
        drop(journal);
        let shutdown = state.shutdown_tx.clone();
        let stop = async {
            while !*broker.connected.lock().unwrap() { tokio::task::yield_now().await; }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            shutdown.send_replace(true);
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (result, ()) = tokio::join!(state.run_live(None), stop);
            assert!(result.is_err(), "unresolved recovery must not report successful settlement");
        }).await.expect("stop must interrupt recovery promptly");
        assert_eq!(teardown_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        let journal = BrokerCommandJournal::open(&path, &key, &account.account_id).await.unwrap();
        assert_eq!(journal.execution_lifecycle().phase, ExecutionPhase::Stopping);
        assert_eq!(journal.execution_lifecycle().epoch, 1);
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[async_trait::async_trait]
    impl crate::core::broker::traits::OrderLifecycleReconciler for CloseHistory {
        async fn reconcile(&self, ids: &[String]) -> Result<Vec<OrderLifecycleSnapshot>, BrokerError> {
            let requests = self.0.lock().unwrap();
            Ok(ids.iter().map(|id| {
                let mut result = evidence(id, OrderLifecycleStatus::Unknown);
                if requests.contains(id) {
                    result.status = OrderLifecycleStatus::Closed;
                    result.close_details = Some(OrderLifecycleCloseDetails {
                        close_price: 101.0, realized_pnl: 2.0, commission: 0.0, swap: 0.0,
                        position_accounting: None,
                        closed_at: Utc::now(),
                    });
                }
                result
            }).collect())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ambiguous_missing_ticket_close_settles_from_history_and_persists() {
        let directory = std::env::temp_dir().join(format!("aqe-ambiguous-close-{}", uuid::Uuid::new_v4()));
        let mut broker = TestExecutionBroker::with_features(&[]);
        broker.fail_close = true;
        broker.lifecycle_reader = Some(Arc::new(CloseHistory(broker.close_requests.clone())));
        let mut state = StrategyState::new(
            "AmbiguousClose".into(), "1".into(),
            LiveCommandLifecycleFixture { teardown_calls: Default::default(), cleanup: true },
            UnifiedBroker::new(broker.clone(), FixedScaleOutDataFeed::new()),
            StrategyMode::Live, TimeFrame::new(1, TimeFrameUnit::Minute),
        ).with_artifact_root(&directory);
        state.insights.add_insight(row("already-closed", true));
        let shutdown = state.shutdown_tx.clone();
        let stop = async {
            while !*broker.connected.lock().unwrap() { tokio::task::yield_now().await; }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            shutdown.send_replace(true);
        };
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let (result, ()) = tokio::join!(state.run_live(None), stop);
            result.unwrap();
        }).await.expect("ambiguous close must wait for history instead of abandoning teardown");
        assert_eq!(state.status, crate::core::strategy::StrategyStatus::Stopped);
        assert_eq!(state.insights.active_insight_count(), 0);
        assert_eq!(broker.close_requests.lock().unwrap().as_slice(), &["already-closed"]);
        assert!(!*broker.connected.lock().unwrap());
        let account = broker.get_account().await.unwrap();
        let strategy = state.live_metrics_strategy_key(None);
        let journal = crate::core::broker::command_journal::BrokerCommandJournal::open(
            state.live_execution_journal_path(&strategy, &account.account_id), &strategy, &account.account_id,
        ).await.unwrap();
        assert_eq!(journal.execution_lifecycle().phase,
            crate::core::broker::command_journal::lifecycle::ExecutionPhase::Stopped);
        assert!(journal.recovery_page(0, 256).await.unwrap().is_empty());
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[async_trait::async_trait]
    impl crate::core::broker::traits::OrderLifecycleReconciler for CancelHistory {
        async fn reconcile(
            &self,
            ids: &[String],
        ) -> Result<Vec<OrderLifecycleSnapshot>, BrokerError> {
            let cancelled = self.0.lock().unwrap();
            Ok(ids
                .iter()
                .map(|id| {
                    evidence(
                        id,
                        if cancelled.contains(id) {
                            OrderLifecycleStatus::Cancelled
                        } else {
                            OrderLifecycleStatus::Unknown
                        },
                    )
                })
                .collect())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn teardown_uses_terminal_history_when_cancel_callback_is_missing() {
        let directory =
            std::env::temp_dir().join(format!("aqe-history-stop-{}", uuid::Uuid::new_v4()));
        let mut broker = TestExecutionBroker::with_features(&[]);
        broker.lifecycle_reader = Some(Arc::new(CancelHistory(broker.cancel_requests.clone())));
        let teardown_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut state = StrategyState::new(
            "HistoryStop".into(),
            "1".into(),
            LiveCommandLifecycleFixture {
                teardown_calls: teardown_calls.clone(),
                cleanup: true,
            },
            UnifiedBroker::new(broker.clone(), FixedScaleOutDataFeed::new()),
            StrategyMode::Live,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        )
        .with_artifact_root(&directory);
        let row = row("manually-cancelled", false);
        state.insights.add_insight(row);
        let shutdown = state.shutdown_tx.clone();
        let stop = async {
            while !*broker.connected.lock().unwrap() {
                tokio::task::yield_now().await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            shutdown.send_replace(true);
        };
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let (result, ()) = tokio::join!(state.run_live(None), stop);
            result.unwrap();
        })
        .await
        .expect("history evidence must unblock teardown");
        assert_eq!(state.status, crate::core::strategy::StrategyStatus::Stopped);
        assert_eq!(state.insights.active_insight_count(), 0);
        assert_eq!(teardown_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(!*broker.connected.lock().unwrap());
        assert!(broker.close_requests.lock().unwrap().is_empty());
        std::fs::remove_dir_all(directory).unwrap();
    }

    fn fixture() -> StrategyState<NoopStrategy, TestExecutionBroker, FixedScaleOutDataFeed> {
        let mut state = StrategyState::new(
            "Reconcile".into(),
            "1".into(),
            NoopStrategy,
            UnifiedBroker::new(
                TestExecutionBroker::with_features(&[]),
                FixedScaleOutDataFeed::new(),
            ),
            StrategyMode::Live,
            TimeFrame::new(1, TimeFrameUnit::Minute),
        );
        state.universe.insert("AAPL".into(), test_asset("AAPL"));
        state
    }

    fn row(order: &str, filled: bool) -> Insight {
        let mut row = Insight::default();
        row.symbol = "AAPL".into();
        row.set_quantity(Some(2.0));
        row.order_accepted(order);
        if filled {
            row.position_filled(100.0, 2.0, order, None);
        }
        row
    }

    fn evidence(order: &str, status: OrderLifecycleStatus) -> OrderLifecycleSnapshot {
        OrderLifecycleSnapshot {
            order_id: order.into(),
            status,
            position_id: None,
            position_qty: None,
            symbol: Some("AAPL".into()),
            detail: None,
            close_details: None,
        }
    }

    #[test]
    fn history_closes_missed_fill_without_activating_children_or_inventing_entry() {
        for initial in [InsightState::New, InsightState::Executed] {
            let mut state = fixture();
            let mut parent = row("missed-fill", false);
            parent.state = initial.clone();
            parent.cancelling = true;
            let id = parent.insight_id;
            let mut child = Insight::default();
            child.symbol = "AAPL".into();
            child.parent_id = Some(id);
            parent.children.push(child);
            state.insights.add_insight(parent);
            let mut snapshot = evidence("missed-fill", OrderLifecycleStatus::Closed);
            snapshot.close_details = Some(OrderLifecycleCloseDetails {
                close_price: 101.0, closed_at: Utc::now(),
                realized_pnl: 1.25, commission: 0.5, swap: -0.25,
                position_accounting: None,
            });
            // The state alone, without final execution values, is insufficient.
            state.apply_lifecycle_snapshot(evidence("missed-fill", OrderLifecycleStatus::Closed));
            assert_eq!(state.insights.get(&id).unwrap().state, initial);
            let mut wrong = snapshot.clone();
            wrong.symbol = Some("BTCUSD".into());
            state.apply_lifecycle_snapshot(wrong);
            assert_eq!(state.insights.get(&id).unwrap().state, initial);
            state.apply_lifecycle_snapshot(snapshot.clone());
            let parent = state.insights.get(&id).unwrap();
            assert_eq!(parent.state, InsightState::Closed);
            assert_eq!(parent.broker_realized_pnl, Some(1.25));
            assert_eq!(parent.closed_at, Some(snapshot.close_details.as_ref().unwrap().closed_at));
            assert_eq!(parent.filled_price, None);
            assert_eq!(parent.filled_at, None);
            assert!(parent.suppress_child_entries);
            assert!(!parent.cancelling);
            assert!(parent.children.iter().all(|child| !child.has_pending_submit()));
            let history = parent.state_history.len();
            state.apply_lifecycle_snapshot(snapshot);
            assert_eq!(state.insights.get(&id).unwrap().state_history.len(), history);
        }
    }

    #[test]
    fn manual_cancel_is_applied_from_history_without_active_order_and_is_idempotent() {
        let mut state = fixture();
        let mut row = row("gone", false);
        row.cancelling = true;
        let id = row.insight_id;
        state.insights.add_insight(row);
        state.apply_lifecycle_snapshot(evidence("gone", OrderLifecycleStatus::Cancelled));
        let row = state.insights.get(&id).unwrap();
        assert_eq!(row.state, InsightState::Cancelled);
        assert!(!row.cancelling);
        let history = row.state_history.len();
        state.apply_lifecycle_snapshot(evidence("gone", OrderLifecycleStatus::Cancelled));
        assert_eq!(
            state.insights.get(&id).unwrap().state_history.len(),
            history
        );
    }

    #[test]
    fn missing_mismatched_or_incomplete_history_never_closes_exposure() {
        let mut state = fixture();
        let row = row("open", true);
        let id = row.insight_id;
        state.insights.add_insight(row);
        for status in [
            OrderLifecycleStatus::Unknown,
            OrderLifecycleStatus::Cancelled,
            OrderLifecycleStatus::Closed,
        ] {
            state.apply_lifecycle_snapshot(evidence("open", status));
            assert_eq!(state.insights.get(&id).unwrap().state, InsightState::Filled);
        }
        let mut wrong = evidence("open", OrderLifecycleStatus::Cancelled);
        wrong.symbol = Some("BTCUSD".into());
        state.apply_lifecycle_snapshot(wrong);
        assert_eq!(state.insights.get(&id).unwrap().state, InsightState::Filled);
    }

    #[test]
    fn manual_close_retains_broker_time_and_pnl_without_duplicate_history() {
        let mut state = fixture();
        let mut row = row("closed", true);
        row.filled_at = Some(Utc::now() - chrono::Duration::hours(2));
        let id = row.insight_id;
        state.insights.add_insight(row);
        let closed_at = Utc::now() - chrono::Duration::hours(1);
        let mut snapshot = evidence("closed", OrderLifecycleStatus::Closed);
        // Some brokers identify a historical position by its exact ticket but
        // do not repeat the symbol. An explicitly conflicting symbol is rejected.
        snapshot.symbol = None;
        snapshot.close_details = Some(OrderLifecycleCloseDetails {
            close_price: 101.0,
            closed_at,
            realized_pnl: 1.25,
            position_accounting: None,
            commission: 0.5,
            swap: -0.25,
        });
        state.apply_lifecycle_snapshot(snapshot.clone());
        let row = state.insights.get(&id).unwrap();
        assert_eq!(row.state, InsightState::Closed);
        assert_eq!(row.closed_at, Some(closed_at));
        assert_eq!(row.broker_realized_pnl, Some(1.25));
        let history = row.state_history.len();
        state.apply_lifecycle_snapshot(snapshot);
        assert_eq!(
            state.insights.get(&id).unwrap().state_history.len(),
            history
        );
    }
}
