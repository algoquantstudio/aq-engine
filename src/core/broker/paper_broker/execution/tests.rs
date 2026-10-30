use super::*;
use crate::core::{
    broker::types::BrokerOrderEvent,
    insight::{Insight, types::StrategyType},
    utils::timeframe::{TimeFrame, TimeFrameUnit},
};
use aqe_live_protocol::ExecutionFillEffect;

fn insight(side: OrderSide) -> Insight {
    let mut insight = Insight::new(
        side,
        "GENERIC".into(),
        StrategyType::Testing,
        TimeFrame::new(1, TimeFrameUnit::Minute),
        80,
        None,
    );
    insight.set_quantity(Some(2.));
    insight
}

fn step(b: &PaperBroker, price: f64, minute: i64) {
    let at = DateTime::from_timestamp(1_700_000_040 + minute * 60, 0).unwrap();
    b.process_step(
        &HashMap::from([(
            "GENERIC".into(),
            Bar {
                symbol: "GENERIC".into(),
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 1000.,
                timestamp: at,
            },
        )]),
        at,
    );
}

#[tokio::test]
async fn real_paper_stream_carries_entry_partial_and_final_execution_deltas() {
    let b = PaperBroker::new(AccountType::Paper, 10000., 1);
    b.connect().await.unwrap();
    let streamed = Arc::new(Mutex::new(Vec::<BrokerOrderEvent>::new()));
    let received = streamed.clone();
    b.subscribe_to_broker_event_stream(Arc::new(move |event| {
        received.lock().push(event);
    }))
    .await
    .unwrap();
    let mut insight = Insight::new(
        OrderSide::Buy,
        "GENERIC".into(),
        StrategyType::Testing,
        TimeFrame::new(1, TimeFrameUnit::Minute),
        80,
        None,
    );
    insight.set_quantity(Some(2.));
    let order = b.submit_order(insight.into()).await.unwrap();
    step(&b, 100., 0);
    assert_eq!(
        streamed
            .lock()
            .iter()
            .filter(|e| e.metadata.execution_fill.is_some())
            .count(),
        1,
        "the actual Paper producer must deliver an exact entry fill"
    );
    b.close_position(&order.order_id, 1., Some(110.))
        .await
        .unwrap();
    step(&b, 110., 1);
    b.close_position(&order.order_id, 0., Some(90.))
        .await
        .unwrap();
    step(&b, 90., 2);
    // Repeated closes and another account step are not new executions.
    let _ = b.close_position(&order.order_id, 0., Some(90.)).await;
    step(&b, 90., 3);
    let events = b.drain_broker_events();
    assert_eq!(
        serde_json::to_value(&events).unwrap(),
        serde_json::to_value(&*streamed.lock()).unwrap(),
        "queue and stream share event identities"
    );
    let fills: Vec<_> = events
        .iter()
        .filter_map(|e| e.metadata.execution_fill.as_deref())
        .collect();
    assert_eq!(fills.len(), 3);
    assert_eq!(
        fills
            .iter()
            .map(|f| (f.effect, f.quantity, f.price))
            .collect::<Vec<_>>(),
        vec![
            (ExecutionFillEffect::Entry, 2., 100.),
            (ExecutionFillEffect::Exit, 1., 110.),
            (ExecutionFillEffect::Exit, 1., 90.),
        ]
    );
    assert!(
        fills
            .iter()
            .all(|f| f.position_id.as_deref() == Some(order.order_id.as_str()))
    );
    assert_eq!(
        fills
            .iter()
            .map(|f| f.fill_id.as_str())
            .collect::<HashSet<_>>()
            .len(),
        3
    );
    for fill in &fills {
        fill.validate().unwrap();
    }
    assert_eq!(
        fills
            .iter()
            .map(|f| f.cash.as_ref().unwrap().net().unwrap())
            .sum::<f64>(),
        0.
    );
    assert_eq!(b.get_account_sync().unwrap().equity, 10000.);
    assert!(!b.has_pending_trade_events());
    b.unsubscribe_from_trade_stream().await.unwrap();
    b.disconnect().await.unwrap();
}

#[tokio::test]
async fn real_paper_history_drives_durable_fills_marks_and_replay_without_state_archive() {
    use crate::core::{
        broker::{
            command_journal::{
                BrokerCommandJournal, ExecutionCheckpointBatch, ExecutionFillBatch,
                marks::PositionMarkBatch,
            },
            fill_history::FillHistoryOwner,
            position_valuation::PositionValuationReader,
        },
        insight::execution_checkpoint::InsightExecutionCheckpoint,
    };
    struct Fixture(std::path::PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let fixture = Fixture(std::env::temp_dir().join(format!("paper-research-{}", Uuid::new_v4())));
    let b = PaperBroker::new(AccountType::Paper, 10000., 1);
    b.connect().await.unwrap();
    let account = b.get_account_sync().unwrap().account_id;
    let mut insight = insight(OrderSide::Buy);
    let order = b.submit_order(insight.clone().into()).await.unwrap();
    insight.order_id = Some(order.order_id.clone());
    let mut journal = BrokerCommandJournal::open_with_state_history(
        fixture.0.join("journal.db"),
        "standalone-paper",
        &account,
        false,
    )
    .await
    .unwrap();
    journal
        .persist_execution_batch(
            &ExecutionCheckpointBatch::new(
                vec![InsightExecutionCheckpoint::capture(&insight, 1, true, None)],
                vec![],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let owner = FillHistoryOwner {
        insight_id: insight.insight_id.to_string(),
        execution_epoch: 1,
        entry_order_id: order.order_id.clone(),
        symbol: "GENERIC".into(),
    };
    step(&b, 100., 0);
    step(&b, 120., 1);
    for minute in 1..=3 {
        if minute > 1 {
            b.close_position(
                &order.order_id,
                if minute == 2 { 1. } else { 0. },
                Some(if minute == 2 { 110. } else { 90. }),
            )
            .await
            .unwrap();
            step(&b, if minute == 2 { 110. } else { 90. }, minute);
        }
        // Release terminal OMS state before the final history read.
        b.drain_broker_events();
        let history = ExecutionFillHistoryReader::read(&b, &account, &order.order_id, "GENERIC")
            .await
            .unwrap()
            .unwrap();
        let check = history.position_check().unwrap();
        assert!(check.matched);
        assert_eq!(check.snapshot.quantity, (3 - minute) as f64);
        journal
            .record_execution_fills(
                &ExecutionFillBatch::from_history(owner.clone(), history).unwrap(),
            )
            .await
            .unwrap();
        let slot = (1_700_000_040 + minute * 60) * 1_000_000;
        let owners = journal
            .position_mark_owners(slot, None)
            .await
            .unwrap()
            .owners;
        assert_eq!(owners.len(), 1, "Paper history must qualify mark ownership");
        let values = b
            .read_positions(&account, "GENERIC", &[order.order_id.clone()])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            journal
                .record_position_marks(PositionMarkBatch::new(owners, values, slot).unwrap())
                .await
                .unwrap(),
            1
        );
    }
    let page = journal.execution_fill_page(0, None, 128).await.unwrap();
    assert_eq!(page.records.len(), 3);
    assert_eq!(
        page.records
            .iter()
            .map(|r| r.observation.fill.cash.as_ref().unwrap().net().unwrap())
            .sum::<f64>(),
        0.
    );
    assert_eq!(
        journal.position_mark_page(0).await.unwrap().records.len(),
        3
    );
    let head = page.through_sequence;
    step(&b, 90., 4);
    let history = ExecutionFillHistoryReader::read(&b, &account, &order.order_id, "GENERIC")
        .await
        .unwrap()
        .unwrap();
    journal
        .record_execution_fills(&ExecutionFillBatch::from_history(owner, history).unwrap())
        .await
        .unwrap();
    assert_eq!(
        journal
            .execution_fill_page(0, None, 128)
            .await
            .unwrap()
            .through_sequence,
        head,
        "fresh timestamps must not append unchanged facts"
    );
    assert!(
        journal
            .prepare_execution_evidence_export()
            .await
            .unwrap()
            .is_none()
    );
    let frozen = journal
        .prepare_execution_fill_export()
        .await
        .unwrap()
        .unwrap();
    drop(journal);
    let journal = BrokerCommandJournal::open_with_state_history(
        fixture.0.join("journal.db"),
        "standalone-paper",
        &account,
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        journal
            .prepare_execution_fill_export()
            .await
            .unwrap()
            .unwrap(),
        frozen
    );
    assert_eq!(
        journal.position_mark_page(0).await.unwrap().records.len(),
        3
    );
    assert!(
        ExecutionFillHistoryReader::read(&b, "foreign-account", &order.order_id, "GENERIC")
            .await
            .is_err()
    );
    assert!(
        ExecutionFillHistoryReader::read(&b, &account, &order.order_id, "OTHER")
            .await
            .is_err()
    );
    assert!(
        ExecutionFillHistoryReader::read(&b, &account, "missing", "GENERIC")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn paper_short_cash_matches_actual_settlement_without_recounting_entry_fees() {
    use crate::core::broker::types::{AssetCommissionFees, AssetFee, AssetSideFees, AssetSwapFees};
    let fees = AssetFees {
        commission: AssetCommissionFees {
            entry: AssetSideFees {
                long: AssetFee::FixedFee(2.),
                short: AssetFee::FixedFee(2.),
            },
            exit: AssetSideFees {
                long: AssetFee::FixedFee(3.),
                short: AssetFee::FixedFee(3.),
            },
        },
        swap: AssetSwapFees {
            long: AssetFee::FixedFee(-1.),
            short: AssetFee::FixedFee(-1.),
        },
        ..Default::default()
    };
    let b = PaperBroker::new(AccountType::Paper, 10000., 1).with_asset_fees(fees);
    b.connect().await.unwrap();
    let order = b
        .submit_order(insight(OrderSide::Sell).into())
        .await
        .unwrap();
    step(&b, 100., 0);
    b.close_position(&order.order_id, 1., Some(90.))
        .await
        .unwrap();
    step(&b, 90., 1);
    b.close_position(&order.order_id, 0., Some(110.))
        .await
        .unwrap();
    step(&b, 110., 2);
    let fills: Vec<_> = b
        .drain_broker_events()
        .into_iter()
        .filter_map(|e| e.metadata.execution_fill)
        .collect();
    assert_eq!(fills.len(), 3);
    assert_eq!(fills[0].side, ExecutionFillSide::Sell);
    assert!(fills[1..].iter().all(|f| f.side == ExecutionFillSide::Buy));
    assert_eq!(
        fills
            .iter()
            .map(|f| f.cash.as_ref().unwrap().commission.unwrap())
            .sum::<f64>(),
        8.
    );
    let net: f64 = fills
        .iter()
        .map(|f| f.cash.as_ref().unwrap().net().unwrap())
        .sum();
    assert!((b.get_account_sync().unwrap().equity - 10000. - net).abs() < 1e-9);
}

#[tokio::test]
async fn optional_paper_history_is_bounded_and_never_claims_a_truncated_prefix() {
    let b = PaperBroker::new(AccountType::Paper, 10000., 1);
    let mut order = b
        .submit_order(insight(OrderSide::Buy).into())
        .await
        .unwrap();
    order.order_id = "position".into();
    order.asset.symbol = "GENERIC".into();
    order.filled_qty = 1.;
    order.commission = Some(0.);
    let mut fill = b.entry_execution(
        &order,
        100.,
        DateTime::from_timestamp(1_700_000_040, 0).unwrap(),
    );
    let mut history = PaperExecutionHistory::default();
    for i in 0..MAX_RETAINED_POSITIONS + 10 {
        fill.order_id = i.to_string();
        fill.position_id = Some(i.to_string());
        history.record(&fill, false);
    }
    assert_eq!(history.positions.len(), MAX_RETAINED_POSITIONS);
    assert_eq!(history.rows, MAX_RETAINED_POSITIONS);
    assert!(
        !history
            .positions
            .contains_key(&MAX_RETAINED_POSITIONS.to_string())
    );
    let mut history = PaperExecutionHistory::default();
    fill.position_id = Some("oversized".into());
    history.record(&fill, false);
    fill.effect = ExecutionFillEffect::Exit;
    for _ in 0..MAX_HISTORY_FILLS {
        history.record(&fill, false);
    }
    assert!(history.positions.is_empty());
    assert_eq!(history.rows, 0);
    // An evicted/unqualified history cannot be rebuilt from exit-only data.
    history.record(&fill, true);
    assert!(history.positions.is_empty());
}
