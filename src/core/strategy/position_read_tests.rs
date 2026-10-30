//! Paper execution is entirely in memory. The optional MT5 test only constructs
//! a context, never runs a strategy, and uses an RPC allowlist before queueing.
use super::*;
use crate::core::broker::{
    data_feeds::yahoo::YahooFinanceDataFeed,
    mt5_bridge::{Mt5Bridge, Mt5BridgeConfig},
    mt5_broker::Mt5Broker,
    snapshots::{SnapshotFreshness, SnapshotKind, SnapshotRefreshConfig, SnapshotUnavailable},
    types::{AccountType, Bar, OrderSide, Position},
};
use crate::core::insight::types::StrategyType;
use crate::core::utils::timeframe::TimeFrameUnit;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

struct ReadOnlyContext;
impl Strategy for ReadOnlyContext {
    fn on_start(&mut self, _: &mut dyn StrategyContext) {
        panic!("position probe must never start a strategy")
    }
    fn init(&mut self, _: &mut dyn StrategyContext, _: &Asset) {
        panic!("position probe must never initialize a universe")
    }
    fn universe(&self, _: &mut dyn StrategyContext) -> HashSet<String> {
        panic!("position probe must never load a universe")
    }
    fn on_bar(&mut self, _: &mut dyn StrategyContext, _: &str, _: &BarData) {
        panic!("position probe must never process bars")
    }
    fn generate_insights(&mut self, _: &mut dyn StrategyContext, _: &str) {
        panic!("position probe must never generate insights")
    }
    fn insight_pipeline(&mut self, _: &mut dyn StrategyContext, _: &Insight) {
        panic!("position probe must never run a pipeline")
    }
    fn on_teardown(&mut self, _: &mut dyn StrategyContext) {
        panic!("position probe must never run trading cleanup")
    }
}

fn entry(side: OrderSide, quantity: f64) -> Insight {
    let mut row = Insight::new(
        side,
        "GENERIC".into(),
        StrategyType::Testing,
        TimeFrame::new(1, TimeFrameUnit::Minute),
        80,
        None,
    );
    row.set_quantity(Some(quantity));
    row
}
fn paper_step(broker: &PaperBroker, price: f64, minute: i64) {
    let at = chrono::DateTime::from_timestamp(1_700_000_040 + minute * 60, 0).unwrap();
    broker.process_step(
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
async fn wait_for_positions(
    ctx: &dyn StrategyContext,
    check: impl Fn(&[Position]) -> bool,
) -> Vec<Position> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(rows) = ctx.get_positions() {
                if check(&rows) {
                    return rows;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a fresh position snapshot should arrive")
}
fn check_insight_reads(ctx: &dyn StrategyContext, positions: &[Position]) -> Vec<Insight> {
    let mut ids = HashSet::new();
    positions
        .iter()
        .map(|position| {
            let order = position
                .order_id
                .as_ref()
                .expect("EA must provide exact position/order IDs");
            assert!(
                ids.insert(order.clone()),
                "same-symbol entries must retain separate identities"
            );
            assert!(!position.asset.symbol.is_empty());
            assert!(position.qty > 0. && position.unrealized_pnl.is_finite());
            let mut insight = Insight::default();
            insight.state = InsightState::Filled;
            insight.order_id = Some(order.clone());
            insight.symbol = position.asset.symbol.clone();
            insight.side = position.side.clone();
            ctx.bind_insight_context(&mut insight);
            let before = insight.snapshot_fingerprint_hash();
            let actual = insight
                .get_position()
                .unwrap()
                .expect("known position must resolve by its entry ID");
            assert_eq!(
                serde_json::to_value(&actual).unwrap(),
                serde_json::to_value(position).unwrap()
            );
            assert_eq!(before, insight.snapshot_fingerprint_hash());
            insight
        })
        .collect()
}

async fn paper_case(mode: StrategyMode) {
    let broker = PaperBroker::new(AccountType::Paper, 100_000., 1);
    broker.connect().await.unwrap();
    let state = StrategyState::new(
        "isolated-position-test".into(),
        "1".into(),
        ReadOnlyContext,
        UnifiedBroker::new(broker.clone(), YahooFinanceDataFeed::new()),
        mode.clone(),
        TimeFrame::new(1, TimeFrameUnit::Minute),
    );
    let live = matches!(mode, StrategyMode::Live);
    if live {
        assert!(matches!(
            StrategyContext::get_positions(&state),
            Err(BrokerError::SnapshotUnavailable { .. })
        ));
        state
            .broker
            .start_snapshot_reads(
                broker.get_account().await.unwrap(),
                Instant::now(),
                &[],
                Default::default(),
                SnapshotRefreshConfig {
                    interval: Duration::from_millis(10),
                    ..Default::default()
                },
            )
            .unwrap();
    }
    assert!(
        wait_for_positions(&state, |rows| rows.is_empty())
            .await
            .is_empty()
    );
    let mut buy = entry(OrderSide::Buy, 2.);
    let mut sell = entry(OrderSide::Sell, 3.);
    for insight in [&mut buy, &mut sell] {
        let order = broker.submit_order(insight.clone().into()).await.unwrap();
        insight.order_id = Some(order.order_id);
        insight.state = InsightState::Executed;
        state.bind_insight_context(insight);
        assert!(insight.get_position().unwrap().is_none());
    }
    paper_step(&broker, 100., 0);
    let rows = wait_for_positions(&state, |rows| rows.len() == 2).await;
    assert_eq!(broker.get_positions().await.unwrap().len(), 2);
    assert_eq!(rows.iter().filter(|p| p.asset.symbol == "GENERIC").count(), 2);
    let reads = check_insight_reads(&state, &rows);
    paper_step(&broker, 110., 1);
    let rows = wait_for_positions(&state, |rows| {
        rows.len() == 2 && (rows.iter().map(|p| p.unrealized_pnl).sum::<f64>() + 10.).abs() < 1e-8
    })
    .await;
    check_insight_reads(&state, &rows);
    assert_eq!(buy.get_position().unwrap().unwrap().unrealized_pnl, 20.);
    assert_eq!(sell.get_position().unwrap().unwrap().unrealized_pnl, -30.);
    let fingerprint = reads[0].snapshot_fingerprint_hash();
    assert_eq!(
        reads[0].get_position().unwrap().unwrap().current_price,
        110.
    );
    assert_eq!(reads[0].snapshot_fingerprint_hash(), fingerprint);
    broker
        .close_position(buy.order_id.as_ref().unwrap(), 1., Some(110.))
        .await
        .unwrap();
    paper_step(&broker, 110., 2);
    wait_for_positions(&state, |rows| {
        rows.iter()
            .any(|p| p.order_id == buy.order_id && p.qty == 1.)
    })
    .await;
    assert_eq!(buy.get_position().unwrap().unwrap().qty, 1.);
    broker
        .close_position(buy.order_id.as_ref().unwrap(), 0., Some(110.))
        .await
        .unwrap();
    paper_step(&broker, 110., 3);
    wait_for_positions(&state, |rows| rows.len() == 1).await;
    assert!(buy.get_position().unwrap().is_none());
    assert_eq!(buy.state, InsightState::Executed);
    assert!(state.command_driver.is_none());
    assert!(state.insights.is_empty());
    if live {
        state.broker.stop_snapshot_reads().await;
        assert!(matches!(
            sell.get_position(),
            Err(BrokerError::SnapshotUnavailable { .. })
        ));
    }
}
#[tokio::test]
async fn paper_backtest_insight_and_strategy_positions_are_exact_and_update_without_state_mutation()
{
    paper_case(StrategyMode::Backtest).await;
}
#[tokio::test]
async fn paper_live_insight_and_strategy_positions_are_exact_and_update_without_state_mutation() {
    paper_case(StrategyMode::Live).await;
}

#[tokio::test]
async fn isolated_mt5_probe_blocks_trading_rpcs_before_queueing() {
    let bridge = Mt5Bridge::new(Mt5BridgeConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        token: "test-only".into(),
        request_timeout: Duration::from_millis(1),
        connect_timeout: Duration::from_millis(1),
        poll_interval: Duration::from_millis(1),
        symbol_map: Default::default(),
    });
    bridge.enable_read_only_position_probe();
    for action in [
        crate::core::broker::mt5_bridge::Mt5RpcAction::SubmitOrder,
        crate::core::broker::mt5_bridge::Mt5RpcAction::CancelOrder,
        crate::core::broker::mt5_bridge::Mt5RpcAction::CloseAllPositions,
    ] {
        assert!(
            bridge
                .request_rpc(action, serde_json::json!({}))
                .await
                .is_err()
        );
    }
    assert_eq!(bridge.read_only_position_probe_stats(), (0, 3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an explicitly configured spare MT5 EA endpoint; broker reads only"]
async fn live_mt5_read_only_insight_and_strategy_positions() {
    let mut config =
        Mt5BridgeConfig::from_env().expect("provide the isolated EA bridge configuration");
    config.connect_timeout = Duration::from_secs(30);
    config.request_timeout = Duration::from_secs(5);
    let bridge = Arc::new(Mt5Bridge::new(config));
    bridge.enable_read_only_position_probe();
    bridge
        .start()
        .await
        .expect("spare bridge endpoint must be unused");
    let state = StrategyState::new(
        "read-only-position-probe".into(),
        "1".into(),
        ReadOnlyContext,
        UnifiedBroker::new(Mt5Broker::new(bridge.clone()), YahooFinanceDataFeed::new()),
        StrategyMode::Live,
        TimeFrame::new(1, TimeFrameUnit::Minute),
    );
    let result=tokio::time::timeout(Duration::from_secs(50),async {
        bridge.wait_for_rpc_poll().await?;
        let account=bridge.request_account().await?;
        state.broker.start_snapshot_reads(account.clone(),Instant::now(),&[],SnapshotFreshness {account:Duration::from_secs(20),positions:Duration::from_secs(20),..Default::default()},
            SnapshotRefreshConfig {interval:Duration::from_secs(60),request_timeout:Duration::from_secs(5),..Default::default()})?;
        let rows=wait_for_positions(&state,|rows|!rows.is_empty()).await;
        assert!(rows.iter().all(|row|row.account_id==account.account_id));
        let insights=check_insight_reads(&state,&rows);
        let count=bridge.read_only_position_probe_stats().0;let started=Instant::now();
        for _ in 0..1000 {for insight in &insights {assert!(insight.get_position()?.is_some());}}
        assert_eq!(bridge.read_only_position_probe_stats().0,count,"cached Insight reads must not emit position RPCs");
        let elapsed_us=started.elapsed().as_micros();
        let fresh=state.broker.execution.get_positions().await?;
        assert!(fresh.iter().all(|p|p.account_id==account.account_id));
        state.broker.live_snapshot_cache().unwrap().seed_positions_for_test(fresh);
        let current=StrategyContext::get_positions(&state)?;check_insight_reads(&state,&current);
        let mut absent=Insight::default();absent.state=InsightState::Executed;
        absent.order_id=Some(format!("absent-{}",Uuid::new_v4()));absent.symbol=rows[0].asset.symbol.clone();state.bind_insight_context(&mut absent);
        assert!(absent.get_position()?.is_none());
        let mut wrong=insights[0].clone();wrong.symbol="__wrong_symbol__".into();
        assert!(matches!(wrong.get_position(),Err(BrokerError::SnapshotUnavailable {reason:SnapshotUnavailable::Invalid,kind:SnapshotKind::Positions})));
        assert!(state.command_driver.is_none());assert!(state.insights.is_empty());assert!(matches!(state.status,StrategyStatus::Initialised));
        assert_eq!(bridge.read_only_position_probe_stats().1,0);
        println!("{}",serde_json::json!({"broker":"MT5","positions":rows.len(),"refreshed_positions":current.len(),
            "same_symbol_entries_preserved":rows.len()>rows.iter().map(|p|&p.asset.symbol).collect::<HashSet<_>>().len(),
            "currency":account.currency,"cached_lookups":insights.len()*1000,"cached_lookup_time_us":elapsed_us,
            "lookup_position_rpcs":0,"trading_rpc_attempts":0,"strategy_started":false,"journal_opened":false}));
        Ok::<(),BrokerError>(())
    }).await;
    state.broker.stop_snapshot_reads().await;
    bridge.shutdown().await;
    result
        .expect("read-only MT5 position test timed out")
        .expect("read-only MT5 position test failed");
}
