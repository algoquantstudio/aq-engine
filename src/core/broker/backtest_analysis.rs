//! Finalization-scoped derived data. Borrowing results prevents stale analytics
//! if a caller later edits the public trade log or account history.
use super::*;

pub(crate) struct PreparedBacktestAnalysis<'a> {
    pub(super) results: &'a BacktestResults,
    pub(super) trips: Vec<RoundTripTrade>,
    values: DerivedValues,
}

struct DerivedValues {
    sharpe_ratio: f64,
    annualized_volatility: f64,
    sortino_ratio: f64,
    expectancy: f64,
    profit_factor: f64,
    payoff_ratio: f64,
    avg_winner: f64,
    avg_loser: f64,
    avg_winner_pct: f64,
    avg_loser_pct: f64,
    avg_return_pct_long: f64,
    avg_return_pct_short: f64,
    avg_nominal_long: f64,
    avg_nominal_short: f64,
    average_trade_held_secs: i64,
    longest_winning_trade_held_secs: i64,
    longest_losing_trade_held_secs: i64,
    count_long_short: (usize, usize),
}

impl std::ops::Deref for PreparedBacktestAnalysis<'_> {
    type Target = BacktestResults;
    fn deref(&self) -> &Self::Target {
        self.results
    }
}

impl<'a> PreparedBacktestAnalysis<'a> {
    pub(super) fn new(results: &'a BacktestResults) -> Self {
        let trips = results.round_trip_trades();
        let returns = results.equity_returns();
        // Preserve each metric's existing filter, reduction order, zero handling,
        // and partial-exit accounting; only pairing/materialization is shared.
        let winners = trips.iter().filter(|trip| trip.pnl > 0.0).count();
        let losers = trips.iter().filter(|trip| trip.pnl < 0.0).count();
        let longs = trips
            .iter()
            .filter(|trip| trip.side == OrderSide::Buy)
            .count();
        let shorts = trips
            .iter()
            .filter(|trip| trip.side == OrderSide::Sell)
            .count();
        let gross_profit: f64 = trips
            .iter()
            .filter(|trip| trip.pnl > 0.0)
            .map(|trip| trip.pnl)
            .sum();
        let gross_loss: f64 = trips
            .iter()
            .filter(|trip| trip.pnl < 0.0)
            .map(|trip| trip.pnl.abs())
            .sum();
        let mean = |sum: f64, count: usize| if count == 0 { 0.0 } else { sum / count as f64 };
        let avg_winner = mean(gross_profit, winners);
        let avg_loser = mean(gross_loss, losers);
        let values = DerivedValues {
            sharpe_ratio: annualized_sharpe(&returns),
            annualized_volatility: annualized_volatility(&returns),
            sortino_ratio: annualized_sortino(&returns),
            expectancy: mean(trips.iter().map(|trip| trip.pnl).sum(), trips.len()),
            profit_factor: if gross_loss == 0.0 {
                if gross_profit > 0.0 {
                    f64::INFINITY
                } else {
                    0.0
                }
            } else {
                gross_profit / gross_loss
            },
            payoff_ratio: if avg_loser == 0.0 {
                0.0
            } else {
                avg_winner / avg_loser
            },
            avg_winner,
            avg_loser,
            avg_winner_pct: mean(
                trips
                    .iter()
                    .filter(|trip| trip.pnl > 0.0)
                    .map(|trip| trip.return_pct)
                    .sum(),
                winners,
            ),
            avg_loser_pct: mean(
                trips
                    .iter()
                    .filter(|trip| trip.pnl < 0.0)
                    .map(|trip| trip.return_pct.abs())
                    .sum(),
                losers,
            ),
            avg_return_pct_long: mean(
                trips
                    .iter()
                    .filter(|trip| trip.side == OrderSide::Buy)
                    .map(|trip| trip.return_pct)
                    .sum(),
                longs,
            ),
            avg_return_pct_short: mean(
                trips
                    .iter()
                    .filter(|trip| trip.side == OrderSide::Sell)
                    .map(|trip| trip.return_pct)
                    .sum(),
                shorts,
            ),
            avg_nominal_long: mean(
                trips
                    .iter()
                    .filter(|trip| trip.side == OrderSide::Buy)
                    .map(|trip| trip.pnl)
                    .sum(),
                longs,
            ),
            avg_nominal_short: mean(
                trips
                    .iter()
                    .filter(|trip| trip.side == OrderSide::Sell)
                    .map(|trip| trip.pnl)
                    .sum(),
                shorts,
            ),
            average_trade_held_secs: if trips.is_empty() {
                0
            } else {
                trips.iter().map(|trip| trip.hold_secs).sum::<i64>() / trips.len() as i64
            },
            longest_winning_trade_held_secs: trips
                .iter()
                .filter(|trip| trip.pnl > 0.0)
                .map(|trip| trip.hold_secs)
                .max()
                .unwrap_or(0),
            longest_losing_trade_held_secs: trips
                .iter()
                .filter(|trip| trip.pnl < 0.0)
                .map(|trip| trip.hold_secs)
                .max()
                .unwrap_or(0),
            count_long_short: (longs, shorts),
        };
        Self {
            results,
            trips,
            values,
        }
    }
}

macro_rules! derived_accessors {
    ($($name:ident: $kind:ty),* $(,)?) => {
        impl PreparedBacktestAnalysis<'_> {
            $(pub(super) fn $name(&self) -> $kind { self.values.$name })*
        }
    };
}
derived_accessors! {
    sharpe_ratio: f64,
    annualized_volatility: f64,
    sortino_ratio: f64,
    expectancy: f64,
    profit_factor: f64,
    payoff_ratio: f64,
    avg_winner: f64,
    avg_loser: f64,
    avg_winner_pct: f64,
    avg_loser_pct: f64,
    avg_return_pct_long: f64,
    avg_return_pct_short: f64,
    avg_nominal_long: f64,
    avg_nominal_short: f64,
    average_trade_held_secs: i64,
    longest_winning_trade_held_secs: i64,
    longest_losing_trade_held_secs: i64,
    count_long_short: (usize, usize),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::broker::types::AccountType;
    use chrono::TimeZone;

    fn account(equity: f64) -> Account {
        Account {
            account_id: "analysis-test".into(),
            max_total_orders: None,
            account_type: AccountType::Paper,
            equity,
            cash: equity,
            currency: "USD".into(),
            buying_power: equity,
            accrued_commission: 0.0,
            shorting_enabled: true,
            leverage: 1,
        }
    }

    fn fixture(entries: usize) -> BacktestResults {
        let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mut trades = Vec::with_capacity(entries * 3);
        for index in 0..entries {
            let entry = TradeRecord {
                date: start + chrono::Duration::minutes(index as i64),
                symbol: "AAPL".into(),
                side: if index % 2 == 0 {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                },
                qty: 10.0,
                price: if index % 11 == 0 { 0.0 } else { 100.0 },
                order_id: format!("order-{index}"),
                insight_id: Some(format!("insight-{index}")),
                strategy_type: Some("AnalysisFixture".into()),
                commission: 3.5,
                swap: 0.0,
                execution_cash: None,
                account_notional: None,
                valuation_basis: None,
                trade_type: TradeRecordType::Entry,
            };
            trades.push(entry.clone());
            for (offset, qty, price, swap) in [(1, 4.0, 98.25, -0.75), (2, 8.0, 105.5, 2.5)] {
                trades.push(TradeRecord {
                    date: entry.date
                        + chrono::Duration::seconds(if index % 7 == 0 { -offset } else { offset }),
                    qty,
                    price,
                    commission: 1.25,
                    swap,
                    trade_type: TradeRecordType::Exit,
                    ..entry.clone()
                });
            }
        }
        BacktestResults {
            starting_cash: 10_000.0,
            final_equity: 10_050.0,
            total_return_pct: 0.5,
            total_trades: entries * 2,
            winning_trades: entries,
            losing_trades: entries,
            win_rate: 0.5,
            max_drawdown: 0.05,
            trade_log: trades,
            account_history: [10_000.0, 10_005.0, 0.0, 0.0, 100.0, -50.0, 10_050.0]
                .into_iter()
                .enumerate()
                .map(|(index, equity)| {
                    (
                        start + chrono::Duration::days(index as i64),
                        account(equity),
                    )
                })
                .collect(),
            executed_at: start,
            finished_at: start + chrono::Duration::days(7),
            backtest_start: Some(start),
            backtest_end: Some(start + chrono::Duration::days(7)),
        }
    }

    fn baseline_values(results: &BacktestResults) -> Vec<f64> {
        vec![
            results.sharpe_ratio(),
            results.annualized_volatility(),
            results.sortino_ratio(),
            results.expectancy(),
            results.profit_factor(),
            results.payoff_ratio(),
            results.avg_winner(),
            results.avg_loser(),
            results.avg_winner_pct(),
            results.avg_loser_pct(),
            results.avg_return_pct_long(),
            results.avg_return_pct_short(),
            results.avg_nominal_long(),
            results.avg_nominal_short(),
        ]
    }
    fn prepared_values(analysis: &PreparedBacktestAnalysis<'_>) -> Vec<f64> {
        vec![
            analysis.sharpe_ratio(),
            analysis.annualized_volatility(),
            analysis.sortino_ratio(),
            analysis.expectancy(),
            analysis.profit_factor(),
            analysis.payoff_ratio(),
            analysis.avg_winner(),
            analysis.avg_loser(),
            analysis.avg_winner_pct(),
            analysis.avg_loser_pct(),
            analysis.avg_return_pct_long(),
            analysis.avg_return_pct_short(),
            analysis.avg_nominal_long(),
            analysis.avg_nominal_short(),
        ]
    }

    fn check(results: &BacktestResults) {
        let prepared = results.prepare_analysis();
        assert_eq!(
            baseline_values(results)
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>(),
            prepared_values(&prepared)
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            prepared.average_trade_held_secs(),
            results.average_trade_held_secs()
        );
        assert_eq!(
            prepared.longest_winning_trade_held_secs(),
            results.longest_winning_trade_held_secs()
        );
        assert_eq!(
            prepared.longest_losing_trade_held_secs(),
            results.longest_losing_trade_held_secs()
        );
        assert_eq!(
            serde_json::to_value(&prepared.trips).unwrap(),
            serde_json::to_value(results.round_trip_trades()).unwrap()
        );
        let lines = prepared.metric_summary_lines();
        assert!(lines.contains(&format!(
            "  Expectancy:        ${:.2}",
            results.expectancy()
        )));
        assert!(lines.contains(&format!(
            "  Profit Factor:     {:.2}",
            results.profit_factor()
        )));
    }

    #[test]
    fn prepared_finalization_matches_partial_exits_fees_zero_prices_and_mutation() {
        for entries in [0, 1, 32] {
            check(&fixture(entries));
        }
        let mut results = fixture(3);
        // Zero-PnL and all-winning/all-losing branches keep their original semantics.
        for exit_price in [100.0, 110.0, 90.0] {
            for record in &mut results.trade_log {
                record.side = OrderSide::Buy;
                record.price = if matches!(record.trade_type, TradeRecordType::Entry) {
                    100.0
                } else {
                    exit_price
                };
                record.commission = 0.0;
                record.swap = 0.0;
            }
            check(&results);
        }
        // Preparation is scoped, so later public mutations are read on the next save.
        results.trade_log.clear();
        results.account_history.clear();
        check(&results);
    }

    #[cfg(feature = "runtime")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prepared_finalization_persists_public_metric_values_and_all_trips() {
        struct Directory(std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let directory = Directory(
            std::env::temp_dir().join(format!("aqe-prepared-analysis-{}", uuid::Uuid::new_v4())),
        );
        let results = fixture(32);
        let state = BacktestState::new();
        let prepared = results.prepare_analysis();
        prepared
            .save_to_disk_async(&directory.0, &state)
            .await
            .unwrap();
        let metrics: serde_json::Value =
            serde_json::from_slice(&std::fs::read(directory.0.join("metrics.json")).unwrap())
                .unwrap();
        macro_rules! check_metric {
            ($($name:ident),* $(,)?) => { $(assert_eq!(metrics[stringify!($name)], serde_json::json!(results.$name()), stringify!($name));)* };
        }
        check_metric!(
            sharpe_ratio,
            annualized_volatility,
            sortino_ratio,
            expectancy,
            profit_factor,
            payoff_ratio,
            avg_winner,
            avg_loser,
            avg_winner_pct,
            avg_loser_pct,
            average_trade_held_secs,
            longest_winning_trade_held_secs,
            longest_losing_trade_held_secs
        );
        let db =
            turso::Builder::new_local(directory.0.join("backtest.db").to_string_lossy().as_ref())
                .build()
                .await
                .unwrap();
        let conn = db.connect().unwrap();
        let mut rows = conn
            .query("SELECT COUNT(*) FROM round_trips", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            prepared.trips.len() as i64
        );
    }

    #[test]
    #[ignore = "manual paired finalization analytics performance probe"]
    fn prepared_finalization_performance_probe() {
        use std::hint::black_box;
        use std::time::Instant;
        let results = fixture(10_000);
        check(&results);
        let mut baseline = Vec::new();
        let mut prepared = Vec::new();
        for _ in 0..20 {
            let start = Instant::now();
            black_box(baseline_values(black_box(&results)));
            black_box(results.round_trip_trades());
            baseline.push(start.elapsed().as_nanos());
            let start = Instant::now();
            let analysis = black_box(&results).prepare_analysis();
            black_box(prepared_values(&analysis));
            black_box(&analysis.trips);
            prepared.push(start.elapsed().as_nanos());
        }
        baseline.sort_unstable();
        prepared.sort_unstable();
        eprintln!(
            "finalization_analytics events=30000 trips=20000 baseline_p50_ns={} baseline_p95_ns={} prepared_p50_ns={} prepared_p95_ns={}",
            baseline[10], baseline[19], prepared[10], prepared[19]
        );
    }
}
