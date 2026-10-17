use super::*;
use crate::core::broker::{UnifiedBroker, paper_broker::PaperBroker, types::AccountType};
use crate::core::events::{EventStreamType, ResolvedEventStream};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_full_strategy_backtest_uses_paper_execution_and_rejects_missing_assets() {
    use crate::core::broker::types::OrderSide;
    use crate::core::insight::{Insight, types::StrategyType};
    use crate::core::strategy::{Strategy, StrategyContext, StrategyMode, StrategyState};
    use std::sync::Mutex;

    struct Fixture {
        submitted: bool,
        initialized: Arc<Mutex<Vec<String>>>,
        seen: Arc<Mutex<Vec<String>>>,
    }
    impl Strategy for Fixture {
        fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
            assert!(ctx.has_datafeed_feature(DataFeedFeature::CandleStream));
            ctx.add_events(
                EventStreamType::Bar,
                Some(TimeFrame::parse_timeframe("5m").unwrap()),
            );
        }
        fn universe(&self, _: &mut dyn StrategyContext) -> HashSet<String> {
            HashSet::from(["BTCUSD".into(), "MISSING".into()])
        }
        fn init(&mut self, _: &mut dyn StrategyContext, asset: &Asset) {
            self.initialized.lock().unwrap().push(asset.symbol.clone());
        }
        fn on_bar(&mut self, _: &mut dyn StrategyContext, symbol: &str, _: &BarData) {
            self.seen.lock().unwrap().push(symbol.into());
        }
        fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) {
            if self.submitted {
                return;
            }
            let mut insight = Insight::new(
                OrderSide::Buy,
                symbol.into(),
                StrategyType::Testing,
                *ctx.timeframe(),
                90,
                None,
            );
            insight.set_quantity(Some(1.0));
            insight.submit(ctx).expect("CSV fixture market order should be accepted");
            ctx.add_insight(insight);
            self.submitted = true;
        }
        fn insight_pipeline(&mut self, _: &mut dyn StrategyContext, _: &Insight) {}
        fn on_teardown(&mut self, ctx: &mut dyn StrategyContext) {
            ctx.cleanup_active_insights_for_teardown();
        }
    }
    let data = feed("BTCUSD,1m,2026-10-01T07:00:00Z,100,102,99,101,1\nBTCUSD,1m,2026-10-01T07:01:00Z,101,103,100,102,1\nBTCUSD,1m,2026-10-01T07:02:00Z,102,104,101,103,1\nBTCUSD,1m,2026-10-01T07:03:00Z,103,105,102,104,1\n").await;
    let initialized = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let fixture = Fixture {
        submitted: false,
        initialized: initialized.clone(),
        seen: seen.clone(),
    };
    let broker = UnifiedBroker::new(PaperBroker::new(AccountType::Paper, 1000., 1), data);
    let mut state = StrategyState::new(
        "CSV integration".into(),
        "1".into(),
        fixture,
        broker,
        StrategyMode::Backtest,
        TimeFrame::default(),
    );
    // Framework regression artifacts stay in a uniquely owned temporary directory,
    // never in a user's strategy or the engine checkout.
    let output = std::env::temp_dir().join(format!("aqe-csv-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&output).unwrap();
    state.set_artifact_root(&output);
    state.set_warm_up_bars(0);
    let results = state
        .run_backtest(
            parse_timestamp("2026-10-01T07:00:00Z").unwrap(),
            parse_timestamp("2026-10-01T07:04:00Z").unwrap(),
            TimeFrame::default(),
        )
        .await
        .unwrap();
    assert_eq!(*initialized.lock().unwrap(), ["BTCUSD"]);
    assert!(!seen.lock().unwrap().is_empty());
    assert!(seen.lock().unwrap().iter().all(|s| s == "BTCUSD"));
    assert!(
        !results.trade_log.is_empty(),
        "CSV backtest should execute the fixture order"
    );
    assert!(results.final_equity.is_finite());
    drop(state);
    std::fs::remove_dir_all(output).unwrap();
}

#[tokio::test]
async fn csv_backtest_loader_skips_unavailable_streams_and_replays_exact_bars() {
    let feed = feed("BTCUSD,1m,2026-10-01T07:00:00Z,100,102,99,101,1\nBTCUSD,1m,2026-10-01T07:01:00Z,101,103,100,102,1\n").await;
    let mut broker =
        UnifiedBroker::new_backtest(PaperBroker::new(AccountType::Paper, 1000., 1), feed);
    let main = TimeFrame::default();
    let streams = [
        ResolvedEventStream::new(EventStreamType::Bar, "BTCUSD", main, main, true),
        ResolvedEventStream::new(
            EventStreamType::Bar,
            "BTCUSD",
            TimeFrame::parse_timeframe("5m").unwrap(),
            main,
            false,
        ),
        ResolvedEventStream::new(EventStreamType::Bar, "ETHUSD", main, main, true),
    ];
    let start = parse_timestamp("2026-10-01T07:00:00Z").unwrap();
    let end = parse_timestamp("2026-10-01T07:02:00Z").unwrap();
    broker
        .load_backtest_event_streams(&streams, start, end)
        .await
        .unwrap();
    assert!(broker.has_feature(DataFeedFeature::CandleStream));
    assert!(broker.has_feature(DataFeedFeature::QuoteStream));
    assert!(!broker.has_feature(DataFeedFeature::NativeBidAsk));
    let mut closes = Vec::new();
    while let Some(step) = broker.step_market_streams().unwrap() {
        for event in step.events {
            assert_eq!(event.context.timeframe, main);
            assert_eq!(event.bar.symbol, "BTCUSD");
            closes.push(event.bar.close);
        }
    }
    assert_eq!(closes, [101., 102.]);
}

#[tokio::test]
async fn csv_backtest_loader_fails_clearly_when_all_requested_streams_are_missing() {
    let feed = feed("BTCUSD,5m,2026-10-01T07:00:00Z,100,110,90,109,1\n").await;
    let mut broker =
        UnifiedBroker::new_backtest(PaperBroker::new(AccountType::Paper, 1000., 1), feed);
    let tf = TimeFrame::default();
    let streams = [ResolvedEventStream::new(
        EventStreamType::Bar,
        "BTCUSD",
        tf,
        tf,
        true,
    )];
    let start = parse_timestamp("2026-10-01T07:00:00Z").unwrap();
    let end = parse_timestamp("2026-10-01T07:10:00Z").unwrap();
    assert!(
        broker
            .load_backtest_event_streams(&streams, start, end)
            .await
            .unwrap_err()
            .to_string()
            .contains("No requested historical streams")
    );
}

const HEADER: &str = "symbol,timeframe,timestamp,open,high,low,close,volume\n";

fn read(rows: &str) -> Result<CsvDataset, BrokerError> {
    CsvDataset::read(format!("{HEADER}{rows}").as_bytes())
}

async fn feed(rows: &str) -> CsvDataFeed {
    let feed = CsvDataFeed::new("unused-in-memory-test.csv");
    *feed.data.lock().await = Some(Arc::new(read(rows).unwrap()));
    feed.connected.store(true, Ordering::Release);
    feed
}

#[test]
fn csv_validates_headers_and_handles_quoting_bom_reordering() {
    let data = "\u{feff}VOLUME,CLOSE,LOW,HIGH,OPEN,TIMESTAMP,TIMEFRAME,SYMBOL\r\n1,101,99,102,100,2026-10-01T07:00:00Z,1m,\"BTC,USD\"\r\n";
    let parsed = CsvDataset::read(data.as_bytes()).unwrap();
    assert_eq!(parsed.streams.len(), 1);
    assert_eq!(parsed.streams.values().next().unwrap()[0].symbol, "BTC,USD");
    for data in [
        "symbol,timeframe\n",
        "symbol,timeframe,timestamp,open,high,low,close,volume,open\n",
        HEADER,
    ] {
        assert!(CsvDataset::read(data.as_bytes()).is_err());
    }
}

#[test]
fn csv_rejects_invalid_rows_and_duplicate_normalized_bars() {
    for row in [
        "BTCUSD,1m,bad,100,102,99,101,1\n",
        "BTCUSD,0m,2026-10-01T07:00:00Z,100,102,99,101,1\n",
        "BTCUSD,60m,2026-10-01T07:00:00Z,100,102,99,101,1\n",
        "BTCUSD,1m,2026-10-01T07:00:00Z,NaN,102,99,101,1\n",
        "BTCUSD,1m,2026-10-01T07:00:00Z,100,102,99,103,1\n",
        "BTCUSD,1m,2026-10-01T07:00:00Z,100,102,99,101,-1\n",
        "BTCUSD,1m,2026-10-01T07:00:00Z,100,102,99,101,inf\n",
        ",1m,2026-10-01T07:00:00Z,100,102,99,101,1\n",
        "BTCUSD,1m,2026-10-01T07:00:00Z,100,102,99,101\n",
        "BTCUSD,1m,2026-10-01T07:00:00.000001Z,100,102,99,101,1\n",
    ] {
        assert!(read(row).is_err(), "accepted invalid row {row}");
    }
    let duplicate = "BTCUSD,1m,2026-10-01T07:00:00Z,100,102,99,101,1\nBTCUSD,01M,2026-10-01T08:00:00+01:00,100,102,99,101,1\n";
    assert!(
        read(duplicate)
            .unwrap_err()
            .to_string()
            .contains("duplicate bar")
    );
}

#[test]
fn csv_timestamps_normalize_to_utc_without_losing_precision() {
    let expected = DateTime::parse_from_rfc3339("2026-10-01T07:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    for value in [
        "2026-10-01T08:00:00+01:00".to_string(),
        "2026-10-01 07:00:00".to_string(),
        expected.timestamp().to_string(),
        expected.timestamp_millis().to_string(),
    ] {
        assert_eq!(parse_timestamp(&value).unwrap(), expected);
    }
}

#[tokio::test]
async fn csv_history_is_sorted_isolated_and_end_exclusive() {
    let feed = feed("BTCUSD,1m,2026-10-01T07:02:00Z,102,104,101,103,1\nBTCUSD,1m,2026-10-01T07:00:00Z,100,102,99,101,1\nBTCUSD,1m,2026-10-01T07:01:00Z,101,103,100,102,1\nETHUSD,1m,2026-10-01T07:00:00Z,10,12,9,11,1\nBTCUSD,5m,2026-10-01T07:00:00Z,100,110,90,109,1\n").await;
    let start = parse_timestamp("2026-10-01T07:00:00Z").unwrap();
    let end = parse_timestamp("2026-10-01T07:02:00Z").unwrap();
    let frame = feed
        .get_history("BTCUSD", start, end, TimeFrame::default())
        .await
        .unwrap();
    assert_eq!(frame.height(), 2);
    assert_eq!(
        frame
            .column("close")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>(),
        vec![101., 102.]
    );
    assert_eq!(
        frame.column("timestamp").unwrap().i64().unwrap().get(0),
        Some(start.timestamp_millis())
    );
    assert_eq!(feed.summary().await.unwrap().len(), 3);
}

#[tokio::test]
async fn csv_missing_symbols_are_untradable_and_missing_timeframes_are_skipped() {
    let feed = feed("BTCUSD,5m,2026-10-01T07:00:00Z,100,110,90,109,1\n").await;
    assert!(feed.get_ticker_info("BTCUSD").await.unwrap().tradable);
    assert!(!feed.get_ticker_info("ETHUSD").await.unwrap().tradable);
    let start = parse_timestamp("2026-10-01T07:00:00Z").unwrap();
    let end = parse_timestamp("2026-10-01T07:03:00Z").unwrap();
    // Never expose the 07:05 close as a 07:00 1m candle.
    let frame = feed
        .get_history("BTCUSD", start, end, TimeFrame::default())
        .await
        .unwrap();
    assert_eq!(frame.height(), 0);
    assert!(frame.column("timestamp").unwrap().i64().is_ok());
    assert_eq!(feed.get_features(), &[DataFeedFeature::HistoricalCandles]);
    assert!(feed.get_latest_bar("BTCUSD").await.is_err());
    assert!(feed.get_latest_quote("BTCUSD").await.is_err());
    assert!(feed.configure_live_session("test").is_err());
}

#[tokio::test]
async fn csv_connect_reports_missing_file_without_network_fallback() {
    let feed = CsvDataFeed::new(
        std::env::temp_dir().join(format!("missing-{}.csv", uuid::Uuid::new_v4())),
    );
    assert!(
        feed.connect()
            .await
            .unwrap_err()
            .to_string()
            .contains("Cannot open CSV")
    );
    assert!(!feed.is_connected());
}

#[tokio::test]
async fn csv_file_is_loaded_once_per_connection_and_revalidated_after_disconnect() {
    let path = std::env::temp_dir().join(format!("aqe-csv-file-test-{}.csv", uuid::Uuid::new_v4()));
    std::fs::write(
        &path,
        format!("{HEADER}BTCUSD,1m,2026-10-01T07:00:00Z,100,102,99,101,1\n"),
    )
    .unwrap();
    let feed = CsvDataFeed::new(&path);
    assert!(feed.connect().await.unwrap());
    assert!(feed.is_connected());
    std::fs::write(&path, "invalid file").unwrap();
    assert_eq!(
        feed.summary().await.unwrap()[0].bars,
        1,
        "in-flight research/backtest snapshot must remain stable"
    );
    feed.disconnect().await.unwrap();
    assert!(feed.connect().await.is_err());
    assert!(!feed.is_connected());
    std::fs::remove_file(path).unwrap();
}
