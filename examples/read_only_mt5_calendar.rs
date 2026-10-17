//! Explicit read-only EA canary: account and asset metadata RPCs only.
//! Never constructs a StrategyState, reconciles orders, or submits a trade.
use aq_engine::core::broker::{
    mt5_bridge::{Mt5Bridge, Mt5BridgeConfig},
    types::BrokerError,
};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() {
    let mut symbols: Vec<_> = std::env::args().skip(1).collect();
    if symbols.is_empty() {
        symbols.push("BTCUSD".into());
    }
    assert!(symbols.len() <= 16 && symbols.iter().all(|s| !s.is_empty() && s.len() <= 64));
    let mut config = Mt5BridgeConfig::from_env()
        .expect("configure the authorized token and private listener address");
    config.connect_timeout = Duration::from_secs(30);
    config.request_timeout = Duration::from_secs(10);
    let bridge = Arc::new(Mt5Bridge::new(config));
    bridge
        .start()
        .await
        .expect("unused, EA-configured listener address required");
    let result = tokio::time::timeout(Duration::from_secs(55), async {
        bridge.wait_for_rpc_poll().await?;
        let account = bridge.request_account().await?;
        let mut calendars = vec![];
        for symbol in symbols {
            let asset = bridge.request_asset(&symbol).await?;
            assert_eq!(asset.symbol, symbol);
            let raw = asset.trading_calendar.expect("EA did not return trading_calendar; compile/reload EA 1.425 or newer");
            let calendar: serde_json::Value = serde_json::from_str(&raw).expect("producer calendar JSON");
            assert_eq!(calendar["source"], "mt5_weekly_trade_sessions");
            assert!(matches!(calendar["kind"].as_str(), Some("continuous" | "sessions" | "unknown")));
            let week = calendar["weekly_seconds"].as_array().expect("weekly session evidence");
            assert_eq!(week.len(), 7);
            for day in week {
                for session in day.as_array().expect("session list") {
                    let interval = session.as_array().expect("session interval");
                    assert_eq!(interval.len(), 2);
                    let start = interval[0].as_i64().unwrap(); let end = interval[1].as_i64().unwrap();
                    assert!((0..86400).contains(&start) && (start..=172800).contains(&end));
                }
            }
            let offset = calendar["observed_utc_offset_seconds"].as_i64().expect("automatically observed broker offset");
            assert!(offset.abs() <= 18 * 3600);
            assert_eq!(calendar["holiday_exceptions_recorded"], false);
            calendars.push(serde_json::json!({"symbol":symbol,"calendar":calendar}));
        }
        Ok::<_, BrokerError>(serde_json::json!({"read_only":true,"currency":account.currency,"calendars":calendars,"trading_commands_sent":0}))
    }).await;
    bridge.shutdown().await;
    println!(
        "{}",
        serde_json::to_string_pretty(
            &result
                .expect("read-only probe timed out")
                .expect("read-only RPC failed")
        )
        .unwrap()
    );
}
