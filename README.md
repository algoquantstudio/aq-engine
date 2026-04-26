# AlgoQuant Engine

AlgoQuant Engine, or AQE, is the Rust runtime behind AlgoQuant Studio. It provides the strategy lifecycle, insight state machine, broker and datafeed traits, backtest runner, live runner, and optional AQS Cloud sync used by the Studio desktop application.

AQE is early alpha software. APIs can still change while the runtime, broker integrations, and AQS Cloud workflow mature.

## What AQE Provides

- A strategy lifecycle built around `on_start`, `universe`, `init`, `on_bar`, `generate_insights`, `insight_pipeline`, and `on_teardown`.
- First-class `Insight` objects for trade intent, state history, entries, fills, closes, rejections, cancellations, child insights, and order context.
- A unified broker surface that separates execution brokers from market data feeds.
- Backtest execution with result artifacts written to `metrics.json` and `backtest.db`.
- Live execution with optional AQS Cloud sync for active strategy monitoring in AlgoQuant Studio.
- Paper broker and Yahoo Finance datafeed support.
- Experimental MT5 broker and datafeed bridge support.

## Installation

AQE is not yet published to crates.io. Use the Git repository directly:

```toml
[dependencies]
aq-engine = { git = "https://github.com/algoquantstudio/aq-engine.git", features = ["runtime"] }
```

Use the default feature set only if you need the lightweight model/codegen types without the runtime dependencies:

```toml
[dependencies]
aq-engine = { git = "https://github.com/algoquantstudio/aq-engine.git" }
```

## Feature Flags

- `runtime` enables the full trading runtime, including broker implementations, datafeeds, backtest storage, async runtime support, AQS Cloud sync, and MT5 bridge types.
- `node` enables node/editor-facing types used by AlgoQuant Studio code generation.
- `default` is intentionally empty.

Most strategy projects should use `runtime`.

## Minimal Strategy Shape

Strategies implement the `Strategy` trait. AQE calls the lifecycle methods in a fixed order for backtests and live runs.

```rust
use aq_engine::core::broker::types::{Asset, BarData};
use aq_engine::core::strategy::{Strategy, StrategyContext};
use std::collections::HashSet;

pub struct BlankStrategy;

impl Strategy for BlankStrategy {
    fn name(&self) -> &str {
        "Blank Strategy"
    }

    fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
        ctx.set_execution_risk(0.02);
        ctx.set_min_reward_risk_ratio(2.0);
        ctx.set_base_confidence(1.0);
    }

    fn universe(&self, ctx: &mut dyn StrategyContext) -> HashSet<String> {
        let mut symbols = HashSet::new();
        symbols.insert("AAPL".to_string());
        symbols
    }

    fn init(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset) {
        // Register indicators or per-asset setup here.
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, symbol: &str, bar: &BarData) {
        // Read market data and update local strategy state here.
    }

    fn generate_insights(&mut self, ctx: &mut dyn StrategyContext, symbol: &str) {
        // Create and add Insight values here when your signal fires.
    }

    fn insight_pipeline(&mut self, ctx: &mut dyn StrategyContext, insight: &aq_engine::core::insight::Insight) {
        // Manage active insights here, or register reusable insight pipes.
    }

    fn on_teardown(&mut self, ctx: &mut dyn StrategyContext) {
        // Flush external resources or final strategy state here.
    }
}
```

## Backtesting

A backtest uses a `StrategyState`, a `UnifiedBroker`, an execution broker, and a datafeed.

The default alpha path is:

1. Create your strategy.
2. Select a broker and datafeed.
3. Create `StrategyState`.
4. Call `run_backtest(start, end, timeframe)`.
5. Save or inspect the returned `BacktestResults`.

Backtest persistence writes:

- `metrics.json` for summary metrics.
- `backtest.db` for larger historical artifacts that can be opened by AQS or regular SQLite readers.

AlgoQuant Studio can read these artifacts and present the backtest review UI.

## Live Running

Live runs use the same strategy lifecycle and broker abstraction as backtests:

```rust
strategy_state.run_live(None).await?;
```

Passing `None` runs without AQS Cloud. This is the correct mode for local/offline usage or unauthenticated Studio runs.

To sync a live run into AQS Cloud, pass an `AqsAuth` value issued by AQS:

```rust
use aq_engine::core::strategy::AqsAuth;

let auth = AqsAuth {
    access_method: "aqe_live".to_string(),
    session_id: "...".to_string(),
    session_secret: "...".to_string(),
    strategy_id: "...".to_string(),
    user_id: "...".to_string(),
    node_id: None,
    live_session_id: Some("...".to_string()),
    url: None,
};

strategy_state.run_live(Some(auth)).await?;
```

Do not hard-code real AQS session secrets in source code. AQS is responsible for creating live strategy sessions and issuing short-lived live tokens.

## Brokers And Datafeeds

Current runtime integrations:

- `PaperBroker` for simulated execution and local testing.
- `YahooFinanceDataFeed` for historical and quote data.
- `Mt5Broker` and MT5 datafeed bridge for experimental live MT5 workflows.

MT5 requires a running MetaTrader 5 terminal with the AQE bridge Expert Advisor attached to a chart. See [integrations/mt5/README.md](integrations/mt5/README.md).

## AQS Cloud Sync

AQS Cloud sync is optional. When enabled, AQE persists live strategy state, insight updates, account snapshots, metrics, and strategy events to AQS Cloud so AlgoQuant Studio can show active strategy dashboards and detail views.

AQE reconnects transient Cloud websocket failures, but local strategy execution should not depend on Cloud availability. Local and offline execution are supported modes.

## Development

Run checks from the AQE repository root:

```bash
cargo check
cargo check --features runtime
cargo test --features runtime
```

When AQE is used as the `aq-engine` submodule inside AlgoQuant Studio, run checks from the AQS workspace root:

```bash
cargo check -p aq-engine --features runtime
cargo test -p aq-engine --features runtime
```

## Status

AQE is under active development. The public repository is intended for early adopters who are comfortable testing alpha trading infrastructure, reading release notes carefully, and validating broker behaviour before using any live execution path.
