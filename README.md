# AlgoQuant Engine

AlgoQuant Engine (AQE) is a modular Rust framework for algorithmic trading. Build reusable strategy components and run them across backtesting and live execution, with control over your data, brokers and infrastructure.

Website: [algoquantstudio.com/aqe](https://algoquantstudio.com/aqe).

Use AQE independently or connect it to AlgoQuant Studio, the desktop workspace for research, visual strategy composition, backtest review and live monitoring. AQE provides the strategy lifecycle, insight state machine, broker and datafeed traits, backtest runner, live runner and optional AQS Cloud synchronisation.

AQE is independently buildable: no AlgoQuant Studio checkout, private research
crate, hosted gateway or AQS database schema is needed. The
`live-sync-protocol/` crate contains public wire types; optional client-side sync
is included here, while the AQS runtime host, server projector and deployment
infrastructure live in the private Studio repository.

## What AQE Provides

- A strategy lifecycle built around `on_start`, `universe`, `init`, `on_bar`, `generate_insights`, `insight_pipeline`, and `on_teardown`.
- First-class `Insight` objects for trade intent, state history, entries, fills, closes, rejections, cancellations, child insights, and order context.
- A unified broker surface that separates execution brokers from market data feeds.
- Backtest execution with result artefacts written to `metrics.json` and `backtest.db`.
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
- `backtest.db` for larger historical artefacts that can be opened by AQS or regular SQLite readers.

AlgoQuant Studio can read these artefacts and present the backtest review UI.

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
    live_session_id: None, // session_id is the canonical session identity
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

### Protected AQS launch configuration

Current AQS launches use `--live --launch-config <private-file>` instead of placing
session/MT5/NATS credentials in command-line arguments or environment assignments.
The versioned JSON file is restricted to its owner (Unix directory `0700`, file
`0600`; owner-only Windows ACL). Invalid, oversized, shared or symlinked launch
files fail closed before broker connection. Do not commit or share these files.
Rebuild a strategy with current AQE before copying its new launch command; AQS
checks binary support without executing a trading process.

Managed launches retain their private NATS/runtime-host delivery. Copied commands
run without a desktop dependency: an owned background sync worker registers with
the configured event gateway, persists retryable work to a local outbox and
forwards cloud controls. Its execution fence matches the run prepared by AQS;
each protected copied launch is single-use. Stop/recover the previous run in AQS
and copy a fresh command for a new run. The outbox/runtime identity is retained.
Initial copied startup requires gateway admission before the broker connects;
ordinary cloud outages after admission are handled by the background outbox.
Retry backoff remains protective during an outage. Once an existing gateway
heartbeat or successful batch establishes connectivity, the sender can wake a
bounded page of matching transient retries instead of waiting out a stale
five-minute timer. Recovery bursts are limited to once per 30 seconds per sync
agent; no extra heartbeat polling is added. Authentication, explicit cloud
pauses, rate limits and critical sequence ordering are not bypassed. Only local
retry deadlines change, never event identities, payloads, attempts or receipts.
Connectivity only changes retry eligibility: every event still needs its own
durable acknowledgement, and this does not prove downstream SQL has caught up.
Standalone explicit hosted-sync APIs still support their existing restart flow.
Gateway acknowledgement is not a claim
of complete broker history. Do not delete an outbox with unresolved work.

The legacy CLI `AqsAuth` arguments and MT5 environment variables still work for
standalone code; `--access-method` remains an optional override defaulting to
`aqe_live`. Do not combine cloud credential arguments with a launch file. The new
AQS files carry one canonical session ID; the duplicate Insight-state archive
and its export switch have been removed. Fill delivery is enabled by AQS and remains
opt-in for ordinary standalone AQE. Live position-mark capture and export now
default to **on** for both MT5 and Paper; standalone callers can opt out with
`AQE_POSITION_MARK_CAPTURE=false` and `AQE_POSITION_MARK_EXPORT=false`.

Copied processes detach from the terminal, disable the interactive TUI and write
private `live.log` (and `live.err.log` on Windows) beside the launch config. They
can be stopped through AQS Cloud, or by creating `stop.signal` beside that file
for an offline graceful stop. They are not an installed auto-restart service.
Existing running processes do not
reload these settings and need not be restarted to prepare the next launch.

## Development

Run checks from the AQE repository root:

```bash
cargo check
cargo check --features node --target wasm32-unknown-unknown
cargo check --features runtime
cargo test --features runtime
cargo test --manifest-path live-sync-protocol/Cargo.toml
```

When AQE is used as the `aq-engine` submodule inside AlgoQuant Studio, run checks from the AQS workspace root:

```bash
cargo check -p aq-engine --features runtime
cargo test -p aq-engine --features runtime
```

When AQE is consumed from another Cargo project (including a Git dependency),
Cargo does not inherit AQE's `[patch.crates-io]` section. The consuming workspace
must select the equivalent `zstd`, `quick-xml` and `lru` security backports itself;
see `vendor/zstd/AQS-PATCH.txt`, `vendor/quick-xml/AQS-PATCH.txt` and
`vendor/lru/AQS-PATCH.txt`. A standalone
checkout of AQE selects these patches directly. Do not treat a successful build
of an unpatched downstream dependency graph as security qualification.

Git dependencies are also pinned by the consuming project's `Cargo.lock`.
Pushing a new `main` commit makes it available to new resolutions, but an
existing project must intentionally refresh its AQE lockfile revision before
rebuilding to use that commit. This does not update an already running strategy.

<a id="status"></a>

### Reading the position for an Insight

Inside a strategy or pipe, `insight.get_position()` reads the latest broker
snapshot for that Insight's saved order ID and symbol without network I/O:

`ctx.get_positions()` returns all individual positions from the same scoped
snapshot as `Result<Vec<Position>, BrokerError>`, including separate entries on
one symbol. It is available through the object-safe `StrategyContext` interface
in strategies, alpha models and pipes. Missing/stale live snapshots return an
error; they are never reported as an empty portfolio.

```rust,ignore
match insight.get_position() {
    Ok(Some(position)) => {
        println!("{:?} {} P&L: {}", position.order_id, position.asset.symbol, position.unrealized_pnl);
    }
    Ok(None) => {} // No open position, including an unfilled pending order.
    Err(error) => {} // Snapshot unavailable/stale: defer the decision.
}
```

The live lookup is indexed by order ID and checks the broker account and snapshot
freshness. It does not submit orders, change Insight state, or copy P&L into the
Insight. Legacy position payloads without an order ID remain readable, but cannot
be used for an exact lookup. Symbol aggregates expose `order_id: None` when they
combine multiple entries. MT5 reports position profit in account currency;
commissions and swap remain separate. Paper positions use the paper execution
model's valuation.

## Release and API Compatibility

Current release status: alpha. APIs may change between releases. Read the release notes, validate broker behaviour with backtests and paper sessions, and assess execution risks before using a live account. A version number alone does not establish production suitability or latency guarantees.

<a id="license-and-ownership"></a>

## Licence and ownership

AQE and its live protocol crate are owned by AlgoQuantStudio and made
source-available under the [PolyForm Shield License 1.0.0](LICENSE), with the
required attribution and protected line of business in [NOTICE](NOTICE).
This is not an OSI-approved open-source licence or a blanket non-commercial
licence. It has no scheduled conversion to MIT or Apache-2.0.

You may use and modify AQE for your own research, backtesting and live trading,
including internal commercial trading operations, subject to the licence.
The licence does not require you to publish your private strategy code.
Non-competing distributions must retain the licence and required notices.

You may not use AQE to provide a product or service that competes with AQE or
with a product AlgoQuantStudio provides using AQE, including AlgoQuant Studio
and AQS Cloud. This applies to desktop applications and hosted services, even
if offered free of charge. Contact AlgoQuantStudio for separate written
permission if your proposed use is competitive or you are unsure.

The licence text controls over this summary. It restricts competitive use of
AQE; it is not a claim over independently written software. It does not license
the private AQS application, gateway, runtime host, research implementation,
hosted services or branding. Third-party dependencies retain their own licences.

## Current-format storage baseline (0.2.0 development cutover)

Legacy saved-data converters and the duplicate Insight-state archive have been
removed. Current execution recovery uses canonical Insight checkpoints; cloud
analytics delivery contains exact fills and position marks only.

The current private broker-journal format is application ID 1095846482/version 2,
the live outbox is application ID 1095847746/version 1, execution checkpoints use
version 6, and generated AQBT backtests use user version 2. Files from before
this cutover are intentionally unsupported: opening them fails before schema
initialization instead of altering, resetting or deleting their contents.
There is no automatic conversion or offline legacy-outbox upgrade command.

This is a breaking development cutover, including files created by older 0.2.0
development binaries. Keep those binaries and storage together until their
pending execution/delivery work is settled. Back up retained files and choose
a fresh storage location for the new build; never discard an unsettled command
journal merely to make startup succeed. Changing source does not update a
running strategy or runtime host.

Cloud metrics continuity requires a full current tracker checkpoint. Aggregate
snapshots are no longer converted into guessed return accumulators or rebuilt
by paging older Insight rows; unsupported history leaves cloud writes deferred.

`AllowTradingWindowPipe` accepts comma-separated weekday strings (`String` or
`&str`). The retired array-valued graph adapter is gone; regenerate older source
that passes a `Vec<String>`.

Native debug tests use the same larger stack as CI:

```sh
RUST_MIN_STACK=16777216 cargo test --features runtime,node --lib -- --test-threads=1
```
