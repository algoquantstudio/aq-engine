# Offline lifecycle performance companion

`offline_lifecycle_performance` adds three real runtime workloads without changing the workloads, capacities or defaults in `engine_performance` or `execution_performance`:

| Case | Measured work | Default sample |
| --- | --- | --- |
| `backtest_close` | Actual backtest startup, journal, submission, fill reduction, explicit close, terminal release, result generation and normal teardown | 1,000 complete trades over 8 flat bars |
| `live_close` | Actual `run_live(None)` with a finite callback feed, real PaperBroker, command worker, durable journal, fill reduction, explicit close and normal shutdown | 1,000 complete trades |
| `backtest_bars` | Actual bar replay, bounded Polars history, four registered streaming indicators and per-bar strategy callbacks | 64 separately seeded pre-start warmup rows, then 10,000 replay bars/callbacks; no trades |

The live feed has at most one unacknowledged completed bar and pauses 1 ms between acknowledged bars so asynchronous commands can progress. It does not raise engine queue capacities or replace owner-side market processing. The report retains actual processed bar counts: compare them as well as wall time when assessing live measurements. The live bar count can vary with scheduling. Its bound is 100,000 and each sample has a five-minute deadline. This is an offline CPU/storage lifecycle workload, not a claim about MT5, network or UI latency.

The production live pipeline uses its unchanged 5-second normal sweep interval. Its initial tick may precede generated or fill-ready entries, leaving short live samples waiting for the next sweep before their close pipe runs. This creates a roughly 5-second wall-time floor, rather than a fixed teardown wait, and can mask small CPU improvements. Always assess raw entry/close latency and `backtest_close` alongside live wall time. Entry latency ends at the actual broker fill; close latency starts at the first explicit close intent and ends at the actual broker exit, including admission delays. Close latency excludes the earlier wait for the pipeline sweep and the later canonical close reduction; full lifecycle wall time includes both.

Each warmup and measured sample creates an unpredictable, fresh temporary directory and sets it as the strategy artifact root. No saved strategy, application services, cloud credentials, real journal or broker connection is used. Launch configuration and cloud/MT5-related environment settings are rejected. Successful samples remove only their own temporary directory after normal shutdown and validation. Failed samples retain their owned directory for diagnosis. Do not pass production launch arguments to the example.

## Commands

Run from the workspace root, using the same build flags, toolchain, `Cargo.lock`, CPU capacity and `POLARS_MAX_THREADS` for baseline and candidate:

```sh
cargo build -p aq-engine --release --features runtime --example offline_lifecycle_performance
cargo run -p aq-engine --release --features runtime --example offline_lifecycle_performance -- --smoke --output /tmp/aqe-lifecycle-smoke.json
cargo run -p aq-engine --release --features runtime --example offline_lifecycle_performance -- --output /tmp/aqe-lifecycle.json
```

`--smoke` is one 16-trade / 128-bar correctness sample per case, with no warmups. Do not use its throughput as a statistical baseline. Full defaults are one warmup and seven measured samples per case, on a Tokio runtime with two worker threads and a 512-row history limit. The original execution suite remains the primary unchanged aggregate control.

For `backtest_bars`, `--bars` counts replay bars and strategy callbacks, excluding the separate 64-row historical indicator seed. The fixture fetches that seed explicitly during strategy initialization and asserts its size; merely setting the runtime warmup threshold would consume the first 63 replay bars before callbacks. Seed loading and indicator initialization remain inside the timed lifecycle. The report and outcome fingerprint include the seeded row count. Benchmark warmup **runs** above are separate repetitions of the entire sample and are unrelated to these historical warmup rows.

Cases can be selected independently and sample sizes explicitly pinned:

```sh
target/release/examples/offline_lifecycle_performance --case live_close --orders 1000 --warmups 1 --samples 7 --output /tmp/aqe-live-close.json
target/release/examples/offline_lifecycle_performance --case backtest_close --orders 1000 --warmups 1 --samples 7 --output /tmp/aqe-backtest-close.json
target/release/examples/offline_lifecycle_performance --case backtest_bars --bars 10000 --warmups 1 --samples 7 --output /tmp/aqe-backtest-bars.json
```

Builds are outside the measured interval. Freeze distinct baseline and candidate executables before comparing them, record binary and source hashes including dirty source changes, and alternate runs to reduce thermal/cache bias. Record the actual release profile and compiler flags; the workspace has package-specific profile settings. Do not run builds, tests, other agents' timing work or real strategy startup concurrently with measurements. Do not run all ignored engine tests: some ignored fixtures connect to real MT5 accounts and place orders.

## Correctness and measurement boundaries

Entries alternate Buy and Sell, with quantity `1 + (index % 3) * 0.25`. Flat bars fill entries at 100. The strategy explicitly closes each position at 101 using the public receipt-based Insight API. Every entry and exit therefore has an independent expected quantity and realized P&L, even when the aggregate is near zero. An admission receipt or close RPC acknowledgement is insufficient.

The harness asserts:

- Exactly one broker-confirmed fill and close for every requested Insight, unique broker entry order IDs, matching canonical Filled and Closed states, correct quantities, entry/exit prices and realized P&L.
- Correct final cash, equity, buying power and zero fixture commissions; no broker positions, active insights, retained PaperBroker orders or pending trade events.
- Exactly one teardown call, stopped/completed lifecycle, disconnected broker/feed, removed data subscription.
- Exactly one owned execution journal after runtime disposal, a durable `stopped` phase, exactly two applied commands per complete trade, no pending commands and no recovery seeds.
- An exact 64-row pre-start history seed, all requested replay bars reaching strategy callbacks, a known per-bar SMA/EMA/RSI/ATR oracle, bounded history and an indicator checksum in the bar-heavy workload.

The JSON includes a structured deterministic outcome fingerprint, excluding UUIDs, account IDs, machine time and scheduler-dependent live bar counts. Every warmup and sample must produce the same fingerprint. Compare this fingerprint across baseline and candidate before considering timing. Raw measured durations are retained along with median and nearest-rank-style p95, throughput, processed bar counts and pooled entry-to-fill / close-to-exit latency percentiles. Latencies use monotonic time and actual PaperBroker execution callbacks. They include workload queueing and fixture observation overhead.

The wall-time scope starts immediately before `run_live` / `run_backtest` and ends after the runtime returns, including normal terminal settlement and disconnect. Fixture construction, the independent post-run economic/journal checks, callback-producer join and temporary-directory deletion are excluded. Journal inspection opens only the sample's own database after the runtime has released it. No durability settings, queue limits or production interfaces are changed.

Assess matching case IDs independently. Never add bars/s to trades/s, or present direct broker operations as equivalent to complete journaled trades. A candidate must preserve economic/order fingerprints and improve real lifecycle results without material regressions in the original controls. A normalized geometric mean may summarize matching case ratios, but retain each case's median, p95 and raw durations so a regression cannot be hidden by the aggregate.
