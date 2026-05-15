# AQE MT5 Bridge

AQE talks to MetaTrader 5 through a local HTTP bridge:

- AQE starts an HTTP server on `127.0.0.1:18080` by default.
- `AqeMt5BridgeEA.mq5` runs inside a logged-in MT5 terminal.
- AQE requests account, symbol, quote, bar, history, and order operations through the EA.
- The EA pushes subscribed bar updates and trade events back to the running strategy.

This keeps MT5 behind the standard AQE broker and data-feed interfaces. A strategy still selects symbols through `universe()`, uses its configured `TimeFrame`, and submits insights normally. AQE sets the bridge session from the runtime strategy id, so users do not manage a separate MT5 session id.

The bridge is intentionally local. It does not use the MetaTrader Python package and can be used with MT5 running under Wine on macOS, as long as the terminal can call `WebRequest()` to AQE.

## AQE Environment

Set these before running an AQE live strategy that uses MT5:

```bash
export AQE_MT5_BRIDGE_BIND_ADDR="127.0.0.1:18080"
export AQE_MT5_BRIDGE_TOKEN="replace-with-a-long-random-secret"
export AQE_MT5_REQUEST_TIMEOUT_MS="5000"
export AQE_MT5_POLL_INTERVAL_MS="250"
export AQE_MT5_SYMBOL_MAP="GBPUSD=X=GBPUSD,EURUSD=X=EURUSD"
```

`AQE_MT5_BRIDGE_TOKEN` must match the EA input.

`AQE_MT5_SYMBOL_MAP` is optional. Use it when the AQE symbol differs from the MT5 broker symbol, for example broker suffixes like `EURUSD.a` or `GBPUSDm`.

## MT5 Setup

1. Copy `AqeMt5BridgeEA.mq5` into the MT5 `MQL5/Experts` folder. 
    - Mac (Wine): `~/Library/Application Support/net.metaquotes.wine.metatrader5/drive_c/Program Files/MetaTrader 5/MQL5/Experts`
2. Open MetaEditor and compile the EA.
3. In MT5, open `Tools > Options > Expert Advisors`.
4. Enable `Allow WebRequest for listed URL`.
5. Add the AQE bridge URL, for example:

```text
http://127.0.0.1:18080
```

The WebRequest allow-list must contain the exact URL used in `InpBridgeUrl`. If MT5 is running under Wine/CrossOver and cannot reach `127.0.0.1`, use the Mac LAN IP instead, for example:

```text
http://192.168.1.144:18080
```

In that case, run AQE with:

```bash
export AQE_MT5_BRIDGE_BIND_ADDR="0.0.0.0:18080"
```

6. Attach `AqeMt5BridgeEA` to one chart.
7. Configure EA inputs:

```text
InpBridgeUrl        = http://127.0.0.1:18080
InpBridgeToken      = same value as AQE_MT5_BRIDGE_TOKEN
InpPollIntervalMs   = 250
InpRequestTimeoutMs = 5000
```

8. Keep MT5 logged in and running before starting the AQE live strategy.

If MT5 logs `initializing of AqeMt5BridgeEA failed with code 32767`, one of the EA inputs is invalid. The most common cause is an empty `InpBridgeToken`; it must be set to the same value as `AQE_MT5_BRIDGE_TOKEN`.

## Smoke Test

The ignored AQE smoke test uses the strategy universe symbol and the strategy timeframe. It does not need a symbol, timeframe, or session id env var.
It uses `BTCUSD`, validates account, ticker, and quote RPC calls, then runs a live strategy loop until it receives a `1 Minute` bar.

All tests
```bash
AQE_MT5_BRIDGE_TOKEN=test cargo test -p aq-engine --features runtime mt5 -- --ignored --nocapture
```

```bash
cargo test --features runtime test_run_live_mt5_bridge_smoke -- --ignored --nocapture
```

To run a paper-broker backtest using MT5 as the data feed:

```bash
cargo test --features runtime test_run_backtest_mt5_datafeed_paper_broker_single_entry_close -- --ignored --nocapture
```

To run the live MT5 broker/data-feed single-entry close test, use the dedicated order test. It places and closes a `0.01` BUY order on `BTCUSD`.

```bash
cargo test --features runtime test_run_live_mt5_broker_datafeed_single_entry_close -- --ignored --nocapture
```

Only run the order test on an account and symbol where `0.01` volume is valid.

## Current v1 Limits

- MT5 is live-only in v1. Use Paper/Yahoo for backtests.
- Bracket orders map to MT5 TP/SL values where possible.
- Trailing stops are not implemented in v1.
- The EA polls AQE for work and pushes subscribed bar/trade events back to AQE.
- If the bridge disconnects, the EA continues polling and resumes once AQE is reachable again.
