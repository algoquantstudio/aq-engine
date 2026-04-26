# AQE MT5 Bridge

AQE talks to MT5 through a local bridge:

- AQE starts an HTTP server on `127.0.0.1:18080` by default.
- `AqeMt5BridgeEA.mq5` runs inside a logged-in MT5 terminal.
- The EA posts account, symbol, quote, bar, and trade-event updates to AQE.
- The EA polls AQE for order commands and acknowledges the result.

This is intentionally local. It does not use the MetaTrader Python package and can be used with MT5 running under Wine on macOS, as long as the terminal can call `WebRequest()` to AQE.

## AQE Environment

Set these before running an AQE live strategy that uses MT5:

```bash
export AQE_MT5_BRIDGE_BIND_ADDR="127.0.0.1:18080"
export AQE_MT5_BRIDGE_TOKEN="replace-with-a-long-random-secret"
export AQE_MT5_SESSION_ID="replace-with-a-session-id"
export AQE_MT5_REQUEST_TIMEOUT_MS="5000"
export AQE_MT5_POLL_INTERVAL_MS="250"
export AQE_MT5_SYMBOL_MAP="GBPUSD=X=GBPUSD,EURUSD=X=EURUSD"
```

`AQE_MT5_SYMBOL_MAP` is optional. Use it when the AQE symbol differs from the MT5 broker symbol, for example broker suffixes like `EURUSD.a` or `GBPUSDm`.

## MT5 Setup

1. Copy `AqeMt5BridgeEA.mq5` into the MT5 `MQL5/Experts` folder.
2. Open MetaEditor and compile the EA.
3. In MT5, open `Tools > Options > Expert Advisors`.
4. Enable `Allow WebRequest for listed URL`.
5. Add the AQE bridge URL, for example:

```text
http://127.0.0.1:18080
```

6. Attach `AqeMt5BridgeEA` to one chart.
7. Configure EA inputs:

```text
InpBridgeUrl       = http://127.0.0.1:18080
InpSessionId       = same value as AQE_MT5_SESSION_ID
InpBridgeToken     = same value as AQE_MT5_BRIDGE_TOKEN
InpSymbols         = EURUSD,GBPUSD
InpTimeframe       = PERIOD_M1
InpPollIntervalMs  = 250
InpRequestTimeoutMs = 5000
```

8. Keep MT5 logged in and running before starting the AQE live strategy.

## Current v1 Limits

- MT5 is live-only in v1. Use Paper/Yahoo for backtests.
- Bracket orders map to MT5 TP/SL values where possible.
- Trailing stops are not implemented in v1.
- The EA intentionally polls commands with `maxCommands = 1` to keep the MQL5 JSON parser small and predictable.
- If the bridge disconnects, the EA sends a fresh snapshot on reconnect.
