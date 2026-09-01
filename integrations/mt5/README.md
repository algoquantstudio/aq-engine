# AQE MT5 Bridge

AQE talks to MetaTrader 5 through a local Actix Web HTTP bridge:

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
export AQE_MT5_CONNECT_TIMEOUT_MS="15000"
export AQE_MT5_REQUEST_TIMEOUT_MS="15000"
export AQE_MT5_POLL_INTERVAL_MS="250"
export AQE_MT5_SYMBOL_MAP="GBPUSD=X=GBPUSD,EURUSD=X=EURUSD"
```

`AQE_MT5_BRIDGE_TOKEN` must match the EA input.

`AQE_MT5_CONNECT_TIMEOUT_MS` controls how long AQE waits for the EA to poll the bridge before loading the strategy universe. This is useful when restarting a live strategy while MT5 is still finishing an old WebRequest attempt.

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
7. Configure the EA inputs. The values below are the defaults; the bridge token is the only value that must be supplied:

```text
InpBridgeUrl         = http://127.0.0.1:18080
InpBridgeToken       = same value as AQE_MT5_BRIDGE_TOKEN
InpBridgeConnections = optional extra bridge URLs separated by commas
InpPollIntervalMs = 100
InpRequestTimeoutMs = 5000
InpTradeEventFlushIntervalMs = 10
InpTradeEventPostTimeoutMs = 100
InpTradeEventBatchSize = 32
InpTradeEventQueueCapacity = 2048
InpMarketDataIntervalMs = 50
InpMarketDataPostTimeoutMs = 100
```

| Input | Purpose |
| --- | --- |
| `InpBridgeUrl` | Primary AQE HTTP bridge URL. It must exactly match an entry in the MT5 WebRequest allow-list. |
| `InpBridgeToken` | Shared authentication secret. It must match `AQE_MT5_BRIDGE_TOKEN` and cannot be empty. |
| `InpBridgeConnections` | Optional comma-separated additional bridge URLs for running multiple AQE strategies from one EA. |
| `InpPollIntervalMs` | Base interval used to poll active AQE bridges for RPC work. |
| `InpRequestTimeoutMs` | Timeout for ordinary active-bridge WebRequests. |
| `InpTradeEventFlushIntervalMs` | Minimum interval between attempts to flush queued order and trade events. This also drives the EA's high-resolution timer. |
| `InpTradeEventPostTimeoutMs` | Short WebRequest timeout used when posting trade-event batches. |
| `InpTradeEventBatchSize` | Maximum number of queued trade events sent in one request. Events remain FIFO within each bridge; a failed front batch is retried before later batches. |
| `InpTradeEventQueueCapacity` | Maximum number of trade events retained during a temporary disconnection. If the queue is full, the newest event is rejected so retained events cannot be reordered. |
| `InpMarketDataIntervalMs` | Target interval for subscribed quotes/intrabar updates and detection of newly completed bars. |
| `InpMarketDataPostTimeoutMs` | Short WebRequest timeout used when posting quotes and completed bars. |

For multiple AQE live strategies from one MT5 terminal, either attach one EA per bridge URL or set extra bridge endpoints in `InpBridgeConnections`, for example:

```text
InpBridgeConnections = http://127.0.0.1:18081,http://127.0.0.1:18082
```

The EA automatically discovers every URL in `InpBridgeConnections`. A configured endpoint without an AQE server is probed one at a time with a short timeout and adaptive backoff; it does not participate in the fast active polling loop until it establishes a session. If an active bridge disappears, it returns to this discovery state and reconnects automatically when the AQE server is available again.

AQE cannot initiate this connection because the EA exposes no listening server: MT5 `WebRequest` is client-initiated and AQE is the HTTP listener. Some periodic discovery request is therefore required for a stopped AQE endpoint to become visible later. The discovery timing is now an internal implementation detail instead of an EA input.

All configured bridge URLs use `InpBridgeToken`; per-endpoint tokens are not supported. Add every URL in `InpBridgeUrl` and `InpBridgeConnections` to the MT5 WebRequest allow-list.

AQE and AQS use UTC internally. The EA converts incoming UTC history request windows to MT5 broker-server time before calling `CopyRates`, then converts MT5 quote and bar timestamps back to UTC before sending data to AQE.

The EA keeps data subscriptions and trade events scoped to each AQE runtime session. Trade events are routed back to the bridge that submitted the order; manual MT5 trades are not broadcast to every strategy. Multi-strategy trade routing is reliable for hedging accounts with separate order/position tickets. MT5 netting accounts can merge same-symbol positions, so same-symbol strategy attribution is limited there.

MetaTrader serializes an EA's event handlers and `WebRequest` is synchronous. The bridge therefore does not perform history lookups, JSON construction, or HTTP inside `OnTradeTransaction`. It copies the three native MT5 transaction structures into a preallocated FIFO ring buffer and returns. A high-resolution timer expands those structures, then prioritizes one trade-event batch, market-data update, or RPC request per turn. This keeps the terminal's 1,024-entry transaction callback queue free while retaining the immutable AQE event sequence across retries. For strict isolation between several latency-sensitive strategies, one EA instance per bridge URL still provides a separate MetaTrader EA execution thread.

On AQE, Actix only performs authentication, JSON decoding, and bounded-queue admission in the request path. Ordered trade batches and market-data callbacks run on independent ingress workers, so a slow bar callback cannot delay a fill acknowledgement and a slow trade callback cannot block an RPC poll. The trade ingress queue is FIFO and never intentionally coalesces events. The market-data ingress queue is deliberately small: if AQE falls behind it responds with `503 Service Unavailable`, causing the EA to retain completed candles and retry with its newest quote snapshot instead of building an unbounded stale-price backlog. The Actix listener keeps HTTP connections alive when the MT5 client supports reuse.

Completed bars are marked delivered only after AQE acknowledges the market-data request. A timeout keeps the candle pending for retry; AQE deduplicates a retried completed candle by symbol, timeframe, and timestamp. Intrabar quotes retain MT5's millisecond tick timestamp and unchanged ticks are not emitted twice. Trade-event retries likewise retain the original event sequence, and AQE applies each batch in sequence order.

8. Keep MT5 logged in and running before starting the AQE live strategy.

If MT5 logs `initializing of AqeMt5BridgeEA failed with code 32767`, one of the EA inputs is invalid. The most common cause is an empty `InpBridgeToken`; it must be set to the same value as `AQE_MT5_BRIDGE_TOKEN`.

## Connection Health and Automatic Recovery

AQE tracks three separate connection states so an active HTTP poll is not mistaken for a healthy trading connection:

- **Transport:** an authorized EA heartbeat or RPC poll reached AQE recently.
- **Broker:** the transport is active and the MT5 terminal reports that it is connected to its broker account.
- **Data feed:** the broker state is healthy and AQE is receiving current market-data posts for its active subscriptions.

MT5 owns the broker login and reconnects its own terminal connection. AQE cannot reconnect the terminal to the broker on the user's behalf. Once MT5 reconnects, the EA heartbeat reports the transition and AQE automatically refreshes the account, open orders, and positions and restores the active bar subscriptions.

AQE also restores runtime state when authorized polling resumes after a stale interval or the EA returns with a stale runtime session. If polling remains healthy but market-data posts stop, AQE marks only the data feed as disconnected and replays the active subscriptions. The running strategy process therefore remains alive while recovery is attempted instead of silently remaining in a stale state.

The current EA heartbeat includes these diagnostic fields:

- `terminalConnected`: whether MT5 currently has a broker-server connection.
- `terminalTradeAllowed`: whether terminal and MQL trading permissions are enabled.
- `terminalName` and `accountId`: identify the terminal and account reporting the status.

Older EA builds remain protocol-compatible, but they do not report the two terminal status fields. Recompile and reload the current `AqeMt5BridgeEA.mq5` to enable broker-disconnection detection.

EA version `1.307` and later also recovers position-modification requests when AQE retained the original entry-order ticket instead of MT5's current position ticket. Resolution is attempted by exact ticket/position identifier first, then the position's historical entry-order ticket, and finally a unique routed insight id. Ambiguous insight matches are rejected rather than modifying the wrong position. Recompile and reload the EA to enable this fallback.

EA version `1.312` and later supports AQE's broker-neutral startup reconciliation. In one RPC batch, MT5 resolves persisted order ids against pending orders, currently open positions, and order/deal history. AQE cancels confirmed pending orphan orders, closes confirmed orphan positions, and applies confirmed terminal states. An unavailable or inconclusive lookup is preserved instead of being guessed as cancelled. Other broker integrations receive the same lifecycle request through `OrderManagementProvider` and can override the default active-order lookup when they expose richer position or history identity.

You can inspect the bridge without placing an order:

```bash
curl http://127.0.0.1:18080/health
```

The response includes `transportConnected`, `brokerConnected`, `datafeedConnected`, the last heartbeat, poll, market-data, subscription-sync, and recovery timestamps, the latest terminal status, and the current ordered trade and market-data ingress depths. When AQE recovers, its logs report the reconnect reason, subscription replay, and broker-state refresh result.

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
- Trailing stops are maintained by the EA timer after the related MT5 position is open.
- The EA polls AQE for work and pushes subscribed bar/trade events back to AQE.
- If the bridge disconnects, the EA continues polling and resumes once AQE is reachable again. AQE then restores subscriptions and refreshes broker state.
