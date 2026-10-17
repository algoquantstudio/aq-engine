# AQE MT5 Bridge

AQE talks to MetaTrader 5 through a local Actix Web bridge with a persistent WebSocket transport and an HTTP compatibility fallback:

- AQE starts an Actix server on `127.0.0.1:18080` by default.
- The EA prefers the persistent `GET /v2/bridge` WebSocket. Single-bridge Auto mode supports v1 HTTP fallback; multi-bridge Auto mode uses sockets only to isolate dead endpoints.
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
InpConfigVersion = 2
InpTransportMode = Auto
InpSocketReconnectBaseMs = 100
InpSocketReconnectMaxMs = 5000
InpHttpFallbackAfterMs = 5000
InpWebSocketProbeIntervalMs = 30000
InpPollIntervalMs = 50
InpRequestTimeoutMs = 5000
InpTradeEventFlushIntervalMs = 10
InpTradeEventPostTimeoutMs = 50
InpTradeEventBatchSize = 32
InpTradeEventQueueCapacity = 2048
InpMarketDataIntervalMs = 25
InpMarketDataPostTimeoutMs = 100
```

| Input | Purpose |
| --- | --- |
| `InpBridgeUrl` | Primary AQE HTTP bridge URL. It must exactly match an entry in the MT5 WebRequest allow-list. |
| `InpBridgeToken` | Shared authentication secret. It must match `AQE_MT5_BRIDGE_TOKEN` and cannot be empty. |
| `InpBridgeConnections` | Optional comma-separated additional bridge URLs for running multiple AQE strategies from one EA. |
| `InpConfigVersion` | Input-default generation. Version 2 enables the faster scheduler defaults; it lets logs distinguish old saved `.set` files from new attachments. |
| `InpTransportMode` | `Auto` prefers `/v2/bridge`, `WebSocket` disables HTTP fallback, and `HTTP` keeps the compatibility transport only. |
| `InpSocketReconnectBaseMs` | Initial reconnect delay for the persistent transport. |
| `InpSocketReconnectMaxMs` | Maximum exponential reconnect delay for the persistent transport. |
| `InpHttpFallbackAfterMs` | In `Auto`, time allowed for WebSocket negotiation before v1 HTTP starts. |
| `InpWebSocketProbeIntervalMs` | In `Auto`, interval for probing `/v2/bridge` while HTTP fallback is active. |
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

The EA automatically discovers every URL in `InpBridgeConnections`. In `Auto` mode with multiple URLs, **all endpoints, including the primary URL, use WebSocket only**. A dead primary previously issued synchronous HTTP polls that blocked healthy chart/strategy connections for roughly 14 seconds under Wine. Explicit `HTTP` mode remains available for legacy installations but cannot provide the same isolation. If an active bridge disappears, it returns to socket discovery with adaptive backoff.

AQE cannot initiate this connection because the EA exposes no listening server: the EA must discover each AQE listener. In `Auto` mode, optional URLs are discovered with the bounded `/v2/bridge` socket connection path rather than synchronous HTTP probes. This is important under Wine, where a failed `WebRequest` can block MetaTrader's serialized EA event thread for several seconds despite a small configured timeout. Select `HTTP` explicitly only when an optional endpoint must use the compatibility-only transport.

All configured bridge URLs use `InpBridgeToken`; per-endpoint tokens are not supported. Add every URL in `InpBridgeUrl` and `InpBridgeConnections` to the MT5 WebRequest allow-list.

AQE and AQS use UTC internally. The EA converts incoming UTC history request windows to MT5 broker-server time before calling `CopyRates`, then converts MT5 quote and bar timestamps back to UTC before sending data to AQE.

The EA keeps data subscriptions and trade events scoped to each AQE runtime session. Trade events are routed back to the bridge that submitted the order; manual MT5 trades are not broadcast to every strategy. Multi-strategy trade routing is reliable for hedging accounts with separate order/position tickets. MT5 netting accounts can merge same-symbol positions, so same-symbol strategy attribution is limited there.

Order events resolve their instrument from the transaction and the actual order/deal ticket, never from the EA chart. An EA attached to BTCUSD can therefore report ETHUSD/SOLUSD cancellations correctly. AQE also preserves the cached instrument for an existing ticket if an older EA sends a conflicting symbol. History notifications only confirm terminal order states; they do not emit spurious acceptance after cancellation. Broker expiration transitions an unfilled insight to Cancelled.

To deploy this correction, compile/reload this EA and rebuild the strategy with the updated AQE. Existing stale Executed insights are not repaired merely by changing the UI or reloading the EA: restart the updated runtime to run its broker-history-backed startup reconciliation. Do not manually mark missing orders cancelled; filled orders require confirmed close evidence. Schedule the restart safely because strategy teardown may close/cancel managed orders.

MetaTrader serializes an EA's event handlers and `WebRequest` is synchronous. The bridge therefore does not perform history lookups, JSON construction, socket reads, or HTTP inside `OnTradeTransaction`. It copies the three native MT5 transaction structures and the broker timestamp into a preallocated FIFO ring buffer and returns. A high-resolution timer expands those structures, then prioritizes trade events, prepared broker-command responses, one broker command, persistent transport service, completed candles, and background work. The timer has an 8 ms nonblocking work budget and performs at most one synchronous HTTP request per turn. This keeps the terminal's 1,024-entry transaction callback queue free while retaining the immutable AQE event sequence across retries. For strict isolation between several latency-sensitive strategies, one EA instance per bridge URL still provides a separate MetaTrader EA execution thread.

The `/v2/bridge` connection uses socket readability/writability checks and bounded OS-level send/read timeouts. Critical trade batches remain FIFO until AQE acknowledges their broker event sequence. Completed candles remain pending until AQE acknowledges their market-data batch. Quotes are latest-wins within the configured 25 ms cadence, **not every-tick/HFT delivery**. A single-bridge `Auto` deployment can fall back to HTTP after five seconds and probe WebSocket every 30 seconds; multi-bridge Auto never falls back to synchronous HTTP.

EA **1.414** adds `intrabarBars` to market-data batches. These are coherent MT5 shift-zero OHLC snapshots with cumulative `tick_volume`; they are not volumes inferred from sampled quotes. AQE replaces cumulative volume instead of adding it, rejects regressing snapshots, delivers final completed volume, and retains the old quote-derived fallback for older EAs. AQS forwards candle volume to the chart histogram and independently coalesces bid/ask display updates at 16 ms. AQS needs rebuilding and the updated EA needs compiling/reloading to use this path. No database migration is needed.

Buffered WebSocket frames are drained in bounded batches (32 frames / 8 ms), and market data receives a timer turn even under sustained RPC/acknowledgement traffic. `GetHistory started` and `GetHistory prepared` logs include the request ID, preparation time, and response size, distinguishing EA-side history preparation from delivery delays. Cold MT5 history downloads can still take time; one EA per bridge is recommended when strict isolation from history work is needed.

Quote-only subscriptions are restored on socket authentication and HTTP recovery, even if there are no candle consumers. Requeued requests are not necessarily undelivered: read/subscription requests may retry, but possibly executed trade mutations are not blindly replayed after a lost connection. Their outcome must be confirmed through broker state/lifecycle evidence.

Large history responses previously exceeded Actix WebSocket's default **64 KiB frame limit**, closing the socket and requeuing the request. EA 1.414 sends RFC 6455 continuation frames of at most 32 KiB; updated AQE reassembles a bounded 16 MiB message without raising the per-frame limit. Oversized RPC replies become an explicit error instead of blocking the response queue. Upgrade AQE/AQS and the EA together. The local socket regression test sends a 1,000-row fragmented history reply and verifies another request succeeds on the same connection.

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

Manual-close routing after an EA reload also depends on recovering the original
entry comment from broker history. The lookup selects history for the exact
position identifier and iterates with `HistoryDealGetTicket`; it must not call
`HistoryDealSelect` inside that loop, because that API replaces the selected
history list. Recompile and reload the updated EA to adopt this correction.
This does not replay closes missed by an older EA: those require exact broker
lifecycle reconciliation, not a synthetic close based on an absent position.
Lifecycle reconciliation now requires entry and exit deals for the exact
position, balanced traded volume, and no remaining open position before it
returns `Closed`. Entry-only, exit-only, partial or unavailable history does not
prove closure. Netting reversals (`INOUT`) remain inconclusive until their
opening/closing volume can be resolved explicitly.

EA `1.413` additionally returns `closeDetails` for conclusive closures: the
volume-weighted exit price, last broker exit-deal timestamp, net realized P&L,
commission/fees and swap. AQS migration `101` persists these fields and adds a
bounded enrichment pass for already-Closed rows missing their exit price. This
pass never reopens/reclassifies terminal insights; unavailable details remain
unknown and can be retried on a later run. Update AQE too, so the broker-neutral
lifecycle adapter preserves the new optional fields.

The isolated lookup regression test requires Node.js and a C++17 compiler and
does not connect to MT5 or place orders:

```sh
node integrations/mt5/test-entry-history.mjs
```

It executes the source lookup against simulated MT5 selection APIs, covers
parent/child manual closes, partial-close history, order-comment fallback,
unavailable history and unrelated account history, and verifies that restoring
the original destructive-selection bug fails. A MetaEditor compilation and a
controlled terminal test remain required before deploying a new EA binary.

EA version `1.312` and later supports AQE's broker-neutral startup reconciliation. In one RPC batch, MT5 resolves persisted order ids against pending orders, currently open positions, and order/deal history. AQE cancels confirmed pending orphan orders, closes confirmed orphan positions, and applies confirmed terminal states. An unavailable or inconclusive lookup is preserved instead of being guessed as cancelled. Other broker integrations receive the same lifecycle request through `OrderManagementProvider` and can override the default active-order lookup when they expose richer position or history identity.

You can inspect the bridge without placing an order:

```bash
curl http://127.0.0.1:18080/health
```

The response includes `transportConnected`, `brokerConnected`, `datafeedConnected`, the last heartbeat, poll, market-data, subscription-sync, and recovery timestamps, the latest terminal status, and the current ordered trade and market-data ingress depths. When AQE recovers, its logs report the reconnect reason, subscription replay, and broker-state refresh result.

For an explicitly authorized read-only terminal check, the dedicated ignored
test starts a temporary listener and reads account, asset, quote, current orders,
positions, latest bar and 15 minutes of BTCUSD candle history. It never starts a
strategy, reconciles orphan orders, subscribes a trading strategy or sends a
trading command. Use an unused listener address already configured in the EA;
do not stop another runtime to free its port. The listener shuts down on success,
RPC failure or its 55-second overall timeout. It does not validate fill execution
or correction delivery, which require controlled broker evidence.

```sh
AQE_MT5_BRIDGE_TOKEN=test AQE_MT5_BRIDGE_BIND_ADDR=127.0.0.1:18080 \
  cargo test -p aq-engine --features runtime,node --no-default-features --lib \
  live_mt5_read_only_account_quote_history_probe -- --ignored --nocapture
```

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

## Version 2 migration

| Setting | Legacy default | Version 2 default |
| --- | ---: | ---: |
| Poll interval | 100 ms | 50 ms |
| Poll `maxRequests` | 16 | 1 |
| Trade-event flush | 10 ms | 10 ms |
| Trade-event HTTP timeout | 100 ms | 50 ms |
| Trade-event batch size | 32 | 32 |
| Market-data interval | 50 ms | 25 ms |
| Completed-candle maximum batching delay | implicit | 10 ms |
| Timer nonblocking work budget | none | 8 ms |

Existing `.set` files keep their saved values. After recompiling the EA, reload it or reset its inputs to defaults to adopt version 2 timings. Startup logs print the normalized config version, transport mode, timer, polling, and market-data intervals. Unsafe reconnect and timing values fail initialization rather than leaving the EA in an ambiguous mode.

When troubleshooting `Auto`, look for `persistent /v2/bridge authenticated` in the Experts log. If it is absent, confirm the bridge URL is in MT5's allowed URL list and that the AQE build exposes `/v2/bridge`. HTTP fallback remains supported for one compatibility release.

## Observed execution fills for analytics

The trade-event envelope now optionally carries the exact `DEAL_ADD` execution
fact: stable broker deal/order identity, executed volume delta, deal price and
time, signed realized cash and declared account currency. Execution commission
and fees use a positive-expense/negative-rebate convention; swap remains signed.
No contract multiplier, currency conversion or account notional is inferred.
This data is independent of Insight's strategy type and does not add alpha-node
metadata to the engine primitive.

AQE journals these observations separately from cumulative position snapshots.
The optional `AQE_EXECUTION_FILL_EXPORT` gate admits bounded immutable pages to
the durable audit outbox. It is off by default and requires the matching AQS
gateway and migration 118 before rollout. Local admission is not proof of cloud
persistence or complete broker history. Readable `DEAL_UPDATE` observations now
replace the corresponding fill's cash/quantity/price evidence in the same durable
ledger. They are analytics-only: no order-cache update, trading callback, or new
Insight lifecycle transition. Both HTTP and WebSocket advertise
`executionFillRevisionVersion: 1`; the EA does not send revisions to an older
engine, including queued revisions after a capability downgrade. Revisions use
their own `/v1/execution-fill-revisions` endpoint or `ExecutionFillRevisionBatch`
WebSocket frame, so even a downgrade between handshake and delivery cannot turn
one into a legacy order event. Both receivers force analytics-only handling;
normal and correction batches preserve per-bridge FIFO order. Duplicate
delivery is harmless, while successive updates of one deal have distinct frozen
transport identities. Same-second watermarks use an in-process logical
microsecond tie-break, not a measured latency. A conflicting watermark after a
terminal restart fails closed and still requires reconciliation.

EA 1.420 adds read-only `GET_EXECUTION_FILL_HISTORY` for an exact saved entry
order. AQE pages journal-owned orders (32 at a time), including terminal Insights,
and requests at most one history read per second per strategy outside its trading
turn. One snapshot is bounded to 256 deal records and 256 Ki characters; larger
or mixed-entry/netted/reversal histories are unavailable, never truncated and
claimed complete. Admission rechecks account, unambiguous saved ownership and
original execution epoch. Retried fills are idempotent; changed observations
replace earlier economics through the existing revision ledger, not lifecycle
callbacks. Pending reads are cancelled at strategy teardown.

EA 1.424 removes the manual offset/from/through inputs. It observes broker/server
time against UTC automatically, only while connected with recent broker ticks.
A stable contiguous span establishes the constant-offset evidence window; clock
changes, account/server changes, backward time, restart or observation gaps start
a new span. Partially observed boundary seconds are excluded from historical
recovery. Every recovered deal must fall inside that span: today's offset is not
applied to unobserved older history. The journal retains clock provenance and
exported coverage remains bounded/unknown rather than claiming full recovery.
This clock evidence currently lives for the EA session; durable multi-offset
historical clock coverage remains a follow-up, not a user configuration task.
The same window gates analytics-only `DEAL_UPDATE` timestamps, which can refer to
old deals; unverified updates are withheld instead of moving historical fills to
the wrong UTC bucket. New `DEAL_ADD` events retain the current broker clock path.
MT5 history lookup itself is synchronous and a cold broker-history load can still
delay the EA thread. The AQE background task, request pacing and row/byte limits
bound application-side work; they are not a guarantee of terminal-side latency.

EA 1.421 also reports explicit `BUY_CANCELED` / `SELL_CANCELED` history records
when the caller advertises `invalidationVersion: 1`. These are tombstones, not
zero-valued fills. The journal preserves the original fact for audit, deduplicates
the cancellation and fences older delayed observations across restarts. The
desktop removes its original currency/day costs and root-`strategy_type`
attribution contribution atomically with its cursor; a strictly newer broker
observation can reinstate the fill. Unknown cancelled identities are retained as
suppression markers without inventing an economic record. Missing history rows
alone are never treated as deletions. Broker balance compensations remain
unallocated rather than being guessed as fill P&L.

Invalidation pages use transfer version 2; ordinary/legacy v1 pages remain
readable. Deploy migration 119 and v2-capable gateway/readers before enabling
their export. The migration changes only the accepted version guard and does
not rewrite evidence. An older reader/runtime fails closed on the new records.
Coverage may evolve as verified history arrives, but valuation/account identity
must not change inside a source.

Raw `DEAL_DELETE` events with no readable deal, larger/netted histories and complete
broker-history/account reconciliation remain outstanding. `recovery_complete`
stays unknown; a successful bounded lookup does not promote overall coverage to
complete. Existing closed-position lifetime accounting must not be added to
these fill values a second time.

Recompile in MetaEditor and run a controlled demo-terminal validation before
deploying a new EA binary. The source-level fill regression runs without MT5 or
broker actions, but does not replace that release gate.

Validation of the bounded recovery slice (2026-10-12): 47 MT5 adapter tests,
10 fill-journal tests, one paced-read/cancellation test and 11 native fill-analysis
tests passed. The executable MQL-body shim checks complete partial-exit traversal,
exact account/order/symbol scope, unknown/out-of-window clocks, mixed entries,
reversals, cancelled deals and oversized histories. The real-terminal `test`-token
read-only account/quote/candle probe passed before this history extension; it did
not test EA 1.420's history RPC. No live order mutation, terminal installation,
database migration or export-gate activation was performed. MetaEditor compilation
and a configured-clock read-only historical canary remain release gates. This is
Phase 0/1 evidence work, not completion of the full analysis specification.

Validation of the invalidation slice (2026-10-12): 13 fill-journal tests,
13 native fill-analysis tests, the exact-history adapter test, two delivery
tests and the gateway fill-admission test passed. An isolated memory-only SurrealDB test verifies idempotent
migration 119, mixed v1/v2 storage, tenant/scope checks and critical-cursor
isolation. The executable MQL-body shim verifies capability-gated cancellations
without emitting a second economic fill. The full journal → frozen transfer →
chart-cache regression covers cancellation, replay, restart, ticker filtering,
costs, root attribution and later reinstatement. MetaEditor compilation and a
configured-clock terminal canary for EA 1.421 are still required; these changes
were not installed or applied to the user's database automatically.

## Current limits

Since EA 1.422, a capability-gated `positionSnapshotVersion: 1` observation is added
to the existing owned fill-history read. It scans the bounded open-position list
by the exact MT5 position identifier, before and after reading deals. Failed
position reads, a changing quantity/side/update timestamp, or an account change
make the read unavailable; they never imply a flat position. A successfully
observed absence is explicit zero quantity with no side. This does not infer
ownership of other positions on the account.

AQE compares that observation with the active fill prefix and durably retains
the latest position-quantity check alongside the journal, with the original
owner/epoch and ledger sequence. Both matches and mismatches are diagnostics;
neither can submit or modify orders. New fills/corrections/invalidations remove
the previous check. Repeated unchanged history refreshes advance its observation
time without appending duplicate fills; an older history or one omitting known
deals cannot certify the current ledger. Legacy adapters lacking the optional
snapshot remain readable and have no position check.

Position checks now travel as non-economic records in version 3 of the existing
bounded execution-fill audit feed. Fill and comparison records share one ordered
cursor; a refresh with no new fill can deliver a check-only page. The journal
freezes exact transfer bytes across retry/restart. Corrections revoke old checks
without adding economic fills or changing cash totals. Older version 1/2 pages
remain readable; older consumers must reject version 3 rather than skip evidence.

The native cache incrementally retains the latest comparison for each exact
provider/position and fences owner, account, source, epoch and ticker. The live
Analysis view reuses its existing fill resource and refresh cadence, with
matched/mismatched/check-required counts, the newest 50 comparison rows, explicit
UTC observation times and copyable provenance. Counts cover all retained checks,
not just rendered rows. The cache refuses capacity overflow atomically instead
of silently evicting evidence. This is not account cash/FX/mark reconciliation;
`recovery_complete` remains unknown.

Deployment requires the version-3-capable gateway/desktop and migration 120 before
enabling the existing export gate. Neither the user's database nor gate was
changed automatically. EA source 1.424 still requires MetaEditor compilation and
a controlled automatically-clocked read-only terminal canary; the running EA binary
has not been overwritten.

After operator compilation/reload and automatic clock observation, use an explicit
authorized historical entry-order ID and its symbol with the dedicated canary:

```bash
AQE_MT5_BRIDGE_TOKEN='<configured token>' \
AQE_MT5_BRIDGE_BIND_ADDR='<configured private bind address>' \
AQE_MT5_HISTORY_ENTRY_ORDER_ID='<entry order ID>' \
AQE_MT5_HISTORY_SYMBOL='<symbol>' \
cargo test -p aq-engine --features runtime,node --no-default-features --lib \
  live_mt5_read_only_owned_history_position_probe -- --ignored --nocapture
```

This probe starts only a temporary bridge, reads the account and the selected
position history, checks the position quantity and shuts down. It does not start
a strategy, admit evidence to a journal or send trading commands. An unavailable
clock, old EA without the new snapshot, ambiguous history or quantity mismatch
fails the canary. The general account/quote/candle probe is not a substitute.

Validation for the position-quantity foundation: 19 fill-journal tests and 47
MT5 bridge tests pass; the two real-terminal probes remain explicitly ignored in
the ordinary suite. The executable MQL-body regression passes (including failure,
disconnect and changing-position cases), and the stream-isolation check passes
all eight assertions. Quantity tests include partial short exits, simultaneous
fills, negative inventory prefixes and small nonzero positions. These checks do
not replace a MetaEditor build or validate the running terminal's EA version.

The version-3 delivery slice also passed protocol/gateway validation, a disposable
memory-only SurrealDB migration test and the production native reader test. Native
cache regressions cover restart, corrections, revocation, full-population counts,
bounded rendering and retention reset; EN/FR component rendering and WASM checking
pass. Dark and narrow light-theme layout previews keep long tables inside the card.
The general read-only terminal probe was rerun successfully with the authorized
test token (account, quote, bar, 15 recent minute bars, orders and positions; no
trading commands). Installed source still reported 1.420, so this does **not**
validate the new 1.422 quantity capability. Phase 0/1 remain in progress.

## Broker-neutral valuation foundation (EA 1.424)

`PositionValuationReader` is optional on the generic execution provider and is
implemented by both MT5 and PaperBroker. Backtests still execute through
PaperBroker; live execution can use either provider, independently of the data
feed. No Insight strategy-instance/node fields are introduced.

The read contract accepts at most 32 exact physical position IDs for one account
and symbol. It separates open quantity/side, gross unrealized P&L, carry, price
timestamp evidence and notional basis. An observed missing position is `not_open`,
not a fabricated close fill. An unavailable read is not an empty portfolio.

MT5's new `GET_POSITION_VALUATIONS` RPC is read-only, uses two bounded position
passes and rejects scope or position/quote changes during the read. Linear
contracts can report price × quantity × contract size when their price currency
already matches account currency. Unsupported contracts, missing metadata and
missing FX conversion remain explicit. `OrderCalcProfit` is not misused as a
generic account-notional calculator. Latest marks use automatically observed
clock evidence; there are no manual UTC-offset or validity-window inputs.

PaperBroker returns the same contract using its own completed execution step,
per-order positions and UTC bar timestamps. Its notional is explicitly
`paper_price_units`, matching the current paper engine's accounting; live MT5
contract multipliers/FX are not silently imposed on simulated executions. Missing
symbol bars retain their old mark time and become stale. Bounded reads cannot see
a half-completed step, and no analytics read submits or modifies an order. Sparse
live updates also retain other symbols' open P&L in paper account equity.

This is the producer/valuation contract, not a completed new chart delivery path.
Scheduled durable mark collection, export/cache projections and shared live-chart
integration remain to be connected for both providers. Existing saved backtest
fill/cash producers and reconstruction remain intact. Automatic historical clock
evidence is currently session-local, so pre-session history remains unavailable
without independent evidence; no current offset is guessed for those records.

Validation: shared protocol tests, exact-scope MT5 adapter tests, three paper
valuation regressions and the executable MQL-body suite (35 valuation cases plus
automatic clock transitions) pass. The general `test`-token real-terminal
account/quote/history probe and absent-ID valuation RPC compatibility canary both
passed read-only. There were no open terminal positions, so real open-position
economic values and quote-clock coverage have not been verified by those probes.
Broader broker regressions found three unrelated recovery-test corruption
injections rejected by JSON validation;
these are outside the valuation path and have not been changed here. No production
database migration, strategy start or broker order mutation was performed.

Read-only valuation compatibility canary (an absent-ID request by default):

```bash
AQE_MT5_BRIDGE_TOKEN='<configured token>' \
AQE_MT5_BRIDGE_BIND_ADDR='<configured private bind address>' \
cargo test -p aq-engine --features runtime,node --no-default-features --lib \
  live_mt5_read_only_position_valuation_probe -- --ignored --nocapture
```

Set `AQE_MT5_VALUATION_POSITION_ID` and `AQE_MT5_VALUATION_SYMBOL` to an explicit
authorized open position to inspect an actual mark. The default absent-ID canary
only validates RPC compatibility, not quote freshness, economic values or clock
coverage. Neither probe invokes a strategy or sends trading commands.

- MT5 is live-only in v1. Use Paper/Yahoo for backtests.
- Bracket orders map to MT5 TP/SL values where possible.
- Trailing stops are maintained by the EA timer after the related MT5 position is open.
- `wss://` is not currently used for the loopback bridge. Keep the listener private and use `http://`/`ws://` on the execution machine.
- If the persistent bridge disconnects, the EA reconnects with exponential backoff and uses HTTP fallback in `Auto`. AQE then restores subscriptions and refreshes broker state.
