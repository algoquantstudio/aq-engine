# AQE runtime TUI

Build the strategy with AQE's optional `tui` feature. The TUI activates when stdout and stderr are terminals; `--no-tui` disables it. Redirected AQS output/non-terminal execution remains headless. No new server or database migration is required.

## Views and controls

- `Tab` / `Shift+Tab`: cycle Metrics, Insights, Inspector, Variables, Watch, Events.
- `m`, `i`, `d`, `v`, `e`: focus the corresponding pane (`d` opens Inspector focus).
- `z`: expand the focused pane; `Esc` leaves expanded mode. Terminals below 120 columns or 30 rows automatically show the focused pane instead of squeezing three columns together.
- `s`: runtime/sync diagnostics. `s` or `Esc` returns; arrows/PageUp/PageDown scroll.
- Inspector: arrows/PageUp/PageDown scroll wrapped details and recent history; Home returns to the top. Selection follows insight ID through updates/reordering.
- Insights: arrows select; PageUp/PageDown jump ten rows.
- Variables: Enter/Right opens children; Left/Backspace returns. `w` toggles a watch; Delete removes a focused watch.
- Events: arrows/PageUp/PageDown scroll; Home returns to newest events; End shows older retained entries.
- `q`: requests the existing graceful strategy shutdown. Once Stopped/Completed, `q` closes the TUI. No force-kill or broker actions are added. Outside diagnostic/expanded mode, Esc keeps the existing stop/close behavior.

## Updated information

Backtests retain their progress gauge. Live runs show snapshot freshness, a separately labelled market clock, and counts by lifecycle state. Metrics include equity, return, closed trades, win rate, drawdown, profit factor, expectancy, open positions/insights, and unrealized P&L.

The active preview contains the latest-updated 128 active insights and explicitly reports displayed/total counts. Details include parent and broker order IDs, entry/exit fields and timestamps, and the latest 25 history entries labelled with their history kind. This is a bounded live preview, not a complete history browser; terminal records and archived history remain in the persistence/UI systems.

Diagnostics show runtime/run IDs, the embedded/managed/in-process delivery path, queued critical events, coalesced telemetry entities, cumulative acceptance/deduplication/emergency/failure counters, and the last local ACK latency. Samples come from cached publisher counters on the existing heartbeat; no database queries, remote requests, or broker polling are added to the strategy loop. Advertised provider capabilities include quote streaming/lifecycle lookup where supported; capability does not imply a subscription is currently active.

**Local acceptance is not confirmation of cloud persistence or dashboard rendering.** The TUI explicitly reports remote outbox/DLQ/reconciliation as unobserved rather than claiming they are empty or caught up. Use runtime-host/gateway health for those worker-owned measurements. Snapshot, metrics, and publisher sample ages expose stale observations. No credentials or authenticated endpoints are added to diagnostics.

## Long-run bounds and testing

Log rings retain 256 runtime events and 512 pending logger events. Each captured line is capped at 4 KiB, including unterminated lines; ANSI control sequences are stripped and ring eviction counts are visible. Variable previews have per-value bounds of 256 nodes, 64 children, five levels, and 160-character scalar values; at most 64 top-level variables are summarized. Insight detail/history is cloned only after selecting the bounded preview, with history messages capped at 512 characters.

The renderer is independent of terminal input and covered by Ratatui TestBackend tests, including small/large terminals, both run modes, sync semantics, selection stability, scrolling, and memory bounds:

```sh
cargo test -p aq-engine --features tui core::tui --offline
cargo check -p aq-engine --no-default-features --features runtime --offline
```

See [Ratatui's backend/testing documentation](https://ratatui.rs/concepts/backends/). Tests use synthetic snapshots and do not connect a broker or start a live strategy.
