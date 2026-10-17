# Data-provider quote subscriptions

`DataProvider` now exposes independent quote leases, forwarded by `UnifiedBroker`:

```rust,ignore
use aq_engine::core::broker::{DataFeedFeature, traits::DataProvider};
use std::sync::Arc;

if broker.has_feature(DataFeedFeature::QuoteStream) {
    broker.subscribe_to_quote_stream(
        "my-unique-consumer-id".into(), "EURUSD".into(),
        Arc::new(|quote| { /* enqueue bounded work; do not block */ }),
    ).await?;
    // Later: only this consumer is removed, not candle or other quote subscriptions.
    broker.unsubscribe_from_quote_stream("my-unique-consumer-id").await?;
}
```

Capabilities are typed flags, not deployment/environment requirements:

| Provider/mode | QuoteStream | NativeBidAsk | SyntheticQuotes |
| --- | --- | --- | --- |
| MT5 live (including paper execution using MT5 data) | Yes | Yes | No |
| Yahoo live/paper | Yes, polled | No | Yes |
| UnifiedBroker backtest replay | Yes, at consumed bars | No | Yes |
| Unimplemented providers | No by default | No | No |

MT5 streams sampled broker bid/ask independently of candle timeframes. Yahoo polls minute-bar data on its existing 10-second cadence and emits bid=ask=close, with the source bar timestamp. This is not a real spread, not an executable quote and not a real-time exchange feed. Synthetic sizes are zero/unknown; candle volume is never represented as last-trade size. AQS labels synthetic quotes and does not draw a fake bid/ask spread.

Backtests never fetch live quotes: callbacks and `get_latest_quote` use only consumed replay bars, before advancing to the next step. Paper execution in live mode forwards to its selected data provider. This does **not** add a strategy `on_quote` event or tick-driven backtest execution.

Lease IDs must be unique per owner. Unsubscribe is idempotent; provider disconnect/drop stops Yahoo polling tasks. Live rolling Yahoo windows bypass the immutable backtest cache to prevent long-running polling from retaining every fetched window.

Callbacks must remain short/nonblocking. This API is groundwork for future quote events, not a guarantee of HFT latency, every-tick delivery, or lossless quote replay. MT5's default sampling interval is 25 ms; AQS's 16 ms latest-value batching applies only to display delivery, not provider callbacks. No NATS/AQS dependency is introduced for standalone AQE consumers.
