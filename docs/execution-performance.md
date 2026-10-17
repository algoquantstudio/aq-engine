# Execution performance example

From the workspace root:

```sh
cargo run -p aq-engine --release --features runtime --example execution_performance -- --output /tmp/execution-performance.json
```

For a quick correctness check (one 32-order sample, no warm-up or control workloads):

```sh
cargo run -p aq-engine --release --features runtime --example execution_performance -- --smoke --output /tmp/execution-smoke.json
```

The default suite reuses `engine_performance.rs`'s exact three control workloads:
250,000 bars through four indicators, 20,000 insights through three configuration/
validation stages, and 5,000 direct PaperBroker submissions/fills. It adds 5,000
market insights through the real `StrategyState::run_backtest` execution path.
There are three warm-ups and fifteen measured samples, with median/p95 elapsed
time and median-derived throughput in the JSON report.

The additional test uses a synthetic minute-bar feed and PaperBroker. It exercises
submission deferral, bounded command admission, the command worker, durable
execution journals/checkpoints, broker evidence reduction, Filled-state pipeline
execution, and normal backtest teardown. A Filled-state observer asserts that every
unique requested insight received an order ID and a positive entry fill price.
No network, AQS, MT5, or live credentials are needed.

The measured interval covers the entire `run_backtest` call, including connection,
history loading, execution journal creation, result generation, and teardown.
Fixture construction, final assertions, and deletion of the sample's private temp
directory are excluded. Every sample gets a fresh directory and journal; normal
teardown completes before the directory is removed. Samples time out after five
minutes instead of reporting incomplete execution as successful throughput.

`execution_backtest` orders/s is **not equivalent work** to `paper_broker` orders/s:
the latter bypasses the journal and strategy runtime. Compare matching result IDs
across revisions on the same hardware, storage, toolchain, and build settings.
The smoke result is not a statistically meaningful performance baseline. Neither
test measures live broker latency or AQS delivery/render latency. Avoid running
other builds or intensive workloads during measurements.
