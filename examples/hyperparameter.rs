//! Run with either:
//!
//! ```text
//! cargo run -p aq-engine --example hyperparameter --features runtime -- --hyper-sweep
//! cargo run -p aq-engine --example hyperparameter --features runtime -- --hyper-seed <seed>
//! cargo run -p aq-engine --example hyperparameter --features runtime -- --live --hyper-seed <seed>
//! ```
//!
//! Without either hyperparameter flag this runs once using the ordinary fallback values in the
//! strategy. Live mode accepts one optional `--hyper-seed`; a full sweep creates one fresh state
//! per seed before calling the normal `StrategyState::run_backtest` interface.

use aq_engine::core::alpha::{EmaPriceCrossover, WrappedAlphaModel};
use aq_engine::core::pipeline::insight_submit::InsightSubmitPipe;
use aq_engine::core::pipeline::{InsightPipe, InsightPipeResult, WrappedInsightPipe};
use aq_engine::prelude::*;
use std::collections::HashSet;

struct HyperEmaStrategy;

fn build_state(
    timeframe: TimeFrame,
    mode: StrategyMode,
) -> StrategyState<HyperEmaStrategy, PaperBroker, YahooFinanceDataFeed> {
    let broker = UnifiedBroker::new(
        PaperBroker::new(AccountType::Paper, 100_000.0, 1),
        YahooFinanceDataFeed::new(),
    );
    let mut state = StrategyState::new(
        "Hyperparameter EMA crossover".to_string(),
        "1.0.0".to_string(),
        HyperEmaStrategy,
        broker,
        mode,
        timeframe,
    );
    state.set_artifact_root(env!("CARGO_MANIFEST_DIR"));
    state
}

/// Minimal sizing stage for this example. It runs before `InsightSubmitPipe`, so every New
/// insight has one whole AAPL share when the submit stage creates its paper-broker order.
struct FixedQuantityPipe;

impl InsightPipe for FixedQuantityPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, _ctx: &mut dyn StrategyContext, insight: &mut Insight) -> InsightPipeResult {
        insight.set_quantity(Some(1.0));
        InsightPipeResult::new(
            true,
            true,
            Some("Set quantity to 1".to_string()),
            self.name().to_string(),
        )
    }
}

impl Strategy for HyperEmaStrategy {
    fn name(&self) -> &str {
        "Hyperparameter EMA crossover"
    }

    fn on_start(&mut self, ctx: &mut dyn StrategyContext) {
        // `hyper_int` reads the active run when AQE receives --hyper-sweep or --hyper-seed.
        // The literal fallbacks make an ordinary run behave exactly like a non-hyper strategy.
        let atr_period =
            aq_engine::core::strategy::hyperparameters::hyper_int(ctx, "atr_period", 14_usize);
        let ema_period =
            aq_engine::core::strategy::hyperparameters::hyper_int(ctx, "ema_period", 21_usize);
        ctx.add_alpha(
            WrappedAlphaModel::builder(Box::new(EmaPriceCrossover::new(
                atr_period,
                ema_period,
                String::new(),
            )))
            .build(),
        );
        ctx.add_pipe(WrappedInsightPipe::builder(Box::new(FixedQuantityPipe)).build());
        ctx.add_pipe(WrappedInsightPipe::builder(Box::new(InsightSubmitPipe::new())).build());
    }

    fn init(&mut self, _ctx: &mut dyn StrategyContext, _asset: &Asset) {}

    fn universe(&self, _ctx: &mut dyn StrategyContext) -> HashSet<String> {
        ["AAPL".to_string()].into_iter().collect()
    }

    fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str, _bar: &BarData) {}
    fn generate_insights(&mut self, _ctx: &mut dyn StrategyContext, _symbol: &str) {}
    fn insight_pipeline(&mut self, _ctx: &mut dyn StrategyContext, _insight: &Insight) {}
    fn on_teardown(&mut self, _ctx: &mut dyn StrategyContext) {}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut hyperparameters = HyperParameterConfig::new();
    hyperparameters
        .set_sweep_id("ema-crossover-example-v1")
        .add_hyper_parameter(HyperParameter::new("atr_period", 14).values([10, 14, 20]))?
        .add_hyper_parameter(HyperParameter::new("ema_period", 21).range(10.0, 30.0, 5.0))?;

    let timeframe = TimeFrame::new(1, TimeFrameUnit::Day);
    let args = std::env::args().collect::<Vec<_>>();
    if args.iter().any(|argument| argument == "--live") {
        let mut state = build_state(timeframe, StrategyMode::Live);
        state.set_hyper_parameter_config(hyperparameters);
        state.run_live(None).await?;
        return Ok(());
    }

    let is_sweep = hyperparameters.is_sweep_requested(&args);
    let selections = hyperparameters.process_runs(&args)?;
    let mut results = Vec::new();
    for selection in selections {
        let mut state = build_state(timeframe.clone(), StrategyMode::Backtest);
        state.set_hyper_parameter_config(hyperparameters.clone());
        if let Some(selection) = selection.as_ref() {
            state.set_hyper_parameter_run(selection, is_sweep)?;
        }
        results.push(
            state
                .run_backtest(
                    Utc::now() - chrono::Duration::days(180),
                    Utc::now(),
                    timeframe.clone(),
                )
                .await?,
        );
    }

    println!("Completed {} backtest run(s).", results.len());
    Ok(())
}
