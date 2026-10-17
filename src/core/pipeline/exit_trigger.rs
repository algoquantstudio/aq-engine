//! Shared decision boundary for the built-in protections. Broker mutations and
//! receipt consumption remain in the owning pipes, outside this read helper.
use crate::core::{
    broker::{
        DataFeedFeature,
        snapshots::{SnapshotKind, SnapshotUnavailable},
        types::{BrokerError, OrderSide},
    },
    insight::Insight,
    strategy::{StrategyContext, StrategyMode},
};

#[derive(Clone, Copy)]
pub(super) enum ExitTrigger {
    StopLoss,
    TakeProfit,
}

impl ExitTrigger {
    pub fn reached(
        self,
        ctx: &dyn StrategyContext,
        insight: &Insight,
        level: f64,
    ) -> Result<bool, BrokerError> {
        // Long exits sell at bid; short exits buy at ask. Do not use last/mid,
        // the entry side of the spread, or an earlier candle's pre-fill extrema.
        let rising = matches!(
            (self, &insight.side),
            (Self::StopLoss, OrderSide::Sell) | (Self::TakeProfit, OrderSide::Buy)
        );
        let price = if ctx.mode() == StrategyMode::Live
            && ctx.has_datafeed_feature(DataFeedFeature::NativeBidAsk)
        {
            let quote = ctx.latest_quote(&insight.symbol)?;
            if quote.symbol != insight.symbol || !quote.valid_market_values() {
                return Err(BrokerError::SnapshotUnavailable {
                    kind: SnapshotKind::Quote {
                        symbol: insight.symbol.clone(),
                    },
                    reason: SnapshotUnavailable::Invalid,
                });
            }
            Some(match insight.side {
                OrderSide::Buy => quote.bid,
                OrderSide::Sell => quote.ask,
            })
        } else {
            // Preserve OHLC semantics for deterministic backtests and bar-only
            // feeds. Read the final scalar directly, without cloning a tail frame.
            let history = ctx
                .history()
                .get(&insight.symbol)
                .ok_or_else(|| BrokerError::DataFeedError("No history found".into()))?;
            history.height().checked_sub(1).and_then(|last| {
                history
                    .column(if rising { "high" } else { "low" })
                    .ok()?
                    .f64()
                    .ok()?
                    .get(last)
            })
        };
        Ok(price
            .filter(|price| price.is_finite())
            .is_some_and(|price| {
                if rising {
                    price >= level
                } else {
                    price <= level
                }
            }))
    }
}

#[cfg(test)]
mod tests;
