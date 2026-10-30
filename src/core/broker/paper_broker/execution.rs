//! Paper executions are exact deltas produced at settlement, not inferred from
//! cumulative order snapshots or Insight lifecycle states.

use super::*;
use crate::core::broker::{
    fill_history::{
        ExecutionFillHistory, ExecutionFillHistoryReader, ExecutionHistoryClock, MAX_HISTORY_FILLS,
        PositionQuantitySnapshot,
    },
    types::{BrokerClockDomain, BrokerEventMetadata},
};
use aqe_live_protocol::{
    EXECUTION_FILL_VERSION, ExecutionFill, ExecutionFillCash, ExecutionFillEffect,
    ExecutionFillSide,
};

const PROVIDER: &str = "paper-price-units-v1";
const MAX_RETAINED_POSITIONS: usize = 1024;
const MAX_RETAINED_FILLS: usize = 4096;

/// Optional, bounded recovery evidence, separate from OMS state. Evict completed
/// positions first. If an active history cannot fit, omit its whole history;
/// never return a truncated prefix as a complete position comparison.
#[derive(Default)]
pub(super) struct PaperExecutionHistory {
    positions: HashMap<String, Vec<ExecutionFill>>,
    closed: VecDeque<String>,
    rows: usize,
}
impl PaperExecutionHistory {
    fn remove(&mut self, id: &str) {
        if let Some(rows) = self.positions.remove(id) {
            self.rows -= rows.len();
        }
    }
    fn make_room(&mut self, new_position: bool) -> bool {
        while self.rows >= MAX_RETAINED_FILLS
            || (new_position && self.positions.len() >= MAX_RETAINED_POSITIONS)
        {
            let Some(id) = self.closed.pop_front() else {
                return false;
            };
            self.remove(&id);
        }
        true
    }
    fn record(&mut self, fill: &ExecutionFill, closed: bool) {
        let Some(id) = fill.position_id.as_deref() else {
            return;
        };
        let entry = fill.effect == ExecutionFillEffect::Entry;
        if !entry && !self.positions.contains_key(id) {
            return;
        }
        if self
            .positions
            .get(id)
            .is_some_and(|rows| rows.len() >= MAX_HISTORY_FILLS)
            || !self.make_room(entry && !self.positions.contains_key(id))
        {
            self.remove(id);
            return;
        }
        self.positions
            .entry(id.into())
            .or_default()
            .push(fill.clone());
        self.rows += 1;
        if closed {
            self.closed.push_back(id.into());
        }
    }
}

impl PaperBroker {
    pub(super) fn entry_execution(
        &self,
        order: &Order,
        price: f64,
        at: DateTime<Utc>,
    ) -> ExecutionFill {
        ExecutionFill {
            version: EXECUTION_FILL_VERSION,
            provider: PROVIDER.into(),
            fill_id: Uuid::new_v4().to_string(),
            order_id: order.order_id.clone(),
            position_id: Some(order.order_id.clone()),
            symbol: order.asset.symbol.clone(),
            side: match order.side {
                OrderSide::Buy => ExecutionFillSide::Buy,
                OrderSide::Sell => ExecutionFillSide::Sell,
            },
            effect: ExecutionFillEffect::Entry,
            quantity: order.filled_qty,
            price,
            occurred_at_us: at.timestamp_micros(),
            as_of_us: at.timestamp_micros(),
            cash: Some(ExecutionFillCash {
                currency: self.account.read().currency.clone(),
                gross_pnl: Some(0.),
                commission: order.commission,
                carry: Some(0.),
            }),
            // Paper intentionally settles price units, regardless of the
            // real instrument's multiplier/FX. Match its actual account math.
            account_notional: Some((order.filled_qty * price).abs()),
        }
    }
    pub(super) fn exit_execution(
        &self,
        entry: &Order,
        exit_order_id: &str,
        price: f64,
        at: DateTime<Utc>,
        result: ClosePositionResult,
    ) -> ExecutionFill {
        let mut fill = self.entry_execution(entry, price, at);
        fill.order_id = exit_order_id.into();
        fill.side = match entry.side {
            OrderSide::Buy => ExecutionFillSide::Sell,
            OrderSide::Sell => ExecutionFillSide::Buy,
        };
        fill.effect = ExecutionFillEffect::Exit;
        fill.quantity = result.qty;
        fill.cash = Some(ExecutionFillCash {
            currency: self.account.read().currency.clone(),
            gross_pnl: Some(result.gross_pnl),
            commission: Some(result.exit_commission),
            carry: Some(result.swap),
        });
        fill.account_notional = Some((result.qty * price).abs());
        fill
    }
    pub(super) fn emit_execution_event(
        &self,
        order: &Order,
        event: TradeUpdateEvent,
        fill: Option<ExecutionFill>,
    ) {
        let fill = fill.filter(|fill| {
            if let Err(error) = fill.validate() {
                log::warn!("Paper execution analytics unavailable: {error}");
                false
            } else {
                true
            }
        });
        if self.backtest_state.is_none() {
            if let Some(fill) = &fill {
                self.execution_history
                    .lock()
                    .record(fill, event == TradeUpdateEvent::Closed);
            }
        }
        let envelope =
            BrokerOrderEvent::new((order.clone(), event)).with_metadata(BrokerEventMetadata {
                execution_fill: fill.map(Box::new),
                source_occurred_at_us: Some(self.current_time.read().timestamp_micros()),
                adapter_queued_at_us: Some(Utc::now().timestamp_micros()),
                source_clock: BrokerClockDomain::RuntimeHost,
                ..Default::default()
            });
        self.trade_events.lock().push_back(envelope.clone());
        // Callbacks may unsubscribe themselves; never invoke one under the
        // subscriber lock. Queue and stream reuse the same event/fill identity.
        let subscribers = self.trade_stream_subscribers.lock().clone();
        for sub in subscribers {
            sub(envelope.clone());
        }
    }
    pub(super) fn drain_execution_events(&self, max_events: usize) -> Vec<BrokerOrderEvent> {
        let mut queue = self.trade_events.lock();
        let count = max_events.min(queue.len());
        let events = queue.drain(..count).collect();
        let drained = queue.is_empty();
        drop(queue);
        if drained && count != 0 {
            self.release_terminal_orders();
        }
        events
    }
}

#[async_trait::async_trait]
impl ExecutionFillHistoryReader for PaperBroker {
    async fn read(
        &self,
        account_id: &str,
        entry_order_id: &str,
        symbol: &str,
    ) -> Result<Option<ExecutionFillHistory>, BrokerError> {
        let Some(_guard) = self.valuation_gate.try_read() else {
            return Ok(None);
        };
        if !self.is_connected() || self.backtest_state.is_some() {
            return Ok(None);
        }
        if self.account.read().account_id != account_id {
            return Err(BrokerError::TradeError(
                "paper history account mismatch".into(),
            ));
        }
        let Some(as_of_us) = *self.valuation_as_of_us.read() else {
            return Ok(None);
        };
        let Some(mut fills) = self
            .execution_history
            .lock()
            .positions
            .get(entry_order_id)
            .cloned()
        else {
            return Ok(None);
        };
        if fills
            .iter()
            .any(|fill| fill.symbol != symbol || fill.occurred_at_us > as_of_us)
        {
            return Err(BrokerError::TradeError(
                "paper history symbol/clock mismatch".into(),
            ));
        }
        for fill in &mut fills {
            fill.as_of_us = as_of_us;
        }
        let (quantity, side) = self
            .positions
            .get(symbol)
            .and_then(|positions| {
                positions.get(entry_order_id).map(|p| {
                    (
                        p.qty,
                        Some(match p.side {
                            OrderSide::Buy => ExecutionFillSide::Buy,
                            OrderSide::Sell => ExecutionFillSide::Sell,
                        }),
                    )
                })
            })
            .unwrap_or((0., None));
        let history = ExecutionFillHistory {
            account_id: account_id.into(),
            entry_order_id: entry_order_id.into(),
            position_id: entry_order_id.into(),
            symbol: symbol.into(),
            as_of_us,
            clock: ExecutionHistoryClock::Utc,
            fills,
            invalidations: vec![],
            position_snapshot: Some(PositionQuantitySnapshot {
                position_id: entry_order_id.into(),
                symbol: symbol.into(),
                quantity,
                side,
                as_of_us,
            }),
        };
        history.validate(account_id, entry_order_id, symbol)?;
        Ok(Some(history))
    }
}

#[cfg(test)]
mod tests;
