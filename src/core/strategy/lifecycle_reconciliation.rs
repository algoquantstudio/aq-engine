//! Bounded, read-only broker history reconciliation for locally owned insights.
//! Active-order absence is never terminal evidence. The owner applies confirmed
//! results through the same reducer as live broker events.
use super::*;
use crate::core::broker::types::{OrderLifecycleSnapshot, OrderLifecycleStatus, TimeInForce};
use std::sync::Arc;
use std::time::Duration;

pub(super) const BATCH_SIZE: usize = 32;

pub(super) struct LifecycleCheck {
    requests: tokio::sync::mpsc::Sender<Vec<String>>,
    pub results: tokio::sync::mpsc::Receiver<Vec<OrderLifecycleSnapshot>>,
    pub busy: bool,
    after: Option<Uuid>,
}

impl LifecycleCheck {
    pub fn start(
        reconciler: Arc<dyn crate::core::broker::traits::OrderLifecycleReconciler>,
        tasks: &mut tokio::task::JoinSet<()>,
    ) -> Self {
        let (requests, mut receive) = tokio::sync::mpsc::channel::<Vec<String>>(1);
        let (send, results) = tokio::sync::mpsc::channel(1);
        tasks.spawn(async move {
            while let Some(ids) = receive.recv().await {
                let result = tokio::time::timeout(Duration::from_secs(10), reconciler.reconcile(&ids)).await;
                let snapshots = match result {
                    Ok(Ok(rows)) => rows.into_iter().filter(|row| ids.contains(&row.order_id)).take(BATCH_SIZE).collect(),
                    Ok(Err(error)) => {
                        log::warn!("Local broker lifecycle reconciliation unavailable: {error}");
                        Vec::new()
                    }
                    Err(_) => {
                        log::warn!("Local broker lifecycle reconciliation timed out; preserving active insights");
                        Vec::new()
                    }
                };
                if send.send(snapshots).await.is_err() { break; }
            }
        });
        Self {
            requests,
            results,
            busy: false,
            after: None,
        }
    }

    pub fn request(&mut self, insights: &InsightCollection) {
        if self.busy {
            return;
        }
        let Some(upper) = insights.active_insight_upper_bound() else {
            self.after = None;
            return;
        };
        let mut page = insights.active_insight_page(self.after, upper, BATCH_SIZE);
        if page.is_empty() && self.after.take().is_some() {
            page = insights.active_insight_page(None, upper, BATCH_SIZE);
        }
        self.after = page.last().copied();
        let orders: Vec<_> = page
            .into_iter()
            .filter_map(|id| insights.get(&id))
            .filter_map(|row| row.order_id.clone())
            .filter(|id| !id.is_empty())
            .collect();
        if !orders.is_empty() {
            self.busy = self.requests.try_send(orders).is_ok();
        }
    }
}

fn terminal_order(
    insight: &Insight,
    asset: &Asset,
    snapshot: &OrderLifecycleSnapshot,
) -> Option<Order> {
    if (insight.state.is_inactive() && !(insight.state == InsightState::Closed
        && snapshot.status == OrderLifecycleStatus::Closed
        && snapshot.close_details.as_ref().is_some_and(|close|
            close.validated_position_accounting(&snapshot.order_id).is_some())))
        || insight.order_id.as_deref() != Some(snapshot.order_id.as_str())
        || snapshot
            .symbol
            .as_deref()
            .is_some_and(|symbol| symbol != insight.symbol)
    {
        return None;
    }
    let (event, close) = match snapshot.status {
        OrderLifecycleStatus::Cancelled | OrderLifecycleStatus::Expired => {
            (TradeUpdateEvent::Cancelled, None)
        }
        OrderLifecycleStatus::Rejected => (TradeUpdateEvent::Rejected, None),
        OrderLifecycleStatus::Closed => {
            let close = snapshot.close_details.as_ref()?;
            if !close.close_price.is_finite()
                || close.close_price <= 0.0
                || !close.realized_pnl.is_finite()
                || !close.commission.is_finite()
                || !close.swap.is_finite()
                || close.closed_at > chrono::Utc::now()
                || insight.filled_at.is_some_and(|at| close.closed_at < at)
                || (insight.filled_at.is_none() && close.closed_at < insight.created_at)
            {
                return None;
            }
            (TradeUpdateEvent::Closed, Some(close))
        }
        _ => return None,
    };
    Some(Order {
        order_id: snapshot.order_id.clone(),
        insight_id: Some(insight.insight_id.to_string()),
        strategy_type: None,
        asset: asset.clone(),
        qty: insight.remaining_quantity(),
        filled_qty: if close.is_some() {
            insight.remaining_quantity()
        } else {
            0.0
        },
        limit_price: insight.limit_price,
        filled_price: close.map(|close| close.close_price),
        stop_price: insight.stop_price,
        side: insight.side.clone(),
        order_type: insight.order_type.clone(),
        time_in_force: TimeInForce::GTC,
        status: event,
        order_class: insight.order_class.clone(),
        created_at: 0,
        updated_at: 0,
        submitted_at: 0,
        filled_at: None,
        realized_pnl: close.map(|close| close.realized_pnl),
        position_accounting: close.and_then(|close| close.validated_position_accounting(&snapshot.order_id)),
        commission: close.map(|close| close.commission),
        swap: close.map(|close| close.swap),
        rejection_reason: snapshot.detail.clone(),
        legs: None,
    })
}

impl<S: Strategy, E: Broker + OrderManagementProvider, D: DataFeed + DataProvider>
    StrategyState<S, E, D>
{
    pub(super) fn apply_lifecycle_snapshot(&mut self, snapshot: OrderLifecycleSnapshot) {
        // Indexed order identity, not symbol-only matching; nested split orders
        // remain independent owners. Repeated terminal snapshots are no-ops.
        if let Some(id) = self.insights.insight_id_for_order(&snapshot.order_id) {
            let Some(asset) = self
                .insights
                .get(&id)
                .and_then(|row| self.universe.get(&row.symbol))
            else {
                return;
            };
            let Some(order) = self
                .insights
                .get(&id)
                .and_then(|row| terminal_order(row, asset, &snapshot))
            else {
                return;
            };
            let event = order.status.clone();
            let already_closed = self.insights.get(&id).is_some_and(|row| row.state == InsightState::Closed);
            if self.process_order_evidence(order, event, OrderEvidenceSource::TerminalHistory, None) {
                if let Some(close) = snapshot.close_details.as_ref().filter(|_| !already_closed) {
                    if let Some(row) = self
                        .insights
                        .get_mut(&id)
                        .filter(|row| row.state == InsightState::Closed)
                    {
                        row.closed_at = Some(close.closed_at);
                    }
                }
                log::info!(
                    "Broker history reconciled local insight {id}: {:?}",
                    snapshot.status
                );
            }
        }
    }
}
