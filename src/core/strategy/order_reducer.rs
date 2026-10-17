//! Single-owner order evidence reduction. Both submit responses and streamed
//! broker events pass here; transport tasks never mutate insights.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OrderEvidenceSource {
    SubmitResponse,
    TradeStream,
    /// Validated final broker history for an exact locally owned order ticket.
    TerminalHistory,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum OrderReduction {
    #[default]
    Ignored,
    Updated,
    /// Read-only economics/watermark correction; no trading callbacks or cleanup.
    AccountingUpdated,
    EntryFilled,
    LateEntryFilled,
    Terminal,
}

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    pub(super) fn reduce_broker_order(
        insight: &mut Insight,
        order: &Order,
        event: &TradeUpdateEvent,
        source: OrderEvidenceSource,
    ) -> OrderReduction {
        use OrderReduction::*;
        let correlated_response = source == OrderEvidenceSource::SubmitResponse
            && order.insight_id.is_none()
            && insight
                .order_id
                .as_ref()
                .is_none_or(|id| id == &order.order_id);
        if order.order_id.is_empty()
            || insight.symbol != order.asset.symbol
            || !(correlated_response || Self::trade_event_matches_insight(insight, order))
        {
            return Ignored;
        }
        if insight.state == InsightState::Closed
            && *event == TradeUpdateEvent::Closed
            && source != OrderEvidenceSource::SubmitResponse
            && order.position_accounting.as_ref().is_some_and(|value|
                insight.observe_position_accounting(value))
        {
            return AccountingUpdated;
        }
        let entry_side = order.side == insight.side;
        let entry_order = insight
            .order_id
            .as_ref()
            .is_none_or(|id| id == &order.order_id)
            // Some adapters replace a pending order ticket with a position
            // ticket on entry execution. Only an explicit, exact client insight
            // identity may establish that alias; never symbol-only routing.
            || (matches!(insight.state, InsightState::New | InsightState::Executed)
                && order.insight_id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) == Some(insight.insight_id));
        let price = || {
            Self::round_order_value(
                order,
                order.filled_price.or(order.limit_price).unwrap_or(0.0),
            )
        };
        let commission = || Self::round_order_option_value(order, order.commission);
        let valid_fill = || {
            order.filled_qty.is_finite()
                && order.filled_qty > 0.0
                && price().is_finite()
                && price() > 0.0
        };

        // A disconnected runtime can miss both entry and exit. Confirmed final
        // history closes that ticket directly: do not synthesize an entry fill
        // (which would activate dormant DCA children or invent an entry price).
        if source == OrderEvidenceSource::TerminalHistory
            && *event == TradeUpdateEvent::Closed
            && matches!(insight.state, InsightState::New | InsightState::Executed | InsightState::Filled)
            && insight.order_id.as_deref() == Some(order.order_id.as_str())
            && order.realized_pnl.is_some_and(f64::is_finite)
            && price().is_finite()
            && price() > 0.0
        {
            insight.suppress_child_entries = true;
            insight.position_closed(
                price(), &order.order_id, order.filled_qty,
                order.realized_pnl, order.commission, order.swap,
            );
            if let Some(accounting) = &order.position_accounting {
                insight.observe_position_accounting(accounting);
            }
            return Terminal;
        }

        // Cancellation/rejection of the entry's unfilled remainder is not proof
        // that the already executed position was closed. Keep that exposure
        // active, using the last cumulative broker fill and average entry.
        if matches!(insight.state, InsightState::New | InsightState::Executed)
            && entry_order
            && matches!(
                event,
                TradeUpdateEvent::Cancelled
                    | TradeUpdateEvent::Expired
                    | TradeUpdateEvent::Rejected
            )
            && let (Some(quantity), Some(entry_price)) =
                (insight.entry_filled_quantity, insight.filled_price)
            && quantity > 0.0
            && quantity.is_finite()
            && entry_price > 0.0
            && entry_price.is_finite()
        {
            let commission = insight.commission;
            insight.suppress_child_entries = true;
            insight.set_quantity(Some(quantity));
            insight.position_filled(entry_price, quantity, &order.order_id, commission);
            insight.state_log(Some(format!(
                "Entry remainder {:?}; preserving broker-confirmed filled quantity {}",
                event, quantity,
            )));
            return LateEntryFilled;
        }

        match (&insight.state, event) {
            (
                InsightState::New,
                TradeUpdateEvent::Accepted
                | TradeUpdateEvent::PendingNew
                | TradeUpdateEvent::New
                | TradeUpdateEvent::Pending,
            ) if entry_side && entry_order => {
                insight.order_accepted(&order.order_id);
                Self::sync_broker_managed_levels(insight, order);
                Updated
            }
            (InsightState::New | InsightState::Executed, TradeUpdateEvent::Filled)
                if entry_side && entry_order && valid_fill() =>
            {
                if insight.state == InsightState::New {
                    insight.order_accepted(&order.order_id);
                }
                insight.position_filled(price(), order.filled_qty, &order.order_id, commission());
                Self::sync_broker_managed_levels(insight, order);
                EntryFilled
            }
            (InsightState::New | InsightState::Executed, TradeUpdateEvent::PartialFilled)
                if entry_side && entry_order && valid_fill() =>
            {
                // BrokerOrder.filled_qty is cumulative for an entry. Equal or
                // older snapshots (including the submit response) are no-ops.
                if order.filled_qty <= insight.entry_filled_quantity.unwrap_or(0.0) {
                    return Ignored;
                }
                if insight.state == InsightState::New {
                    insight.order_accepted(&order.order_id);
                }
                insight.partial_filled(order.filled_qty, price(), &order.order_id, commission());
                Self::sync_broker_managed_levels(insight, order);
                Updated
            }
            (InsightState::Filled, TradeUpdateEvent::PartialFilled)
                if !entry_side && source == OrderEvidenceSource::TradeStream =>
            {
                let average_close_price = Self::close_price_from_order(order);
                // Like entry fills, filled_qty is cumulative per broker order.
                // Distinct exit orders have distinct identities; redelivery of
                // the same snapshot cannot reduce remaining exposure twice.
                let (previously_closed, previous_notional) = insight
                    .partial_closes
                    .iter()
                    .filter(|partial| partial.order_id == order.order_id)
                    .fold((0.0, 0.0), |(quantity, notional), partial| {
                        (
                            quantity + partial.quantity,
                            notional
                                + partial.quantity
                                    * partial.filled_price.unwrap_or(partial.entry_price),
                        )
                    });
                let close_qty = order.filled_qty - previously_closed;
                // The broker price is the average for the cumulative fill, not
                // the incremental execution. Preserve its notional when turning
                // cumulative evidence into individual partial-close records.
                let close_price =
                    (order.filled_qty * average_close_price - previous_notional) / close_qty;
                if !order.filled_qty.is_finite()
                    || !close_qty.is_finite()
                    || close_qty <= 0.0
                    || close_qty > insight.remaining_quantity()
                    || !close_price.is_finite()
                    || close_price <= 0.0
                {
                    return Ignored;
                }
                insight.partial_closed(close_qty, close_price, &order.order_id, commission());
                Updated
            }
            (
                InsightState::New | InsightState::Executed,
                TradeUpdateEvent::Cancelled | TradeUpdateEvent::Expired,
            ) if entry_order => {
                Self::sync_broker_managed_levels(insight, order);
                insight.order_cancelled("Order cancelled");
                Terminal
            }
            (InsightState::New | InsightState::Executed, TradeUpdateEvent::Rejected)
                if entry_order =>
            {
                Self::sync_broker_managed_levels(insight, order);
                insight.order_rejected(
                    order
                        .rejection_reason
                        .as_deref()
                        .unwrap_or("Order rejected"),
                );
                Terminal
            }
            (InsightState::Executed, TradeUpdateEvent::Replaced)
                if entry_order && source == OrderEvidenceSource::TradeStream =>
            {
                if order.limit_price != insight.limit_price {
                    insight.limit_price = Self::round_order_option_value(order, order.limit_price);
                }
                if insight.quantity != Some(order.qty) {
                    insight.quantity = Some(order.qty);
                }
                Self::sync_broker_managed_levels(insight, order);
                Updated
            }
            (InsightState::Filled, TradeUpdateEvent::Filled | TradeUpdateEvent::Closed)
                if source == OrderEvidenceSource::TradeStream
                    && (!entry_side || *event == TradeUpdateEvent::Closed) =>
            {
                let close_price =
                    Self::round_order_value(order, Self::close_price_from_order(order));
                if !close_price.is_finite() || close_price <= 0.0 {
                    return Ignored;
                }
                Self::sync_broker_managed_levels(insight, order);
                insight.position_closed(
                    close_price,
                    &order.order_id,
                    order.filled_qty,
                    Self::round_order_option_value(order, order.realized_pnl),
                    commission(),
                    Self::round_order_option_value(order, order.swap),
                );
                if let Some(accounting) = &order.position_accounting {
                    insight.observe_position_accounting(accounting);
                }
                Terminal
            }
            // A genuine late entry fill is authoritative after cancellation or
            // rejection. A delayed submit response must not reopen a closed trade.
            (InsightState::Cancelled | InsightState::Rejected, TradeUpdateEvent::Filled)
                if entry_side && valid_fill() =>
            {
                insight.suppress_child_entries = true;
                insight.position_filled(price(), order.filled_qty, &order.order_id, commission());
                Self::sync_broker_managed_levels(insight, order);
                LateEntryFilled
            }
            // In particular: accepted-after-fill, duplicate full fill,
            // partial-entry-after-full-fill, and any event after Closed.
            _ => Ignored,
        }
    }
}
