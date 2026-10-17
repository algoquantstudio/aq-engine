//! Receipt-based Insight mutations. Queue admission never changes broker state.
use super::{
    Insight,
    types::{InsightState, InsightValidation},
};
use crate::core::{
    broker::commands::{
        BrokerCommandAction, BrokerCommandError, BrokerCommandOutcome, BrokerCommandReceipt,
        BrokerOperationGroup,
    },
    strategy::{StrategyContext, command_driver::BrokerCommandProgress},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InsightMutationState {
    /// No broker work is required, or the requested mutation was acknowledged.
    Applied,
    /// The receipt is retained by the Insight; retrying a deferred stage polls it.
    Pending(BrokerCommandReceipt),
}
pub type InsightMutationResult = Result<InsightMutationState, BrokerCommandError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum MutationKind {
    Submit,
    Cancel,
    Close,
    TakeProfit,
    StopLoss,
    Replace,
    AmendEntry,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) enum MutationEffect {
    Submit,
    Cancel,
    Close {
        price: Option<f64>,
    },
    TakeProfit(Option<Vec<f64>>),
    StopLoss(Option<Vec<f64>>),
    Replace {
        price: f64,
        quantity: f64,
    },
    AmendEntry {
        limit_price: Option<f64>,
        stop_price: Option<f64>,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct PendingInsightMutation {
    pub kind: MutationKind,
    pub receipt: BrokerCommandReceipt,
    pub effect: MutationEffect,
}

impl Insight {
    /// Terminal state can arrive from the trade stream before a cancel/close
    /// response. Consume its receipts without depending on an active-state pipe.
    pub(crate) fn poll_terminal_mutations(&mut self, ctx: &mut dyn StrategyContext) {
        if !self.state.is_inactive() {
            return;
        }
        for kind in [
            MutationKind::Submit,
            MutationKind::Cancel,
            MutationKind::Close,
            MutationKind::TakeProfit,
            MutationKind::StopLoss,
            MutationKind::Replace,
            MutationKind::AmendEntry,
        ] {
            let _ = self.poll_mutation(ctx, kind);
        }
    }

    pub fn has_pending_submit(&self) -> bool {
        self.pending_mutations
            .iter()
            .any(|pending| pending.kind == MutationKind::Submit)
    }

    pub(crate) fn poll_pending_submission(
        &mut self,
        ctx: &mut dyn StrategyContext,
    ) -> InsightMutationResult {
        self.poll_mutation(ctx, MutationKind::Submit)
            .unwrap_or(Ok(InsightMutationState::Applied))
    }
    /// The earliest outstanding broker dependency, suitable for pipe deferral.
    pub fn pending_broker_command(&self) -> Option<&BrokerCommandReceipt> {
        self.pending_mutations
            .first()
            .map(|pending| &pending.receipt)
    }

    pub fn has_pending_close(&self) -> bool {
        self.pending_mutations
            .iter()
            .any(|pending| pending.kind == MutationKind::Close)
    }

    pub fn pending_close_price(&self) -> Option<f64> {
        self.pending_mutations
            .iter()
            .find_map(|pending| match pending.effect {
                MutationEffect::Close { price } => price,
                _ => None,
            })
    }

    fn poll_mutation(
        &mut self,
        ctx: &mut dyn StrategyContext,
        kind: MutationKind,
    ) -> Option<InsightMutationResult> {
        let index = self
            .pending_mutations
            .iter()
            .position(|pending| pending.kind == kind)?;
        let receipt = self.pending_mutations[index].receipt.clone();
        let completion = match ctx.broker_command_progress(&receipt) {
            None | Some(BrokerCommandProgress::Queued) => {
                return Some(Ok(InsightMutationState::Pending(receipt)));
            }
            Some(BrokerCommandProgress::Failed(error)) => return Some(Err(error)),
            Some(BrokerCommandProgress::Completed(completion)) => completion,
        };
        if matches!(completion.outcome, BrokerCommandOutcome::Ambiguous { .. }) {
            let terminal_cleanup = self.order_id.is_some()
                && match kind {
                    MutationKind::Close => self.state == InsightState::Closed,
                    MutationKind::Cancel => matches!(
                        self.state,
                        InsightState::Cancelled | InsightState::Rejected | InsightState::Closed
                    ),
                    MutationKind::TakeProfit | MutationKind::StopLoss => matches!(
                        self.state,
                        InsightState::Closed | InsightState::Cancelled
                    ),
                    _ => false,
                };
            if terminal_cleanup {
                // The durable checkpoint boundary additionally verifies that
                // this receipt targeted the exact broker order on this row.
                self.pending_mutations.remove(index);
                ctx.broker_command_reduced(&receipt);
                return Some(Ok(InsightMutationState::Applied));
            }
            // Retain the exact identity until broker-backed reconciliation. A
            // transport timeout must never manufacture another submission.
            return Some(Ok(InsightMutationState::Pending(receipt)));
        }
        if let BrokerCommandOutcome::OrderResponse { order } = &completion.outcome {
            // A rejected entry never acquired a broker ticket. The reducer has
            // already confirmed rejection; correlate its completed submission
            // receipt instead of requiring a ticket that cannot exist.
            let rejected_submit = kind == MutationKind::Submit
                && self.state == InsightState::Rejected
                && self.order_id.is_none()
                && order.status == crate::core::broker::types::TradeUpdateEvent::Rejected
                && completion.receipt == receipt
                && receipt.insight_id == self.insight_id.to_string();
            // Only the strategy reducer binds broker evidence. A malformed or
            // wrong-symbol response must not become success merely by polling it.
            if (self.order_id.as_deref() != Some(order.order_id.as_str())
                && !matches!(self.state, InsightState::Filled | InsightState::Closed)
                && !rejected_submit)
                || self.symbol != order.asset.symbol
                || self.side != order.side
                || order
                    .insight_id
                    .as_deref()
                    .is_some_and(|id| id != self.insight_id.to_string())
            {
                return Some(Ok(InsightMutationState::Pending(receipt)));
            }
        }
        let pending = self.pending_mutations.remove(index);
        let result = match &completion.outcome {
            BrokerCommandOutcome::Acknowledged | BrokerCommandOutcome::OrderResponse { .. } => {
                if !self.state.is_inactive() {
                    match pending.effect {
                        MutationEffect::Submit => {
                            // Cancelling a still-pending submit gates the entry;
                            // it has not sent a broker Cancel yet. Any reducer or
                            // deferred pipe may consume this acknowledgement,
                            // not just cancel(). Let cleanup issue the real Cancel
                            // now that the broker order exists. Keep child-entry
                            // suppression and any actual cancellation ownership.
                            if !self
                                .pending_mutations
                                .iter()
                                .any(|pending| pending.kind == MutationKind::Cancel)
                            {
                                self.cancelling = false;
                            }
                        }
                        MutationEffect::TakeProfit(levels) => {
                            let price = levels.as_ref().and_then(|levels| levels.first()).copied();
                            self.set_take_profit_levels(levels);
                            if let Some(leg) = self.legs.take_profit.as_mut() {
                                leg.limit_price = price;
                                leg.updated_at = ctx.current_time().timestamp().max(0) as u64;
                            }
                        }
                        MutationEffect::StopLoss(levels) => {
                            let price = levels.as_ref().and_then(|levels| levels.first()).copied();
                            self.set_stop_loss_levels(levels);
                            if let Some(leg) = self.legs.stop_loss.as_mut() {
                                leg.limit_price = price;
                                leg.updated_at = ctx.current_time().timestamp().max(0) as u64;
                            }
                        }
                        MutationEffect::AmendEntry {
                            limit_price,
                            stop_price,
                        } => {
                            // A fill can win the race with the RPC acknowledgement.
                            if self.state == InsightState::Executed
                                && self.entry_filled_quantity.unwrap_or(0.0) == 0.0
                            {
                                if let Some(price) = limit_price {
                                    self.set_limit_price(Some(price));
                                }
                                if let Some(price) = stop_price {
                                    self.set_stop_price(Some(price));
                                }
                            }
                        }
                        MutationEffect::Replace { price, quantity } => {
                            self.set_limit_price(Some(price));
                            self.set_quantity(Some(quantity));
                        }
                        _ => {}
                    }
                }
                self.state_log(Some(format!(
                    "Broker command acknowledged: {}",
                    receipt.id.0
                )));
                Ok(InsightMutationState::Applied)
            }
            BrokerCommandOutcome::Declined { reason } => {
                match kind {
                    MutationKind::Cancel => self.cancelling = false,
                    MutationKind::Close => self.closing = false,
                    MutationKind::Submit if self.state == InsightState::New => {
                        self.order_rejected(reason)
                    }
                    _ => {}
                }
                Err(BrokerCommandError::Declined(reason.clone()))
            }
            BrokerCommandOutcome::CancelledBeforeDispatch => {
                if kind == MutationKind::Submit && self.state == InsightState::New {
                    self.order_cancelled("Entry intent cancelled before broker dispatch");
                }
                Ok(InsightMutationState::Applied)
            }
            BrokerCommandOutcome::Ambiguous { .. } => unreachable!("handled above"),
        };
        ctx.broker_command_reduced(&receipt);
        Some(result)
    }

    fn queue_mutation(
        &mut self,
        ctx: &mut dyn StrategyContext,
        kind: MutationKind,
        action: BrokerCommandAction,
        effect: MutationEffect,
        group: Option<BrokerOperationGroup>,
    ) -> InsightMutationResult {
        let receipt = ctx.enqueue_broker_command(self.insight_id, action, group)?;
        self.pending_mutations.push(PendingInsightMutation {
            kind,
            receipt: receipt.clone(),
            effect,
        });
        self.state_log(Some(format!("Broker command queued: {}", receipt.id.0)));
        Ok(InsightMutationState::Pending(receipt))
    }

    pub fn submit(&mut self, ctx: &mut dyn StrategyContext) -> InsightMutationResult {
        if let Some(result) = self.poll_mutation(ctx, MutationKind::Submit) {
            return result;
        }
        if self.submitted || self.order_id.is_some() || self.state != InsightState::New {
            return Ok(InsightMutationState::Applied);
        }
        if self.suppress_child_entries || self.cancelling || self.closing {
            return Err(BrokerCommandError::Invalid("Entry is gated for cancellation/cleanup".into()));
        }
        if let Err(validation) = self.validate(ctx) {
            let reason = match validation {
                InsightValidation::Invalid(reason) => reason,
                _ => "Insight validation failed".into(),
            };
            self.order_rejected(&reason);
            return Err(BrokerCommandError::Invalid(reason));
        }
        self.queue_mutation(
            ctx,
            MutationKind::Submit,
            BrokerCommandAction::Submit {
                request: (&*self).into(),
            },
            MutationEffect::Submit,
            None,
        )
    }

    pub fn cancel(&mut self, ctx: &mut dyn StrategyContext) -> InsightMutationResult {
        if let Some(result) = self.poll_mutation(ctx, MutationKind::Submit) {
            match result {
                Ok(InsightMutationState::Pending(receipt)) => {
                    ctx.cancel_pending_entry(&receipt)?;
                    self.suppress_child_entries = true;
                    self.cancelling = true;
                    return Ok(InsightMutationState::Pending(receipt));
                }
                Ok(InsightMutationState::Applied) => self.cancelling = false,
                Err(error) => return Err(error),
            }
        }
        if let Some(result) = self.poll_mutation(ctx, MutationKind::Cancel) {
            return result;
        }
        if self.state.is_inactive() || self.cancelling {
            return Ok(InsightMutationState::Applied);
        }
        let order_id = self.order_id.clone().ok_or_else(|| {
            BrokerCommandError::Invalid("cancel requires a broker order ID".into())
        })?;
        let result = self.queue_mutation(
            ctx,
            MutationKind::Cancel,
            BrokerCommandAction::Cancel { order_id },
            MutationEffect::Cancel,
            None,
        )?;
        self.cancelling = true;
        self.suppress_child_entries = true;
        Ok(result)
    }

    pub fn cancel_order_by_id(
        &mut self,
        order_id: String,
        ctx: &mut dyn StrategyContext,
    ) -> InsightMutationResult {
        if let Some(result) = self.poll_mutation(ctx, MutationKind::Cancel) {
            return result;
        }
        self.queue_mutation(
            ctx,
            MutationKind::Cancel,
            BrokerCommandAction::Cancel { order_id },
            MutationEffect::Cancel,
            None,
        )
    }

    pub fn close(&mut self, ctx: &mut dyn StrategyContext) -> InsightMutationResult {
        self.try_close_partial(ctx, self.remaining_quantity(), None)
    }

    pub fn close_partial(
        &mut self,
        ctx: &mut dyn StrategyContext,
        quantity: f64,
        price: Option<f64>,
    ) -> InsightMutationResult {
        self.try_close_partial(ctx, quantity, price)
    }

    pub fn try_close_partial(
        &mut self,
        ctx: &mut dyn StrategyContext,
        quantity: f64,
        price: Option<f64>,
    ) -> InsightMutationResult {
        if let Some(result) = self.poll_mutation(ctx, MutationKind::Close) {
            return result;
        }
        if self.state.is_inactive() || self.closing {
            return Ok(InsightMutationState::Applied);
        }
        if self.state != InsightState::Filled
            || !quantity.is_finite()
            || quantity <= 0.0
            || quantity > self.remaining_quantity()
        {
            return Err(BrokerCommandError::Invalid(
                "close requires a positive quantity within the filled remainder".into(),
            ));
        }
        let order_id = self.order_id.clone().ok_or_else(|| {
            BrokerCommandError::Invalid("close requires a broker order ID".into())
        })?;
        // The OMS owns protective-order teardown as part of closing its position.
        // A context must not race independent leg cancellation against this close.
        let result = self.queue_mutation(
            ctx,
            MutationKind::Close,
            BrokerCommandAction::Close {
                order_id,
                quantity,
                price,
            },
            MutationEffect::Close { price },
            None,
        )?;
        self.closing = true;
        Ok(result)
    }

    pub fn update_take_profit(
        &mut self,
        ctx: &mut dyn StrategyContext,
        levels: Option<Vec<f64>>,
    ) -> InsightMutationResult {
        self.update_take_profit_in_group(ctx, levels, None)
    }

    pub fn update_take_profit_in_group(
        &mut self,
        ctx: &mut dyn StrategyContext,
        levels: Option<Vec<f64>>,
        group: Option<BrokerOperationGroup>,
    ) -> InsightMutationResult {
        let levels = Self::normalize_levels(levels);
        if let Some(result) = self.poll_mutation(ctx, MutationKind::TakeProfit) {
            match result {
                Ok(InsightMutationState::Applied) => {}
                other => return other,
            }
        }
        if self.take_profit_levels == levels {
            return Ok(InsightMutationState::Applied);
        }
        if !matches!(self.state, InsightState::Executed | InsightState::Filled) {
            self.set_take_profit_levels(levels);
            return Ok(InsightMutationState::Applied);
        }
        let price = levels
            .as_ref()
            .and_then(|levels| levels.first())
            .copied()
            .ok_or_else(|| {
                BrokerCommandError::Invalid(
                    "clearing live TP requires an explicit broker clear-protection operation"
                        .into(),
                )
            })?;
        let order_id = self
            .legs
            .take_profit
            .as_ref()
            .and_then(|leg| leg.order_id.clone())
            .or_else(|| self.order_id.clone())
            .ok_or_else(|| {
                BrokerCommandError::Invalid("TP update requires a broker order ID".into())
            })?;
        self.queue_mutation(
            ctx,
            MutationKind::TakeProfit,
            BrokerCommandAction::TakeProfit {
                order_id,
                price,
                quantity: self.remaining_quantity(),
            },
            MutationEffect::TakeProfit(levels),
            group,
        )
    }

    pub fn update_stop_loss(
        &mut self,
        ctx: &mut dyn StrategyContext,
        levels: Option<Vec<f64>>,
    ) -> InsightMutationResult {
        let levels = Self::normalize_levels(levels);
        if let Some(result) = self.poll_mutation(ctx, MutationKind::StopLoss) {
            match result {
                Ok(InsightMutationState::Applied) => {}
                other => return other,
            }
        }
        if self.stop_loss_levels == levels {
            return Ok(InsightMutationState::Applied);
        }
        if !matches!(self.state, InsightState::Executed | InsightState::Filled) {
            self.set_stop_loss_levels(levels);
            return Ok(InsightMutationState::Applied);
        }
        let price = levels
            .as_ref()
            .and_then(|levels| levels.first())
            .copied()
            .ok_or_else(|| {
                BrokerCommandError::Invalid(
                    "clearing live SL requires an explicit broker clear-protection operation"
                        .into(),
                )
            })?;
        let order_id = self
            .legs
            .stop_loss
            .as_ref()
            .and_then(|leg| leg.order_id.clone())
            .or_else(|| self.order_id.clone())
            .ok_or_else(|| {
                BrokerCommandError::Invalid("SL update requires a broker order ID".into())
            })?;
        self.queue_mutation(
            ctx,
            MutationKind::StopLoss,
            BrokerCommandAction::StopLoss {
                order_id,
                price,
                quantity: self.remaining_quantity(),
            },
            MutationEffect::StopLoss(levels),
            None,
        )
    }

    /// Amend the pending entry's limit component; not a take-profit update.
    pub fn update_limit(
        &mut self,
        ctx: &mut dyn StrategyContext,
        price: f64,
    ) -> InsightMutationResult {
        self.update_entry_price(ctx, price, false)
    }

    /// Amend the stop trigger of a Stop or StopLimit entry, not its stop loss.
    /// For StopLimit, use update_limit to amend the separate limit component.
    pub fn update_stop_limit(
        &mut self,
        ctx: &mut dyn StrategyContext,
        price: f64,
    ) -> InsightMutationResult {
        self.update_entry_price(ctx, price, true)
    }

    fn update_entry_price(
        &mut self,
        ctx: &mut dyn StrategyContext,
        price: f64,
        stop: bool,
    ) -> InsightMutationResult {
        use crate::core::broker::{features::BrokerFeature, types::OrderType};
        if self.state == InsightState::Filled
            || self.state.is_inactive()
            || self.entry_filled_quantity.unwrap_or(0.0) > 0.0
        {
            // Consume a completed receipt, but never reject a filled position
            // because the broker declined its now-obsolete entry amendment.
            let _ = self.poll_mutation(ctx, MutationKind::AmendEntry);
            self.state_log(Some(
                "Cannot update entry limit/stop price: order has already filled or is inactive"
                    .into(),
            ));
            return Ok(InsightMutationState::Applied);
        }
        if let Some(result) = self.poll_mutation(ctx, MutationKind::AmendEntry) {
            match result {
                Ok(InsightMutationState::Applied) => {}
                other => return other,
            }
        }
        // Submission owns an immutable request. Wait for its ticket before amending.
        if self.has_pending_submit() {
            match self.poll_pending_submission(ctx)? {
                InsightMutationState::Pending(receipt) => {
                    return Ok(InsightMutationState::Pending(receipt));
                }
                InsightMutationState::Applied => {}
            }
        }
        if !price.is_finite() || price <= 0.0 {
            return Err(BrokerCommandError::Invalid(
                "Entry price must be finite and positive".into(),
            ));
        }
        if self.state == InsightState::New && self.order_id.is_none() {
            if stop {
                self.set_stop_price(Some(price));
            } else {
                self.set_limit_price(Some(price));
            }
            return Ok(InsightMutationState::Applied);
        }
        let valid_type = if stop {
            matches!(self.order_type, OrderType::Stop | OrderType::StopLimit)
        } else {
            matches!(self.order_type, OrderType::Limit | OrderType::StopLimit)
        };
        if self.state != InsightState::Executed || !valid_type || self.closing || self.cancelling {
            return Err(BrokerCommandError::Invalid(
                "Entry amendment requires a matching, active pending order".into(),
            ));
        }
        let current = if stop {
            self.stop_price
        } else {
            self.limit_price
        };
        if current == Some(price) {
            return Ok(InsightMutationState::Applied);
        }
        let feature = if stop {
            BrokerFeature::AmendStopPrice
        } else {
            BrokerFeature::AmendLimitPrice
        };
        if !ctx.broker_features().contains(&feature) {
            return Err(BrokerCommandError::Invalid(
                "Broker does not support this entry price amendment".into(),
            ));
        }
        let order_id = self.order_id.clone().ok_or_else(|| {
            BrokerCommandError::Invalid("Entry amendment requires a broker order ID".into())
        })?;
        let (limit_price, stop_price) = if stop {
            (None, Some(price))
        } else {
            (Some(price), None)
        };
        self.queue_mutation(
            ctx,
            MutationKind::AmendEntry,
            BrokerCommandAction::AmendEntry {
                order_id,
                limit_price,
                stop_price,
            },
            MutationEffect::AmendEntry {
                limit_price,
                stop_price,
            },
            None,
        )
    }

    pub fn replace_pending_order(
        &mut self,
        ctx: &mut dyn StrategyContext,
        price: f64,
        quantity: f64,
    ) -> InsightMutationResult {
        if let Some(result) = self.poll_mutation(ctx, MutationKind::Replace) {
            return result;
        }
        if self.state != InsightState::Executed {
            return Err(BrokerCommandError::Invalid(
                "replacement requires an Executed entry".into(),
            ));
        }
        let order_id = self.order_id.clone().ok_or_else(|| {
            BrokerCommandError::Invalid("replacement requires a broker order ID".into())
        })?;
        self.queue_mutation(
            ctx,
            MutationKind::Replace,
            BrokerCommandAction::Replace {
                order_id,
                price,
                quantity,
            },
            MutationEffect::Replace { price, quantity },
            None,
        )
    }
}
