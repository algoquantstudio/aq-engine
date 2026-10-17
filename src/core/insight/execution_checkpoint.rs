//! Versioned, flat execution state. This is private recovery data, not an AQS
//! response model. Children are referenced by identity so a split tree is never
//! cloned into every journal row; dormant templates are saved as separate rows.
use super::{Insight, types::*};
use crate::core::{
    broker::{
        commands::{BrokerCommandError, BrokerCommandReceipt},
        types::{OrderClass, OrderLegs, OrderSide, OrderType},
    },
    strategy::InsightPipelineCheckpoint,
    utils::timeframe::TimeFrame,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const EXECUTION_CHECKPOINT_VERSION: u16 = 1;
const EXECUTION_CHECKPOINT_WITH_DATA_VERSION: u16 = 2;
const EXECUTION_CHECKPOINT_WITH_DEPENDENCIES_VERSION: u16 = 3;
const EXECUTION_CHECKPOINT_WITH_ACCOUNTING_VERSION: u16 = 4;

impl Insight {
    /// Embedded links to materialized children retain configuration/quantity
    /// shape, not another copy of their history or operation ownership. Runtime
    /// traversal resolves those links through the canonical collection row.
    pub(crate) fn recovery_structure_reference(&self) -> Self {
        Self {
            insight_id: self.insight_id,
            parent_id: self.parent_id,
            is_split_insight: self.is_split_insight,
            state: self.state.clone(),
            children: self
                .children
                .iter()
                .map(Self::recovery_structure_reference)
                .collect(),
            order_id: self.order_id.clone(),
            side: self.side.clone(),
            symbol: self.symbol.clone(),
            quantity: self.quantity,
            contracts: self.contracts,
            order_type: self.order_type.clone(),
            order_class: self.order_class.clone(),
            limit_price: self.limit_price,
            stop_price: self.stop_price,
            take_profit_levels: self.take_profit_levels.clone(),
            stop_loss_levels: self.stop_loss_levels.clone(),
            trailing_stop_price: self.trailing_stop_price,
            strategy_type: self.strategy_type.clone(),
            confidence: self.confidence,
            timeframe: self.timeframe.clone(),
            period_unfilled: self.period_unfilled,
            period_till_tp: self.period_till_tp,
            execution_depends: self.execution_depends.clone(),
            filled_price: self.filled_price,
            close_order_id: self.close_order_id.clone(),
            close_price: self.close_price,
            broker_realized_pnl: self.broker_realized_pnl,
            position_accounting: self.position_accounting.clone(),
            commission: self.commission,
            swap: self.swap,
            partial_closes: self.partial_closes.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            filled_at: self.filled_at,
            closed_at: self.closed_at,
            legs: self.legs.clone(),
            market_changed: self.market_changed,
            submitted: self.submitted,
            pending_mutations: Vec::new(),
            execution_data: Default::default(),
            suppress_child_entries: self.suppress_child_entries,
            cancelling: self.cancelling,
            closing: self.closing,
            first_on_fill: self.first_on_fill,
            entry_filled_quantity: self.entry_filled_quantity,
            partial_filled_quantity: self.partial_filled_quantity,
            state_history: Vec::new(),
            state_history_kinds: Vec::new(),
            context: self.context.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct InsightExecutionCheckpoint {
    pub version: u16,
    /// Monotonic per-row revision. Zero is reserved for "no previous row".
    pub revision: u64,
    pub materialized: bool,
    /// Driver-level dependencies also cover context commands not represented by
    /// an Insight helper's pending mutation. They remain scoped to this row.
    #[serde(default)]
    pub owner_receipts: Vec<BrokerCommandReceipt>,
    #[serde(default)]
    pub source_fingerprint: u64,
    pub child_ids: Vec<Uuid>,
    pub pipeline: Option<InsightPipelineCheckpoint>,
    pub insight_id: Uuid,
    pub parent_id: Option<Uuid>,
    pub is_split_insight: bool,
    pub state: InsightState,
    pub order_id: Option<String>,
    pub side: OrderSide,
    pub symbol: String,
    pub quantity: Option<f64>,
    pub contracts: Option<f64>,
    pub order_type: OrderType,
    pub order_class: OrderClass,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub take_profit_levels: Option<Vec<f64>>,
    pub stop_loss_levels: Option<Vec<f64>>,
    pub trailing_stop_price: Option<f64>,
    pub strategy_type: StrategyType,
    pub confidence: u8,
    pub timeframe: TimeFrame,
    pub period_unfilled: Option<u32>,
    pub period_till_tp: Option<u32>,
    pub execution_depends: Vec<StrategyDependentConfirmation>,
    pub filled_price: Option<f64>,
    pub close_order_id: Option<String>,
    pub close_price: Option<f64>,
    pub broker_realized_pnl: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position_accounting: Option<aqe_live_protocol::PositionCloseAccounting>,
    pub commission: Option<f64>,
    pub swap: Option<f64>,
    pub partial_closes: Vec<PartialCloseResult>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub filled_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
    pub legs: OrderLegs,
    pub market_changed: bool,
    pub submitted: bool,
    pub pending_mutations: Vec<super::operations::PendingInsightMutation>,
    #[serde(
        default,
        skip_serializing_if = "super::execution_data::ExecutionData::is_empty"
    )]
    pub execution_data: super::execution_data::ExecutionData,
    pub suppress_child_entries: bool,
    pub cancelling: bool,
    pub closing: bool,
    pub first_on_fill: bool,
    pub entry_filled_quantity: Option<f64>,
    pub partial_filled_quantity: Option<f64>,
    pub state_history: Vec<(DateTime<Utc>, InsightState, Option<String>)>,
    pub state_history_kinds: Vec<InsightHistoryKind>,
}

impl InsightExecutionCheckpoint {
    pub fn capture(
        insight: &Insight,
        revision: u64,
        materialized: bool,
        pipeline: Option<InsightPipelineCheckpoint>,
    ) -> Self {
        Self {
            version: if insight.position_accounting.is_some() {
                EXECUTION_CHECKPOINT_WITH_ACCOUNTING_VERSION
            } else if insight.execution_data.has_recovery_dependencies() {
                EXECUTION_CHECKPOINT_WITH_DEPENDENCIES_VERSION
            } else if insight.execution_data.is_empty() {
                EXECUTION_CHECKPOINT_VERSION
            } else {
                EXECUTION_CHECKPOINT_WITH_DATA_VERSION
            },
            revision,
            materialized,
            owner_receipts: Vec::new(),
            source_fingerprint: insight.snapshot_fingerprint_hash(),
            child_ids: insight
                .children
                .iter()
                .map(|child| child.insight_id)
                .collect(),
            pipeline,
            insight_id: insight.insight_id.clone(),
            parent_id: insight.parent_id.clone(),
            is_split_insight: insight.is_split_insight.clone(),
            state: insight.state.clone(),
            order_id: insight.order_id.clone(),
            side: insight.side.clone(),
            symbol: insight.symbol.clone(),
            quantity: insight.quantity.clone(),
            contracts: insight.contracts.clone(),
            order_type: insight.order_type.clone(),
            order_class: insight.order_class.clone(),
            limit_price: insight.limit_price.clone(),
            stop_price: insight.stop_price.clone(),
            take_profit_levels: insight.take_profit_levels.clone(),
            stop_loss_levels: insight.stop_loss_levels.clone(),
            trailing_stop_price: insight.trailing_stop_price.clone(),
            strategy_type: insight.strategy_type.clone(),
            confidence: insight.confidence.clone(),
            timeframe: insight.timeframe.clone(),
            period_unfilled: insight.period_unfilled.clone(),
            period_till_tp: insight.period_till_tp.clone(),
            execution_depends: insight.execution_depends.clone(),
            filled_price: insight.filled_price.clone(),
            close_order_id: insight.close_order_id.clone(),
            close_price: insight.close_price.clone(),
            broker_realized_pnl: insight.broker_realized_pnl.clone(),
            position_accounting: insight.position_accounting.clone(),
            commission: insight.commission.clone(),
            swap: insight.swap.clone(),
            partial_closes: insight.partial_closes.clone(),
            created_at: insight.created_at.clone(),
            updated_at: insight.updated_at.clone(),
            filled_at: insight.filled_at.clone(),
            closed_at: insight.closed_at.clone(),
            legs: insight.legs.clone(),
            market_changed: insight.market_changed.clone(),
            submitted: insight.submitted.clone(),
            pending_mutations: insight.pending_mutations.clone(),
            execution_data: insight.execution_data.clone(),
            suppress_child_entries: insight.suppress_child_entries.clone(),
            cancelling: insight.cancelling.clone(),
            closing: insight.closing.clone(),
            first_on_fill: insight.first_on_fill.clone(),
            entry_filled_quantity: insight.entry_filled_quantity.clone(),
            partial_filled_quantity: insight.partial_filled_quantity.clone(),
            state_history: insight.state_history.clone(),
            state_history_kinds: insight.state_history_kinds.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), BrokerCommandError> {
        let invalid = |message: &str| BrokerCommandError::Invalid(message.to_owned());
        if !matches!(
            self.version,
            EXECUTION_CHECKPOINT_VERSION
                | EXECUTION_CHECKPOINT_WITH_DATA_VERSION
                | EXECUTION_CHECKPOINT_WITH_DEPENDENCIES_VERSION
                | EXECUTION_CHECKPOINT_WITH_ACCOUNTING_VERSION
                // Version 5 only added optional metadata, now ignored by serde.
                | 5
        ) || (self.version == EXECUTION_CHECKPOINT_VERSION && !self.execution_data.is_empty())
            || (self.version < EXECUTION_CHECKPOINT_WITH_DEPENDENCIES_VERSION
                && self.execution_data.has_recovery_dependencies())
            || (self.version < EXECUTION_CHECKPOINT_WITH_ACCOUNTING_VERSION
                && self.position_accounting.is_some())
            || self.revision == 0
            || self.revision > i64::MAX as u64
        {
            return Err(invalid("unsupported execution checkpoint version/revision"));
        }
        self.execution_data.validate()?;
        if let Some(accounting) = &self.position_accounting {
            accounting.validate().map_err(invalid)?;
            if self.state != InsightState::Closed || !self.order_id.as_deref().is_some_and(|id| accounting.owns_order(id)) {
                return Err(invalid("position accounting must belong to this closed insight"));
            }
        }
        if self.parent_id == Some(self.insight_id) || self.child_ids.contains(&self.insight_id) {
            return Err(invalid("execution checkpoint contains a self-parent/child"));
        }
        let mut children = std::collections::HashSet::with_capacity(self.child_ids.len());
        if self.child_ids.iter().any(|id| !children.insert(*id)) {
            return Err(invalid("execution checkpoint contains duplicate children"));
        }
        if self.symbol.trim().is_empty() {
            return Err(invalid("execution checkpoint is missing its symbol"));
        }
        // serde_json represents non-finite optional floats as null. Refuse that
        // lossy conversion rather than silently dropping execution metadata.
        if [
            self.quantity,
            self.contracts,
            self.limit_price,
            self.stop_price,
            self.trailing_stop_price,
            self.filled_price,
            self.close_price,
            self.broker_realized_pnl,
            self.commission,
            self.swap,
            self.entry_filled_quantity,
            self.partial_filled_quantity,
        ]
        .into_iter()
        .flatten()
        .any(|number| !number.is_finite())
            || self
                .take_profit_levels
                .iter()
                .chain(self.stop_loss_levels.iter())
                .flatten()
                .any(|number| !number.is_finite())
            || self.partial_closes.iter().any(|close| {
                [
                    Some(close.quantity),
                    Some(close.entry_price),
                    close.filled_price,
                ]
                .into_iter()
                .flatten()
                .any(|number| !number.is_finite())
            })
            || [
                &self.legs.take_profit,
                &self.legs.stop_loss,
                &self.legs.trailing_stop,
            ]
            .into_iter()
            .flatten()
            .any(|leg| {
                [leg.limit_price, leg.trail_price, leg.filled_price]
                    .into_iter()
                    .flatten()
                    .any(|number| !number.is_finite())
            })
        {
            return Err(invalid("execution checkpoint contains a non-finite number"));
        }
        if self.owner_receipts.len() > 256 || self.pending_mutations.len() > 256 {
            return Err(invalid("execution checkpoint exceeds receipt limit"));
        }
        let owner = self.insight_id.to_string();
        let mut owned = std::collections::HashMap::with_capacity(self.owner_receipts.len());
        for receipt in &self.owner_receipts {
            if !self.materialized
                || receipt.insight_id != owner
                || owned.insert(&receipt.id, receipt).is_some()
            {
                return Err(invalid(
                    "execution checkpoint contains an invalid owner receipt",
                ));
            }
        }
        let mut commands = std::collections::HashSet::with_capacity(self.pending_mutations.len());
        for pending in &self.pending_mutations {
            use super::operations::{MutationEffect, MutationKind};
            if !matches!(
                (pending.kind, &pending.effect),
                (MutationKind::Submit, MutationEffect::Submit)
                    | (MutationKind::Cancel, MutationEffect::Cancel)
                    | (MutationKind::Close, MutationEffect::Close { .. })
                    | (MutationKind::TakeProfit, MutationEffect::TakeProfit(_))
                    | (MutationKind::StopLoss, MutationEffect::StopLoss(_))
                    | (MutationKind::Replace, MutationEffect::Replace { .. })
                    | (MutationKind::AmendEntry, MutationEffect::AmendEntry { .. })
            ) {
                return Err(invalid("pending mutation kind/effect mismatch"));
            }
            let finite = match &pending.effect {
                MutationEffect::AmendEntry { limit_price, stop_price } => {
                    (limit_price.is_some() || stop_price.is_some())
                        && limit_price.iter().chain(stop_price.iter())
                            .all(|price| price.is_finite() && *price > 0.0)
                }
                MutationEffect::Close { price } => price.is_none_or(f64::is_finite),
                MutationEffect::Replace { price, quantity } => {
                    price.is_finite() && quantity.is_finite()
                }
                MutationEffect::TakeProfit(levels) | MutationEffect::StopLoss(levels) => {
                    levels.iter().flatten().all(|number| number.is_finite())
                }
                _ => true,
            };
            if !finite {
                return Err(invalid("pending mutation contains a non-finite number"));
            }
            if pending.receipt.insight_id != owner
                || !commands.insert(&pending.receipt.id)
                || owned
                    .get(&pending.receipt.id)
                    .is_some_and(|receipt| **receipt != pending.receipt)
            {
                return Err(invalid(
                    "execution checkpoint contains a mismatched/duplicate receipt",
                ));
            }
        }
        Ok(())
    }

    pub fn retains(&self, receipt: &BrokerCommandReceipt) -> bool {
        self.pending_mutations
            .iter()
            .any(|pending| pending.receipt.id == receipt.id)
            || self
                .owner_receipts
                .iter()
                .any(|owned| owned.id == receipt.id)
    }

    /// The caller must resolve child IDs and bind the new runtime clock before
    /// installation. Runtime closures and stale process pointers are never saved.
    pub fn into_parts(self) -> (Insight, Vec<Uuid>, Option<InsightPipelineCheckpoint>, bool) {
        let insight = Insight {
            insight_id: self.insight_id,
            parent_id: self.parent_id,
            is_split_insight: self.is_split_insight,
            state: self.state,
            order_id: self.order_id,
            side: self.side,
            symbol: self.symbol,
            quantity: self.quantity,
            contracts: self.contracts,
            order_type: self.order_type,
            order_class: self.order_class,
            limit_price: self.limit_price,
            stop_price: self.stop_price,
            take_profit_levels: self.take_profit_levels,
            stop_loss_levels: self.stop_loss_levels,
            trailing_stop_price: self.trailing_stop_price,
            strategy_type: self.strategy_type,
            confidence: self.confidence,
            timeframe: self.timeframe,
            period_unfilled: self.period_unfilled,
            period_till_tp: self.period_till_tp,
            execution_depends: self.execution_depends,
            filled_price: self.filled_price,
            close_order_id: self.close_order_id,
            close_price: self.close_price,
            broker_realized_pnl: self.broker_realized_pnl,
            position_accounting: self.position_accounting,
            commission: self.commission,
            swap: self.swap,
            partial_closes: self.partial_closes,
            created_at: self.created_at,
            updated_at: self.updated_at,
            filled_at: self.filled_at,
            closed_at: self.closed_at,
            legs: self.legs,
            market_changed: self.market_changed,
            submitted: self.submitted,
            pending_mutations: self.pending_mutations,
            execution_data: self.execution_data,
            suppress_child_entries: self.suppress_child_entries,
            cancelling: self.cancelling,
            closing: self.closing,
            first_on_fill: self.first_on_fill,
            entry_filled_quantity: self.entry_filled_quantity,
            partial_filled_quantity: self.partial_filled_quantity,
            state_history: self.state_history,
            state_history_kinds: self.state_history_kinds,
            children: Vec::new(),
            context: None,
        };
        (insight, self.child_ids, self.pipeline, self.materialized)
    }
}
