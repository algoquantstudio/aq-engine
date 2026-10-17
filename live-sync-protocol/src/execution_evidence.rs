//! Sanitized canonical execution-state evidence for read-only analytics.
//!
//! These are revisioned observations, NOT independent fills. In particular,
//! `broker_realized_pnl` must not be summed across revisions or combined with
//! partial-exit price P&L without a declared broker valuation contract.
use serde::{Deserialize, Serialize};

pub const EXECUTION_EVIDENCE_VERSION: u16 = 2;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEvidenceState {
    pub insight_id: String,
    pub parent_id: Option<String>,
    pub is_split_insight: bool,
    pub symbol: String,
    /// Strategy type used for attribution; instances of the same type are grouped.
    pub strategy_label: String,
    pub state: String,
    pub side: String,
    pub entry_order_id: Option<String>,
    pub close_order_id: Option<String>,
    pub intended_quantity: Option<f64>,
    pub entry_filled_quantity: Option<f64>,
    pub partial_closed_quantity: Option<f64>,
    pub entry_price: Option<f64>,
    pub close_price: Option<f64>,
    pub broker_realized_pnl: Option<f64>,
    pub commission: Option<f64>,
    pub swap: Option<f64>,
    /// Optional typed lifetime economics. Absent on legacy/unsupported adapters.
    /// This is not additive with the raw fields or partial closes above/below.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position_accounting: Option<crate::PositionCloseAccounting>,
    pub order_type: String,
    pub order_class: String,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub take_profit_levels: Option<Vec<f64>>,
    pub stop_loss_levels: Option<Vec<f64>>,
    pub trailing_stop_price: Option<f64>,
    pub created_at_us: i64,
    /// Runtime state-transition timestamps; not guaranteed broker execution time.
    pub filled_at_us: Option<i64>,
    pub closed_at_us: Option<i64>,
    /// Ordered canonical fragments. The same order may have multiple partials;
    /// no broker fill ID or timestamp is invented from vector position.
    pub partial_closes: Vec<ExecutionEvidencePartialClose>,
    /// Invalid numbers are unavailable, never converted to economic zero.
    pub invalid_numeric_fields: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEvidencePartialClose {
    pub order_id: String,
    pub side: String,
    pub quantity: Option<f64>,
    pub entry_price: Option<f64>,
    pub filled_price: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEvidenceRecord {
    /// Durable journal-local cursor, independent from broker delivery order.
    pub sequence: u64,
    pub checkpoint_revision: u64,
    pub observed_at_us: i64,
    pub recorded_at_us: i64,
    pub state: ExecutionEvidenceState,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEvidencePage {
    pub schema_version: u16,
    /// Persistent journal incarnation. Recreating a journal must start a new
    /// stream even if strategy/account, epoch and sequence happen to match.
    pub source_id: String,
    pub strategy_id: String,
    pub account_id: String,
    /// Execution continuity epoch. A stopped-and-restarted deployment is separate.
    pub execution_epoch: u64,
    /// Reserved until the producer can prove these identities. Epoch alone must
    /// not be relabelled as a session, configuration version, or run ID.
    pub run_id: Option<String>,
    pub session_id: Option<String>,
    pub configuration_version: Option<String>,
    pub after_sequence: u64,
    pub through_sequence: u64,
    pub next_sequence: u64,
    pub has_more: bool,
    pub records: Vec<ExecutionEvidenceRecord>,
    pub generated_at_us: i64,
    pub coverage: String,
    pub recovery_complete: Option<bool>,
    pub valuation_basis: String,
}

/// One source observation with its economic owner epoch. The exporting process
/// may be in a newer epoch when a late cost/fill correction is recorded.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEvidenceObservation {
    pub execution_epoch: u64,
    pub record: ExecutionEvidenceRecord,
}

/// Transport feed over every epoch of a single durable source. This is ordered
/// by journal commit sequence, not event time, so old-epoch corrections cannot
/// disappear behind a current-deployment-only subscription.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEvidenceFeedPage {
    pub schema_version: u16,
    pub source_id: String,
    pub strategy_id: String,
    pub account_id: String,
    pub after_sequence: u64,
    pub through_sequence: u64,
    pub next_sequence: u64,
    pub has_more: bool,
    pub observations: Vec<ExecutionEvidenceObservation>,
    pub generated_at_us: i64,
    pub coverage: String,
    pub recovery_complete: Option<bool>,
    pub valuation_basis: String,
}
