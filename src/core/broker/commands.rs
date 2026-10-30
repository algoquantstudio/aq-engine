//! Immutable broker-bound data. No strategy context, nested insights, history or
//! mutable collection references may cross the execution task boundary.
use super::types::{Order, OrderClass, OrderSide, OrderType};
use crate::core::insight::Insight;
use serde::{Deserialize, Serialize};

/// Account-scoped identity plus a digest of the complete immutable request.
/// The digest detects accidental reuse of a client ID with different terms.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionCorrelation {
    pub account_id: String,
    pub key: String,
    pub fingerprint: String,
}
impl SubmissionCorrelation {
    pub fn new(
        account_id: &str,
        request: &BrokerOrderRequest,
    ) -> Result<Self, super::types::BrokerError> {
        use sha2::{Digest, Sha256};
        let account_id = account_id.trim();
        if account_id.is_empty()
            || account_id.len() > 512
            || request.client_order_id.trim().is_empty()
            || request.client_order_id.len() > 512
        {
            return Err(super::types::BrokerError::TradeError(
                "invalid submission correlation identity".into(),
            ));
        }
        let key = serde_json::to_vec(&("aqe-submission-v1", account_id, &request.client_order_id))
            .map_err(|e| super::types::BrokerError::TradeError(e.to_string()))?;
        let bytes = serde_json::to_vec(&("aqe-submission-terms-v1", account_id, request))
            .map_err(|e| super::types::BrokerError::TradeError(e.to_string()))?;
        Ok(Self {
            account_id: account_id.into(),
            key: hex::encode(Sha256::digest(key)),
            fingerprint: hex::encode(Sha256::digest(bytes)),
        })
    }
}

/// Unknown is not rejection and must never authorize replaying a submission.
#[derive(Clone, Debug)]
pub enum SubmissionResolution {
    Unsupported,
    Unknown,
    Confirmed(Order),
}

/// The bounded execution payload for one insight, not its complete strategy tree.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BrokerOrderRequest {
    /// Stable across retries/recovery of the same entry intent. This is a client
    /// correlation ID, never an assertion that the broker created an order.
    pub client_order_id: String,
    pub insight_id: String,
    pub strategy_type: String,
    pub symbol: String,
    pub quantity: Option<f64>,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub order_class: OrderClass,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub take_profit_levels: Option<Vec<f64>>,
    pub stop_loss_levels: Option<Vec<f64>>,
    pub trailing_stop_price: Option<f64>,
}

impl From<&Insight> for BrokerOrderRequest {
    fn from(insight: &Insight) -> Self {
        let insight_id = insight.insight_id.to_string();
        Self {
            client_order_id: insight_id.clone(),
            insight_id,
            strategy_type: insight.strategy_type.to_string(),
            symbol: insight.symbol.clone(),
            quantity: insight.quantity,
            side: insight.side.clone(),
            order_type: insight.order_type.clone(),
            order_class: insight.order_class.clone(),
            limit_price: insight.limit_price,
            stop_price: insight.stop_price,
            take_profit_levels: insight.take_profit_levels.clone(),
            stop_loss_levels: insight.stop_loss_levels.clone(),
            trailing_stop_price: insight.trailing_stop_price,
        }
    }
}

impl From<Insight> for BrokerOrderRequest {
    fn from(insight: Insight) -> Self {
        Self::from(&insight)
    }
}

pub const BROKER_COMMAND_VERSION: u16 = 1;
/// Bound both queued memory and persisted decoding work; child trees never belong here.
pub const MAX_BROKER_COMMAND_BYTES: usize = 16 * 1024;

/// Local dispatch-only limits captured with an immutable command. These are
/// journaled for crash recovery but never added to the broker wire request.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BrokerDispatchConstraints {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_total_symbol_quantity: Option<f64>,
}

impl BrokerDispatchConstraints {
    fn is_empty(&self) -> bool {
        self.max_total_symbol_quantity.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BrokerCommandId(pub String);

impl std::fmt::Display for BrokerCommandId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A newer version supersedes an older target for the same logical operation group.
/// Completions carry this unchanged so the strategy reducer can reject stale results.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerOperationGroup {
    pub id: String,
    pub version: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BrokerCommandAction {
    AmendEntry {
        order_id: String,
        limit_price: Option<f64>,
        stop_price: Option<f64>,
    },
    Submit {
        request: BrokerOrderRequest,
    },
    Cancel {
        order_id: String,
    },
    Close {
        order_id: String,
        quantity: f64,
        price: Option<f64>,
    },
    Replace {
        order_id: String,
        price: f64,
        quantity: f64,
    },
    TakeProfit {
        order_id: String,
        price: f64,
        quantity: f64,
    },
    StopLoss {
        order_id: String,
        price: f64,
        quantity: f64,
    },
}

#[cfg(test)]
mod terminal_protection_tests {
    use super::*;
    use crate::core::insight::types::InsightState;

    #[test]
    fn obsolete_protection_requires_terminal_state_and_exact_ticket() {
        for action in [
            BrokerCommandAction::StopLoss { order_id: "target".into(), price: 90.0, quantity: 1.0 },
            BrokerCommandAction::TakeProfit { order_id: "target".into(), price: 110.0, quantity: 1.0 },
        ] {
            for state in [InsightState::Closed, InsightState::Cancelled] {
                assert!(action.settled_by_terminal(Some("target"), &state));
                assert!(!action.settled_by_terminal(Some("other"), &state));
                assert!(!action.settled_by_terminal(None, &state));
            }
            for state in [InsightState::New, InsightState::Executed, InsightState::Filled, InsightState::Rejected] {
                assert!(!action.settled_by_terminal(Some("target"), &state));
            }
        }
        // Replacement may have created another order; closing the old one is insufficient.
        assert!(!BrokerCommandAction::Replace { order_id: "target".into(), price: 100.0, quantity: 1.0 }
            .settled_by_terminal(Some("target"), &InsightState::Closed));
    }
}

impl BrokerCommandAction {
    /// Only terminal evidence for the exact targeted order can settle an
    /// uncertain cleanup RPC or obsolete in-place protection update. This does
    /// not prove the update succeeded; the terminal order no longer needs it.
    /// Entries and replacements still require their own broker evidence.
    pub(crate) fn settled_by_terminal(
        &self,
        order_id: Option<&str>,
        state: &crate::core::insight::types::InsightState,
    ) -> bool {
        use crate::core::insight::types::InsightState;
        self.order_id().is_some_and(|id| Some(id) == order_id)
            && match self {
                Self::Close { .. } => *state == InsightState::Closed,
                Self::Cancel { .. } => matches!(
                    state,
                    InsightState::Cancelled | InsightState::Rejected | InsightState::Closed
                ),
                Self::TakeProfit { .. } | Self::StopLoss { .. } => matches!(
                    state,
                    InsightState::Closed | InsightState::Cancelled
                ),
                _ => false,
            }
    }
    pub fn is_entry(&self) -> bool {
        matches!(self, Self::Submit { .. })
    }

    pub fn is_teardown(&self) -> bool {
        matches!(self, Self::Cancel { .. } | Self::Close { .. })
    }

    pub fn order_id(&self) -> Option<&str> {
        match self {
            Self::Submit { .. } => None,
            Self::Cancel { order_id }
            | Self::Close { order_id, .. }
            | Self::Replace { order_id, .. }
            | Self::AmendEntry { order_id, .. }
            | Self::TakeProfit { order_id, .. }
            | Self::StopLoss { order_id, .. } => Some(order_id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BrokerCommand {
    pub version: u16,
    pub id: BrokerCommandId,
    pub strategy_id: String,
    pub account_id: String,
    pub insight_id: String,
    pub group: Option<BrokerOperationGroup>,
    #[serde(default, skip_serializing_if = "BrokerDispatchConstraints::is_empty")]
    pub dispatch_constraints: BrokerDispatchConstraints,
    pub action: BrokerCommandAction,
}

impl BrokerCommand {
    fn entry_id(strategy_id: &str, account_id: &str, client_order_id: &str) -> uuid::Uuid {
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            &serde_json::to_vec(&("aqe-entry-v1", strategy_id, account_id, client_order_id))
                .expect("string tuple serialization is infallible"),
        )
    }

    pub fn new(
        strategy_id: &str,
        account_id: &str,
        insight_id: &str,
        action: BrokerCommandAction,
        group: Option<BrokerOperationGroup>,
    ) -> Self {
        Self::new_with_constraints(
            strategy_id,
            account_id,
            insight_id,
            action,
            group,
            BrokerDispatchConstraints::default(),
        )
    }

    pub fn new_with_constraints(
        strategy_id: &str,
        account_id: &str,
        insight_id: &str,
        action: BrokerCommandAction,
        group: Option<BrokerOperationGroup>,
        dispatch_constraints: BrokerDispatchConstraints,
    ) -> Self {
        let strategy_id = strategy_id.trim().to_owned();
        let account_id = account_id.trim().to_owned();
        // An entry has one identity across process recovery, even if the caller no
        // longer has its receipt. Length-prefixed serialization avoids delimiter collisions.
        let id = match &action {
            BrokerCommandAction::Submit { request } => {
                Self::entry_id(&strategy_id, &account_id, &request.client_order_id)
            }
            _ => uuid::Uuid::new_v4(),
        };
        Self {
            version: BROKER_COMMAND_VERSION,
            id: BrokerCommandId(id.to_string()),
            strategy_id,
            account_id,
            insight_id: insight_id.to_owned(),
            group,
            dispatch_constraints,
            action,
        }
    }

    pub fn receipt(&self) -> BrokerCommandReceipt {
        BrokerCommandReceipt {
            id: self.id.clone(),
            insight_id: self.insight_id.clone(),
            group: self.group.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), BrokerCommandError> {
        if self.version != BROKER_COMMAND_VERSION {
            return Err(BrokerCommandError::Invalid(
                "unsupported command version".into(),
            ));
        }
        for value in [
            &self.id.0,
            &self.strategy_id,
            &self.account_id,
            &self.insight_id,
        ] {
            if value.is_empty() || value.trim() != value || value.len() > 512 {
                return Err(BrokerCommandError::Invalid(
                    "invalid command identity".into(),
                ));
            }
        }
        if self
            .group
            .as_ref()
            .is_some_and(|g| g.id.trim().is_empty() || g.id.len() > 512)
        {
            return Err(BrokerCommandError::Invalid(
                "invalid operation group".into(),
            ));
        }
        let positive = |v: f64| v.is_finite() && v > 0.0;
        if self
            .dispatch_constraints
            .max_total_symbol_quantity
            .is_some_and(|value| !positive(value))
            || (!self.dispatch_constraints.is_empty() && !self.action.is_entry())
        {
            return Err(BrokerCommandError::Invalid(
                "invalid dispatch quantity constraint".into(),
            ));
        }
        let valid = match &self.action {
            BrokerCommandAction::Submit { request } => {
                if request.client_order_id.len() > 512
                    || request.strategy_type.len() > 512
                    || request.symbol.len() > 128
                    || request
                        .take_profit_levels
                        .as_ref()
                        .is_some_and(|v| v.len() > 256)
                    || request
                        .stop_loss_levels
                        .as_ref()
                        .is_some_and(|v| v.len() > 256)
                {
                    return Err(BrokerCommandError::TooLarge);
                }
                if self.id.0
                    != Self::entry_id(
                        &self.strategy_id,
                        &self.account_id,
                        &request.client_order_id,
                    )
                    .to_string()
                {
                    return Err(BrokerCommandError::Invalid(
                        "submit command identity must be stable".into(),
                    ));
                }
                request.insight_id == self.insight_id
                    && !request.client_order_id.trim().is_empty()
                    && !request.symbol.trim().is_empty()
                    && request.quantity.is_some_and(positive)
                    && [
                        request.limit_price,
                        request.stop_price,
                        request.trailing_stop_price,
                    ]
                    .into_iter()
                    .flatten()
                    .all(positive)
                    && request
                        .take_profit_levels
                        .iter()
                        .flatten()
                        .chain(request.stop_loss_levels.iter().flatten())
                        .all(|v| positive(*v))
            }
            BrokerCommandAction::Cancel { .. } => true,
            BrokerCommandAction::AmendEntry {
                limit_price,
                stop_price,
                ..
            } => {
                (limit_price.is_some() || stop_price.is_some())
                    && limit_price.is_none_or(positive)
                    && stop_price.is_none_or(positive)
            }
            BrokerCommandAction::Close {
                quantity, price, ..
            } => positive(*quantity) && price.is_none_or(positive),
            BrokerCommandAction::Replace {
                quantity, price, ..
            }
            | BrokerCommandAction::TakeProfit {
                quantity, price, ..
            }
            | BrokerCommandAction::StopLoss {
                quantity, price, ..
            } => positive(*quantity) && positive(*price),
        };
        if !valid
            || self
                .action
                .order_id()
                .is_some_and(|id| id.trim().is_empty() || id.len() > 512)
        {
            return Err(BrokerCommandError::Invalid(
                "invalid broker command payload".into(),
            ));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<String, BrokerCommandError> {
        self.validate()?;
        let json =
            serde_json::to_string(self).map_err(|e| BrokerCommandError::Invalid(e.to_string()))?;
        if json.len() > MAX_BROKER_COMMAND_BYTES {
            return Err(BrokerCommandError::TooLarge);
        }
        Ok(json)
    }
}

/// Queue acceptance only. It is neither a broker acknowledgement nor a state transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerCommandReceipt {
    pub id: BrokerCommandId,
    pub insight_id: String,
    pub group: Option<BrokerOperationGroup>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BrokerCommandOutcome {
    /// Inspect the returned status: it may report acceptance, a fill or rejection.
    OrderResponse {
        order: Order,
    },
    Acknowledged,
    Declined {
        reason: String,
    },
    /// A transport error/timeout is not proof that the broker rejected a mutation.
    Ambiguous {
        reason: String,
    },
    /// The broker was never called. This is not an Insight Cancelled transition.
    CancelledBeforeDispatch,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrokerCommandCompletion {
    pub receipt: BrokerCommandReceipt,
    pub queued_at_us: i64,
    pub dispatched_at_us: Option<i64>,
    pub completed_at_us: i64,
    pub outcome: BrokerCommandOutcome,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BrokerCommandError {
    Declined(String),
    Invalid(String),
    TooLarge,
    QueueFull,
    /// Another optional evidence operation owns the single analytics permit.
    AnalyticsBusy,
    /// The serialized journal request channel (not broker order admission) is full.
    AnalyticsQueueFull,
    /// Only optional analytics capture was refused. This never rejects a
    /// broker command, checkpoint, receipt or stop operation.
    CapturePaused,
    Closed,
    Stopping,
    IdentityConflict,
    Journal(String),
}

impl std::fmt::Display for BrokerCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Declined(reason) => write!(f, "Broker declined command: {reason}"),
            Self::Invalid(reason) => write!(f, "Invalid broker command: {reason}"),
            Self::TooLarge => write!(f, "Broker command exceeds {MAX_BROKER_COMMAND_BYTES} bytes"),
            Self::QueueFull => {
                f.write_str("Broker command queue is full; defer and retry admission")
            }
            Self::AnalyticsBusy => f.write_str("Analytics reader is busy; defer and retry"),
            Self::AnalyticsQueueFull => f.write_str("Analytics journal request queue is full; defer and retry"),
            Self::CapturePaused => f.write_str(
                "New optional analytics capture is paused; no journal receipt was issued",
            ),
            Self::Closed => f.write_str("Broker command worker is closed"),
            Self::Stopping => f.write_str("Broker entry admission is stopped"),
            Self::IdentityConflict => {
                f.write_str("Broker command identity conflicts with its durable intent")
            }
            Self::Journal(reason) => write!(f, "Broker command journal: {reason}"),
        }
    }
}

impl std::error::Error for BrokerCommandError {}

impl BrokerCommandError {
    pub(crate) fn is_analytics_backpressure(&self) -> bool {
        matches!(self, Self::AnalyticsBusy | Self::AnalyticsQueueFull)
    }
}

#[cfg(test)]
mod submission_correlation_tests {
    use super::*;

    #[test]
    fn stable_identity_is_account_scoped_and_terms_are_fingerprinted() {
        let mut request = BrokerOrderRequest::from(&Insight::default());
        request.symbol = "ETHUSD".into();
        request.quantity = Some(1.0);
        let original = SubmissionCorrelation::new(" account ", &request).unwrap();
        assert_eq!(
            original,
            SubmissionCorrelation::new("account", &request).unwrap()
        );
        assert_eq!(original.key.len(), 64);
        assert_eq!(original.fingerprint.len(), 64);
        assert_ne!(
            original.key,
            SubmissionCorrelation::new("other", &request).unwrap().key
        );
        request.quantity = Some(2.0);
        let changed = SubmissionCorrelation::new("account", &request).unwrap();
        assert_eq!(original.key, changed.key);
        assert_ne!(original.fingerprint, changed.fingerprint);
        request.client_order_id = "another-client".into();
        assert_ne!(
            original.key,
            SubmissionCorrelation::new("account", &request).unwrap().key
        );
    }

    #[test]
    fn submission_identity_is_bounded_and_rejects_missing_account_or_client() {
        let mut request = BrokerOrderRequest::from(&Insight::default());
        for account in [String::new(), " ".into(), "x".repeat(513)] {
            assert!(SubmissionCorrelation::new(&account, &request).is_err());
        }
        for client in [String::new(), " ".into(), "x".repeat(513)] {
            request.client_order_id = client;
            assert!(SubmissionCorrelation::new("account", &request).is_err());
        }
    }

    #[test]
    fn ea_receipt_fences_submission_before_mutation_and_lookup_is_read_only() {
        // Source contract only: this does not replace compilation in MetaEditor.
        let ea = include_str!("../../../integrations/mt5/AqeMt5BridgeEA.mq5");
        let begin = ea
            .find("int submission_receipt = BeginSubmissionReceipt")
            .unwrap();
        let send = ea[begin..].find("trade.Sell(qty").unwrap() + begin;
        let save = ea[send..]
            .find("FileWriteString(submission_receipt, payload")
            .unwrap()
            + send;
        let reply = ea[save..].find("SendRpcResponse").unwrap() + save;
        assert!(begin < send && send < save && save < reply);
        let lookup = ea
            .split("if(action == \"GET_SUBMISSION\")")
            .nth(1)
            .unwrap()
            .split("if(action == \"GET_ACCOUNT\")")
            .next()
            .unwrap();
        assert!(lookup.contains("LookupSubmissionReceipt"));
        assert!(!lookup.contains("trade."));
        assert!(ea.contains("submission outcome unknown; replay refused"));
        assert!(
            super::super::types::Mt5RpcAction::GetSubmission.can_retry_after_transport_failure()
        );
        assert!(
            !super::super::types::Mt5RpcAction::SubmitOrder.can_retry_after_transport_failure()
        );
    }
}
