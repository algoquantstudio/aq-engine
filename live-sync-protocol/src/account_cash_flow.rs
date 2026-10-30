//! Account-level money movements. Never an Insight, fill or strategy profit.
use serde::{Deserialize, Serialize};
pub const ACCOUNT_CASH_FLOW_VERSION: u16 = 1;
pub const ACCOUNT_CASH_FLOW_PAGE_ROWS: usize = 32;
pub const ACCOUNT_CASH_FLOW_RETAINED_ROWS: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountCashFlowKind {
    Deposit,
    Withdrawal,
    /// A broker balance operation is not necessarily an external transfer.
    Balance,
    Credit,
    Charge,
    Correction,
    Bonus,
    Commission,
    Interest,
    Dividend,
    Tax,
    CancelledTrade,
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountCashFlowCoverage {
    ObservedOnly,
    /// Complete producer-declared history over the stated interval.
    Complete,
    /// A simulation with no external-funding operation, not a broker assumption.
    ClosedSimulation,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountCashFlowRecord {
    pub sequence: u64,
    /// Stable broker transaction identity, independent of transport retries.
    pub event_id: String,
    pub kind: AccountCashFlowKind,
    /// Signed broker-posted account-currency delta; not an inferred balance diff.
    pub amount: f64,
    /// Unknown historical clock conversion stays unknown.
    pub occurred_at_us: Option<i64>,
    pub observed_at_us: i64,
    pub broker_time_ms: Option<i64>,
    #[serde(default)]
    pub deleted: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountCashFlowPage {
    pub version: u16,
    pub provider: String,
    pub source_id: String,
    pub account_id: String,
    pub currency: String,
    pub from_us: i64,
    pub through_us: i64,
    pub coverage: AccountCashFlowCoverage,
    pub first_after_sequence: u64,
    pub after_sequence: u64,
    pub next_sequence: u64,
    pub through_sequence: u64,
    pub records: Vec<AccountCashFlowRecord>,
}
fn identity(s: &str, max: usize) -> bool {
    !s.trim().is_empty() && s.len() <= max && !s.chars().any(char::is_control)
}
impl AccountCashFlowPage {
    pub fn validate(&self, account: &str) -> Result<(), &'static str> {
        if self.version != ACCOUNT_CASH_FLOW_VERSION
            || self.account_id != account
            || !identity(account, 128)
            || !identity(&self.currency, 32)
            || !identity(&self.provider, 128)
            || !identity(&self.source_id, 128)
            || self.from_us <= 0
            || self.through_us < self.from_us
            || self.after_sequence > self.next_sequence
            || self.next_sequence > self.through_sequence
            || self.through_sequence > i64::MAX as u64
            || self.first_after_sequence > self.after_sequence
            || self.records.len() > ACCOUNT_CASH_FLOW_PAGE_ROWS
            || self.next_sequence.saturating_sub(self.after_sequence) != self.records.len() as u64
            || (self.records.is_empty() && self.next_sequence < self.through_sequence)
            || (self.coverage == AccountCashFlowCoverage::ClosedSimulation
                && (self.through_sequence != 0 || !self.records.is_empty()))
        {
            return Err("invalid account cash-flow page");
        }
        for (i, r) in self.records.iter().enumerate() {
            if r.sequence != self.after_sequence + i as u64 + 1
                || !identity(&r.event_id, 128)
                || !r.amount.is_finite()
                || r.observed_at_us <= 0
                || r.observed_at_us > self.through_us
                || r.occurred_at_us
                    .is_some_and(|t| t <= 0 || t > r.observed_at_us.saturating_add(5_000_000))
                || r.broker_time_ms.is_some_and(|t| t <= 0)
                || (r.kind == AccountCashFlowKind::Deposit && r.amount < 0.)
                || (r.kind == AccountCashFlowKind::Withdrawal && r.amount > 0.)
            {
                return Err("invalid account cash-flow record");
            }
        }
        if crate::json_encoded_len(self)
            .map_err(|_| "invalid cash-flow encoding")?
            > 32 * 1024
        {
            return Err("account cash-flow page byte limit");
        }
        Ok(())
    }
}

/// End-of-interval flow convention, not exact time-weighted performance. Only
/// complete matching history is admissible; ambiguous balance entries fail closed.
pub fn cash_flow_adjusted_change_pct(
    previous: f64,
    current: f64,
    page: &AccountCashFlowPage,
    from_us: i64,
    through_us: i64,
) -> Option<f64> {
    page.validate(&page.account_id).ok()?;
    if !previous.is_finite()
        || previous <= 0.
        || !current.is_finite()
        || from_us >= through_us
        || page.coverage == AccountCashFlowCoverage::ObservedOnly
        || page.from_us > from_us
        || page.through_us < through_us
        || page.first_after_sequence != 0
        || page.after_sequence != 0
        || page.next_sequence != page.through_sequence
    {
        return None;
    }
    // A page is a revision stream. Count the latest broker fact, not every
    // transport revision of the same money operation.
    let latest = page
        .records
        .iter()
        .map(|r| (r.event_id.as_str(), r))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut net = 0.;
    for r in latest.values() {
        let at = r.occurred_at_us?;
        if at <= from_us || at > through_us {
            continue;
        }
        if r.deleted {
            return None;
        }
        match r.kind {
            AccountCashFlowKind::Deposit | AccountCashFlowKind::Withdrawal => net += r.amount,
            AccountCashFlowKind::Charge
            | AccountCashFlowKind::Commission
            | AccountCashFlowKind::Interest
            | AccountCashFlowKind::Dividend
            | AccountCashFlowKind::Tax => {}
            _ => return None,
        }
    }
    let value = (current - previous - net) / previous * 100.;
    value.is_finite().then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn page(kind: AccountCashFlowKind, amount: f64) -> AccountCashFlowPage {
        AccountCashFlowPage {
            version: 1,
            provider: "broker".into(),
            source_id: "source".into(),
            account_id: "account".into(),
            currency: "EUR".into(),
            from_us: 10,
            through_us: 100,
            coverage: AccountCashFlowCoverage::Complete,
            first_after_sequence: 0,
            after_sequence: 0,
            next_sequence: 1,
            through_sequence: 1,
            records: vec![AccountCashFlowRecord {
                sequence: 1,
                event_id: "deal".into(),
                kind,
                amount,
                occurred_at_us: Some(50),
                observed_at_us: 60,
                broker_time_ms: None,
                deleted: false,
            }],
        }
    }
    #[test]
    fn only_explicit_external_transfers_are_removed_not_costs_or_ambiguous_balance_entries() {
        assert_eq!(
            cash_flow_adjusted_change_pct(
                100.,
                160.,
                &page(AccountCashFlowKind::Deposit, 50.),
                10,
                100
            ),
            Some(10.)
        );
        assert_eq!(
            cash_flow_adjusted_change_pct(
                100.,
                60.,
                &page(AccountCashFlowKind::Withdrawal, -50.),
                10,
                100
            ),
            Some(10.)
        );
        assert_eq!(
            cash_flow_adjusted_change_pct(
                100.,
                95.,
                &page(AccountCashFlowKind::Charge, -5.),
                10,
                100
            ),
            Some(-5.)
        );
        for kind in [
            AccountCashFlowKind::Balance,
            AccountCashFlowKind::Credit,
            AccountCashFlowKind::Correction,
            AccountCashFlowKind::Bonus,
            AccountCashFlowKind::CancelledTrade,
            AccountCashFlowKind::Unknown,
        ] {
            assert_eq!(
                cash_flow_adjusted_change_pct(100., 160., &page(kind, 50.), 10, 100),
                None
            );
        }
    }
    #[test]
    fn unknown_coverage_clock_corrections_and_paginated_prefixes_cannot_qualify_returns() {
        let p = page(AccountCashFlowKind::Deposit, 50.);
        let mut v = p.clone();
        v.coverage = AccountCashFlowCoverage::ObservedOnly;
        assert_eq!(cash_flow_adjusted_change_pct(100., 160., &v, 10, 100), None);
        let mut v = p.clone();
        v.records[0].occurred_at_us = None;
        assert_eq!(cash_flow_adjusted_change_pct(100., 160., &v, 10, 100), None);
        let mut v = p.clone();
        v.records[0].deleted = true;
        assert_eq!(cash_flow_adjusted_change_pct(100., 160., &v, 10, 100), None);
        assert_eq!(cash_flow_adjusted_change_pct(100., 160., &p, 1, 100), None);
        assert_eq!(cash_flow_adjusted_change_pct(0., 160., &p, 10, 100), None);
    }
    #[test]
    fn revised_transfer_is_counted_once_and_a_deleted_fact_never_qualifies() {
        let mut p = page(AccountCashFlowKind::Deposit, 50.);
        let mut revision = p.records[0].clone();
        revision.sequence = 2;
        revision.amount = 40.;
        p.records.push(revision);
        p.next_sequence = 2;
        p.through_sequence = 2;
        assert_eq!(
            cash_flow_adjusted_change_pct(100., 150., &p, 10, 100),
            Some(10.)
        );
        p.records[1].deleted = true;
        assert_eq!(cash_flow_adjusted_change_pct(100., 150., &p, 10, 100), None);
    }
    #[test]
    fn page_is_bounded_scoped_and_chronological_without_assuming_a_broker_clock() {
        let p = page(AccountCashFlowKind::Balance, 50.);
        assert!(p.validate("foreign").is_err());
        let mut v = p.clone();
        v.records[0].occurred_at_us = None;
        v.records[0].broker_time_ms = Some(123);
        v.validate("account").unwrap();
        let mut v = p.clone();
        v.next_sequence = 2;
        assert!(v.validate("account").is_err());
        let mut v = p.clone();
        v.records[0].amount = f64::NAN;
        assert!(v.validate("account").is_err());
        let mut v = p;
        v.coverage = AccountCashFlowCoverage::ClosedSimulation;
        assert!(v.validate("account").is_err());
    }
}
