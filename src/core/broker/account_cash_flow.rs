//! Read-only account-level observations, independent of the trading reducer.
use super::types::BrokerError;
pub use aqe_live_protocol::{
    AccountCashFlowCoverage, AccountCashFlowKind, AccountCashFlowPage, AccountCashFlowRecord,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[async_trait::async_trait]
pub trait AccountCashFlowReader: Send + Sync {
    async fn read(
        &self,
        account_id: &str,
        source_id: Option<&str>,
        after_sequence: u64,
    ) -> Result<Option<AccountCashFlowPage>, BrokerError>;
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct AccountCashFlows {
    pub provider: String,
    pub source_id: String,
    pub account_id: String,
    pub currency: String,
    pub from_us: i64,
    pub through_us: i64,
    pub cursor: u64,
    pub through_sequence: u64,
    pub coverage: AccountCashFlowCoverage,
    pub missing_prefix: bool,
    pub capacity_exceeded: bool,
    pub records: BTreeMap<String, AccountCashFlowRecord>,
    #[serde(skip)]
    last_page: Option<(u64, [u8; 32])>,
}
impl AccountCashFlows {
    pub fn from_page(page: &AccountCashFlowPage) -> Result<Self, BrokerError> {
        page.validate(&page.account_id)
            .map_err(|e| BrokerError::AccountError(e.into()))?;
        if page.after_sequence != page.first_after_sequence {
            return Err(BrokerError::AccountError(
                "cash-flow source must begin at its retained boundary".into(),
            ));
        }
        let mut state = Self {
            provider: page.provider.clone(),
            source_id: page.source_id.clone(),
            account_id: page.account_id.clone(),
            currency: page.currency.clone(),
            from_us: page.from_us,
            through_us: page.through_us,
            cursor: page.after_sequence,
            through_sequence: page.through_sequence,
            coverage: page.coverage,
            missing_prefix: page.first_after_sequence != 0,
            capacity_exceeded: false,
            records: BTreeMap::new(),
            last_page: None,
        };
        state.apply(page)?;
        Ok(state)
    }
    pub fn apply(&mut self, page: &AccountCashFlowPage) -> Result<(), BrokerError> {
        page.validate(&self.account_id)
            .map_err(|e| BrokerError::AccountError(e.into()))?;
        let digest: [u8; 32] =
            Sha256::digest(serde_json::to_vec(page).expect("validated cash-flow page")).into();
        if self.cursor == page.next_sequence
            && self.last_page == Some((page.after_sequence, digest))
        {
            return Ok(());
        }
        if self.provider != page.provider
            || self.source_id != page.source_id
            || self.currency != page.currency
            || self.from_us != page.from_us
            || page.after_sequence != self.cursor
            || page.through_us < self.through_us
            || page.through_sequence < self.through_sequence
        {
            return Err(BrokerError::AccountError(
                "account cash-flow scope/cursor changed".into(),
            ));
        }
        // Reserve/check before changing state. Never evict evidence and pretend
        // the remaining records are complete.
        let additional = page
            .records
            .iter()
            .filter(|r| !self.records.contains_key(&r.event_id))
            .map(|r| r.event_id.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        if self.records.len().saturating_add(additional)
            > aqe_live_protocol::ACCOUNT_CASH_FLOW_RETAINED_ROWS
        {
            self.capacity_exceeded = true;
            return Err(BrokerError::AccountError(
                "account cash-flow retention capacity reached".into(),
            ));
        }
        for r in &page.records {
            self.records.insert(r.event_id.clone(), r.clone());
        }
        self.cursor = page.next_sequence;
        self.through_sequence = page.through_sequence;
        self.through_us = page.through_us;
        self.coverage = page.coverage;
        self.missing_prefix |= page.first_after_sequence > 0;
        self.last_page = Some((page.after_sequence, digest));
        Ok(())
    }
}

#[async_trait::async_trait]
impl AccountCashFlowReader for super::paper_broker::PaperBroker {
    async fn read(
        &self,
        account_id: &str,
        _source_id: Option<&str>,
        _after: u64,
    ) -> Result<Option<AccountCashFlowPage>, BrokerError> {
        use super::traits::{Broker, OrderManagementProvider};
        if !self.is_connected() {
            return Ok(None);
        }
        let a = self.get_account().await?;
        if a.account_id != account_id {
            return Err(BrokerError::AccountError(
                "paper cash-flow account mismatch".into(),
            ));
        }
        let now = chrono::Utc::now().timestamp_micros();
        Ok(Some(AccountCashFlowPage {
            version: 1,
            provider: "paper-closed-account-v1".into(),
            source_id: format!("paper:{}", a.account_id),
            account_id: a.account_id,
            currency: a.currency,
            from_us: 1,
            through_us: now,
            coverage: AccountCashFlowCoverage::ClosedSimulation,
            first_after_sequence: 0,
            after_sequence: 0,
            next_sequence: 0,
            through_sequence: 0,
            records: vec![],
        }))
    }
}

#[async_trait::async_trait]
impl AccountCashFlowReader for super::mt5_broker::Mt5Broker {
    async fn read(
        &self,
        account: &str,
        source: Option<&str>,
        after: u64,
    ) -> Result<Option<AccountCashFlowPage>, BrokerError> {
        self.read_account_cash_flows(account, source, after).await
    }
}

#[cfg(test)]
mod tests;
