//! Read-only broker economics. Never changes Insight state or trading P&L helpers.
use super::{Insight, types::InsightState};
use aqe_live_protocol::PositionCloseAccounting;

impl Insight {
    pub(crate) fn observe_position_accounting(
        &mut self,
        accounting: &PositionCloseAccounting,
    ) -> bool {
        if self.state != InsightState::Closed
            || accounting.validate().is_err()
            || accounting.as_of_us
                > chrono::Utc::now()
                    .timestamp_micros()
                    .saturating_add(5_000_000)
            || !self
                .order_id
                .as_deref()
                .is_some_and(|id| accounting.owns_order(id))
        {
            return false;
        }
        if let Some(previous) = &self.position_accounting {
            if previous.provider != accounting.provider
                || previous.position_id != accounting.position_id
                || previous.entry_order_id != accounting.entry_order_id
                || previous.currency != accounting.currency
                || previous.opened_at_us != accounting.opened_at_us
                || previous.closed_at_us != accounting.closed_at_us
                || accounting.as_of_us <= previous.as_of_us
            {
                return false;
            }
            // Retain the observation watermark even when amounts are equal:
            // otherwise an older, different correction could overwrite a more
            // recent unchanged read. This is metadata, never an additional fill.
        }
        self.position_accounting = Some(accounting.clone());
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence() -> PositionCloseAccounting {
        PositionCloseAccounting {
            version: 1,
            provider: "test-v1".into(),
            position_id: "position-a".into(),
            entry_order_id: "entry-a".into(),
            currency: "USD".into(),
            opened_at_us: 100,
            closed_at_us: 200,
            as_of_us: 300,
            net_pnl: 95.,
            commission: 7.,
            swap: 2.,
        }
    }
    fn closed() -> Insight {
        let mut row = Insight::default();
        row.symbol = "GENERIC-ASSET".into();
        row.state = InsightState::Closed;
        row.order_id = Some("entry-a".into());
        row
    }
    #[test]
    fn position_accounting_requires_closed_owned_valid_evidence() {
        let mut row = closed();
        let original = evidence();
        row.state = InsightState::Filled;
        assert!(!row.observe_position_accounting(&original));
        row.state = InsightState::Closed;
        let mut invalid = original.clone();
        invalid.entry_order_id = "another-owner".into();
        assert!(!row.observe_position_accounting(&invalid));
        invalid = original.clone();
        invalid.net_pnl = f64::NAN;
        assert!(!row.observe_position_accounting(&invalid));
        invalid = original.clone();
        invalid.as_of_us = chrono::Utc::now().timestamp_micros() + 60_000_000;
        assert!(!row.observe_position_accounting(&invalid));
        assert!(row.observe_position_accounting(&original));
        assert_eq!(row.state, InsightState::Closed);
        assert!(row.closed_at.is_none()); // Metadata does not rewrite trading timestamps.
    }
    #[test]
    fn position_accounting_newer_revisions_replace_never_accumulate() {
        let mut row = closed();
        let mut value = evidence();
        assert!(row.observe_position_accounting(&value));
        assert!(!row.observe_position_accounting(&value));
        value.as_of_us = 500;
        assert!(row.observe_position_accounting(&value)); // Preserve unchanged watermark.
        value.as_of_us = 400;
        value.net_pnl = 99.;
        assert!(!row.observe_position_accounting(&value));
        value.as_of_us = 600;
        assert!(row.observe_position_accounting(&value));
        assert_eq!(row.position_accounting.as_ref().unwrap().net_pnl, 99.);
        for field in ["currency", "position", "opened"] {
            let mut wrong = value.clone();
            wrong.as_of_us += 1;
            match field {
                "currency" => wrong.currency = "EUR".into(),
                "position" => wrong.position_id = "b".into(),
                _ => wrong.opened_at_us += 1,
            }
            assert!(!row.observe_position_accounting(&wrong));
        }
    }
    #[test]
    fn position_accounting_checkpoint_roundtrip_and_fingerprint() {
        use crate::core::insight::execution_checkpoint::InsightExecutionCheckpoint;
        let mut row = closed();
        let before = row.snapshot_fingerprint_hash();
        assert!(row.observe_position_accounting(&evidence()));
        assert_ne!(row.snapshot_fingerprint_hash(), before);
        let saved = InsightExecutionCheckpoint::capture(&row, 1, true, None);
        assert_eq!(saved.version, 4);
        saved.validate().unwrap();
        let json = serde_json::to_string(&saved).unwrap();
        let restored: InsightExecutionCheckpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(
            restored.into_parts().0.position_accounting,
            row.position_accounting
        );
        let mut invalid = saved;
        invalid.version = 3;
        assert!(invalid.validate().is_err());
    }
}
