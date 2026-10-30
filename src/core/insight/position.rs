use super::{Insight, types::InsightState};
use crate::core::broker::{
    snapshots::{SnapshotKind, SnapshotUnavailable},
    types::{BrokerError, Position},
};

impl Insight {
    /// Current exact-order snapshot without RPC, state mutation or stored P&L.
    /// `Ok(None)` means this entry has no open position (including pending
    /// Executed orders). Missing/stale broker observations return an error.
    ///
    /// ```ignore
    /// if let Some(position) = insight.get_position()? {
    ///     println!("{}: {}", position.asset.symbol, position.unrealized_pnl);
    /// }
    /// ```
    pub fn get_position(&self) -> Result<Option<Position>, BrokerError> {
        if !matches!(self.state, InsightState::Executed | InsightState::Filled) {
            return Ok(None);
        }
        let Some(order) = self.order_id.as_deref().filter(|id| !id.is_empty()) else {
            return Ok(None);
        };
        let context = self
            .context
            .as_ref()
            .ok_or(BrokerError::SnapshotUnavailable {
                kind: SnapshotKind::Positions,
                reason: SnapshotUnavailable::Missing,
            })?;
        let position = context.position(order, &self.symbol)?;
        if let Some(position) = position.as_ref() {
            if position.order_id.as_deref() != Some(order)
                || position.asset.symbol != self.symbol
            {
                return Err(BrokerError::SnapshotUnavailable {
                    kind: SnapshotKind::Positions,
                    reason: SnapshotUnavailable::Invalid,
                });
            }
        }
        Ok(position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::insight::InsightStrategyContext;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn exact_position_lookup_is_live_read_only_and_pending_is_not_an_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut insight = Insight::default();
        insight.symbol = "BTCUSD".into();
        insight.order_id = Some("entry-a".into());
        insight.bind_context(
            InsightStrategyContext::new(chrono::Utc::now).with_position_reader(Some(Arc::new(
                move |order, symbol| {
                    assert_eq!((order, symbol), ("entry-a", "BTCUSD"));
                    count.fetch_add(1, Ordering::Relaxed);
                    Ok(None)
                },
            ))),
        );
        let before = insight.snapshot_fingerprint_hash();
        assert!(insight.get_position().unwrap().is_none()); // New: no broker lookup.
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        insight.state = InsightState::Executed;
        let executed = insight.snapshot_fingerprint_hash();
        assert!(insight.get_position().unwrap().is_none()); // Pending order, confirmed empty snapshot.
        assert_eq!(insight.snapshot_fingerprint_hash(), executed);
        assert_ne!(before, executed);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        insight.state = InsightState::Filled;
        insight.context = None;
        assert!(matches!(
            insight.get_position(),
            Err(BrokerError::SnapshotUnavailable {
                reason: SnapshotUnavailable::Missing,
                ..
            })
        ));
    }
}
