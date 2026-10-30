//! Scheduled broker marks, separate from Insight state and trading recovery.
use super::*;
use aqe_live_protocol::{
    MAX_POSITION_MARK_ROWS, POSITION_MARK_MINUTE_US, POSITION_MARK_TRANSFER_BYTES,
    POSITION_MARK_VERSION, PositionMarkObservation, PositionMarkPage, PositionMarkRecord,
    PositionQuantityCheck, PositionValuationState, PositionValuations,
};
mod export;
pub(super) const SCHEMA:&str="
CREATE TABLE IF NOT EXISTS broker_position_marks(sequence INTEGER PRIMARY KEY AUTOINCREMENT,execution_epoch INTEGER NOT NULL,recorded_at_us INTEGER NOT NULL,body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS broker_position_mark_heads(fill_provider TEXT NOT NULL,position_id TEXT NOT NULL,sample_minute_us INTEGER NOT NULL,source_as_of_us INTEGER NOT NULL,is_open INTEGER NOT NULL,PRIMARY KEY(fill_provider,position_id));
CREATE INDEX IF NOT EXISTS broker_mark_check_candidates ON broker_execution_position_checks(position_id,provider);
CREATE TABLE IF NOT EXISTS broker_position_mark_export(singleton INTEGER PRIMARY KEY CHECK(singleton=1),cursor INTEGER NOT NULL CHECK(cursor>=0),last_transfer_id TEXT,pending_transfer_id TEXT,pending_next INTEGER,pending_json TEXT);
INSERT INTO broker_position_mark_export(singleton,cursor) VALUES(1,0) ON CONFLICT(singleton) DO NOTHING;";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PositionMarkOwner {
    pub fill_provider: String,
    pub position_id: String,
    pub insight_id: String,
    pub execution_epoch: u64,
    pub symbol: String,
    pub ledger_sequence: u64,
    pub quantity: f64,
    pub side: Option<aqe_live_protocol::ExecutionFillSide>,
}
pub(crate) struct PositionMarkOwners {
    pub owners: Vec<PositionMarkOwner>,
    pub next_after: Option<(String, String)>,
}
pub(crate) struct PositionMarkBatch {
    pub owners: Vec<PositionMarkOwner>,
    pub values: PositionValuations,
    pub sample_minute_us: i64,
}
/// Keep optional sampling transactions short on the serialized journal owner.
pub(crate) const MAX_MARK_ADMISSION: usize = 4;
impl PositionMarkBatch {
    pub fn new(
        owners: Vec<PositionMarkOwner>,
        values: PositionValuations,
        sample_minute_us: i64,
    ) -> Result<Self, BrokerCommandError> {
        if owners.is_empty()
            || owners.len() > MAX_MARK_ADMISSION
            || owners.iter().any(|o| o.symbol != values.symbol)
            || sample_minute_us <= 0
            || sample_minute_us % POSITION_MARK_MINUTE_US != 0
        {
            return Err(journal_error("invalid position mark batch"));
        }
        values
            .validate(
                &values.account_id,
                &values.symbol,
                &owners
                    .iter()
                    .map(|o| o.position_id.clone())
                    .collect::<Vec<_>>(),
            )
            .map_err(journal_error)?;
        Ok(Self {
            owners,
            values,
            sample_minute_us,
        })
    }
}
impl BrokerCommandJournal {
    /// Owned candidates come from actual position comparisons, never from an
    /// intended quantity or a symbol-to-owner guess. Unsupported reads advance
    /// the bounded cursor, so one position cannot starve the rest.
    pub(crate) async fn position_mark_owners(
        &self,
        minute: i64,
        after: Option<(String, String)>,
    ) -> Result<PositionMarkOwners, BrokerCommandError> {
        if minute <= 0 || minute % POSITION_MARK_MINUTE_US != 0 {
            return Err(journal_error("invalid sampling minute"));
        }
        let (provider, position) = after.unwrap_or_default();
        let mut rows=self.analytics_connection().query("SELECT p.provider,p.position_id,p.insight_id,p.execution_epoch,p.ledger_sequence,p.check_json
            FROM broker_execution_position_checks p
            LEFT JOIN broker_position_mark_heads h ON h.fill_provider=p.provider AND h.position_id=p.position_id
            WHERE json_extract(p.check_json,'$.matched')=1 AND (json_extract(p.check_json,'$.snapshot.quantity')>0 OR h.is_open=1)
            AND NOT EXISTS(SELECT 1 FROM broker_execution_position_checks other WHERE other.position_id=p.position_id AND other.provider<>p.provider)
            AND COALESCE(h.sample_minute_us,0)<?1 AND (p.provider>?2 OR (p.provider=?2 AND p.position_id>?3)) ORDER BY p.provider,p.position_id LIMIT 33",params![minute,provider,position]).await.map_err(journal_error)?;
        let mut owners = Vec::new();
        let mut examined = 0;
        let mut last_examined = None;
        let mut more = false;
        while let Some(r) = rows.next().await.map_err(journal_error)? {
            if examined == MAX_POSITION_MARK_ROWS { more = true; break; }
            examined += 1;
            let provider: String = r.get(0).map_err(journal_error)?;
            let position: String = r.get(1).map_err(journal_error)?;
            let insight: String = r.get(2).map_err(journal_error)?;
            let epoch: i64 = r.get(3).map_err(journal_error)?;
            last_examined = Some((provider.clone(), position.clone()));
            // Canonical ownership remains in the uncapped recovery store.
            if !self.mark_checkpoint_matches(&insight, epoch, None).await? { continue; }
            let check: PositionQuantityCheck =
                serde_json::from_str(&r.get::<String>(5).map_err(journal_error)?)
                    .map_err(journal_error)?;
            owners.push(PositionMarkOwner {
                fill_provider: provider,
                position_id: position,
                insight_id: insight,
                execution_epoch: epoch as u64,
                ledger_sequence: r.get::<i64>(4).map_err(journal_error)? as u64,
                symbol: check.snapshot.symbol,
                quantity: check.snapshot.quantity,
                side: check.snapshot.side,
            });
        }
        let next_after = more.then_some(last_examined).flatten();
        Ok(PositionMarkOwners { owners, next_after })
    }

    async fn mark_checkpoint_matches(&self, insight: &str, epoch: i64, symbol: Option<&str>) -> Result<bool, BrokerCommandError> {
        let mut rows = self.connection.query("SELECT symbol FROM broker_insight_checkpoints WHERE insight_id=?1 AND execution_epoch=?2 AND materialized=1",
            params![insight.to_owned(),epoch]).await.map_err(journal_error)?;
        let Some(row) = rows.next().await.map_err(journal_error)? else { return Ok(false); };
        Ok(symbol.is_none_or(|symbol| row.get::<String>(0).ok().as_deref() == Some(symbol)))
    }
    pub(crate) async fn record_position_marks(
        &mut self,
        batch: PositionMarkBatch,
    ) -> Result<usize, BrokerCommandError> {
        self.check_analytics_admission().await?;
        // Revalidate at the journal boundary even if a caller bypassed `new`.
        let batch = PositionMarkBatch::new(batch.owners, batch.values, batch.sample_minute_us)?;
        if batch.values.account_id != self.account_id {
            return Err(journal_error("position mark account changed"));
        }
        batch
            .values
            .validate(
                &self.account_id,
                &batch.values.symbol,
                &batch
                    .owners
                    .iter()
                    .map(|o| o.position_id.clone())
                    .collect::<Vec<_>>(),
            )
            .map_err(journal_error)?;
        let recorded_at_us = chrono::Utc::now().timestamp_micros();
        let tx = turso::transaction::Transaction::new_unchecked(
            self.analytics_connection(),turso::transaction::TransactionBehavior::Immediate)
            .await
            .map_err(journal_error)?;
        let mut next = tx
            .query(
                "SELECT COALESCE(MAX(sequence),0)+1 FROM broker_position_marks",
                (),
            )
            .await
            .map_err(journal_error)?;
        let mut sequence = next
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing mark sequence"))?
            .get::<i64>(0)
            .map_err(journal_error)? as u64;
        drop(next);
        let mut admitted = 0;
        for owner in &batch.owners {
            let position = batch
                .values
                .positions
                .iter()
                .find(|p| p.position_id == owner.position_id)
                .ok_or_else(|| journal_error("missing position mark identity"))?;
            // Recheck the lease on the serialized worker after the broker read.
            // A later fill/correction revokes the comparison before admission.
            let mut rows=tx.query("SELECT p.insight_id,p.execution_epoch,p.ledger_sequence,p.check_json FROM broker_execution_position_checks p WHERE p.provider=?1 AND p.position_id=?2 AND NOT EXISTS(SELECT 1 FROM broker_execution_position_checks other WHERE other.position_id=p.position_id AND other.provider<>p.provider)",params![owner.fill_provider.clone(),owner.position_id.clone()]).await.map_err(journal_error)?;
            let Some(r) = rows.next().await.map_err(journal_error)? else {
                continue;
            };
            let check: PositionQuantityCheck =
                serde_json::from_str(&r.get::<String>(3).map_err(journal_error)?)
                    .map_err(journal_error)?;
            if r.get::<String>(0).map_err(journal_error)? != owner.insight_id
                || r.get::<i64>(1).map_err(journal_error)? as u64 != owner.execution_epoch
                || r.get::<i64>(2).map_err(journal_error)? as u64 != owner.ledger_sequence
                || !self.mark_checkpoint_matches(&owner.insight_id, owner.execution_epoch as i64, Some(&owner.symbol)).await?
                || !check.matched
                || check.snapshot.quantity != owner.quantity
                || check.snapshot.side != owner.side
                || batch.values.as_of_us < check.snapshot.as_of_us
            {
                continue;
            }
            let (quantity, side) = match &position.state {
                PositionValuationState::NotOpen => (0., None),
                PositionValuationState::Open { quantity, side, .. } => (*quantity, Some(*side)),
            };
            if side != owner.side
                || (quantity - owner.quantity).abs()
                    > quantity.abs().max(owner.quantity.abs()) * 1e-9
            {
                continue;
            }
            drop(rows);
            let mut head=tx.query("SELECT sample_minute_us,source_as_of_us FROM broker_position_mark_heads WHERE fill_provider=?1 AND position_id=?2",params![owner.fill_provider.clone(),owner.position_id.clone()]).await.map_err(journal_error)?;
            if let Some(old) = head.next().await.map_err(journal_error)? {
                if old.get::<i64>(0).map_err(journal_error)? >= batch.sample_minute_us
                    || old.get::<i64>(1).map_err(journal_error)? > batch.values.as_of_us
                {
                    continue;
                }
            }
            drop(head);
            let mut values = batch.values.clone();
            values.positions = vec![position.clone()];
            let observation = PositionMarkObservation {
                insight_id: owner.insight_id.clone(),
                fill_provider: owner.fill_provider.clone(),
                ledger_sequence: owner.ledger_sequence,
                sample_minute_us: batch.sample_minute_us,
                values,
            };
            let record = PositionMarkRecord {
                sequence,
                execution_epoch: owner.execution_epoch,
                recorded_at_us,
                observation,
            };
            record.validate(&self.account_id).map_err(journal_error)?;
            let body = serde_json::to_string(&record).map_err(journal_error)?;
            if body.len() > POSITION_MARK_TRANSFER_BYTES / 2 {
                return Err(journal_error("individual position mark byte limit"));
            }
            tx.execute("INSERT INTO broker_position_marks(execution_epoch,recorded_at_us,body) VALUES(?1,?2,?3)",params![owner.execution_epoch as i64,recorded_at_us,body]).await.map_err(journal_error)?;
            tx.execute("INSERT INTO broker_position_mark_heads VALUES(?1,?2,?3,?4,?5) ON CONFLICT(fill_provider,position_id) DO UPDATE SET sample_minute_us=excluded.sample_minute_us,source_as_of_us=excluded.source_as_of_us,is_open=excluded.is_open",params![owner.fill_provider.clone(),owner.position_id.clone(),batch.sample_minute_us,batch.values.as_of_us,i64::from(record.is_open())]).await.map_err(journal_error)?;
            admitted += 1;
            sequence += 1;
        }
        super::analytics_capture::commit_observations(tx,self.analytics_connection(),self.analytics_path(),self.analytics_capture_budget,self.lifecycle.epoch).await?;
        Ok(admitted)
    }
    pub(crate) async fn position_mark_page(
        &self,
        after: u64,
    ) -> Result<PositionMarkPage, BrokerCommandError> {
        if after > i64::MAX as u64 {
            return Err(journal_error("invalid position mark cursor"));
        }
        let mut rows = self.analytics_connection()
            .query(
                "SELECT COALESCE(MAX(sequence),0) FROM broker_position_marks",
                (),
            )
            .await
            .map_err(journal_error)?;
        let through = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing mark head"))?
            .get::<i64>(0)
            .map_err(journal_error)? as u64;
        drop(rows);
        if after > through {
            return Err(journal_error("position mark cursor exceeds retained head"));
        }
        let mut rows=self.analytics_connection().query("SELECT body FROM broker_position_marks WHERE sequence>?1 AND sequence<=?2 ORDER BY sequence LIMIT 32",params![after as i64,through as i64]).await.map_err(journal_error)?;
        let mut page = PositionMarkPage {
            version: POSITION_MARK_VERSION,
            source_id: self.evidence_source_id.clone(),
            strategy_id: self.strategy_id.clone(),
            account_id: self.account_id.clone(),
            after_sequence: after,
            through_sequence: through,
            next_sequence: after,
            has_more: after < through,
            records: vec![],
            generated_at_us: chrono::Utc::now().timestamp_micros(),
        };
        // Bound bytes incrementally. Do not serialize an ever-growing page on
        // each row while holding the serialized journal worker. Reserve room
        // for changed cursor digits and the final has_more flag.
        let mut bytes = serde_json::to_vec(&page).map_err(journal_error)?.len() + 64;
        while let Some(r) = rows.next().await.map_err(journal_error)? {
            let body = r.get::<String>(0).map_err(journal_error)?;
            let record: PositionMarkRecord = serde_json::from_str(&body).map_err(journal_error)?;
            record.validate(&self.account_id).map_err(journal_error)?;
            if bytes + body.len() + 1 > POSITION_MARK_TRANSFER_BYTES {
                break;
            }
            bytes += body.len() + 1;
            page.next_sequence = record.sequence;
            page.records.push(record);
            page.has_more = page.next_sequence < through;
        }
        page.has_more = page.next_sequence < through;
        page.validate().map_err(journal_error)?;
        Ok(page)
    }
}

#[cfg(test)]
mod tests;
