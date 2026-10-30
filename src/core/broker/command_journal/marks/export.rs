use super::*;
use aqe_live_protocol::{
    POSITION_MARK_TRANSFER_BYTES, PositionMarkExportReceipt, PositionMarkTransfer,
};

impl BrokerCommandJournal {
    /// Freeze exact bytes before asynchronous publication. Only the bounded
    /// journal owner calls this; reconnect/restart resumes the same transfer.
    pub async fn prepare_position_mark_export(
        &self,
    ) -> Result<Option<PositionMarkTransfer>, BrokerCommandError> {
        self.try_reclaim_cloud_wal().await?;
        let mut rows = self.analytics_connection().query(
            "SELECT cursor,pending_json,pending_transfer_id,pending_next FROM broker_position_mark_export WHERE singleton=1", ()
        ).await.map_err(journal_error)?;
        let row = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing mark export cursor"))?;
        let cursor =
            u64::try_from(row.get::<i64>(0).map_err(journal_error)?).map_err(journal_error)?;
        let pending: Option<String> = row.get(1).map_err(journal_error)?;
        let pending_id: Option<String> = row.get(2).map_err(journal_error)?;
        let pending_next: Option<i64> = row.get(3).map_err(journal_error)?;
        drop(rows);
        if let Some(body) = pending {
            if body.len() > POSITION_MARK_TRANSFER_BYTES {
                return Err(journal_error("oversized saved mark transfer"));
            }
            let page: PositionMarkPage = serde_json::from_str(&body).map_err(journal_error)?;
            let transfer = PositionMarkTransfer {
                schema_version: page.version,
                source_id: page.source_id.clone(),
                strategy_id: page.strategy_id.clone(),
                account_id: page.account_id.clone(),
                after_sequence: page.after_sequence,
                next_sequence: page.next_sequence,
                transfer_id: pending_id
                    .ok_or_else(|| journal_error("missing mark transfer checksum"))?,
                body,
            };
            transfer.page().map_err(journal_error)?;
            if transfer.source_id != self.evidence_source_id
                || transfer.strategy_id != self.strategy_id
                || transfer.account_id != self.account_id
                || transfer.after_sequence != cursor
                || pending_next != Some(transfer.next_sequence as i64)
            {
                return Err(journal_error("saved mark transfer scope mismatch"));
            }
            return Ok(Some(transfer));
        }
        let page = self.position_mark_page(cursor).await?;
        if page.records.is_empty() {
            return Ok(None);
        }
        let transfer = PositionMarkTransfer::from_page(&page).map_err(journal_error)?;
        let tx=turso::transaction::Transaction::new_unchecked(self.analytics_connection(),turso::transaction::TransactionBehavior::Immediate).await.map_err(journal_error)?;
        tx.execute("UPDATE broker_position_mark_export SET pending_transfer_id=?1,pending_next=?2,pending_json=?3 WHERE singleton=1",
            params![transfer.transfer_id.clone(),transfer.next_sequence as i64,transfer.body.clone()]).await.map_err(journal_error)?;
        self.commit_analytics_maintenance(tx).await?;
        Ok(Some(transfer))
    }

    pub async fn acknowledge_position_mark_export(
        &self,
        receipt: &PositionMarkExportReceipt,
    ) -> Result<(), BrokerCommandError> {
        self.try_reclaim_cloud_wal().await?;
        if receipt.source_id != self.evidence_source_id
            || receipt.after_sequence >= receipt.next_sequence
            || receipt.next_sequence > i64::MAX as u64
        {
            return Err(journal_error("invalid mark export receipt"));
        }
        let mut rows = self.analytics_connection().query("SELECT cursor,last_transfer_id,pending_transfer_id,pending_next FROM broker_position_mark_export WHERE singleton=1", ()).await.map_err(journal_error)?;
        let row = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing mark export cursor"))?;
        let cursor: i64 = row.get(0).map_err(journal_error)?;
        let last: Option<String> = row.get(1).map_err(journal_error)?;
        let pending: Option<String> = row.get(2).map_err(journal_error)?;
        let next: Option<i64> = row.get(3).map_err(journal_error)?;
        drop(rows);
        if cursor == receipt.next_sequence as i64 && last.as_deref() == Some(&receipt.transfer_id) {
            return self.retire_handed_off_cloud_history().await;
        }
        if cursor != receipt.after_sequence as i64
            || pending.as_deref() != Some(&receipt.transfer_id)
            || next != Some(receipt.next_sequence as i64)
        {
            return Err(journal_error(
                "mark export receipt does not match pending page",
            ));
        }
        let tx=turso::transaction::Transaction::new_unchecked(self.analytics_connection(),turso::transaction::TransactionBehavior::Immediate).await.map_err(journal_error)?;
        tx.execute("UPDATE broker_position_mark_export SET cursor=?1,last_transfer_id=pending_transfer_id,pending_transfer_id=NULL,pending_next=NULL,pending_json=NULL WHERE singleton=1",
            params![receipt.next_sequence as i64]).await.map_err(journal_error)?;
        self.commit_analytics_maintenance(tx).await?;
        self.retire_handed_off_cloud_history().await
    }
}
