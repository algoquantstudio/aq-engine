use super::*;
use aqe_live_protocol::{
    EXECUTION_FILL_TRANSFER_BYTES, EXECUTION_FILL_TRANSFER_ROWS, ExecutionFillExportReceipt,
    ExecutionFillTransfer,
};

impl BrokerCommandJournal {
    /// Freeze exact bytes before asynchronous publication. Only the bounded
    /// journal owner calls this; reconnect/restart resumes the same transfer.
    pub async fn prepare_execution_fill_export(
        &self,
    ) -> Result<Option<ExecutionFillTransfer>, BrokerCommandError> {
        let mut rows = self.connection.query(
            "SELECT cursor,pending_json,pending_transfer_id,pending_next FROM broker_execution_fill_export WHERE singleton=1", ()
        ).await.map_err(journal_error)?;
        let row = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing fill export cursor"))?;
        let cursor =
            u64::try_from(row.get::<i64>(0).map_err(journal_error)?).map_err(journal_error)?;
        let pending: Option<String> = row.get(1).map_err(journal_error)?;
        let pending_id: Option<String> = row.get(2).map_err(journal_error)?;
        let pending_next: Option<i64> = row.get(3).map_err(journal_error)?;
        drop(rows);
        if let Some(body) = pending {
            if body.len() > EXECUTION_FILL_TRANSFER_BYTES {
                return Err(journal_error("oversized saved fill transfer"));
            }
            let page: ExecutionFillPage = serde_json::from_str(&body).map_err(journal_error)?;
            let transfer = ExecutionFillTransfer {
                schema_version: page.version,
                source_id: page.source_id.clone(),
                strategy_id: page.strategy_id.clone(),
                account_id: page.account_id.clone(),
                after_sequence: page.after_sequence,
                next_sequence: page.next_sequence,
                transfer_id: pending_id
                    .ok_or_else(|| journal_error("missing fill transfer checksum"))?,
                body,
            };
            transfer.page().map_err(journal_error)?;
            if transfer.source_id != self.evidence_source_id
                || transfer.strategy_id != self.strategy_id
                || transfer.account_id != self.account_id
                || transfer.after_sequence != cursor
                || pending_next != Some(transfer.next_sequence as i64)
            {
                return Err(journal_error("saved fill transfer scope mismatch"));
            }
            return Ok(Some(transfer));
        }
        let mut page = self
            .execution_fill_page(cursor, None, EXECUTION_FILL_TRANSFER_ROWS)
            .await?;
        if page.records.is_empty() && page.position_checks.is_empty() {
            return Ok(None);
        }
        // Encode at most a few bounded candidates; escape-heavy identifiers can
        // need more bytes than their raw input. Never split an individual fill.
        let transfer = loop {
            let bytes = serde_json::to_vec(&page).map_err(journal_error)?.len();
            if bytes <= EXECUTION_FILL_TRANSFER_BYTES {
                break ExecutionFillTransfer::from_page(&page).map_err(journal_error)?;
            }
            if page.records.len() + page.position_checks.len() <= 1 {
                return Err(journal_error("individual fill exceeds transfer limit"));
            }
            page.next_sequence =
                page.after_sequence + (page.records.len() + page.position_checks.len()) as u64 / 2;
            page.records.retain(|r| r.sequence <= page.next_sequence);
            page.position_checks
                .retain(|r| r.sequence <= page.next_sequence);
            page.has_more = true;
        };
        self.connection.execute("UPDATE broker_execution_fill_export SET pending_transfer_id=?1,pending_next=?2,pending_json=?3 WHERE singleton=1",
            params![transfer.transfer_id.clone(),transfer.next_sequence as i64,transfer.body.clone()]).await.map_err(journal_error)?;
        Ok(Some(transfer))
    }

    pub async fn acknowledge_execution_fill_export(
        &self,
        receipt: &ExecutionFillExportReceipt,
    ) -> Result<(), BrokerCommandError> {
        if receipt.source_id != self.evidence_source_id
            || receipt.after_sequence >= receipt.next_sequence
            || receipt.next_sequence > i64::MAX as u64
        {
            return Err(journal_error("invalid fill export receipt"));
        }
        let mut rows = self.connection.query("SELECT cursor,last_transfer_id,pending_transfer_id,pending_next FROM broker_execution_fill_export WHERE singleton=1", ()).await.map_err(journal_error)?;
        let row = rows
            .next()
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error("missing fill export cursor"))?;
        let cursor: i64 = row.get(0).map_err(journal_error)?;
        let last: Option<String> = row.get(1).map_err(journal_error)?;
        let pending: Option<String> = row.get(2).map_err(journal_error)?;
        let next: Option<i64> = row.get(3).map_err(journal_error)?;
        drop(rows);
        if cursor == receipt.next_sequence as i64 && last.as_deref() == Some(&receipt.transfer_id) {
            return Ok(());
        }
        if cursor != receipt.after_sequence as i64
            || pending.as_deref() != Some(&receipt.transfer_id)
            || next != Some(receipt.next_sequence as i64)
        {
            return Err(journal_error(
                "fill export receipt does not match pending page",
            ));
        }
        self.connection.execute("UPDATE broker_execution_fill_export SET cursor=?1,last_transfer_id=pending_transfer_id,pending_transfer_id=NULL,pending_next=NULL,pending_json=NULL WHERE singleton=1",
            params![receipt.next_sequence as i64]).await.map_err(journal_error)?;
        Ok(())
    }
}
