use super::*;
use crate::core::broker::fill_history::FillHistoryOwnerPage;

pub(super) async fn invalidate_position_check(
    tx: &Connection,
    provider: &str,
    position: Option<&str>,
    as_of_us: i64,
) -> Result<(), BrokerCommandError> {
    if let Some(position) = position {
        let mut rows = tx.query("SELECT insight_id,execution_epoch,ledger_sequence,as_of_us,check_json FROM broker_execution_position_checks WHERE provider=?1 AND position_id=?2",
            params![provider.to_owned(), position.to_owned()]).await.map_err(journal_error)?;
        let old = rows.next().await.map_err(journal_error)?;
        drop(rows);
        if let Some(old) = old {
            let check: aqe_live_protocol::PositionQuantityCheck =
                serde_json::from_str(&old.get::<String>(4).map_err(journal_error)?)
                    .map_err(journal_error)?;
            append_position_check(
                tx,
                old.get(1).map_err(journal_error)?,
                PositionQuantityObservation {
                    provider: provider.to_owned(),
                    position_id: position.to_owned(),
                    symbol: check.snapshot.symbol,
                    insight_id: old.get(0).map_err(journal_error)?,
                    ledger_sequence: old.get::<i64>(2).map_err(journal_error)? as u64,
                    as_of_us: as_of_us.max(old.get(3).map_err(journal_error)?),
                    check: None,
                },
            )
            .await?;
        }
        tx.execute(
            "DELETE FROM broker_execution_position_checks WHERE provider=?1 AND position_id=?2",
            params![provider.to_owned(), position.to_owned()],
        )
        .await
        .map_err(journal_error)?;
    }
    Ok(())
}

async fn append_position_check(
    conn: &Connection,
    epoch: i64,
    observation: PositionQuantityObservation,
) -> Result<(), BrokerCommandError> {
    let mut rows = conn
        .query(
            "SELECT COALESCE(MAX(sequence),0)+1 FROM broker_execution_fills",
            (),
        )
        .await
        .map_err(journal_error)?;
    let sequence: i64 = rows
        .next()
        .await
        .map_err(journal_error)?
        .ok_or_else(|| journal_error("missing fill cursor"))?
        .get(0)
        .map_err(journal_error)?;
    drop(rows);
    let recorded_at_us = chrono::Utc::now().timestamp_micros();
    PositionQuantityRecord {
        sequence: sequence as u64,
        execution_epoch: epoch as u64,
        recorded_at_us,
        observation: observation.clone(),
    }
    .validate()
    .map_err(journal_error)?;
    let body = serde_json::to_string(&SavedFill::PositionCheck {
        position_check: observation,
    })
    .map_err(journal_error)?;
    conn.execute("INSERT INTO broker_execution_fills(execution_epoch,recorded_at_us,observation_json) VALUES(?1,?2,?3)",
        params![epoch,recorded_at_us,body]).await.map_err(journal_error)?;
    Ok(())
}

/// The comparison belongs to exactly the admitted position facts. A refresh
/// racing a newer stream revision cannot certify the newer ledger. Missing or
/// additional broker deals likewise keep the comparison unavailable.
pub(super) async fn persist_position_check(
    tx: &turso::transaction::Transaction<'_>,
    batch: &ExecutionFillBatch,
) -> Result<(), BrokerCommandError> {
    let (Some(check), Some((_, owner))) = (&batch.position_check, &batch.history_owner) else {
        return Ok(());
    };
    let provider = batch
        .rows
        .first()
        .map(|r| r.0.fill.provider.as_str())
        .or_else(|| batch.invalidations.first().map(|v| v.provider.as_str()))
        .ok_or_else(|| journal_error("position check has no execution provider"))?;
    let position = &check.snapshot.position_id;
    let mut rows = tx.query("SELECT f.sequence,h.as_of_us,f.observation_json FROM broker_execution_fills f INDEXED BY idx_fill_position_identity
        JOIN broker_execution_fill_heads h ON h.sequence=f.sequence
        WHERE COALESCE(json_extract(f.observation_json,'$.fill.position_id'),json_extract(f.observation_json,'$.invalidated_observation.fill.position_id'))=?1
        AND h.provider=?2 LIMIT 257",params![position.clone(),provider.to_owned()]).await.map_err(journal_error)?;
    let mut count = 0;
    let mut sequence = 0i64;
    while let Some(row) = rows.next().await.map_err(journal_error)? {
        count += 1;
        if count > MAX_FILL_BATCH_ROWS
            || row.get::<i64>(1).map_err(journal_error)? > check.snapshot.as_of_us
        {
            return Ok(());
        }
        sequence = sequence.max(row.get::<i64>(0).map_err(journal_error)?);
        let saved: SavedFill = serde_json::from_str(&row.get::<String>(2).map_err(journal_error)?)
            .map_err(journal_error)?;
        let (saved, invalidation) = saved.into_parts()?;
        if saved.insight_id.as_deref() != Some(owner.insight_id.as_str()) {
            return Ok(());
        }
        let agrees = if let Some(invalidation) = invalidation {
            batch.invalidations.iter().any(|v| {
                let mut refreshed = v.clone();
                refreshed.as_of_us = invalidation.as_of_us;
                refreshed == invalidation
            })
        } else {
            batch
                .rows
                .iter()
                .any(|(v, _)| v.fill.same_facts(&saved.fill))
        };
        if !agrees {
            return Ok(());
        }
    }
    drop(rows);
    // An invalidation for a never-observed deal has no original fact to export;
    // do not manufacture a complete ledger/position comparison from it.
    if count != batch.rows.len() + batch.invalidations.len() {
        return Ok(());
    }
    let body = serde_json::to_string(check).map_err(journal_error)?;
    let mut rows = tx.query("SELECT as_of_us,check_json,ledger_sequence,insight_id,execution_epoch FROM broker_execution_position_checks WHERE provider=?1 AND position_id=?2",
        params![provider.to_owned(),position.clone()]).await.map_err(journal_error)?;
    if let Some(old) = rows.next().await.map_err(journal_error)? {
        let old_time: i64 = old.get(0).map_err(journal_error)?;
        if old_time > check.snapshot.as_of_us {
            return Ok(());
        }
        if old_time == check.snapshot.as_of_us {
            if old.get::<String>(1).map_err(journal_error)? != body {
                return Err(journal_error(
                    "conflicting position check at same observation watermark",
                ));
            }
            return Ok(());
        }
        // A fresh poll is not another historical quantity event. Keep the
        // latest observation watermark locally for stale/conflicting-response
        // detection, but export only changed facts or a changed ledger/owner.
        let mut previous: aqe_live_protocol::PositionQuantityCheck =
            serde_json::from_str(&old.get::<String>(1).map_err(journal_error)?)
                .map_err(journal_error)?;
        previous.snapshot.as_of_us = check.snapshot.as_of_us;
        if previous == *check
            && old.get::<i64>(2).map_err(journal_error)? == sequence
            && old.get::<String>(3).map_err(journal_error)? == owner.insight_id
            && old.get::<i64>(4).map_err(journal_error)? == owner.execution_epoch as i64
        {
            drop(rows);
            tx.execute("UPDATE broker_execution_position_checks SET as_of_us=?1,check_json=?2 WHERE provider=?3 AND position_id=?4",
                params![check.snapshot.as_of_us,body,provider.to_owned(),position.clone()])
                .await.map_err(journal_error)?;
            return Ok(());
        }
    }
    drop(rows);
    append_position_check(
        tx,
        owner.execution_epoch as i64,
        PositionQuantityObservation {
            provider: provider.to_owned(),
            position_id: position.clone(),
            symbol: check.snapshot.symbol.clone(),
            insight_id: owner.insight_id.clone(),
            ledger_sequence: sequence as u64,
            as_of_us: check.snapshot.as_of_us,
            check: Some(check.clone()),
        },
    )
    .await?;
    tx.execute("INSERT INTO broker_execution_position_checks VALUES(?1,?2,?3,?4,?5,?6,?7)
        ON CONFLICT(provider,position_id) DO UPDATE SET insight_id=excluded.insight_id,execution_epoch=excluded.execution_epoch,
        ledger_sequence=excluded.ledger_sequence,as_of_us=excluded.as_of_us,check_json=excluded.check_json
        WHERE excluded.as_of_us>=broker_execution_position_checks.as_of_us",
        params![provider.to_owned(),position.clone(),owner.insight_id.clone(),owner.execution_epoch as i64,sequence,check.snapshot.as_of_us,body]).await.map_err(journal_error)?;
    Ok(())
}

impl BrokerCommandJournal {
    /// Saved ownership, including terminal Insights, paged by primary key. The
    /// latest sanitized evidence supplies the original execution epoch. No
    /// broker account scan or in-memory registry is used to guess owners.
    pub(crate) async fn fill_history_owner_page(
        &self,
        after: &str,
    ) -> Result<FillHistoryOwnerPage, BrokerCommandError> {
        self.fill_history_owner_page_for(after, false).await
    }

    pub(crate) async fn active_fill_history_owner_page(
        &self,
        after: &str,
    ) -> Result<FillHistoryOwnerPage, BrokerCommandError> {
        self.fill_history_owner_page_for(after, true).await
    }

    async fn fill_history_owner_page_for(
        &self,
        after: &str,
        active_only: bool,
    ) -> Result<FillHistoryOwnerPage, BrokerCommandError> {
        let sql = if active_only {
            format!("SELECT insight_id,execution_epoch,order_id,symbol FROM broker_insight_checkpoints
                INDEXED BY idx_fill_history_active_owner WHERE insight_id>?1 AND {}
                ORDER BY insight_id LIMIT 32",super::super::checkpoints::ACTIVE_FILL_OWNER_PREDICATE)
        } else {
            "SELECT insight_id,execution_epoch,order_id,symbol
             FROM broker_insight_checkpoints WHERE insight_id>?1
                AND execution_epoch>0 AND materialized=1
             ORDER BY insight_id LIMIT 32".to_owned()
        };
        let mut rows = self
            .connection
            .query(
                &sql,
                params![after.to_owned()],
            )
            .await
            .map_err(journal_error)?;
        let mut owners = Vec::new();
        let mut next_cursor = None;
        while let Some(row) = rows.next().await.map_err(journal_error)? {
            let id: String = row.get(0).map_err(journal_error)?;
            next_cursor = Some(id.clone());
            let Some(order) = row
                .get::<Option<String>>(2)
                .map_err(journal_error)?
                .filter(|id| !id.is_empty())
            else {
                continue;
            };
            let symbol: String = row.get(3).map_err(journal_error)?;
            let epoch: i64 = row.get(1).map_err(journal_error)?;
            if epoch <= 0
                || id.len() > 128
                || order.len() > 128
                || symbol.is_empty()
                || symbol.len() > 256
            {
                return Err(journal_error("invalid fill history ownership"));
            }
            owners.push(FillHistoryOwner {
                insight_id: id,
                execution_epoch: epoch as u64,
                entry_order_id: order,
                symbol,
            });
        }
        Ok(FillHistoryOwnerPage {
            owners,
            next_cursor,
        })
    }
}

pub(super) async fn validate_owner(
    tx: &Connection,
    owner: &FillHistoryOwner,
) -> Result<(), BrokerCommandError> {
    let mut rows = tx
        .query(
            "SELECT insight_id FROM broker_insight_checkpoints
        WHERE order_id=?1 LIMIT 2",
            params![owner.entry_order_id.clone()],
        )
        .await
        .map_err(journal_error)?;
    let id = rows
        .next()
        .await
        .map_err(journal_error)?
        .ok_or_else(|| journal_error("history order has no saved owner"))?
        .get::<String>(0)
        .map_err(journal_error)?;
    if id != owner.insight_id || rows.next().await.map_err(journal_error)?.is_some() {
        return Err(journal_error("ambiguous history order ownership"));
    }
    drop(rows);
    let mut rows = tx
        .query(
            "SELECT execution_epoch,order_id,symbol,materialized FROM broker_insight_checkpoints WHERE insight_id=?1",
            params![owner.insight_id.clone()],
        )
        .await
        .map_err(journal_error)?;
    let row = rows
        .next()
        .await
        .map_err(journal_error)?
        .ok_or_else(|| journal_error("missing history owner checkpoint"))?;
    let epoch: i64 = row.get(0).map_err(journal_error)?;
    if epoch <= 0
        || epoch as u64 != owner.execution_epoch
        || row
            .get::<Option<String>>(1)
            .map_err(journal_error)?
            .as_deref()
            != Some(owner.entry_order_id.as_str())
        || row.get::<String>(2).map_err(journal_error)? != owner.symbol
        || row.get::<i64>(3).map_err(journal_error)? != 1
    {
        return Err(journal_error("history ownership changed before admission"));
    }
    Ok(())
}
