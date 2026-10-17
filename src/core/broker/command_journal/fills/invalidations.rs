//! Tombstones retain broker identity and suppress delayed observations across
//! restarts. Missing rows in a history response are never deletion evidence.
use super::*;
use aqe_live_protocol::ExecutionFill;

pub(super) async fn suppressed(
    conn: &Connection,
    fill: &ExecutionFill,
) -> Result<bool, BrokerCommandError> {
    let mut rows = conn
        .query(
            "SELECT body FROM broker_execution_fill_invalidated WHERE provider=?1 AND fill_id=?2",
            params![fill.provider.clone(), fill.fill_id.clone()],
        )
        .await
        .map_err(journal_error)?;
    let invalidation = match rows.next().await.map_err(journal_error)? {
        Some(row) => Some(
            serde_json::from_str::<ExecutionFillInvalidation>(
                &row.get::<String>(0).map_err(journal_error)?,
            )
            .map_err(journal_error)?,
        ),
        None => None,
    };
    drop(rows);
    if let Some(v) = invalidation {
        if !v.matches(fill) {
            return Err(journal_error("invalidated execution identity changed"));
        }
        if fill.as_of_us <= v.as_of_us {
            return Ok(true);
        }
        conn.execute(
            "DELETE FROM broker_execution_fill_invalidated WHERE provider=?1 AND fill_id=?2",
            params![fill.provider.clone(), fill.fill_id.clone()],
        )
        .await
        .map_err(journal_error)?;
    }
    Ok(false)
}

pub(super) async fn apply(
    conn: &Connection,
    input: &[ExecutionFillInvalidation],
    owner: Option<&FillHistoryOwner>,
) -> Result<bool, BrokerCommandError> {
    let mut appended = false;
    for v in input {
        v.validate().map_err(journal_error)?;
        let mut rows = conn.query("SELECT body FROM broker_execution_fill_invalidated WHERE provider=?1 AND fill_id=?2",
            params![v.provider.clone(), v.fill_id.clone()]).await.map_err(journal_error)?;
        if let Some(row) = rows.next().await.map_err(journal_error)? {
            let old: ExecutionFillInvalidation =
                serde_json::from_str(&row.get::<String>(0).map_err(journal_error)?)
                    .map_err(journal_error)?;
            let mut same = old.clone();
            same.as_of_us = v.as_of_us;
            if same != *v {
                return Err(journal_error(
                    "execution invalidation identity/reason conflict",
                ));
            }
            if v.as_of_us <= old.as_of_us {
                continue;
            }
        }
        drop(rows);
        let mut rows = conn.query("SELECT h.as_of_us,f.execution_epoch,f.observation_json FROM broker_execution_fill_heads h
            JOIN broker_execution_fills f ON f.sequence=h.sequence WHERE h.provider=?1 AND h.fill_id=?2",
            params![v.provider.clone(),v.fill_id.clone()]).await.map_err(journal_error)?;
        let old = match rows.next().await.map_err(journal_error)? {
            Some(row) => Some((
                row.get::<i64>(0).map_err(journal_error)?,
                row.get::<i64>(1).map_err(journal_error)?,
                serde_json::from_str::<SavedFill>(&row.get::<String>(2).map_err(journal_error)?)
                    .map_err(journal_error)?,
            )),
            None => None,
        };
        drop(rows);
        if let Some((watermark, epoch, saved)) = old {
            let (observation, invalidation) = saved.into_parts()?;
            if !v.matches(&observation.fill)
                || owner.is_some_and(|o| {
                    o.execution_epoch as i64 != epoch
                        || observation
                            .insight_id
                            .as_deref()
                            .is_some_and(|id| id != o.insight_id)
                })
            {
                return Err(journal_error("execution invalidation ownership conflict"));
            }
            if v.as_of_us < watermark {
                continue;
            }
            if invalidation.is_none() {
                if v.as_of_us == watermark {
                    return Err(journal_error("conflicting invalidation at fill watermark"));
                }
                let body = serde_json::to_string(&SavedFill::Invalidated {
                    invalidated_observation: observation,
                    invalidation: v.clone(),
                })
                .map_err(journal_error)?;
                conn.execute("INSERT INTO broker_execution_fills(execution_epoch,recorded_at_us,observation_json) VALUES(?1,?2,?3)",
                    params![epoch,chrono::Utc::now().timestamp_micros(),body]).await.map_err(journal_error)?;
                conn.execute("UPDATE broker_execution_fill_heads SET sequence=last_insert_rowid(),as_of_us=?3 WHERE provider=?1 AND fill_id=?2",
                    params![v.provider.clone(),v.fill_id.clone(),v.as_of_us]).await.map_err(journal_error)?;
                appended = true;
            } else {
                conn.execute("UPDATE broker_execution_fill_heads SET as_of_us=?3 WHERE provider=?1 AND fill_id=?2",
                    params![v.provider.clone(),v.fill_id.clone(),v.as_of_us]).await.map_err(journal_error)?;
            }
        }
        // An unseen cancelled deal has no economic record to retract, but its
        // identity must still fence a late, older event from creating one.
        history::invalidate_position_check(conn, &v.provider, v.position_id.as_deref(), v.as_of_us)
            .await?;
        conn.execute(
            "INSERT INTO broker_execution_fill_invalidated(provider,fill_id,body) VALUES(?1,?2,?3)
            ON CONFLICT(provider,fill_id) DO UPDATE SET body=excluded.body",
            params![
                v.provider.clone(),
                v.fill_id.clone(),
                serde_json::to_string(v).map_err(journal_error)?
            ],
        )
        .await
        .map_err(journal_error)?;
    }
    Ok(appended)
}
