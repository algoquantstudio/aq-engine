//! Exact, durable replacement of uncertain submission evidence. This does not
//! submit orders or acknowledge insight reduction; the owner must still do both
//! normal evidence delivery and checkpoint acknowledgement after this commit.
use super::*;
use crate::core::broker::{commands::BrokerCommandAction, types::Order};

impl BrokerCommandJournal {
    /// Caller must obtain the order through account-scoped reconcile_submission,
    /// never a symbol-only active-order search. Immutable command equality fences
    /// delayed lookup results for changed identities or terms.
    pub(crate) async fn confirm_ambiguous_submission(
        &mut self,
        expected: &BrokerCommand,
        order: Order,
    ) -> Result<BrokerCommandCompletion, BrokerCommandError> {
        let record = self
            .get(&expected.id)
            .await?
            .ok_or(BrokerCommandError::IdentityConflict)?;
        let BrokerCommandAction::Submit { request } = &expected.action else {
            return Err(BrokerCommandError::IdentityConflict);
        };
        if record.command != *expected
            || expected.strategy_id != self.strategy_id
            || expected.account_id != self.account_id
            || order.order_id.trim().is_empty()
            || order.insight_id.as_deref() != Some(request.insight_id.as_str())
            || order.asset.symbol != request.symbol
            || order.side != request.side
        {
            return Err(BrokerCommandError::IdentityConflict);
        }
        let existing = record
            .completion
            .ok_or(BrokerCommandError::IdentityConflict)?;
        if let BrokerCommandOutcome::OrderResponse { order: saved } = &existing.outcome {
            // Identical retry is safe, even after normal reducer acknowledgement.
            if serde_json::to_string(saved).map_err(journal_error)?
                == serde_json::to_string(&order).map_err(journal_error)?
            {
                return Ok(existing);
            }
        }
        if record.state != JournalCommandState::Completed
            || !matches!(existing.outcome, BrokerCommandOutcome::Ambiguous { .. })
        {
            return Err(BrokerCommandError::IdentityConflict);
        }
        let before = serde_json::to_string(&existing).map_err(journal_error)?;
        let mut confirmed = existing;
        confirmed.completed_at_us = chrono::Utc::now()
            .timestamp_micros()
            .max(confirmed.completed_at_us);
        confirmed.outcome = BrokerCommandOutcome::OrderResponse { order };
        let after = serde_json::to_string(&confirmed).map_err(journal_error)?;
        let changed = self.connection.execute(
            "UPDATE broker_commands SET completion_json=?2 WHERE command_id=?1 AND state='completed' AND completion_json=?3",
            params![expected.id.0.clone(), after, before],
        ).await.map_err(journal_error)?;
        if changed != 1 {
            return Err(BrokerCommandError::IdentityConflict);
        }
        Ok(confirmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        broker::{paper_broker::PaperBroker, traits::OrderManagementProvider, types::AccountType},
        insight::Insight,
    };

    #[tokio::test]
    async fn ambiguous_submission_confirmation_is_durable_exact_and_idempotent() {
        let path = std::env::temp_dir().join(format!("aqe-proof-{}", uuid::Uuid::new_v4()));
        let mut journal =
            BrokerCommandJournal::open(path.join("commands.db"), "strategy", "account")
                .await
                .unwrap();
        let mut insight = Insight::default();
        insight.symbol = "ETHUSD".into();
        insight.set_quantity(Some(1.0)).set_limit_price(Some(100.0));
        let request = (&insight).into();
        let command = BrokerCommand::new(
            "strategy",
            "account",
            &insight.insight_id().to_string(),
            BrokerCommandAction::Submit { request },
            None,
        );
        journal.prepare(&command, 1).await.unwrap();
        journal.mark_dispatched(&command.id, 2).await.unwrap();
        journal
            .complete(&BrokerCommandCompletion {
                receipt: command.receipt(),
                queued_at_us: 1,
                dispatched_at_us: Some(2),
                completed_at_us: 3,
                outcome: BrokerCommandOutcome::Ambiguous {
                    reason: "lost response".into(),
                },
            })
            .await
            .unwrap();
        let paper = PaperBroker::new(AccountType::Paper, 100_000.0, 1);
        let order = paper.submit_order((&insight).into()).await.unwrap();
        let mut wrong = order.clone();
        wrong.asset.symbol = "BTCUSD".into();
        assert!(
            journal
                .confirm_ambiguous_submission(&command, wrong)
                .await
                .is_err()
        );
        let confirmed = journal
            .confirm_ambiguous_submission(&command, order.clone())
            .await
            .unwrap();
        assert_eq!(confirmed.dispatched_at_us, Some(2));
        drop(journal);
        let mut journal =
            BrokerCommandJournal::open(path.join("commands.db"), "strategy", "account")
                .await
                .unwrap();
        assert_eq!(
            journal.get(&command.id).await.unwrap().unwrap().state,
            JournalCommandState::Completed
        );
        journal
            .confirm_ambiguous_submission(&command, order.clone())
            .await
            .unwrap();
        let mut conflict = order;
        conflict.order_id = "different-ticket".into();
        assert!(
            journal
                .confirm_ambiguous_submission(&command, conflict)
                .await
                .is_err()
        );
        drop(journal);
        std::fs::remove_dir_all(path).unwrap();
    }
}
