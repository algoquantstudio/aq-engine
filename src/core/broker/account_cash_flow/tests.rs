use super::*;
fn page(after: u64, id: &str, amount: f64) -> AccountCashFlowPage {
    AccountCashFlowPage {
        version: 1,
        provider: "broker".into(),
        source_id: "source".into(),
        account_id: "account".into(),
        currency: "EUR".into(),
        from_us: 10,
        through_us: 100 + after as i64,
        coverage: AccountCashFlowCoverage::ObservedOnly,
        first_after_sequence: 0,
        after_sequence: after,
        next_sequence: after + 1,
        through_sequence: after + 1,
        records: vec![AccountCashFlowRecord {
            sequence: after + 1,
            event_id: id.into(),
            kind: AccountCashFlowKind::Balance,
            amount,
            occurred_at_us: None,
            observed_at_us: 60,
            broker_time_ms: Some(50),
            deleted: false,
        }],
    }
}
#[test]
fn account_cash_flow_replay_corrections_deletes_and_scope_fences_keep_one_broker_fact() {
    let p = page(0, "deal", 100.);
    let mut s = AccountCashFlows::from_page(&p).unwrap();
    s.apply(&p).unwrap();
    assert_eq!(s.records.len(), 1);
    s.apply(&page(1, "deal", 90.)).unwrap();
    assert_eq!(s.records.len(), 1);
    assert_eq!(s.records["deal"].amount, 90.);
    let mut deleted = page(2, "deal", 90.);
    deleted.records[0].deleted = true;
    s.apply(&deleted).unwrap();
    assert!(s.records["deal"].deleted);
    let mut foreign = page(3, "other", 10.);
    foreign.account_id = "foreign".into();
    assert!(s.apply(&foreign).is_err());
    assert_eq!(s.cursor, 3);
    assert_eq!(s.records.len(), 1);
    let mut foreign = page(3, "other", 10.);
    foreign.currency = "USD".into();
    assert!(s.apply(&foreign).is_err());
    assert_eq!(s.cursor, 3);
}
#[test]
fn account_cash_flow_capacity_never_evicts_facts_or_advances_the_cursor() {
    let mut s = AccountCashFlows::from_page(&page(0, "0", 0.)).unwrap();
    for i in 1..aqe_live_protocol::ACCOUNT_CASH_FLOW_RETAINED_ROWS {
        s.apply(&page(i as u64, &i.to_string(), i as f64)).unwrap();
    }
    let cursor = s.cursor;
    assert!(s.apply(&page(cursor, "overflow", 1.)).is_err());
    assert!(s.capacity_exceeded);
    assert_eq!(s.cursor, cursor);
    assert_eq!(s.records.len(), 256);
}
#[test]
fn account_cash_flow_missing_prefix_is_not_empty_or_complete_history() {
    let mut p = page(10, "late", 1.);
    p.first_after_sequence = 10;
    let s = AccountCashFlows::from_page(&p).unwrap();
    assert!(s.missing_prefix);
    assert_eq!(s.cursor, 11);
    let mut p = page(1, "bad-start", 1.);
    p.first_after_sequence = 0;
    assert!(AccountCashFlows::from_page(&p).is_err());
}
#[test]
fn account_cash_flow_source_cannot_retract_an_unread_tail() {
    let mut p = page(0, "first", 1.);
    p.through_sequence = 3;
    let mut s = AccountCashFlows::from_page(&p).unwrap();
    assert!(s.apply(&page(1, "second", 2.)).is_err());
    assert_eq!(s.cursor, 1);
    assert_eq!(s.through_sequence, 3);
    assert!(!s.records.contains_key("second"));
}
#[tokio::test]
async fn account_cash_flow_paper_is_explicitly_closed_and_read_only_not_a_deposit_api() {
    use crate::core::broker::{
        paper_broker::PaperBroker,
        traits::{Broker, OrderManagementProvider},
        types::AccountType,
    };
    let b = PaperBroker::new(AccountType::Paper, 1000., 1);
    b.connect().await.unwrap();
    let account = b.get_account().await.unwrap();
    let p = b
        .account_cash_flow_reader()
        .unwrap()
        .read(&account.account_id, None, 0)
        .await
        .unwrap()
        .unwrap();
    p.validate(&account.account_id).unwrap();
    assert_eq!(p.coverage, AccountCashFlowCoverage::ClosedSimulation);
    assert!(p.records.is_empty());
    assert_eq!(
        aqe_live_protocol::cash_flow_adjusted_change_pct(100., 105., &p, 10, 100),
        Some(5.)
    );
    assert!(
        b.account_cash_flow_reader()
            .unwrap()
            .read("foreign", None, 0)
            .await
            .is_err()
    );
    assert_eq!(b.get_account().await.unwrap().cash, account.cash);
    assert!(!b.has_pending_trade_events());
    b.disconnect().await.unwrap();
}
