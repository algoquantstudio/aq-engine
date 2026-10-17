use super::*;
use crate::core::broker::position_valuation::PositionValuationReader;
use aqe_live_protocol::{
    ExecutionFillSide, POSITION_VALUATION_VERSION, PositionNotional, PositionQuoteTime,
    PositionValuation, PositionValuationClock, PositionValuationState, PositionValuations,
    validate_position_valuation_request,
};

#[async_trait::async_trait]
impl PositionValuationReader for PaperBroker {
    async fn read_positions(
        &self,
        account_id: &str,
        symbol: &str,
        position_ids: &[String],
    ) -> Result<Option<PositionValuations>, BrokerError> {
        validate_position_valuation_request(account_id, symbol, position_ids)
            .map_err(|e| BrokerError::TradeError(e.into()))?;
        let Some(_guard) = self.valuation_gate.try_read() else {
            return Ok(None);
        };
        if !self.is_connected() && self.backtest_state.is_none() {
            return Ok(None);
        }
        let account = self.account.read().clone();
        if account.account_id != account_id {
            return Err(BrokerError::TradeError(
                "paper valuation account mismatch".into(),
            ));
        }
        let Some(as_of_us) = *self.valuation_as_of_us.read() else {
            return Ok(None);
        };
        let mark_time = self.valuation_mark_times.get(symbol).map(|t| *t);
        for id in position_ids {
            if self
                .orders
                .get(id)
                .is_some_and(|o| o.asset.symbol != symbol)
            {
                return Err(BrokerError::TradeError(
                    "paper valuation symbol mismatch".into(),
                ));
            }
        }
        let book = self.positions.get(symbol);
        let mut positions = Vec::with_capacity(position_ids.len());
        for id in position_ids {
            let state = match book.as_ref().and_then(|b| b.get(id)) {
                None => PositionValuationState::NotOpen,
                Some(p) => PositionValuationState::Open {
                    quantity: p.qty,
                    side: match p.side {
                        OrderSide::Buy => ExecutionFillSide::Buy,
                        OrderSide::Sell => ExecutionFillSide::Sell,
                    },
                    average_entry_price: p.avg_entry_price,
                    current_price: p.current_price,
                    gross_unrealized_pnl: p.unrealized_pnl,
                    // Paper carry settles on exit; it is not also accrued here.
                    carry: 0.,
                    quote_time: mark_time.map(|occurred_at_us| PositionQuoteTime {
                        occurred_at_us,
                        clock: PositionValuationClock::Utc,
                    }),
                    notional: PositionNotional::PaperPriceUnits {
                        account_value: (p.qty * p.current_price).abs(),
                    },
                },
            };
            positions.push(PositionValuation {
                position_id: id.clone(),
                state,
            });
        }
        let values = PositionValuations {
            version: POSITION_VALUATION_VERSION,
            provider: "paper-position-v1".into(),
            account_id: account.account_id,
            account_currency: account.currency,
            symbol: symbol.into(),
            as_of_us,
            positions,
        };
        values
            .validate(account_id, symbol, position_ids)
            .map_err(|e| BrokerError::TradeError(e.into()))?;
        Ok(Some(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        insight::{Insight, types::StrategyType},
        utils::timeframe::{TimeFrame, TimeFrameUnit},
    };
    use aqe_live_protocol::PositionValuationFreshness;

    async fn open(b: &PaperBroker, symbol: &str, side: OrderSide) -> String {
        let mut insight = Insight::new(
            side,
            symbol.into(),
            StrategyType::Testing,
            TimeFrame::new(1, TimeFrameUnit::Minute),
            80,
            None,
        );
        insight.set_quantity(Some(2.));
        b.submit_order(insight.into()).await.unwrap().order_id
    }
    fn step(b: &PaperBroker, symbol: &str, price: f64, minute: i64) {
        let at = DateTime::from_timestamp(1_700_000_000 + minute * 60, 0).unwrap();
        b.process_step(
            &HashMap::from([(
                symbol.into(),
                Bar {
                    symbol: symbol.into(),
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume: 1000.,
                    timestamp: at,
                },
            )]),
            at,
        );
    }
    #[tokio::test]
    async fn paper_live_marks_preserve_hedges_partial_exits_and_sparse_symbol_pnl() {
        let b = PaperBroker::new(AccountType::Paper, 10000., 1);
        b.connect().await.unwrap();
        let account = b.get_account_sync().unwrap().account_id;
        let long = open(&b, "A", OrderSide::Buy).await;
        let short = open(&b, "B", OrderSide::Sell).await;
        step(&b, "A", 100., 0);
        step(&b, "B", 100., 1);
        step(&b, "A", 110., 2);
        step(&b, "B", 90., 3);
        assert_eq!(
            b.get_account_sync().unwrap().equity,
            10040.,
            "sparse updates retain other-symbol P&L"
        );
        let a = b
            .read_positions(&account, "A", &[long.clone()])
            .await
            .unwrap()
            .unwrap();
        let bb = b
            .read_positions(&account, "B", &[short.clone()])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            a.freshness(&a.positions[0], a.as_of_us, 30_000_000),
            PositionValuationFreshness::Stale
        );
        assert_eq!(
            bb.freshness(&bb.positions[0], bb.as_of_us, 30_000_000),
            PositionValuationFreshness::Fresh
        );
        if let PositionValuationState::Open {
            gross_unrealized_pnl,
            notional,
            side,
            ..
        } = &bb.positions[0].state
        {
            assert_eq!(*gross_unrealized_pnl, 20.);
            assert_eq!(*side, ExecutionFillSide::Sell);
            assert_eq!(
                *notional,
                PositionNotional::PaperPriceUnits {
                    account_value: 180.
                }
            );
        } else {
            panic!("open short expected");
        }
        b.close_position(&short, 1., Some(90.)).await.unwrap();
        step(&b, "B", 90., 4);
        let partial = b
            .read_positions(&account, "B", &[short.clone()])
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            partial.positions[0].state,
            PositionValuationState::Open {
                quantity: 1.,
                gross_unrealized_pnl: 10.,
                ..
            }
        ));
        b.close_position(&short, 0., Some(90.)).await.unwrap();
        step(&b, "B", 90., 5);
        let closed = b
            .read_positions(&account, "B", &[short.clone()])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(closed.positions[0].state, PositionValuationState::NotOpen);
        let before = b.orders.len();
        assert!(
            b.read_positions("foreign", "A", &[long.clone()])
                .await
                .is_err()
        );
        assert!(
            b.read_positions(&account, "B", &[long.clone()])
                .await
                .is_err()
        );
        assert!(
            b.read_positions(&account, "A", &[long.clone(), long.clone()])
                .await
                .is_err()
        );
        {
            let _step = b.valuation_gate.write();
            assert!(
                b.read_positions(&account, "A", &[long.clone()])
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(b.orders.len(), before);
        b.disconnect().await.unwrap();
        assert!(
            b.read_positions(&account, "A", &[long])
                .await
                .unwrap()
                .is_none()
        );
    }
    #[tokio::test]
    async fn backtest_uses_identical_paper_valuation_without_broker_clock_or_fx_inference() {
        let mut b = PaperBroker::new(AccountType::Paper, 10000., 1);
        b.set_backtest_state(Arc::new(RwLock::new(BacktestState::new())));
        let account = b.get_account_sync().unwrap().account_id;
        assert!(
            b.read_positions(&account, "CUSTOM", &["missing".into()])
                .await
                .unwrap()
                .is_none()
        );
        let id = open(&b, "CUSTOM", OrderSide::Buy).await;
        step(&b, "CUSTOM", 100., 0);
        step(&b, "CUSTOM", 110., 1);
        let p = b
            .read_positions(&account, "CUSTOM", &[id.clone()])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(p.provider, "paper-position-v1");
        assert!(matches!(
            p.positions[0].state,
            PositionValuationState::Open {
                gross_unrealized_pnl: 20.,
                notional: PositionNotional::PaperPriceUnits {
                    account_value: 220.
                },
                ..
            }
        ));
        // Observation time does not jump when the UI reads a historical snapshot.
        assert_eq!(
            p,
            b.read_positions(&account, "CUSTOM", &[id])
                .await
                .unwrap()
                .unwrap()
        );
    }
    #[tokio::test]
    async fn same_symbol_opposing_positions_remain_separate_and_paper_units_are_explicit() {
        let b = PaperBroker::new(AccountType::Paper, 10000., 1);
        b.connect().await.unwrap();
        let account = b.get_account_sync().unwrap().account_id;
        let long = open(&b, "CUSTOM", OrderSide::Buy).await;
        let short = open(&b, "CUSTOM", OrderSide::Sell).await;
        // Instrument metadata must not silently change the paper execution
        // model's price-unit accounting into live MT5 contract accounting.
        let mut asset = b.orders.get(&long).unwrap().asset.clone();
        asset.contract_size = Some(100);
        b.register_asset_metadata(&asset);
        step(&b, "CUSTOM", 100., 0);
        step(&b, "CUSTOM", 110., 1);
        let ids = vec![long, short];
        let before_events = b.trade_events.lock().len();
        let p = b
            .position_valuation_reader()
            .unwrap()
            .read_positions(&account, "CUSTOM", &ids)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(p.positions.len(), 2);
        for (position, side, pnl) in [
            (&p.positions[0], ExecutionFillSide::Buy, 20.),
            (&p.positions[1], ExecutionFillSide::Sell, -20.),
        ] {
            assert!(
                matches!(position.state,PositionValuationState::Open{side:s,gross_unrealized_pnl:v,
                notional:PositionNotional::PaperPriceUnits{account_value:220.},..} if s==side&&v==pnl)
            );
        }
        assert_eq!(
            b.trade_events.lock().len(),
            before_events,
            "analytics cannot emit fills"
        );
        assert_eq!(b.get_account_sync().unwrap().equity, 10000.);
    }
}
