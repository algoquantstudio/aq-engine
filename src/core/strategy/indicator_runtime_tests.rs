#[test]
fn builtin_indicator_runtime_preserves_multicolumn_batches_and_recursive_history() {
    use crate::core::indicators::{I::*, Indicator};
    let mut state = StrategyState::new(
        "Indicators".into(),
        "1".into(),
        NoopStrategy,
        UnifiedBroker::new(
            PaperBroker::new(AccountType::Paper, 100_000.0, 1),
            FixedScaleOutDataFeed::new(),
        ),
        StrategyMode::Backtest,
        TimeFrame::new(1, TimeFrameUnit::Minute),
    );
    state.update_max_history_rows(4);
    for indicator in [
        Box::new(Macd::new(2, 5, 3, "close").with_output_prefix("m")) as Box<dyn Indicator>,
        Box::new(VolumeProfile::new(3, 1.).with_output_prefix("vp")),
        Box::new(ExponentialMovingAverage::close(5)),
    ] {
        state.register_indicator(indicator);
    }
    let mut dummy = NoopStrategy;
    for symbol in ["AAPL", "BTC-USD"] {
        let scale = if symbol == "AAPL" { 1.0 } else { 100.0 };
        let bars: Vec<_> = (0..40)
            .map(|i| Bar {
                symbol: symbol.into(),
                timestamp: Utc.timestamp_opt(1_700_000_000 + i * 60, 0).unwrap(),
                open: scale * (10. + i as f64 / 2.),
                high: scale * (12. + i as f64 / 2.),
                low: scale * (9. + i as f64 / 2.),
                close: scale * (10. + (i as f64 / 3.).sin() + i as f64 / 2.),
                volume: 100.0,
            })
            .collect();
        let mut expected = state
            .broker
            .data
            .format_on_bar(BarData::Bars(bars.clone()))
            .unwrap();
        for mut indicator in [
            Box::new(Macd::new(2, 5, 3, "close").with_output_prefix("m")) as Box<dyn Indicator>,
            Box::new(VolumeProfile::new(3, 1.).with_output_prefix("vp")),
            Box::new(ExponentialMovingAverage::close(5)),
        ] {
            indicator.run(&mut expected).unwrap();
        }
        for batch in bars.chunks(3) {
            state._on_bar(&mut dummy, symbol, &BarData::Bars(batch.to_vec()));
        }
        let history = &state.history[symbol];
        assert_eq!(history.height(), 4);
        for name in [
            "m",
            "m_signal",
            "m_histogram",
            "vp",
            "vp_val",
            "vp_vah",
            "vp_volume",
            "EMA_5",
        ] {
            let actual: Vec<_> = history
                .column(name)
                .unwrap()
                .f64()
                .unwrap()
                .iter()
                .collect();
            let expected: Vec<_> = expected
                .column(name)
                .unwrap()
                .f64()
                .unwrap()
                .tail(Some(4))
                .iter()
                .collect();
            for (a, b) in actual.into_iter().zip(expected) {
                assert!(
                    (a.unwrap() - b.unwrap()).abs() < 1e-8,
                    "{symbol} {name}: {a:?} {b:?}"
                );
            }
        }
    }
}

#[test]
fn builtin_indicator_registration_reorders_chained_sources_without_losing_new_bars() {
    use crate::core::indicators::I::*;
    let mut state = StrategyState::new(
        "Chained indicators".into(),
        "1".into(),
        NoopStrategy,
        UnifiedBroker::new(
            PaperBroker::new(AccountType::Paper, 100_000.0, 1),
            FixedScaleOutDataFeed::new(),
        ),
        StrategyMode::Backtest,
        TimeFrame::new(1, TimeFrameUnit::Minute),
    );
    let bar = |i: i64| Bar {
        symbol: "AAPL".into(),
        timestamp: Utc.timestamp_opt(1_700_000_000 + i * 60, 0).unwrap(),
        open: i as f64,
        high: i as f64 + 1.,
        low: i as f64 - 1.,
        close: i as f64,
        volume: 10.,
    };
    let mut dummy = NoopStrategy;
    state._on_bar(
        &mut dummy,
        "AAPL",
        &BarData::Bars((1..=4).map(bar).collect()),
    );
    // Consumer comes first: its nullable column is physically before its producer.
    state.register_indicator(Box::new(
        SimpleMovingAverage::new(2, "producer").with_output_prefix("consumer"),
    ));
    state.register_indicator(Box::new(
        SimpleMovingAverage::close(2).with_output_prefix("producer"),
    ));
    assert_eq!(state.indicator_order, ["producer", "consumer"]);
    state._on_bar(&mut dummy, "AAPL", &BarData::Bars(vec![bar(5), bar(6)]));
    let history = &state.history["AAPL"];
    assert_eq!(history.height(), 6);
    assert_eq!(
        history.column("consumer").unwrap().f64().unwrap().get(5),
        Some(5.)
    );
    assert_eq!(
        history.column("producer").unwrap().f64().unwrap().get(5),
        Some(5.5)
    );
}
