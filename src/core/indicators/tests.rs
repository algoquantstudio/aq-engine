use super::{I::*, *};

fn values(df: &DataFrame, name: &str) -> Vec<Option<f64>> {
    df.column(name).unwrap().f64().unwrap().iter().collect()
}
fn near(actual: Option<f64>, expected: Option<f64>) {
    match (actual, expected) {
        (Some(a), Some(b)) => assert!((a - b).abs() <= 1e-9 * b.abs().max(1.0), "{a} != {b}"),
        (None, None) => (),
        pair => panic!("null mismatch: {pair:?}"),
    }
}
fn check(indicator: &mut dyn Indicator, df: &DataFrame, expected: &[Option<f64>]) {
    let mut frame = df.clone();
    indicator.run(&mut frame).unwrap();
    for (actual, expected) in values(&frame, &indicator.name()).into_iter().zip(expected) {
        near(actual, *expected);
    }
    assert_eq!(frame.height(), expected.len());
    near(
        indicator
            .run_bar(df)
            .unwrap()
            .get(&indicator.name())
            .copied(),
        *expected.last().unwrap(),
    );
}
fn fixture(n: usize, gaps: bool) -> DataFrame {
    let close: Vec<_> = (0..n)
        .map(|i| {
            if gaps && i % 17 == 7 {
                None
            } else if gaps && i % 31 == 5 {
                Some(f64::NAN)
            } else {
                Some(100.0 + (i as f64 / 7.0).sin() * 5.0 + i as f64 * 0.03)
            }
        })
        .collect();
    df!(
        "timestamp" => (0..n as i64).collect::<Vec<_>>(),
        "open" => close.iter().map(|x| x.map(|v| v-0.5)).collect::<Vec<_>>(),
        "high" => close.iter().map(|x| x.map(|v| v+1.0)).collect::<Vec<_>>(),
        "low" => close.iter().map(|x| x.map(|v| v-1.0)).collect::<Vec<_>>(),
        "close" => close,
        "volume" => (0..n).map(|i| (i % 11 + 1) as i64).collect::<Vec<_>>()
    )
    .unwrap()
}
fn builtin(index: usize) -> Box<dyn Indicator> {
    match index {
        0 => Box::new(SimpleMovingAverage::close(5)),
        1 => Box::new(ExponentialMovingAverage::close(5)),
        2 => Box::new(RelativeStrengthIndex::close(5)),
        3 => Box::new(AverageTrueRange::new(5)),
        4 => Box::new(QStick::new(5)),
        5 => Box::new(Macd::new(3, 7, 4, "close")),
        6 => Box::new(BollingerBands::new(5, 2.0, "close")),
        7 => Box::new(VolumeWeightedMovingAverage::new(5, "close")),
        8 => Box::new(RateOfChange::new(5, "close")),
        9 => Box::new(VolumeProfile::new(5, 1.0)),
        10 => Box::new(AverageTrueRange::new(5).with_smoothing(Smoothing::Wilder)),
        11 => Box::new(QStick::new(5).with_smoothing(Smoothing::Exponential)),
        _ => unreachable!(),
    }
}

#[test]
fn moving_averages_and_qstick_match_hand_calculations() {
    let df = df!("close"=>[1.0,2.0,3.0,8.0,5.0], "open"=>[0.0,3.0,1.0,4.0,6.0]).unwrap();
    check(
        &mut SimpleMovingAverage::close(3),
        &df,
        &[None, None, Some(2.0), Some(13.0 / 3.0), Some(16.0 / 3.0)],
    );
    check(
        &mut ExponentialMovingAverage::close(3),
        &df,
        &[None, None, Some(2.0), Some(5.0), Some(5.0)],
    );
    check(
        &mut QStick::new(3),
        &df,
        &[
            None,
            None,
            Some(2.0 / 3.0),
            Some(5.0 / 3.0),
            Some(5.0 / 3.0),
        ],
    );
}

#[test]
fn rsi_handles_flat_and_directional_prices_and_wilder_recursion() {
    let df = df!("close"=>[10.0,11.0,10.0,12.0,11.0]).unwrap();
    check(
        &mut RelativeStrengthIndex::close(3),
        &df,
        &[None, None, None, Some(75.0), Some(600.0 / 11.0)],
    );
    for (prices, result) in [
        (vec![2.0; 6], 50.0),
        (vec![1., 2., 3., 4., 5., 6.], 100.0),
        (vec![6., 5., 4., 3., 2., 1.], 0.0),
    ] {
        let mut df = df!("close"=>prices).unwrap();
        let mut rsi = RelativeStrengthIndex::close(3);
        rsi.run(&mut df).unwrap();
        near(values(&df, &rsi.name())[5], Some(result));
    }
}

#[test]
fn atr_preserves_legacy_simple_default_and_supports_wilder() {
    let df =
        df!("high"=>[10.,12.,13.,18.],"low"=>[8.,9.,11.,15.],"close"=>[9.,11.,12.,16.]).unwrap();
    check(
        &mut AverageTrueRange::new(3),
        &df,
        &[None, None, Some(7. / 3.), Some(11. / 3.)],
    );
    check(
        &mut AverageTrueRange::new(3).with_smoothing(Smoothing::Wilder),
        &df,
        &[None, None, Some(7. / 3.), Some(32. / 9.)],
    );
}

#[test]
fn macd_has_independent_ema_seeds_and_three_outputs() {
    let mut df = df!("close"=>[1.,2.,4.,3.,7.,5.]).unwrap();
    let mut indicator = Macd::new(2, 3, 2, "close").with_output_prefix("m");
    indicator.run(&mut df).unwrap();
    let line = [
        None,
        None,
        Some(5. / 6.),
        Some(7. / 18.),
        Some(23. / 27.),
        Some(101. / 324.),
    ];
    let signal = [
        None,
        None,
        None,
        Some(11. / 18.),
        Some(125. / 162.),
        Some(113. / 243.),
    ];
    for i in 0..6 {
        near(values(&df, "m")[i], line[i]);
        near(values(&df, "m_signal")[i], signal[i]);
        near(
            values(&df, "m_histogram")[i],
            line[i].zip(signal[i]).map(|(a, b)| a - b),
        );
    }
    assert_eq!(indicator.window_size(), 4);
}

#[test]
fn bands_use_population_deviation() {
    let mut df = df!("close"=>[1.,2.,3.,5.]).unwrap();
    BollingerBands::new(3, 2., "close")
        .with_output_prefix("b")
        .run(&mut df)
        .unwrap();
    near(values(&df, "b")[2], Some(2.));
    near(
        values(&df, "b_upper")[2],
        Some(2. + 2. * (2.0f64 / 3.).sqrt()),
    );
    near(
        values(&df, "b_lower")[3],
        Some(10. / 3. - 2. * (14.0f64 / 9.).sqrt()),
    );
}

#[test]
fn vwma_and_roc_handle_volume_and_zero_denominators() {
    let df = df!("close"=>[10.,20.,30.,40.],"volume"=>[1i64,2,3,0]).unwrap();
    check(
        &mut VolumeWeightedMovingAverage::new(3, "close"),
        &df,
        &[None, None, Some(140. / 6.), Some(130. / 5.)],
    );
    check(
        &mut RateOfChange::new(2, "close"),
        &df,
        &[None, None, Some(200.), Some(100.)],
    );
    let df = df!("close"=>[0.,1.,2.,3.],"volume"=>[0.,0.,-1.,1.]).unwrap();
    check(
        &mut VolumeWeightedMovingAverage::new(2, "close"),
        &df,
        &[None, None, None, None],
    );
    check(
        &mut RateOfChange::new(1, "close"),
        &df,
        &[None, None, Some(100.), Some(50.)],
    );
}

#[test]
fn volume_profile_rolls_bins_and_exposes_poc_area_and_histogram() {
    let mut df = df!("close"=>[10.1,11.2,11.8,13.1,10.2],"volume"=>[2.,3.,4.,1.,10.]).unwrap();
    let mut vp = VolumeProfile::new(4, 1.)
        .with_value_area(0.7)
        .with_output_prefix("vp");
    vp.run(&mut df).unwrap();
    for (name, a, b) in [
        ("vp", 11.5, 10.5),
        ("vp_val", 11., 10.),
        ("vp_vah", 12., 12.),
        ("vp_volume", 10., 18.),
    ] {
        let values = values(&df, name);
        assert_eq!(&values[..3], &[None; 3]);
        near(values[3], Some(a));
        near(values[4], Some(b));
    }
    let bins = vp.histogram(&df).unwrap();
    assert_eq!(bins.len(), 3);
    assert_eq!(
        bins[0],
        VolumeProfileBin {
            low: 10.,
            high: 11.,
            volume: 10.
        }
    );
    assert_eq!(bins[1].volume, 7.);
    let mut tie = df!("close"=>[-0.1,1.1],"volume"=>[1.,1.]).unwrap();
    VolumeProfile::new(2, 1.)
        .with_output_prefix("tie")
        .run(&mut tie)
        .unwrap();
    near(values(&tie, "tie")[1], Some(-0.5));
    near(values(&tie, "tie_val")[1], Some(-1.));
    near(values(&tie, "tie_vah")[1], Some(2.));
}

#[test]
fn all_builtins_match_bulk_for_single_and_batched_updates_with_trimmed_history() {
    for gaps in [false, true] {
        let input = fixture(223, gaps);
        for index in 0..12 {
            let mut expected = input.clone();
            builtin(index).run(&mut expected).unwrap();
            for batch in [1, 7, 37] {
                let mut indicator = builtin(index);
                let mut history = input.head(Some(21));
                indicator.seed_stream("BTC:5m", &mut history).unwrap();
                let names = indicator.output_columns();
                let mut offset = history.height();
                while offset < input.height() {
                    let count = batch.min(input.height() - offset);
                    let mut incoming = input.slice(offset as i64, count);
                    indicator
                        .update_stream("BTC:5m", &history, &mut incoming)
                        .unwrap();
                    for name in &names {
                        let actual = values(&incoming, name);
                        let full = values(&expected, name);
                        for i in 0..count {
                            near(actual[i], full[offset + i]);
                        }
                    }
                    history.vstack_mut(&incoming).unwrap();
                    history = history.tail(Some(3)); // Smaller than every indicator window.
                    offset += count;
                }
            }
        }
    }
}

#[test]
fn streams_are_isolated_by_symbol_and_timeframe_and_can_be_reseeded() {
    let source = fixture(50, false);
    let mut indicator = ExponentialMovingAverage::close(5);
    let mut a = source.head(Some(20));
    let mut b = source.slice(10, 20);
    indicator.seed_stream("BTC:1m", &mut a).unwrap();
    indicator.seed_stream("BTC:5m", &mut b).unwrap();
    for (key, start, history) in [("BTC:1m", 20, &a), ("BTC:5m", 30, &b), ("ETH:1m", 20, &a)] {
        let mut next = source.slice(start, 1);
        indicator.update_stream(key, history, &mut next).unwrap();
        let mut expected = source.slice(start - 20, 21);
        ExponentialMovingAverage::close(5)
            .run(&mut expected)
            .unwrap();
        near(values(&next, "EMA_5")[0], values(&expected, "EMA_5")[20]);
    }
    indicator.reset_stream("BTC:1m");
    let mut next = source.slice(20, 1);
    indicator.update_stream("BTC:1m", &a, &mut next).unwrap();
    let mut expected = source.head(Some(21));
    indicator.run(&mut expected).unwrap();
    near(values(&next, "EMA_5")[0], values(&expected, "EMA_5")[20]);
    // Replacing history's final row invalidates its cached continuation automatically.
    let changed = df!("timestamp"=>[99i64,100,101,102,103],"close"=>[2.,4.,6.,8.,10.]).unwrap();
    let mut next = df!("timestamp"=>[104i64],"close"=>[12.]).unwrap();
    indicator
        .update_stream("BTC:1m", &changed, &mut next)
        .unwrap();
    near(values(&next, "EMA_5")[0], Some(8.));
}

#[test]
fn missing_and_nonfinite_values_have_consistent_warmup_and_recover() {
    let df = df!("close"=>[Some(1.),None,Some(3.),Some(f64::INFINITY),Some(5.),Some(7.)]).unwrap();
    check(
        &mut SimpleMovingAverage::close(2),
        &df,
        &[None, None, None, None, None, Some(6.)],
    );
    check(
        &mut ExponentialMovingAverage::close(2),
        &df,
        &[None, None, Some(2.), Some(2.), Some(4.), Some(6.)],
    );
    let mut df =
        df!("close"=>[10.,11.,12.,13.],"volume"=>[Some(1.),None,Some(2.),Some(3.)]).unwrap();
    VolumeProfile::new(2, 1.)
        .with_output_prefix("v")
        .run(&mut df)
        .unwrap();
    assert_eq!(values(&df, "v_volume"), vec![None, None, None, Some(5.)]);
}

#[test]
fn arithmetic_overflow_does_not_poison_future_windows() {
    let df = df!("close"=>[1e308,1e308,2.,4.]).unwrap();
    check(
        &mut SimpleMovingAverage::close(2),
        &df,
        &[None, None, None, Some(3.)],
    );
    check(
        &mut ExponentialMovingAverage::close(2),
        &df,
        &[None, None, None, Some(3.)],
    );
    let mut df = df!("close"=>[1e308,-1e308,2.,4.]).unwrap();
    BollingerBands::new(2, 2., "close")
        .with_output_prefix("b")
        .run(&mut df)
        .unwrap();
    near(values(&df, "b")[3], Some(3.));
    near(values(&df, "b_upper")[3], Some(5.));
    let mut df = df!("close"=>[1.,2.,3.,4.],"volume"=>[1e308,1e308,2.,4.]).unwrap();
    VolumeProfile::new(2, 1.)
        .with_output_prefix("vp")
        .run(&mut df)
        .unwrap();
    near(values(&df, "vp_volume")[3], Some(6.));
}

#[test]
fn arbitrary_numeric_sources_and_output_prefixes_are_supported() {
    let mut df = df!("open"=>[1i32,3,5],"close"=>[2.,4.,6.],"size"=>[1u32,2,3],"h"=>[3.,5.,7.],"l"=>[0.,2.,4.]).unwrap();
    SimpleMovingAverage::new(2, "open").run(&mut df).unwrap();
    assert_eq!(values(&df, "SMA_2_open"), vec![None, Some(2.), Some(4.)]);
    assert_ne!(
        ExponentialMovingAverage::new(2, "open").name(),
        ExponentialMovingAverage::close(2).name()
    );
    assert_ne!(
        RelativeStrengthIndex::new(2, "open").name(),
        RelativeStrengthIndex::close(2).name()
    );
    assert_eq!(SimpleMovingAverage::new(2, "").name(), "SMA_2_close");
    let configured: Vec<Box<dyn Indicator>> = vec![
        Box::new(
            AverageTrueRange::new(2)
                .with_output_prefix("atr")
                .with_columns("h", "l", "close")
                .with_smoothing(Smoothing::Wilder),
        ),
        Box::new(
            QStick::new(2)
                .with_output_prefix("q")
                .with_columns("open", "close")
                .with_smoothing(Smoothing::Exponential),
        ),
        Box::new(
            VolumeWeightedMovingAverage::new(2, "open")
                .with_output_prefix("v")
                .with_volume_column("size"),
        ),
        Box::new(
            VolumeProfile::new(2, 1.)
                .with_output_prefix("p")
                .with_columns("open", "size")
                .with_value_area(0.9),
        ),
    ];
    for (mut indicator, name) in configured.into_iter().zip(["atr", "q", "v", "p"]) {
        assert_eq!(indicator.name(), name);
        indicator.run(&mut df).unwrap();
        assert!(values(&df, name)[2].is_some());
    }
}

#[test]
fn invalid_configurations_and_input_types_return_errors_without_panics() {
    let mut invalid: Vec<Box<dyn Indicator>> = vec![
        Box::new(SimpleMovingAverage::close(0)),
        Box::new(RelativeStrengthIndex::close(usize::MAX)),
        Box::new(Macd::new(12, 12, 9, "close")),
        Box::new(Macd::new(1, 2, 0, "close")),
        Box::new(BollingerBands::new(2, f64::NAN, "close")),
        Box::new(VolumeProfile::new(2, 0.)),
        Box::new(VolumeProfile::new(2, 1.).with_value_area(1.1)),
        Box::new(QStick::new(2).with_output_prefix("close")),
        Box::new(SimpleMovingAverage::close(2).with_output_prefix(" ")),
    ];
    for indicator in &mut invalid {
        assert!(indicator.run(&mut fixture(3, false)).is_err());
    }
    let mut df = df!("close"=>["1","2"]).unwrap();
    assert!(SimpleMovingAverage::close(2).run(&mut df).is_err());
    assert!(
        SimpleMovingAverage::new(2, "missing")
            .run(&mut fixture(3, false))
            .is_err()
    );
}

#[test]
fn chunked_frames_and_repeated_runs_preserve_schema_and_do_not_look_ahead() {
    let source = fixture(100, false);
    let mut chunked = source.head(Some(43));
    chunked.vstack_mut(&source.slice(43, 57)).unwrap();
    for index in 0..12 {
        let mut indicator = builtin(index);
        let mut full = chunked.clone();
        indicator.run(&mut full).unwrap();
        let latest = indicator.run_bar(&chunked).unwrap();
        for name in indicator.output_columns() {
            near(latest.get(&name).copied(), values(&full, &name)[99]);
        }
        let width = full.width();
        indicator.run(&mut full).unwrap();
        assert_eq!(full.width(), width);
        let mut prefix = chunked.head(Some(67));
        indicator.run(&mut prefix).unwrap();
        for name in indicator.output_columns() {
            assert_eq!(values(&full, &name)[..67], values(&prefix, &name));
        }
        let mut empty = source.clear();
        indicator.run(&mut empty).unwrap();
        assert_eq!(empty.height(), 0);
        assert_eq!(empty.width(), width);
    }
}

#[test]
fn rolling_bands_remain_accurate_for_large_prices_with_small_spread() {
    let prices: Vec<_> = (0..5000).map(|i| 1e9 + (i % 13) as f64 * 0.25).collect();
    let mut df = df!("close"=>prices.clone()).unwrap();
    BollingerBands::new(50, 2., "close")
        .with_output_prefix("b")
        .run(&mut df)
        .unwrap();
    for i in (49..5000).step_by(71) {
        let window = &prices[i - 49..=i];
        let mean = window.iter().sum::<f64>() / 50.;
        let std = (window.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / 50.).sqrt();
        assert!((values(&df, "b_upper")[i].unwrap() - (mean + 2. * std)).abs() < 0.0001);
    }
}

#[test]
fn dependency_order_is_stable_and_rejects_cycles_and_output_collisions() {
    let mut map: HashMap<String, Box<dyn Indicator>> = HashMap::new();
    for indicator in [
        Box::new(SimpleMovingAverage::new(2, "z").with_output_prefix("a")),
        Box::new(SimpleMovingAverage::close(2).with_output_prefix("z")),
    ] {
        map.insert(indicator.name(), indicator);
    }
    assert_eq!(execution_order(&map).unwrap(), ["z", "a"]);
    map.insert(
        "z".into(),
        Box::new(SimpleMovingAverage::new(2, "a").with_output_prefix("z")),
    );
    assert!(execution_order(&map).unwrap_err().contains("Cyclic"));
    map.insert(
        "m".into(),
        Box::new(Macd::default().with_output_prefix("m")),
    );
    map.insert(
        "other".into(),
        Box::new(SimpleMovingAverage::close(2).with_output_prefix("m_signal")),
    );
    assert!(execution_order(&map).unwrap_err().contains("Duplicate"));
}

#[test]
#[ignore = "manual, offline throughput measurement; no timing assertions"]
fn indicator_throughput_smoke() {
    use std::time::Instant;
    for index in 0..10 {
        let rows = if index == 9 { 10_000 } else { 100_000 };
        let mut df = fixture(rows, false);
        let mut indicator = builtin(index);
        let start = Instant::now();
        indicator.seed_stream("bench", &mut df).unwrap();
        let bulk = start.elapsed();
        let mut history = df.tail(Some(3));
        let incoming = fixture(1000, false);
        let start = Instant::now();
        for i in 0..1000 {
            let mut row = incoming.slice(i, 1);
            indicator
                .update_stream("bench", &history, &mut row)
                .unwrap();
            history = row;
        }
        eprintln!(
            "{}: bulk {rows} rows {:?} ({:.0} rows/s), 1000 single-row Polars updates {:?}",
            indicator.name(),
            bulk,
            rows as f64 / bulk.as_secs_f64(),
            start.elapsed()
        );
    }
    // Long-window standard indicators must not scan the entire lookback per new row.
    for period in [20, 1000, 100_000] {
        let mut df = fixture(100_001, false);
        let mut indicator = SimpleMovingAverage::close(period);
        let start = Instant::now();
        indicator.seed_stream("long", &mut df).unwrap();
        let bulk = start.elapsed();
        let mut row = fixture(1, false);
        let start = Instant::now();
        indicator.update_stream("long", &df, &mut row).unwrap();
        eprintln!(
            "SMA period {period}: 100001 rows {bulk:?}, single-row continuation {:?}",
            start.elapsed()
        );
    }
    for period in [100, 1000] {
        let rows = 10_000;
        let mut df = fixture(rows, false);
        let mut indicator = VolumeProfile::new(period, 0.01);
        let start = Instant::now();
        indicator.run(&mut df).unwrap();
        eprintln!(
            "Volume profile period {period}, bin width 0.01: {rows} rows {:?}",
            start.elapsed()
        );
        let start = Instant::now();
        let bins = indicator.histogram(&df).unwrap();
        eprintln!(
            "Final histogram {} occupied bins: {:?}",
            bins.len(),
            start.elapsed()
        );
    }
}
