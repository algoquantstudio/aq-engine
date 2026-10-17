// Shared built-in alpha regressions, using the offline context above.
use crate::core::alpha::{EmaPriceCrossover, TestEntry};

fn builtin_model(kind: &str, options: BuiltinAlphaOptions) -> Box<dyn AlphaModel> {
    match kind {
        "test" => Box::new(TestEntry::new(2, false, 2, String::new()).with_options(options)),
        "ema" => Box::new(EmaPriceCrossover::new(2, 2, String::new()).with_options(options)),
        _ => Box::new(RsiDiverganceAlpha::new(1, 4, 2, 2, String::new()).with_options(options)),
    }
}

fn builtin_context(kind: &str, short: bool) -> MockContext {
    let mut history = if kind == "rsi" {
        divergence_history(
            vec![11., 11., 6., 4., 5., 3., 4.],
            vec![10., 10., 5., 3., 4., 2., 3.],
            vec![10.5, 10.5, 5.5, 3.5, 4.5, 2.5, 3.5],
            vec![
                None,
                None,
                Some(20.),
                Some(30.),
                Some(25.),
                Some(40.),
                Some(35.),
            ],
        )
    } else {
        df!("high" => [99.0,99.0,101.0], "low" => [98.0,98.0,99.0],
            "close" => [98.5,98.5,100.5], "ATRr_2" => [1.0,1.0,1.0],
            "EMA_2" => [100.0,100.0,100.0])
        .unwrap()
    };
    let open: Vec<_> = history
        .column("close")
        .unwrap()
        .f64()
        .unwrap()
        .into_no_null_iter()
        .map(|v| v - 0.2)
        .collect();
    history
        .with_column(Column::new("open".into(), open))
        .unwrap();
    if short {
        let high: Vec<_> = history
            .column("low")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .map(|v| 200.0 - v)
            .collect();
        let low: Vec<_> = history
            .column("high")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .map(|v| 200.0 - v)
            .collect();
        for name in ["close", "open", "EMA_2", "RSI_2"] {
            if let Ok(column) = history.column(name) {
                let values: Vec<_> = column
                    .f64()
                    .unwrap()
                    .iter()
                    .map(|v| {
                        v.map(|v| {
                            if name == "RSI_2" {
                                100.0 - v
                            } else {
                                200.0 - v
                            }
                        })
                    })
                    .collect();
                history
                    .with_column(Column::new(name.into(), values))
                    .unwrap();
            }
        }
        history
            .with_column(Column::new("high".into(), high))
            .unwrap();
        history.with_column(Column::new("low".into(), low)).unwrap();
    }
    let timestamps: Vec<i64> = (0..history.height() as i64).map(|n| n * 60_000).collect();
    history
        .with_column(Column::new("timestamp".into(), timestamps))
        .unwrap();
    MockContext::new("BTCUSD", history)
}

fn next_observed_bar(ctx: &mut MockContext) {
    let df = ctx.history.get_mut("BTCUSD").unwrap();
    let values: Vec<_> = df
        .column("timestamp")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .map(|v| v + 60_000)
        .collect();
    df.with_column(Column::new("timestamp".into(), values))
        .unwrap();
}

fn set_latest(ctx: &mut MockContext, name: &str, value: Option<f64>) {
    let df = ctx.history.get_mut("BTCUSD").unwrap();
    let mut values: Vec<_> = df.column(name).unwrap().f64().unwrap().iter().collect();
    *values.last_mut().unwrap() = value;
    df.with_column(Column::new(name.into(), values)).unwrap();
}

#[test]
fn all_builtins_generate_valid_long_and_short_brackets() {
    for kind in ["test", "ema", "rsi"] {
        for short in [false, true] {
            let mut ctx = builtin_context(kind, short);
            let mut model = builtin_model(kind, BuiltinAlphaOptions::default());
            let result = model.generate_insights(&mut ctx, "BTCUSD");
            let insight = result
                .insight
                .unwrap_or_else(|| panic!("{kind} short={short}: {:?}", result.message));
            assert_eq!(
                insight.side,
                if short {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                }
            );
            let entry = insight
                .limit_price
                .unwrap_or(options::numeric(&ctx.history["BTCUSD"], "close", 0).unwrap());
            assert!(options::valid_levels(
                &insight.side,
                entry,
                insight.stop_loss().unwrap(),
                insight.take_profit_levels().unwrap()[0]
            ));
        }
    }
}

#[test]
fn all_builtins_respect_direction_shortability_and_tradability() {
    for kind in ["test", "ema", "rsi"] {
        for short in [false, true] {
            let mut ctx = builtin_context(kind, short);
            let opts = BuiltinAlphaOptions {
                allow_long: short,
                allow_short: !short,
                ..Default::default()
            };
            assert!(
                builtin_model(kind, opts)
                    .generate_insights(&mut ctx, "BTCUSD")
                    .insight
                    .is_none()
            );
            ctx.universe.get_mut("BTCUSD").unwrap().tradable = false;
            assert!(
                builtin_model(kind, BuiltinAlphaOptions::default())
                    .generate_insights(&mut ctx, "BTCUSD")
                    .insight
                    .is_none()
            );
            if short {
                let asset = ctx.universe.get_mut("BTCUSD").unwrap();
                asset.tradable = true;
                asset.shortable = false;
                assert!(
                    builtin_model(kind, BuiltinAlphaOptions::default())
                        .generate_insights(&mut ctx, "BTCUSD")
                        .insight
                        .is_none()
                );
            }
        }
    }
}

#[test]
fn all_builtins_suppress_same_bar_and_honour_observed_bar_cooldown() {
    for kind in ["test", "ema", "rsi"] {
        let mut ctx = builtin_context(kind, false);
        let mut model = builtin_model(
            kind,
            BuiltinAlphaOptions {
                cooldown_bars: 3,
                ..Default::default()
            },
        );
        assert!(
            model
                .generate_insights(&mut ctx, "BTCUSD")
                .insight
                .is_some()
        );
        for _ in 0..4 {
            assert!(
                model
                    .generate_insights(&mut ctx, "BTCUSD")
                    .insight
                    .is_none()
            );
        }
        for _ in 0..2 {
            next_observed_bar(&mut ctx);
            for _ in 0..4 {
                assert!(
                    model
                        .generate_insights(&mut ctx, "BTCUSD")
                        .insight
                        .is_none()
                );
            }
        }
        next_observed_bar(&mut ctx);
        assert!(
            model
                .generate_insights(&mut ctx, "BTCUSD")
                .insight
                .is_some()
        );
        model.start(&mut ctx);
        assert!(
            model
                .generate_insights(&mut ctx, "BTCUSD")
                .insight
                .is_some()
        );
    }
}

#[test]
fn all_builtins_apply_active_family_root_cap_and_release_closed_slots() {
    for kind in ["test", "ema", "rsi"] {
        let mut ctx = builtin_context(kind, false);
        let mut model = builtin_model(
            kind,
            BuiltinAlphaOptions {
                max_active_insights: 1,
                ..Default::default()
            },
        );
        let root = model.generate_insights(&mut ctx, "BTCUSD").insight.unwrap();
        let id = root.insight_id;
        ctx.add_insight(root);
        next_observed_bar(&mut ctx);
        assert!(
            model
                .generate_insights(&mut ctx, "BTCUSD")
                .insight
                .is_none()
        );
        let mut finished = ctx.insights.take_for_update(&id).unwrap();
        finished.order_rejected("Fixture capacity released");
        ctx.insights.restore_after_update(finished, true);
        assert!(
            model
                .generate_insights(&mut ctx, "BTCUSD")
                .insight
                .is_some()
        );
    }
}

#[test]
fn active_limits_exclude_children_other_symbols_and_other_alpha_families() {
    for kind in ["test", "ema", "rsi"] {
        let mut ctx = builtin_context(kind, false);
        let mut model = builtin_model(
            kind,
            BuiltinAlphaOptions {
                max_active_insights: 1,
                ..Default::default()
            },
        );
        let root = model.generate_insights(&mut ctx, "BTCUSD").insight.unwrap();
        let family = model.name().to_string();
        let mut child = root.clone();
        child.parent_id = Some(root.insight_id);
        ctx.add_insight(child);
        let mut other_symbol = root.clone();
        other_symbol.insight_id = uuid::Uuid::new_v4();
        other_symbol.symbol = "ETHUSD".into();
        ctx.add_insight(other_symbol);
        let mut other_family = root.clone();
        other_family.insight_id = uuid::Uuid::new_v4();
        other_family.strategy_type = StrategyType::Custom("DifferentAlpha".into());
        ctx.add_insight(other_family);
        assert_eq!(options::active_roots(&ctx, "BTCUSD", &family), 0);
        next_observed_bar(&mut ctx);
        assert!(
            model
                .generate_insights(&mut ctx, "BTCUSD")
                .insight
                .is_some()
        );
    }
}

#[test]
fn default_gate_suppresses_same_bar_but_allows_the_next_bar() {
    for kind in ["test", "ema", "rsi"] {
        let mut ctx = builtin_context(kind, false);
        let mut model = builtin_model(kind, BuiltinAlphaOptions::default());
        assert!(
            model
                .generate_insights(&mut ctx, "BTCUSD")
                .insight
                .is_some()
        );
        assert!(
            model
                .generate_insights(&mut ctx, "BTCUSD")
                .insight
                .is_none()
        );
        next_observed_bar(&mut ctx);
        assert!(
            model
                .generate_insights(&mut ctx, "BTCUSD")
                .insight
                .is_some()
        );
    }
}

#[test]
fn confidence_modifier_is_trimmed_absolute_and_finite() {
    for kind in ["test", "ema", "rsi"] {
        for modifier in [-0.5, f64::NAN, f64::INFINITY, 0.0] {
            let mut ctx = builtin_context(kind, false);
            ctx.base_confidence = 1.0;
            let df = ctx.history.get_mut("BTCUSD").unwrap();
            df.with_column(Column::new("strength".into(), vec![modifier; df.height()]))
                .unwrap();
            let mut model: Box<dyn AlphaModel> = match kind {
                "test" => Box::new(TestEntry::new(2, false, 1, " strength ".into())),
                "ema" => Box::new(EmaPriceCrossover::new(2, 2, " strength ".into())),
                _ => Box::new(RsiDiverganceAlpha::new(1, 4, 2, 2, " strength ".into())),
            };
            let result = model.generate_insights(&mut ctx, "BTCUSD");
            if modifier == -0.5 {
                assert_eq!(result.insight.unwrap().confidence, 50);
            } else {
                assert!(result.insight.is_none());
            }
        }
    }
}

#[test]
fn all_builtins_support_custom_exit_multipliers_and_expiry() {
    for kind in ["test", "ema", "rsi"] {
        let mut ctx = builtin_context(kind, false);
        let mut model = builtin_model(
            kind,
            BuiltinAlphaOptions {
                stop_loss_atr: 0.5,
                take_profit_atr: 4.0,
                period_unfilled: 7,
                period_till_tp: 23,
                ..Default::default()
            },
        );
        let insight = model.generate_insights(&mut ctx, "BTCUSD").insight.unwrap();
        let entry = insight
            .limit_price
            .unwrap_or(options::numeric(&ctx.history["BTCUSD"], "close", 0).unwrap());
        assert_eq!(insight.stop_loss(), Some(entry - 0.5));
        let tp_base = if kind == "ema" { 101.0 } else { entry };
        assert_eq!(insight.take_profit_levels(), Some(vec![tp_base + 4.0]));
        assert_eq!(insight.period_unfilled, Some(7));
        assert_eq!(insight.period_till_tp, Some(23));
    }
}

#[test]
fn all_builtins_reject_non_finite_zero_and_null_market_inputs() {
    for kind in ["test", "ema", "rsi"] {
        for field in ["close", "ATRr_2"] {
            for value in [
                None,
                Some(0.0),
                Some(-1.0),
                Some(f64::NAN),
                Some(f64::INFINITY),
            ] {
                let mut ctx = builtin_context(kind, false);
                set_latest(&mut ctx, field, value);
                assert!(
                    builtin_model(kind, BuiltinAlphaOptions::default())
                        .generate_insights(&mut ctx, "BTCUSD")
                        .insight
                        .is_none(),
                    "{kind} {field} {value:?}"
                );
            }
        }
    }
}

#[test]
fn all_builtins_reject_invalid_or_zero_rounded_confidence() {
    for kind in ["test", "ema", "rsi"] {
        for confidence in [0.0, -1.0, 0.0001, f64::NAN, f64::INFINITY] {
            let mut ctx = builtin_context(kind, false);
            ctx.base_confidence = confidence;
            assert!(
                builtin_model(kind, BuiltinAlphaOptions::default())
                    .generate_insights(&mut ctx, "BTCUSD")
                    .insight
                    .is_none()
            );
        }
    }
}

#[test]
fn all_builtins_reject_invalid_exit_options_and_collapsed_rounded_stops() {
    for kind in ["test", "ema", "rsi"] {
        let mut ctx = builtin_context(kind, false);
        for value in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let result = builtin_model(
                kind,
                BuiltinAlphaOptions {
                    stop_loss_atr: value,
                    ..Default::default()
                },
            )
            .generate_insights(&mut ctx, "BTCUSD");
            assert!(!result.success && result.insight.is_none());
        }
        ctx.round_prices = true;
        set_latest(
            &mut ctx,
            "close",
            Some(if kind == "rsi" {
                3.1
            } else if kind == "test" {
                100.0
            } else {
                100.1
            }),
        );
        let mut model = builtin_model(
            kind,
            BuiltinAlphaOptions {
                stop_loss_atr: 0.0001,
                ..Default::default()
            },
        );
        assert!(
            model
                .generate_insights(&mut ctx, "BTCUSD")
                .insight
                .is_none()
        );
    }
}

#[test]
fn invalid_periods_do_not_register_indicators_or_overflow() {
    for value in [0, usize::MAX] {
        let mut models: Vec<Box<dyn AlphaModel>> = vec![
            Box::new(TestEntry::new(value, false, 1, String::new())),
            Box::new(EmaPriceCrossover::new(2, value, String::new())),
            Box::new(RsiDiverganceAlpha::new(value, 50, 2, 2, String::new())),
        ];
        for model in &mut models {
            let mut ctx = builtin_context("rsi", false);
            model.start(&mut ctx);
            assert!(ctx.registered.is_empty());
            let result = model.generate_insights(&mut ctx, "BTCUSD");
            assert!(!result.success && result.insight.is_none());
        }
    }
}

#[test]
fn test_entry_skips_flat_candles_and_retains_legacy_exit_defaults() {
    let mut ctx = builtin_context("test", false);
    assert!(
        TestEntry::new(2, false, 0, String::new())
            .generate_insights(&mut ctx, "BTCUSD")
            .insight
            .is_none()
    );
    let mut model = TestEntry::new(2, false, 2, String::new());
    let insight = model.generate_insights(&mut ctx, "BTCUSD").insight.unwrap();
    assert_eq!(insight.take_profit_levels(), Some(vec![102.5]));
    assert_eq!(insight.period_unfilled, Some(5));
    assert_eq!(insight.period_till_tp, Some(10));
    next_observed_bar(&mut ctx);
    set_latest(&mut ctx, "open", Some(100.5));
    assert!(
        model
            .generate_insights(&mut ctx, "BTCUSD")
            .insight
            .is_none()
    );
}

#[test]
fn ema_distance_filter_is_configurable_and_warmup_preserves_other_models() {
    let mut ctx = builtin_context("ema", false);
    assert!(
        EmaPriceCrossover::new(2, 2, String::new())
            .with_signal_options(0.4, 1.0)
            .generate_insights(&mut ctx, "BTCUSD")
            .insight
            .is_none()
    );
    assert!(
        EmaPriceCrossover::new(2, 2, String::new())
            .with_signal_options(0.6, 1.0)
            .generate_insights(&mut ctx, "BTCUSD")
            .insight
            .is_some()
    );
    ctx.warm_up_bars = 500;
    for kind in ["test", "ema", "rsi"] {
        builtin_model(kind, BuiltinAlphaOptions::default()).start(&mut ctx);
        assert_eq!(ctx.warm_up_bars, 500);
    }
}

#[test]
fn rsi_minimum_change_and_confirmation_are_configurable() {
    let mut ctx = builtin_context("rsi", false);
    assert!(
        RsiDiverganceAlpha::new(1, 4, 2, 2, String::new())
            .with_divergence_options(11.0, true)
            .generate_insights(&mut ctx, "BTCUSD")
            .insight
            .is_none()
    );
    let insight = RsiDiverganceAlpha::new(1, 4, 2, 2, String::new())
        .with_divergence_options(10.0, false)
        .generate_insights(&mut ctx, "BTCUSD")
        .insight
        .unwrap();
    assert!(matches!(
        insight.execution_depends.as_slice(),
        [StrategyDependentConfirmation::None]
    ));
}

#[test]
fn rsi_null_price_rows_do_not_shift_indicator_alignment() {
    let mut ctx = builtin_context("rsi", false);
    let df = ctx.history.get_mut("BTCUSD").unwrap();
    let mut lows: Vec<_> = df.column("low").unwrap().f64().unwrap().iter().collect();
    lows[3] = None; // previous pivot is invalid, not a removable row
    df.with_column(Column::new("low".into(), lows)).unwrap();
    assert!(
        builtin_model("rsi", BuiltinAlphaOptions::default())
            .generate_insights(&mut ctx, "BTCUSD")
            .insight
            .is_none()
    );
}

#[test]
fn rsi_requires_confirmed_latest_pivot_and_valid_pivot_rsi() {
    let mut ctx = builtin_context("rsi", false);
    set_latest(&mut ctx, "low", Some(1.0)); // invalidates latest pivot confirmation
    assert!(
        builtin_model("rsi", BuiltinAlphaOptions::default())
            .generate_insights(&mut ctx, "BTCUSD")
            .insight
            .is_none()
    );
    for value in [None, Some(-1.0), Some(101.0), Some(f64::NAN)] {
        let mut ctx = builtin_context("rsi", false);
        let df = ctx.history.get_mut("BTCUSD").unwrap();
        let mut rsi: Vec<_> = df.column("RSI_2").unwrap().f64().unwrap().iter().collect();
        rsi[5] = value;
        df.with_column(Column::new("RSI_2".into(), rsi)).unwrap();
        assert!(
            builtin_model("rsi", BuiltinAlphaOptions::default())
                .generate_insights(&mut ctx, "BTCUSD")
                .insight
                .is_none()
        );
    }
}

fn reference_divergence(
    prices: &[f64],
    rsi: &[f64],
    width: usize,
    distance: usize,
    low: bool,
) -> bool {
    let pivots: Vec<_> = prices
        .windows(width * 2 + 1)
        .enumerate()
        .filter_map(|(index, window)| {
            let center = window[width];
            window
                .iter()
                .enumerate()
                .all(|(offset, v)| offset == width || if low { center < *v } else { center > *v })
                .then_some(index + width)
        })
        .collect();
    if pivots.len() < 2 {
        return false;
    }
    let last = pivots[pivots.len() - 1];
    let previous = pivots[pivots.len() - 2];
    last == prices.len() - width - 1
        && last - previous <= distance
        && if low {
            prices[last] < prices[previous] && rsi[last] > rsi[previous]
        } else {
            prices[last] > prices[previous] && rsi[last] < rsi[previous]
        }
}

#[test]
fn bounded_rsi_matches_full_scan_on_finite_data_and_ignores_old_prefix() {
    let mut random = 1234567u64;
    for width in [1, 2, 4, 12] {
        for distance in [width + 1, 25, 80] {
            for _ in 0..30 {
                let mut prices = Vec::new();
                let mut rsis = Vec::new();
                for _ in 0..300 {
                    random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                    prices.push(100.0 + (random >> 32) as f64 / u32::MAX as f64);
                    random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                    rsis.push((random >> 32) as f64 / u32::MAX as f64 * 100.0);
                }
                let p = Series::new("p".into(), &prices);
                let r = Series::new("r".into(), &rsis);
                for low in [false, true] {
                    assert_eq!(
                        RsiDiverganceAlpha::diverges(
                            p.f64().unwrap(),
                            r.f64().unwrap(),
                            width,
                            distance,
                            0.0,
                            low
                        ),
                        reference_divergence(&prices, &rsis, width, distance, low)
                    );
                }
            }
        }
    }
    let ctx = builtin_context("rsi", false);
    let suffix = &ctx.history["BTCUSD"];
    let mut prices = vec![f64::NAN; 100_000];
    prices.extend(
        suffix
            .column("low")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter(),
    );
    let mut rsis = vec![None; 100_000];
    rsis.extend(suffix.column("RSI_2").unwrap().f64().unwrap().iter());
    let p = Series::new("p".into(), prices);
    let r = Series::new("r".into(), rsis);
    assert!(RsiDiverganceAlpha::diverges(
        p.f64().unwrap(),
        r.f64().unwrap(),
        1,
        4,
        0.0,
        true
    ));
}

#[test]
#[ignore = "Manual offline microbenchmark; no wall-clock assertion"]
fn benchmark_bounded_rsi_against_full_history_scan() {
    use std::{hint::black_box, time::Instant};
    for size in [2_000usize, 100_000] {
        let mut prices = vec![10.0; size];
        let mut rsis = vec![50.0; size];
        prices[size - 5..].copy_from_slice(&[5., 3., 4., 2., 3.]);
        rsis[size - 5..].copy_from_slice(&[20., 30., 25., 40., 35.]);
        let p = Series::new("p".into(), &prices);
        let r = Series::new("r".into(), &rsis);
        let start = Instant::now();
        for _ in 0..200 {
            black_box(reference_divergence(
                black_box(&prices),
                black_box(&rsis),
                1,
                50,
                true,
            ));
        }
        let reference = start.elapsed();
        let start = Instant::now();
        for _ in 0..200 {
            black_box(RsiDiverganceAlpha::diverges(
                black_box(p.f64().unwrap()),
                black_box(r.f64().unwrap()),
                1,
                50,
                0.0,
                true,
            ));
        }
        eprintln!(
            "RSI {size} rows, 200 calls: full scan {reference:?}, bounded {:?}",
            start.elapsed()
        );
    }
}
