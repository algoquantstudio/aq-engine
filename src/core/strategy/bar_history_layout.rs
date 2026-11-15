//! Reusable, validated layouts for the append-only bar path.
use crate::core::indicators::Indicator;
use polars::prelude::*;
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct BarHistoryLayout {
    // One current layout/height, shared safely through Polars' immutable/COW columns.
    // No per-stream or per-height entries accumulate as streams change.
    null_columns: Vec<Column>,
    append_layout: Option<AppendLayout>,
    projection_buffer: Vec<Column>,
}

struct AppendLayout {
    history: Vec<(PlSmallStr, DataType)>,
    incoming: Vec<(PlSmallStr, DataType)>,
    indices: Vec<usize>,
    identity: bool,
}

fn schema_matches(frame: &DataFrame, schema: &[(PlSmallStr, DataType)]) -> bool {
    frame.width() == schema.len()
        && frame
            .columns()
            .iter()
            .zip(schema)
            .all(|(column, (name, dtype))| column.name() == name && column.dtype() == dtype)
}

fn schema(frame: &DataFrame) -> Vec<(PlSmallStr, DataType)> {
    frame
        .columns()
        .iter()
        .map(|column| (column.name().clone(), column.dtype().clone()))
        .collect()
}

impl BarHistoryLayout {
    pub(super) fn materialize_outputs(
        &mut self,
        indicators: &HashMap<String, Box<dyn Indicator>>,
        order: &[String],
        incoming: &mut DataFrame,
    ) {
        let mut index = 0;
        for name in order {
            let Some(indicator) = indicators.get(name) else {
                continue;
            };
            let owned;
            let outputs = match indicator.borrowed_output_columns() {
                Some(outputs) => outputs,
                None => {
                    owned = indicator.output_columns();
                    &owned
                }
            };
            for output in outputs {
                let height = incoming.height();
                if self
                    .null_columns
                    .get(index)
                    .is_none_or(|column| column.name().as_str() != output || column.len() != height)
                {
                    let column =
                        Series::full_null(output.as_str().into(), height, &DataType::Float64)
                            .into();
                    if index == self.null_columns.len() {
                        self.null_columns.push(column);
                    } else {
                        self.null_columns[index] = column;
                    }
                }
                // Always replace: failures and custom implementations must still observe
                // the same fresh nullable outputs as the uncached path.
                let _ = incoming.with_column(self.null_columns[index].clone());
                index += 1;
            }
        }
        self.null_columns.truncate(index);
    }

    pub(super) fn append(
        &mut self,
        history: &mut DataFrame,
        incoming: &DataFrame,
    ) -> PolarsResult<()> {
        if self.append_layout.as_ref().is_none_or(|layout| {
            !schema_matches(history, &layout.history) || !schema_matches(incoming, &layout.incoming)
        }) {
            // Resolve names only after a real schema change. Use Polars' checked
            // lookup so missing columns retain the original failure behavior.
            let indices = history
                .columns()
                .iter()
                .map(|column| incoming.try_get_column_index(column.name()))
                .collect::<PolarsResult<Vec<_>>>()?;
            let identity = incoming.width() == indices.len()
                && indices
                    .iter()
                    .enumerate()
                    .all(|(position, &index)| position == index);
            self.append_layout = Some(AppendLayout {
                history: schema(history),
                incoming: schema(incoming),
                indices,
                identity,
            });
        }
        let layout = self
            .append_layout
            .as_ref()
            .expect("validated append layout");
        if layout.identity {
            history.vstack_mut(incoming)?;
        } else {
            self.projection_buffer.extend(
                layout
                    .indices
                    .iter()
                    .map(|&index| incoming.columns()[index].clone()),
            );
            let aligned = DataFrame::new(
                incoming.height(),
                std::mem::take(&mut self.projection_buffer),
            )?;
            let result = history.vstack_mut(&aligned).map(|_| ());
            self.projection_buffer = aligned.into_columns();
            self.projection_buffer.clear();
            result?;
        }
        Ok(())
    }

    pub(super) fn compact_history_chunks(&self, history: &mut DataFrame) {
        // One-row appends retain one Arrow chunk per bar. Both tail() and the
        // indicators' continuity reads then walk those chunks, even with bounded
        // history. Rechunk occasionally, after trimming, rather than copying the
        // full window on every append. The square-root limit balances chunk walks
        // against amortized row copying for large windows; small windows allow 32
        // chunks before paying for a copy. Values, nulls and schema stay unchanged.
        let chunk_limit = history.height().isqrt().max(32);
        if history
            .columns()
            .iter()
            .any(|column| column.n_chunks() > chunk_limit)
        {
            // Sequential work avoids sending small retained frames to Rayon.
            // Polars' immutable buffers also preserve any retained frame clones.
            history.rechunk_mut();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    };

    struct CustomOutputs {
        outputs: Arc<RwLock<Vec<String>>>,
        reads: Arc<AtomicUsize>,
    }
    impl Indicator for CustomOutputs {
        fn name(&self) -> String {
            "custom".into()
        }
        fn input_columns(&self) -> Vec<String> {
            vec!["close".into()]
        }
        fn output_columns(&self) -> Vec<String> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.outputs.read().unwrap().clone()
        }
        fn window_size(&self) -> usize {
            1
        }
        fn run(&mut self, _: &mut DataFrame) -> Result<(), String> {
            Err("custom failure".into())
        }
        fn run_bar(&mut self, _: &DataFrame) -> Result<HashMap<String, f64>, String> {
            Err("custom failure".into())
        }
        fn last_run_time(&self) -> Option<chrono::DateTime<chrono::Utc>> {
            None
        }
        fn set_last_run_time(&mut self, _: chrono::DateTime<chrono::Utc>) {}
    }

    #[test]
    fn cached_outputs_follow_custom_names_height_replacement_and_failures() {
        let outputs = Arc::new(RwLock::new(vec!["first".into(), "second".into()]));
        let reads = Arc::new(AtomicUsize::new(0));
        let mut indicators: HashMap<String, Box<dyn Indicator>> = HashMap::from([(
            "custom".into(),
            Box::new(CustomOutputs {
                outputs: outputs.clone(),
                reads: reads.clone(),
            }) as Box<dyn Indicator>,
        )]);
        let mut layout = BarHistoryLayout::default();
        let order = vec!["custom".into()];
        for iteration in 0..2 {
            let mut incoming = df!("close" => [1.0], "first" => [99.0]).unwrap();
            layout.materialize_outputs(&indicators, &order, &mut incoming);
            assert_eq!(incoming.column("first").unwrap().null_count(), 1);
            assert_eq!(incoming.column("second").unwrap().null_count(), 1);
            let indicator = indicators.get_mut("custom").unwrap();
            assert!(
                indicator
                    .update_stream("symbol/timeframe", &DataFrame::default(), &mut incoming)
                    .is_err()
            );
            assert_eq!(incoming.column("first").unwrap().null_count(), 1);
            // Mutating a reused column's consumer cannot contaminate the null template.
            incoming
                .with_column(Column::new("first".into(), [iteration as f64]))
                .unwrap();
        }
        assert_eq!(reads.load(Ordering::Relaxed), 4); // materialization + default update, per batch
        *outputs.write().unwrap() = vec!["changed".into()];
        let mut incoming = df!("close" => [2.0, 3.0]).unwrap();
        layout.materialize_outputs(&indicators, &order, &mut incoming);
        assert_eq!(incoming.column("changed").unwrap().null_count(), 2);
        assert!(incoming.column("first").is_err());
        assert_eq!(layout.null_columns.len(), 1);
        indicators.insert(
            "custom".into(),
            Box::new(CustomOutputs {
                outputs: Arc::new(RwLock::new(vec!["close".into()])),
                reads,
            }),
        );
        layout.materialize_outputs(&indicators, &order, &mut incoming);
        assert_eq!(incoming.column("close").unwrap().null_count(), 2);
        indicators.clear();
        layout.materialize_outputs(&indicators, &order, &mut incoming);
        assert!(layout.null_columns.is_empty());
    }

    fn original_append(history: &mut DataFrame, incoming: &DataFrame) -> PolarsResult<()> {
        let aligned = incoming.select(
            history
                .get_column_names()
                .into_iter()
                .map(|name| name.as_str()),
        )?;
        history.vstack_mut(&aligned).map(|_| ())
    }

    #[test]
    fn cached_append_matches_name_alignment_across_schema_and_dtype_changes() {
        let mut layout = BarHistoryLayout::default();
        for (mut history, batches) in [
            (
                df!("close" => [1.0], "signal" => [2.0]).unwrap(),
                vec![
                    df!("close" => [3.0, 4.0], "signal" => [5.0, 6.0]).unwrap(),
                    df!("signal" => [7.0], "close" => [8.0]).unwrap(),
                    df!("signal" => [9.0], "unused" => [0.0], "close" => [10.0]).unwrap(),
                    df!("signal" => [11.0], "unused" => [0.0], "close" => [12.0]).unwrap(),
                ],
            ),
            (
                df!("signal" => [1i64], "close" => [2i64]).unwrap(),
                vec![
                    df!("close" => [3i64], "signal" => [4i64]).unwrap(),
                    df!("close" => [5i64], "signal" => [6i64]).unwrap(),
                ],
            ),
        ] {
            let mut expected = history.clone();
            for incoming in batches {
                original_append(&mut expected, &incoming).unwrap();
                layout.append(&mut history, &incoming).unwrap();
                assert!(history.equals_missing(&expected));
            }
        }
    }

    #[test]
    fn cached_append_preserves_missing_and_wrong_dtype_failures() {
        let mut layout = BarHistoryLayout::default();
        for incoming in [
            df!("close" => [2.0]).unwrap(),
            df!("close" => [2.0], "signal" => [3i64]).unwrap(),
        ] {
            let mut history = df!("close" => [1.0], "signal" => [2.0]).unwrap();
            let mut expected = history.clone();
            let baseline = original_append(&mut expected, &incoming).unwrap_err();
            let actual = layout.append(&mut history, &incoming).unwrap_err();
            assert_eq!(
                std::mem::discriminant(&actual),
                std::mem::discriminant(&baseline)
            );
            // Polars can have appended earlier columns before a later dtype fails.
            assert_eq!(history.shape(), expected.shape());
            for (left, right) in history.columns().iter().zip(expected.columns()) {
                assert!(left.equals_missing(right));
            }
        }
    }

    #[test]
    fn history_compaction_bounds_chunks_preserves_custom_columns_and_snapshots() {
        let row = |index: i64| {
            df!(
                "timestamp" => [index],
                "close" => [index as f64 + 0.25],
                "custom_signal" => [(index % 3 != 0).then_some(index as f64 / 2.0)],
                "custom_label" => [if index % 2 == 0 { "even" } else { "odd" }]
            )
            .unwrap()
        };
        let mut layout = BarHistoryLayout::default();
        let mut history = row(0);
        let mut expected = history.clone();
        let mut snapshot = None;
        for index in 1..1100 {
            let mut incoming = row(index);
            // Custom output order may differ from retained physical column order.
            if index % 2 == 0 {
                incoming = incoming
                    .select(["custom_label", "custom_signal", "close", "timestamp"])
                    .unwrap();
            }
            original_append(&mut expected, &incoming).unwrap();
            expected = expected.tail(Some(512));
            layout.append(&mut history, &incoming).unwrap();
            history = history.tail(Some(512));
            if index == 32 {
                // Keep a frame sharing all pre-compaction buffers alive while the
                // owner rechunks, appends and evicts its oldest rows.
                assert!(
                    history
                        .columns()
                        .iter()
                        .all(|column| column.n_chunks() > 32)
                );
                snapshot = Some((history.clone(), expected.clone()));
            }
            layout.compact_history_chunks(&mut history);
            assert!(
                history
                    .columns()
                    .iter()
                    .all(|column| column.n_chunks() <= 32)
            );
            assert_eq!(schema(&history), schema(&expected));
            assert!(history.equals_missing(&expected));
        }
        let (snapshot, expected_snapshot) = snapshot.unwrap();
        assert!(snapshot.equals_missing(&expected_snapshot));
        assert_eq!(snapshot.height(), 33);
        assert_eq!(
            history.column("timestamp").unwrap().i64().unwrap().get(0),
            Some(588)
        );

        // A changed custom schema/dtype must still invalidate the append layout
        // correctly after a compaction; no chunk or column position is cached.
        let mut changed = df!("custom_signal" => [7i64], "close" => [8.0]).unwrap();
        let reordered = df!("close" => [9.0], "custom_signal" => [10i64]).unwrap();
        for _ in 0..40 {
            layout.append(&mut changed, &reordered).unwrap();
        }
        let changed_snapshot = changed.clone();
        layout.compact_history_chunks(&mut changed);
        assert!(changed.equals_missing(&changed_snapshot));
        assert_eq!(schema(&changed), schema(&changed_snapshot));
        assert!(
            changed
                .columns()
                .iter()
                .all(|column| column.n_chunks() == 1)
        );
    }

    #[test]
    fn history_compaction_amortizes_copying_larger_windows() {
        let layout = BarHistoryLayout::default();
        let mut history = df!("close" => (0..65_536).map(|value| value as f64).collect::<Vec<_>>())
            .unwrap();
        let incoming = df!("close" => [Some(65_536.0), None]).unwrap();
        for _ in 0..40 {
            history.vstack_mut(&incoming).unwrap();
        }
        let before = history.clone();
        layout.compact_history_chunks(&mut history);
        assert_eq!(history.columns()[0].n_chunks(), 41);
        assert!(history.equals_missing(&before));

        for _ in 40..256 {
            history.vstack_mut(&incoming).unwrap();
        }
        let before = history.clone();
        assert_eq!(history.columns()[0].n_chunks(), 257);
        layout.compact_history_chunks(&mut history);
        assert_eq!(history.columns()[0].n_chunks(), 1);
        assert!(history.equals_missing(&before));
        assert_eq!(history.columns()[0].null_count(), 256);
    }

    #[test]
    #[ignore = "manual bar layout performance probe"]
    fn bar_layout_performance_probe() {
        use crate::core::indicators::I::SimpleMovingAverage;
        use std::hint::black_box;
        use std::time::Instant;
        let indicators: HashMap<String, Box<dyn Indicator>> = (1..=16)
            .map(|period| {
                let indicator = SimpleMovingAverage::close(period);
                (indicator.name(), Box::new(indicator) as Box<dyn Indicator>)
            })
            .collect();
        let mut order: Vec<_> = indicators.keys().cloned().collect();
        order.sort();
        let initial = df!("close" => [1.0]).unwrap();
        let mut layout = BarHistoryLayout::default();
        let mut history = initial.clone();
        layout.materialize_outputs(&indicators, &order, &mut history);
        let baseline = |history: &mut DataFrame| {
            let mut incoming = initial.clone();
            for name in &order {
                for output in indicators[name].output_columns() {
                    incoming
                        .with_column(Series::full_null(output.into(), 1, &DataType::Float64).into())
                        .unwrap();
                }
            }
            original_append(history, &incoming).unwrap();
            *history = history.tail(Some(32));
        };
        let mut expected = history.clone();
        let mut before = Vec::new();
        let mut after = Vec::new();
        for _ in 0..40 {
            let start = Instant::now();
            for _ in 0..100 {
                baseline(black_box(&mut expected));
            }
            before.push(start.elapsed().as_nanos() / 100);
            let start = Instant::now();
            for _ in 0..100 {
                let mut incoming = initial.clone();
                layout.materialize_outputs(&indicators, &order, &mut incoming);
                layout.append(black_box(&mut history), &incoming).unwrap();
                history = history.tail(Some(32));
            }
            after.push(start.elapsed().as_nanos() / 100);
        }
        assert!(history.equals_missing(&expected));
        before.sort_unstable();
        after.sort_unstable();
        eprintln!(
            "bar_layout indicators=16 rows=1 history=32 baseline_p50_ns={} baseline_p95_ns={} cached_p50_ns={} cached_p95_ns={}",
            before[20], before[38], after[20], after[38]
        );
    }
}
