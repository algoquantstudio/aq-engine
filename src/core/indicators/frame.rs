use polars::prelude::{Column, DataFrame, PolarsResult};

/// Keep Polars' indexed lookup on the native strategy path. A notebook can pass a
/// DataFrame between separately linked cell libraries, whose foldhash global
/// seeds differ; its cached schema can then miss a column that is still present.
/// The fallback borrows the actual column without copying any price buffers.
#[inline]
pub(super) fn column<'a>(df: &'a DataFrame, name: &str) -> PolarsResult<&'a Column> {
    df.column(name).or_else(|error| {
        df.columns()
            .iter()
            .find(|column| column.name().as_str() == name)
            .ok_or(error)
    })
}

/// Indicator outputs always match the frame height. Resolve replacements against
/// actual columns too, so a foreign schema cache cannot append a duplicate output
/// on rerun. Polars' checked positional APIs invalidate that cache on mutation.
#[inline]
pub(super) fn upsert_column(df: &mut DataFrame, column: Column) -> PolarsResult<()> {
    let index = df.get_column_index(column.name()).or_else(|| {
        df.columns()
            .iter()
            .position(|existing| existing.name() == column.name())
    });
    if let Some(index) = index {
        df.replace_column(index, column)?;
    } else {
        df.insert_column(df.width(), column)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn reads_cached_columns_and_preserves_missing_column_errors() {
        let df = df!("high" => [2.0, 3.0], "low" => [0.0, 1.0]).unwrap();
        let _ = df.schema();
        assert_eq!(
            column(&df, "high").unwrap().f64().unwrap().get(1),
            Some(3.0)
        );
        assert!(matches!(
            column(&df, "missing"),
            Err(PolarsError::ColumnNotFound(_))
        ));
    }

    #[test]
    fn outputs_append_then_replace_without_reordering_inputs() {
        let mut df = df!("high" => [2.0, 3.0], "low" => [0.0, 1.0]).unwrap();
        let _ = df.schema();
        upsert_column(&mut df, Column::new("ATRr_14".into(), [1.0, 2.0])).unwrap();
        let _ = df.schema();
        upsert_column(&mut df, Column::new("ATRr_14".into(), [3.0, 4.0])).unwrap();
        assert_eq!(
            df.columns()
                .iter()
                .map(|c| c.name().as_str())
                .collect::<Vec<_>>(),
            ["high", "low", "ATRr_14"]
        );
        assert_eq!(df.height(), 2);
        assert_eq!(
            column(&df, "high").unwrap().f64().unwrap().get(0),
            Some(2.0)
        );
        assert_eq!(
            column(&df, "ATRr_14").unwrap().f64().unwrap().get(1),
            Some(4.0)
        );
    }

    #[test]
    fn rejects_mismatched_output_lengths_without_mutating_the_frame() {
        let mut df = df!("high" => [2.0, 3.0]).unwrap();
        assert!(upsert_column(&mut df, Column::new("high".into(), [0.0])).is_err());
        assert!(upsert_column(&mut df, Column::new("ATRr_14".into(), [0.0])).is_err());
        assert_eq!(df.shape(), (2, 1));
        assert_eq!(
            column(&df, "high").unwrap().f64().unwrap().get(0),
            Some(2.0)
        );
    }
}
