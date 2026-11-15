//! Bound transient SQL parameter storage by count and payload bytes.
use super::*;

const MAX_BATCH_BYTES: usize = 1024 * 1024;

pub(super) struct InsertBatch {
    prefix: &'static str,
    columns: usize,
    rows: Vec<Vec<Value>>,
    bytes: usize,
    // Keep only the current shape. Normal full batches reuse the VM instead of
    // reparsing hundreds of placeholders for every flush; tails replace it.
    prepared: Option<(usize, turso::Statement)>,
    #[cfg(test)]
    flushed: Vec<(usize, usize, usize)>,
    #[cfg(test)]
    preparations: usize,
}

fn row_bytes(row: &[Value]) -> usize {
    row.iter().fold(0usize, |bytes, value| {
        bytes
            .saturating_add(std::mem::size_of::<Value>())
            .saturating_add(match value {
                Value::Text(text) => text.len(),
                Value::Blob(blob) => blob.len(),
                _ => 0,
            })
    })
}

impl InsertBatch {
    pub(super) fn new(prefix: &'static str, columns: usize) -> Self {
        assert!(columns > 0 && columns <= MAX_BATCH_PARAMETERS);
        Self {
            prefix,
            columns,
            rows: Vec::with_capacity(MAX_BATCH_PARAMETERS / columns),
            bytes: 0,
            prepared: None,
            #[cfg(test)]
            flushed: Vec::new(),
            #[cfg(test)]
            preparations: 0,
        }
    }

    pub(super) async fn push(
        &mut self,
        tx: &Transaction<'_>,
        row: Vec<Value>,
    ) -> Result<(), String> {
        if row.len() != self.columns {
            return Err("Backtest insert row has an invalid column count".into());
        }
        let bytes = row_bytes(&row);
        if !self.rows.is_empty()
            && ((self.rows.len() + 1) * self.columns > MAX_BATCH_PARAMETERS
                || self.bytes.saturating_add(bytes) > MAX_BATCH_BYTES)
        {
            self.flush(tx).await?;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.rows.push(row);
        // Large pre-existing histories remain legal. An oversized individual row
        // is written alone rather than truncated, rejected, or combined with others.
        if self.bytes >= MAX_BATCH_BYTES
            || (self.rows.len() + 1) * self.columns > MAX_BATCH_PARAMETERS
        {
            self.flush(tx).await?;
        }
        Ok(())
    }

    pub(super) async fn flush(&mut self, tx: &Transaction<'_>) -> Result<(), String> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let row_count = self.rows.len();
        if self
            .prepared
            .as_ref()
            .is_none_or(|(rows, _)| *rows != row_count)
        {
            let placeholders = format!("({})", vec!["?"; self.columns].join(", "));
            let sql = format!(
                "{} {}",
                self.prefix,
                vec![placeholders; row_count].join(", ")
            );
            self.prepared = Some((row_count, tx.prepare(&sql).await.map_err(to_storage_err)?));
            #[cfg(test)]
            {
                self.preparations += 1;
            }
        }
        #[cfg(test)]
        {
            self.flushed
                .push((row_count, row_count * self.columns, self.bytes));
        }
        let values = self.rows.drain(..).flatten().collect::<Vec<_>>();
        self.prepared
            .as_mut()
            .expect("prepared batch")
            .1
            .execute(values)
            .await
            .map_err(to_storage_err)?;
        self.bytes = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn connection() -> Connection {
        let db = Builder::new_local(":memory:").build().await.unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, payload TEXT NOT NULL CHECK(payload != 'reject'))", ()).await.unwrap();
        conn
    }

    #[tokio::test]
    async fn bounded_insert_limits_parameters_bytes_and_keeps_oversized_rows() {
        let mut conn = connection().await;
        let tx = conn.transaction().await.unwrap();
        let mut batch = InsertBatch::new("INSERT INTO batch_test (id, payload) VALUES", 2);
        for id in 0..1001 {
            batch
                .push(
                    &tx,
                    vec![Value::Integer(id), Value::Text(format!("row-{id}"))],
                )
                .await
                .unwrap();
        }
        assert_eq!(batch.flushed.len(), 2);
        assert_eq!(
            batch.preparations, 1,
            "full batches must reuse one prepared statement"
        );
        for (id, size) in [
            (1001, MAX_BATCH_BYTES / 2 + 1),
            (1002, MAX_BATCH_BYTES / 2 + 1),
            (1003, MAX_BATCH_BYTES * 2),
        ] {
            batch
                .push(&tx, vec![Value::Integer(id), Value::Text("x".repeat(size))])
                .await
                .unwrap();
        }
        batch.flush(&tx).await.unwrap();
        assert!(batch.flushed.len() > 3);
        assert!(
            batch
                .flushed
                .iter()
                .all(
                    |&(rows, parameters, bytes)| parameters <= MAX_BATCH_PARAMETERS
                        && (bytes <= MAX_BATCH_BYTES || rows == 1)
                )
        );
        tx.commit().await.unwrap();
        let mut rows = conn
            .query("SELECT COUNT(*), MAX(LENGTH(payload)) FROM batch_test", ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<i64>(0).unwrap(), 1004);
        assert_eq!(row.get::<i64>(1).unwrap(), (MAX_BATCH_BYTES * 2) as i64);
    }

    #[tokio::test]
    async fn bounded_insert_failure_rolls_back_prior_flushed_batches() {
        let mut conn = connection().await;
        conn.execute("INSERT INTO batch_test VALUES (-1, 'retained')", ())
            .await
            .unwrap();
        {
            let tx = conn.transaction().await.unwrap();
            let mut batch = InsertBatch::new("INSERT INTO batch_test (id, payload) VALUES", 2);
            for id in 0..901 {
                batch
                    .push(
                        &tx,
                        vec![Value::Integer(id), Value::Text("accepted".into())],
                    )
                    .await
                    .unwrap();
            }
            batch
                .push(&tx, vec![Value::Integer(902), Value::Text("reject".into())])
                .await
                .unwrap();
            assert!(batch.flush(&tx).await.is_err());
            // The caller's early error drops the same owning transaction.
        }
        let mut rows = conn
            .query("SELECT id, payload FROM batch_test", ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<i64>(0).unwrap(), -1);
        assert_eq!(row.get::<String>(1).unwrap(), "retained");
        assert!(rows.next().await.unwrap().is_none());
    }
}
