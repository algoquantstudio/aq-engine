//! Current-format admission only. Never alter or upgrade an existing older file.
use turso::Connection;

pub(crate) async fn require_current(
    connection: &Connection,
    label: &str,
    application_id: i64,
    version: i64,
) -> Result<(), String> {
    async fn scalar(connection: &Connection, sql: &str) -> Result<i64, String> {
        let mut rows = connection.query(sql, ()).await.map_err(|e| e.to_string())?;
        rows.next()
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing database format marker")?
            .get::<i64>(0)
            .map_err(|e| e.to_string())
    }
    let saved_application = scalar(connection, "PRAGMA application_id").await?;
    let saved_version = scalar(connection, "PRAGMA user_version").await?;
    let mut tables = connection
        .query(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1",
            (),
        )
        .await
        .map_err(|e| e.to_string())?;
    let populated = tables.next().await.map_err(|e| e.to_string())?.is_some();
    if !populated && saved_application == 0 && saved_version == 0 {
        return Ok(());
    }
    if saved_application != application_id || saved_version != version {
        return Err(format!(
            "unsupported {label} saved-data format (application={saved_application}, version={saved_version}); current format requires application={application_id}, version={version}. Automatic legacy upgrades were removed; existing data has not been reset"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{broker::command_journal::BrokerCommandJournal, live_sync::OutboxStore};
    #[tokio::test]
    async fn old_files_are_refused_without_upgrading_or_deleting_their_rows() {
        for (label, application, version) in [
            ("broker journal", 1095846482, 2),
            ("live outbox", 1095847746, 1),
        ] {
            let root =
                std::env::temp_dir().join(format!("aqe-format-rejection-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            let path = root.join("old.db");
            let database = turso::Builder::new_local(path.to_string_lossy().as_ref())
                .build()
                .await
                .unwrap();
            let connection = database.connect().unwrap();
            connection.execute_batch("CREATE TABLE retained_evidence(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO retained_evidence VALUES(1,'do not delete');").await.unwrap();
            assert!(
                require_current(&connection, label, application, version)
                    .await
                    .unwrap_err()
                    .contains("unsupported")
            );
            if label == "broker journal" {
                assert!(
                    BrokerCommandJournal::open(&path, "strategy", "account")
                        .await
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("unsupported broker journal saved-data format")
                );
            } else {
                assert!(
                    OutboxStore::open(&path)
                        .await
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("unsupported live outbox saved-data format")
                );
            }
            let mut rows = connection
                .query("SELECT value FROM retained_evidence WHERE id=1", ())
                .await
                .unwrap();
            assert_eq!(
                rows.next()
                    .await
                    .unwrap()
                    .unwrap()
                    .get::<String>(0)
                    .unwrap(),
                "do not delete"
            );
            drop(rows);
            let mut rows = connection.query("PRAGMA user_version", ()).await.unwrap();
            assert_eq!(
                rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
                0
            );
            drop(rows);
            drop(connection);
            drop(database);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
