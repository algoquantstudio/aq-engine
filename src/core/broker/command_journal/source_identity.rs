//! One stable identity shared by exact fills, marks and capture-recovery receipts.
//! The historical table name is retained; it is not an Insight-state archive.
use super::*;

pub(super) const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS broker_execution_evidence_source (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        source_id TEXT NOT NULL
    );
";

pub(super) async fn install_source(connection: &Connection) -> Result<String, BrokerCommandError> {
    connection
        .execute(
            "INSERT INTO broker_execution_evidence_source(singleton,source_id)
         VALUES(1,?1) ON CONFLICT(singleton) DO NOTHING",
            params![uuid::Uuid::new_v4().to_string()],
        )
        .await
        .map_err(journal_error)?;
    let mut rows = connection
        .query(
            "SELECT source_id FROM broker_execution_evidence_source WHERE singleton=1",
            (),
        )
        .await
        .map_err(journal_error)?;
    let source: String = rows
        .next()
        .await
        .map_err(journal_error)?
        .ok_or_else(|| journal_error("missing execution evidence source identity"))?
        .get(0)
        .map_err(journal_error)?;
    let id = uuid::Uuid::parse_str(&source).map_err(journal_error)?;
    if id.is_nil() {
        return Err(journal_error("invalid execution evidence source identity"));
    }
    Ok(source)
}
