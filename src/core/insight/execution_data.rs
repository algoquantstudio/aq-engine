//! Small custom execution state, journaled with the owning insight and receipts.
//! This is not a cloud payload, history store, or arbitrary strategy variable dump.
use super::Insight;
use crate::core::broker::commands::BrokerCommandError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, io::Write};
use uuid::Uuid;

pub(super) const RECOVERY_DEPENDENCIES: &str = "aqe:recovery-dependencies:v1";
const MAX_RECOVERY_DEPENDENCIES: usize = 16;

const MAX_BYTES: usize = 16 * 1024;
const MAX_KEYS: usize = 16;
const MAX_KEY_BYTES: usize = 128;

#[derive(Clone, Debug, Default, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ExecutionData(BTreeMap<String, String>);

fn invalid(message: impl Into<String>) -> BrokerCommandError {
    BrokerCommandError::Invalid(message.into())
}

impl ExecutionData {
    pub(super) fn read(&self, key: &str) -> Result<Option<Value>, BrokerCommandError> {
        self.0
            .get(key)
            .map(|encoded| serde_json::from_str(encoded).map_err(|e| invalid(e.to_string())))
            .transpose()
    }

    pub(super) fn has_recovery_dependencies(&self) -> bool {
        self.0.contains_key(RECOVERY_DEPENDENCIES)
    }

    pub(crate) fn recovery_dependencies(&self) -> Result<Vec<Uuid>, BrokerCommandError> {
        self.0
            .get(RECOVERY_DEPENDENCIES)
            .map(|value| decode_dependencies(value))
            .transpose()
            .map(Option::unwrap_or_default)
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn validate(&self) -> Result<(), BrokerCommandError> {
        if self.0.len() > MAX_KEYS
            || self.0.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() > MAX_BYTES
        {
            return Err(invalid(
                "custom execution data exceeds its per-insight bound",
            ));
        }
        for (key, encoded) in &self.0 {
            validate_key(key)?;
            serde_json::from_str::<Value>(encoded).map_err(|e| invalid(e.to_string()))?;
        }
        self.recovery_dependencies()?;
        Ok(())
    }
}

fn decode_dependencies(encoded: &str) -> Result<Vec<Uuid>, BrokerCommandError> {
    let ids: Vec<Uuid> = serde_json::from_str(encoded).map_err(|e| invalid(e.to_string()))?;
    if ids.len() > MAX_RECOVERY_DEPENDENCIES || ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid(
            "recovery dependencies must be at most 16 sorted unique UUIDs",
        ));
    }
    Ok(ids)
}

fn validate_key(key: &str) -> Result<(), BrokerCommandError> {
    if key.trim().is_empty() || key.len() > MAX_KEY_BYTES {
        Err(invalid(
            "custom execution data key must contain 1..=128 bytes",
        ))
    } else {
        Ok(())
    }
}

struct BoundedJson(Vec<u8>);
impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_BYTES.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other(
                "custom execution data exceeds 16 KiB",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Insight {
    /// Recover private component data. Use a namespaced, versioned key and keep
    /// only state needed to resume decisions, never market history or credentials.
    pub fn execution_data(&self, key: &str) -> Result<Option<Value>, BrokerCommandError> {
        self.execution_data.read(key)
    }

    /// Retain compact recovery views of these owners while this insight is
    /// active. Register before dependent execution is checkpointed/submitted.
    /// Maximum 16 owners; histories, child trees and broker operations are not
    /// retained. Recovery data remains private to AQE, not an AQS projection.
    pub fn set_recovery_dependencies(&mut self, owners: &[Uuid]) -> Result<(), BrokerCommandError> {
        if owners.len() > MAX_RECOVERY_DEPENDENCIES {
            return Err(invalid("at most 16 recovery dependencies are supported"));
        }
        let mut ids = owners.to_vec();
        ids.sort_unstable();
        ids.dedup();
        self.set_execution_data(RECOVERY_DEPENDENCIES, &serde_json::json!(ids))
    }

    /// Atomically replace one component's recovery state. At most 16 keys and
    /// 16 KiB of encoded keys/values belong to an insight. Errors change nothing.
    /// Save BEFORE enqueueing dependent broker work so intent checkpointing
    /// includes the calculation that produced the command.
    pub fn set_execution_data(
        &mut self,
        key: &str,
        value: &Value,
    ) -> Result<(), BrokerCommandError> {
        validate_key(key)?;
        let old = self.execution_data.0.get(key);
        if old.is_none() && self.execution_data.0.len() == MAX_KEYS {
            return Err(invalid("custom execution data exceeds 16 component keys"));
        }
        let mut encoded = BoundedJson(Vec::new());
        serde_json::to_writer(&mut encoded, value).map_err(|e| invalid(e.to_string()))?;
        if key == RECOVERY_DEPENDENCIES {
            decode_dependencies(std::str::from_utf8(&encoded.0).expect("JSON is UTF-8"))?;
        }
        let retained = self
            .execution_data
            .0
            .iter()
            .filter(|(k, _)| k.as_str() != key)
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>();
        if retained + key.len() + encoded.0.len() > MAX_BYTES {
            return Err(invalid("custom execution data exceeds 16 KiB"));
        }
        self.execution_data.0.insert(
            key.into(),
            String::from_utf8(encoded.0).expect("JSON is UTF-8"),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::execution_checkpoint::InsightExecutionCheckpoint;
    use super::*;

    #[test]
    fn execution_data_is_atomic_bounded_and_changes_checkpoint_fingerprint() {
        let mut row = Insight::default();
        let before = row.snapshot_fingerprint_hash();
        row.set_execution_data("trail:v1", &serde_json::json!({"step":3,"target":96.0}))
            .unwrap();
        assert_ne!(before, row.snapshot_fingerprint_hash());
        let saved = row.snapshot_fingerprint_hash();
        assert!(
            row.set_execution_data("trail:v1", &Value::String("x".repeat(MAX_BYTES)))
                .is_err()
        );
        assert_eq!(saved, row.snapshot_fingerprint_hash());
        assert_eq!(row.execution_data("trail:v1").unwrap().unwrap()["step"], 3);
        for i in 1..MAX_KEYS {
            row.set_execution_data(&format!("key:{i}"), &Value::Null)
                .unwrap();
        }
        assert!(row.set_execution_data("extra", &Value::Null).is_err());
        row.set_execution_data("trail:v1", &Value::Null).unwrap();
    }

    #[test]
    fn execution_data_restores_exactly_and_keeps_legacy_empty_checkpoints_readable() {
        let mut row = Insight::default();
        row.symbol = "ETHUSD".into();
        row.set_execution_data(
            "trail:v1",
            &serde_json::json!({"pending":{"target":96.0,"step":3}}),
        )
        .unwrap();
        let checkpoint = InsightExecutionCheckpoint::capture(&row, 1, true, None);
        let encoded = serde_json::to_value(&checkpoint).unwrap();
        assert_eq!(encoded["version"], 2);
        let decoded: InsightExecutionCheckpoint = serde_json::from_value(encoded.clone()).unwrap();
        decoded.validate().unwrap();
        let (restored, _, _, _) = decoded.into_parts();
        assert_eq!(
            row.execution_data("trail:v1").unwrap(),
            restored.execution_data("trail:v1").unwrap()
        );
        let mut legacy = encoded;
        legacy["version"] = Value::from(1);
        assert!(
            serde_json::from_value::<InsightExecutionCheckpoint>(legacy.clone())
                .unwrap()
                .validate()
                .is_err()
        );
        legacy.as_object_mut().unwrap().remove("execution_data");
        serde_json::from_value::<InsightExecutionCheckpoint>(legacy)
            .unwrap()
            .validate()
            .unwrap();
    }
}
