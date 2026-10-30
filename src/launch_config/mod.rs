//! A launch file contains credentials, not strategy source or arbitrary code.
//! Never derive Debug or include its contents in an error/log message.
use serde::{Deserialize, Serialize};
mod capture;
pub use capture::BOUNDED_METADATA_SUPPORT_MARKER;
pub use capture::{CloudCaptureConfig,CaptureOutboxConfig,CapturePartitionConfig,CAPTURE_SUPPORT_MARKER,SOURCE_OBSERVATION_SUPPORT_MARKER,JOURNAL_RECOVERY_SUPPORT_MARKER,SEPARATE_HISTORY_SUPPORT_MARKER,MIN_CAPTURE_OPERATIONAL_HEADROOM_BYTES};

#[cfg(not(target_arch = "wasm32"))]
mod private_file;
#[cfg(not(target_arch = "wasm32"))]
mod process;
#[cfg(not(target_arch = "wasm32"))]
pub use private_file::{protect_private_path, read_private, write_private};
#[cfg(not(target_arch = "wasm32"))]
pub use process::{from_process, setting};

pub const VERSION: u16 = 1;
pub const MAX_BYTES: usize = 64 * 1024;
pub const ARGUMENT: &str = "--launch-config";
/// AQS checks this marker without executing a possibly old trading binary.
pub const SUPPORT_MARKER: &str = "aqe-protected-launch-config-v1";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchConfig {
    pub version: u16,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud: Option<CloudConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mt5: Option<Mt5Config>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<DeliveryConfig>,
    #[serde(default)]
    pub fill_export: bool,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub capture:Option<CloudCaptureConfig>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudConfig {
    pub url: String,
    pub session_id: String,
    pub session_secret: String,
    pub strategy_id: String,
    pub user_id: String,
    pub node_id: String,
    #[serde(default = "default_access", skip_serializing_if = "is_default_access")]
    pub access_method: String,
}

fn default_access() -> String {
    "aqe_live".into()
}
fn is_default_access(value: &String) -> bool {
    value == "aqe_live"
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mt5Config {
    pub bind_addr: String,
    pub token: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DeliveryConfig {
    ManagedNats {
        runtime_id: String,
        url: String,
        token: String,
        outbox_path: String,
        account_coordinator_dir: String,
        split_insight_history: bool,
    },
    EmbeddedGateway {
        url: String,
        outbox_directory: String,
        account_coordinator_dir: String,
    },
}

fn bounded(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.contains('\0')
}

impl LaunchConfig {
    pub fn validate(&self) -> Result<(), String> {
        let invalid = || format!("{SUPPORT_MARKER}: invalid or unsupported launch configuration");
        if self.version != VERSION || uuid::Uuid::parse_str(&self.run_id).is_err() {
            return Err(invalid());
        }
        if let Some(cloud) = &self.cloud {
            if !bounded(&cloud.url, 4096)
                || !["ws://", "wss://", "http://", "https://"]
                    .iter()
                    .any(|p| cloud.url.starts_with(p))
                || uuid::Uuid::parse_str(&cloud.session_id).is_err()
                || !bounded(&cloud.session_secret, 4096)
                || !bounded(&cloud.strategy_id, 512)
                || !bounded(&cloud.user_id, 512)
                || !bounded(&cloud.node_id, 512)
                || !bounded(&cloud.access_method, 128)
                || self.delivery.is_none()
            {
                return Err(invalid());
            }
        } else if self.delivery.is_some() {
            return Err(invalid());
        }
        if let Some(capture)=self.capture {
            capture.validate()?;
            let cloud=self.cloud.as_ref().ok_or_else(invalid)?;
            if cloud.strategy_id.len()>128
                ||match &self.delivery {
                    Some(DeliveryConfig::EmbeddedGateway{..})=>capture.embedded_receiver.is_none(),
                    Some(DeliveryConfig::ManagedNats{..})=>capture.embedded_receiver.is_some(),
                    None=>true,
                } {
                return Err(invalid());
            }
        }
        if let Some(mt5) = &self.mt5 {
            if mt5.bind_addr.parse::<std::net::SocketAddr>().is_err() || !bounded(&mt5.token, 4096)
            {
                return Err(invalid());
            }
        }
        match &self.delivery {
            Some(DeliveryConfig::ManagedNats {
                runtime_id,
                url,
                token,
                outbox_path,
                account_coordinator_dir,
                ..
            }) => {
                if uuid::Uuid::parse_str(runtime_id).is_err()
                    || !bounded(url, 4096)
                    || !bounded(token, 4096)
                    || !bounded(outbox_path, 4096)
                    || !bounded(account_coordinator_dir, 4096)
                {
                    return Err(invalid());
                }
            }
            Some(DeliveryConfig::EmbeddedGateway {
                url,
                outbox_directory,
                account_coordinator_dir,
            }) => {
                if !bounded(url, 4096)
                    || !bounded(outbox_directory, 4096)
                    || !bounded(account_coordinator_dir, 4096)
                {
                    return Err(invalid());
                }
            }
            None => {}
        }
        Ok(())
    }

    /// Only known runtime settings are projected. Never mutate the process
    /// environment (unsafe once broker/runtime worker threads have started).
    pub fn setting(&self, name: &str) -> Option<String> {
        match name {
            "AQE_RUN_ID" => return Some(self.run_id.clone()),
            // Canonical Insight/history already exists. New launch files never
            // enable the obsolete duplicate state archive, including by inheritance.
            "AQE_EXECUTION_EVIDENCE_EXPORT" => return Some("false".into()),
            "AQE_EXECUTION_FILL_EXPORT" => return Some(self.fill_export.to_string()),
            _ => {}
        }
        if let Some(mt5) = &self.mt5 {
            match name {
                "AQE_MT5_BRIDGE_TOKEN" => return Some(mt5.token.clone()),
                "AQE_MT5_BRIDGE_BIND_ADDR" => return Some(mt5.bind_addr.clone()),
                _ => {}
            }
        }
        match &self.delivery {
            Some(DeliveryConfig::ManagedNats {
                runtime_id,
                url,
                token,
                outbox_path,
                account_coordinator_dir,
                split_insight_history,
            }) => match name {
                "AQE_EVENT_BUS" => Some("nats".into()),
                "AQE_NATS_URL" => Some(url.clone()),
                "AQE_NATS_TOKEN" => Some(token.clone()),
                "AQE_RUNTIME_ID" => Some(runtime_id.clone()),
                "AQE_OUTBOX_PATH" => Some(outbox_path.clone()),
                "AQE_ACCOUNT_COORDINATOR_DIR" => Some(account_coordinator_dir.clone()),
                "AQE_SPLIT_INSIGHT_HISTORY" => Some(split_insight_history.to_string()),
                _ => None,
            },
            Some(DeliveryConfig::EmbeddedGateway {
                account_coordinator_dir,
                ..
            }) => match name {
                "AQE_EVENT_BUS" => Some("in_process".into()),
                "AQE_ACCOUNT_COORDINATOR_DIR" => Some(account_coordinator_dir.clone()),
                _ => None,
            },
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;
