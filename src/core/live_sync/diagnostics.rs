//! Secret-bearing configuration must never inherit a field-by-field Debug.
//! Omit configured URLs too: userinfo, query strings and paths can carry secrets.
use super::{HttpBatchSinkConfig, RuntimeHostMetadata, RuntimeStrategyRegistration};
use crate::core::{broker::mt5_bridge::Mt5BridgeConfig, strategy::AqsAuth};
use std::fmt;

impl fmt::Debug for AqsAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AqsAuth")
            .field("strategy_id", &self.strategy_id)
            .field("session_id", &self.session_id)
            .field("access_method", &self.access_method)
            .field("session_secret", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for Mt5BridgeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mt5BridgeConfig")
            .field("bind_addr", &self.bind_addr)
            .field("token", &"[REDACTED]")
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("poll_interval", &self.poll_interval)
            .field("symbol_count", &self.symbol_map.len())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for HttpBatchSinkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpBatchSinkConfig")
            .field("strategy_id", &self.strategy_id)
            .field("runtime_id", &self.runtime_id)
            .field("run_id", &self.run_id)
            .field("bearer_token", &"[REDACTED]")
            .field("has_session_secret", &self.session_secret.is_some())
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for RuntimeHostMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeHostMetadata")
            .field("protocol_version", &self.protocol_version)
            .field("host_version", &self.host_version)
            .field("runtime_id", &self.runtime_id)
            .field("pid", &self.pid)
            .field("nats_pid", &self.nats_pid)
            .field("nats_token", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for RuntimeStrategyRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeStrategyRegistration")
            .field("protocol_version", &self.protocol_version)
            .field("registration_generation", &self.registration_generation)
            .field("runtime_id", &self.runtime_id)
            .field("run_id", &self.run_id)
            .field("strategy_id", &self.strategy_id)
            .field("process_id", &self.process_id)
            .field("bearer_token", &"[REDACTED]")
            .field("has_session_secret", &self.session_secret.is_some())
            .field("log_source_count", &self.log_sources.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, path::PathBuf, time::Duration};
    #[test]
    fn diagnostics_redact_credentials_and_url_borne_secrets() {
        let credential = "unique-sensitive-credential";
        let endpoint =
            format!("https://user:{credential}@example.invalid/{credential}?token={credential}");
        let auth = AqsAuth {
            access_method: "aqe_live".into(),
            session_id: "session".into(),
            session_secret: credential.into(),
            strategy_id: "strategy".into(),
            user_id: "users:test".into(),
            node_id: None,
            live_session_id: None,
            url: Some(endpoint.clone()),
        };
        let bridge = Mt5BridgeConfig {
            bind_addr: "127.0.0.1:1".parse().unwrap(),
            token: credential.into(),
            request_timeout: Duration::from_secs(1),
            connect_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_secs(1),
            symbol_map: HashMap::new(),
        };
        let sink = HttpBatchSinkConfig {
            batch_url: endpoint.clone(),
            actions_url: Some(endpoint.clone()),
            bearer_token: credential.into(),
            session_secret: Some(credential.into()),
            signin_url: Some(endpoint.clone()),
            namespace: "aqs".into(),
            database: "aqs".into(),
            access_method: "aqe_live".into(),
            user_id: "users:test".into(),
            strategy_id: "strategy".into(),
            live_session_id: "session".into(),
            runtime_id: "runtime".into(),
            run_id: "run".into(),
            server_node_id: "local".into(),
            request_timeout: Duration::from_secs(1),
        };
        let metadata = RuntimeHostMetadata {
            protocol_version: 1,
            host_version: "test".into(),
            runtime_id: "runtime".into(),
            pid: 1,
            nats_pid: 2,
            nats_url: endpoint.clone(),
            nats_token: credential.into(),
            runtime_directory: PathBuf::from("/unused"),
            started_at_us: 1,
        };
        let registration = RuntimeStrategyRegistration {
            protocol_version: 1,
            registration_generation: 1,
            runtime_id: "runtime".into(),
            run_id: "run".into(),
            strategy_id: "strategy".into(),
            live_session_id: "session".into(),
            process_id: 1,
            user_id: "users:test".into(),
            server_node_id: "local".into(),
            batch_url: endpoint.clone(),
            event_gateway_url: Some(endpoint.clone()),
            actions_url: endpoint.clone(),
            signin_url: endpoint,
            namespace: "aqs".into(),
            database: "aqs".into(),
            access_method: "aqe_live".into(),
            bearer_token: credential.into(),
            session_secret: Some(credential.into()),
            log_sources: vec![],
            created_at_us: 1,
        };
        for formatted in [
            format!("{auth:?}"),
            format!("{bridge:?}"),
            format!("{sink:?}"),
            format!("{metadata:?}"),
            format!("{registration:?}"),
        ] {
            assert!(!formatted.contains(credential));
            assert!(formatted.contains("REDACTED"));
        }
        // Wire/private persistence still carries the credentials; only Debug is redacted.
        assert_eq!(
            serde_json::to_value(registration).unwrap()["bearerToken"],
            credential
        );
    }
}
