//! Private desktop-to-runtime audit evidence, never a broker command or event packet.
use super::RuntimeStrategyRegistration;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const REGISTRATION_AUDIT_DIRECTORY: &str = "registration-audits";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RegistrationAuditDecision {
    Present,
    Deleted {
        job_id: String,
        completed_at_us: i64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegistrationAudit {
    pub version: u16,
    pub fingerprint: Uuid,
    pub checked_at_us: i64,
    pub decision: RegistrationAuditDecision,
}

impl RuntimeStrategyRegistration {
    /// Bind all registration fields, including rotated credentials and destination,
    /// without putting those credentials in the diagnostic or its filename.
    pub fn audit_fingerprint(&self) -> Uuid {
        let bytes = serde_json::to_vec(self).expect("registration has only JSON-safe fields");
        Uuid::new_v5(&Uuid::NAMESPACE_OID, &bytes)
    }
}

impl RegistrationAudit {
    pub fn matches(&self, registration: &RuntimeStrategyRegistration, now_us: i64) -> bool {
        if self.version != 1
            || self.fingerprint != registration.audit_fingerprint()
            || self.checked_at_us <= 0
            || self.checked_at_us < registration.created_at_us
            || self.checked_at_us > now_us.saturating_add(5_000_000)
        {
            return false;
        }
        match &self.decision {
            RegistrationAuditDecision::Present => true,
            RegistrationAuditDecision::Deleted {
                job_id,
                completed_at_us,
            } => {
                let strategy_job = format!("strategy::{}", registration.strategy_id);
                let session_job = format!(
                    "live_session::{}::{}",
                    registration.strategy_id, registration.live_session_id
                );
                (job_id == &strategy_job || job_id == &session_job)
                    && *completed_at_us > 0
                    && *completed_at_us >= registration.created_at_us
                    && *completed_at_us <= self.checked_at_us
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn registration() -> RuntimeStrategyRegistration {
        serde_json::from_value(serde_json::json!({
            "protocolVersion": 2, "registrationGeneration": 1, "runtimeId": "runtime",
            "runId": "run", "strategyId": "strategy", "liveSessionId": "session",
            "processId": 42, "userId": "users:owner", "serverNodeId": "node",
            "batchUrl": "http://localhost/batch", "actionsUrl": "http://localhost/actions",
            "signinUrl": "http://localhost/signin", "namespace": "aqs", "database": "aqs",
            "accessMethod": "aqe_live", "bearerToken": "private-token",
            "sessionSecret": "private-secret", "createdAtUs": 10
        }))
        .unwrap()
    }

    #[test]
    fn evidence_is_bound_to_exact_registration_without_exposing_credentials() {
        let original = registration();
        let audit = RegistrationAudit {
            version: 1,
            fingerprint: original.audit_fingerprint(),
            checked_at_us: 30,
            decision: RegistrationAuditDecision::Deleted {
                job_id: "strategy::strategy".into(),
                completed_at_us: 20,
            },
        };
        assert!(audit.matches(&original, 40));
        let json = serde_json::to_string(&audit).unwrap();
        assert!(!json.contains("private-token") && !json.contains("private-secret"));
        for change in 0..6 {
            let mut next = original.clone();
            match change {
                0 => next.registration_generation += 1,
                1 => next.session_secret = Some("rotated".into()),
                2 => next.user_id = "users:another".into(),
                3 => next.batch_url = "http://other/batch".into(),
                4 => next.run_id = "new-run".into(),
                _ => next.process_id += 1,
            }
            assert!(!audit.matches(&next, 40));
        }
    }

    #[test]
    fn old_or_unrelated_deletion_jobs_are_not_authority() {
        let registration = registration();
        for (job_id, completed) in [
            ("strategy::other", 20),
            ("strategy::strategy", 9),
            ("strategy::strategy", 40),
        ] {
            let audit = RegistrationAudit {
                version: 1,
                fingerprint: registration.audit_fingerprint(),
                checked_at_us: 30,
                decision: RegistrationAuditDecision::Deleted {
                    job_id: job_id.into(),
                    completed_at_us: completed,
                },
            };
            assert!(!audit.matches(&registration, 40));
        }
    }
}
