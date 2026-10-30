//! Gateway-only cloud delivery. The local publisher and strategy loop are unaffected by cloud
//! latency: their existing durable outbox transfers ownership only after JetStream acceptance.
use super::{
    HttpBatchSinkConfig, LiveSyncBatch, LiveSyncBatchResult, RemoteSyncError, RemoteSyncErrorKind,
    now_epoch_micros,
};
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig},
};

const RUN_RETIRED: &str = "hosted run already retired";
#[cfg(test)]
mod capture_tests;
#[cfg(test)]
mod protobuf_tests;

#[derive(Clone)]
pub struct GatewayConnection {
    url: String,
    config: HttpBatchSinkConfig,
    client: reqwest::Client,
    access: Arc<tokio::sync::Mutex<Option<Access>>>,
    control_access: Arc<tokio::sync::Mutex<Option<Access>>>,
    // Diagnostics never hold or invalidate the safety-token refresh lock.
    capture_access: Arc<tokio::sync::Mutex<Option<Access>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Access {
    token: String,
    expires_at_us: i64,
    #[serde(skip)]
    run_id: String,
}

impl GatewayConnection {
    /// Server-timed cloud connectivity is independent of broker activity.
    /// Control APIs never depend on this heartbeat succeeding.
    pub async fn heartbeat(&self) -> Result<(), RemoteSyncError> {
        for attempt in 0..2 {
            let token = self.token(&self.config.run_id).await?;
            let response = self
                .client
                .post(format!("{}/v1/heartbeat", self.url))
                .bearer_auth(token)
                .send()
                .await
                .map_err(|_| {
                    error(
                        RemoteSyncErrorKind::Transient,
                        "cloud heartbeat unavailable",
                    )
                })?;
            if response.status().as_u16() == 401 && attempt == 0 {
                *self.access.lock().await = None;
                continue;
            }
            successful(response).await?;
            return Ok(());
        }
        Err(error(
            RemoteSyncErrorKind::Authentication,
            "cloud heartbeat credential rejected",
        ))
    }

    /// A copied AQS launch has no execution route until its first authenticated
    /// admission. Establish that ownership before AQE connects the broker.
    pub(crate) async fn admit_prepared_run(&self) -> Result<(), RemoteSyncError> {
        self.token(&self.config.run_id).await.map(|_| ())
    }

    /// Only call once the producer has stopped and both publisher/consumer outboxes
    /// are drained. Acceptance seals ingestion; database drain continues on the gateway.
    pub async fn seal_drained_run(&self) -> Result<(), RemoteSyncError> {
        for attempt in 0..2 {
            let token = match self.token(&self.config.run_id).await {
                Ok(token) => token,
                Err(error)
                    if error.kind == RemoteSyncErrorKind::Terminal
                        && error.message == RUN_RETIRED =>
                {
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            let response = self
                .client
                .post(format!("{}/v1/runs/seal", self.url))
                .bearer_auth(token)
                .json(&serde_json::json!({"producerStopped":true,"outboxDrained":true}))
                .send()
                .await
                .map_err(|_| error(RemoteSyncErrorKind::Transient, "run retirement unavailable"))?;
            if response.status().as_u16() == 401 && attempt == 0 {
                *self.access.lock().await = None;
                continue;
            }
            successful(response).await?;
            return Ok(());
        }
        Err(error(
            RemoteSyncErrorKind::Authentication,
            "run retirement credential rejected",
        ))
    }

    /// Transfers immutable evidence to the hosted coordinator, or reads its current
    /// progress via the idempotent completion request. Never calls SurrealDB here.
    pub async fn reconcile(
        &self,
        operation: &str,
        body: &impl serde::Serialize,
    ) -> Result<serde_json::Value, RemoteSyncError> {
        if !["start", "chunk", "complete", "lifecycle"].contains(&operation) {
            return Err(error(
                RemoteSyncErrorKind::InvalidBatch,
                "invalid hosted reconciliation operation",
            ));
        }
        for attempt in 0..2 {
            let token = self.token(&self.config.run_id).await?;
            let response = self
                .client
                .post(format!("{}/v1/reconciliation/{operation}", self.url))
                .bearer_auth(token)
                .json(body)
                .send()
                .await
                .map_err(|_| {
                    error(
                        RemoteSyncErrorKind::Transient,
                        "hosted reconciliation unavailable",
                    )
                })?;
            if response.status().as_u16() == 401 && attempt == 0 {
                *self.access.lock().await = None;
                continue;
            }
            return successful(response).await?.json().await.map_err(|_| {
                error(
                    RemoteSyncErrorKind::Transient,
                    "invalid hosted reconciliation response",
                )
            });
        }
        Err(error(
            RemoteSyncErrorKind::Authentication,
            "hosted reconciliation credential rejected",
        ))
    }

    pub async fn connect_controls(
        &self,
    ) -> Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, RemoteSyncError> {
        let token = self.control_token().await?;
        let mut url = reqwest::Url::parse(&self.url)
            .map_err(|_| error(RemoteSyncErrorKind::Terminal, "invalid gateway URL"))?;
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme)
            .map_err(|_| error(RemoteSyncErrorKind::Terminal, "invalid gateway scheme"))?;
        url.set_path("/v1/controls/events");
        let mut request = url
            .as_str()
            .into_client_request()
            .map_err(|_| error(RemoteSyncErrorKind::Terminal, "invalid control request"))?;
        request.headers_mut().insert(
            "authorization",
            format!("Bearer {token}")
                .parse()
                .map_err(|_| error(RemoteSyncErrorKind::Terminal, "invalid control credential"))?,
        );
        let config = WebSocketConfig::default()
            .max_message_size(Some(16 * 1024))
            .max_frame_size(Some(16 * 1024));
        match tokio_tungstenite::connect_async_with_config(request, Some(config), false).await {
            Ok((socket, _)) => Ok(socket),
            Err(tokio_tungstenite::tungstenite::Error::Http(response))
                if response.status().as_u16() == 401 =>
            {
                *self.control_access.lock().await = None;
                Err(error(
                    RemoteSyncErrorKind::Authentication,
                    "hosted control credential expired",
                ))
            }
            Err(_) => Err(error(
                RemoteSyncErrorKind::Transient,
                "hosted control stream unavailable",
            )),
        }
    }

    pub async fn acknowledge_control(&self, action_id: &str) -> Result<(), RemoteSyncError> {
        let token = self.control_token().await?;
        let response = self
            .client
            .post(format!("{}/v1/controls/received", self.url))
            .bearer_auth(token)
            .json(&serde_json::json!({"actionId":action_id}))
            .send()
            .await
            .map_err(|_| {
                error(
                    RemoteSyncErrorKind::Transient,
                    "hosted control receipt unavailable",
                )
            })?;
        if response.status().as_u16() == 401 {
            *self.control_access.lock().await = None;
        }
        successful(response).await?;
        Ok(())
    }

    pub fn new(url: &str, config: HttpBatchSinkConfig) -> Result<Self, RemoteSyncError> {
        let parsed = super::gateway_endpoint(url)
            .map_err(|message| error(RemoteSyncErrorKind::Terminal, message))?;
        if config.session_secret.is_none() {
            return Err(error(
                RemoteSyncErrorKind::Authentication,
                "event gateway requires an AQS live-session secret",
            ));
        }
        Ok(Self {
            url: parsed.as_str().trim_end_matches('/').into(),
            config,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                // Registration POSTs contain a session secret. Redirects must
                // not move that body outside the configured gateway origin.
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| error(RemoteSyncErrorKind::Terminal, &e.to_string()))?,
            access: Arc::new(tokio::sync::Mutex::new(None)),
            control_access: Arc::new(tokio::sync::Mutex::new(None)),
            capture_access: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    async fn control_token(&self) -> Result<String, RemoteSyncError> {
        self.operational_token(&self.control_access).await
    }

    async fn operational_token(
        &self,
        cache: &tokio::sync::Mutex<Option<Access>>,
    ) -> Result<String, RemoteSyncError> {
        let mut access = cache.lock().await;
        if let Some(access) = access
            .as_ref()
            .filter(|a| a.expires_at_us > now_epoch_micros() + 30_000_000)
        {
            return Ok(access.token.clone());
        }
        let response=self.client.post(format!("{}/v1/safety/register",self.url)).json(&serde_json::json!({
            "strategyId":self.config.strategy_id,"liveSessionId":self.config.live_session_id,
            "runtimeId":self.config.runtime_id,"runId":self.config.run_id,"serverNodeId":self.config.server_node_id,
            "sessionSecret":self.config.session_secret,
        })).send().await.map_err(|_|error(RemoteSyncErrorKind::Transient,"safety registration unavailable"))?;
        let mut granted: Access = successful(response).await?.json().await.map_err(|_| {
            error(
                RemoteSyncErrorKind::Transient,
                "invalid safety registration response",
            )
        })?;
        granted.run_id = self.config.run_id.clone();
        let token = granted.token.clone();
        *access = Some(granted);
        Ok(token)
    }

    pub async fn publish_safety(
        &self,
        observation: &aqe_live_protocol::SafetyObservation,
    ) -> Result<bool, RemoteSyncError> {
        observation
            .validate()
            .map_err(|message| error(RemoteSyncErrorKind::InvalidBatch, message))?;
        for attempt in 0..2 {
            let token = self.control_token().await?;
            let response = self
                .client
                .post(format!("{}/v1/safety/observe", self.url))
                .bearer_auth(token)
                .json(observation)
                .send()
                .await
                .map_err(|_| {
                    error(
                        RemoteSyncErrorKind::Transient,
                        "safety projection unavailable",
                    )
                })?;
            if matches!(response.status().as_u16(), 404 | 405) {
                return Ok(false);
            }
            if response.status().as_u16() == 401 && attempt == 0 {
                *self.control_access.lock().await = None;
                continue;
            }
            let value: serde_json::Value =
                successful(response).await?.json().await.map_err(|_| {
                    error(
                        RemoteSyncErrorKind::Transient,
                        "invalid safety projection response",
                    )
                })?;
            return if value["valid"] == true {
                Ok(true)
            } else {
                Err(error(
                    RemoteSyncErrorKind::Authentication,
                    "safety authority changed",
                ))
            };
        }
        Err(error(
            RemoteSyncErrorKind::Authentication,
            "safety credential rejected",
        ))
    }

    pub async fn publish_capture_gap(
        &self,
        notice: &aqe_live_protocol::CaptureGapNotice,
    ) -> Result<aqe_live_protocol::CaptureGapReceipt, RemoteSyncError> {
        notice
            .validate()
            .map_err(|m| error(RemoteSyncErrorKind::InvalidBatch, m))?;
        for attempt in 0..2 {
            let token = self.operational_token(&self.capture_access).await?;
            let response = self
                .client
                .post(format!("{}/v1/capture/gap", self.url))
                .bearer_auth(token)
                .json(notice)
                .send()
                .await
                .map_err(|_| {
                    error(
                        RemoteSyncErrorKind::Transient,
                        "capture gap delivery unavailable",
                    )
                })?;
            if matches!(response.status().as_u16(), 401 | 403) && attempt == 0 {
                *self.capture_access.lock().await = None;
                continue;
            }
            let value: serde_json::Value =
                successful(response).await?.json().await.map_err(|_| {
                    error(
                        RemoteSyncErrorKind::Transient,
                        "invalid capture gap receipt",
                    )
                })?;
            let receipt: aqe_live_protocol::CaptureGapReceipt =
                serde_json::from_value(value["receipt"].clone()).map_err(|_| {
                    error(
                        RemoteSyncErrorKind::Transient,
                        "invalid capture gap receipt",
                    )
                })?;
            if value["valid"] != true || !receipt.covers(notice) {
                return Err(error(
                    RemoteSyncErrorKind::Transient,
                    "capture gap authority or receipt changed",
                ));
            }
            return Ok(receipt);
        }
        Err(error(
            RemoteSyncErrorKind::Authentication,
            "capture gap credential rejected",
        ))
    }

    pub async fn safety_registered(&self) -> bool {
        self.control_access.lock().await.is_some()
    }

    pub async fn pending_stop_actions(
        &self,
    ) -> Result<Option<Vec<super::RemoteStrategyAction>>, RemoteSyncError> {
        for attempt in 0..2 {
            let token = self.control_token().await?;
            let response = self
                .client
                .get(format!("{}/v1/safety/actions", self.url))
                .bearer_auth(token)
                .send()
                .await
                .map_err(|_| {
                    error(
                        RemoteSyncErrorKind::Transient,
                        "safety action query unavailable",
                    )
                })?;
            if matches!(response.status().as_u16(), 404 | 405) {
                return Ok(None);
            }
            if response.status().as_u16() == 401 && attempt == 0 {
                *self.control_access.lock().await = None;
                continue;
            }
            let value: serde_json::Value =
                successful(response).await?.json().await.map_err(|_| {
                    error(
                        RemoteSyncErrorKind::Transient,
                        "invalid safety action response",
                    )
                })?;
            if value["valid"] != true {
                return Err(error(
                    RemoteSyncErrorKind::Authentication,
                    "safety authority changed",
                ));
            }
            return serde_json::from_value(value["actions"].clone())
                .map(Some)
                .map_err(|_| error(RemoteSyncErrorKind::InvalidBatch, "invalid safety actions"));
        }
        Err(error(
            RemoteSyncErrorKind::Authentication,
            "safety credential rejected",
        ))
    }

    async fn token(&self, run_id: &str) -> Result<String, RemoteSyncError> {
        let mut access = self.access.lock().await;
        if let Some(access) = access
            .as_ref()
            .filter(|a| a.expires_at_us > now_epoch_micros() + 30_000_000 && a.run_id == run_id)
        {
            return Ok(access.token.clone());
        }
        let response = self.client.post(format!("{}/v1/register", self.url)).json(&serde_json::json!({
            "strategyId":self.config.strategy_id, "liveSessionId":self.config.live_session_id,
            "runtimeId":self.config.runtime_id, "runId":run_id, "serverNodeId":self.config.server_node_id,
            "sessionSecret":self.config.session_secret,
        })).send().await.map_err(|_| error(RemoteSyncErrorKind::Transient, "event gateway registration unavailable"))?;
        if response.status() == reqwest::StatusCode::GONE {
            return Err(error(RemoteSyncErrorKind::Terminal, RUN_RETIRED));
        }
        let response = successful(response).await?;
        let mut granted: Access = response.json().await.map_err(|_| {
            error(
                RemoteSyncErrorKind::Transient,
                "invalid event gateway registration response",
            )
        })?;
        granted.run_id = run_id.into();
        let token = granted.token.clone();
        *access = Some(granted);
        Ok(token)
    }

    async fn send_batch(
        &self,
        token: &str,
        batch: &LiveSyncBatch,
        protobuf: bool,
    ) -> Result<reqwest::Response, RemoteSyncError> {
        let request = self
            .client
            .post(format!("{}/v1/batch", self.url))
            .bearer_auth(token);
        let request = if protobuf {
            request
                .header(
                    reqwest::header::CONTENT_TYPE,
                    reqwest::header::HeaderValue::from_static(aqe_live_protocol::protobuf::MIME),
                )
                .body(aqe_live_protocol::protobuf::encode(batch).map_err(|_| {
                    error(
                        RemoteSyncErrorKind::InvalidBatch,
                        "invalid Protobuf live batch",
                    )
                })?)
        } else {
            request.json(batch)
        };
        request.send().await.map_err(|_| {
            error(
                RemoteSyncErrorKind::Transient,
                "event gateway publication unavailable",
            )
        })
    }
    pub async fn publish(
        &self,
        batch: &LiveSyncBatch,
    ) -> Result<LiveSyncBatchResult, RemoteSyncError> {
        let run_id = batch
            .events()
            .next()
            .and_then(|e| e.run_id.as_deref())
            .unwrap_or(&self.config.run_id);
        let protobuf = aqe_live_protocol::protobuf::prefer(batch);
        for attempt in 0..2 {
            let token = self.token(run_id).await?;
            let response = self.send_batch(&token, batch, protobuf).await?;
            if response.status().as_u16() == 401 && attempt == 0 {
                *self.access.lock().await = None;
                continue;
            }
            return successful(response).await?.json().await.map_err(|_| {
                error(
                    RemoteSyncErrorKind::Transient,
                    "invalid event gateway acknowledgement",
                )
            });
        }
        Err(error(
            RemoteSyncErrorKind::Authentication,
            "event gateway rejected refreshed credentials",
        ))
    }
}

fn error(kind: RemoteSyncErrorKind, message: &str) -> RemoteSyncError {
    RemoteSyncError {
        kind,
        message: message.into(),
    }
}

async fn successful(response: reqwest::Response) -> Result<reqwest::Response, RemoteSyncError> {
    let status = response.status().as_u16();
    if response.status().is_success() {
        return Ok(response);
    }
    if status == 429 {
        // Only the versioned pause response changes delivery behavior. Ordinary
        // load shedding remains a transient failure. Bound untrusted error bodies.
        let mut response = response;
        let mut body = Vec::new();
        while let Ok(Some(chunk)) = response.chunk().await {
            if body.len() + chunk.len() > 4096 {
                break;
            }
            body.extend_from_slice(&chunk);
        }
        if is_cloud_pause(&body) {
            return Err(error(
                RemoteSyncErrorKind::CloudPaused,
                "Cloud delivery is paused by the service; local execution continues.",
            ));
        }
    }
    Err(error(
        match status {
            401 | 403 => RemoteSyncErrorKind::Authentication,
            400 | 413 | 422 => RemoteSyncErrorKind::InvalidBatch,
            409 => RemoteSyncErrorKind::SequenceConflict,
            _ => RemoteSyncErrorKind::Transient,
        },
        &format!("event gateway returned HTTP {status}"),
    ))
}

fn is_cloud_pause(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body).is_ok_and(|value| {
        value["version"] == 1 && value["code"] == "cloud_quota" && value["cloudPaused"] == true
    })
}

#[cfg(test)]
mod quota_tests {
    #[test]
    fn cloud_pause_requires_versioned_explicit_service_response() {
        assert!(super::is_cloud_pause(
            br#"{"version":1,"code":"cloud_quota","cloudPaused":true}"#
        ));
        for body in [
            br#"{"code":"rate_limit"}"#.as_slice(),
            br#"{"version":2,"code":"cloud_quota","cloudPaused":true}"#,
            br#"{"version":1,"code":"cloud_quota","cloudPaused":false}"#,
            b"temporarily unavailable",
        ] {
            assert!(!super::is_cloud_pause(body));
        }
    }
}
