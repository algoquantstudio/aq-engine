use super::StrategyState;
use super::aqs_types::{self, AqsAuth};
use super::traits::Strategy;
use crate::core::broker::traits::{Broker, DataFeed, DataProvider, OrderManagementProvider};
use crate::core::broker::types::BrokerError;
use log::{debug, error, info, warn};
use std::time::Duration;
use surrealdb::Notification;
use surrealdb::engine::any;
use surrealdb::method::QueryStream;
use surrealdb::opt::auth::Record;

pub(super) type StrategyActionStream = QueryStream<Notification<surrealdb::types::Value>>;

const LIVE_SYNC_CONNECT_MAX_ATTEMPTS: usize = 3;
const LIVE_SYNC_RETRY_BASE_MS: u64 = 500;
const LIVE_SYNC_CONNECT_TIMEOUT_SECS: u64 = 10;
const LIVE_SYNC_STREAM_TIMEOUT_SECS: u64 = 10;
const LIVE_SYNC_RECONNECT_TIMEOUT_SECS: u64 = 40;

impl<S, E, D> StrategyState<S, E, D>
where
    S: Strategy,
    E: Broker + OrderManagementProvider,
    D: DataFeed + DataProvider,
{
    pub(super) fn live_session_key_for_auth(auth: &AqsAuth) -> String {
        auth.live_session_id
            .as_deref()
            .map(aqs_types::live_session_key)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| auth.session_id.clone())
    }

    pub(super) async fn connect_live_sync_db(
        auth: &AqsAuth,
    ) -> Result<surrealdb::Surreal<any::Any>, BrokerError> {
        let signin_session_id = auth.session_id.clone();
        info!(
            "Authenticating AQS Cloud live sync with session id: {}",
            signin_session_id
        );
        for attempt in 1..=LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
            let connect_result = tokio::time::timeout(
                Duration::from_secs(LIVE_SYNC_CONNECT_TIMEOUT_SECS),
                any::connect(auth.url()),
            )
            .await;

            match connect_result {
                Err(_) => {
                    let message = format!(
                        "Timed out connecting to AQS Cloud after {}s",
                        LIVE_SYNC_CONNECT_TIMEOUT_SECS
                    );
                    if attempt < LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
                        warn!(
                            "AQS live sync connection error for strategy {} on attempt {}/{}: {}",
                            auth.strategy_id, attempt, LIVE_SYNC_CONNECT_MAX_ATTEMPTS, message
                        );
                        tokio::time::sleep(Duration::from_millis(
                            LIVE_SYNC_RETRY_BASE_MS * attempt as u64,
                        ))
                        .await;
                        continue;
                    }
                    error!("{}", message);
                    return Err(BrokerError::ConnectionError(message));
                }
                Ok(Err(error)) => {
                    let message = format!("Failed to connect to AQS Cloud: {}", error);
                    if attempt < LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
                        warn!(
                            "AQS live sync connection error for strategy {} on attempt {}/{}: {}",
                            auth.strategy_id, attempt, LIVE_SYNC_CONNECT_MAX_ATTEMPTS, message
                        );
                        tokio::time::sleep(Duration::from_millis(
                            LIVE_SYNC_RETRY_BASE_MS * attempt as u64,
                        ))
                        .await;
                        continue;
                    }
                    error!("{}", message);
                    return Err(BrokerError::ConnectionError(message));
                }
                Ok(Ok(client)) => {
                    debug!(
                        "Connected transport for AQS live sync on strategy {}; authenticating session",
                        auth.strategy_id
                    );
                    let signin_result = tokio::time::timeout(
                        Duration::from_secs(LIVE_SYNC_CONNECT_TIMEOUT_SECS),
                        async {
                            client
                                .signin(Record {
                                    namespace: "aqs".to_string(),
                                    database: "aqs".to_string(),
                                    access: auth.access_method.clone(),
                                    params: std::collections::BTreeMap::from([
                                        ("session_id".to_string(), signin_session_id.clone()),
                                        ("secret".to_string(), auth.session_secret.clone()),
                                    ]),
                                })
                                .await
                                .map_err(|e| {
                                    format!("Failed to authenticate AQS Cloud live sync: {}", e)
                                })?;
                            client.use_ns("aqs").use_db("aqs").await.map_err(|e| {
                                format!("Failed to select AQS database for live sync: {}", e)
                            })?;
                            Ok::<(), String>(())
                        },
                    )
                    .await;

                    match signin_result {
                        Ok(Ok(())) => {
                            // This connection is deliberately read/action-only. Strategy/session
                            // projections are owned by the runtime host and event gateway.
                            info!("Authenticated AQS Cloud read/action connection");
                            return Ok(client);
                        }
                        Ok(Err(error)) => {
                            if attempt < LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
                                warn!(
                                    "AQS live sync authentication error for strategy {} on attempt {}/{}: {}",
                                    auth.strategy_id,
                                    attempt,
                                    LIVE_SYNC_CONNECT_MAX_ATTEMPTS,
                                    error
                                );
                                tokio::time::sleep(Duration::from_millis(
                                    LIVE_SYNC_RETRY_BASE_MS * attempt as u64,
                                ))
                                .await;
                                continue;
                            }
                            error!("{}", error);
                            return Err(BrokerError::ConnectionError(error));
                        }
                        Err(_) => {
                            let message = format!(
                                "Timed out authenticating AQS Cloud live sync after {}s",
                                LIVE_SYNC_CONNECT_TIMEOUT_SECS
                            );
                            if attempt < LIVE_SYNC_CONNECT_MAX_ATTEMPTS {
                                warn!(
                                    "AQS live sync authentication error for strategy {} on attempt {}/{}: {}",
                                    auth.strategy_id,
                                    attempt,
                                    LIVE_SYNC_CONNECT_MAX_ATTEMPTS,
                                    message
                                );
                                tokio::time::sleep(Duration::from_millis(
                                    LIVE_SYNC_RETRY_BASE_MS * attempt as u64,
                                ))
                                .await;
                                continue;
                            }
                            error!("{}", message);
                            return Err(BrokerError::ConnectionError(message));
                        }
                    }
                }
            }
        }

        Err(BrokerError::ConnectionError(format!(
            "Failed to connect to AQS Cloud after {} attempt(s)",
            LIVE_SYNC_CONNECT_MAX_ATTEMPTS
        )))
    }

    pub(super) async fn create_strategy_action_stream(
        client: &surrealdb::Surreal<any::Any>,
        auth: &AqsAuth,
    ) -> Option<StrategyActionStream> {
        debug!(
            "Creating AQS strategy action stream for strategy {}",
            auth.strategy_id
        );
        let live_query = tokio::time::timeout(
            Duration::from_secs(LIVE_SYNC_STREAM_TIMEOUT_SECS),
            client
                .query(
                    "LIVE SELECT id, action, status FROM strategy_actions
                     WHERE strategy_id = type::record('strategy', $strategy_id)
                       AND live_session_id = type::record('live_strategy_session', <uuid>$live_session_key)
                       AND (source = NONE OR source != 'hosted_gateway')
                       AND status = 'pending'",
                )
                .bind(("strategy_id", auth.strategy_id.clone()))
                .bind(("live_session_key", Self::live_session_key_for_auth(auth))),
        )
        .await;

        match live_query {
            Ok(Ok(mut results)) => match results.stream::<Notification<surrealdb::types::Value>>(0)
            {
                Ok(stream) => {
                    debug!(
                        "AQS strategy action stream ready for strategy {}",
                        auth.strategy_id
                    );
                    Some(stream)
                }
                Err(error) => {
                    warn!(
                        "Failed to materialize AQS strategy action stream for strategy {}: {}",
                        auth.strategy_id, error
                    );
                    None
                }
            },
            Ok(Err(error)) => {
                warn!(
                    "Failed to create AQS strategy action stream for strategy {}: {}",
                    auth.strategy_id, error
                );
                None
            }
            Err(_) => {
                warn!(
                    "Timed out creating AQS strategy action stream for strategy {} after {}s",
                    auth.strategy_id, LIVE_SYNC_STREAM_TIMEOUT_SECS
                );
                None
            }
        }
    }

    pub(super) async fn connect_live_sync_resources(
        auth: &AqsAuth,
    ) -> Result<(surrealdb::Surreal<any::Any>, Option<StrategyActionStream>), BrokerError> {
        warn!(
            "Reconnecting AQE live sync to AQS for strategy {}",
            auth.strategy_id
        );
        let reconnect_result = tokio::time::timeout(
            Duration::from_secs(LIVE_SYNC_RECONNECT_TIMEOUT_SECS),
            async {
                let next_db = Self::connect_live_sync_db(auth).await?;
                let next_action_stream = Self::create_strategy_action_stream(&next_db, auth).await;
                Ok::<_, BrokerError>((next_db, next_action_stream))
            },
        )
        .await;

        match reconnect_result {
            Ok(Ok((next_db, next_action_stream))) => {
                info!(
                    "AQE live sync reconnect complete for strategy {}",
                    auth.strategy_id
                );
                if next_action_stream.is_none() {
                    warn!(
                        "AQE live sync reconnected for strategy {} but the strategy action stream is unavailable",
                        auth.strategy_id
                    );
                }
                Ok((next_db, next_action_stream))
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(BrokerError::ConnectionError(format!(
                "AQE live sync reconnect timed out for strategy {} after {}s",
                auth.strategy_id, LIVE_SYNC_RECONNECT_TIMEOUT_SECS
            ))),
        }
    }
}
