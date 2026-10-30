//! Reject unauthorized HTTP producers before Actix takes or polls their body.
//! Session/envelope checks still happen in each handler after bounded decoding.
use super::{Mt5Bridge, authorize_token, bridge_error};
use actix_web::{FromRequest, HttpRequest, dev::Payload, http::StatusCode, web};
use futures::future::LocalBoxFuture;
use serde::de::DeserializeOwned;

pub(super) struct AuthenticatedJson<T>(pub(super) T);

impl<T> AuthenticatedJson<T> {
    pub(super) fn into_inner(self) -> T {
        self.0
    }
}

impl<T: DeserializeOwned + 'static> FromRequest for AuthenticatedJson<T> {
    type Error = actix_web::Error;
    type Future = LocalBoxFuture<'static, Result<Self, Self::Error>>;

    fn from_request(request: &HttpRequest, payload: &mut Payload) -> Self::Future {
        let Some(bridge) = request.app_data::<web::Data<Mt5Bridge>>() else {
            return Box::pin(async {
                Err(actix_web::error::ErrorInternalServerError(
                    "MT5 bridge unavailable",
                ))
            });
        };
        if let Err(message) = authorize_token(bridge.get_ref(), request.headers()) {
            if request.path() == "/v1/rpc/poll" {
                // Keep reachability diagnostics without decoding an untrusted
                // envelope or treating the header as an authenticated session.
                bridge.record_rpc_poll_attempt("");
                bridge.record_rpc_poll_auth_failure(message.clone());
            }
            let response = bridge_error(
                bridge.get_ref(),
                uuid::Uuid::new_v4().to_string(),
                StatusCode::UNAUTHORIZED,
                message,
            );
            return Box::pin(async move {
                Err(actix_web::error::InternalError::from_response(
                    "MT5 bridge authorization failed",
                    response,
                )
                .into())
            });
        }
        // Delegate only after admission: keep the existing JsonConfig limit,
        // media-type validation and decompression handling for authorized EAs.
        let decoded = web::Json::<T>::from_request(request, payload);
        Box::pin(async move { decoded.await.map(|json| Self(json.into_inner())) })
    }
}
