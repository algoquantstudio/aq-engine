//! Optional broker analytics reads, independent of order execution and Insight
//! structure. Callers establish ownership and sampling cadence separately.
use super::types::BrokerError;
pub use aqe_live_protocol::{PositionValuations, validate_position_valuation_request};

#[async_trait::async_trait]
pub trait PositionValuationReader: Send + Sync {
    /// One bounded symbol batch. None means unavailable, not a flat portfolio.
    /// Implementations must not select subscriptions or mutate broker orders.
    async fn read_positions(
        &self,
        account_id: &str,
        symbol: &str,
        position_ids: &[String],
    ) -> Result<Option<PositionValuations>, BrokerError>;
}
