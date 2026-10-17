use super::{UnifiedBroker, traits};
use crate::core::broker::types::{Account, Asset};

/// Execution capabilities exposed by an AQE broker implementation.
///
/// These describe broker-native behavior, not strategy policy. In particular,
/// `OcoOrder` means the broker owns the lifecycle of attached take-profit and
/// stop-loss protection after an entry is accepted.
#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum BrokerFeature {
    MarketOrder,
    LimitOrder,
    StopOrder,
    StopLimitOrder,
    OcoOrder,
    TrailingOrder,
    PartialClose,
    /// Amend the limit component of an unfilled entry without cancel/re-submit.
    AmendLimitPrice,
    /// Amend the trigger component of an unfilled stop/stop-limit entry.
    AmendStopPrice,
    /// `Account::buying_power` already includes the broker's available leverage.
    ///
    /// Brokers that expose unleveraged free margin should omit this feature so
    /// strategy sizing can explicitly apply `Account::leverage` when appropriate.
    BuyingPowerLeveraged,
    /// Reports a broker-imposed pending-order ceiling on `Account::max_total_orders`.
    MaxTotalOrders,
    /// Reports aggregate symbol volume limits on `Asset::max_total_size`.
    MaxTotalSize,
    /// Broker can estimate required margin in account currency for an exact
    /// immutable order request without placing it.
    AuthoritativeMarginEstimate,
    /// Broker emits order lifecycle events without AQE polling for them.
    TradeEventStream,
    /// Broker emits account changes without AQE polling `get_account`.
    AccountStream,
    /// Broker can produce an authoritative batch lifecycle snapshot for restart reconciliation.
    OrderLifecycleSnapshot,
    /// Broker owns attached take-profit/stop-loss lifecycle after entry acceptance.
    ///
    /// `OcoOrder` is retained for source compatibility; new runtime decisions should use this
    /// explicit capability.
    BrokerManagedOco,
}

/// Market-data capabilities exposed by an AQE data-feed implementation.
#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DataFeedFeature {
    HistoricalCandles,
    LatestCandle,
    LatestQuote,
    CandleStream,
    IntraCandleStream,
    /// Independent, lease-scoped quote callbacks. Does not imply tick-complete delivery.
    QuoteStream,
    /// Quotes carry the provider's actual bid/ask rather than a bar-close approximation.
    NativeBidAsk,
    /// Quotes are bar-close approximations (bid == ask), not executable market quotes.
    SyntheticQuotes,
}

/// Resolve the buying power available for sizing from a broker account snapshot.
///
/// Some brokers, including the paper broker, already include leverage in
/// [`Account::buying_power`] and advertise [`BrokerFeature::BuyingPowerLeveraged`]. Other
/// brokers expose unleveraged free margin alongside [`Account::leverage`]. Apply leverage only
/// for marginable assets whose broker reports the latter representation, preventing both
/// under-sizing and accidental double leverage.
pub fn effective_buying_power_from(
    account: &Account,
    asset: &Asset,
    buying_power_is_leveraged: bool,
) -> f64 {
    if asset.marginable && !buying_power_is_leveraged {
        account.buying_power * f64::from(account.leverage.max(1))
    } else {
        account.buying_power
    }
}

/// Feature type accepted by [`UnifiedBroker::has_feature`] and
/// [`UnifiedBroker::has_features`].
///
/// Rust does not overload functions by argument type. This trait provides the
/// same call-site ergonomics while dispatching `BrokerFeature` to the execution
/// broker and `DataFeedFeature` to the data feed.
pub trait UnifiedBrokerFeature: Copy {
    fn is_supported_by<E, D>(self, broker: &UnifiedBroker<E, D>) -> bool
    where
        E: traits::Broker + traits::OrderManagementProvider,
        D: traits::DataFeed + traits::DataProvider;
}

impl UnifiedBrokerFeature for BrokerFeature {
    fn is_supported_by<E, D>(self, broker: &UnifiedBroker<E, D>) -> bool
    where
        E: traits::Broker + traits::OrderManagementProvider,
        D: traits::DataFeed + traits::DataProvider,
    {
        broker.execution.get_features().contains(&self)
    }
}

impl UnifiedBrokerFeature for DataFeedFeature {
    fn is_supported_by<E, D>(self, broker: &UnifiedBroker<E, D>) -> bool
    where
        E: traits::Broker + traits::OrderManagementProvider,
        D: traits::DataFeed + traits::DataProvider,
    {
        traits::DataFeed::get_features(broker).contains(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::broker::types::{AccountType, AssetExchange, AssetStatus, AssetType};

    fn account(buying_power: f64, leverage: u8) -> Account {
        Account {
            account_id: "test".to_string(),
            max_total_orders: None,
            account_type: AccountType::Paper,
            equity: 100_000.0,
            cash: 90_000.0,
            currency: "USD".to_string(),
            buying_power,
            accrued_commission: 0.0,
            shorting_enabled: true,
            leverage,
        }
    }

    fn asset(marginable: bool) -> Asset {
        Asset {
            id: "ETHUSD".to_string(),
            symbol: "ETHUSD".to_string(),
            name: "Ethereum".to_string(),
            asset_type: AssetType::Crypto,
            status: AssetStatus::Active,
            exchange: AssetExchange::UNKNOWN("TEST".to_string()),
            tradable: true,
            marginable,
            shortable: true,
            fractional: true,
            min_order_size: Some(0.01),
            quantity_base: Some(2),
            max_order_size: None,
            max_total_size: None,
            min_price_increment: Some(0.01),
            price_base: Some(2),
            contract_size: None,
            trading_calendar: None,
            fees: Default::default(),
        }
    }

    #[test]
    fn applies_leverage_to_unleveraged_marginable_buying_power() {
        assert_eq!(
            effective_buying_power_from(&account(80_000.0, 10), &asset(true), false),
            800_000.0
        );
    }

    #[test]
    fn does_not_double_apply_broker_included_leverage() {
        assert_eq!(
            effective_buying_power_from(&account(800_000.0, 10), &asset(true), true),
            800_000.0
        );
    }

    #[test]
    fn does_not_apply_leverage_to_non_marginable_assets() {
        assert_eq!(
            effective_buying_power_from(&account(80_000.0, 10), &asset(false), false),
            80_000.0
        );
    }
}
