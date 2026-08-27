use crate::core::broker::DataStreamMode;
use crate::core::broker::data_stream::{DataStreamManager, FetchBarsFn};
use crate::core::broker::traits::{DataFeed, DataProvider};
use crate::core::broker::types::{
    Asset, AssetExchange, AssetStatus, AssetType, Bar, BarData, BrokerError, Quote,
};
use crate::core::utils::timeframe::{TimeFrame, TimeFrameUnit};
use chrono::{DateTime, TimeZone, Utc};
use dashmap::DashMap;
use log::{info, warn};
use polars::prelude::*;
use reqwest::{Client, StatusCode, header::RETRY_AFTER};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

const YAHOO_CHART_MAX_ATTEMPTS: usize = 4;
const YAHOO_RETRY_BASE_DELAY_MS: u64 = 250;
const YAHOO_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

pub struct YahooFinanceDataFeed {
    client: Client,
    connected: Arc<Mutex<bool>>,
    ticker_info: Arc<Mutex<HashMap<String, Asset>>>,
    crumb: Arc<Mutex<Option<String>>>,
    session_generation: Arc<AtomicU64>,
    session_refresh_lock: Arc<AsyncMutex<()>>,
    stream_manager: DataStreamManager,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum YahooRetryDecision {
    Stop,
    Retry,
    RefreshSession,
}

/// Immutable, process-local normalized bar snapshots. Hyperparameter seeds deliberately create
/// fresh brokers/strategies, but must not redownload or renormalize an identical market window.
static HISTORICAL_BAR_CACHE: OnceLock<DashMap<String, Vec<Bar>>> = OnceLock::new();

fn historical_bar_cache() -> &'static DashMap<String, Vec<Bar>> {
    HISTORICAL_BAR_CACHE.get_or_init(DashMap::new)
}

impl YahooFinanceDataFeed {
    pub fn new() -> Self {
        let client = Client::builder()
            .cookie_store(true)
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(30))
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            connected: Arc::new(Mutex::new(false)),
            ticker_info: Arc::new(Mutex::new(HashMap::new())),
            crumb: Arc::new(Mutex::new(None)),
            session_generation: Arc::new(AtomicU64::new(0)),
            session_refresh_lock: Arc::new(AsyncMutex::new(())),
            stream_manager: DataStreamManager::new(),
        }
    }

    fn set_connected(&self, connected: bool) {
        *self.connected.lock().unwrap() = connected;
    }

    fn retry_decision(status: StatusCode, body: &str) -> YahooRetryDecision {
        if Self::body_requires_session_refresh(body) {
            return YahooRetryDecision::RefreshSession;
        }
        match status {
            StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_EARLY | StatusCode::TOO_MANY_REQUESTS => {
                YahooRetryDecision::Retry
            }
            status if status.is_server_error() => YahooRetryDecision::Retry,
            _ => YahooRetryDecision::Stop,
        }
    }

    fn retry_after(response: &reqwest::Response) -> Option<Duration> {
        response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_secs)
    }

    fn retry_delay(attempt: usize, retry_after: Option<Duration>) -> Duration {
        let exponential = Duration::from_millis(
            YAHOO_RETRY_BASE_DELAY_MS.saturating_mul(1_u64 << attempt.min(4)),
        );
        retry_after
            .unwrap_or(exponential)
            .min(YAHOO_RETRY_MAX_DELAY)
    }

    fn yahoo_error_description(json: &Value) -> Option<&str> {
        json.pointer("/chart/error/description")
            .or_else(|| json.pointer("/finance/error/description"))
            .and_then(Value::as_str)
    }

    fn description_requires_session_refresh(description: &str) -> bool {
        let description = description.to_ascii_lowercase();
        description.contains("invalid crumb") || description.contains("invalid cookie")
    }

    fn json_requires_session_refresh(json: &Value) -> bool {
        Self::yahoo_error_description(json)
            .map(Self::description_requires_session_refresh)
            .unwrap_or(false)
    }

    fn body_requires_session_refresh(body: &str) -> bool {
        serde_json::from_str::<Value>(body)
            .ok()
            .as_ref()
            .map(Self::json_requires_session_refresh)
            .unwrap_or_else(|| Self::description_requires_session_refresh(body))
    }

    async fn get_crumb(&self) -> Result<String, BrokerError> {
        let response = self
            .client
            .get("https://query2.finance.yahoo.com/v1/test/getcrumb")
            .send()
            .await?;
        let response = response.error_for_status().map_err(BrokerError::from)?;
        let crumb: String = response.text().await?;
        let normalized = crumb.trim();
        let normalized_lower = normalized.to_ascii_lowercase();
        if normalized.is_empty()
            || normalized_lower.contains("invalid crumb")
            || normalized_lower.contains("invalid cookie")
            || normalized_lower.contains("too many requests")
            || normalized_lower.contains("html")
            || normalized_lower.starts_with("{\"finance\"")
        {
            return Err(BrokerError::DataFeedConnectionError(
                "Yahoo did not return a usable crumb".to_string(),
            ));
        }
        Ok(normalized.to_string())
    }

    async fn refresh_session(&self) -> Result<(), BrokerError> {
        let _refresh_guard = self.session_refresh_lock.lock().await;
        self.refresh_session_locked().await
    }

    async fn refresh_session_for_generation(
        &self,
        observed_generation: u64,
    ) -> Result<(), BrokerError> {
        let _refresh_guard = self.session_refresh_lock.lock().await;
        if self.session_generation.load(Ordering::Acquire) != observed_generation
            && self.crumb.lock().unwrap().is_some()
        {
            return Ok(());
        }
        self.refresh_session_locked().await
    }

    async fn refresh_session_locked(&self) -> Result<(), BrokerError> {
        let _ = self.client.get("https://finance.yahoo.com").send().await;
        let _ = self.client.get("https://fc.yahoo.com").send().await;
        let crumb = match self.get_crumb().await {
            Ok(crumb) => crumb,
            Err(error) => {
                self.set_connected(false);
                return Err(error);
            }
        };
        *self.crumb.lock().unwrap() = Some(crumb);
        self.session_generation.fetch_add(1, Ordering::Release);
        self.set_connected(true);
        Ok(())
    }

    fn chart_interval(time_frame: TimeFrame) -> String {
        match time_frame.get_unit() {
            TimeFrameUnit::Minute => format!("{}m", time_frame.get_amount()),
            TimeFrameUnit::Hour => format!("{}h", time_frame.get_amount()),
            TimeFrameUnit::Day => format!("{}d", time_frame.get_amount()),
            TimeFrameUnit::Month => format!("{}mo", time_frame.get_amount()),
            _ => "1m".to_string(),
        }
    }

    fn chart_url(host: &str, symbol: &str, start_ts: i64, end_ts: i64, interval: &str) -> String {
        format!(
            "https://{}/v8/finance/chart/{}?period1={}&period2={}&interval={}",
            host, symbol, start_ts, end_ts, interval
        )
    }

    async fn request_chart_json(
        &self,
        symbol: &str,
        start_ts: i64,
        end_ts: i64,
        interval: &str,
    ) -> Result<Value, BrokerError> {
        let hosts = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"];
        let mut last_error = None;
        let mut observed_generation = self.session_generation.load(Ordering::Acquire);
        let mut refreshed_after_host_failover = false;
        let mut final_failure_disconnects = false;

        for attempt in 0..YAHOO_CHART_MAX_ATTEMPTS {
            let host = hosts[attempt % hosts.len()];
            let url = Self::chart_url(host, symbol, start_ts, end_ts, interval);
            match self.client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status();
                    let retry_after = Self::retry_after(&response);
                    let body = match response.text().await {
                        Ok(body) => body,
                        Err(error) => {
                            final_failure_disconnects = true;
                            last_error = Some(BrokerError::ConnectionError(format!(
                                "Yahoo chart response body failed for {symbol} via {host}: {error}"
                            )));
                            if attempt + 1 < YAHOO_CHART_MAX_ATTEMPTS {
                                let delay = Self::retry_delay(attempt, retry_after);
                                tokio::time::sleep(delay).await;
                            }
                            continue;
                        }
                    };
                    if status.is_success() {
                        match serde_json::from_str::<Value>(&body) {
                            Ok(json) if Self::json_requires_session_refresh(&json) => {
                                final_failure_disconnects = true;
                                last_error = Some(BrokerError::DataFeedConnectionError(format!(
                                    "Yahoo rejected the active cookie/crumb session for {symbol}"
                                )));
                                if let Err(error) = self
                                    .refresh_session_for_generation(observed_generation)
                                    .await
                                {
                                    last_error = Some(error);
                                }
                                observed_generation =
                                    self.session_generation.load(Ordering::Acquire);
                            }
                            Ok(json) => {
                                self.set_connected(true);
                                return Ok(json);
                            }
                            Err(error) => {
                                final_failure_disconnects = true;
                                last_error = Some(BrokerError::ConnectionError(format!(
                                    "Yahoo returned invalid chart JSON for {symbol} via {host}: {error}"
                                )));
                            }
                        }
                    } else {
                        let decision = Self::retry_decision(status, &body);
                        let yahoo_description =
                            serde_json::from_str::<Value>(&body).ok().and_then(|json| {
                                Self::yahoo_error_description(&json).map(ToString::to_string)
                            });
                        last_error = Some(BrokerError::ConnectionError(format!(
                            "Yahoo chart request failed for {symbol} with status {status} via {host}{}",
                            yahoo_description
                                .map(|description| format!(": {description}"))
                                .unwrap_or_default()
                        )));

                        if decision == YahooRetryDecision::Stop {
                            return Err(last_error.expect("Yahoo status error was recorded"));
                        }
                        final_failure_disconnects = decision == YahooRetryDecision::RefreshSession
                            || status != StatusCode::TOO_MANY_REQUESTS;
                        if decision == YahooRetryDecision::RefreshSession {
                            if let Err(error) = self
                                .refresh_session_for_generation(observed_generation)
                                .await
                            {
                                last_error = Some(error);
                            }
                            observed_generation = self.session_generation.load(Ordering::Acquire);
                        }
                    }

                    if attempt + 1 < YAHOO_CHART_MAX_ATTEMPTS {
                        let delay = Self::retry_delay(attempt, retry_after);
                        warn!(
                            "Yahoo chart request for {symbol} failed on attempt {}/{} via {host}; retrying in {:?}",
                            attempt + 1,
                            YAHOO_CHART_MAX_ATTEMPTS,
                            delay
                        );
                        tokio::time::sleep(delay).await;
                    }
                }
                Err(error) => {
                    final_failure_disconnects = true;
                    last_error = Some(BrokerError::ConnectionError(format!(
                        "error sending request for url ({}) -> {}",
                        url, error
                    )));
                    if attempt + 1 < YAHOO_CHART_MAX_ATTEMPTS {
                        let delay = Self::retry_delay(attempt, None);
                        warn!(
                            "Yahoo chart request for {symbol} failed on attempt {}/{} via {host}; retrying in {:?}: {error}",
                            attempt + 1,
                            YAHOO_CHART_MAX_ATTEMPTS,
                            delay
                        );
                        tokio::time::sleep(delay).await;
                    }
                }
            }

            if !refreshed_after_host_failover && attempt + 1 == hosts.len() {
                if let Err(error) = self
                    .refresh_session_for_generation(observed_generation)
                    .await
                {
                    warn!("Yahoo session refresh during chart failover failed: {error}");
                }
                observed_generation = self.session_generation.load(Ordering::Acquire);
                refreshed_after_host_failover = true;
            }
        }

        if final_failure_disconnects {
            self.set_connected(false);
        }
        Err(last_error.unwrap_or_else(|| {
            BrokerError::ConnectionError(format!(
                "Yahoo chart request failed for {} with unknown error",
                symbol
            ))
        }))
    }

    async fn request_quote_summary_json(&self, symbol: &str) -> Option<Value> {
        let mut observed_generation = self.session_generation.load(Ordering::Acquire);
        for attempt in 0..2 {
            let crumb = self.crumb.lock().unwrap().clone();
            let Some(crumb) = crumb else {
                if self
                    .refresh_session_for_generation(observed_generation)
                    .await
                    .is_err()
                {
                    return None;
                }
                observed_generation = self.session_generation.load(Ordering::Acquire);
                continue;
            };
            let url = format!(
                "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{}?crumb={}&modules=financialData,quoteType,defaultKeyStatistics,assetProfile,summaryDetail",
                symbol, crumb
            );
            let response = match self.client.get(&url).send().await {
                Ok(response) => response,
                Err(error) => {
                    warn!(
                        "Yahoo metadata request failed for {symbol}; using fallback asset metadata: {error}"
                    );
                    return None;
                }
            };
            let status = response.status();
            let body = match response.text().await {
                Ok(body) => body,
                Err(error) => {
                    warn!(
                        "Yahoo metadata response failed for {symbol}; using fallback asset metadata: {error}"
                    );
                    return None;
                }
            };
            let parsed = serde_json::from_str::<Value>(&body).ok();
            let invalid_session = parsed
                .as_ref()
                .map(Self::json_requires_session_refresh)
                .unwrap_or_else(|| Self::body_requires_session_refresh(&body));

            if invalid_session && attempt == 0 {
                if let Err(error) = self
                    .refresh_session_for_generation(observed_generation)
                    .await
                {
                    warn!(
                        "Yahoo metadata session refresh failed for {symbol}; using fallback asset metadata: {error}"
                    );
                    return None;
                }
                observed_generation = self.session_generation.load(Ordering::Acquire);
                continue;
            }

            if !status.is_success() {
                warn!(
                    "Yahoo metadata request returned {status} for {symbol}; using fallback asset metadata"
                );
                return None;
            }
            return parsed;
        }
        None
    }

    fn map_asset_type(&self, asset_type: &str) -> AssetType {
        match asset_type {
            "EQUITY" => AssetType::Stock,
            "CRYPTOCURRENCY" => AssetType::Crypto,
            "ETF" => AssetType::ETF,
            "INDEX" => AssetType::Index,
            "CURRENCY" => AssetType::Forex,
            "COMMODITY" => AssetType::Commodity,
            "MUTUALFUND" => AssetType::MutualFund,
            _ => AssetType::UNKNOWN(asset_type.to_string()),
        }
    }

    fn map_asset_exchange(&self, asset_exchange: &str) -> AssetExchange {
        match asset_exchange {
            "NYSE" | "NYQ" => AssetExchange::NYSE,
            "NASDAQ" | "NAS" | "NGM" | "NMS" | "NasdaqGS" => AssetExchange::NASDAQ,
            "AMEX" => AssetExchange::AMEX,
            _ => AssetExchange::UNKNOWN(asset_exchange.to_string()),
        }
    }

    /// Internal helper: fetch raw bars from Yahoo Finance API as Vec<Bar>.
    /// Used by both `get_history` (DataFrame) and `get_latest_bar`.
    async fn fetch_bars_raw(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        time_frame: TimeFrame,
    ) -> Result<BarData, BrokerError> {
        let start_ts = start.timestamp();
        let end_ts = end.timestamp();
        let interval = Self::chart_interval(time_frame);
        let cache_key = format!("{symbol}|{start_ts}|{end_ts}|{interval}");
        if let Some(bars) = historical_bar_cache().get(&cache_key) {
            info!(
                "Reusing immutable Yahoo bar snapshot for {symbol} {interval} ({start_ts}..{end_ts})"
            );
            return Ok(BarData::Bars(bars.clone()));
        }
        let json = self
            .request_chart_json(symbol, start_ts, end_ts, &interval)
            .await?;

        let result = json["chart"]["result"][0]
            .as_object()
            .ok_or(BrokerError::TradeError(
                "Invalid Yahoo response: ".to_string()
                    + &json["chart"]["error"]["description"]
                        .as_str()
                        .unwrap_or("Unknown error"),
            ))?;

        let timestamp = match result.get("timestamp").and_then(|v| v.as_array()) {
            Some(ts) => ts,
            None => return Ok(BarData::Bars(Vec::new())),
        };

        let indicators = match result
            .get("indicators")
            .and_then(|v| v.get("quote"))
            .and_then(|v| v.get(0))
            .and_then(|v| v.as_object())
        {
            Some(ind) => ind,
            None => return Ok(BarData::Bars(Vec::new())),
        };

        let opens = indicators
            .get("open")
            .and_then(|v| v.as_array())
            .ok_or(BrokerError::TradeError("No open data".to_string()))?;
        let highs = indicators
            .get("high")
            .and_then(|v| v.as_array())
            .ok_or(BrokerError::TradeError("No high data".to_string()))?;
        let lows = indicators
            .get("low")
            .and_then(|v| v.as_array())
            .ok_or(BrokerError::TradeError("No low data".to_string()))?;
        let closes = indicators
            .get("close")
            .and_then(|v| v.as_array())
            .ok_or(BrokerError::TradeError("No close data".to_string()))?;
        let volumes = indicators
            .get("volume")
            .and_then(|v| v.as_array())
            .ok_or(BrokerError::TradeError("No volume data".to_string()))?;

        let mut bars = Vec::with_capacity(timestamp.len());

        for (((((timestamp_value, open), high), low), close), volume) in timestamp
            .iter()
            .zip(opens.iter())
            .zip(highs.iter())
            .zip(lows.iter())
            .zip(closes.iter())
            .zip(volumes.iter())
        {
            let ts = timestamp_value.as_i64().unwrap_or(0);
            if open.is_null() || high.is_null() || low.is_null() || close.is_null() {
                continue;
            }

            let raw_time = Utc.timestamp_opt(ts, 0).unwrap();
            let normalized_time = time_frame.get_current_time_increment(raw_time);

            bars.push(Bar {
                symbol: symbol.to_string(),
                open: open.as_f64().unwrap_or(0.0),
                high: high.as_f64().unwrap_or(0.0),
                low: low.as_f64().unwrap_or(0.0),
                close: close.as_f64().unwrap_or(0.0),
                volume: volume.as_f64().unwrap_or(0.0),
                timestamp: normalized_time,
            });
        }

        historical_bar_cache().insert(cache_key, bars.clone());
        Ok(BarData::Bars(bars))
    }
}

impl DataFeed for YahooFinanceDataFeed {
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.refresh_session().await?;
        Ok(true)
    }

    async fn disconnect(&self) -> Result<bool, BrokerError> {
        let mut connected = self.connected.lock().unwrap();
        let mut crumb = self.crumb.lock().unwrap();
        *connected = false;
        *crumb = None;
        Ok(true)
    }

    fn is_connected(&self) -> bool {
        *self.connected.lock().unwrap()
    }
}

impl DataProvider for YahooFinanceDataFeed {
    async fn get_ticker_info(&self, symbol: &str) -> Result<Asset, BrokerError> {
        if let Some(asset) = self.ticker_info.lock().unwrap().get(symbol).cloned() {
            return Ok(asset);
        }

        let fallback_asset = || Asset {
            id: format!("yahoo:{}", symbol),
            symbol: symbol.to_string(),
            name: symbol.to_string(),
            asset_type: AssetType::Stock,
            status: AssetStatus::Active,
            exchange: AssetExchange::NYSE,
            tradable: true,
            marginable: true,
            shortable: true,
            fractional: true,
            min_order_size: Some(0.01),
            quantity_base: Some(2),
            max_order_size: None,
            min_price_increment: Some(0.01),
            price_base: Some(2),
            contract_size: None,
            fees: Default::default(),
        };

        // Yahoo's quoteSummary endpoint is frequently entitlement-gated even while the chart
        // endpoint (used for the actual backtest data) remains available. Metadata must not
        // prevent an otherwise valid equity universe from reaching the chart request.
        let json = self.request_quote_summary_json(symbol).await;
        let Some(result) = json
            .as_ref()
            .and_then(|json| json["quoteSummary"]["result"].get(0))
            .and_then(Value::as_object)
        else {
            let asset = fallback_asset();
            self.ticker_info
                .lock()
                .unwrap()
                .insert(symbol.to_string(), asset.clone());
            warn!("Yahoo metadata was unavailable for {symbol}; using fallback asset metadata");
            return Ok(asset);
        };

        let instrument_type = result["quoteType"]["quoteType"].as_str().unwrap();
        let exchange_name = result["quoteType"]["exchange"].as_str().unwrap();
        let price_hint = result["summaryDetail"]["priceHint"]["raw"]
            .as_i64()
            .unwrap();
        let asset_uuid = result["quoteType"]["uuid"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        // `summaryDetail.tradeable` is commonly false/missing for normal Yahoo equities.
        // A configured Yahoo symbol is eligible for a historical backtest; the chart request
        // below is the authoritative validation of whether data is available.
        let is_tradable = true;

        let asset = Asset {
            id: asset_uuid,
            symbol: result["quoteType"]["symbol"]
                .as_str()
                .unwrap_or(symbol)
                .to_string(),
            name: result["quoteType"]["symbol"]
                .as_str()
                .unwrap_or(symbol)
                .to_string(),
            asset_type: self.map_asset_type(instrument_type),
            status: AssetStatus::Active,
            exchange: self.map_asset_exchange(exchange_name),
            tradable: is_tradable,
            marginable: true,
            shortable: true,
            fractional: true,
            min_order_size: Some(0.01),
            quantity_base: Some(2),
            max_order_size: None,
            min_price_increment: Some(1.0 / 10f64.powf(price_hint as f64)),
            price_base: Some(price_hint),
            contract_size: None,
            fees: Default::default(),
        };

        self.ticker_info
            .lock()
            .unwrap()
            .insert(symbol.to_string(), asset.clone());
        Ok(asset)
    }

    /// Returns historical bar data as a Polars DataFrame.
    /// Columns: symbol (str), open (f64), high (f64), low (f64), close (f64), volume (f64), timestamp (datetime[ms])
    async fn get_history(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        time_frame: TimeFrame,
    ) -> Result<DataFrame, BrokerError> {
        let bars = self.fetch_bars_raw(symbol, start, end, time_frame).await?;

        // Wrap in BarData::Bars for the default Vec<Bar> → DataFrame conversion
        self.format_on_bar(bars)
            .map_err(|e| BrokerError::DataFeedError(e.to_string()))
    }

    async fn get_latest_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        let bar = self.get_latest_bar(symbol).await?;
        Ok(Quote {
            symbol: symbol.to_string(),
            bid: bar.close,
            ask: bar.close,
            bid_size: 100.0,
            ask_size: 100.0,
            last: Some(bar.close),
            last_size: Some(bar.volume),
            timestamp: bar.timestamp,
        })
    }

    async fn get_latest_bar(&self, symbol: &str) -> Result<Bar, BrokerError> {
        let end = Utc::now();
        let start = end - chrono::Duration::days(5);
        let bars = self
            .fetch_bars_raw(symbol, start, end, TimeFrame::new(1, TimeFrameUnit::Day))
            .await?;
        match bars {
            BarData::Bars(bars) => bars
                .last()
                .cloned()
                .ok_or(BrokerError::DataFeedError("No data found".to_string())),
            BarData::Frame(df) => {
                let last = df.tail(Some(1));
                if last.height() == 0 {
                    return Err(BrokerError::DataFeedError("No data found".to_string()));
                }
                let row_symbol = last
                    .column("symbol")
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .str()
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .get(0)
                    .unwrap_or(symbol)
                    .to_string();
                let open = last
                    .column("open")
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .f64()
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .get(0)
                    .unwrap_or(0.0);
                let high = last
                    .column("high")
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .f64()
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .get(0)
                    .unwrap_or(0.0);
                let low = last
                    .column("low")
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .f64()
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .get(0)
                    .unwrap_or(0.0);
                let close = last
                    .column("close")
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .f64()
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .get(0)
                    .unwrap_or(0.0);
                let volume = last
                    .column("volume")
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .f64()
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .get(0)
                    .unwrap_or(0.0);
                let ts_ms = last
                    .column("timestamp")
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .cast(&DataType::Int64)
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .i64()
                    .map_err(|e| BrokerError::DataFeedError(e.to_string()))?
                    .get(0)
                    .unwrap_or(0);
                let timestamp = Utc.timestamp_millis_opt(ts_ms).unwrap();

                Ok(Bar {
                    symbol: row_symbol,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    timestamp,
                })
            }
        }
    }

    async fn subscribe_to_data_stream(
        &self,
        symbols: Vec<String>,
        time_frame: TimeFrame,
        mode: DataStreamMode,
        on_bar: Arc<dyn Fn(Bar) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        // Keep one shared fetcher alive for all polling tasks created by this call.
        // reqwest::Client clones share the same connection pool and cookie store.
        let fetcher = Arc::new(YahooFinanceDataFeed {
            client: self.client.clone(),
            connected: self.connected.clone(),
            ticker_info: self.ticker_info.clone(),
            crumb: self.crumb.clone(),
            session_generation: self.session_generation.clone(),
            session_refresh_lock: self.session_refresh_lock.clone(),
            stream_manager: DataStreamManager::new(),
        });
        let fetch_fn: FetchBarsFn = Arc::new(move |symbol, start, end, tf| {
            let fetcher = fetcher.clone();
            Box::pin(async move { fetcher.fetch_bars_raw(&symbol, start, end, tf).await })
        });

        for symbol in symbols {
            self.stream_manager.subscribe(
                symbol,
                time_frame,
                mode,
                on_bar.clone(),
                fetch_fn.clone(),
            );
        }
        Ok(())
    }

    async fn unsubscribe_from_data_stream(&self, symbols: Vec<String>) -> Result<(), BrokerError> {
        for symbol in &symbols {
            self.stream_manager.unsubscribe(symbol);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[test]
    fn yahoo_retry_policy_only_refreshes_explicit_cookie_or_crumb_failures() {
        let invalid_crumb = r#"{"finance":{"result":null,"error":{"code":"Unauthorized","description":"Invalid Crumb"}}}"#;
        let invalid_cookie = r#"{"finance":{"result":null,"error":{"code":"Unauthorized","description":"Invalid Cookie"}}}"#;
        let entitlement = r#"{"finance":{"result":null,"error":{"code":"Unauthorized","description":"User is unable to access this feature"}}}"#;

        assert_eq!(
            YahooFinanceDataFeed::retry_decision(StatusCode::UNAUTHORIZED, invalid_crumb),
            YahooRetryDecision::RefreshSession
        );
        assert_eq!(
            YahooFinanceDataFeed::retry_decision(StatusCode::UNAUTHORIZED, invalid_cookie),
            YahooRetryDecision::RefreshSession
        );
        assert_eq!(
            YahooFinanceDataFeed::retry_decision(StatusCode::UNAUTHORIZED, entitlement),
            YahooRetryDecision::Stop
        );
        assert_eq!(
            YahooFinanceDataFeed::retry_decision(StatusCode::NOT_FOUND, "symbol not found"),
            YahooRetryDecision::Stop
        );
        assert_eq!(
            YahooFinanceDataFeed::retry_decision(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid period parameter",
            ),
            YahooRetryDecision::Stop
        );
    }

    #[test]
    fn yahoo_retry_policy_retries_throttling_and_transient_server_failures() {
        assert_eq!(
            YahooFinanceDataFeed::retry_decision(
                StatusCode::TOO_MANY_REQUESTS,
                "Edge: Too Many Requests",
            ),
            YahooRetryDecision::Retry
        );
        assert_eq!(
            YahooFinanceDataFeed::retry_decision(StatusCode::BAD_GATEWAY, "upstream error"),
            YahooRetryDecision::Retry
        );
        assert_eq!(
            YahooFinanceDataFeed::retry_delay(10, None),
            YAHOO_RETRY_MAX_DELAY.min(Duration::from_millis(
                YAHOO_RETRY_BASE_DELAY_MS * (1_u64 << 4)
            ))
        );
        assert_eq!(
            YahooFinanceDataFeed::retry_delay(0, Some(Duration::from_secs(60))),
            YAHOO_RETRY_MAX_DELAY
        );
    }

    #[tokio::test]
    async fn test_yahoo_connection() {
        let feed = YahooFinanceDataFeed::new();
        assert!(!feed.is_connected());

        let connected = feed.connect().await;
        if connected.is_err() {
            println!(
                "Connection failed (network issues known in CI/headless?): {:?}",
                connected.err()
            );
        } else {
            assert!(feed.is_connected());
            let disconnected = feed.disconnect().await;
            assert!(disconnected.is_ok());
            assert!(!feed.is_connected());
        }
    }

    #[tokio::test]
    async fn test_yahoo_ticker_info() {
        let feed = YahooFinanceDataFeed::new();
        let connected = feed.connect().await;
        if connected.is_err() {
            println!("Skipping test_yahoo_ticker_info due to connection failure");
            return;
        }

        let symbol = "AAPL";
        let info = feed.get_ticker_info(symbol).await;
        assert!(info.is_ok(), "Failed to get ticker info: {:?}", info.err());
        let asset = info.unwrap();
        assert_eq!(asset.symbol, "AAPL");
        assert!(asset.min_price_increment.is_some());
        assert_eq!(asset.exchange, AssetExchange::NASDAQ);
        println!("Asset Data: {:?}", asset);
    }

    #[tokio::test]
    async fn test_yahoo_history() {
        let feed = YahooFinanceDataFeed::new();
        let connected = feed.connect().await;
        if connected.is_err() {
            println!("Skipping test_yahoo_history due to connection failure");
            return;
        }

        let symbol = "AAPL";
        let end = Utc::now();
        let start = end - chrono::Duration::days(10);

        let history = feed
            .get_history(symbol, start, end, TimeFrame::new(1, TimeFrameUnit::Day))
            .await;

        assert!(
            history.is_ok(),
            "Failed to get history: {:?}",
            history.err()
        );
        let df = history.unwrap();
        assert!(df.height() > 0, "Expected at least one row in DataFrame");
        println!("DataFrame shape: {:?}", df.shape());
        println!("DataFrame:\n{}", df.head(Some(5)));
    }
}
