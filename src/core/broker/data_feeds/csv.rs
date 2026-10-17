//! Validated, immutable historical bars from a local CSV. Never supplies live quotes.
//!
//! Required headers (case insensitive, any order):
//! symbol,timeframe,timestamp,open,high,low,close,volume
//! Timestamps are bar OPEN times: RFC3339, Unix seconds/milliseconds, or UTC
//! `YYYY-MM-DD HH:MM:SS`. History requests use [start, end); no resampling or
//! forward filling is performed. Each symbol/timeframe/timestamp must be unique.

use crate::core::broker::traits::{DataFeed, DataProvider};
use crate::core::broker::types::{
    Asset, AssetExchange, AssetStatus, AssetType, Bar, BarData, BrokerError, Quote,
};
use crate::core::broker::{DataFeedFeature, DataStreamMode};
use crate::core::utils::timeframe::TimeFrame;
use chrono::{DateTime, NaiveDateTime, Utc};
use polars::prelude::DataFrame;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub const CSV_PATH_ENV: &str = "AQE_CSV_PATH";

pub struct CsvDataFeed {
    path: PathBuf,
    connected: AtomicBool,
    data: tokio::sync::Mutex<Option<Arc<CsvDataset>>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CsvStreamSummary {
    pub symbol: String,
    pub timeframe: String,
    pub bars: usize,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Debug)]
struct CsvDataset {
    streams: HashMap<(String, TimeFrame), Vec<Bar>>,
}

fn error(message: impl Into<String>) -> BrokerError {
    BrokerError::DataFeedError(message.into())
}

fn historical_only() -> BrokerError {
    error("CSV is a historical-only data feed; live quotes and streaming are not supported")
}

impl CsvDataFeed {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            connected: AtomicBool::new(false),
            data: tokio::sync::Mutex::new(None),
        }
    }

    pub fn from_env() -> Result<Self, BrokerError> {
        let path = std::env::var_os(CSV_PATH_ENV)
            .filter(|path| !path.is_empty())
            .ok_or_else(|| error(format!("{CSV_PATH_ENV} must name a historical CSV file")))?;
        Ok(Self::new(PathBuf::from(path)))
    }

    async fn snapshot(&self) -> Result<Arc<CsvDataset>, BrokerError> {
        let mut data = self.data.lock().await;
        if let Some(data) = data.as_ref() {
            return Ok(data.clone());
        }
        let path = self.path.clone();
        let loaded = tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(&path)
                .map_err(|e| error(format!("Cannot open CSV '{}': {e}", path.display())))?;
            CsvDataset::read(file)
        })
        .await
        .map_err(|e| error(format!("CSV loading failed: {e}")))??;
        let loaded = Arc::new(loaded);
        *data = Some(loaded.clone());
        self.connected.store(true, Ordering::Release);
        Ok(loaded)
    }

    pub async fn summary(&self) -> Result<Vec<CsvStreamSummary>, BrokerError> {
        let data = self.snapshot().await?;
        let mut summaries: Vec<_> = data
            .streams
            .iter()
            .map(|((symbol, tf), bars)| CsvStreamSummary {
                symbol: symbol.clone(),
                timeframe: tf.compact_label(),
                bars: bars.len(),
                start: bars[0].timestamp,
                end: bars[bars.len() - 1].timestamp,
            })
            .collect();
        summaries.sort_by(|a, b| (&a.symbol, &a.timeframe).cmp(&(&b.symbol, &b.timeframe)));
        Ok(summaries)
    }
}

impl DataFeed for CsvDataFeed {
    async fn connect(&self) -> Result<bool, BrokerError> {
        self.snapshot().await?;
        Ok(true)
    }

    async fn disconnect(&self) -> Result<bool, BrokerError> {
        let mut data = self.data.lock().await;
        *data = None;
        self.connected.store(false, Ordering::Release);
        Ok(true)
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn get_features(&self) -> &'static [DataFeedFeature] {
        &[DataFeedFeature::HistoricalCandles]
    }
}

impl DataProvider for CsvDataFeed {
    async fn supports_historical_stream(
        &self,
        symbol: &str,
        timeframe: TimeFrame,
    ) -> Result<bool, BrokerError> {
        Ok(self
            .snapshot()
            .await?
            .streams
            .contains_key(&(symbol.to_string(), timeframe)))
    }

    async fn get_ticker_info(&self, symbol: &str) -> Result<Asset, BrokerError> {
        let data = self.snapshot().await?;
        let available = data.streams.keys().any(|(name, _)| name == symbol);
        // OHLCV does not encode broker contract specifications. Use explicit
        // generic unit-based simulation defaults, not guessed MT5 lot sizes.
        Ok(Asset {
            id: format!("csv:{symbol}"),
            symbol: symbol.into(),
            name: symbol.into(),
            asset_type: AssetType::UNKNOWN("CSV".into()),
            status: if available {
                AssetStatus::Active
            } else {
                AssetStatus::Inactive
            },
            exchange: AssetExchange::UNKNOWN("CSV".into()),
            tradable: available,
            marginable: true,
            shortable: true,
            fractional: true,
            min_order_size: Some(0.00000001),
            quantity_base: Some(8),
            max_order_size: None,
            max_total_size: None,
            min_price_increment: Some(0.00000001),
            price_base: Some(8),
            contract_size: Some(1),
            trading_calendar: None,
            fees: Default::default(),
        })
    }

    async fn get_history(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        timeframe: TimeFrame,
    ) -> Result<DataFrame, BrokerError> {
        if start >= end {
            return Err(error("CSV history start must be before end"));
        }
        let data = self.snapshot().await?;
        let Some(bars) = data.streams.get(&(symbol.to_string(), timeframe)) else {
            log::warn!(
                "Skipping CSV stream {symbol} {}: no exact timeframe data; no substitution or resampling",
                timeframe.compact_label()
            );
            return self.format_on_bar(BarData::Bars(Vec::new())).map_err(error);
        };
        let first = bars.partition_point(|bar| bar.timestamp < start);
        let last = bars.partition_point(|bar| bar.timestamp < end);
        self.format_on_bar(BarData::Bars(bars[first..last].to_vec()))
            .map_err(error)
    }

    async fn get_latest_quote(&self, _symbol: &str) -> Result<Quote, BrokerError> {
        Err(historical_only())
    }

    async fn get_latest_bar(&self, _symbol: &str) -> Result<Bar, BrokerError> {
        Err(historical_only())
    }

    async fn subscribe_to_data_stream(
        &self,
        _symbols: Vec<String>,
        _timeframe: TimeFrame,
        _mode: DataStreamMode,
        _on_bar: Arc<dyn Fn(Bar) + Send + Sync>,
    ) -> Result<(), BrokerError> {
        Err(historical_only())
    }

    async fn unsubscribe_from_data_stream(&self, _symbols: Vec<String>) -> Result<(), BrokerError> {
        Ok(())
    }

    fn configure_live_session(&self, _session_id: &str) -> Result<(), BrokerError> {
        Err(historical_only())
    }
}

impl CsvDataset {
    fn read(input: impl Read) -> Result<Self, BrokerError> {
        let mut reader = ::csv::ReaderBuilder::new()
            .trim(::csv::Trim::All)
            .from_reader(input);
        let headers: Vec<String> = reader
            .headers()
            .map_err(|e| error(e.to_string()))?
            .iter()
            .map(|h| h.trim_start_matches('\u{feff}').to_ascii_lowercase())
            .collect();
        let required = [
            "symbol",
            "timeframe",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ];
        let mut indices = Vec::new();
        for name in required {
            let matches: Vec<_> = headers
                .iter()
                .enumerate()
                .filter(|(_, h)| h.as_str() == name)
                .collect();
            if matches.len() != 1 {
                return Err(error(format!(
                    "CSV must contain exactly one '{name}' header"
                )));
            }
            indices.push(matches[0].0);
        }
        let mut streams: HashMap<(String, TimeFrame), Vec<Bar>> = HashMap::new();
        let mut seen = HashSet::new();
        for record in reader.records() {
            let record = record.map_err(|e| error(format!("Invalid CSV: {e}")))?;
            let line = record.position().map(|p| p.line()).unwrap_or(0);
            let fail = |message: String| error(format!("CSV line {line}: {message}"));
            let values: Vec<_> = indices.iter().map(|&i| &record[i]).collect();
            let symbol = values[0];
            if symbol.is_empty() || symbol.chars().any(char::is_control) {
                return Err(fail(
                    "symbol must be nonempty and contain no control characters".into(),
                ));
            }
            let timeframe = TimeFrame::parse_timeframe(values[1]).map_err(|_| {
                fail(format!(
                    "invalid timeframe '{}'; use AQE labels such as 1m, 5m, 15m, 1h, 1d or 1mo",
                    values[1]
                ))
            })?;
            let timestamp = parse_timestamp(values[2]).map_err(&fail)?;
            let mut numbers = Vec::new();
            for (i, value) in values[3..].iter().enumerate() {
                let number = value
                    .parse::<f64>()
                    .map_err(|_| fail(format!("invalid {}: '{value}'", required[i + 3])))?;
                if !number.is_finite() || (i < 4 && number <= 0.0) || (i == 4 && number < 0.0) {
                    return Err(fail(format!(
                        "{} must be finite and {}",
                        required[i + 3],
                        if i == 4 { "nonnegative" } else { "positive" }
                    )));
                }
                numbers.push(number);
            }
            let (open, high, low, close, volume) =
                (numbers[0], numbers[1], numbers[2], numbers[3], numbers[4]);
            if low > high || open < low || open > high || close < low || close > high {
                return Err(fail(
                    "OHLC prices must satisfy low <= open/close <= high".into(),
                ));
            }
            if !seen.insert((symbol.to_string(), timeframe, timestamp)) {
                return Err(fail(format!(
                    "duplicate bar for {symbol} {} at {timestamp}",
                    timeframe.compact_label()
                )));
            }
            streams
                .entry((symbol.to_string(), timeframe))
                .or_default()
                .push(Bar {
                    symbol: symbol.to_string(),
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    volume,
                });
        }
        if streams.is_empty() {
            return Err(error("CSV contains no bars"));
        }
        for bars in streams.values_mut() {
            bars.sort_unstable_by_key(|bar| bar.timestamp);
        }
        Ok(Self { streams })
    }
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, String> {
    let parsed = if let Ok(number) = value.parse::<i64>() {
        // Contemporary epoch values: seconds have <=10 digits, milliseconds >=12.
        if number.unsigned_abs() < 100_000_000_000 {
            DateTime::from_timestamp(number, 0)
        } else {
            DateTime::from_timestamp_millis(number)
        }
    } else if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        Some(dt.with_timezone(&Utc))
    } else {
        NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
            .ok()
            .map(|dt| dt.and_utc())
    };
    parsed.filter(|dt| dt.timestamp_subsec_nanos() % 1_000_000 == 0)
        .ok_or_else(|| format!("invalid timestamp '{value}'; use RFC3339, UTC YYYY-MM-DD HH:MM:SS, or Unix seconds/milliseconds (millisecond precision)"))
}

#[cfg(test)]
mod tests;
