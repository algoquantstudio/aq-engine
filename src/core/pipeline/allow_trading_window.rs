use crate::core::insight::Insight;
use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;
use chrono::{DateTime, Datelike, Timelike, Utc};
use std::collections::HashSet;

use super::InsightPipe;

const DEFAULT_START_UTC: &str = "06:00";
const DEFAULT_END_UTC: &str = "20:00";
const DEFAULT_ALLOWED_DAYS: &str = "0,1,2,3,4,5,6";

/// Allows an insight to continue only inside a configured UTC trading window.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `start_utc`: Inclusive start time in `HH:MM` UTC format.
/// - `end_utc`: Inclusive end time in `HH:MM` UTC format.
/// - `allowed_days`: Comma-separated weekday numbers, where `0` is Monday and `6` is Sunday.
///
/// Behaviour:
/// Uses the strategy context's current time so backtests evaluate the historical bar time and
/// live strategies evaluate the runtime clock. Windows that cross midnight are supported. An
/// empty or invalid day list defaults to all seven days.
pub struct AllowTradingWindowPipe {
    pub start_utc: String,
    pub end_utc: String,
    pub allowed_days: String,
}

/// Compatibility conversion for older AQS graphs that stored weekdays as an array.
pub trait IntoAllowedDays {
    fn into_allowed_days(self) -> String;
}

impl IntoAllowedDays for String {
    fn into_allowed_days(self) -> String {
        self
    }
}

impl IntoAllowedDays for Vec<String> {
    fn into_allowed_days(self) -> String {
        self.join(",")
    }
}

impl IntoAllowedDays for &str {
    fn into_allowed_days(self) -> String {
        self.to_string()
    }
}

impl AllowTradingWindowPipe {
    pub fn new(start_utc: String, end_utc: String, allowed_days: impl IntoAllowedDays) -> Self {
        let allowed_days = allowed_days.into_allowed_days();
        Self {
            start_utc: normalize_time(&start_utc, DEFAULT_START_UTC),
            end_utc: normalize_time(&end_utc, DEFAULT_END_UTC),
            allowed_days: normalize_days(&allowed_days),
        }
    }

    /// Convenience constructor for a Monday-Friday trading window.
    pub fn weekdays(start_utc: &str, end_utc: &str) -> Self {
        Self::new(
            start_utc.to_string(),
            end_utc.to_string(),
            "0,1,2,3,4".to_string(),
        )
    }

    fn allowed_day_set(&self) -> HashSet<u32> {
        self.allowed_days
            .split(',')
            .filter_map(|item| item.trim().parse::<u32>().ok())
            .filter(|day| *day <= 6)
            .collect()
    }

    fn allows(&self, now: DateTime<Utc>) -> Result<(), String> {
        let weekday = now.weekday().num_days_from_monday();
        let allowed_days = self.allowed_day_set();
        if !allowed_days.is_empty() && !allowed_days.contains(&weekday) {
            return Err(format!("Trading not allowed on weekday {weekday}"));
        }

        let current_minutes = i64::from(now.hour() * 60 + now.minute());
        let start_minutes = parse_minutes(&self.start_utc).unwrap_or(360);
        let end_minutes = parse_minutes(&self.end_utc).unwrap_or(1200);
        let in_window = if start_minutes <= end_minutes {
            current_minutes >= start_minutes && current_minutes <= end_minutes
        } else {
            current_minutes >= start_minutes || current_minutes <= end_minutes
        };

        if in_window {
            Ok(())
        } else {
            Err(format!(
                "Outside trading window {}-{} UTC",
                self.start_utc, self.end_utc
            ))
        }
    }
}

impl InsightPipe for AllowTradingWindowPipe {
    fn version(&self) -> &str {
        "1.1"
    }

    fn run(&mut self, ctx: &mut dyn StrategyContext, _insight: &mut Insight) -> InsightPipeResult {
        self.start_utc = normalize_time(&self.start_utc, DEFAULT_START_UTC);
        self.end_utc = normalize_time(&self.end_utc, DEFAULT_END_UTC);
        self.allowed_days = normalize_days(&self.allowed_days);

        match self.allows(ctx.current_time()) {
            Ok(()) => InsightPipeResult::new(
                true,
                true,
                Some("Trading window passed".to_string()),
                self.name().to_string(),
            ),
            Err(reason) => {
                InsightPipeResult::new(false, true, Some(reason), self.name().to_string())
            }
        }
    }
}

fn normalize_time(value: &str, default: &str) -> String {
    parse_minutes(value)
        .map(|minutes| format!("{:02}:{:02}", minutes / 60, minutes % 60))
        .unwrap_or_else(|| default.to_string())
}

fn normalize_days(value: &str) -> String {
    let days = value
        .split(',')
        .filter_map(|item| item.trim().parse::<u32>().ok())
        .filter(|day| *day <= 6)
        .map(|day| day.to_string())
        .collect::<Vec<_>>();
    if days.is_empty() {
        DEFAULT_ALLOWED_DAYS.to_string()
    } else {
        days.join(",")
    }
}

fn parse_minutes(value: &str) -> Option<i64> {
    let (hour, minute) = value.trim().split_once(':')?;
    let hour = hour.parse::<i64>().ok()?;
    let minute = minute.parse::<i64>().ok()?;
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) {
        return None;
    }
    Some(hour * 60 + minute)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn normalizes_public_configuration_fields() {
        let pipe = AllowTradingWindowPipe::new(
            "6:05".to_string(),
            "invalid".to_string(),
            "1, 4, 8, invalid".to_string(),
        );

        assert_eq!(pipe.start_utc, "06:05");
        assert_eq!(pipe.end_utc, DEFAULT_END_UTC);
        assert_eq!(pipe.allowed_days, "1,4");
    }

    #[test]
    fn accepts_legacy_array_day_configuration() {
        let pipe = AllowTradingWindowPipe::new(
            "09:30".to_string(),
            "16:00".to_string(),
            vec!["0".to_string(), "2".to_string(), "4".to_string()],
        );

        assert_eq!(pipe.allowed_days, "0,2,4");
    }

    #[test]
    fn evaluates_the_supplied_strategy_time_and_allowed_day() {
        let pipe = AllowTradingWindowPipe::new(
            "09:30".to_string(),
            "16:00".to_string(),
            "0,1,2,3,4".to_string(),
        );

        let monday_open = Utc.with_ymd_and_hms(2026, 8, 31, 9, 30, 0).unwrap();
        let monday_before_open = Utc.with_ymd_and_hms(2026, 8, 31, 9, 29, 0).unwrap();
        let sunday = Utc.with_ymd_and_hms(2026, 8, 30, 12, 0, 0).unwrap();

        assert!(pipe.allows(monday_open).is_ok());
        assert!(pipe.allows(monday_before_open).is_err());
        assert!(pipe.allows(sunday).is_err());
    }

    #[test]
    fn supports_windows_that_cross_midnight() {
        let pipe = AllowTradingWindowPipe::new(
            "22:00".to_string(),
            "02:00".to_string(),
            DEFAULT_ALLOWED_DAYS.to_string(),
        );

        assert!(
            pipe.allows(Utc.with_ymd_and_hms(2026, 8, 30, 23, 0, 0).unwrap())
                .is_ok()
        );
        assert!(
            pipe.allows(Utc.with_ymd_and_hms(2026, 8, 31, 1, 0, 0).unwrap())
                .is_ok()
        );
        assert!(
            pipe.allows(Utc.with_ymd_and_hms(2026, 8, 31, 12, 0, 0).unwrap())
                .is_err()
        );
    }
}
