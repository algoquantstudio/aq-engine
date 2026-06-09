use crate::core::insight::Insight;

use crate::core::pipeline::InsightPipeResult;
use crate::core::strategy::StrategyContext;
use std::collections::HashSet;

use super::InsightPipe;

/// Allows an insight to continue only inside a configured UTC trading window.
///
/// Author: @isaac-diaby
///
/// Inputs:
/// - `start`: Inclusive start time in `HH:MM` UTC format.
/// - `end`: Inclusive end time in `HH:MM` UTC format.
/// - `days`: Optional weekday numbers as strings, where `0` is Monday and `6` is Sunday.
///
/// Behaviour:
/// Parses the configured time window once in `new`, then checks the current UTC weekday and
/// minute during `run`. If `days` is empty, all weekdays are allowed. The pipe returns
/// `passed=false` when the current day or time is outside the configured window, without
/// mutating the insight.
pub struct AllowTradingWindowPipe {
    /// Start hour (0-23)
    start_hour: u32,
    /// Start minute (0-59)
    start_minute: u32,
    /// End hour (0-23)
    end_hour: u32,
    /// End minute (0-59)
    end_minute: u32,
    /// Allowed weekdays (0=Monday .. 6=Sunday).
    /// If empty, all days are allowed.
    allowed_days: HashSet<u32>,
}

impl AllowTradingWindowPipe {
    /// Create a new trading window pipe.
    ///
    /// `start` and `end` are in "HH:MM" format.
    /// `days` is a set of weekday strings (e.g., "0" for Monday .. "6" for Sunday).
    pub fn new(start: String, end: String, days: Vec<String>) -> Self {
        let (sh, sm) = parse_hm(&start);
        let (eh, em) = parse_hm(&end);

        let allowed_days: HashSet<u32> = days
            .iter()
            .filter_map(|d| d.parse::<u32>().ok())
            .filter(|&d| d <= 6)
            .collect();

        Self {
            start_hour: sh,
            start_minute: sm,
            end_hour: eh,
            end_minute: em,
            allowed_days,
        }
    }

    /// Convenience: allow Monday–Friday, any time window.
    pub fn weekdays(start: &str, end: &str) -> Self {
        let default_days = vec![
            "0".to_string(),
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
            "4".to_string(),
        ];
        Self::new(start.to_string(), end.to_string(), default_days)
    }
}

impl InsightPipe for AllowTradingWindowPipe {
    fn version(&self) -> &str {
        "1.0"
    }

    fn run(&mut self, _ctx: &mut dyn StrategyContext, _insight: &mut Insight) -> InsightPipeResult {
        use chrono::{Datelike, Timelike, Utc};
        let now = Utc::now();
        let hour = now.hour();
        let minute = now.minute();
        let weekday = now.weekday().num_days_from_monday(); // 0=Mon..6=Sun

        // Check day
        if !self.allowed_days.is_empty() && !self.allowed_days.contains(&weekday) {
            return InsightPipeResult::new(
                false,
                true,
                Some(format!("Trading not allowed on weekday {}", weekday)),
                self.name().to_string(),
            );
        }

        // Check time window
        let current_minutes = hour * 60 + minute;
        let start_minutes = self.start_hour * 60 + self.start_minute;
        let end_minutes = self.end_hour * 60 + self.end_minute;

        if current_minutes >= start_minutes && current_minutes <= end_minutes {
            InsightPipeResult::new(
                true,
                true,
                Some("Trading allowed".to_string()),
                self.name().to_string(),
            )
        } else {
            InsightPipeResult::new(
                false,
                true,
                Some(format!(
                    "Outside trading window {:02}:{:02}-{:02}:{:02}",
                    self.start_hour, self.start_minute, self.end_hour, self.end_minute
                )),
                self.name().to_string(),
            )
        }
    }
}

/// Parse "HH:MM" into (hour, minute).
fn parse_hm(s: &str) -> (u32, u32) {
    let parts: Vec<&str> = s.split(':').collect();
    let h = parts.first().and_then(|p| p.parse().ok()).unwrap_or(0);
    let m = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
    (h, m)
}
