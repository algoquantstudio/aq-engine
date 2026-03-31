use chrono::{DateTime, Datelike, Months, TimeDelta, Timelike, Utc};
use serde::{Deserialize, Serialize};

/// Lightweight canonical `TimeFrameUnit` available on **all** targets (including wasm).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeFrameUnit {
    Second,
    Minute,
    Hour,
    Day,
    Month,
}

impl std::fmt::Display for TimeFrameUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Second => write!(f, "Second"),
            Self::Minute => write!(f, "Minute"),
            Self::Hour => write!(f, "Hour"),
            Self::Day => write!(f, "Day"),
            Self::Month => write!(f, "Month"),
        }
    }
}

impl TimeFrameUnit {
    pub fn variants() -> &'static [TimeFrameUnit] {
        &[
            TimeFrameUnit::Second,
            TimeFrameUnit::Minute,
            TimeFrameUnit::Hour,
            TimeFrameUnit::Day,
            TimeFrameUnit::Month,
        ]
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq)]
pub struct TimeFrame {
    amount: u8,
    unit: TimeFrameUnit,
}
#[derive(Debug, Clone)]

pub enum TimeFrameError {
    InvalidTimeFrameUnit,
    InvalidTimeFrameAmount,
    InvalidTimeFramePeriod,
}

impl TimeFrame {
    /// Creates a new `TimeFrame` instance.
    ///
    /// # Arguments
    ///
    /// * `amount` - The duration amount (e.g., 1, 5, 15).
    /// * `unit` - The time unit (e.g., Minute, Hour, Day).
    ///
    /// # Panics
    ///
    /// Panics if the provided `amount` and `unit` combination is invalid.
    pub fn new(amount: u8, unit: TimeFrameUnit) -> Self {
        Self::validate_time_frame(&amount, &unit).unwrap()
    }
    pub fn get_amount(&self) -> u8 {
        self.amount.clone()
    }
    pub fn get_unit(&self) -> TimeFrameUnit {
        self.unit.clone()
    }

    fn validate_time_frame(amount: &u8, unit: &TimeFrameUnit) -> Result<TimeFrame, TimeFrameError> {
        if *amount == 0 {
            return Err(TimeFrameError::InvalidTimeFrameAmount);
        }

        let is_valid = match unit {
            TimeFrameUnit::Second | TimeFrameUnit::Minute => *amount <= 59,
            TimeFrameUnit::Hour => *amount <= 23,
            TimeFrameUnit::Day | TimeFrameUnit::Month => *amount == 1,
            // | TimeFrameUnit::Week
        };

        if !is_valid {
            return Err(TimeFrameError::InvalidTimeFrameAmount);
        }

        Ok(Self {
            amount: *amount,
            unit: unit.clone(),
        })
    }

    /// Checks if a given time is valid for this timeframe.
    ///
    /// Returns `true` if the time aligns with the timeframe interval (e.g., 15-minute intervals check if minutes are 0, 15, 30, 45).
    pub fn is_interval_valid(&self, time: DateTime<Utc>) -> bool {
        match self.unit {
            TimeFrameUnit::Second => time.second() % self.amount as u32 == 0,
            TimeFrameUnit::Minute => time.minute() % self.amount as u32 == 0 && time.second() == 0,
            TimeFrameUnit::Hour => {
                time.hour() % self.amount as u32 == 0 && time.minute() == 0 && time.second() == 0
            }
            TimeFrameUnit::Day => {
                time.day() % self.amount as u32 == 0
                    && time.hour() == 0
                    && time.minute() == 0
                    && time.second() == 0
            }
            // TimeFrameUnit::Week => time.week() % self.amount == 0,
            TimeFrameUnit::Month => {
                time.month() % self.amount as u32 == 0
                    && time.day() == 1
                    && time.hour() == 0
                    && time.minute() == 0
                    && time.second() == 0
            }
        }
    }
    /// Adds a number of timeframe periods to a given time.
    ///
    /// # Arguments
    ///
    /// * `time` - The starting time.
    /// * `periods` - The number of periods to add (can be negative).
    ///
    /// # Returns
    ///
    /// Returns the new `DateTime<Utc>` or a `TimeFrameError` if the calculation fails.
    pub fn add_time_increment(
        &self,
        time: DateTime<Utc>,
        periods: i64,
    ) -> Result<DateTime<Utc>, TimeFrameError> {
        match self.unit {
            // TODO:  strip the ms from the time so we can get a round time.
            TimeFrameUnit::Second => {
                Ok(time + (TimeDelta::seconds((self.amount as i64) * periods)))
            }
            TimeFrameUnit::Minute => Ok(time + TimeDelta::minutes((self.amount as i64) * periods)),
            TimeFrameUnit::Hour => Ok(time + TimeDelta::hours((self.amount as i64) * periods)),
            TimeFrameUnit::Day => Ok(time + TimeDelta::days((self.amount as i64) * periods)),
            // TimeFrameUnit::Week => time + chrono::Duration::weeks(self.amount as i64 * periods),
            TimeFrameUnit::Month => {
                let total_months = (self.amount as i64) * periods;
                if total_months >= 0 {
                    time.checked_add_months(Months::new(total_months as u32))
                        .ok_or(TimeFrameError::InvalidTimeFramePeriod)
                } else {
                    time.checked_sub_months(Months::new((-total_months) as u32))
                        .ok_or(TimeFrameError::InvalidTimeFramePeriod)
                }
            }
        }
    }
    pub fn get_current_time_increment(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        match self.unit {
            TimeFrameUnit::Second => {
                time - (TimeDelta::seconds((time.second() % self.amount as u32) as i64)
                    + TimeDelta::nanoseconds(time.nanosecond() as i64))
            }
            TimeFrameUnit::Minute => {
                time - (TimeDelta::minutes((time.minute() % self.amount as u32) as i64)
                    + TimeDelta::seconds(time.second() as i64)
                    + TimeDelta::nanoseconds(time.nanosecond() as i64))
            }
            TimeFrameUnit::Hour => {
                time - (TimeDelta::hours((time.hour() % self.amount as u32) as i64)
                    + TimeDelta::minutes(time.minute() as i64)
                    + TimeDelta::seconds(time.second() as i64)
                    + TimeDelta::nanoseconds(time.nanosecond() as i64))
            }
            TimeFrameUnit::Day => {
                time - (TimeDelta::days((time.day() % self.amount as u32) as i64)
                    + TimeDelta::hours(time.hour() as i64)
                    + TimeDelta::minutes(time.minute() as i64)
                    + TimeDelta::seconds(time.second() as i64)
                    + TimeDelta::nanoseconds(time.nanosecond() as i64))
            }
            // TimeFrameUnit::Week => time - TimeDelta::weeks(time.week() % self.amount as u32 as i64),
            TimeFrameUnit::Month => {
                let time = time
                    .checked_sub_months(Months::new(time.month() % self.amount as u32))
                    .expect("Valid month calculation");

                time - (TimeDelta::days((time.day() - 1) as i64)
                    + TimeDelta::hours(time.hour() as i64)
                    + TimeDelta::minutes(time.minute() as i64)
                    + TimeDelta::seconds(time.second() as i64)
                    + TimeDelta::nanoseconds(time.nanosecond() as i64))
            }
        }
    }
    pub fn get_next_time_increment(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        match self.unit {
            TimeFrameUnit::Second => {
                time + TimeDelta::seconds(
                    (self.amount as i64) - (time.second() % self.amount as u32) as i64,
                ) - TimeDelta::nanoseconds(time.nanosecond() as i64)
            }
            TimeFrameUnit::Minute => {
                time + TimeDelta::minutes(
                    (self.amount as i64) - (time.minute() % self.amount as u32) as i64,
                ) - TimeDelta::seconds(time.second() as i64)
                    - TimeDelta::nanoseconds(time.nanosecond() as i64)
            }
            TimeFrameUnit::Hour => {
                time + TimeDelta::hours(
                    (self.amount as i64) - (time.hour() % self.amount as u32) as i64,
                ) - TimeDelta::minutes(time.minute() as i64)
                    - TimeDelta::seconds(time.second() as i64)
                    - TimeDelta::nanoseconds(time.nanosecond() as i64)
            }
            TimeFrameUnit::Day => {
                time + TimeDelta::days(
                    (self.amount as i64) - (time.day() % self.amount as u32) as i64,
                ) - TimeDelta::hours(time.hour() as i64)
                    - TimeDelta::minutes(time.minute() as i64)
                    - TimeDelta::seconds(time.second() as i64)
                    - TimeDelta::nanoseconds(time.nanosecond() as i64)
            }
            // TimeFrameUnit::Week => time + TimeDelta::weeks((self.amount as i64) - (time.week() % self.amount as u32) as i64),
            TimeFrameUnit::Month => {
                let time = time
                    .checked_add_months(Months::new(
                        (self.amount as u32) - (time.month() % self.amount as u32),
                    ))
                    .expect("Valid month calculation");

                time - TimeDelta::days((time.day() - 1) as i64)
                    - TimeDelta::hours(time.hour() as i64)
                    - TimeDelta::minutes(time.minute() as i64)
                    - TimeDelta::seconds(time.second() as i64)
                    - TimeDelta::nanoseconds(time.nanosecond() as i64)
            }
        }
    }
}

impl PartialEq for TimeFrame {
    fn eq(&self, other: &Self) -> bool {
        self.amount == other.amount && self.unit == other.unit
    }
}
impl Default for TimeFrame {
    fn default() -> Self {
        Self::new(1, TimeFrameUnit::Minute)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_new_valid_and_invalid() {
        // Valid cases
        let tf = TimeFrame::new(1, TimeFrameUnit::Minute);
        assert_eq!(tf.get_amount(), 1);

        let tf = TimeFrame::new(5, TimeFrameUnit::Minute);
        assert_eq!(tf.get_amount(), 5);

        let tf = TimeFrame::new(1, TimeFrameUnit::Hour);
        assert_eq!(tf.get_amount(), 1);

        // Invalid cases - should panic as per current implementation of new()
        // Ideally new() should return Result, but it unwrap()s internally.
        // We can use should_panic
    }

    #[test]
    #[should_panic]
    fn test_new_invalid_amount() {
        TimeFrame::new(0, TimeFrameUnit::Minute);
    }

    #[test]
    fn test_eq() {
        let tf1 = TimeFrame::new(1, TimeFrameUnit::Minute);
        let tf2 = TimeFrame::new(1, TimeFrameUnit::Minute);
        let tf3 = TimeFrame::new(2, TimeFrameUnit::Minute);

        assert_eq!(tf1, tf2);
        assert_ne!(tf1, tf3);

        let tf4 = TimeFrame::new(12, TimeFrameUnit::Hour);
        assert_ne!(tf1, tf4);

        let tf5 = TimeFrame::new(12, TimeFrameUnit::Hour);
        assert_eq!(tf4, tf5);
    }

    #[test]
    #[should_panic]
    fn test_new_invalid_minute_amount() {
        TimeFrame::new(60, TimeFrameUnit::Minute);
    }

    #[test]
    fn test_is_interval_valid() {
        let tf_15m = TimeFrame::new(15, TimeFrameUnit::Minute);

        let valid_time = Utc.with_ymd_and_hms(2023, 1, 1, 12, 15, 0).unwrap();
        let invalid_time = Utc.with_ymd_and_hms(2023, 1, 1, 12, 16, 0).unwrap();

        assert!(tf_15m.is_interval_valid(valid_time));
        assert!(!tf_15m.is_interval_valid(invalid_time));

        let tf_1h = TimeFrame::new(1, TimeFrameUnit::Hour);
        let valid_hour = Utc.with_ymd_and_hms(2023, 1, 1, 13, 0, 0).unwrap();
        let invalid_hour = Utc.with_ymd_and_hms(2023, 1, 1, 13, 30, 0).unwrap();

        assert!(tf_1h.is_interval_valid(valid_hour));
        assert!(!tf_1h.is_interval_valid(invalid_hour));
    }

    #[test]
    fn test_add_time_increment() {
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 12, 0, 0).unwrap();

        // Minutes
        let tf_15m = TimeFrame::new(15, TimeFrameUnit::Minute);
        let new_time = tf_15m.add_time_increment(base_time, 2).unwrap();
        assert_eq!(
            new_time,
            Utc.with_ymd_and_hms(2023, 1, 1, 12, 30, 0).unwrap()
        );

        let prev_time = tf_15m.add_time_increment(base_time, -1).unwrap();
        assert_eq!(
            prev_time,
            Utc.with_ymd_and_hms(2023, 1, 1, 11, 45, 0).unwrap()
        );

        // Days
        let tf_1d = TimeFrame::new(1, TimeFrameUnit::Day);
        let next_day = tf_1d.add_time_increment(base_time, 1).unwrap();
        assert_eq!(
            next_day,
            Utc.with_ymd_and_hms(2023, 1, 2, 12, 0, 0).unwrap()
        );

        // Months
        let tf_1mo = TimeFrame::new(1, TimeFrameUnit::Month);
        let next_month = tf_1mo.add_time_increment(base_time, 1).unwrap();
        assert_eq!(
            next_month,
            Utc.with_ymd_and_hms(2023, 2, 1, 12, 0, 0).unwrap()
        );

        let prev_month = tf_1mo.add_time_increment(base_time, -1).unwrap();
        assert_eq!(
            prev_month,
            Utc.with_ymd_and_hms(2022, 12, 1, 12, 0, 0).unwrap()
        );
    }

    #[test]
    fn test_get_current_time_increment() {
        // 15-minute timeframe
        let tf_15m = TimeFrame::new(15, TimeFrameUnit::Minute);
        let time = Utc.with_ymd_and_hms(2023, 1, 1, 12, 7, 30).unwrap();
        let snapped = tf_15m.get_current_time_increment(time);
        assert_eq!(snapped, Utc.with_ymd_and_hms(2023, 1, 1, 12, 0, 0).unwrap());

        let time2 = Utc.with_ymd_and_hms(2023, 1, 1, 12, 22, 45).unwrap();
        let snapped2 = tf_15m.get_current_time_increment(time2);
        assert_eq!(
            snapped2,
            Utc.with_ymd_and_hms(2023, 1, 1, 12, 15, 0).unwrap()
        );

        // 1-hour timeframe
        let tf_1h = TimeFrame::new(1, TimeFrameUnit::Hour);
        let time_h = Utc.with_ymd_and_hms(2023, 1, 1, 12, 45, 0).unwrap();
        let snapped_h = tf_1h.get_current_time_increment(time_h);
        assert_eq!(
            snapped_h,
            Utc.with_ymd_and_hms(2023, 1, 1, 12, 0, 0).unwrap()
        );

        // 1-day timeframe
        let tf_1d = TimeFrame::new(1, TimeFrameUnit::Day);
        let time_d = Utc.with_ymd_and_hms(2023, 1, 1, 15, 30, 0).unwrap();
        let snapped_d = tf_1d.get_current_time_increment(time_d);
        assert_eq!(
            snapped_d,
            Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()
        ); // Day starts at 00:00

        // 1-month timeframe
        let tf_1mo = TimeFrame::new(1, TimeFrameUnit::Month);
        let time_mo = Utc.with_ymd_and_hms(2023, 2, 15, 10, 0, 0).unwrap();
        let snapped_mo = tf_1mo.get_current_time_increment(time_mo);
        assert_eq!(
            snapped_mo,
            Utc.with_ymd_and_hms(2023, 2, 1, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn test_get_next_time_increment() {
        // 15-minute timeframe
        let tf_15m = TimeFrame::new(15, TimeFrameUnit::Minute);
        let time = Utc.with_ymd_and_hms(2023, 1, 1, 12, 7, 30).unwrap();
        let next = tf_15m.get_next_time_increment(time);
        assert_eq!(next, Utc.with_ymd_and_hms(2023, 1, 1, 12, 15, 0).unwrap());

        let time2 = Utc.with_ymd_and_hms(2023, 1, 1, 12, 22, 45).unwrap();
        let next2 = tf_15m.get_next_time_increment(time2);
        assert_eq!(next2, Utc.with_ymd_and_hms(2023, 1, 1, 12, 30, 0).unwrap());

        // 1-hour timeframe
        let tf_1h = TimeFrame::new(1, TimeFrameUnit::Hour);
        let time_h = Utc.with_ymd_and_hms(2023, 1, 1, 12, 45, 0).unwrap();
        let next_h = tf_1h.get_next_time_increment(time_h);
        assert_eq!(next_h, Utc.with_ymd_and_hms(2023, 1, 1, 13, 0, 0).unwrap());

        // 1-day timeframe
        let tf_1d = TimeFrame::new(1, TimeFrameUnit::Day);
        let time_d = Utc.with_ymd_and_hms(2023, 1, 1, 15, 30, 0).unwrap();
        let next_d = tf_1d.get_next_time_increment(time_d);
        assert_eq!(next_d, Utc.with_ymd_and_hms(2023, 1, 2, 0, 0, 0).unwrap());

        // 1-month timeframe
        let tf_1mo = TimeFrame::new(1, TimeFrameUnit::Month);
        let time_mo = Utc.with_ymd_and_hms(2023, 2, 15, 10, 0, 0).unwrap();
        let next_mo = tf_1mo.get_next_time_increment(time_mo);
        assert_eq!(next_mo, Utc.with_ymd_and_hms(2023, 3, 1, 0, 0, 0).unwrap());
    }
}
