//! Output deadband filter for derived tags.
//!
//! Prevents writing derived tag results that differ insignificantly from
//! the previous value. This reduces storage churn and WAL traffic for
//! slowly-changing derived values.
//!
//! # Filter Logic
//!
//! A new value passes the deadband filter if ANY of these conditions are true:
//! 1. Absolute change exceeds `abs` threshold.
//! 2. Relative change exceeds `rel` threshold.
//! 3. Time since last write exceeds `max_time_secs` (forced write).
//!
//! All thresholds default to 0.0 (disabled).

use serde::{Deserialize, Serialize};

// ─── DeadbandConfig ───

/// Configuration for the deadband output filter.
///
/// 派生 `Serialize/Deserialize` 用于持久化到 `derived_tags.deadband_json`。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeadbandConfig {
    /// Absolute change threshold. A new value passes if |new - old| > abs.
    pub abs: f64,
    /// Relative change threshold. A new value passes if |new - old| / |old| > rel.
    /// Ignored if `old == 0.0`.
    pub rel: f64,
    /// Maximum silence time in seconds. A write is forced after this duration
    /// even if the change thresholds aren't met.
    pub max_time_secs: u32,
}

impl Default for DeadbandConfig {
    fn default() -> Self {
        Self {
            abs: 0.0,
            rel: 0.0,
            max_time_secs: 0,
        }
    }
}

impl DeadbandConfig {
    /// Create a deadband config with only an absolute threshold.
    pub fn absolute(abs: f64) -> Self {
        Self {
            abs,
            rel: 0.0,
            max_time_secs: 0,
        }
    }

    /// Create a deadband config with absolute and relative thresholds.
    pub fn with_relative(abs: f64, rel: f64) -> Self {
        Self {
            abs,
            rel,
            max_time_secs: 0,
        }
    }

    /// Check if deadband filtering is enabled.
    pub fn is_enabled(&self) -> bool {
        self.abs > 0.0 || self.rel > 0.0
    }
}

// ─── DeadbandResult ───

/// Result of a deadband check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadbandResult {
    /// The new value passes the filter; it should be written.
    Write,
    /// The new value is within the deadband; it should be suppressed.
    Suppress,
}

// ─── check_deadband ───

/// Check whether a new value should be written or suppressed based on deadband rules.
///
/// # Arguments
/// * `new_value` - The newly computed value.
/// * `old_value` - The previously written value.
/// * `old_ts` - Epoch seconds of the previous write.
/// * `now` - Current epoch seconds.
/// * `config` - Deadband configuration.
///
/// # Returns
/// [`DeadbandResult::Write`] if the value should be written,
/// [`DeadbandResult::Suppress`] if it should be skipped.
pub fn check_deadband(
    new_value: f64,
    old_value: f64,
    old_ts: i64,
    now: i64,
    config: &DeadbandConfig,
) -> DeadbandResult {
    // NaN always passes (it's a significant event)
    if new_value.is_nan() {
        return DeadbandResult::Write;
    }

    // First write: always pass
    if old_ts == 0 {
        return DeadbandResult::Write;
    }

    // Forced write due to max silence time
    if config.max_time_secs > 0 {
        let elapsed = (now - old_ts) as u32;
        if elapsed >= config.max_time_secs {
            return DeadbandResult::Write;
        }
    }

    // Absolute threshold check
    if config.abs > 0.0 {
        let delta = (new_value - old_value).abs();
        if delta > config.abs {
            return DeadbandResult::Write;
        }
    }

    // Relative threshold check
    if config.rel > 0.0 && old_value != 0.0 {
        let relative_change = (new_value - old_value).abs() / old_value.abs();
        if relative_change > config.rel {
            return DeadbandResult::Write;
        }
    }

    // If thresholds are all 0.0, always write (no filtering)
    if config.abs == 0.0 && config.rel == 0.0 && config.max_time_secs == 0 {
        return DeadbandResult::Write;
    }

    DeadbandResult::Suppress
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_filter_writes() {
        let config = DeadbandConfig::default();
        let result = check_deadband(100.0, 100.0, 1000, 1001, &config);
        assert_eq!(result, DeadbandResult::Write);
    }

    #[test]
    fn test_first_write_always_passes() {
        let config = DeadbandConfig::absolute(0.01);
        let result = check_deadband(100.0, 0.0, 0, 1000, &config);
        assert_eq!(result, DeadbandResult::Write);
    }

    #[test]
    fn test_nan_always_passes() {
        let config = DeadbandConfig::absolute(1000.0);
        let result = check_deadband(f64::NAN, 100.0, 1000, 1001, &config);
        assert_eq!(result, DeadbandResult::Write);
    }

    #[test]
    fn test_absolute_threshold_pass() {
        let config = DeadbandConfig::absolute(0.5);
        let result = check_deadband(101.0, 100.0, 1000, 1001, &config);
        assert_eq!(result, DeadbandResult::Write);
    }

    #[test]
    fn test_absolute_threshold_suppress() {
        let config = DeadbandConfig::absolute(0.5);
        let result = check_deadband(100.1, 100.0, 1000, 1001, &config);
        assert_eq!(result, DeadbandResult::Suppress);
    }

    #[test]
    fn test_relative_threshold_pass() {
        let config = DeadbandConfig::with_relative(0.0, 0.01); // 1%
        let result = check_deadband(102.0, 100.0, 1000, 1001, &config);
        assert_eq!(result, DeadbandResult::Write);
    }

    #[test]
    fn test_relative_threshold_suppress() {
        let config = DeadbandConfig::with_relative(0.0, 0.01); // 1%
        let result = check_deadband(100.5, 100.0, 1000, 1001, &config);
        assert_eq!(result, DeadbandResult::Suppress);
    }

    #[test]
    fn test_max_time_force_write() {
        let config = DeadbandConfig {
            abs: 1000.0, // huge threshold that would suppress
            rel: 0.0,
            max_time_secs: 60,
        };
        // 100 seconds elapsed → force write
        let result = check_deadband(100.1, 100.0, 1000, 1100, &config);
        assert_eq!(result, DeadbandResult::Write);
    }

    #[test]
    fn test_max_time_not_exceeded() {
        let config = DeadbandConfig {
            abs: 1000.0,
            rel: 0.0,
            max_time_secs: 60,
        };
        // Only 10 seconds elapsed → suppress
        let result = check_deadband(100.1, 100.0, 1000, 1010, &config);
        assert_eq!(result, DeadbandResult::Suppress);
    }
}
