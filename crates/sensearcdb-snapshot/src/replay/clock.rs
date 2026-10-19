//! 回放时钟 — 速度控制与耗时补偿。
//!
//! ## 速度控制
//!
//! 支持任意倍速（0.5x/1.5x/2.5x/100x 等，f64 直接存储不取整）。
//! 实际间隔 = 数据点时间间隔 / speed。
//!
//! ## 耗时补偿（Pacing）
//!
//! 每处理完一个数据点后，计算实际耗时与期望间隔的差值。
//! 如果处理过快，sleep 差值；如果处理过慢（赶不上），跳过 sleep。
//!
//! ## 回放速度 <= 0 的特殊语义
//!
//! `speed <= 0`（含负数）表示 **最大速度**（无 pacing），尽快处理所有数据点。

use std::time::{Duration, Instant};
use tokio::time;

/// 回放速度（任意倍速 newtype）。
///
/// - `speed <= 0.0` → 最大速度（无 pacing）
/// - `speed > 0.0` → 任意倍速（0.5x/1x/2.5x/100x），直接存储不取整
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReplaySpeed(pub f64);

impl ReplaySpeed {
    /// 从 multiplier 创建回放速度（直接存储，不取整）。
    pub fn from_multiplier(multiplier: f64) -> Self {
        Self(multiplier)
    }

    /// 返回速度倍率。
    ///
    /// `<= 0.0` 时调用方应跳过 pacing（负值与 0 同视为最大速度）。
    pub fn multiplier(&self) -> f64 {
        self.0
    }

    /// 是否启用 pacing（`speed > 0.0`）。
    pub fn is_pacing_enabled(&self) -> bool {
        self.0 > 0.0
    }
}

impl std::fmt::Display for ReplaySpeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 <= 0.0 {
            write!(f, "max")
        } else {
            write!(f, "{}x", self.0)
        }
    }
}

/// 回放时钟控制器。
///
/// 跟踪当前回放虚拟时间和实际耗时，
/// 通过 pacing 机制保证回放速度与目标速度一致。
#[derive(Debug)]
pub struct ReplayClock {
    /// 回放速度。
    speed: ReplaySpeed,
    /// 当前虚拟时间（已回放到的数据点时间戳，epoch ns）。
    current_ts: i64,
    /// 上一数据点处理完成的实际时刻。
    last_tick: Instant,
    /// 累计 pacing sleep 时间 (ns)。
    total_pacing_ns: u64,
}

impl ReplayClock {
    /// 创建回放时钟。
    ///
    /// # Arguments
    ///
    /// - `speed`: 回放速度。
    /// - `start_ts`: 回放起始虚拟时间（epoch ns）。
    pub fn new(speed: ReplaySpeed, start_ts: i64) -> Self {
        Self {
            speed,
            current_ts: start_ts,
            last_tick: Instant::now(),
            total_pacing_ns: 0,
        }
    }

    /// 推进虚拟时间到目标时间戳，执行 pacing。
    ///
    /// 计算上一数据点到当前数据点的期望间隔（除以 speed），
    /// 减去实际耗时，sleep 剩余差值。
    ///
    /// 如果实际耗时超过期望间隔（处理太慢），跳过 sleep。
    ///
    /// ## 最大速度模式
    ///
    /// 当 `speed <= 0.0` 时，跳过所有 pacing。
    pub async fn advance_to(&mut self, target_ts: i64) {
        if !self.speed.is_pacing_enabled() {
            self.current_ts = target_ts;
            return;
        }

        let elapsed_real = self.last_tick.elapsed();
        let time_delta_ns = (target_ts - self.current_ts).max(0) as u64;
        let speed = self.speed.multiplier();
        let expected_interval_ns = (time_delta_ns as f64 / speed) as u64;

        if (elapsed_real.as_nanos() as u64) < expected_interval_ns {
            let sleep_ns = expected_interval_ns - elapsed_real.as_nanos() as u64;
            self.total_pacing_ns += sleep_ns;
            time::sleep(Duration::from_nanos(sleep_ns)).await;
        }

        self.current_ts = target_ts;
        self.last_tick = Instant::now();
    }

    /// 同步版本：推进虚拟时间（用于非 async 上下文）。
    pub fn advance_to_sync(&mut self, target_ts: i64) {
        if !self.speed.is_pacing_enabled() {
            self.current_ts = target_ts;
            return;
        }

        let elapsed_real = self.last_tick.elapsed();
        let time_delta_ns = (target_ts - self.current_ts).max(0) as u64;
        let speed = self.speed.multiplier();
        let expected_interval_ns = (time_delta_ns as f64 / speed) as u64;

        if (elapsed_real.as_nanos() as u64) < expected_interval_ns {
            let sleep_ns = expected_interval_ns - elapsed_real.as_nanos() as u64;
            self.total_pacing_ns += sleep_ns;
            std::thread::sleep(Duration::from_nanos(sleep_ns));
        }

        self.current_ts = target_ts;
        self.last_tick = Instant::now();
    }

    /// 返回当前虚拟时间。
    pub fn current_ts(&self) -> i64 {
        self.current_ts
    }

    /// 返回回放速度。
    pub fn speed(&self) -> ReplaySpeed {
        self.speed
    }

    /// 动态修改回放速度（暂停恢复后可能调整速度）。
    pub fn set_speed(&mut self, speed: ReplaySpeed) {
        self.speed = speed;
        self.last_tick = Instant::now(); // 重置计时基准
    }

    /// 返回累计 pacing 时间 (ns)。
    pub fn total_pacing_ns(&self) -> u64 {
        self.total_pacing_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replay_speed_from_multiplier() {
        assert_eq!(ReplaySpeed::from_multiplier(0.0), ReplaySpeed(0.0));
        assert_eq!(ReplaySpeed::from_multiplier(1.0), ReplaySpeed(1.0));
        assert_eq!(ReplaySpeed::from_multiplier(2.0), ReplaySpeed(2.0));
        assert_eq!(ReplaySpeed::from_multiplier(2.5), ReplaySpeed(2.5));
        assert_eq!(ReplaySpeed::from_multiplier(100.0), ReplaySpeed(100.0));
        assert_eq!(ReplaySpeed::from_multiplier(-1.0), ReplaySpeed(-1.0));
    }

    #[test]
    fn test_arbitrary_speed_not_rounded() {
        let speed = ReplaySpeed::from_multiplier(2.5);
        assert_eq!(speed.multiplier(), 2.5);
    }

    #[test]
    fn test_max_speed_no_pacing() {
        let mut clock = ReplayClock::new(ReplaySpeed(0.0), 0);
        assert!(!clock.speed().is_pacing_enabled());
        // advance_to_sync should not block
        let start = Instant::now();
        clock.advance_to_sync(1_000_000_000); // 1 second virtual delta
        let elapsed = start.elapsed();
        // Max speed should complete almost instantly
        assert!(elapsed.as_millis() < 10);
    }

    #[test]
    fn test_negative_speed_no_pacing() {
        let speed = ReplaySpeed(-2.5);
        assert!(!speed.is_pacing_enabled());
        assert_eq!(speed.to_string(), "max");
    }

    #[test]
    fn test_pacing_enabled() {
        assert!(ReplaySpeed(0.5).is_pacing_enabled());
        assert!(ReplaySpeed(1.0).is_pacing_enabled());
        assert!(ReplaySpeed(2.0).is_pacing_enabled());
        assert!(ReplaySpeed(8.0).is_pacing_enabled());
    }

    #[test]
    fn test_sync_advance_small_delta() {
        let mut clock = ReplayClock::new(ReplaySpeed(8.0), 0);
        // Small delta at 8x should be nearly instant
        let start = Instant::now();
        clock.advance_to_sync(1_000_000); // 1ms at 8x
        let elapsed = start.elapsed();
        // Should be nearly instant (1ms/8 ≈ 125μs)
        assert!(elapsed.as_millis() < 5);
    }

    #[test]
    fn test_total_pacing() {
        let mut clock = ReplayClock::new(ReplaySpeed(1.0), 0);
        assert_eq!(clock.total_pacing_ns(), 0);
        // Advance 1ms at 1x = sleep roughly 1ms
        clock.advance_to_sync(1_000_000);
        assert!(clock.total_pacing_ns() > 0);
    }

    #[test]
    fn test_slow_speed_pacing_half() {
        // 0.5x 慢速：1ms 虚拟间隔 → 期望间隔 2ms，pacing 至少 1ms。
        let mut clock = ReplayClock::new(ReplaySpeed(0.5), 0);
        clock.advance_to_sync(1_000_000);
        assert!(clock.total_pacing_ns() >= 1_000_000);
    }

    #[test]
    fn test_display() {
        assert_eq!(ReplaySpeed(0.0).to_string(), "max");
        assert_eq!(ReplaySpeed(-1.0).to_string(), "max");
        assert_eq!(ReplaySpeed(1.0).to_string(), "1x");
        assert_eq!(ReplaySpeed(2.0).to_string(), "2x");
        assert_eq!(ReplaySpeed(2.5).to_string(), "2.5x");
    }
}
