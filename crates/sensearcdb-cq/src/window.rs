//! CQ 窗口状态管理。
//!
//! 定义 CQ 引擎的窗口键、窗口状态和窗口状态枚举。
//! `CqWindowKey` 是所有窗口操作的索引键，
//! `CqWindowState` 记录单个窗口的执行状态，
//! `CqWindowStatus` 定义窗口的生命周期阶段。

use crate::definition::CqGranularity;

// ─── CqWindowKey ───

/// CQ 窗口键，用于索引窗口状态缓存的复合键。
///
/// 由源表 ID、粒度层级和窗口结束时间组成，
/// 保证每个窗口在全局范围内唯一。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CqWindowKey {
    /// 源 raw 表 ID。
    pub source_table_id: u32,
    /// CQ 粒度层级。
    pub granularity: CqGranularity,
    /// 窗口结束时间戳（epoch 纳秒）。
    pub window_end: i64,
}

impl CqWindowKey {
    /// 创建新的窗口键。
    #[inline]
    pub fn new(source_table_id: u32, granularity: CqGranularity, window_end: i64) -> Self {
        Self {
            source_table_id,
            granularity,
            window_end,
        }
    }

    /// 计算窗口起始时间戳。
    ///
    /// `window_start = window_end - interval_ms * 1_000_000`
    #[inline]
    pub fn window_start(&self) -> i64 {
        self.window_end - self.granularity.interval_ns()
    }
}

// ─── CqWindowStatus ───

/// CQ 窗口的执行状态。
///
/// - `Pending`: 尚未计算，等待调度器触发
/// - `Calculated`: 已计算但可被重算（迟到数据可能到达）
/// - `Final`: 已固化，不可重算（超出 staleness 窗口）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CqWindowStatus {
    /// 尚未计算。
    Pending,
    /// 已计算，可被重算。
    Calculated,
    /// 已固化，不可重算。
    Final,
}

impl CqWindowStatus {
    /// 窗口是否已完成计算（Calculated 或 Final）。
    #[inline]
    pub fn is_done(&self) -> bool {
        matches!(self, CqWindowStatus::Calculated | CqWindowStatus::Final)
    }

    /// 窗口是否可重算。
    #[inline]
    pub fn can_recalculate(&self) -> bool {
        matches!(self, CqWindowStatus::Calculated)
    }
}

// ─── CqWindowState ───

/// CQ 窗口的运行时状态。
///
/// 每个 CQ 窗口维护自己的元数据，包括计算次数、
/// dirty 标记和时间戳信息。
#[derive(Debug, Clone)]
pub struct CqWindowState {
    /// 当前状态。
    pub status: CqWindowStatus,
    /// 首次计算时间（epoch 纳秒）。
    pub first_calculated_at: i64,
    /// 最近一次计算时间（epoch 纳秒）。
    pub last_calculated_at: i64,
    /// 是否有迟到数据到达，需要重算。
    pub dirty: bool,
    /// 累计计算次数。
    pub calculation_count: u32,
}

impl CqWindowState {
    /// 创建新的 Pending 窗口状态。
    #[inline]
    pub fn new() -> Self {
        Self {
            status: CqWindowStatus::Pending,
            first_calculated_at: 0,
            last_calculated_at: 0,
            dirty: false,
            calculation_count: 0,
        }
    }

    /// 标记窗口为已计算。
    ///
    /// 更新计算次数和时间戳，清除 dirty 标记。
    #[inline]
    pub fn mark_calculated(&mut self, now: i64) {
        if self.calculation_count == 0 {
            self.first_calculated_at = now;
        }
        self.last_calculated_at = now;
        self.calculation_count += 1;
        self.status = CqWindowStatus::Calculated;
        self.dirty = false;
    }

    /// 标记窗口为 Final（固化，不可重算）。
    #[inline]
    pub fn mark_final(&mut self) {
        self.status = CqWindowStatus::Final;
        self.dirty = false;
    }

    /// 标记窗口为 dirty（有迟到数据到达）。
    #[inline]
    pub fn mark_dirty(&mut self) {
        if self.status == CqWindowStatus::Calculated {
            self.dirty = true;
        }
    }

    /// 检查窗口是否可被执行（Pending 或 dirty Calculated）。
    #[inline]
    pub fn should_execute(&self) -> bool {
        matches!(self.status, CqWindowStatus::Pending)
            || (self.status == CqWindowStatus::Calculated && self.dirty)
    }
}

impl Default for CqWindowState {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_key_window_start() {
        let key = CqWindowKey::new(1, CqGranularity::Cq10s, 30_000_000_000);
        assert_eq!(key.window_start(), 20_000_000_000);
    }

    #[test]
    fn test_window_state_lifecycle() {
        let mut state = CqWindowState::new();
        assert!(state.should_execute());
        assert!(!state.status.is_done());

        state.mark_calculated(1_000_000_000);
        assert!(state.status.is_done());
        assert!(!state.should_execute());
        assert_eq!(state.calculation_count, 1);

        state.mark_dirty();
        assert!(state.should_execute());
        assert!(state.status.can_recalculate());

        state.mark_calculated(2_000_000_000);
        assert_eq!(state.calculation_count, 2);
        assert!(!state.dirty);

        state.mark_final();
        assert!(!state.status.can_recalculate());
        assert_eq!(state.status, CqWindowStatus::Final);

        state.mark_dirty();
        assert!(!state.dirty); // Final 窗口不会被标记为 dirty
    }

    #[test]
    fn test_cq_window_status_variants() {
        assert!(CqWindowStatus::Pending.is_done() == false);
        assert!(CqWindowStatus::Calculated.is_done());
        assert!(CqWindowStatus::Final.is_done());

        assert!(CqWindowStatus::Calculated.can_recalculate());
        assert!(!CqWindowStatus::Final.can_recalculate());
        assert!(!CqWindowStatus::Pending.can_recalculate());
    }
}
