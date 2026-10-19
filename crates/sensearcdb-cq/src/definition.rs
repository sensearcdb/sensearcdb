//! CQ 定义与粒度枚举。
//!
//! `CqDefinition` 描述一个 CQ 预聚合配置：
//! - 源表 → 目标表的映射
//! - 窗口粒度（CQ_10S / CQ_1M / CQ_1H / CQ_1D）
//! - 聚合函数列表（每列一个）
//! - 执行状态和失败计数
//!
//! 定义通过 `config.db` (SQLite) 持久化。

use sensearcdb_common::{AggFunction, DataType};
use serde::{Deserialize, Serialize};

// ─── CqGranularity ───

/// CQ 四层固定粒度枚举。
///
/// 每层有固定的窗口时长（纳秒），按 SECOND → MINUTE → HOUR → DAY 层级递进。
/// 层级间通过 sum+count 传递实现正确的加权聚合。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CqGranularity {
    /// 10 秒窗口（10,000 ms）。
    Cq10s,
    /// 1 分钟窗口（60,000 ms）。
    Cq1m,
    /// 1 小时窗口（3,600,000 ms）。
    Cq1h,
    /// 1 天窗口（86,400,000 ms）。
    Cq1d,
}

impl CqGranularity {
    /// 返回窗口长度（毫秒）。
    #[inline]
    pub fn interval_ms(self) -> i64 {
        match self {
            CqGranularity::Cq10s => 10_000,
            CqGranularity::Cq1m => 60_000,
            CqGranularity::Cq1h => 3_600_000,
            CqGranularity::Cq1d => 86_400_000,
        }
    }

    /// 返回窗口长度（纳秒）。
    #[inline]
    pub fn interval_ns(self) -> i64 {
        self.interval_ms() * 1_000_000
    }

    /// 返回层级序数（0 = Cq10s, 1 = Cq1m, 2 = Cq1h, 3 = Cq1d）。
    #[inline]
    pub fn ordinal(self) -> u8 {
        match self {
            CqGranularity::Cq10s => 0,
            CqGranularity::Cq1m => 1,
            CqGranularity::Cq1h => 2,
            CqGranularity::Cq1d => 3,
        }
    }

    /// 返回直接下层粒度（用于层级间 sum+count 传递）。
    ///
    /// CQ_1M → CQ_10S, CQ_1H → CQ_1M, CQ_1D → CQ_1H.
    /// CQ_10S 没有下层，返回 None。
    #[inline]
    pub fn lower_granularity(self) -> Option<CqGranularity> {
        match self {
            CqGranularity::Cq10s => None,
            CqGranularity::Cq1m => Some(CqGranularity::Cq10s),
            CqGranularity::Cq1h => Some(CqGranularity::Cq1m),
            CqGranularity::Cq1d => Some(CqGranularity::Cq1h),
        }
    }

    /// 返回粒度名称（用于 CQ 表命名和日志）。
    #[inline]
    pub fn name(self) -> &'static str {
        match self {
            CqGranularity::Cq10s => "CQ_10s",
            CqGranularity::Cq1m => "CQ_1m",
            CqGranularity::Cq1h => "CQ_1h",
            CqGranularity::Cq1d => "CQ_1d",
        }
    }

    /// 返回粒度短名（小写，用于 CQ 定义/表命名，如 `10s`/`1m`/`1h`/`1d`）。
    ///
    /// 生成的定义名/表名形如 `cq_10s_42`（见设计 §2.2）。
    #[inline]
    pub fn short_name(self) -> &'static str {
        match self {
            CqGranularity::Cq10s => "10s",
            CqGranularity::Cq1m => "1m",
            CqGranularity::Cq1h => "1h",
            CqGranularity::Cq1d => "1d",
        }
    }

    /// 从粒度名称解析（`name()` 的逆操作，用于 config.db 反序列化）。
    ///
    /// 名称匹配大小写不敏感。未知名称返回 `None`。
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_uppercase().as_str() {
            "CQ_10S" => Some(CqGranularity::Cq10s),
            "CQ_1M" => Some(CqGranularity::Cq1m),
            "CQ_1H" => Some(CqGranularity::Cq1h),
            "CQ_1D" => Some(CqGranularity::Cq1d),
            _ => None,
        }
    }

    /// 返回所有粒度的迭代器（从细到粗）。
    pub fn all() -> [CqGranularity; 4] {
        [
            CqGranularity::Cq10s,
            CqGranularity::Cq1m,
            CqGranularity::Cq1h,
            CqGranularity::Cq1d,
        ]
    }
}

impl std::fmt::Display for CqGranularity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

// ─── CqStatus ───

/// CQ 定义的执行状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CqStatus {
    /// 正常运行。
    Running,
    /// 滞后（实际执行落后于调度）。
    Delayed,
    /// 错误（连续失败超过阈值）。
    Error,
    /// 已禁用。
    Disabled,
}

impl Default for CqStatus {
    #[inline]
    fn default() -> Self {
        CqStatus::Running
    }
}

impl CqStatus {
    /// 状态是否允许调度执行。
    #[inline]
    pub fn is_schedulable(&self) -> bool {
        matches!(self, CqStatus::Running | CqStatus::Delayed)
    }

    /// 转换为 config.db 存储字符串。
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            CqStatus::Running => "Running",
            CqStatus::Delayed => "Delayed",
            CqStatus::Error => "Error",
            CqStatus::Disabled => "Disabled",
        }
    }

    /// 从 config.db 存储字符串解析。
    #[inline]
    pub fn from_str(s: &str) -> CqStatus {
        match s {
            "Delayed" => CqStatus::Delayed,
            "Error" => CqStatus::Error,
            "Disabled" => CqStatus::Disabled,
            _ => CqStatus::Running,
        }
    }
}

// ─── CqDefinition ───

/// CQ 预聚合定义。
///
/// 描述一个从源 raw 表到目标 CQ 聚合表的连续查询配置。
/// 定义通过 `config.db` (SQLite) 持久化，进程重启后自动恢复。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CqDefinition {
    /// CQ 名称，格式如 `"cq_10s_42"`。
    pub name: String,
    /// 源 raw 表 ID。
    pub source_table_id: u32,
    /// 目标 CQ 表 ID。
    pub target_table_id: u32,
    /// 下层 CQ 目标表 ID（用于层级间 sum+count 传递）。
    ///
    /// CQ_10s 为 `None`（直接读 raw 表）；CQ_1m/1h/1d 指向下一层 CQ 表 ID：
    /// CQ_1m → CQ_10s 表，CQ_1h → CQ_1m 表，CQ_1d → CQ_1h 表。
    pub lower_target_table_id: Option<u32>,
    /// 窗口粒度。
    pub granularity: CqGranularity,
    /// 窗口长度（纳秒），由 granularity 派生。
    pub interval_ns: i64,
    /// 源表列 ID 列表（需要聚合的列）。
    pub source_columns: Vec<u16>,
    /// 每列的聚合函数（与 source_columns 一一对应）。
    pub aggregations: Vec<AggFunction>,
    /// 每源列的物理数据类型（与 source_columns 一一对应）。
    ///
    /// 类型化聚合（空窗口类型注入 + 层级 typed 累加）的等公民前置信息。
    /// `#[serde(default)]` 兼容旧 config.db 行（老库迁移时 `ALTER TABLE` 默认 `[]`）。
    #[serde(default)]
    pub source_types: Vec<DataType>,
    /// 执行状态。
    pub status: CqStatus,
    /// 上次已执行的窗口结束时间（epoch 纳秒）。
    pub last_executed_window_end: i64,
    /// 连续失败次数。
    pub consecutive_failures: u32,
}

impl CqDefinition {
    /// 创建新的 CQ 定义。
    ///
    /// # Arguments
    /// * `name` - CQ 名称。
    /// * `source_table_id` - 源表 ID。
    /// * `target_table_id` - 目标表 ID。
    /// * `granularity` - 窗口粒度。
    /// * `source_columns` - 源表列 ID 列表。
    /// * `aggregations` - 聚合函数列表（与 columns 一一对应）。
    pub fn new(
        name: String,
        source_table_id: u32,
        target_table_id: u32,
        granularity: CqGranularity,
        source_columns: Vec<u16>,
        aggregations: Vec<AggFunction>,
    ) -> Self {
        assert_eq!(
            source_columns.len(),
            aggregations.len(),
            "source_columns and aggregations must have the same length"
        );
        Self {
            name,
            source_table_id,
            target_table_id,
            lower_target_table_id: None,
            interval_ns: granularity.interval_ns(),
            granularity,
            source_columns,
            aggregations,
            source_types: Vec::new(),
            status: CqStatus::Running,
            last_executed_window_end: 0,
            consecutive_failures: 0,
        }
    }

    /// 获取第 `index` 个源列的物理数据类型。
    ///
    /// `source_types` 缺失或长度不匹配时回退 `DataType::F64`（兼容旧定义/旧库行），
    /// 保证类型化聚合在异常输入下仍可运行且不 panic。
    #[inline]
    pub fn source_type(&self, index: usize) -> DataType {
        self.source_types
            .get(index)
            .copied()
            .unwrap_or(DataType::F64)
    }

    /// 设置下层 CQ 目标表 ID（用于层级间 sum+count 传递）。
    ///
    /// CQ_10s 不调用本方法（保持 `None`，直接读 raw 表）；
    /// CQ_1m/1h/1d 在建表注册时由 `CqEngine::on_table_created` 填充。
    #[inline]
    pub fn set_lower_target_table_id(&mut self, lower_target_table_id: u32) {
        self.lower_target_table_id = Some(lower_target_table_id);
    }

    /// 记录一次成功的执行。
    ///
    /// 单调推进 `last_executed_window_end` 并重置失败计数。
    ///
    /// # 单调性
    /// 重算旧窗口（Phase 1）时 `window_end` 可能小于当前水位，若不取 max 会回退水位，
    /// 导致 Phase 2 重新调度已执行过的窗口。这里取 `max` 保证水位只前进不回退。
    #[inline]
    pub fn record_success(&mut self, window_end: i64) {
        self.last_executed_window_end = self.last_executed_window_end.max(window_end);
        self.consecutive_failures = 0;
        if self.status == CqStatus::Error {
            self.status = CqStatus::Running;
        }
    }

    /// 记录一次执行失败。
    ///
    /// 递增失败计数；超过阈值时标记为 Error。
    #[inline]
    pub fn record_failure(&mut self, max_retries: u32) {
        self.consecutive_failures += 1;
        if self.consecutive_failures >= max_retries {
            self.status = CqStatus::Error;
        }
    }

    /// 禁用该 CQ 定义。
    #[inline]
    pub fn disable(&mut self) {
        self.status = CqStatus::Disabled;
    }

    /// 启用该 CQ 定义。
    #[inline]
    pub fn enable(&mut self) {
        if self.status == CqStatus::Disabled {
            self.status = CqStatus::Running;
            self.consecutive_failures = 0;
        }
    }

    /// 当前是否可被调度。
    #[inline]
    pub fn is_schedulable(&self) -> bool {
        self.status.is_schedulable()
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_granularity_intervals() {
        assert_eq!(CqGranularity::Cq10s.interval_ms(), 10_000);
        assert_eq!(CqGranularity::Cq1m.interval_ms(), 60_000);
        assert_eq!(CqGranularity::Cq1h.interval_ms(), 3_600_000);
        assert_eq!(CqGranularity::Cq1d.interval_ms(), 86_400_000);
    }

    #[test]
    fn test_granularity_lower() {
        assert_eq!(
            CqGranularity::Cq1m.lower_granularity(),
            Some(CqGranularity::Cq10s)
        );
        assert_eq!(
            CqGranularity::Cq1h.lower_granularity(),
            Some(CqGranularity::Cq1m)
        );
        assert_eq!(
            CqGranularity::Cq1d.lower_granularity(),
            Some(CqGranularity::Cq1h)
        );
        assert_eq!(CqGranularity::Cq10s.lower_granularity(), None);
    }

    #[test]
    fn test_definition_new() {
        let def = CqDefinition::new(
            "cq_10s_42".into(),
            42,
            142,
            CqGranularity::Cq10s,
            vec![1, 2],
            vec![AggFunction::Avg, AggFunction::Sum],
        );
        assert_eq!(def.name, "cq_10s_42");
        assert_eq!(def.source_table_id, 42);
        assert_eq!(def.target_table_id, 142);
        assert_eq!(def.lower_target_table_id, None);
        assert!(def.is_schedulable());
    }

    #[test]
    fn test_definition_set_lower_target_table_id() {
        let mut def = CqDefinition::new(
            "cq_1m_42".into(),
            42,
            242,
            CqGranularity::Cq1m,
            vec![1],
            vec![AggFunction::Avg],
        );
        assert_eq!(def.lower_target_table_id, None);
        def.set_lower_target_table_id(142);
        assert_eq!(def.lower_target_table_id, Some(142));
    }

    #[test]
    fn test_definition_record_success() {
        let mut def = CqDefinition::new(
            "test".into(),
            1,
            2,
            CqGranularity::Cq1m,
            vec![1],
            vec![AggFunction::Avg],
        );
        def.consecutive_failures = 5;
        def.status = CqStatus::Error;
        def.record_success(60_000_000_000);
        assert_eq!(def.last_executed_window_end, 60_000_000_000);
        assert_eq!(def.consecutive_failures, 0);
        assert_eq!(def.status, CqStatus::Running);
    }

    /// 单调性：重算旧窗口（window_end 小于当前水位）不得回退 `last_executed_window_end`。
    #[test]
    fn test_definition_record_success_is_monotonic() {
        let mut def = CqDefinition::new(
            "test".into(),
            1,
            2,
            CqGranularity::Cq1m,
            vec![1],
            vec![AggFunction::Avg],
        );
        def.record_success(100_000_000_000);
        assert_eq!(def.last_executed_window_end, 100_000_000_000);

        // 重算旧窗口（水位已推进到 100s，重算 60s 的窗口）——水位不得回退。
        def.record_success(60_000_000_000);
        assert_eq!(
            def.last_executed_window_end, 100_000_000_000,
            "record_success 必须单调，重算旧窗口不得回退水位"
        );

        // 正常推进更大窗口仍生效。
        def.record_success(120_000_000_000);
        assert_eq!(def.last_executed_window_end, 120_000_000_000);
    }

    #[test]
    fn test_definition_record_failure() {
        let mut def = CqDefinition::new(
            "test".into(),
            1,
            2,
            CqGranularity::Cq1m,
            vec![1],
            vec![AggFunction::Avg],
        );
        def.record_failure(3);
        assert_eq!(def.consecutive_failures, 1);
        assert_eq!(def.status, CqStatus::Running);
        def.record_failure(3);
        def.record_failure(3);
        assert_eq!(def.status, CqStatus::Error);
    }

    #[test]
    fn test_definition_enable_disable() {
        let mut def = CqDefinition::new(
            "test".into(),
            1,
            2,
            CqGranularity::Cq1m,
            vec![1],
            vec![AggFunction::Avg],
        );
        assert!(def.is_schedulable());
        def.disable();
        assert!(!def.is_schedulable());
        def.enable();
        assert!(def.is_schedulable());
    }
}
