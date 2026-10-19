//! SenseArcDB 共享核心类型。
//!
//! 本模块定义所有 crate 共用的基础数据类型，包括时间戳、物理数据类型、
//! OPC UA 质量码和数据点标记位。

use serde::{Deserialize, Serialize};

/// 时间戳 newtype（epoch 纳秒）——禁止隐式单位转换。
///
/// 内部存储为 `i64`，表示自 Unix epoch (1970-01-01T00:00:00Z) 以来的纳秒数。
/// 通过 newtype 包装确保所有时间戳单位在类型层面明确，防止纳秒与毫秒混用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampNanos(pub i64);

impl TimestampNanos {
    /// 从 epoch 毫秒构造时间戳。
    ///
    /// # Examples
    /// ```
    /// use sensearcdb_common::TimestampNanos;
    /// let ts = TimestampNanos::from_epoch_millis(1692180000000);
    /// assert_eq!(ts.as_nanos(), 1_692_180_000_000_000_000);
    /// ```
    #[inline]
    pub fn from_epoch_millis(ms: i64) -> Self {
        Self(ms * 1_000_000)
    }

    /// 返回内部纳秒值。
    #[inline]
    pub fn as_nanos(self) -> i64 {
        self.0
    }

    /// 返回 epoch 毫秒（截断）。
    #[inline]
    pub fn as_millis(self) -> i64 {
        self.0 / 1_000_000
    }
}

/// 租户 ID newtype（u32）。
///
/// **语义定稿（铁律，S2~S5 全遵守）**：
/// - `0` = 默认租户（default），**不许复用为「未初始化」哨兵**。
/// - `1..` = 真实租户。
/// - 「未关联/未初始化」用 `Option<TenantId>`（`None`）；真要哨兵用 `u32::MAX`。
///
/// 数据面字段 `Default` = 0（= 默认租户），天然正确、无需显式初始化；
/// N=1 单租户下 tenant_id 恒为 `TenantId::DEFAULT`（=0）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TenantId(pub u32);

impl TenantId {
    /// 默认租户（tenant_id = 0）。
    pub const DEFAULT: TenantId = TenantId(0);

    /// 返回内部 u32 值。
    #[inline]
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl From<u32> for TenantId {
    fn from(v: u32) -> Self {
        TenantId(v)
    }
}

impl Default for TenantId {
    fn default() -> Self {
        TenantId::DEFAULT
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 物理数据类型——对应 SAC 列存储的底层编码格式。
///
/// 每个变体的 discriminant 即为 SAC 文件格式中的 type tag 值，
/// 修改现有变体的 discriminant 会破坏文件格式兼容性。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum DataType {
    /// 64 位浮点 (IEEE 754 double)
    F64 = 0,
    /// 32 位浮点 (IEEE 754 single)
    F32 = 1,
    /// 64 位有符号整数
    I64 = 2,
    /// 64 位无符号整数
    U64 = 3,
    /// 32 位有符号整数
    I32 = 4,
    /// 布尔值
    Bool = 5,
    /// UTF-8 字符串
    String = 6,
    /// 16 位枚举 (通过 enum_values 映射表解析)
    Enum16 = 7,
}

/// 表级 WAL 持久化强度（durability）。
///
/// 落在 catalog `tables.durability`（TEXT）作为表属性，写路径按 point 所属 table
/// 解析后分流到三条互不混 fsync 的持久化路径。语义见 `docs/wal-tiered-durability-design.md` §1：
/// - `Group`（默认）：group commit 攒批 fsync 后 ACK，丢失窗口 ≤1s；
/// - `Durable`：ACK 在 fsync 后返回，零丢失；
/// - `Async`：写 OS 缓冲（page cache）后立即 ACK，掉电可丢。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Durability {
    /// 默认档：group commit 攒批 fsync 后 ACK（≤1s 窗口）。
    Group,
    /// 强一致档：ACK 在 fsync 后返回（零丢失）。
    Durable,
    /// 可丢档：写 OS 缓冲后立即 ACK（掉电可丢）。
    Async,
}

impl Durability {
    /// 从字符串解析（大小写不敏感，未知值安全降级为 `Group`）。
    pub fn from_str(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "durable" => Durability::Durable,
            "async" => Durability::Async,
            _ => Durability::Group,
        }
    }

    /// 转换为 SQLite 存储字符串。
    pub fn as_str(&self) -> &'static str {
        match self {
            Durability::Group => "group",
            Durability::Durable => "durable",
            Durability::Async => "async",
        }
    }
}

impl Default for Durability {
    fn default() -> Self {
        Durability::Group
    }
}

impl DataType {
    /// 返回该类型的固定字节宽度（变长类型返回 0）。
    #[inline]
    pub fn fixed_size(self) -> usize {
        match self {
            DataType::F64 => 8,
            DataType::F32 => 4,
            DataType::I64 => 8,
            DataType::U64 => 8,
            DataType::I32 => 4,
            DataType::Bool => 1,
            DataType::String => 0,
            DataType::Enum16 => 2,
        }
    }
}

/// 带类型的标量值——多类型直通（MemTable 写入、SnapshotStore 当前值）的载体。
///
/// 与 [`DataType`] 一一对应：8 个物理类型各对应一个变体。
/// 序列化（serde）用于 `snapshot.bin` 的多类型 `SnapshotEntry` 持久化。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TypedValue {
    /// 64 位浮点值。
    F64(f64),
    /// 32 位浮点值。
    F32(f32),
    /// 64 位有符号整数。
    I64(i64),
    /// 64 位无符号整数。
    U64(u64),
    /// 32 位有符号整数。
    I32(i32),
    /// 布尔值。
    Bool(bool),
    /// UTF-8 字符串。
    String(String),
    /// 16 位枚举值。
    Enum16(u16),
}

impl TypedValue {
    /// 返回该值对应的物理数据类型。
    #[inline]
    pub fn data_type(&self) -> DataType {
        match self {
            TypedValue::F64(_) => DataType::F64,
            TypedValue::F32(_) => DataType::F32,
            TypedValue::I64(_) => DataType::I64,
            TypedValue::U64(_) => DataType::U64,
            TypedValue::I32(_) => DataType::I32,
            TypedValue::Bool(_) => DataType::Bool,
            TypedValue::String(_) => DataType::String,
            TypedValue::Enum16(_) => DataType::Enum16,
        }
    }

    /// 将数值类型转换为 f64（Bool 转 0.0/1.0，String 转 NaN）。
    ///
    /// 用于查询执行器等仍以 f64 表示的兼容路径。
    #[inline]
    pub fn as_f64(&self) -> f64 {
        match self {
            TypedValue::F64(v) => *v,
            TypedValue::F32(v) => *v as f64,
            TypedValue::I64(v) => *v as f64,
            TypedValue::U64(v) => *v as f64,
            TypedValue::I32(v) => *v as f64,
            TypedValue::Bool(v) => {
                if *v {
                    1.0
                } else {
                    0.0
                }
            }
            TypedValue::Enum16(v) => *v as f64,
            TypedValue::String(_) => f64::NAN,
        }
    }
}

impl From<f64> for TypedValue {
    fn from(v: f64) -> Self {
        TypedValue::F64(v)
    }
}

/// OPC UA QQSSSSLL 质量码 (u8)。
///
/// 质量码编码遵循 OPC UA 规范 Part 8 §6.2：
/// - Bits 7-6: 主质量 (00=Bad, 01=Uncertain, 10=Good, 11=Good)
/// - Bits 5-4: 子状态
/// - Bits 3-2: 限制位
/// - Bits 1-0: 原始数据源
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quality(pub u8);

impl Quality {
    /// Good (Non-Specific): 0xC0 = 1100_0000
    pub const GOOD: Self = Self(0xC0);
    /// Bad (Non-Specific): 0x00 = 0000_0000
    pub const BAD: Self = Self(0x00);
    /// Uncertain (Non-Specific): 0x40 = 0100_0000
    pub const UNCERTAIN: Self = Self(0x40);
    /// Bad — TTL Expired (Last Usable Value 耗尽): 0x00
    pub const BAD_TTL_EXPIRED: Self = Self(0x00);

    /// 检查质量码主状态是否为 Good (bits 7-6 均为 1)。
    #[inline]
    pub fn is_good(self) -> bool {
        self.0 & 0xC0 == 0xC0
    }

    /// 检查质量码是否为 Bad。
    #[inline]
    pub fn is_bad(self) -> bool {
        self.0 & 0xC0 == 0x00
    }

    /// 检查质量码是否为 Uncertain。
    #[inline]
    pub fn is_uncertain(self) -> bool {
        self.0 & 0xC0 == 0x40
    }
}

/// 数据点标记位——标识数据点的特殊来源或状态。
///
/// 用于 SAC Block 的 `flags` 字段，标记数据点的写入来源或时间属性。
#[derive(Debug, Clone, Copy)]
pub struct PointFlags(pub u8);

impl PointFlags {
    /// 插值填充的数据点
    pub const INTERPOLATED: u8 = 0b0000_0001;
    /// 历史回填的数据点
    pub const BACKFILL: u8 = 0b0000_0010;
    /// 数据断层的间隙标记
    pub const GAP_MARKER: u8 = 0b0000_0100;
}

/// OPC UA 指数退避配置（运行时数据结构）。
///
/// 用于 Collector 驱动的重连逻辑，控制退避序列：
/// `initial → initial*m → initial*m^2 → ... → max`
///
/// 与 `ReconnectConfig`（配置序列化层）区分：
/// - `BackoffConfig` 是运行时类型，被重连逻辑直接消费
/// - `ReconnectConfig` 是配置层类型，通过 serde 从 toml 反序列化
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// 首次退避间隔（毫秒）。
    pub initial_backoff_ms: u64,

    /// 最大退避间隔（毫秒）。
    pub max_backoff_ms: u64,

    /// 退避乘数（通常为 2.0）。
    pub multiplier: f64,

    /// 最大重试次数（0 表示无限重试）。
    pub max_retries: u32,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 1_000,
            max_backoff_ms: 60_000,
            multiplier: 2.0,
            max_retries: 0,
        }
    }
}

impl BackoffConfig {
    /// 根据重试次数计算当前退避间隔（毫秒）。
    ///
    /// 公式：`min(initial * multiplier^retry_count, max)`
    ///
    /// # Arguments
    /// * `retry_count` - 当前重试次数（从 0 开始）
    ///
    /// # Examples
    /// ```
    /// use sensearcdb_common::BackoffConfig;
    /// let cfg = BackoffConfig::default();
    /// assert_eq!(cfg.backoff_ms(0), 1_000);
    /// assert_eq!(cfg.backoff_ms(1), 2_000);
    /// assert_eq!(cfg.backoff_ms(2), 4_000);
    /// ```
    pub fn backoff_ms(&self, retry_count: u32) -> u64 {
        let computed = self.initial_backoff_ms as f64 * self.multiplier.powi(retry_count as i32);
        (computed as u64).min(self.max_backoff_ms)
    }

    /// 判断是否应该继续重试。
    ///
    /// 返回 `true` 当 `max_retries == 0`（无限）或 `retry_count < max_retries`。
    #[inline]
    pub fn should_retry(&self, retry_count: u32) -> bool {
        self.max_retries == 0 || retry_count < self.max_retries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_nanos_from_millis() {
        let ts = TimestampNanos::from_epoch_millis(1_000);
        assert_eq!(ts.as_nanos(), 1_000_000_000);
        assert_eq!(ts.as_millis(), 1_000);
    }

    #[test]
    fn test_quality_is_good() {
        assert!(Quality::GOOD.is_good());
        assert!(!Quality::BAD.is_good());
        assert!(!Quality::UNCERTAIN.is_good());
    }

    #[test]
    fn test_data_type_fixed_size() {
        assert_eq!(DataType::F64.fixed_size(), 8);
        assert_eq!(DataType::F32.fixed_size(), 4);
        assert_eq!(DataType::I64.fixed_size(), 8);
        assert_eq!(DataType::U64.fixed_size(), 8);
        assert_eq!(DataType::I32.fixed_size(), 4);
        assert_eq!(DataType::Bool.fixed_size(), 1);
        assert_eq!(DataType::String.fixed_size(), 0);
        assert_eq!(DataType::Enum16.fixed_size(), 2);
    }

    #[test]
    fn test_quality_is_bad() {
        assert!(Quality::BAD.is_bad());
        assert!(Quality::BAD_TTL_EXPIRED.is_bad());
    }

    #[test]
    fn test_timestamp_ordering() {
        let t1 = TimestampNanos(100);
        let t2 = TimestampNanos(200);
        assert!(t1 < t2);
    }

    #[test]
    fn test_tenant_id_semantics() {
        // 0 = 默认租户（铁律，不许复用为「未初始化」哨兵）。
        assert_eq!(TenantId::DEFAULT.as_u32(), 0);
        assert_eq!(TenantId::default(), TenantId::DEFAULT);
        assert_eq!(TenantId::from(0u32), TenantId::DEFAULT);
        assert_eq!(TenantId::from(7u32).as_u32(), 7);
        assert_eq!(TenantId::DEFAULT.to_string(), "0");
        // newtype 类型安全：不与其他 u32 混淆。
        assert!(TenantId::DEFAULT < TenantId(1));
    }

    #[test]
    fn test_durability_from_str_and_as_str() {
        // from_str：大小写不敏感，未知值安全降级 Group。
        assert_eq!(Durability::from_str("group"), Durability::Group);
        assert_eq!(Durability::from_str("GROUP"), Durability::Group);
        assert_eq!(Durability::from_str("durable"), Durability::Durable);
        assert_eq!(Durability::from_str("Durable"), Durability::Durable);
        assert_eq!(Durability::from_str("async"), Durability::Async);
        assert_eq!(Durability::from_str("ASYNC"), Durability::Async);
        assert_eq!(Durability::from_str("unknown"), Durability::Group);
        assert_eq!(Durability::from_str(""), Durability::Group);
        // as_str 往返。
        assert_eq!(Durability::Group.as_str(), "group");
        assert_eq!(Durability::Durable.as_str(), "durable");
        assert_eq!(Durability::Async.as_str(), "async");
        assert_eq!(
            Durability::from_str(Durability::Durable.as_str()),
            Durability::Durable
        );
        // Default = Group。
        assert_eq!(Durability::default(), Durability::Group);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Wave 5 新增类型
// ══════════════════════════════════════════════════════════════════════════════

/// 预测步长 newtype——明确预测输出的时间跨度。
///
/// 内部存储为 `u32`，表示预测的点数（步数）。
/// 通过 newtype 包装确保预测步长在类型层面明确，
/// 防止与其他长度值（如窗口大小、批量大小）混用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ForecastHorizon(pub u32);

impl ForecastHorizon {
    /// 创建新的预测步长。
    ///
    /// # Arguments
    /// * `steps` - 预测步数（点数），必须 > 0。
    ///
    /// # Panics
    /// 当 `steps == 0` 时 panic。
    #[inline]
    pub fn new(steps: u32) -> Self {
        assert!(steps > 0, "ForecastHorizon must be > 0");
        Self(steps)
    }

    /// 返回预测步数。
    #[inline]
    pub fn steps(self) -> u32 {
        self.0
    }
}

impl Default for ForecastHorizon {
    fn default() -> Self {
        Self(96)
    }
}

/// 条件标识符 newtype——唯一标识一个条件规则。
///
/// 内部存储为 `u64`，用于在条件引擎中索引和查找条件规则。
/// 条件 ID 在创建时由引擎分配，保证唯一性。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ConditionId(pub u64);

impl ConditionId {
    /// 创建新的条件标识符。
    #[inline]
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// 返回内部 u64 值。
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

/// 异常分数 newtype——表示 IsolationForest 的异常程度。
///
/// 内部存储为 `f64`，遵循 sklearn IsolationForest 的 decision_function 约定：
/// - 分数 < 0 表示异常（anomaly）
/// - 分数 >= 0 表示正常（normal）
/// - 分数越低，异常程度越高
///
/// 通过 newtype 包装确保异常分数的语义在使用时明确，
/// 防止与普通浮点值（如传感器读数、阈值）混用。
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct AnomalyScore(pub f64);

impl AnomalyScore {
    /// 创建新的异常分数。
    #[inline]
    pub fn new(score: f64) -> Self {
        Self(score)
    }

    /// 判断该分数是否表示异常（< 0 为异常）。
    #[inline]
    pub fn is_anomaly(self) -> bool {
        self.0 < 0.0
    }

    /// 判断该分数是否表示正常（>= 0 为正常）。
    #[inline]
    pub fn is_normal(self) -> bool {
        self.0 >= 0.0
    }

    /// 返回原始分数值。
    #[inline]
    pub fn score(self) -> f64 {
        self.0
    }

    /// 将分数归一化到 [0, 1] 范围（0 = 正常，1 = 最异常）。
    ///
    /// 使用 sigmoid 归一化：`1 / (1 + exp(score))`。
    /// 当 `score → -∞` 时结果趋近 1（极度异常），
    /// 当 `score → +∞` 时结果趋近 0（完全正常）。
    #[inline]
    pub fn normalized(self) -> f64 {
        1.0 / (1.0 + (self.0).exp())
    }
}

#[cfg(test)]
mod wave5_tests {
    use super::*;

    #[test]
    fn test_forecast_horizon_new() {
        let h = ForecastHorizon::new(96);
        assert_eq!(h.steps(), 96);
    }

    #[test]
    #[should_panic(expected = "ForecastHorizon must be > 0")]
    fn test_forecast_horizon_zero_panics() {
        ForecastHorizon::new(0);
    }

    #[test]
    fn test_forecast_horizon_default() {
        assert_eq!(ForecastHorizon::default().steps(), 96);
    }

    #[test]
    fn test_condition_id() {
        let id = ConditionId::new(42);
        assert_eq!(id.as_u64(), 42);
    }

    #[test]
    fn test_anomaly_score_is_anomaly() {
        assert!(AnomalyScore::new(-0.5).is_anomaly());
        assert!(AnomalyScore::new(-0.001).is_anomaly());
        assert!(!AnomalyScore::new(0.0).is_anomaly());
        assert!(!AnomalyScore::new(0.5).is_anomaly());
    }

    #[test]
    fn test_anomaly_score_is_normal() {
        assert!(AnomalyScore::new(0.0).is_normal());
        assert!(AnomalyScore::new(0.5).is_normal());
        assert!(!AnomalyScore::new(-0.1).is_normal());
    }

    #[test]
    fn test_anomaly_score_normalized() {
        // score = 0 → normalized ≈ 0.5
        let s = AnomalyScore::new(0.0);
        assert!((s.normalized() - 0.5).abs() < 1e-6);

        // score = -5 → normalized ≈ 0.993 (非常异常)
        let s = AnomalyScore::new(-5.0);
        assert!(s.normalized() > 0.99);

        // score = 5 → normalized ≈ 0.007 (非常正常)
        let s = AnomalyScore::new(5.0);
        assert!(s.normalized() < 0.01);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// 宽表模型：统一 tag（测点）标识
// ══════════════════════════════════════════════════════════════════════════════

/// 宽表模型下的统一 tag（测点）标识：某张设备表的某一列。
///
/// 宽表语义下「测点」的本质是「某张设备表（实体）的某一列」。
/// `(table_id, column_id)` 即全局稳定、天然唯一的测点标识，
/// 无需再包一层独立 `tag_id`。
///
/// - `table_id`：设备/实体表 ID。
/// - `column_id`：该设备表内的列 ID。`column_id == 0` 即
///   「一张表只有一列」的 N=1 特例，对应旧「1 tag = 1 table」
///   的退化形态，完全合法。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// 设备/实体表 ID。
    pub table_id: u32,
    /// 该表内的列 ID（N=1 特例时为 0）。
    pub column_id: u16,
}

impl ColumnRef {
    /// 创建新的列引用。
    #[inline]
    pub fn new(table_id: u32, column_id: u16) -> Self {
        Self {
            table_id,
            column_id,
        }
    }

    /// 便捷构造：单列（N=1）测点，即 `column_id == 0`。
    ///
    /// 对应旧「1 tag = 1 table」语义：一张表只有一列（column 0）。
    /// 宽表模型下这是合法的退化形态。
    #[inline]
    pub fn single(table_id: u32) -> Self {
        Self {
            table_id,
            column_id: 0,
        }
    }
}

impl std::fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.table_id, self.column_id)
    }
}

#[cfg(test)]
mod column_ref_tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_column_ref_new_and_single() {
        let c = ColumnRef::new(42, 3);
        assert_eq!(c.table_id, 42);
        assert_eq!(c.column_id, 3);

        let s = ColumnRef::single(7);
        assert_eq!(s.table_id, 7);
        assert_eq!(s.column_id, 0);
    }

    #[test]
    fn test_column_ref_display() {
        assert_eq!(ColumnRef::new(42, 3).to_string(), "42:3");
        assert_eq!(ColumnRef::single(7).to_string(), "7:0");
    }

    #[test]
    fn test_column_ref_eq_hash() {
        let mut set = HashSet::new();
        set.insert(ColumnRef::new(1, 2));
        assert!(set.contains(&ColumnRef::new(1, 2)));
        assert!(!set.contains(&ColumnRef::new(1, 3)));
    }
}
