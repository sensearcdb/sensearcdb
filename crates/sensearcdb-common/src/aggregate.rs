//! CQ 引擎和流计算引擎共享的**类型化**增量聚合状态。
//!
//! `AggregateState` 提供增量更新的聚合状态机，支持：
//! - 单值累加 (`accumulate`, `accumulate_with_ts`)
//! - 多状态合并 (`merge`) — 用于 CQ 层级间 sum+count 传递
//! - 聚合函数求值 (`result` → `TypedValue`)
//!
//! # 类型语义（Phase7-B5 统一类型化收敛）
//!
//! 状态在构造时由 `result_type`（= 源数值类型）决定累加域与输出域：
//!
//! | 源类型 | SUM 累加域 | MIN/MAX/FIRST/LAST 存储域 | 输出类型 |
//! |---|---|---|---|
//! | U64 | u64（原生，wrapping） | u64 | U64 |
//! | I64 | i64（原生，wrapping） | i64 | I64 |
//! | I32 | i64 加宽 | i32 | I32（输出 cast 回） |
//! | F32 | f64 加宽 | f32 | F32（输出 cast 回） |
//! | F64 | f64 | f64 | F64 |
//!
//! `result()` 的聚合函数结果类型映射（`agg_result_type`）：
//! - `COUNT` → `U64`；`AVG`/`STDDEV` → `F64`；其余 → 源数值类型。
//!
//! # 设计原则
//!
//! 放在 `sensearcdb-common` 中，CQ 引擎和流计算引擎各自引用，
//! 避免两模块间的编译期交叉依赖。

use serde::{Deserialize, Serialize};

use crate::types::{DataType, TypedValue};

// ─── Numeric ───

/// 数值标量——类型化聚合状态的数值载体。
///
/// 覆盖 5 个数值物理类型（F64/F32/I64/U64/I32），
/// 用于在聚合过程中按源类型域精确累加，避免 f64 折叠导致的精度丢失。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Numeric {
    /// 64 位浮点。
    F64(f64),
    /// 32 位浮点。
    F32(f32),
    /// 64 位有符号整数。
    I64(i64),
    /// 64 位无符号整数。
    U64(u64),
    /// 32 位有符号整数。
    I32(i32),
}

impl Numeric {
    /// 转 f64（用于 AVG/STDDEV 等浮点结果，或跨类型 fallback 比较）。
    #[inline]
    pub fn as_f64(&self) -> f64 {
        match self {
            Numeric::F64(v) => *v,
            Numeric::F32(v) => *v as f64,
            Numeric::I64(v) => *v as f64,
            Numeric::U64(v) => *v as f64,
            Numeric::I32(v) => *v as f64,
        }
    }

    /// 转 u64（整数源类型精确；浮点源类型截断）。
    #[inline]
    pub fn as_u64(&self) -> u64 {
        match self {
            Numeric::U64(v) => *v,
            Numeric::I64(v) => *v as u64,
            Numeric::I32(v) => *v as u64,
            Numeric::F32(v) => *v as u64,
            Numeric::F64(v) => *v as u64,
        }
    }

    /// 转 i64（整数源类型精确；浮点源类型截断）。
    #[inline]
    pub fn as_i64(&self) -> i64 {
        match self {
            Numeric::U64(v) => *v as i64,
            Numeric::I64(v) => *v,
            Numeric::I32(v) => *v as i64,
            Numeric::F32(v) => *v as i64,
            Numeric::F64(v) => *v as i64,
        }
    }

    /// 按源域做 wrapping 加法（整数 wrapping，浮点普通加法）。
    ///
    /// `self` 与 `other` 在正常路径下恒为同一累加域变体；
    /// 兜底分支仅在跨类型 `merge` 时触发，此时退化为 f64 加法。
    fn wrapping_add(self, other: &Numeric) -> Numeric {
        match (self, *other) {
            (Numeric::U64(a), Numeric::U64(b)) => Numeric::U64(a.wrapping_add(b)),
            (Numeric::I64(a), Numeric::I64(b)) => Numeric::I64(a.wrapping_add(b)),
            (Numeric::I32(a), Numeric::I32(b)) => Numeric::I32(a.wrapping_add(b)),
            (Numeric::F32(a), Numeric::F32(b)) => Numeric::F32(a + b),
            (Numeric::F64(a), Numeric::F64(b)) => Numeric::F64(a + b),
            _ => Numeric::F64(self.as_f64() + other.as_f64()),
        }
    }

    /// `a == b`（同变体精确比较，跨类型退化为 f64 比较）。
    pub fn equal(a: &Numeric, b: &Numeric) -> bool {
        match (a, b) {
            (Numeric::F64(x), Numeric::F64(y)) => x == y,
            (Numeric::F32(x), Numeric::F32(y)) => x == y,
            (Numeric::I64(x), Numeric::I64(y)) => x == y,
            (Numeric::U64(x), Numeric::U64(y)) => x == y,
            (Numeric::I32(x), Numeric::I32(y)) => x == y,
            _ => a.as_f64() == b.as_f64(),
        }
    }

    /// `a < b`（同变体精确比较，跨类型退化为 f64 比较）。
    pub fn less(a: &Numeric, b: &Numeric) -> bool {
        match (a, b) {
            (Numeric::F64(x), Numeric::F64(y)) => x < y,
            (Numeric::F32(x), Numeric::F32(y)) => x < y,
            (Numeric::I64(x), Numeric::I64(y)) => x < y,
            (Numeric::U64(x), Numeric::U64(y)) => x < y,
            (Numeric::I32(x), Numeric::I32(y)) => x < y,
            _ => a.as_f64() < b.as_f64(),
        }
    }

    /// `a > b`（同变体精确比较，跨类型退化为 f64 比较）。
    pub fn greater(a: &Numeric, b: &Numeric) -> bool {
        match (a, b) {
            (Numeric::F64(x), Numeric::F64(y)) => x > y,
            (Numeric::F32(x), Numeric::F32(y)) => x > y,
            (Numeric::I64(x), Numeric::I64(y)) => x > y,
            (Numeric::U64(x), Numeric::U64(y)) => x > y,
            (Numeric::I32(x), Numeric::I32(y)) => x > y,
            _ => a.as_f64() > b.as_f64(),
        }
    }

    /// 将 `TypedValue` 提取为**累加域**（SUM 加宽后）的 `Numeric`。
    ///
    /// U64→u64、I64→i64、I32→i64 加宽、F32→f64 加宽、F64→f64；非数值→f64。
    fn sum_addend(v: &TypedValue, result_type: DataType) -> Numeric {
        match result_type {
            DataType::U64 => Numeric::U64(typed_to_u64(v)),
            DataType::I64 => Numeric::I64(typed_to_i64(v)),
            DataType::I32 => Numeric::I64(typed_to_i64(v)), // 加宽
            DataType::F32 => Numeric::F64(v.as_f64()),      // 加宽
            DataType::F64 => Numeric::F64(v.as_f64()),
            _ => Numeric::F64(v.as_f64()),
        }
    }

    /// 将 `TypedValue` 提取为**源域**（MIN/MAX/FIRST/LAST 存储域）的 `Numeric`。
    fn from_source(v: &TypedValue, result_type: DataType) -> Numeric {
        match result_type {
            DataType::U64 => Numeric::U64(typed_to_u64(v)),
            DataType::I64 => Numeric::I64(typed_to_i64(v)),
            DataType::I32 => Numeric::I32(typed_to_i64(v) as i32),
            DataType::F32 => Numeric::F32(v.as_f64() as f32),
            DataType::F64 => Numeric::F64(v.as_f64()),
            _ => Numeric::F64(v.as_f64()),
        }
    }

    /// 累加域的零值。
    fn zero_sum(result_type: DataType) -> Numeric {
        match result_type {
            DataType::U64 => Numeric::U64(0),
            DataType::I64 => Numeric::I64(0),
            DataType::I32 => Numeric::I64(0),   // 加宽域
            DataType::F32 => Numeric::F64(0.0), // 加宽域
            DataType::F64 => Numeric::F64(0.0),
            _ => Numeric::F64(0.0),
        }
    }

    /// 源域的零值。
    fn zero_source(result_type: DataType) -> Numeric {
        match result_type {
            DataType::U64 => Numeric::U64(0),
            DataType::I64 => Numeric::I64(0),
            DataType::I32 => Numeric::I32(0),
            DataType::F32 => Numeric::F32(0.0),
            DataType::F64 => Numeric::F64(0.0),
            _ => Numeric::F64(0.0),
        }
    }

    /// 将源域 `Numeric` 转回 `TypedValue`（MIN/MAX/FIRST/LAST 已在源域）。
    fn to_typed_value(self) -> TypedValue {
        match self {
            Numeric::F64(v) => TypedValue::F64(v),
            Numeric::F32(v) => TypedValue::F32(v),
            Numeric::I64(v) => TypedValue::I64(v),
            Numeric::U64(v) => TypedValue::U64(v),
            Numeric::I32(v) => TypedValue::I32(v),
        }
    }
}

// ─── 内部：TypedValue → 整数提取（避免经 f64 折叠丢精度） ───

/// 提取 `TypedValue` 的 u64 值（整数变体精确；浮点/非数值截断或归零）。
#[inline]
fn typed_to_u64(v: &TypedValue) -> u64 {
    match v {
        TypedValue::U64(x) => *x,
        TypedValue::I64(x) => *x as u64,
        TypedValue::I32(x) => *x as u64,
        TypedValue::F32(x) => *x as u64,
        TypedValue::F64(x) => *x as u64,
        TypedValue::Bool(b) => u64::from(*b),
        TypedValue::Enum16(x) => u64::from(*x),
        TypedValue::String(_) => 0,
    }
}

/// 提取 `TypedValue` 的 i64 值（整数变体精确；浮点/非数值截断或归零）。
#[inline]
fn typed_to_i64(v: &TypedValue) -> i64 {
    match v {
        TypedValue::U64(x) => *x as i64,
        TypedValue::I64(x) => *x,
        TypedValue::I32(x) => i64::from(*x),
        TypedValue::F32(x) => *x as i64,
        TypedValue::F64(x) => *x as i64,
        TypedValue::Bool(b) => i64::from(*b),
        TypedValue::Enum16(x) => i64::from(*x),
        TypedValue::String(_) => 0,
    }
}

// ─── AggregateState ───

/// CQ 引擎和流计算引擎共享的**类型化**增量聚合状态。
///
/// 维护一次聚合窗口内的累计值，支持 AVG、SUM、COUNT、MIN、MAX、
/// FIRST、LAST、STDDEV 等标准聚合函数的增量计算。
#[derive(Debug, Clone)]
pub struct AggregateState {
    /// 源数值类型（决定累加域与 SUM/MIN/MAX/FIRST/LAST 的输出类型）。
    pub result_type: DataType,
    /// 累加和 Σx（按 result_type 累加域：U64→u64、I64→i64、I32→i64、F32→f64、F64→f64）。
    pub sum: Numeric,
    /// 最小值（源域）。
    pub min: Numeric,
    /// 最大值（源域）。
    pub max: Numeric,
    /// 第一条记录的值（源域）。
    pub first: Numeric,
    /// 最后一条记录的值（源域）。
    pub last: Numeric,
    /// 记录数。
    pub count: u64,
    /// 平方和 Σx² (仅用于 AVG/STDDEV 增量计算)。
    pub sum_squared: f64,
    /// 是否已接收过有效数据。
    pub has_value: bool,
    /// 第一条记录的时间戳 (epoch 纳秒)。
    pub first_ts: i64,
    /// 最后一条记录的时间戳 (epoch 纳秒)。
    pub last_ts: i64,
}

impl AggregateState {
    /// 创建一个空的类型化聚合状态。
    ///
    /// # Arguments
    /// * `result_type` - 源数值类型，决定累加域与输出域。
    #[inline]
    pub fn new(result_type: DataType) -> Self {
        Self {
            result_type,
            sum: Numeric::zero_sum(result_type),
            min: Numeric::zero_source(result_type),
            max: Numeric::zero_source(result_type),
            first: Numeric::zero_source(result_type),
            last: Numeric::zero_source(result_type),
            count: 0,
            sum_squared: 0.0,
            has_value: false,
            first_ts: 0,
            last_ts: 0,
        }
    }

    /// 累加一个带类型值（不带时间戳，FIRST/LAST 无时间戳语义）。
    ///
    /// 按 `result_type` 域累加：U64→u64、I64→i64、I32→i64 加宽、F32→f64 加宽、
    /// F64→f64；整数溢出 wrapping。
    #[inline]
    pub fn accumulate(&mut self, v: &TypedValue) {
        let sum_add = Numeric::sum_addend(v, self.result_type);
        let val = Numeric::from_source(v, self.result_type);

        self.sum = self.sum.wrapping_add(&sum_add);
        self.count = self.count.wrapping_add(1);
        self.sum_squared += v.as_f64() * v.as_f64();

        if !self.has_value {
            self.min = val;
            self.max = val;
            self.first = val;
            self.last = val;
            self.has_value = true;
        } else {
            if Numeric::less(&val, &self.min) {
                self.min = val;
            }
            if Numeric::greater(&val, &self.max) {
                self.max = val;
            }
            self.last = val;
        }
    }

    /// 累加一个带类型值（带时间戳，支持 FIRST/LAST 时间戳比较）。
    ///
    /// # Arguments
    /// * `v` - 数据点值
    /// * `ts` - 数据点时间戳 (epoch 纳秒)
    #[inline]
    pub fn accumulate_with_ts(&mut self, v: &TypedValue, ts: i64) {
        let sum_add = Numeric::sum_addend(v, self.result_type);
        let val = Numeric::from_source(v, self.result_type);

        self.sum = self.sum.wrapping_add(&sum_add);
        self.count = self.count.wrapping_add(1);
        self.sum_squared += v.as_f64() * v.as_f64();

        if !self.has_value {
            self.min = val;
            self.max = val;
            self.first = val;
            self.first_ts = ts;
            self.has_value = true;
        } else {
            if Numeric::less(&val, &self.min) {
                self.min = val;
            }
            if Numeric::greater(&val, &self.max) {
                self.max = val;
            }
        }

        if ts > self.last_ts {
            self.last_ts = ts;
            self.last = val;
        } else if ts == self.last_ts {
            // 同时间戳，取后到达的 (写后保留)
            self.last = val;
        }
    }

    /// 获取指定聚合函数的**带类型**结果。
    ///
    /// # Returns
    /// - `COUNT` → `TypedValue::U64`
    /// - `AVG`/`STDDEV` → `TypedValue::F64`
    /// - `SUM`/`MIN`/`MAX`/`FIRST`/`LAST` → 源类型（按 `result_type`）
    ///
    /// 空状态：`COUNT`→U64(0)、`AVG`/`STDDEV`→F64(0.0)、
    /// 其余→源类型零值。
    pub fn result(&self, function: AggFunction) -> TypedValue {
        match function {
            AggFunction::Count => TypedValue::U64(self.count),
            AggFunction::Avg => {
                if self.count > 0 {
                    TypedValue::F64(self.sum.as_f64() / self.count as f64)
                } else {
                    TypedValue::F64(0.0)
                }
            }
            AggFunction::StdDev => {
                if self.count > 1 {
                    let mean = self.sum.as_f64() / self.count as f64;
                    let variance = (self.sum_squared / self.count as f64) - (mean * mean);
                    if variance > 0.0 {
                        TypedValue::F64(variance.sqrt())
                    } else {
                        TypedValue::F64(0.0)
                    }
                } else {
                    TypedValue::F64(0.0)
                }
            }
            AggFunction::Sum => self.sum_typed(),
            AggFunction::Min => self.min_typed(),
            AggFunction::Max => self.max_typed(),
            AggFunction::First => self.first_typed(),
            AggFunction::Last => self.last_typed(),
        }
    }

    /// `SUM` 的带类型结果（累加域 cast 回源类型：I32/F32 输出时截断回）。
    pub fn sum_typed(&self) -> TypedValue {
        match self.result_type {
            DataType::U64 => TypedValue::U64(self.sum.as_u64()),
            DataType::I64 => TypedValue::I64(self.sum.as_i64()),
            DataType::I32 => TypedValue::I32(self.sum.as_i64() as i32),
            DataType::F32 => TypedValue::F32(self.sum.as_f64() as f32),
            DataType::F64 => TypedValue::F64(self.sum.as_f64()),
            _ => TypedValue::F64(self.sum.as_f64()),
        }
    }

    /// `MIN` 的带类型结果（源域）。
    #[inline]
    pub fn min_typed(&self) -> TypedValue {
        self.min.to_typed_value()
    }

    /// `MAX` 的带类型结果（源域）。
    #[inline]
    pub fn max_typed(&self) -> TypedValue {
        self.max.to_typed_value()
    }

    /// `FIRST` 的带类型结果（源域）。
    #[inline]
    pub fn first_typed(&self) -> TypedValue {
        self.first.to_typed_value()
    }

    /// `LAST` 的带类型结果（源域）。
    #[inline]
    pub fn last_typed(&self) -> TypedValue {
        self.last.to_typed_value()
    }

    /// 合并另一个 `AggregateState`（用于 CQ 层级间 sum+count 传递）。
    ///
    /// 合并后 `self` 包含两个状态的联合信息：
    /// - `sum`（按类型 wrapping 加）、`count`、`sum_squared` 直接相加
    /// - `min` 取两者较小值（源域精确比较）
    /// - `max` 取两者较大值（源域精确比较）
    /// - `first`/`first_ts` 取较早的时间戳
    /// - `last`/`last_ts` 取较晚的时间戳
    ///
    /// # Arguments
    /// * `other` - 待合并的聚合状态（须与 `self` 同 `result_type`）
    pub fn merge(&mut self, other: &AggregateState) {
        if !other.has_value {
            return; // 对方为空，无需合并
        }

        self.sum = self.sum.wrapping_add(&other.sum);
        self.count = self.count.wrapping_add(other.count);
        self.sum_squared += other.sum_squared;

        if !self.has_value {
            // self 为空：直接继承 other
            self.min = other.min;
            self.max = other.max;
            self.first = other.first;
            self.first_ts = other.first_ts;
            self.last = other.last;
            self.last_ts = other.last_ts;
            self.has_value = true;
            return;
        }

        if Numeric::less(&other.min, &self.min) {
            self.min = other.min;
        }
        if Numeric::greater(&other.max, &self.max) {
            self.max = other.max;
        }

        // 取较早的 FIRST
        if other.first_ts < self.first_ts {
            self.first = other.first;
            self.first_ts = other.first_ts;
        }
        // 取较晚的 LAST
        if other.last_ts > self.last_ts {
            self.last = other.last;
            self.last_ts = other.last_ts;
        } else if other.last_ts == self.last_ts && other.last_ts > 0 {
            self.last = other.last; // 同时间戳，后合并的优先
        }
    }

    /// 重置聚合状态为初始空状态（保留 `result_type`）。
    #[inline]
    pub fn reset(&mut self) {
        *self = Self::new(self.result_type);
    }
}

impl Default for AggregateState {
    /// 默认使用 `DataType::F64` 作为源类型（等价旧 f64 版行为）。
    #[inline]
    fn default() -> Self {
        Self::new(DataType::F64)
    }
}

// ─── AggFunction ───

/// 聚合函数枚举（放在 common 中供 CQ 和 Stream 共享）。
///
/// 每个变体对应标准 SQL 聚合函数。
/// `Serialize/Deserialize` 派生用于配置持久化 (config.db)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggFunction {
    /// 平均值 AVG(x)
    Avg,
    /// 求和 SUM(x)
    Sum,
    /// 计数 COUNT(x)
    Count,
    /// 最小值 MIN(x)
    Min,
    /// 最大值 MAX(x)
    Max,
    /// 第一条记录 FIRST(x)
    First,
    /// 最后一条记录 LAST(x)
    Last,
    /// 总体标准差 STDDEV(x)
    StdDev,
}

/// 计算聚合结果列的物理数据类型（CQ 与 Stream 共用的单一映射表）。
///
/// - `Count` 结果为非负整数 → `U64`。
/// - `Avg` / `StdDev` 结果为浮点 → `F64`。
/// - `Sum` / `Min` / `Max` / `First` / `Last` 保留数值源列类型；
///   非数值源列（Bool/Enum16/String）折叠为 `F64`。
pub fn agg_result_type(function: AggFunction, source_type: DataType) -> DataType {
    match function {
        AggFunction::Count => DataType::U64,
        AggFunction::Avg | AggFunction::StdDev => DataType::F64,
        AggFunction::Sum
        | AggFunction::Min
        | AggFunction::Max
        | AggFunction::First
        | AggFunction::Last => match source_type {
            DataType::F64 | DataType::F32 | DataType::I64 | DataType::U64 | DataType::I32 => {
                source_type
            }
            _ => DataType::F64,
        },
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_state_typed() {
        let state = AggregateState::new(DataType::F64);
        assert!(!state.has_value);
        assert_eq!(state.count, 0);
        assert_eq!(state.sum_typed(), TypedValue::F64(0.0));
        // 空状态：COUNT→U64(0)，AVG/STDDEV→F64(0.0)，SUM→源类型零值
        assert_eq!(state.result(AggFunction::Count), TypedValue::U64(0));
        assert_eq!(state.result(AggFunction::Avg), TypedValue::F64(0.0));
        assert_eq!(state.result(AggFunction::StdDev), TypedValue::F64(0.0));
        assert_eq!(state.result(AggFunction::Sum), TypedValue::F64(0.0));
        assert_eq!(state.result(AggFunction::Min), TypedValue::F64(0.0));
        assert_eq!(state.result(AggFunction::Max), TypedValue::F64(0.0));
        assert_eq!(state.result(AggFunction::First), TypedValue::F64(0.0));
        assert_eq!(state.result(AggFunction::Last), TypedValue::F64(0.0));
    }

    #[test]
    fn test_u64_large_sum_no_precision_loss() {
        // U64 大值（> 2^53）SUM 不丢精度：这是 f64 折叠无法保证的关键用例。
        let mut state = AggregateState::new(DataType::U64);
        let a = 9_007_199_254_740_992u64; // 2^53（可精确表示）
        let b = 1u64;
        state.accumulate(&TypedValue::U64(a));
        state.accumulate(&TypedValue::U64(b));

        // 和 = 2^53 + 1（奇数，f64 无法精确表示，会折叠回 2^53）
        assert_eq!(state.count, 2);
        assert_eq!(state.sum_typed(), TypedValue::U64(a + b));
        assert_eq!(state.result(AggFunction::Sum), TypedValue::U64(a + b));
        assert_eq!(state.min_typed(), TypedValue::U64(1));
        assert_eq!(state.max_typed(), TypedValue::U64(a));
    }

    #[test]
    fn test_u64_wrapping_overflow() {
        let mut state = AggregateState::new(DataType::U64);
        state.accumulate(&TypedValue::U64(u64::MAX));
        state.accumulate(&TypedValue::U64(1));
        // wrapping 溢出：MAX + 1 = 0
        assert_eq!(state.result(AggFunction::Sum), TypedValue::U64(0));
    }

    #[test]
    fn test_i64_native_accumulate() {
        let mut state = AggregateState::new(DataType::I64);
        state.accumulate(&TypedValue::I64(-2));
        state.accumulate(&TypedValue::I64(10));
        assert_eq!(state.result(AggFunction::Sum), TypedValue::I64(8));
        assert_eq!(state.result(AggFunction::Min), TypedValue::I64(-2));
        assert_eq!(state.result(AggFunction::Max), TypedValue::I64(10));
        assert_eq!(state.result(AggFunction::First), TypedValue::I64(-2));
        assert_eq!(state.result(AggFunction::Last), TypedValue::I64(10));
        assert_eq!(state.result(AggFunction::Count), TypedValue::U64(2));
    }

    #[test]
    fn test_i32_widened_accumulate_and_cast_back() {
        // I32 用 i64 加宽累加（不溢出），输出 cast 回 I32。
        let mut state = AggregateState::new(DataType::I32);
        // 2_000_000_000 + 2_000_000_000 会溢出 i32，但 i64 加宽不会。
        state.accumulate(&TypedValue::I32(2_000_000_000));
        state.accumulate(&TypedValue::I32(2_000_000_000));
        // 加宽域和 = 4_000_000_000，cast 回 i32 截断（文档化语义）。
        assert_eq!(state.sum.as_i64(), 4_000_000_000);
        assert_eq!(
            state.result(AggFunction::Sum),
            TypedValue::I32(4_000_000_000i64 as i32)
        );
        assert_eq!(
            state.result(AggFunction::Min),
            TypedValue::I32(2_000_000_000)
        );
        assert_eq!(
            state.result(AggFunction::Max),
            TypedValue::I32(2_000_000_000)
        );
    }

    #[test]
    fn test_f32_widened_accumulate_and_cast_back() {
        // F32 用 f64 加宽累加，输出 cast 回 F32。
        let mut state = AggregateState::new(DataType::F32);
        state.accumulate(&TypedValue::F32(1.5));
        state.accumulate(&TypedValue::F32(2.5));
        // 加宽域 f64 和 = 4.0
        assert_eq!(state.sum.as_f64(), 4.0);
        assert_eq!(state.result(AggFunction::Sum), TypedValue::F32(4.0));
        assert_eq!(state.result(AggFunction::Min), TypedValue::F32(1.5));
        assert_eq!(state.result(AggFunction::Max), TypedValue::F32(2.5));
    }

    #[test]
    fn test_f64_accumulate_and_avg() {
        let mut state = AggregateState::new(DataType::F64);
        state.accumulate(&TypedValue::F64(10.0));
        state.accumulate(&TypedValue::F64(20.0));
        state.accumulate(&TypedValue::F64(30.0));
        assert_eq!(state.result(AggFunction::Sum), TypedValue::F64(60.0));
        assert_eq!(state.result(AggFunction::Avg), TypedValue::F64(20.0));
        assert_eq!(state.result(AggFunction::Min), TypedValue::F64(10.0));
        assert_eq!(state.result(AggFunction::Max), TypedValue::F64(30.0));
    }

    #[test]
    fn test_avg_returns_f64_even_for_u64_source() {
        // AVG 结果恒为 F64（源类型 U64，结果 F64）。
        let mut state = AggregateState::new(DataType::U64);
        state.accumulate(&TypedValue::U64(1));
        state.accumulate(&TypedValue::U64(2));
        assert_eq!(state.result(AggFunction::Avg), TypedValue::F64(1.5));
    }

    #[test]
    fn test_accumulate_with_ts_first_last() {
        let mut state = AggregateState::new(DataType::F64);
        state.accumulate_with_ts(&TypedValue::F64(10.0), 1000);
        state.accumulate_with_ts(&TypedValue::F64(20.0), 2000);
        state.accumulate_with_ts(&TypedValue::F64(15.0), 1500); // 中间插入

        assert_eq!(state.first_typed(), TypedValue::F64(10.0));
        assert_eq!(state.first_ts, 1000);
        assert_eq!(state.last_typed(), TypedValue::F64(20.0));
        assert_eq!(state.last_ts, 2000);

        // 同时间戳：后到达保留
        state.accumulate_with_ts(&TypedValue::F64(25.0), 2000);
        assert_eq!(state.last_typed(), TypedValue::F64(25.0));
        assert_eq!(state.last_ts, 2000);
    }

    #[test]
    fn test_stddev() {
        let mut state = AggregateState::new(DataType::F64);
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            state.accumulate(&TypedValue::F64(v));
        }
        // 总体标准差 ≈ 2.0
        match state.result(AggFunction::StdDev) {
            TypedValue::F64(stddev) => assert!((stddev - 2.0).abs() < 0.1, "stddev={stddev}"),
            other => panic!("STDDEV 应为 F64，实际 {other:?}"),
        }
    }

    #[test]
    fn test_merge_two_states_typed() {
        let mut s1 = AggregateState::new(DataType::I64);
        s1.accumulate_with_ts(&TypedValue::I64(10), 1000);
        s1.accumulate_with_ts(&TypedValue::I64(20), 2000);

        let mut s2 = AggregateState::new(DataType::I64);
        s2.accumulate_with_ts(&TypedValue::I64(30), 3000);
        s2.accumulate_with_ts(&TypedValue::I64(40), 4000);

        s1.merge(&s2);

        assert_eq!(s1.count, 4);
        assert_eq!(s1.result(AggFunction::Sum), TypedValue::I64(100));
        assert_eq!(s1.result(AggFunction::Min), TypedValue::I64(10));
        assert_eq!(s1.result(AggFunction::Max), TypedValue::I64(40));
        assert_eq!(s1.first_ts, 1000);
        assert_eq!(s1.last_ts, 4000);
        assert_eq!(s1.result(AggFunction::Avg), TypedValue::F64(25.0));
    }

    #[test]
    fn test_merge_into_empty_typed() {
        let mut s1 = AggregateState::new(DataType::I64);
        let mut s2 = AggregateState::new(DataType::I64);
        s2.accumulate_with_ts(&TypedValue::I64(10), 1000);

        s1.merge(&s2);

        assert_eq!(s1.count, 1);
        assert_eq!(s1.result(AggFunction::Sum), TypedValue::I64(10));
        assert_eq!(s1.result(AggFunction::Min), TypedValue::I64(10));
        assert_eq!(s1.result(AggFunction::Max), TypedValue::I64(10));
        assert_eq!(s1.first_ts, 1000);
    }

    #[test]
    fn test_reset_preserves_result_type() {
        let mut state = AggregateState::new(DataType::U64);
        state.accumulate(&TypedValue::U64(42));
        state.reset();

        assert!(!state.has_value);
        assert_eq!(state.count, 0);
        assert_eq!(state.result_type, DataType::U64);
        assert_eq!(state.result(AggFunction::Sum), TypedValue::U64(0));
    }

    #[test]
    fn test_agg_result_type_mapping() {
        assert_eq!(
            agg_result_type(AggFunction::Count, DataType::F64),
            DataType::U64
        );
        assert_eq!(
            agg_result_type(AggFunction::Avg, DataType::U64),
            DataType::F64
        );
        assert_eq!(
            agg_result_type(AggFunction::StdDev, DataType::F32),
            DataType::F64
        );
        assert_eq!(
            agg_result_type(AggFunction::Sum, DataType::U64),
            DataType::U64
        );
        assert_eq!(
            agg_result_type(AggFunction::Min, DataType::I32),
            DataType::I32
        );
        assert_eq!(
            agg_result_type(AggFunction::Max, DataType::F32),
            DataType::F32
        );
        assert_eq!(
            agg_result_type(AggFunction::First, DataType::I64),
            DataType::I64
        );
        assert_eq!(
            agg_result_type(AggFunction::Last, DataType::F64),
            DataType::F64
        );
        // 非数值源列 → F64
        assert_eq!(
            agg_result_type(AggFunction::Sum, DataType::String),
            DataType::F64
        );
        assert_eq!(
            agg_result_type(AggFunction::Min, DataType::Bool),
            DataType::F64
        );
    }
}
