//! CQ 聚合执行器。
//!
//! `CqAggregationExecutor` 执行单个 CQ 窗口的聚合计算。
//! 支持层级 sum+count 传递（上层从下层聚合，实现正确的 AVG 修正），
//! 以及 MIN/MAX 通过 `_min`/`_max` 辅助列的层级传递，
//! FIRST/LAST 在 CQ_10s 层的带时间戳处理。
//!
//! ## 层级传递策略
//!
//! - CQ_10s: 从 raw 表直接聚合（带逐行时间戳）
//! - CQ_1m:  从 CQ_10s 表聚合（SUM of _sum, SUM of _count → AVG = SUM(_sum)/SUM(_count)；
//!           MIN of _min, MAX of _max → 真 MIN/MAX 层级传递）
//! - CQ_1h:  从 CQ_1m 表聚合
//! - CQ_1d:  从 CQ_1h 表聚合
//!
//! FIRST/LAST 仅 CQ_10s 层使用（无法通过 sum+count 传递）；上层显式报错
//! `UnsupportedAggregation`，绝不静默返回错值（PRD Q2 裁决）。

use std::sync::Arc;

use sensearcdb_common::{AggFunction, AggregateState, SenseArcError, TypedValue};

use crate::column_resolver::CqColumnResolver;
#[cfg(test)]
use crate::column_resolver::FormulaColumnResolver;
use crate::definition::CqDefinition;
use crate::engine::CqQueryExecutor;
use crate::layout::{typed_max_sentinel, typed_min_sentinel, typed_zero};

// ─── CqWindowResult ───

/// CQ 窗口聚合结果。
///
/// 包含每个聚合列的最终结果值，以及用于层级传递的中间值。
/// 所有数值字段均**类型化**（`TypedValue`），`_count` 恒 `u64`，
/// 时间戳恒 `i64`，彻底消除 f64 折叠。
#[derive(Debug, Clone)]
pub struct CqWindowResult {
    /// 目标 CQ 表 ID。
    pub target_table_id: u32,
    /// 窗口结束时间（纳秒），数据点时间戳 = window_start。
    pub window_end: i64,
    /// 聚合结果列（与 source_columns 一一对应）。
    pub columns: Vec<TypedValue>,
    /// sum 中间值（供下层 CQ 使用，源类型）。
    pub _sum: Vec<TypedValue>,
    /// count 中间值。
    pub _count: Vec<u64>,
    /// min 中间值（A4 遗留，层级 MIN 传递，源类型）。
    pub _min: Vec<TypedValue>,
    /// max 中间值（A4 遗留，层级 MAX 传递，源类型）。
    pub _max: Vec<TypedValue>,
    /// FIRST 值对应时间戳（epoch 纳秒）。
    pub _first_ts: Vec<i64>,
    /// LAST 值对应时间戳（epoch 纳秒）。
    pub _last_ts: Vec<i64>,
    /// 原始行数。
    pub row_count: u64,
}

// ─── CqAggregationExecutor ───

/// CQ 聚合执行器。
///
/// 执行单个窗口的聚合计算，支持层级 sum+count 传递。
pub struct CqAggregationExecutor {
    /// 辅助列 ID 解析器（schema 真实 ID / 公式回退）。
    resolver: Arc<dyn CqColumnResolver>,
}

impl CqAggregationExecutor {
    /// 创建新的聚合执行器（公式回退解析器）。
    #[cfg(test)]
    #[inline]
    pub fn new() -> Self {
        Self {
            resolver: Arc::new(FormulaColumnResolver),
        }
    }

    /// 创建带指定辅助列 ID 解析器的聚合执行器。
    #[inline]
    pub fn with_resolver(resolver: Arc<dyn CqColumnResolver>) -> Self {
        Self { resolver }
    }

    /// 执行指定窗口的聚合。
    ///
    /// # Arguments
    /// * `def` - CQ 定义。
    /// * `window_start` - 窗口起始时间（epoch 纳秒）。
    /// * `window_end` - 窗口结束时间（epoch 纳秒）。
    /// * `query_executor` - 查询执行器。
    ///
    /// # Returns
    /// `CqWindowResult` 包含所有列的聚合结果和中间值。
    pub fn execute(
        &self,
        def: &CqDefinition,
        window_start: i64,
        window_end: i64,
        query_executor: &dyn CqQueryExecutor,
    ) -> Result<CqWindowResult, SenseArcError> {
        // 确定源表与聚合模式
        let (source_table_id, is_hierarchical) = self.resolve_source(def);

        if is_hierarchical {
            // 层级传递：从下层 CQ 表读取 _sum/_count/_min/_max 辅助列。
            // 辅助列 ID 由 schema 真实分配（经 resolver 查询），不再现场编造。
            let aux_column_ids = build_aux_column_ids(self.resolver.as_ref(), def)?;
            let (aux_columns_typed, _ts) = query_executor.query_raw_with_ts(
                source_table_id,
                &aux_column_ids,
                window_start,
                window_end,
            )?;

            if aux_columns_typed.is_empty() || aux_columns_typed[0].is_empty() {
                return Ok(self.empty_result(def, window_end));
            }

            // 类型化直通：按源类型累加（U64→u64、I64→i64、I32→i64 加宽、F32→f64、F64→f64）。
            self.aggregate_hierarchical(def, &aux_columns_typed, window_end)
        } else {
            // 直接从 raw 表读取源列（带逐行时间戳）
            let (raw_columns_typed, timestamps) = query_executor.query_raw_with_ts(
                source_table_id,
                &def.source_columns,
                window_start,
                window_end,
            )?;

            if raw_columns_typed.is_empty() || raw_columns_typed[0].is_empty() {
                return Ok(self.empty_result(def, window_end));
            }

            let row_count = raw_columns_typed[0].len() as u64;

            // 类型化直通：按源类型聚合（移除 Step2 的 columns_as_f64 桥接）。
            self.aggregate_direct(def, &raw_columns_typed, &timestamps, window_end, row_count)
        }
    }

    /// 构建空窗口结果（无数据时返回类型化零值 / 空状态哨兵）。
    fn empty_result(&self, def: &CqDefinition, window_end: i64) -> CqWindowResult {
        let col_count = def.source_columns.len();
        let mut columns = Vec::with_capacity(col_count);
        let mut sums = Vec::with_capacity(col_count);
        let mut mins = Vec::with_capacity(col_count);
        let mut maxs = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let source_type = def.source_type(i);
            let func = def.aggregations.get(i).copied().unwrap_or(AggFunction::Avg);
            columns.push(match func {
                AggFunction::Count => TypedValue::U64(0),
                AggFunction::Avg | AggFunction::StdDev => TypedValue::F64(0.0),
                _ => typed_zero(source_type),
            });
            sums.push(typed_zero(source_type));
            mins.push(typed_min_sentinel(source_type));
            maxs.push(typed_max_sentinel(source_type));
        }
        CqWindowResult {
            target_table_id: def.target_table_id,
            window_end,
            columns,
            _sum: sums,
            _count: vec![0u64; col_count],
            _min: mins,
            _max: maxs,
            _first_ts: vec![0; col_count],
            _last_ts: vec![0; col_count],
            row_count: 0,
        }
    }

    /// 确定聚合的源表。
    ///
    /// 有下层粒度时返回 `(下层 CQ 表 ID, true)`，启用层级传递；
    /// 否则（CQ_10s）返回 `(raw 源表 ID, false)`，直接聚合。
    fn resolve_source(&self, def: &CqDefinition) -> (u32, bool) {
        match def.lower_target_table_id {
            Some(lower_table_id) => (lower_table_id, true),
            None => (def.source_table_id, false),
        }
    }

    /// 直接聚合：从 raw 表数据计算聚合（带逐行时间戳，支持 FIRST/LAST）。
    fn aggregate_direct(
        &self,
        def: &CqDefinition,
        raw_columns: &[Vec<Option<TypedValue>>],
        timestamps: &[i64],
        window_end: i64,
        row_count: u64,
    ) -> Result<CqWindowResult, SenseArcError> {
        let col_count = def.source_columns.len();
        // 类型化状态：每列按源类型构造，U64 源真 u64 累加，不折叠为 f64。
        let mut states: Vec<AggregateState> = (0..col_count)
            .map(|i| AggregateState::new(def.source_type(i)))
            .collect();
        let row_len = raw_columns[0].len();

        // 累加所有行（时间戳统一纳秒，D6）。缺失值 `None` 跳过（P2 语义，不补零）。
        for row_idx in 0..row_len {
            let ts = timestamps.get(row_idx).copied().unwrap_or(0);
            for (col_idx, state) in states.iter_mut().enumerate() {
                if let Some(Some(value)) = raw_columns.get(col_idx).and_then(|col| col.get(row_idx))
                {
                    state.accumulate_with_ts(value, ts);
                }
            }
        }

        // 提取结果
        let columns: Vec<TypedValue> = states
            .iter()
            .enumerate()
            .map(|(i, state)| {
                let func = def.aggregations.get(i).copied().unwrap_or(AggFunction::Avg);
                state.result(func)
            })
            .collect();

        let _sum: Vec<TypedValue> = states.iter().map(|s| s.sum_typed()).collect();
        let _count: Vec<u64> = states.iter().map(|s| s.count).collect();
        // P2 边界修复：某列全 None（has_value=false）时，_min/_max 必须写哨兵
        // （typed_min_sentinel/typed_max_sentinel），而非 min_typed() 的零值。
        // 否则层级 MIN/MAX 会被空窗口的 0 污染（他窗口为正/负值时取错）。
        let _min: Vec<TypedValue> = states
            .iter()
            .map(|s| {
                if s.has_value {
                    s.min_typed()
                } else {
                    typed_min_sentinel(s.result_type)
                }
            })
            .collect();
        let _max: Vec<TypedValue> = states
            .iter()
            .map(|s| {
                if s.has_value {
                    s.max_typed()
                } else {
                    typed_max_sentinel(s.result_type)
                }
            })
            .collect();
        let _first_ts: Vec<i64> = states.iter().map(|s| s.first_ts).collect();
        let _last_ts: Vec<i64> = states.iter().map(|s| s.last_ts).collect();

        Ok(CqWindowResult {
            target_table_id: def.target_table_id,
            window_end,
            columns,
            _sum,
            _count,
            _min,
            _max,
            _first_ts,
            _last_ts,
            row_count,
        })
    }

    /// 层级聚合：从下层 CQ 表的 `_sum`/`_count`/`_min`/`_max` 辅助列计算加权聚合。
    ///
    /// 下层 CQ 表的辅助列布局（固定，见 layout.rs）：
    ///   第 i 源列 → `_sum` 位于 `aux_columns[4*i+0]`，`_count` 位于 `aux_columns[4*i+1]`，
    ///   `_min` 位于 `aux_columns[4*i+2]`，`_max` 位于 `aux_columns[4*i+3]`。
    ///
    /// 核心语义（PRD §4.2，避免 avg-of-avg 偏差）：
    ///   AVG = SUM(_sum) / SUM(_count)
    ///   MIN = MIN(_min)；MAX = MAX(_max)
    ///
    /// 类型化：`Σ_sum` 按源类型原生累加（U64→u64、I64→i64、I32→i64 加宽、
    /// F32→f64 加宽、F64→f64）；`Σ_count` 用 u64；加权 AVG 最后转 f64 除法；
    /// MIN/MAX 按源类型比较；COUNT=u64。
    fn aggregate_hierarchical(
        &self,
        def: &CqDefinition,
        aux_columns: &[Vec<Option<TypedValue>>],
        window_end: i64,
    ) -> Result<CqWindowResult, SenseArcError> {
        let col_count = def.source_columns.len();
        let row_len = aux_columns[0].len();

        // 每列三条类型化累加路径：sum（Σ_sum）、min（MIN _min）、max（MAX _max），
        // count 单独用 u64 累加（Σ_count，非行数）。
        let mut sum_states: Vec<AggregateState> = (0..col_count)
            .map(|i| AggregateState::new(def.source_type(i)))
            .collect();
        let mut min_states: Vec<AggregateState> = (0..col_count)
            .map(|i| AggregateState::new(def.source_type(i)))
            .collect();
        let mut max_states: Vec<AggregateState> = (0..col_count)
            .map(|i| AggregateState::new(def.source_type(i)))
            .collect();
        let mut counts: Vec<u64> = vec![0u64; col_count];

        for row_idx in 0..row_len {
            for col_idx in 0..col_count {
                let sum_col = 4 * col_idx;
                let count_col = 4 * col_idx + 1;
                let min_col = 4 * col_idx + 2;
                let max_col = 4 * col_idx + 3;

                // 先取该窗口该列的 _count，判断是否为空窗口（_count==0）。
                let window_count = aux_columns
                    .get(count_col)
                    .and_then(|c| c.get(row_idx))
                    .and_then(|v| v.as_ref())
                    .map(typed_to_u64)
                    .unwrap_or(0);

                if let Some(Some(v)) = aux_columns.get(sum_col).and_then(|c| c.get(row_idx)) {
                    sum_states[col_idx].accumulate(v);
                }
                counts[col_idx] = counts[col_idx].wrapping_add(window_count);

                // P2 边界修复：空窗口（_count==0）的 _min/_max 是哨兵值，
                // 必须跳过，否则哨兵（如 i64::MAX/i64::MIN）会污染层级 MIN/MAX。
                if window_count > 0 {
                    if let Some(Some(v)) = aux_columns.get(min_col).and_then(|c| c.get(row_idx)) {
                        min_states[col_idx].accumulate(v);
                    }
                    if let Some(Some(v)) = aux_columns.get(max_col).and_then(|c| c.get(row_idx)) {
                        max_states[col_idx].accumulate(v);
                    }
                }
            }
        }

        let mut columns: Vec<TypedValue> = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let func = def.aggregations.get(i).copied().unwrap_or(AggFunction::Avg);
            let value = match func {
                // 加权平均：AVG = Σ_sum / Σ_count（最后转 f64 除法），count 为 0 返回 0.0。
                AggFunction::Avg => {
                    if counts[i] > 0 {
                        TypedValue::F64(sum_states[i].sum.as_f64() / counts[i] as f64)
                    } else {
                        TypedValue::F64(0.0)
                    }
                }
                AggFunction::Sum => sum_states[i].sum_typed(),
                AggFunction::Count => TypedValue::U64(counts[i]),
                // MIN/MAX：真层级传递（读取下层 _min/_max），按源类型比较。
                AggFunction::Min => {
                    if counts[i] > 0 {
                        min_states[i].min_typed()
                    } else {
                        typed_min_sentinel(def.source_type(i))
                    }
                }
                AggFunction::Max => {
                    if counts[i] > 0 {
                        max_states[i].max_typed()
                    } else {
                        typed_max_sentinel(def.source_type(i))
                    }
                }
                // FIRST/LAST 无法通过 sum+count 传递，STDDEV 需要 _sum_squared
                // 辅助列（未建列）。均显式报错，绝不静默返回错值。
                AggFunction::First | AggFunction::Last | AggFunction::StdDev => {
                    return Err(SenseArcError::UnsupportedAggregation(format!(
                        "层级聚合不支持 {func:?}（FIRST/LAST 仅 CQ_10s 支持；STDDEV 需 _sum_squared 辅助列）"
                    )));
                }
            };
            columns.push(value);
        }

        Ok(CqWindowResult {
            target_table_id: def.target_table_id,
            window_end,
            columns,
            _sum: sum_states.iter().map(|s| s.sum_typed()).collect(),
            _count: counts,
            _min: min_states.iter().map(|s| s.min_typed()).collect(),
            _max: max_states.iter().map(|s| s.max_typed()).collect(),
            _first_ts: vec![0; col_count],
            _last_ts: vec![0; col_count],
            row_count: row_len as u64,
        })
    }
}

/// 构建层级聚合所需的辅助列 ID 列表。
///
/// 通过 [`CqColumnResolver`] 从 schema 查询真实 `column_id`（不再现场按公式编造）。
/// 返回按「源列下标 × 6 类辅助列」展平的 ID 列表，与 [`crate::layout::CqAuxColumn::ALL`]
/// 顺序一致；层级聚合仅消费前 4 类（`_sum`/`_count`/`_min`/`_max`）。
fn build_aux_column_ids(
    resolver: &dyn CqColumnResolver,
    def: &CqDefinition,
) -> Result<Vec<u16>, SenseArcError> {
    resolver.resolve_aux_column_ids(def.target_table_id, &def.source_columns)
}

/// 提取 `TypedValue` 的 u64 值（`_count` 列恒 U64 精确；其余类型防御性转换）。
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

#[cfg(test)]
impl Default for CqAggregationExecutor {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definition::{CqDefinition, CqGranularity};
    use sensearcdb_common::{AggFunction, DataType, SenseArcError};

    /// Mock query executor that returns fixed data (columns + timestamps).
    struct MockQueryExecutor {
        columns: Vec<Vec<Option<TypedValue>>>,
        timestamps: Vec<i64>,
    }

    impl CqQueryExecutor for MockQueryExecutor {
        fn query_raw_with_ts(
            &self,
            _table_id: u32,
            _column_ids: &[u16],
            _time_start: i64,
            _time_end: i64,
        ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
            Ok((self.columns.clone(), self.timestamps.clone()))
        }
    }

    /// 便捷：以 F64 列构造 mock（沿用旧测试语义）。
    fn mock_ts(columns: Vec<Vec<f64>>, timestamps: Vec<i64>) -> MockQueryExecutor {
        mock_typed(
            columns
                .into_iter()
                .map(|col| col.into_iter().map(|v| Some(TypedValue::F64(v))).collect())
                .collect(),
            timestamps,
        )
    }

    /// 类型化 mock：直接给定 `Vec<Vec<Option<TypedValue>>>`。
    fn mock_typed(
        columns: Vec<Vec<Option<TypedValue>>>,
        timestamps: Vec<i64>,
    ) -> MockQueryExecutor {
        MockQueryExecutor {
            columns,
            timestamps,
        }
    }

    #[test]
    fn test_direct_aggregation() {
        let executor = CqAggregationExecutor::new();

        let mut def = CqDefinition::new(
            "cq_10s_1".into(),
            1,
            101,
            CqGranularity::Cq10s,
            vec![1, 2],
            vec![AggFunction::Avg, AggFunction::Sum],
        );
        def.source_types = vec![DataType::F64, DataType::F64];

        // 两列，各3行数据，逐行时间戳（纳秒）
        let mock = mock_ts(
            vec![vec![10.0, 20.0, 30.0], vec![1.0, 2.0, 3.0]],
            vec![1_000, 2_000, 3_000],
        );

        let result = executor.execute(&def, 0, 10_000_000_000, &mock).unwrap();

        assert_eq!(result.target_table_id, 101);
        assert_eq!(result.row_count, 3);
        // AVG of [10,20,30] = 20.0
        assert_eq!(result.columns[0], TypedValue::F64(20.0));
        // SUM of [1,2,3] = 6.0
        assert_eq!(result.columns[1], TypedValue::F64(6.0));
        // _sum for first column = 60.0
        assert_eq!(result._sum[0], TypedValue::F64(60.0));
        // _count for first column = 3
        assert_eq!(result._count[0], 3);
        // _min/_max for first column
        assert_eq!(result._min[0], TypedValue::F64(10.0));
        assert_eq!(result._max[0], TypedValue::F64(30.0));
    }

    /// U64 源列 SUM/MIN/MAX 类型保真：2^53 大值不丢精度（f64 折叠会丢 +1）。
    #[test]
    fn test_u64_direct_sum_min_max_type_fidelity() {
        let executor = CqAggregationExecutor::new();

        let mut def = CqDefinition::new(
            "cq_10s_u64".into(),
            1,
            101,
            CqGranularity::Cq10s,
            vec![1, 1],
            vec![AggFunction::Sum, AggFunction::Min],
        );
        def.source_types = vec![DataType::U64, DataType::U64];

        let big = 9_007_199_254_740_992u64; // 2^53
        let mock = mock_typed(
            vec![
                vec![Some(TypedValue::U64(big)), Some(TypedValue::U64(1))],
                vec![Some(TypedValue::U64(big)), Some(TypedValue::U64(1))],
            ],
            vec![1_000, 2_000],
        );

        let result = executor.execute(&def, 0, 10_000_000_000, &mock).unwrap();

        // SUM = 2^53 + 1（奇数，f64 无法精确表示）
        assert_eq!(result.columns[0], TypedValue::U64(big + 1));
        assert_eq!(result._sum[0], TypedValue::U64(big + 1));
        // MIN = 1（U64 精确比较）
        assert_eq!(result.columns[1], TypedValue::U64(1));
        assert_eq!(result._min[1], TypedValue::U64(1));
        // _max 同样 U64
        assert_eq!(result._max[0], TypedValue::U64(big));
        assert_eq!(result._count[0], 2);
    }

    /// COUNT 结果恒 U64。
    #[test]
    fn test_count_result_is_u64() {
        let executor = CqAggregationExecutor::new();

        let mut def = CqDefinition::new(
            "cq_10s_count".into(),
            1,
            101,
            CqGranularity::Cq10s,
            vec![1],
            vec![AggFunction::Count],
        );
        def.source_types = vec![DataType::U64];

        let mock = mock_typed(
            vec![vec![
                Some(TypedValue::U64(10)),
                Some(TypedValue::U64(20)),
                Some(TypedValue::U64(30)),
            ]],
            vec![1_000, 2_000, 3_000],
        );

        let result = executor.execute(&def, 0, 10_000_000_000, &mock).unwrap();

        assert_eq!(result.columns[0], TypedValue::U64(3));
        assert_eq!(result._count[0], 3);
    }

    #[test]
    fn test_first_last_with_timestamp() {
        let executor = CqAggregationExecutor::new();

        let mut def = CqDefinition::new(
            "cq_10s_first_last".into(),
            1,
            101,
            CqGranularity::Cq10s,
            vec![1, 1],
            vec![AggFunction::First, AggFunction::Last],
        );
        def.source_types = vec![DataType::F64, DataType::F64];

        // 乱序时间戳：FIRST 取最早 ts 的值，LAST 取最晚 ts 的值
        let mock = mock_ts(
            vec![vec![10.0, 30.0, 20.0], vec![10.0, 30.0, 20.0]],
            vec![1_000_000_000, 3_000_000_000, 2_000_000_000],
        );

        let result = executor.execute(&def, 0, 10_000_000_000, &mock).unwrap();

        assert_eq!(result.columns[0], TypedValue::F64(10.0)); // FIRST = 最早 ts 值
        assert_eq!(result.columns[1], TypedValue::F64(30.0)); // LAST = 最晚 ts 值
        assert_eq!(result._first_ts[0], 1_000_000_000);
        assert_eq!(result._last_ts[1], 3_000_000_000);
    }

    #[test]
    fn test_empty_aggregation() {
        let executor = CqAggregationExecutor::new();

        let mut def = CqDefinition::new(
            "cq_test".into(),
            1,
            101,
            CqGranularity::Cq1m,
            vec![1],
            vec![AggFunction::Avg],
        );
        def.source_types = vec![DataType::F64];

        let mock = mock_ts(vec![vec![]], vec![]);

        let result = executor.execute(&def, 0, 60_000_000_000, &mock).unwrap();

        assert_eq!(result.row_count, 0);
        assert_eq!(result.columns[0], TypedValue::F64(0.0));
        assert_eq!(result._sum[0], TypedValue::F64(0.0));
        assert_eq!(result._count[0], 0);
        assert_eq!(result._min[0], TypedValue::F64(f64::MAX));
        assert_eq!(result._max[0], TypedValue::F64(f64::MIN));
    }

    #[test]
    fn test_hierarchical_weighted_avg() {
        let executor = CqAggregationExecutor::new();

        // CQ_1m 定义：1 个源列，AVG 聚合，下层指向 CQ_10s 表（table_id=10）。
        let mut def = CqDefinition::new(
            "cq_1m_1".into(),
            1,   // source_table_id (raw)
            101, // target_table_id (CQ_1m)
            CqGranularity::Cq1m,
            vec![1],
            vec![AggFunction::Avg],
        );
        def.set_lower_target_table_id(10);
        def.source_types = vec![DataType::F64];

        // 下层 CQ_10s 表两个窗口的 _sum/_count/_min/_max：
        //   窗口1: _sum=100, _count=10, _min=1, _max=50 → AVG=10
        //   窗口2: _sum=40,  _count=1,  _min=40, _max=40 → AVG=40
        // 正确加权：AVG = (100+40)/(10+1) = 140/11 ≈ 12.7273
        let mock = mock_ts(
            vec![
                vec![100.0, 40.0], // _sum 列
                vec![10.0, 1.0],   // _count 列
                vec![1.0, 40.0],   // _min 列
                vec![50.0, 40.0],  // _max 列
            ],
            vec![1_000, 2_000],
        );

        let result = executor.execute(&def, 0, 60_000_000_000, &mock).unwrap();

        assert_eq!(result.target_table_id, 101);
        assert_eq!(result.row_count, 2);

        let expected = 140.0 / 11.0;
        match &result.columns[0] {
            TypedValue::F64(v) => assert!(
                (v - expected).abs() < 1e-9,
                "加权 AVG 应为 {expected}，实际 {v}"
            ),
            other => panic!("AVG 应为 F64，实际 {other:?}"),
        }
        // 上层 _sum/_count 应继续累加传递到更上层。
        assert_eq!(result._sum[0], TypedValue::F64(140.0));
        assert_eq!(result._count[0], 11);
    }

    /// 层级加权 AVG 用 U64 大值不丢精度：Σ_sum 在 u64 域精确累加（f64 会折叠），
    /// 仅最后除法转 f64（AVG 结果恒 F64 的固有边界，见设计 §3.3/P6）。
    #[test]
    fn test_hierarchical_weighted_avg_u64_large_no_precision_loss() {
        let executor = CqAggregationExecutor::new();

        let mut def = CqDefinition::new(
            "cq_1m_u64".into(),
            1,
            101,
            CqGranularity::Cq1m,
            vec![1],
            vec![AggFunction::Avg],
        );
        def.set_lower_target_table_id(10);
        def.source_types = vec![DataType::U64];

        let big = 9_007_199_254_740_992u64; // 2^53
                                            // 两个下层窗口：_sum = 2^53+1 与 _sum = 2
                                            // Σ_sum = 2^53+3（奇数，f64 无法精确表示，u64 域则精确）
        let mock = mock_typed(
            vec![
                vec![Some(TypedValue::U64(big + 1)), Some(TypedValue::U64(2))], // _sum
                vec![Some(TypedValue::U64(1)), Some(TypedValue::U64(1))],       // _count
                vec![Some(TypedValue::U64(big + 1)), Some(TypedValue::U64(2))], // _min
                vec![Some(TypedValue::U64(big + 1)), Some(TypedValue::U64(2))], // _max
            ],
            vec![1_000, 2_000],
        );

        let result = executor.execute(&def, 0, 60_000_000_000, &mock).unwrap();

        // Σ_sum = 2^53 + 3，U64 精确（判别器：f64 折叠会丢失奇数尾数）。
        assert_eq!(result._sum[0], TypedValue::U64(big + 3));
        assert_eq!(result._count[0], 2);
        // AVG = (2^53+3)/2 = 2^52 + 1.5，f64 可精确表示。
        match &result.columns[0] {
            TypedValue::F64(v) => assert_eq!(*v, 4_503_599_627_370_497.5),
            other => panic!("AVG 应为 F64，实际 {other:?}"),
        }
        // MIN/MAX 按源类型（u64）精确比较。
        assert_eq!(result._min[0], TypedValue::U64(2));
        assert_eq!(result._max[0], TypedValue::U64(big + 1));
    }

    #[test]
    fn test_hierarchical_min_max() {
        let executor = CqAggregationExecutor::new();

        let mut def = CqDefinition::new(
            "cq_1m_minmax".into(),
            1,
            101,
            CqGranularity::Cq1m,
            vec![1, 1],
            vec![AggFunction::Min, AggFunction::Max],
        );
        def.set_lower_target_table_id(10);
        def.source_types = vec![DataType::I64, DataType::I64];

        // 两个源列 × 4 辅助列（_sum/_count/_min/_max），各 2 个下层窗口：
        //   源列 0: _min=[3, 5] → MIN=3
        //   源列 1: _max=[9, 20] → MAX=20
        let mock = mock_typed(
            vec![
                vec![Some(TypedValue::I64(1)), Some(TypedValue::I64(1))], // 源0 _sum（占位）
                vec![Some(TypedValue::U64(2)), Some(TypedValue::U64(2))], // 源0 _count（占位，需 > 0）
                vec![Some(TypedValue::I64(3)), Some(TypedValue::I64(5))], // 源0 _min
                vec![Some(TypedValue::I64(9)), Some(TypedValue::I64(20))], // 源0 _max
                vec![Some(TypedValue::I64(1)), Some(TypedValue::I64(1))], // 源1 _sum（占位）
                vec![Some(TypedValue::U64(2)), Some(TypedValue::U64(2))], // 源1 _count（占位，需 > 0）
                vec![Some(TypedValue::I64(1)), Some(TypedValue::I64(2))], // 源1 _min
                vec![Some(TypedValue::I64(9)), Some(TypedValue::I64(20))], // 源1 _max
            ],
            vec![1_000, 2_000],
        );

        let result = executor.execute(&def, 0, 60_000_000_000, &mock).unwrap();

        assert_eq!(result.columns[0], TypedValue::I64(3)); // MIN of [3,5]
        assert_eq!(result.columns[1], TypedValue::I64(20)); // MAX of [9,20]
        assert_eq!(result._min[0], TypedValue::I64(3));
        assert_eq!(result._max[1], TypedValue::I64(20));
    }

    #[test]
    fn test_hierarchical_empty() {
        let executor = CqAggregationExecutor::new();

        let mut def = CqDefinition::new(
            "cq_1m_empty".into(),
            1,
            101,
            CqGranularity::Cq1m,
            vec![1],
            vec![AggFunction::Avg],
        );
        def.set_lower_target_table_id(10);
        def.source_types = vec![DataType::F64];

        let mock = mock_ts(vec![vec![], vec![], vec![], vec![]], vec![]);

        let result = executor.execute(&def, 0, 60_000_000_000, &mock).unwrap();

        assert_eq!(result.row_count, 0);
        assert_eq!(result.columns[0], TypedValue::F64(0.0));
        assert_eq!(result._sum[0], TypedValue::F64(0.0));
        assert_eq!(result._count[0], 0);
    }

    #[test]
    fn test_hierarchical_unsupported_agg_returns_error() {
        let executor = CqAggregationExecutor::new();

        // CQ_1m 定义：1 个源列，First 聚合，下层指向 CQ_10s 表（table_id=10）。
        let mut def = CqDefinition::new(
            "cq_1m_first".into(),
            1,
            101,
            CqGranularity::Cq1m,
            vec![1],
            vec![AggFunction::First],
        );
        def.set_lower_target_table_id(10);
        def.source_types = vec![DataType::F64];

        let mock = mock_ts(
            vec![
                vec![100.0, 40.0],
                vec![10.0, 1.0],
                vec![1.0, 40.0],
                vec![50.0, 40.0],
            ],
            vec![1_000, 2_000],
        );

        let result = executor.execute(&def, 0, 60_000_000_000, &mock);

        assert!(result.is_err(), "层级 First 应显式报错，而非静默返回错值");
        match result {
            Err(SenseArcError::UnsupportedAggregation(msg)) => {
                assert!(
                    msg.contains("First"),
                    "错误信息应包含聚合函数名，实际: {msg}"
                );
            }
            other => panic!("应返回 UnsupportedAggregation，实际: {other:?}"),
        }
    }

    #[test]
    fn test_min_max_aggregation() {
        let executor = CqAggregationExecutor::new();

        let mut def = CqDefinition::new(
            "cq_10s_2".into(),
            2,
            102,
            CqGranularity::Cq10s,
            vec![1, 1],
            vec![AggFunction::Min, AggFunction::Max],
        );
        def.source_types = vec![DataType::F64, DataType::F64];

        let mock = mock_ts(
            vec![vec![5.0, 1.0, 10.0, 3.0], vec![5.0, 1.0, 10.0, 3.0]],
            vec![100, 200, 300, 400],
        );

        let result = executor.execute(&def, 0, 10_000_000_000, &mock).unwrap();

        assert_eq!(result.columns[0], TypedValue::F64(1.0)); // MIN
        assert_eq!(result.columns[1], TypedValue::F64(10.0)); // MAX
    }

    /// P2 边界修复：直接聚合某列全 None 时，`_min`/`_max` 必须写哨兵而非零值。
    #[test]
    fn test_direct_all_none_column_writes_min_max_sentinel() {
        let executor = CqAggregationExecutor::new();

        let mut def = CqDefinition::new(
            "cq_10s_sentinel".into(),
            1,
            101,
            CqGranularity::Cq10s,
            vec![1, 1],
            vec![AggFunction::Min, AggFunction::Max],
        );
        def.source_types = vec![DataType::I64, DataType::I64];

        // 源列 0 全空（全 None）；源列 1 有负值/正值。
        let mock = mock_typed(
            vec![
                vec![None, None],                                           // 源列 0：两行全 None
                vec![Some(TypedValue::I64(-5)), Some(TypedValue::I64(10))], // 源列 1
            ],
            vec![1_000, 2_000],
        );

        let result = executor.execute(&def, 0, 10_000_000_000, &mock).unwrap();

        // 全空列的 _min/_max 写哨兵（i64::MAX / i64::MIN），而非 0。
        assert_eq!(result._min[0], TypedValue::I64(i64::MAX));
        assert_eq!(result._max[0], TypedValue::I64(i64::MIN));
        // 非空列正常传递。
        assert_eq!(result._min[1], TypedValue::I64(-5));
        assert_eq!(result._max[1], TypedValue::I64(10));
    }

    /// P2 边界修复：层级聚合必须忽略空窗口（_count==0）的 _min/_max 哨兵，
    /// 使 MIN/MAX 不被 0（或哨兵）污染。
    #[test]
    fn test_hierarchical_min_max_ignores_empty_window_sentinel() {
        let executor = CqAggregationExecutor::new();

        // CQ_1m 定义：2 源列，Min 与 Max，下层指向 CQ_10s（table_id=10）。
        let mut def = CqDefinition::new(
            "cq_1m_sentinel".into(),
            1,
            101,
            CqGranularity::Cq1m,
            vec![1, 1],
            vec![AggFunction::Min, AggFunction::Max],
        );
        def.set_lower_target_table_id(10);
        def.source_types = vec![DataType::I64, DataType::I64];

        // 两个下层窗口：
        //   窗口 1（非空）：源 0 _min=5（正）、源 1 _max=-3（负），_count=1
        //   窗口 2（全空）：_count=0，_min/_max 为哨兵（i64::MAX / i64::MIN）
        let mock = mock_typed(
            vec![
                vec![Some(TypedValue::I64(5)), Some(TypedValue::I64(0))], // 源0 _sum（占位）
                vec![Some(TypedValue::U64(1)), Some(TypedValue::U64(0))], // 源0 _count
                vec![Some(TypedValue::I64(5)), Some(TypedValue::I64(i64::MAX))], // 源0 _min
                vec![Some(TypedValue::I64(5)), Some(TypedValue::I64(i64::MIN))], // 源0 _max
                vec![Some(TypedValue::I64(1)), Some(TypedValue::I64(0))], // 源1 _sum（占位）
                vec![Some(TypedValue::U64(1)), Some(TypedValue::U64(0))], // 源1 _count
                vec![Some(TypedValue::I64(-3)), Some(TypedValue::I64(i64::MAX))], // 源1 _min
                vec![Some(TypedValue::I64(-3)), Some(TypedValue::I64(i64::MIN))], // 源1 _max
            ],
            vec![1_000, 2_000],
        );

        let result = executor.execute(&def, 0, 60_000_000_000, &mock).unwrap();

        // 空窗口哨兵被忽略：MIN=5（正，不被 0 污染）、MAX=-3（负，不被 0 污染）。
        assert_eq!(result.columns[0], TypedValue::I64(5));
        assert_eq!(result.columns[1], TypedValue::I64(-3));
        assert_eq!(result._min[0], TypedValue::I64(5));
        assert_eq!(result._max[1], TypedValue::I64(-3));
    }
}
