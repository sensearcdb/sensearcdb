//! CQ 目标表辅助列固定布局。
//!
//! ## 宽表语义（S3 计算层列级，确认结论）
//!
//! CQ 目标表 = **源宽表的聚合投影表（多列）**：每个源列在 CQ 目标表内
//! 对应一组物理列，`value` 列的 `column_id` 与源列 `column_id` 相同，
//! `_sum/_count/_min/_max/_first_ts/_last_ts` 等辅助列即该宽表里的列。
//! 本 crate 已按列级实现（`column_resolver.rs` 源列引用为 `(table_id, column_id)`），
//! 无需改动。
//!
//! 每源列在 CQ 物理表中固定占用 7 个列位：
//!
//! ```text
//! [ value, _sum, _count, _min, _max, _first_ts, _last_ts ]
//! ```
//!
//! - `value` 为聚合结果列本身，`column_id` 与源列 `column_id` 相同
//!   （保证 AutoRouter / 查询路径按源列 ID 透明读取 CQ 结果）。
//! - 其余 6 个为辅助列，`column_id` 按 `base = max(source_columns) + 1`
//!   顺序分配，`base + source_index * 6 + kind`。
//!
//! 该布局同时覆盖 A4 遗留 `_min`/`_max` 与 P1-11 `_first_ts`/`_last_ts`，
//! 替代旧的 ad-hoc 高位 ID（`base+2i` / `base+2i+1`）方案。
//!
//! 注意：本模块仅依赖 `sensearcdb-common`，不依赖 `sensearcdb-schema`；
//! 物理 `ColumnDef` / `TableInfo` 的构建由顶层 `src/startup.rs` 的
//! `CqSchemaSynchronizer` 完成（该层同时依赖 cq 与 schema 两个 crate）。

/// 每源列的辅助列数量。
pub const AUX_COLUMNS_PER_SOURCE: usize = 6;

use sensearcdb_common::{AggFunction, DataType, TypedValue};

/// CQ 辅助列种类（固定顺序）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum CqAuxColumn {
    /// 求和 Σx（层级传递用）。
    Sum = 0,
    /// 计数 N（层级传递用）。
    Count = 1,
    /// 最小值（A4 遗留，真 MIN 层级传递）。
    Min = 2,
    /// 最大值（A4 遗留，真 MAX 层级传递）。
    Max = 3,
    /// FIRST 值对应时间戳（epoch 纳秒，i64 落库）。
    FirstTs = 4,
    /// LAST 值对应时间戳（epoch 纳秒，i64 落库）。
    LastTs = 5,
}

impl CqAuxColumn {
    /// 所有辅助列种类（按固定顺序）。
    pub const ALL: [CqAuxColumn; AUX_COLUMNS_PER_SOURCE] = [
        CqAuxColumn::Sum,
        CqAuxColumn::Count,
        CqAuxColumn::Min,
        CqAuxColumn::Max,
        CqAuxColumn::FirstTs,
        CqAuxColumn::LastTs,
    ];

    /// 辅助列名称后缀（不含源列前缀）。
    #[inline]
    pub fn name(self) -> &'static str {
        match self {
            CqAuxColumn::Sum => "_sum",
            CqAuxColumn::Count => "_count",
            CqAuxColumn::Min => "_min",
            CqAuxColumn::Max => "_max",
            CqAuxColumn::FirstTs => "_first_ts",
            CqAuxColumn::LastTs => "_last_ts",
        }
    }

    /// 该辅助列是否为 i64 物理类型（时间戳列）。
    #[inline]
    pub fn is_i64(self) -> bool {
        matches!(self, CqAuxColumn::FirstTs | CqAuxColumn::LastTs)
    }
}

/// 计算辅助列的物理数据类型。
///
/// 规则（Phase7-B5 统一类型化收敛 §3.2）：
/// - `_sum` / `_min` / `_max` = 源列类型（累加/比较域与源一致）。
/// - `_count` = `U64`（计数恒 u64）。
/// - `_first_ts` / `_last_ts` = `I64`（纳秒时间戳）。
#[inline]
pub fn aux_column_type(kind: CqAuxColumn, source_type: DataType) -> DataType {
    match kind {
        CqAuxColumn::Sum | CqAuxColumn::Min | CqAuxColumn::Max => source_type,
        CqAuxColumn::Count => DataType::U64,
        CqAuxColumn::FirstTs | CqAuxColumn::LastTs => DataType::I64,
    }
}

/// 计算 `value` 聚合结果列的物理数据类型。
///
/// 与 Stream 共用同一张映射表（`agg_result_type`）：`COUNT`→U64、
/// `AVG`/`STDDEV`→F64、其余→源数值类型。
#[inline]
pub fn value_column_type(function: AggFunction, source_type: DataType) -> DataType {
    sensearcdb_common::agg_result_type(function, source_type)
}

/// 源数值类型的零值 `TypedValue`（空窗口/缺省占位）。
#[inline]
pub fn typed_zero(data_type: DataType) -> TypedValue {
    match data_type {
        DataType::F64 => TypedValue::F64(0.0),
        DataType::F32 => TypedValue::F32(0.0),
        DataType::I64 => TypedValue::I64(0),
        DataType::U64 => TypedValue::U64(0),
        DataType::I32 => TypedValue::I32(0),
        _ => TypedValue::F64(0.0),
    }
}

/// 源类型的 `_min` 空哨兵（取该类型最大值，使真实值恒更小）。
#[inline]
pub fn typed_min_sentinel(data_type: DataType) -> TypedValue {
    match data_type {
        DataType::F64 => TypedValue::F64(f64::MAX),
        DataType::F32 => TypedValue::F32(f32::MAX),
        DataType::I64 => TypedValue::I64(i64::MAX),
        DataType::U64 => TypedValue::U64(u64::MAX),
        DataType::I32 => TypedValue::I32(i32::MAX),
        _ => TypedValue::F64(f64::MAX),
    }
}

/// 源类型的 `_max` 空哨兵（取该类型最小值，使真实值恒更大）。
#[inline]
pub fn typed_max_sentinel(data_type: DataType) -> TypedValue {
    match data_type {
        DataType::F64 => TypedValue::F64(f64::MIN),
        DataType::F32 => TypedValue::F32(f32::MIN),
        DataType::I64 => TypedValue::I64(i64::MIN),
        DataType::U64 => TypedValue::U64(u64::MIN),
        DataType::I32 => TypedValue::I32(i32::MIN),
        _ => TypedValue::F64(f64::MIN),
    }
}

/// 计算源列 `source_index` 的辅助列 `kind` 在 CQ 目标表中的 `column_id`。
///
/// 布局稳定：`base = max(source_columns) + 1`，同一源表的所有 CQ 粒度
/// （10s/1m/1h/1d）共享相同的 `source_columns`，因此层级间辅助列 ID 完全对齐，
/// 上层可直接按相同 ID 从下层 CQ 表读取 `_sum/_count/_min/_max`。
///
/// # Arguments
/// * `source_columns` - 源 raw 表的数值列 ID 列表。
/// * `source_index` - 源列在 `source_columns` 中的下标。
/// * `kind` - 辅助列种类。
#[inline]
pub fn aux_column_id(source_columns: &[u16], source_index: usize, kind: CqAuxColumn) -> u16 {
    let base = source_columns.iter().copied().max().unwrap_or(0) + 1;
    base + (source_index as u16) * (AUX_COLUMNS_PER_SOURCE as u16) + (kind as u16)
}

/// 计算聚合结果值列（`value`）在 CQ 目标表中的 `column_id`。
///
/// 与源列 ID 相同（见模块注释）。
#[inline]
pub fn value_column_id(source_columns: &[u16], source_index: usize) -> u16 {
    source_columns
        .get(source_index)
        .copied()
        .unwrap_or(source_index as u16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aux_column_id_layout() {
        let cols = [0u16, 1u16];
        // base = 1 + 1 = 2
        // source 0: 2..8, source 1: 8..14
        assert_eq!(aux_column_id(&cols, 0, CqAuxColumn::Sum), 2);
        assert_eq!(aux_column_id(&cols, 0, CqAuxColumn::Count), 3);
        assert_eq!(aux_column_id(&cols, 0, CqAuxColumn::Min), 4);
        assert_eq!(aux_column_id(&cols, 0, CqAuxColumn::Max), 5);
        assert_eq!(aux_column_id(&cols, 0, CqAuxColumn::FirstTs), 6);
        assert_eq!(aux_column_id(&cols, 0, CqAuxColumn::LastTs), 7);

        assert_eq!(aux_column_id(&cols, 1, CqAuxColumn::Sum), 8);
        assert_eq!(aux_column_id(&cols, 1, CqAuxColumn::LastTs), 13);
    }

    #[test]
    fn test_aux_column_id_sparse() {
        let cols = [5u16, 10u16];
        // base = 10 + 1 = 11
        assert_eq!(aux_column_id(&cols, 0, CqAuxColumn::Sum), 11);
        assert_eq!(aux_column_id(&cols, 1, CqAuxColumn::Sum), 17);
    }

    #[test]
    fn test_aux_column_no_collision_with_value() {
        let cols = [0u16, 1u16, 2u16];
        let mut all_ids = Vec::new();
        for i in 0..cols.len() {
            all_ids.push(value_column_id(&cols, i));
            for kind in CqAuxColumn::ALL {
                all_ids.push(aux_column_id(&cols, i, kind));
            }
        }
        // 去重后数量应等于总数（无碰撞）
        let unique: std::collections::HashSet<u16> = all_ids.iter().copied().collect();
        assert_eq!(unique.len(), all_ids.len());
    }

    #[test]
    fn test_aux_column_kind_metadata() {
        assert_eq!(CqAuxColumn::FirstTs.name(), "_first_ts");
        assert_eq!(CqAuxColumn::LastTs.name(), "_last_ts");
        assert!(CqAuxColumn::FirstTs.is_i64());
        assert!(CqAuxColumn::LastTs.is_i64());
        assert!(!CqAuxColumn::Sum.is_i64());
        assert!(!CqAuxColumn::Min.is_i64());
    }

    #[test]
    fn test_aux_column_type_rules() {
        // _sum/_min/_max = 源类型
        assert_eq!(
            aux_column_type(CqAuxColumn::Sum, DataType::U64),
            DataType::U64
        );
        assert_eq!(
            aux_column_type(CqAuxColumn::Min, DataType::I32),
            DataType::I32
        );
        assert_eq!(
            aux_column_type(CqAuxColumn::Max, DataType::F32),
            DataType::F32
        );
        // _count = U64
        assert_eq!(
            aux_column_type(CqAuxColumn::Count, DataType::F64),
            DataType::U64
        );
        // 时间戳 = I64
        assert_eq!(
            aux_column_type(CqAuxColumn::FirstTs, DataType::U64),
            DataType::I64
        );
        assert_eq!(
            aux_column_type(CqAuxColumn::LastTs, DataType::F64),
            DataType::I64
        );
    }

    #[test]
    fn test_value_column_type_rules() {
        assert_eq!(
            value_column_type(AggFunction::Count, DataType::F64),
            DataType::U64
        );
        assert_eq!(
            value_column_type(AggFunction::Avg, DataType::U64),
            DataType::F64
        );
        assert_eq!(
            value_column_type(AggFunction::Sum, DataType::U64),
            DataType::U64
        );
        assert_eq!(
            value_column_type(AggFunction::Min, DataType::I32),
            DataType::I32
        );
        assert_eq!(
            value_column_type(AggFunction::Max, DataType::F32),
            DataType::F32
        );
    }

    #[test]
    fn test_typed_sentinels() {
        assert_eq!(typed_zero(DataType::U64), TypedValue::U64(0));
        assert_eq!(typed_zero(DataType::I64), TypedValue::I64(0));
        assert_eq!(typed_zero(DataType::F64), TypedValue::F64(0.0));
        assert_eq!(typed_min_sentinel(DataType::U64), TypedValue::U64(u64::MAX));
        assert_eq!(typed_max_sentinel(DataType::U64), TypedValue::U64(u64::MIN));
        assert_eq!(typed_min_sentinel(DataType::I32), TypedValue::I32(i32::MAX));
        assert_eq!(typed_max_sentinel(DataType::I32), TypedValue::I32(i32::MIN));
    }
}
