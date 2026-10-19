//! ColumnarShard — 列式快照分片（S1 列式骨架）。
//!
//! 把原 `SnapshotBucket { entries: HashMap<u32, SnapshotEntry> }` 换成 21 列
//! arrow `*Builder` + `table_id → (start,end)` 行区间索引。逻辑行 = 一个测点列
//! `(tenant_id, table_id, column_id)`，同 `table_id` 的多列行连续相邻存放、
//! 按 `column_id` 升序。
//!
//! ## 值承载
//!
//! 8 个值列按 `dtype`（`DataType` discriminant）路由：每行只有一个值列非 null，
//! 其余 7 列 null。读投影靠 `dtype` 还原 `TypedValue`。
//!
//! ## 覆盖语义（S1 实现，与 spec §1.6/§1.7 对齐）
//!
//! - 非空 primitive 列：`values_slice_mut()[row] = v` 原地覆盖（O(1)）。
//! - 可空 primitive 列：同 dtype、null 状态不变的覆盖走 `values_slice_mut`
//!   （O(1)，validity 不动）；跨 dtype / null 状态变化走「整列重建」（O(N)，罕见）。
//! - `Boolean` 列（`value_bool`/`presence`/`is_derived`/`ttl_expired`）与 `String`
//!   列：arrow-rs 51 builder 无原地写值 API，值变化时走「整列重建」（O(N)，罕见）。
//! - 稀疏新增列 / 跨 dtype：`rebuild_sorted`（整 shard drain → 合并 → 排序重放，O(N)）。
//!
//! 注意：arrow-rs 51 的实际 API 是 `values_slice_mut()`（spec §1.6 写作
//! `values_mut_slice`），且 `NullBuffer` 无 `set_valid`/`set_null` 原地方法——
//! 见报告「与 spec 偏差」。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, ArrowPrimitiveType, BooleanArray, BooleanBuilder, Float32Array,
    Float32Builder, Float64Array, Float64Builder, Int32Array, Int32Builder, Int64Array,
    Int64Builder, PrimitiveBuilder, StringArray, StringBuilder, UInt16Array, UInt16Builder,
    UInt32Array, UInt32Builder, UInt64Array, UInt64Builder, UInt8Array, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fxhash::FxBuildHasher;

use crate::entry::SnapshotEntry;
use crate::store::TypedSnapshotValue;
use sensearcdb_common::{TenantId, TypedValue};

/// 待写入的一个测点（写路径批内元素）。
///
/// 与设计 §2.3 相比，S1 补上 `is_derived` / `writer_session_id` 两个字段
/// （`update_typed` shim 需透传入列式行，投影 `SnapshotEntry` 时需要）。
pub struct PendingPoint<'a> {
    pub tenant_id: TenantId,
    pub table_id: u32,
    pub values: &'a [(u16, TypedValue)],
    pub qualities: &'a [(u16, u8)],
    pub ts: i64,
    pub server_ts: i64,
    pub write_seq: Option<u64>,
    pub is_derived: bool,
    pub writer_session_id: Option<u64>,
}

/// 一个候选列值（批内 LWW 决胜后的最终候选，逐列独立决胜三元组）。
#[derive(Clone)]
pub(crate) struct ColumnValue {
    pub value: TypedValue,
    pub quality: u8,
    pub source_ts: i64,
    pub server_ts: i64,
    pub write_seq: Option<u64>,
}

/// 表级元数据（同表所有行共享）。
#[derive(Clone)]
pub(crate) struct TableMeta {
    pub tenant_id: TenantId,
    pub is_derived: bool,
    pub writer_session_id: Option<u64>,
}

/// 一次 upsert 产生的变化（供 store 层 version bump + notify）。
pub(crate) struct RowChange {
    pub entry: SnapshotEntry,
}

/// 一行 = 一个测点列（21 列摊平成行内标量）。
#[derive(Clone)]
struct Row {
    tenant_id: u32,
    table_id: u32,
    column_id: u16,
    dtype: u8,
    value: TypedValue,
    quality: u8,
    source_ts: i64,
    server_ts: i64,
    write_seq: Option<u64>,
    presence: bool,
    version: u64,
    is_derived: bool,
    ttl_expired: bool,
    writer_session_id: Option<u64>,
}

impl Row {
    /// 从候选列值 + 表级元数据构造一行（version 暂 0，由 store 层 set_row_version 回填）。
    fn from_candidate(table_id: u32, meta: &TableMeta, col: u16, cv: &ColumnValue) -> Self {
        Row {
            tenant_id: meta.tenant_id.as_u32(),
            table_id,
            column_id: col,
            dtype: cv.value.data_type() as u8,
            value: cv.value.clone(),
            quality: cv.quality,
            source_ts: cv.source_ts,
            server_ts: cv.server_ts,
            write_seq: cv.write_seq,
            presence: true,
            version: 0,
            is_derived: meta.is_derived,
            ttl_expired: false,
            writer_session_id: meta.writer_session_id,
        }
    }
}

/// 候选列值是否胜出（覆盖 old 候选 / old 存量行）。
///
/// 契约与 `merge.rs::new_wins_tie` 完全一致：`source_ts` 严格 `>` → 新；
/// `==` → `write_seq` 大者胜（Some > None）→ `server_ts` 大者胜 → 平局保留 old；
/// `<` → 拒绝（乱序回滚）。
#[inline]
fn lww_new_wins(new: &ColumnValue, old: &Row) -> bool {
    if new.source_ts > old.source_ts {
        return true;
    }
    if new.source_ts < old.source_ts {
        return false;
    }
    crate::merge::new_wins_tie(
        new.write_seq,
        old.write_seq,
        new.server_ts,
        old.server_ts,
    )
}

/// 批内候选 LWW 决胜（同构 `merge.rs::new_wins_tie`，两个候选都是 `ColumnValue`）。
#[inline]
fn lww_new_wins_cand(new: &ColumnValue, old: &ColumnValue) -> bool {
    if new.source_ts > old.source_ts {
        return true;
    }
    if new.source_ts < old.source_ts {
        return false;
    }
    crate::merge::new_wins_tie(
        new.write_seq,
        old.write_seq,
        new.server_ts,
        old.server_ts,
    )
}

/// 变化检测（行粒度，对齐 `change_detect.rs::typed_values_equal`）。
///
/// 比较 value（typed，f64/f32 走 epsilon）、quality、source_ts、server_ts。
/// 不比较 write_seq（对齐现状「write_seq 不进 detect」）。
#[inline]
fn row_equal(new: &ColumnValue, old: &Row) -> bool {
    crate::change_detect::typed_values_equal(&new.value, &old.value)
        && new.quality == old.quality
        && new.source_ts == old.source_ts
        && new.server_ts == old.server_ts
}

/// 列式快照分片。
pub struct ColumnarShard {
    // ── 21 列 arrow builder（append-only）──
    tenant_id: UInt32Builder,
    table_id: UInt32Builder,
    column_id: UInt16Builder,
    dtype: UInt8Builder,
    value_f64: Float64Builder,
    value_f32: Float32Builder,
    value_i64: Int64Builder,
    value_u64: UInt64Builder,
    value_i32: Int32Builder,
    value_bool: BooleanBuilder,
    value_str: StringBuilder,
    value_enum16: UInt16Builder,
    quality: UInt8Builder,
    source_ts: Int64Builder,
    server_ts: Int64Builder,
    write_seq: UInt64Builder,
    presence: BooleanBuilder,
    version: UInt64Builder,
    is_derived: BooleanBuilder,
    ttl_expired: BooleanBuilder,
    writer_session_id: UInt64Builder,

    // ── table_id → RowRange 索引（半开区间 [start, end)）──
    index: HashMap<u32, (usize, usize), FxBuildHasher>,
    /// 本 shard 最后更新版本（序列化用，语义对齐旧 SnapshotBucket.shard_version）。
    pub shard_version: u64,
}

impl ColumnarShard {
    /// 创建空分片。
    pub fn new() -> Self {
        Self {
            tenant_id: UInt32Builder::new(),
            table_id: UInt32Builder::new(),
            column_id: UInt16Builder::new(),
            dtype: UInt8Builder::new(),
            value_f64: Float64Builder::new(),
            value_f32: Float32Builder::new(),
            value_i64: Int64Builder::new(),
            value_u64: UInt64Builder::new(),
            value_i32: Int32Builder::new(),
            value_bool: BooleanBuilder::new(),
            value_str: StringBuilder::new(),
            value_enum16: UInt16Builder::new(),
            quality: UInt8Builder::new(),
            source_ts: Int64Builder::new(),
            server_ts: Int64Builder::new(),
            write_seq: UInt64Builder::new(),
            presence: BooleanBuilder::new(),
            version: UInt64Builder::new(),
            is_derived: BooleanBuilder::new(),
            ttl_expired: BooleanBuilder::new(),
            writer_session_id: UInt64Builder::new(),
            index: HashMap::with_hasher(FxBuildHasher::default()),
            shard_version: 0,
        }
    }

    /// 分片是否为空。
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// 行数（= tenant_id.len()）。
    #[inline]
    pub fn row_count(&self) -> usize {
        self.tenant_id.len()
    }

    /// 条目数（= table 数）。
    #[inline]
    pub fn entry_count(&self) -> usize {
        self.index.len()
    }

    /// 定位某测点列的行号。
    #[inline]
    pub fn get_row(&self, table_id: u32, column_id: u16) -> Option<usize> {
        let (s, e) = *self.index.get(&table_id)?;
        // 行块内 column_id 升序，线性扫描（宽表列数小；1表1tag 恒单行）。
        (s..e).find(|&r| self.column_id.values_slice()[r] == column_id)
    }

    /// 读某行的完整数据。
    fn row_at(&self, row: usize) -> Row {
        let dtype = self.dtype.values_slice()[row];
        let value = match dtype {
            0 => TypedValue::F64(self.value_f64.values_slice()[row]),
            1 => TypedValue::F32(self.value_f32.values_slice()[row]),
            2 => TypedValue::I64(self.value_i64.values_slice()[row]),
            3 => TypedValue::U64(self.value_u64.values_slice()[row]),
            4 => TypedValue::I32(self.value_i32.values_slice()[row]),
            5 => TypedValue::Bool(self.value_bool.finish_cloned().value(row)),
            6 => TypedValue::String(self.value_str.finish_cloned().value(row).to_string()),
            7 => TypedValue::Enum16(self.value_enum16.values_slice()[row]),
            _ => TypedValue::F64(0.0),
        };
        let write_seq = {
            let arr = self.write_seq.finish_cloned();
            if arr.is_null(row) {
                None
            } else {
                Some(arr.value(row))
            }
        };
        let writer_session_id = {
            let arr = self.writer_session_id.finish_cloned();
            if arr.is_null(row) {
                None
            } else {
                Some(arr.value(row))
            }
        };
        Row {
            tenant_id: self.tenant_id.values_slice()[row],
            table_id: self.table_id.values_slice()[row],
            column_id: self.column_id.values_slice()[row],
            dtype,
            value,
            quality: self.quality.values_slice()[row],
            source_ts: self.source_ts.values_slice()[row],
            server_ts: self.server_ts.values_slice()[row],
            write_seq,
            presence: self.presence.finish_cloned().value(row),
            version: self.version.values_slice()[row],
            is_derived: self.is_derived.finish_cloned().value(row),
            ttl_expired: self.ttl_expired.finish_cloned().value(row),
            writer_session_id,
        }
    }

    /// 追加一行（builder append，含 8 值列的 dtype 路由 null 填充）。
    fn append_row(&mut self, r: &Row) {
        self.tenant_id.append_value(r.tenant_id);
        self.table_id.append_value(r.table_id);
        self.column_id.append_value(r.column_id);
        self.dtype.append_value(r.dtype);
        self.append_value_columns(&r.value);
        self.quality.append_value(r.quality);
        self.source_ts.append_value(r.source_ts);
        self.server_ts.append_value(r.server_ts);
        self.write_seq.append_option(r.write_seq);
        self.presence.append_value(r.presence);
        self.version.append_value(r.version);
        self.is_derived.append_value(r.is_derived);
        self.ttl_expired.append_value(r.ttl_expired);
        self.writer_session_id.append_option(r.writer_session_id);
    }

    /// 8 值列按 dtype 路由 append：一列 value，七列 null。
    fn append_value_columns(&mut self, v: &TypedValue) {
        match v {
            TypedValue::F64(x) => {
                self.value_f64.append_value(*x);
                self.value_f32.append_null();
                self.value_i64.append_null();
                self.value_u64.append_null();
                self.value_i32.append_null();
                self.value_bool.append_null();
                self.value_str.append_null();
                self.value_enum16.append_null();
            }
            TypedValue::F32(x) => {
                self.value_f64.append_null();
                self.value_f32.append_value(*x);
                self.value_i64.append_null();
                self.value_u64.append_null();
                self.value_i32.append_null();
                self.value_bool.append_null();
                self.value_str.append_null();
                self.value_enum16.append_null();
            }
            TypedValue::I64(x) => {
                self.value_f64.append_null();
                self.value_f32.append_null();
                self.value_i64.append_value(*x);
                self.value_u64.append_null();
                self.value_i32.append_null();
                self.value_bool.append_null();
                self.value_str.append_null();
                self.value_enum16.append_null();
            }
            TypedValue::U64(x) => {
                self.value_f64.append_null();
                self.value_f32.append_null();
                self.value_i64.append_null();
                self.value_u64.append_value(*x);
                self.value_i32.append_null();
                self.value_bool.append_null();
                self.value_str.append_null();
                self.value_enum16.append_null();
            }
            TypedValue::I32(x) => {
                self.value_f64.append_null();
                self.value_f32.append_null();
                self.value_i64.append_null();
                self.value_u64.append_null();
                self.value_i32.append_value(*x);
                self.value_bool.append_null();
                self.value_str.append_null();
                self.value_enum16.append_null();
            }
            TypedValue::Bool(x) => {
                self.value_f64.append_null();
                self.value_f32.append_null();
                self.value_i64.append_null();
                self.value_u64.append_null();
                self.value_i32.append_null();
                self.value_bool.append_value(*x);
                self.value_str.append_null();
                self.value_enum16.append_null();
            }
            TypedValue::String(x) => {
                self.value_f64.append_null();
                self.value_f32.append_null();
                self.value_i64.append_null();
                self.value_u64.append_null();
                self.value_i32.append_null();
                self.value_bool.append_null();
                self.value_str.append_value(x);
                self.value_enum16.append_null();
            }
            TypedValue::Enum16(x) => {
                self.value_f64.append_null();
                self.value_f32.append_null();
                self.value_i64.append_null();
                self.value_u64.append_null();
                self.value_i32.append_null();
                self.value_bool.append_null();
                self.value_str.append_null();
                self.value_enum16.append_value(*x);
            }
        }
    }

    /// 原地覆盖非空 primitive 列 + 值列 + write_seq + writer_session_id + boolean 元数据列。
    ///
    /// `old` 为覆盖前的该行（用于判定跨 dtype / null 状态变化，决定 O(1) 原地 vs O(N) 重建）。
    fn overwrite_row(
        &mut self,
        row: usize,
        table_id: u32,
        col: u16,
        cand: &ColumnValue,
        meta: &TableMeta,
        old: &Row,
    ) {
        // 非空 primitive 列：原地覆盖（恒 valid）。
        self.tenant_id.values_slice_mut()[row] = meta.tenant_id.as_u32();
        self.table_id.values_slice_mut()[row] = table_id;
        self.column_id.values_slice_mut()[row] = col;
        self.dtype.values_slice_mut()[row] = cand.value.data_type() as u8;
        self.quality.values_slice_mut()[row] = cand.quality;
        self.source_ts.values_slice_mut()[row] = cand.source_ts;
        self.server_ts.values_slice_mut()[row] = cand.server_ts;
        // version 由 store 层 set_row_version 统一回填，这里不动。
        // presence 恒 true，覆盖不变，不动。

        // 值列。
        self.overwrite_value_column(row, old, cand);

        // write_seq：同 null 状态（Some→Some）原地覆盖；跨 null 状态重建。
        if old.write_seq != cand.write_seq {
            match (old.write_seq, cand.write_seq) {
                (Some(_), Some(b)) => self.write_seq.values_slice_mut()[row] = b,
                _ => rebuild_nullable(&mut self.write_seq, row, cand.write_seq),
            }
        }
        // writer_session_id：同上。
        if old.writer_session_id != meta.writer_session_id {
            match (old.writer_session_id, meta.writer_session_id) {
                (Some(_), Some(b)) => self.writer_session_id.values_slice_mut()[row] = b,
                _ => rebuild_nullable(&mut self.writer_session_id, row, meta.writer_session_id),
            }
        }
        // is_derived：值变则重建（罕见）。
        if old.is_derived != meta.is_derived {
            rebuild_bool(&mut self.is_derived, row, meta.is_derived);
        }
        // ttl_expired：覆盖时重置 false；仅当旧值为 true 才重建（罕见）。
        if old.ttl_expired {
            rebuild_bool(&mut self.ttl_expired, row, false);
        }
    }

    /// 值列覆盖：同 dtype 且 primitive → O(1)；Bool/String 或跨 dtype → 重建。
    fn overwrite_value_column(&mut self, row: usize, old: &Row, cand: &ColumnValue) {
        let new_dtype = cand.value.data_type() as u8;
        if old.dtype == new_dtype {
            match &cand.value {
                TypedValue::F64(v) => self.value_f64.values_slice_mut()[row] = *v,
                TypedValue::F32(v) => self.value_f32.values_slice_mut()[row] = *v,
                TypedValue::I64(v) => self.value_i64.values_slice_mut()[row] = *v,
                TypedValue::U64(v) => self.value_u64.values_slice_mut()[row] = *v,
                TypedValue::I32(v) => self.value_i32.values_slice_mut()[row] = *v,
                TypedValue::Bool(v) => rebuild_bool(&mut self.value_bool, row, *v),
                TypedValue::String(v) => rebuild_string(&mut self.value_str, row, v),
                TypedValue::Enum16(v) => self.value_enum16.values_slice_mut()[row] = *v,
            }
        } else {
            // 跨 dtype：旧值列该行置 null + 新值列该行写值（两个列各重建一次，罕见）。
            set_value_column_null(self, old.dtype, row);
            set_value_column_value(self, &cand.value, row);
        }
    }

    /// 批量 upsert 一个 table 的候选列（每 shard 写锁内逐表调用）。
    ///
    /// 返回该 table 本次是否有变化（**至多一个** `RowChange`，供 store 层 version bump + notify）。
    pub(crate) fn upsert_table(
        &mut self,
        table_id: u32,
        meta: &TableMeta,
        cols: &[(u16, ColumnValue)],
    ) -> Vec<RowChange> {
        let mut changed = false;
        let mut new_columns: Vec<(u16, ColumnValue)> = Vec::new();

        for (col, cand) in cols {
            if let Some(row) = self.get_row(table_id, *col) {
                let old = self.row_at(row);
                if lww_new_wins(cand, &old) {
                    if old.dtype != cand.value.data_type() as u8 {
                        // 跨 dtype：并入重排（rebuild），避免旧行残留。
                        new_columns.push((*col, cand.clone()));
                        changed = true;
                    } else if !row_equal(cand, &old) {
                        self.overwrite_row(row, table_id, *col, cand, meta, &old);
                        changed = true;
                    }
                }
            } else {
                new_columns.push((*col, cand.clone()));
            }
        }

        if !new_columns.is_empty() {
            if self.index.contains_key(&table_id) {
                // 稀疏新增列 / 跨 dtype：整 shard 重排合并。
                self.rebuild_sorted_with_new(table_id, meta, &new_columns);
            } else {
                // 新表：直接 append 连续块。
                self.append_table(table_id, meta, &new_columns);
            }
            changed = true;
        }

        if changed {
            let entry = self.project_entry(table_id).expect("just written table must project");
            vec![RowChange { entry }]
        } else {
            Vec::new()
        }
    }

    /// 新表 append：连续块，更新索引。
    fn append_table(&mut self, table_id: u32, meta: &TableMeta, cols: &[(u16, ColumnValue)]) {
        let start = self.row_count();
        for (col, cv) in cols {
            let r = Row::from_candidate(table_id, meta, *col, cv);
            self.append_row(&r);
        }
        let end = self.row_count();
        self.index.insert(table_id, (start, end));
    }

    /// 稀疏新增列 / 跨 dtype：整 shard drain → 合并 → 按 (table_id, column_id) 升序重放 → 重建索引。
    fn rebuild_sorted_with_new(
        &mut self,
        table_id: u32,
        meta: &TableMeta,
        new_columns: &[(u16, ColumnValue)],
    ) {
        let mut rows = self.drain_rows();
        let new_cols_set: HashSet<(u32, u16), FxBuildHasher> = new_columns
            .iter()
            .map(|(c, _)| (table_id, *c))
            .collect();
        rows.retain(|r| !new_cols_set.contains(&(r.table_id, r.column_id)));
        for (col, cv) in new_columns {
            rows.push(Row::from_candidate(table_id, meta, *col, cv));
        }
        rows.sort_by_key(|r| (r.table_id, r.column_id));
        self.reset();
        for r in &rows {
            self.append_row(r);
        }
        self.rebuild_index();
    }

    /// drain 全部行（读所有 builder，返回 `Vec<Row>`）。
    fn drain_rows(&self) -> Vec<Row> {
        (0..self.row_count()).map(|r| self.row_at(r)).collect()
    }

    /// 清空所有 builder + 索引（保留 shard_version）。
    fn reset(&mut self) {
        self.tenant_id = UInt32Builder::new();
        self.table_id = UInt32Builder::new();
        self.column_id = UInt16Builder::new();
        self.dtype = UInt8Builder::new();
        self.value_f64 = Float64Builder::new();
        self.value_f32 = Float32Builder::new();
        self.value_i64 = Int64Builder::new();
        self.value_u64 = UInt64Builder::new();
        self.value_i32 = Int32Builder::new();
        self.value_bool = BooleanBuilder::new();
        self.value_str = StringBuilder::new();
        self.value_enum16 = UInt16Builder::new();
        self.quality = UInt8Builder::new();
        self.source_ts = Int64Builder::new();
        self.server_ts = Int64Builder::new();
        self.write_seq = UInt64Builder::new();
        self.presence = BooleanBuilder::new();
        self.version = UInt64Builder::new();
        self.is_derived = BooleanBuilder::new();
        self.ttl_expired = BooleanBuilder::new();
        self.writer_session_id = UInt64Builder::new();
        self.index.clear();
    }

    /// 重建索引：按已排序的行扫描，同 table_id 连续块成 RowRange。
    fn rebuild_index(&mut self) {
        self.index.clear();
        let mut i = 0;
        let n = self.row_count();
        while i < n {
            let tid = self.table_id.values_slice()[i];
            let start = i;
            while i < n && self.table_id.values_slice()[i] == tid {
                i += 1;
            }
            self.index.insert(tid, (start, i));
        }
    }

    /// 投影 `SnapshotEntry`（供 get()/notify/serialize 用）。`prev_value`/`prev_ts` 恒 None。
    pub fn project_entry(&self, table_id: u32) -> Option<SnapshotEntry> {
        let (s, e) = *self.index.get(&table_id)?;
        let mut values = Vec::with_capacity(e - s);
        let mut qualities = Vec::with_capacity(e - s);
        let mut ts_values = Vec::with_capacity(e - s);
        let mut server_ts_values = Vec::with_capacity(e - s);
        let mut write_seqs = Vec::with_capacity(e - s);
        for row in s..e {
            let r = self.row_at(row);
            values.push((r.column_id, r.value.clone()));
            qualities.push((r.column_id, r.quality));
            ts_values.push((r.column_id, r.source_ts));
            server_ts_values.push((r.column_id, r.server_ts));
            write_seqs.push((r.column_id, r.write_seq));
        }
        let first = self.row_at(s);
        let ts = ts_values.iter().map(|(_, t)| *t).max().unwrap_or(0);
        let server_ts = server_ts_values.iter().map(|(_, t)| *t).max().unwrap_or(0);
        Some(SnapshotEntry {
            tenant_id: TenantId(first.tenant_id),
            values,
            qualities,
            ts,
            server_ts,
            version: first.version,
            prev_value: None,
            prev_ts: None,
            is_derived: first.is_derived,
            writer_session_id: first.writer_session_id,
            ttl_expired: first.ttl_expired,
            ts_values,
            server_ts_values,
            write_seqs,
        })
    }

    /// 便捷：单点投影 `SnapshotEntry`（serialize/qa 测试用）。
    #[inline]
    pub fn get(&self, table_id: u32) -> Option<SnapshotEntry> {
        self.project_entry(table_id)
    }

    /// 投影 `TypedSnapshotValue`（供 batch_get_typed 用）。
    pub fn project_typed_value(&self, table_id: u32) -> Option<TypedSnapshotValue> {
        let e = self.project_entry(table_id)?;
        Some(TypedSnapshotValue {
            values: e.values,
            qualities: e.qualities,
            ts: e.ts,
            server_ts: e.server_ts,
            version: e.version,
            is_derived: e.is_derived,
            ttl_expired: e.ttl_expired,
            ts_values: e.ts_values,
            server_ts_values: e.server_ts_values,
            tenant_id: e.tenant_id,
        })
    }

    /// 升序返回所有 table_id（确定性，供 serialize/list_all/iter_stream）。
    pub fn iter_table_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self.index.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// 无条件覆盖/插入一个 table（不 LWW），用于 serialize 反序列化 + recovery 恢复。
    ///
    /// version 列统一取 `entry.version`（该 tag 最后一次变化的全局版本号），
    /// 不再接受外部入参覆盖——避免反序列化/rehash 时被 shard_version / 0 冲刷丢版本。
    pub fn upsert_from_entry(&mut self, table_id: u32, entry: SnapshotEntry) {
        let mut values = entry.values.clone();
        values.sort_by_key(|(c, _)| *c);
        let q: HashMap<u16, u8> = entry.qualities.iter().copied().collect();
        let ts: HashMap<u16, i64> = entry.ts_values.iter().copied().collect();
        let sts: HashMap<u16, i64> = entry.server_ts_values.iter().copied().collect();
        let seq: HashMap<u16, Option<u64>> = entry.write_seqs.iter().copied().collect();

        let rows: Vec<Row> = values
            .into_iter()
            .map(|(col, val)| Row {
                tenant_id: entry.tenant_id.as_u32(),
                table_id,
                column_id: col,
                dtype: val.data_type() as u8,
                value: val,
                quality: q.get(&col).copied().unwrap_or(0),
                source_ts: ts.get(&col).copied().unwrap_or(0),
                server_ts: sts.get(&col).copied().unwrap_or(0),
                write_seq: seq.get(&col).copied().unwrap_or(None),
                presence: true,
                version: entry.version,
                is_derived: entry.is_derived,
                ttl_expired: entry.ttl_expired,
                writer_session_id: entry.writer_session_id,
            })
            .collect();

        if self.index.contains_key(&table_id) {
            let mut all = self.drain_rows();
            all.retain(|r| r.table_id != table_id);
            all.extend(rows);
            all.sort_by_key(|r| (r.table_id, r.column_id));
            self.reset();
            for r in &all {
                self.append_row(r);
            }
            self.rebuild_index();
        } else {
            let start = self.row_count();
            for r in &rows {
                self.append_row(r);
            }
            let end = self.row_count();
            self.index.insert(table_id, (start, end));
        }
    }

    /// 该 table 行区间所有行 version 列写 version。
    pub fn set_row_version(&mut self, table_id: u32, version: u64) {
        if let Some((s, e)) = self.index.get(&table_id).copied() {
            for row in s..e {
                self.version.values_slice_mut()[row] = version;
            }
        }
    }

    /// 删除一个 table（重建过滤，O(N)，purge 低频）。
    pub fn remove_table(&mut self, table_id: u32) -> bool {
        if !self.index.contains_key(&table_id) {
            return false;
        }
        let mut rows = self.drain_rows();
        rows.retain(|r| r.table_id != table_id);
        rows.sort_by_key(|r| (r.table_id, r.column_id));
        self.reset();
        for r in &rows {
            self.append_row(r);
        }
        self.rebuild_index();
        true
    }

    /// 该 table 行区间所有行 ttl_expired 列写 true。
    pub fn mark_ttl_expired(&mut self, table_id: u32) -> bool {
        let Some((s, e)) = self.index.get(&table_id).copied() else {
            return false;
        };
        for row in s..e {
            let cur = self.ttl_expired.finish_cloned().value(row);
            if !cur {
                rebuild_bool(&mut self.ttl_expired, row, true);
            }
        }
        true
    }
}

impl Default for ColumnarShard {
    fn default() -> Self {
        Self::new()
    }
}

/// S4：21 列统一 Schema（字段顺序 = 上方 builder 声明顺序）。
/// metadata key `"shard_version"` → u64 字符串。
pub fn columnar_schema(shard_version: u64) -> Schema {
    let fields = vec![
        Field::new("tenant_id", DataType::UInt32, false),
        Field::new("table_id", DataType::UInt32, false),
        Field::new("column_id", DataType::UInt16, false),
        Field::new("dtype", DataType::UInt8, false),
        Field::new("value_f64", DataType::Float64, true),
        Field::new("value_f32", DataType::Float32, true),
        Field::new("value_i64", DataType::Int64, true),
        Field::new("value_u64", DataType::UInt64, true),
        Field::new("value_i32", DataType::Int32, true),
        Field::new("value_bool", DataType::Boolean, true),
        Field::new("value_str", DataType::Utf8, true),
        Field::new("value_enum16", DataType::UInt16, true),
        Field::new("quality", DataType::UInt8, false),
        Field::new("source_ts", DataType::Int64, false),
        Field::new("server_ts", DataType::Int64, false),
        Field::new("write_seq", DataType::UInt64, true),
        Field::new("presence", DataType::Boolean, false),
        Field::new("version", DataType::UInt64, false),
        Field::new("is_derived", DataType::Boolean, false),
        Field::new("ttl_expired", DataType::Boolean, false),
        Field::new("writer_session_id", DataType::UInt64, true),
    ];
    let mut md = std::collections::HashMap::new();
    md.insert("shard_version".to_string(), shard_version.to_string());
    Schema::new_with_metadata(fields, md)
}

/// 从 RecordBatch 第 `row` 行读回一个 `Row`（复用 `row_at` 的「21 列 → Row」映射）。
///
/// 红线：`write_seq` / `writer_session_id` 是 nullable UInt64，必须用 `is_null` 区分
/// `None` 与 `Some(0)`，否则 IPC 反序列化会把 None 压成 Some(0) 导致 LWW 决胜键断裂。
fn read_row_from_arrays(batch: &RecordBatch, row: usize) -> Row {
    let cols = batch.columns();
    let tenant_id = cols[0].as_any().downcast_ref::<UInt32Array>().unwrap().value(row);
    let table_id = cols[1].as_any().downcast_ref::<UInt32Array>().unwrap().value(row);
    let column_id = cols[2].as_any().downcast_ref::<UInt16Array>().unwrap().value(row);
    let dtype = cols[3].as_any().downcast_ref::<UInt8Array>().unwrap().value(row);
    let value = match dtype {
        0 => TypedValue::F64(cols[4].as_any().downcast_ref::<Float64Array>().unwrap().value(row)),
        1 => TypedValue::F32(cols[5].as_any().downcast_ref::<Float32Array>().unwrap().value(row)),
        2 => TypedValue::I64(cols[6].as_any().downcast_ref::<Int64Array>().unwrap().value(row)),
        3 => TypedValue::U64(cols[7].as_any().downcast_ref::<UInt64Array>().unwrap().value(row)),
        4 => TypedValue::I32(cols[8].as_any().downcast_ref::<Int32Array>().unwrap().value(row)),
        5 => TypedValue::Bool(cols[9].as_any().downcast_ref::<BooleanArray>().unwrap().value(row)),
        6 => TypedValue::String(cols[10].as_any().downcast_ref::<StringArray>().unwrap().value(row).to_string()),
        7 => TypedValue::Enum16(cols[11].as_any().downcast_ref::<UInt16Array>().unwrap().value(row)),
        _ => TypedValue::F64(0.0),
    };
    let quality = cols[12].as_any().downcast_ref::<UInt8Array>().unwrap().value(row);
    let source_ts = cols[13].as_any().downcast_ref::<Int64Array>().unwrap().value(row);
    let server_ts = cols[14].as_any().downcast_ref::<Int64Array>().unwrap().value(row);
    let write_seq = {
        let arr = cols[15].as_any().downcast_ref::<UInt64Array>().unwrap();
        if arr.is_null(row) { None } else { Some(arr.value(row)) }
    };
    let presence = cols[16].as_any().downcast_ref::<BooleanArray>().unwrap().value(row);
    let version = cols[17].as_any().downcast_ref::<UInt64Array>().unwrap().value(row);
    let is_derived = cols[18].as_any().downcast_ref::<BooleanArray>().unwrap().value(row);
    let ttl_expired = cols[19].as_any().downcast_ref::<BooleanArray>().unwrap().value(row);
    let writer_session_id = {
        let arr = cols[20].as_any().downcast_ref::<UInt64Array>().unwrap();
        if arr.is_null(row) { None } else { Some(arr.value(row)) }
    };
    Row {
        tenant_id,
        table_id,
        column_id,
        dtype,
        value,
        quality,
        source_ts,
        server_ts,
        write_seq,
        presence,
        version,
        is_derived,
        ttl_expired,
        writer_session_id,
    }
}

impl ColumnarShard {
    /// S4：确定性导出 RecordBatch（**显式按 (table_id, column_id) 升序**）。
    ///
    /// 禁止直接 finish 21 个 builder——跨批 `append_table` 会破坏「builder 内 table_id
    /// 全局升序」不变量，导致字节不确定。这里 drain + sort + 重放。
    pub fn to_record_batch(&self) -> RecordBatch {
        let mut rows = self.drain_rows();
        rows.sort_by_key(|r| (r.table_id, r.column_id));
        let mut tmp = ColumnarShard::new();
        for r in &rows {
            tmp.append_row(r);
        }
        let arrays = tmp.finish_arrays();
        RecordBatch::try_new(Arc::new(columnar_schema(self.shard_version)), arrays)
            .expect("21 列 schema 与 builder finish 出的 arrays 恒匹配")
    }

    /// S4：从 RecordBatch 重建（含索引 rebuild + shard_version 回读）。
    pub fn from_record_batch(batch: &RecordBatch) -> Self {
        let mut shard = ColumnarShard::new();
        for row in 0..batch.num_rows() {
            shard.append_row(&read_row_from_arrays(batch, row));
        }
        shard.rebuild_index();
        shard.shard_version = batch
            .schema()
            .metadata()
            .get("shard_version")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        shard
    }

    /// 把 21 个 builder finish 成 ArrayRef（顺序 = 声明顺序，与 `columnar_schema` 严格一致）。
    fn finish_arrays(&mut self) -> Vec<ArrayRef> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(21);
        arrays.push(Arc::new(self.tenant_id.finish()));
        arrays.push(Arc::new(self.table_id.finish()));
        arrays.push(Arc::new(self.column_id.finish()));
        arrays.push(Arc::new(self.dtype.finish()));
        arrays.push(Arc::new(self.value_f64.finish()));
        arrays.push(Arc::new(self.value_f32.finish()));
        arrays.push(Arc::new(self.value_i64.finish()));
        arrays.push(Arc::new(self.value_u64.finish()));
        arrays.push(Arc::new(self.value_i32.finish()));
        arrays.push(Arc::new(self.value_bool.finish()));
        arrays.push(Arc::new(self.value_str.finish()));
        arrays.push(Arc::new(self.value_enum16.finish()));
        arrays.push(Arc::new(self.quality.finish()));
        arrays.push(Arc::new(self.source_ts.finish()));
        arrays.push(Arc::new(self.server_ts.finish()));
        arrays.push(Arc::new(self.write_seq.finish()));
        arrays.push(Arc::new(self.presence.finish()));
        arrays.push(Arc::new(self.version.finish()));
        arrays.push(Arc::new(self.is_derived.finish()));
        arrays.push(Arc::new(self.ttl_expired.finish()));
        arrays.push(Arc::new(self.writer_session_id.finish()));
        arrays
    }
}

// ─── 重建 helper（arrow-rs 51 无原地 set_valid/set_null，用「整列 drain 重放」兜底）───

/// 重建可空 primitive 列，把 `row` 位改为 `new`（Some=写值，None=置 null）。
fn rebuild_nullable<T: ArrowPrimitiveType>(
    b: &mut PrimitiveBuilder<T>,
    row: usize,
    new: Option<T::Native>,
) {
    let arr = b.finish_cloned();
    let mut nb = PrimitiveBuilder::<T>::new();
    for i in 0..arr.len() {
        if i == row {
            match new {
                Some(v) => nb.append_value(v),
                None => nb.append_null(),
            }
        } else if arr.is_null(i) {
            nb.append_null();
        } else {
            nb.append_value(arr.value(i));
        }
    }
    *b = nb;
}

/// 重建 boolean 列，把 `row` 位改为 `val`（非 null）。
fn rebuild_bool(b: &mut BooleanBuilder, row: usize, val: bool) {
    let arr = b.finish_cloned();
    let mut nb = BooleanBuilder::new();
    for i in 0..arr.len() {
        if i == row {
            nb.append_value(val);
        } else if arr.is_null(i) {
            nb.append_null();
        } else {
            nb.append_value(arr.value(i));
        }
    }
    *b = nb;
}

/// 重建 string 列，把 `row` 位改为 `val`（非 null）。
fn rebuild_string(b: &mut StringBuilder, row: usize, val: &str) {
    let arr = b.finish_cloned();
    let mut nb = StringBuilder::new();
    for i in 0..arr.len() {
        if i == row {
            nb.append_value(val);
        } else if arr.is_null(i) {
            nb.append_null();
        } else {
            nb.append_value(arr.value(i));
        }
    }
    *b = nb;
}

/// 把某 dtype 对应值列的 `row` 位置 null（跨 dtype 覆盖时用，重建该列）。
fn set_value_column_null(shard: &mut ColumnarShard, dtype: u8, row: usize) {
    match dtype {
        0 => rebuild_nullable(&mut shard.value_f64, row, None::<f64>),
        1 => rebuild_nullable(&mut shard.value_f32, row, None::<f32>),
        2 => rebuild_nullable(&mut shard.value_i64, row, None::<i64>),
        3 => rebuild_nullable(&mut shard.value_u64, row, None::<u64>),
        4 => rebuild_nullable(&mut shard.value_i32, row, None::<i32>),
        5 => {
            let arr = shard.value_bool.finish_cloned();
            let mut nb = BooleanBuilder::new();
            for i in 0..arr.len() {
                if i == row {
                    nb.append_null();
                } else if arr.is_null(i) {
                    nb.append_null();
                } else {
                    nb.append_value(arr.value(i));
                }
            }
            shard.value_bool = nb;
        }
        6 => {
            let arr = shard.value_str.finish_cloned();
            let mut nb = StringBuilder::new();
            for i in 0..arr.len() {
                if i == row {
                    nb.append_null();
                } else if arr.is_null(i) {
                    nb.append_null();
                } else {
                    nb.append_value(arr.value(i));
                }
            }
            shard.value_str = nb;
        }
        7 => rebuild_nullable(&mut shard.value_enum16, row, None::<u16>),
        _ => {}
    }
}

/// 把某值写到其 dtype 对应值列的 `row` 位（跨 dtype 覆盖时用，重建该列）。
fn set_value_column_value(shard: &mut ColumnarShard, value: &TypedValue, row: usize) {
    match value {
        TypedValue::F64(v) => rebuild_nullable(&mut shard.value_f64, row, Some(*v)),
        TypedValue::F32(v) => rebuild_nullable(&mut shard.value_f32, row, Some(*v)),
        TypedValue::I64(v) => rebuild_nullable(&mut shard.value_i64, row, Some(*v)),
        TypedValue::U64(v) => rebuild_nullable(&mut shard.value_u64, row, Some(*v)),
        TypedValue::I32(v) => rebuild_nullable(&mut shard.value_i32, row, Some(*v)),
        TypedValue::Bool(v) => rebuild_bool(&mut shard.value_bool, row, *v),
        TypedValue::String(v) => rebuild_string(&mut shard.value_str, row, v),
        TypedValue::Enum16(v) => rebuild_nullable(&mut shard.value_enum16, row, Some(*v)),
    }
}

/// 批内按 (table_id, column_id) 归组 + 批内 LWW 决胜。
///
/// 返回 `(cands, metas)`：`cands` 为每测点列的最终候选；`metas` 为 table_id → TableMeta
/// （后到者覆盖，取批内最后一点的表级字段）。
pub(crate) fn group_by_column<'a>(
    batch: &[PendingPoint<'a>],
) -> (
    HashMap<(u32, u16), ColumnValue, FxBuildHasher>,
    HashMap<u32, TableMeta, FxBuildHasher>,
) {
    let mut cands: HashMap<(u32, u16), ColumnValue, FxBuildHasher> = HashMap::default();
    let mut metas: HashMap<u32, TableMeta, FxBuildHasher> = HashMap::default();

    for p in batch {
        metas.insert(
            p.table_id,
            TableMeta {
                tenant_id: p.tenant_id,
                is_derived: p.is_derived,
                writer_session_id: p.writer_session_id,
            },
        );
        for (i, (col, val)) in p.values.iter().enumerate() {
            let q = p.qualities.get(i).map(|(_, q)| *q).unwrap_or(0);
            let cand = ColumnValue {
                value: val.clone(),
                quality: q,
                source_ts: p.ts,
                server_ts: p.server_ts,
                write_seq: p.write_seq,
            };
            match cands.entry((p.table_id, *col)) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(cand);
                }
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    if lww_new_wins_cand(&cand, e.get()) {
                        e.insert(cand);
                    }
                }
            }
        }
    }

    (cands, metas)
}
