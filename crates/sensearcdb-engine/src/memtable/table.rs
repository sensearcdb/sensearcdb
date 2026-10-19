//! ColumnarMemTable：按 table_id 组织的分片内存表。
//!
//! ## 分片锁（O2 修复）
//!
//! `chunks` 由原来的单把 `Mutex<HashMap<u32, ColumnarChunk>>`（所有 table 串行）
//! 改为 `Vec<Mutex<HashMap<u32, ColumnarChunk>>>`（`SHARD_COUNT` 个分片，按
//! `table_id % SHARD_COUNT` 路由）。不同 table 落入不同分片时可并行写入，
//! 消除跨 table 的全局串行瓶颈。
//!
//! 跨分片操作（`chunks_snapshot` / `table_count`）**逐分片加锁、加完即放**，
//! 任何时刻最多持有一把分片锁，无死锁风险（锁顺序恒为分片下标 0..N 升序）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::engine::DataPoint;
use crate::memtable::chunk::ColumnarChunk;
use crate::memtable::col_vec::TypedValue;
use sensearcdb_common::TenantId;

/// 分片数量（按 `table_id % SHARD_COUNT` 路由；固定值保证行为确定可复现）。
const SHARD_COUNT: usize = 16;

/// 计算 `table_id` 所在分片下标。
#[inline]
fn shard_of(table_id: u32) -> usize {
    (table_id as usize) % SHARD_COUNT
}

/// 列式内存表——管理所有 table 的活跃 Chunk。
///
/// 每个 `table_id` 对应一个 `ColumnarChunk`，按分片路由存储。
/// 当 `total_points` 超过 `freeze_threshold` 时触发冻结。
pub struct ColumnarMemTable {
    /// 分片锁：`shards[i]` 保护落在第 i 分片的 `table_id → ColumnarChunk` 映射。
    shards: Vec<Mutex<HashMap<u32, ColumnarChunk>>>,
    /// 总数据点数（所有 chunk 之和）。
    pub total_points: AtomicUsize,
    /// 估算内存占用（字节）。
    pub size_bytes: AtomicUsize,
    /// 冻结阈值（数据点点数）。从 AppConfig.engine.freeze_threshold 读取。
    pub freeze_threshold: usize,
}

impl ColumnarMemTable {
    /// 创建新的空 ColumnarMemTable。
    ///
    /// # 参数
    /// - `freeze_threshold`: 冻结阈值（数据点点数）。
    pub fn new(freeze_threshold: usize) -> Self {
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(Mutex::new(HashMap::new()));
        }
        Self {
            shards,
            total_points: AtomicUsize::new(0),
            size_bytes: AtomicUsize::new(0),
            freeze_threshold,
        }
    }

    /// 估算单点内存增量（与写入路径保持一致口径）。
    #[inline]
    fn point_size(values: &[(u16, TypedValue)], qualities: &[(u16, u8)]) -> usize {
        8 + values.len() * 10 + qualities.len() * 3
    }

    /// 单点插入到指定 table 的 Chunk（测试/低流量便捷路径，恒默认租户 0）。
    ///
    /// 生产批量热路径请用 [`insert_batch`](Self::insert_batch)（tenant_id 随 DataPoint 贯通）。
    ///
    /// # 参数
    /// - `table_id`: 目标表 ID。
    /// - `ts`: 时间戳。
    /// - `values`: 列值列表（column_id, `TypedValue`）。
    /// - `qualities`: 质量码列表。
    /// - `flags`: 点标记位。
    pub fn insert(
        &self,
        table_id: u32,
        ts: i64,
        values: &[(u16, TypedValue)],
        qualities: &[(u16, u8)],
        flags: u8,
    ) {
        let mut map = self.shards[shard_of(table_id)].lock();
        let chunk = map
            .entry(table_id)
            .or_insert_with(|| ColumnarChunk::new(TenantId::DEFAULT, table_id));
        chunk.append(ts, values, qualities, flags);
        self.total_points.fetch_add(1, Ordering::Relaxed);
        self.size_bytes
            .fetch_add(Self::point_size(values, qualities), Ordering::Relaxed);
    }

    /// 批量插入（O1 修复）：按分片分组，每个分片只加一次锁，锁获取 O(N)→O(1)。
    ///
    /// `seqs[i]` 为 `points[i]` 的 WAL 序列号（回填 Chunk 的 `max_wal_seq`，S7）；
    /// 长度可小于 `points.len()`（如 Raft 重放路径无需 seq，传空切片即可）。
    ///
    /// # 参数
    /// - `points`: 待插入的数据点（顺序与 `seqs` 对齐）。
    /// - `seqs`: 各点的 WAL 序列号（可为空）。
    pub fn insert_batch(&self, points: &[DataPoint], seqs: &[u64]) {
        // 先按分片分组索引，避免对同一分片反复加锁。
        let mut per_shard: Vec<Vec<usize>> = (0..SHARD_COUNT).map(|_| Vec::new()).collect();
        for (i, p) in points.iter().enumerate() {
            per_shard[shard_of(p.table_id)].push(i);
        }

        // 逐分片：加一次锁，按 table_id 分桶（保序）后走按列批量 append。
        for (shard_idx, idxs) in per_shard.iter().enumerate() {
            if idxs.is_empty() {
                continue;
            }
            let mut map = self.shards[shard_idx].lock();
            // 分片内再按 table_id 分桶（idx 已升序，桶内保持原相对顺序）。
            let mut per_table: HashMap<u32, Vec<usize>> = HashMap::new();
            for &i in idxs {
                per_table.entry(points[i].table_id).or_default().push(i);
            }
            for (table_id, group) in per_table {
                let chunk = map.entry(table_id).or_insert_with(|| {
                    ColumnarChunk::new(points[group[0]].tenant_id, table_id)
                });
                let pts: Vec<&DataPoint> = group.iter().map(|&i| &points[i]).collect();
                chunk.append_batch(&pts);
                let mut added = 0usize;
                let mut bytes = 0usize;
                for &i in &group {
                    if let Some(&seq) = seqs.get(i) {
                        chunk.record_wal_seq(seq);
                    }
                    bytes += Self::point_size(&points[i].values, &points[i].qualities);
                    added += 1;
                }
                self.total_points.fetch_add(added, Ordering::Relaxed);
                self.size_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    /// 获取指定 table 的 Chunk 克隆（只读快照）。
    pub fn get_chunk(&self, table_id: u32) -> Option<ColumnarChunk> {
        self.shards[shard_of(table_id)]
            .lock()
            .get(&table_id)
            .cloned()
    }

    /// 在分片锁内访问指定 table 的 Chunk（只读，供查询三源合并使用）。
    ///
    /// 相比 `chunks_snapshot()`（深拷贝整 map），本方法只锁单一分片、
    /// 只读单个 chunk，O(查询范围) 且不阻塞其它分片。
    pub fn with_chunk<R>(&self, table_id: u32, f: impl FnOnce(Option<&ColumnarChunk>) -> R) -> R {
        let map = self.shards[shard_of(table_id)].lock();
        f(map.get(&table_id))
    }

    /// 克隆所有 Chunk（供 Flush 读取一致快照，一次性成本）。
    ///
    /// 注意：仅在冻结表（不可变）或低频场景使用，避免高频查询走此深拷贝路径。
    pub fn chunks_snapshot(&self) -> HashMap<u32, ColumnarChunk> {
        let mut out = HashMap::new();
        for shard in &self.shards {
            let map = shard.lock();
            for (k, v) in map.iter() {
                out.insert(*k, v.clone());
            }
        }
        out
    }

    /// 获取所有 Chunk 的 `Arc` 只读借用快照（零拷贝）。
    ///
    /// 冻结表（immutable）不可变，`Arc<ColumnarChunk>` 直接借用底层 chunk 而非
    /// 深拷贝（对比 [`chunks_snapshot`](Self::chunks_snapshot) 的 `v.clone()`）。
    /// `ColumnarChunk` 为 `Send + Sync`，`Arc` 借用可直接喂 rayon 做并行编码。
    ///
    /// 仅限冻结表 / 低频只读场景使用；活跃表应继续走 `with_chunk` 单分片锁读。
    pub fn chunks_arc_snapshot(&self) -> HashMap<u32, Arc<ColumnarChunk>> {
        let mut out = HashMap::new();
        for shard in &self.shards {
            let map = shard.lock();
            for (k, v) in map.iter() {
                out.insert(*k, Arc::new(v.clone()));
            }
        }
        out
    }

    /// 检查是否应该触发冻结（total_points >= freeze_threshold）。
    pub fn should_freeze(&self) -> bool {
        self.total_points.load(Ordering::Relaxed) >= self.freeze_threshold
    }

    /// 获取当前总数据点数。
    pub fn total_points(&self) -> usize {
        self.total_points.load(Ordering::Relaxed)
    }

    /// 获取估算内存占用。
    pub fn size_bytes(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    /// 检查是否为空。
    pub fn is_empty(&self) -> bool {
        self.total_points() == 0
    }

    /// 获取 chunk 数量（跨分片求和）。
    pub fn table_count(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    /// 跨分片求「本表内最小 WAL 数据 seq」（点级 seq，== 最小帧 `raft_index`）。
    ///
    /// 无任何带 seq 的点（如 Raft 重放/单点便捷插入）时返回 `None`。
    pub fn min_wal_seq(&self) -> Option<u64> {
        let mut min: Option<u64> = None;
        for shard in &self.shards {
            for chunk in shard.lock().values() {
                if let Some(m) = chunk.min_wal_seq {
                    min = Some(min.map_or(m, |cur| cur.min(m)));
                }
            }
        }
        min
    }

    /// 跨分片求「本表内最大 WAL 数据 seq」。
    pub fn max_wal_seq(&self) -> Option<u64> {
        let mut max: Option<u64> = None;
        for shard in &self.shards {
            for chunk in shard.lock().values() {
                if let Some(m) = chunk.max_wal_seq {
                    max = Some(max.map_or(m, |cur| cur.max(m)));
                }
            }
        }
        max
    }
}

impl std::fmt::Debug for ColumnarMemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnarMemTable")
            .field("total_points", &self.total_points())
            .field("table_count", &self.table_count())
            .field("freeze_threshold", &self.freeze_threshold)
            .field("shard_count", &SHARD_COUNT)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::DataPoint;

    fn f64s(v: &[(u16, f64)]) -> Vec<(u16, TypedValue)> {
        v.iter().map(|(id, x)| (*id, TypedValue::F64(*x))).collect()
    }

    #[test]
    fn test_insert_and_get() {
        let mt = ColumnarMemTable::new(460_000);
        mt.insert(1, 100, &f64s(&[(0, 42.0)]), &[(0, 0xC0)], 0);
        mt.insert(1, 200, &f64s(&[(0, 43.0)]), &[(0, 0xC0)], 0);

        let chunk = mt.get_chunk(1).unwrap();
        assert_eq!(chunk.point_count, 2);
        assert_eq!(mt.total_points(), 2);
    }

    #[test]
    fn test_should_freeze() {
        let mt = ColumnarMemTable::new(2); // 小阈值用于测试
        mt.insert(1, 100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
        assert!(!mt.should_freeze());
        mt.insert(1, 200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);
        assert!(mt.should_freeze());
    }

    #[test]
    fn test_multi_table() {
        let mt = ColumnarMemTable::new(460_000);
        mt.insert(10, 100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
        mt.insert(20, 200, &f64s(&[(0, 2.0)]), &[(0, 0xC0)], 0);

        assert_eq!(mt.table_count(), 2);
    }

    #[test]
    fn test_chunks_snapshot() {
        let mt = ColumnarMemTable::new(460_000);
        mt.insert(1, 100, &f64s(&[(0, 1.0)]), &[(0, 0xC0)], 0);
        let snap = mt.chunks_snapshot();
        assert_eq!(snap.len(), 1);
        assert!(snap.contains_key(&1));
    }

    #[test]
    fn test_insert_multi_type() {
        let mt = ColumnarMemTable::new(460_000);
        mt.insert(
            7,
            100,
            &[
                (0, TypedValue::I64(10)),
                (1, TypedValue::String("tag".to_string())),
            ],
            &[(0, 0xC0)],
            0,
        );

        let chunk = mt.get_chunk(7).unwrap();
        assert_eq!(chunk.get_typed_value(0, 0), Some(TypedValue::I64(10)));
        assert_eq!(
            chunk.get_typed_value(1, 0),
            Some(TypedValue::String("tag".to_string()))
        );
    }

    #[test]
    fn test_with_chunk_reads_single_shard() {
        let mt = ColumnarMemTable::new(460_000);
        mt.insert(5, 100, &f64s(&[(0, 9.0)]), &[(0, 0xC0)], 0);

        let v = mt.with_chunk(5, |c| c.map(|c| c.get_value(0, 0)));
        assert_eq!(v, Some(Some(9.0)));

        // 不存在的 table → 闭包收到 None。
        let seen = mt.with_chunk(999, |c| {
            assert!(c.is_none());
            true
        });
        assert!(seen);
    }

    #[test]
    fn test_insert_batch_multi_table_with_seq() {
        let mt = ColumnarMemTable::new(460_000);

        // 3 个点：table 1 两点 + table 2 一点，跨分片（1%16=1, 2%16=2）。
        let points = vec![
            DataPoint {
                table_id: 1,
                tenant_id: TenantId::DEFAULT,
                ts: 100,
                server_ts: 101,
                values: f64s(&[(0, 1.0)]),
                qualities: vec![(0, 0xC0)],
                schema_version: 1,
                flags: 0,
            },
            DataPoint {
                table_id: 2,
                tenant_id: TenantId::DEFAULT,
                ts: 200,
                server_ts: 201,
                values: f64s(&[(0, 2.0)]),
                qualities: vec![(0, 0xC0)],
                schema_version: 1,
                flags: 0,
            },
            DataPoint {
                table_id: 1,
                tenant_id: TenantId::DEFAULT,
                ts: 300,
                server_ts: 301,
                values: f64s(&[(0, 3.0)]),
                qualities: vec![(0, 0xC0)],
                schema_version: 1,
                flags: 0,
            },
        ];
        let seqs = vec![10, 20, 30];

        mt.insert_batch(&points, &seqs);

        assert_eq!(mt.total_points(), 3);
        assert_eq!(mt.table_count(), 2);

        let c1 = mt.get_chunk(1).unwrap();
        assert_eq!(c1.point_count, 2);
        // max_wal_seq = max(10, 30) = 30
        assert_eq!(c1.max_wal_seq, Some(30));

        let c2 = mt.get_chunk(2).unwrap();
        assert_eq!(c2.point_count, 1);
        assert_eq!(c2.max_wal_seq, Some(20));
    }
}
