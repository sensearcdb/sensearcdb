//! SnapshotStore — 32-shard 实时快照数据库核心。
//!
//! ## 架构
//!
//! - **分片策略**: `shard_id = fxhash::hash(&tag_id) % shard_count`
//! - **默认分片数**: `num_cpus * shard_multiplier`（默认 128 on 16-core）。
//! - **并发模型**: 每分片独立 `parking_lot::RwLock`，读多写少场景优于 std RwLock 5x。
//! - **batch_get_typed**: 一次锁获取返回多个 tag（多列带类型），避免逐 tag 加锁。
//!
//! ## TOCTOU 保护（§7.8.1）
//!
//! `update_typed()` 中的值比较（`values_equal`）必须在持有 `write_lock` 期间执行。
//! 如果先 `read_lock` 比较再 `write_lock` 写入，两次锁获取之间可能有其他线程
//! 写入，导致变化漏检（check-then-act 竞态）。
//!
//! ## batch_get 短路优化（§7.8.5）
//!
//! 稀疏场景下，空 shard 的 `read_lock` 获取被跳过，减少无意义的锁竞争。

use std::collections::HashMap;
use std::sync::Arc;

use fxhash::FxBuildHasher;
use parking_lot::RwLock;
use tracing;

use crate::columnar::{group_by_column, ColumnValue, ColumnarShard, PendingPoint, TableMeta};
use crate::entry::SnapshotEntry;
use crate::metrics::SnapshotMetrics;
use crate::subscription::SnapshotSubscriptionManager;
use crate::tag_metadata::TagMetadataStore;
use crate::version::GlobalVersion;
use sensearcdb_common::{write_attr, ConfigHandle, TenantId, TypedValue};
use serde::{Deserialize, Serialize};

/// 列级可见性过滤函数：`(table_id, column_id) -> 是否可见`。
///
/// 语义对齐 `VisibleTagSet::contains`（`security::authorization::tag_visibility.rs`），
/// 但 snapshot crate 不依赖 security crate，故以闭包抽象传递。`None` 过滤 = 全可见。
pub type ColumnVisibleFn<'a> = dyn Fn(u32, u16) -> bool + 'a;

/// 批量查询结果值（多列带类型）。
#[derive(Debug, Clone)]
pub struct TypedSnapshotValue {
    /// 各列值（column_id 升序）。
    pub values: Vec<(u16, TypedValue)>,
    /// 各列质量码（与 values 对齐）。
    pub qualities: Vec<(u16, u8)>,
    /// 源时间戳。
    pub ts: i64,
    /// 服务器时间戳。
    pub server_ts: i64,
    /// 版本号。
    pub version: u64,
    /// 是否派生标签。
    pub is_derived: bool,
    /// TTL 过期标记。
    pub ttl_expired: bool,
    /// 列级源时间戳（与 values 对齐）。
    pub ts_values: Vec<(u16, i64)>,
    /// 列级服务器时间戳（与 values 对齐）。
    pub server_ts_values: Vec<(u16, i64)>,
    /// 租户 ID（内部隔离字段，非 proto；5206 数据面据此过滤）。
    pub tenant_id: TenantId,
}

impl TypedSnapshotValue {
    /// 首列便捷访问器：返回第一列的 f64 视图。
    #[inline]
    pub fn value(&self) -> f64 {
        self.values.first().map(|(_, v)| v.as_f64()).unwrap_or(0.0)
    }

    /// 首列便捷访问器：返回第一列的质量码。
    #[inline]
    pub fn quality(&self) -> u8 {
        self.qualities.first().map(|(_, q)| *q).unwrap_or(0)
    }
}

impl From<&SnapshotEntry> for TypedSnapshotValue {
    fn from(e: &SnapshotEntry) -> Self {
        Self {
            values: e.values.clone(),
            qualities: e.qualities.clone(),
            ts: e.ts,
            server_ts: e.server_ts,
            version: e.version,
            is_derived: e.is_derived,
            ttl_expired: e.ttl_expired,
            ts_values: e.ts_values.clone(),
            server_ts_values: e.server_ts_values.clone(),
            tenant_id: e.tenant_id,
        }
    }
}

/// 弱一致全量枚举游标（G1）。
///
/// 确定性顺序：桶按索引 `0..bucket_count`，桶内 key 升序。每页独立取读锁，
/// 不跨页持锁。游标 bincode 序列化后作为 gRPC `ListSnapshots` 的不透明 `cursor`。
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SnapshotCursor {
    /// 当前扫描到的桶索引。
    pub bucket_idx: usize,
    /// 该桶内已返回的最大 table_id；`None` = 从该桶最小 key 开始。
    pub after_table_id: Option<u32>,
}

/// 更新操作结果。
#[derive(Debug, Clone, Copy)]
pub struct SnapshotUpdateResult {
    /// 值是否发生了变化。
    pub changed: bool,
    /// 分配的版本号。
    pub version: u64,
}

/// 对投影出的 `SnapshotEntry` 做列级过滤。
///
/// 五组（`values`/`qualities`/`ts_values`/`server_ts_values`/`write_seqs`）按
/// column_id 对齐、同下标保留；过滤后 values 为空 → `None`（整表不可见，不返回
/// 该 tag，防泄露存在性）。`ts`/`server_ts` 兼容字段重算 = max(过滤后
/// `ts_values`/`server_ts_values`)；表级字段原样保留。
pub(crate) fn filter_entry_columns(
    entry: &SnapshotEntry,
    table_id: u32,
    visible: &ColumnVisibleFn<'_>,
) -> Option<SnapshotEntry> {
    // 以 values 列集为基准收集「可见列下标」；五组按同一下标集 pick。
    let keep: Vec<usize> = entry
        .values
        .iter()
        .enumerate()
        .filter(|(_, (cid, _))| visible(table_id, *cid))
        .map(|(i, _)| i)
        .collect();
    if keep.is_empty() {
        return None;
    }

    let values: Vec<(u16, TypedValue)> = keep.iter().map(|&i| entry.values[i].clone()).collect();
    let qualities: Vec<(u16, u8)> = keep.iter().map(|&i| entry.qualities[i]).collect();
    let ts_values: Vec<(u16, i64)> = keep.iter().map(|&i| entry.ts_values[i]).collect();
    let server_ts_values: Vec<(u16, i64)> =
        keep.iter().map(|&i| entry.server_ts_values[i]).collect();
    // write_seqs 与 values 对齐（project_entry 保证长度相等）；越界兜底按对应
    // column_id + None，仅防御，避免列子集不齐时静默张冠李戴。
    let write_seqs: Vec<(u16, Option<u64>)> = keep
        .iter()
        .map(|&i| {
            let cid = entry.values[i].0;
            entry.write_seqs.get(i).copied().unwrap_or((cid, None))
        })
        .collect();

    let ts = ts_values.iter().map(|(_, t)| *t).max().unwrap_or(0);
    let server_ts = server_ts_values.iter().map(|(_, t)| *t).max().unwrap_or(0);

    Some(SnapshotEntry {
        values,
        qualities,
        ts,
        server_ts,
        ts_values,
        server_ts_values,
        write_seqs,
        version: entry.version,
        prev_value: entry.prev_value.clone(),
        prev_ts: entry.prev_ts,
        is_derived: entry.is_derived,
        writer_session_id: entry.writer_session_id,
        ttl_expired: entry.ttl_expired,
        tenant_id: entry.tenant_id,
    })
}

/// TTL 清理模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TtlPurgeMode {
    /// 标记为 TTL 过期（保留记录）。
    MarkExpired,
    /// 彻底删除记录。
    Remove,
}

/// 实时快照数据库核心。
///
/// ## 线程安全
///
/// 所有公共方法均线程安全：
/// - `get` / `batch_get_typed`: 获取分片读锁
/// - `update_typed`: 获取分片写锁
pub struct SnapshotStore {
    /// 并发桶数组，每桶独立 parking_lot::RwLock。
    buckets: Vec<Arc<RwLock<ColumnarShard>>>,
    /// 并发桶数量。
    bucket_count: usize,
    /// 全局单调递增版本号。
    global_version: GlobalVersion,
    /// 订阅管理器。
    subscription_manager: Arc<SnapshotSubscriptionManager>,
    /// 标签元数据存储。
    pub metadata: Arc<TagMetadataStore>,
    /// Prometheus 指标。
    metrics: SnapshotMetrics,
}

impl SnapshotStore {
    /// 创建新的 SnapshotStore。
    ///
    /// # Arguments
    ///
    /// - `config_handle`: 全局配置句柄（读取 shard_multiplier 等参数）。
    /// - `subscription_manager`: 订阅管理器（共享引用）。
    /// - `metadata`: 标签元数据存储。
    /// - `metrics`: Prometheus 指标。
    pub fn new(
        config_handle: &ConfigHandle,
        subscription_manager: Arc<SnapshotSubscriptionManager>,
        metadata: Arc<TagMetadataStore>,
        metrics: SnapshotMetrics,
    ) -> Self {
        let config = config_handle.get();
        let bucket_count = num_cpus::get() * config.snapshot.shard_multiplier;

        let buckets: Vec<Arc<RwLock<ColumnarShard>>> = (0..bucket_count)
            .map(|_| Arc::new(RwLock::new(ColumnarShard::new())))
            .collect();

        tracing::info!(bucket_count = bucket_count, "SnapshotStore initialized");

        metrics.store_entries.with_label_values(&["all"]).set(0.0);

        Self {
            buckets,
            bucket_count,
            global_version: GlobalVersion::new(),
            subscription_manager,
            metadata,
            metrics,
        }
    }

    /// 从预构建的并发桶数组创建 SnapshotStore（用于恢复场景）。
    pub fn from_buckets(
        buckets: Vec<ColumnarShard>,
        initial_version: u64,
        subscription_manager: Arc<SnapshotSubscriptionManager>,
        metadata: Arc<TagMetadataStore>,
        metrics: SnapshotMetrics,
    ) -> Self {
        let bucket_count = buckets.len();
        let buckets: Vec<Arc<RwLock<ColumnarShard>>> = buckets
            .into_iter()
            .map(|s| Arc::new(RwLock::new(s)))
            .collect();

        let total_entries: usize = buckets.iter().map(|s| s.read().entry_count()).sum();
        metrics
            .store_entries
            .with_label_values(&["all"])
            .set(total_entries as f64);

        tracing::info!(
            bucket_count = bucket_count,
            total_entries = total_entries,
            initial_version = initial_version,
            "SnapshotStore restored from buckets"
        );

        Self {
            buckets,
            bucket_count,
            global_version: GlobalVersion::with_value(initial_version),
            subscription_manager,
            metadata,
            metrics,
        }
    }

    // ─── 并发桶索引 ───

    /// 计算 table_id 对应的并发桶索引。
    #[inline]
    fn bucket_index(&self, table_id: u32) -> usize {
        fxhash::hash(&table_id) as usize % self.bucket_count
    }

    // ─── 单点查询 ───

    /// 单点查询 O(1)：获取指定 tag 的最新快照。
    ///
    /// 返回 `Arc<SnapshotEntry>` 以避免拷贝。
    pub fn get(&self, table_id: u32) -> Option<Arc<SnapshotEntry>> {
        self.get_visible(table_id, None)
    }

    /// 单点查询 + 列级可见性过滤。
    ///
    /// `visible` 为 `Some` 时仅保留白名单内的列（整表过滤后返回 `None`）；
    /// `None` 等价 [`SnapshotStore::get`]（不过滤）。
    pub fn get_visible(
        &self,
        table_id: u32,
        visible: Option<&ColumnVisibleFn<'_>>,
    ) -> Option<Arc<SnapshotEntry>> {
        let bucket_idx = self.bucket_index(table_id);
        let guard = self.buckets[bucket_idx].read();
        let entry = guard.get(table_id)?;
        match visible {
            Some(v) => filter_entry_columns(&entry, table_id, v).map(Arc::new),
            None => Some(Arc::new(entry)),
        }
    }

    // ─── 批量查询 ───

    /// 同步批量查询（多列带类型）：一次锁获取返回多个 tag。
    ///
    /// 按 shard 分组 table_ids + 空分片短路优化，返回完整多列
    /// `values` / `qualities`，不做首列 as_f64 折叠。
    pub fn batch_get_typed(&self, table_ids: &[u32]) -> Vec<(u32, TypedSnapshotValue)> {
        self.batch_get_typed_visible(table_ids, None)
    }

    /// 同步批量查询 + 列级可见性过滤。
    ///
    /// `visible` 为 `Some` 时对每个 tag 做列级过滤（整表过滤后该 tag 不返回）；
    /// `None` 等价 [`SnapshotStore::batch_get_typed`]（不过滤）。
    pub fn batch_get_typed_visible(
        &self,
        table_ids: &[u32],
        visible: Option<&ColumnVisibleFn<'_>>,
    ) -> Vec<(u32, TypedSnapshotValue)> {
        if table_ids.is_empty() {
            return Vec::new();
        }

        // 按并发桶分组
        let mut bucket_groups: HashMap<usize, Vec<u32>> = HashMap::new();
        for &tid in table_ids {
            bucket_groups
                .entry(self.bucket_index(tid))
                .or_default()
                .push(tid);
        }

        let mut results = Vec::with_capacity(table_ids.len());

        for (bucket_idx, ids) in bucket_groups {
            let bucket = &self.buckets[bucket_idx];
            let guard = bucket.read();

            // OPT: 短路空桶
            if guard.is_empty() {
                continue;
            }

            for tid in ids {
                let entry = match guard.project_entry(tid) {
                    Some(e) => e,
                    None => continue,
                };
                let entry = match visible {
                    Some(v) => match filter_entry_columns(&entry, tid, v) {
                        Some(e) => e,
                        None => continue,
                    },
                    None => entry,
                };
                results.push((tid, TypedSnapshotValue::from(&entry)));
            }
        }

        self.metrics.batch_get_requests.with_label_values(&[]).inc();
        self.metrics
            .batch_get_tags
            .with_label_values(&[])
            .observe(table_ids.len() as f64);

        results
    }

    // ─── 写入/更新 ───

    /// 写入或更新快照值（多列带类型，列级 merge）。
    ///
    /// ## 列级 merge（G2）
    ///
    /// 宽表稀疏列写：写入携带的列覆盖对应四元组（value/quality/source_ts/server_ts），
    /// 未携带但已有 entry 存在的列保留旧四元组；不存在 entry 则新建。
    ///
    /// ## TOCTOU 保护
    ///
    /// 值比较在持有 `write_lock` 期间执行，避免 check-then-act 竞态。
    ///
    /// ## 归一化
    ///
    /// 入参 `values` / `qualities` 会被复制并按 column_id 升序排序后存储，
    /// 保证 entry 形状确定、序列化确定、值比较可按序进行。
    ///
    /// ## 列级时间戳
    ///
    /// 本次写入的所有列共享同一 row `ts` / `server_ts`，写入后 merged entry 的
    /// `ts = max(ts_values)`、`server_ts = max(server_ts_values)`。
    ///
    /// # Arguments
    ///
    /// - `tenant_id`: 租户 ID（写路径显式透传，禁止硬编码默认租户）。
    /// - `table_id`: tag (table) id。
    /// - `values`: 多列值（column_id → `TypedValue`）。
    /// - `qualities`: 多列质量码（column_id → u8）。
    /// - `ts`: 源时间戳 (SourceTimestamp, epoch ns)。
    /// - `server_ts`: 服务器时间戳 (ServerTimestamp, epoch ns)。**必填 i64**。
    /// - `write_seq`: 写入序号（LWW 决胜键）。`Some(seq)` = 单机 WAL 逐点 seq /
    ///   集群 Raft index / WAL 回放 record.seq；`None` = 无序号来源（遗留快照 /
    ///   跨节点迁移 / 批量导入 / SAC 重建）。
    /// - `is_derived`: 是否派生标签。
    /// - `session_id`: 写入来源的内部 session_id（递归防护）。
    pub fn update_typed(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        values: &[(u16, TypedValue)],
        qualities: &[(u16, u8)],
        ts: i64,
        server_ts: i64,
        write_seq: Option<u64>,
        is_derived: bool,
        session_id: Option<u64>,
    ) -> SnapshotUpdateResult {
        let point = PendingPoint {
            tenant_id,
            table_id,
            values,
            qualities,
            ts,
            server_ts,
            write_seq,
            is_derived,
            writer_session_id: session_id,
        };
        let results = self.update_batch(&[point]);
        let (_, changed, version) = results
            .into_iter()
            .next()
            .unwrap_or((table_id, false, self.global_version.current()));
        SnapshotUpdateResult { changed, version }
    }

    /// 批量写入（S1 核心）：四步 = 归组 + 批内 LWW → 按 shard 归组 →
    /// 每 shard 一次写锁批量 upsert → 锁外批量 notify。
    ///
    /// 返回每 table 的 `(table_id, changed, version)`。
    pub fn update_batch(&self, batch: &[PendingPoint<'_>]) -> Vec<(u32, bool, u64)> {
        if batch.is_empty() {
            return Vec::new();
        }

        // ① 按 (table_id, column_id) 归组 + 批内 LWW；同步维护 table_id → TableMeta。
        let (cands, metas) = group_by_column(batch);

        // ② 按 table 聚合列（column_id 升序），再按 shard 归组。
        let mut per_table: HashMap<u32, Vec<(u16, ColumnValue)>, FxBuildHasher> = HashMap::default();
        for ((tid, col), cv) in cands {
            per_table.entry(tid).or_default().push((col, cv));
        }
        for cols in per_table.values_mut() {
            cols.sort_unstable_by_key(|(c, _)| *c);
        }

        let mut shard_groups: HashMap<usize, Vec<(u32, Vec<(u16, ColumnValue)>)>> =
            HashMap::default();
        for (tid, cols) in per_table {
            shard_groups
                .entry(self.bucket_index(tid))
                .or_default()
                .push((tid, cols));
        }

        // ③ 每 shard 一次写锁批量 upsert；收集 (table_id, changed, version) 与 notify 暂存。
        let mut results: Vec<(u32, bool, u64)> = Vec::new();
        let mut notify_buf: Vec<(u32, Arc<SnapshotEntry>, u64)> = Vec::new();

        let mut shard_order: Vec<(usize, Vec<(u32, Vec<(u16, ColumnValue)>)>)> =
            shard_groups.into_iter().collect();
        shard_order.sort_by_key(|(s, _)| *s);

        for (shard, mut tables) in shard_order {
            tables.sort_unstable_by_key(|(tid, _)| *tid);
            let bucket = &self.buckets[shard];
            let mut guard =
                write_attr::timed_write_lock(bucket, write_attr::LockSlot::SnapshotBucket);

            for (tid, cols) in &tables {
                let meta = metas
                    .get(tid)
                    .expect("meta must exist for grouped table");
                let changes = guard.upsert_table(*tid, meta, cols);
                if changes.is_empty() {
                    results.push((*tid, false, self.global_version.current()));
                    continue;
                }
                let v = self.global_version.next();
                guard.set_row_version(*tid, v);
                guard.shard_version = v;
                let mut entry = changes.into_iter().next().unwrap().entry;
                entry.version = v;
                if self.subscription_manager.has_any_subscriber() {
                    notify_buf.push((*tid, Arc::new(entry.clone()), v));
                }
                results.push((*tid, true, v));
            }
            drop(guard);
        }

        // ④ 锁外批量 notify。
        for (tid, entry, v) in notify_buf {
            self.subscription_manager.notify(tid, entry, v);
        }

        results
    }

    /// 逐列导入（跨节点快照迁移用）：保留逐列 write_seqs + ts_values + server_ts_values。
    ///
    /// 与 [`SnapshotStore::update_typed`] 的差异：各列可携带各自独立的
    /// `write_seq` / `ts` / `server_ts`，不做「整行共享」的派生压平。
    ///
    /// # Arguments
    ///
    /// - `values` / `qualities`: 多列值/质量码（column_id → 值）。
    /// - `ts_values` / `server_ts_values`: 逐列 source/server 时间戳（与 values 对齐）。
    /// - `write_seqs`: 逐列写入序号；遗留条目可为空（按全列 `None` 补齐）。
    /// - `is_derived`: 是否派生标签。
    /// - `session_id`: 写入来源内部 session_id（递归防护）。
    pub fn import_columns_typed(
        &self,
        tenant_id: TenantId,
        table_id: u32,
        values: &[(u16, TypedValue)],
        qualities: &[(u16, u8)],
        ts_values: &[(u16, i64)],
        server_ts_values: &[(u16, i64)],
        write_seqs: &[(u16, Option<u64>)],
        is_derived: bool,
        session_id: Option<u64>,
    ) -> SnapshotUpdateResult {
        // 归一化：values 按 column_id 升序（作为基准列集）。
        let mut norm_values = values.to_vec();
        if !norm_values.is_sorted_by_key(|(c, _)| *c) {
            norm_values.sort_unstable_by_key(|(c, _)| *c);
        }

        // 其余四组均按 values 的 column_id 做 HashMap 对齐，缺列补语义安全默认值，
        // 不依赖「五组同列集」契约，避免列子集不齐时静默张冠李戴（QA 🟨 根治）。
        //   - qualities        缺列 → 0（OPC UA Good）
        //   - ts_values        缺列 → 0（无 source ts 信息）
        //   - server_ts_values 缺列 → 0（无 server ts 信息）
        //   - write_seqs       缺列 → None（无序号）
        let q_lookup: HashMap<u16, u8> = qualities.iter().copied().collect();
        let norm_qualities: Vec<(u16, u8)> = norm_values
            .iter()
            .map(|(c, _)| (*c, q_lookup.get(c).copied().unwrap_or(0)))
            .collect();

        let ts_lookup: HashMap<u16, i64> = ts_values.iter().copied().collect();
        let norm_ts_values: Vec<(u16, i64)> = norm_values
            .iter()
            .map(|(c, _)| (*c, ts_lookup.get(c).copied().unwrap_or(0)))
            .collect();

        let sts_lookup: HashMap<u16, i64> = server_ts_values.iter().copied().collect();
        let norm_server_ts_values: Vec<(u16, i64)> = norm_values
            .iter()
            .map(|(c, _)| (*c, sts_lookup.get(c).copied().unwrap_or(0)))
            .collect();

        let seq_lookup: HashMap<u16, Option<u64>> = write_seqs.iter().copied().collect();
        let norm_seqs: Vec<(u16, Option<u64>)> = norm_values
            .iter()
            .map(|(c, _)| (*c, seq_lookup.get(c).copied().unwrap_or(None)))
            .collect();

        // 构造逐列候选（各列独立 source_ts/server_ts/write_seq）+ 表级元数据。
        let mut cols: Vec<(u16, ColumnValue)> = Vec::with_capacity(norm_values.len());
        for i in 0..norm_values.len() {
            cols.push((
                norm_values[i].0,
                ColumnValue {
                    value: norm_values[i].1.clone(),
                    quality: norm_qualities[i].1,
                    source_ts: norm_ts_values[i].1,
                    server_ts: norm_server_ts_values[i].1,
                    write_seq: norm_seqs[i].1,
                },
            ));
        }
        let meta = TableMeta {
            tenant_id,
            is_derived,
            writer_session_id: session_id,
        };

        let bucket_idx = self.bucket_index(table_id);
        let bucket = &self.buckets[bucket_idx];
        let mut guard = write_attr::timed_write_lock(bucket, write_attr::LockSlot::SnapshotBucket);
        let changes = guard.upsert_table(table_id, &meta, &cols);

        if changes.is_empty() {
            drop(guard);
            SnapshotUpdateResult {
                changed: false,
                version: self.global_version.current(),
            }
        } else {
            let v = self.global_version.next();
            guard.set_row_version(table_id, v);
            guard.shard_version = v;
            let mut entry = changes.into_iter().next().unwrap().entry;
            entry.version = v;
            let notify = self.subscription_manager.has_any_subscriber();
            drop(guard);
            if notify {
                self.subscription_manager
                    .notify(table_id, Arc::new(entry), v);
            }
            SnapshotUpdateResult { changed: true, version: v }
        }
    }

    // ─── TTL 过期清理 ───

    /// TTL 过期清理。
    ///
    /// # Arguments
    ///
    /// - `table_id`: 要清理的 tag ID。
    /// - `mode`: 清理模式（标记过期 或 彻底删除）。
    pub fn purge_table(&self, table_id: u32, mode: TtlPurgeMode) {
        let bucket_idx = self.bucket_index(table_id);
        let bucket = &self.buckets[bucket_idx];
        let mut guard = bucket.write();

        match mode {
            TtlPurgeMode::MarkExpired => {
                if guard.mark_ttl_expired(table_id) {
                    let version = self.global_version.next();
                    guard.shard_version = version;
                    tracing::debug!(table_id = table_id, "tag marked TTL expired");
                }
            }
            TtlPurgeMode::Remove => {
                if guard.remove_table(table_id) {
                    tracing::debug!(table_id = table_id, "tag removed from snapshot");
                }
            }
        }
    }

    // ─── 全量转储 ───

    /// 获取所有并发桶的只读快照（用于序列化）。
    ///
    /// 返回 `(guards, global_version)`，调用方负责在 guards 存活期间
    /// 执行序列化操作。
    pub fn snapshot_all_buckets(
        &self,
    ) -> (Vec<parking_lot::RwLockReadGuard<'_, ColumnarShard>>, u64) {
        let guards: Vec<_> = self.buckets.iter().map(|s| s.read()).collect();
        let version = self.global_version.current();
        (guards, version)
    }

    // ─── 全量枚举（G1）───

    /// 弱一致游标分页全量枚举（G1）。
    ///
    /// ## 确定性顺序
    ///
    /// 桶按索引 `0..bucket_count`，桶内 key 升序（活跃桶内一次性排序）。
    ///
    /// ## 游标弱一致
    ///
    /// 每页独立取读锁，不跨页持锁（决议 3）。写入并发时页面边界可能漂移，
    /// 但「从头扫到尾」不重不漏地覆盖扫到时刻存在的条目。
    ///
    /// # Arguments
    ///
    /// - `cursor`: 上一页返回的游标；`None` = 从头开始。
    /// - `limit`: 页大小（`0` 会被钳制为 `1`，避免空转）。
    /// - `tenant_id`: 仅返回该租户的条目（store 内过滤，保证游标分页不重不漏）。
    ///
    /// # Returns
    ///
    /// `(entries, next_cursor)`；`next_cursor` 为 `None` 表示已扫完。
    pub fn list_all(
        &self,
        cursor: Option<SnapshotCursor>,
        limit: usize,
        tenant_id: TenantId,
    ) -> (Vec<(u32, TypedSnapshotValue)>, Option<SnapshotCursor>) {
        self.list_all_visible(cursor, limit, tenant_id, None)
    }

    /// 游标分页全量枚举 + 列级可见性过滤。
    ///
    /// 在原 tenant 过滤之后、`results.push` 之前再叠加列级过滤；过滤后为空的
    /// tag 同样 `continue`（不计入 limit 但推进游标），与 tenant 过滤口径一致，
    /// 保证分页不重不漏。`visible = None` 等价 [`SnapshotStore::list_all`]。
    pub fn list_all_visible(
        &self,
        cursor: Option<SnapshotCursor>,
        limit: usize,
        tenant_id: TenantId,
        visible: Option<&ColumnVisibleFn<'_>>,
    ) -> (Vec<(u32, TypedSnapshotValue)>, Option<SnapshotCursor>) {
        let limit = limit.max(1);
        let start_bucket = cursor.map(|c| c.bucket_idx).unwrap_or(0);
        let mut results: Vec<(u32, TypedSnapshotValue)> = Vec::new();

        for bucket_idx in start_bucket..self.bucket_count {
            let bucket = &self.buckets[bucket_idx];
            let guard = bucket.read();

            let keys: Vec<u32> = guard.iter_table_ids();

            for (idx, tid) in keys.iter().enumerate() {
                // 首桶续传：跳过 <= after_table_id 的 key（上一页已返回）。
                if bucket_idx == start_bucket {
                    if let Some(after) = cursor.and_then(|c| c.after_table_id) {
                        if *tid <= after {
                            continue;
                        }
                    }
                }

                let entry = match guard.project_entry(*tid) {
                    Some(e) => e,
                    None => continue,
                };
                // 租户隔离：跳过非本租户条目（不计入 limit，但仍推进游标），
                // 保证跨租户数据不泄露且分页不重不漏。
                if entry.tenant_id != tenant_id {
                    continue;
                }
                // 列级过滤：整表过滤后为空 → 跳过（不计入 limit，但仍推进游标）。
                let entry = match visible {
                    Some(v) => match filter_entry_columns(&entry, *tid, v) {
                        Some(e) => e,
                        None => continue,
                    },
                    None => entry,
                };
                results.push((*tid, TypedSnapshotValue::from(&entry)));
                if results.len() >= limit {
                    // 判断是否还有后续条目：当前桶内还有更大 key，或后续桶非空。
                    let has_more = (idx + 1) < keys.len()
                        || (bucket_idx + 1..self.bucket_count)
                            .any(|b| !self.buckets[b].read().is_empty());
                    let next = if has_more {
                        Some(SnapshotCursor {
                            bucket_idx,
                            after_table_id: Some(*tid),
                        })
                    } else {
                        None
                    };
                    return (results, next);
                }
            }
        }

        (results, None)
    }

    /// 逐桶短锁流式全量枚举（G1）。
    ///
    /// 每桶单独取读锁 + clone 后释放，单桶内存上界，不阻塞全库写入。
    /// 桶按索引升序、桶内 key 升序（确定性，与 [`SnapshotStore::list_all`] 同序）。
    pub fn iter_stream(&self) -> impl Iterator<Item = (u32, SnapshotEntry)> + '_ {
        self.iter_stream_visible(None)
    }

    /// 逐桶短锁流式全量枚举 + 列级可见性过滤。
    ///
    /// `visible` 为 `Some` 时对每个 tag 做列级过滤（整表过滤后该 tag 不产出）；
    /// `None` 等价 [`SnapshotStore::iter_stream`]（不过滤）。
    pub fn iter_stream_visible<'a>(
        &'a self,
        visible: Option<&'a ColumnVisibleFn<'a>>,
    ) -> impl Iterator<Item = (u32, SnapshotEntry)> + 'a {
        (0..self.bucket_count).flat_map(move |bucket_idx| {
            let guard = self.buckets[bucket_idx].read();
            guard
                .iter_table_ids()
                .into_iter()
                .filter_map(move |tid| {
                    let e = guard.project_entry(tid)?;
                    match visible {
                        Some(v) => filter_entry_columns(&e, tid, v).map(|e| (tid, e)),
                        None => Some((tid, e)),
                    }
                })
                .collect::<Vec<_>>()
        })
    }

    /// 返回并发桶数量。
    pub fn bucket_count(&self) -> usize {
        self.bucket_count
    }

    /// 返回当前全局版本号。
    pub fn current_version(&self) -> u64 {
        self.global_version.current()
    }

    /// 返回总条目数。
    pub fn total_entries(&self) -> usize {
        self.buckets.iter().map(|s| s.read().entry_count()).sum()
    }

    /// 返回订阅管理器的引用。
    pub fn subscription_manager(&self) -> &Arc<SnapshotSubscriptionManager> {
        &self.subscription_manager
    }
}

impl std::fmt::Debug for SnapshotStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotStore")
            .field("bucket_count", &self.bucket_count)
            .field("global_version", &self.global_version.current())
            .field("total_entries", &self.total_entries())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_detect::F64_EPSILON;
    use sensearcdb_common::{AppConfig, ConfigHandle, TypedValue};

    fn make_config() -> ConfigHandle {
        let mut cfg = AppConfig::default();
        cfg.snapshot.shard_multiplier = 1; // 减少测试分片数
        ConfigHandle::new(cfg)
    }

    fn make_store() -> SnapshotStore {
        let cfg = make_config();
        let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
        let metadata = Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror)));
        let metrics = SnapshotMetrics::noop();
        SnapshotStore::new(&cfg, sub_mgr, metadata, metrics)
    }

    struct TestSchemaMirror;
    impl crate::tag_metadata::SchemaMirrorRef for TestSchemaMirror {
        fn get_table_name(&self, _: u32) -> Option<String> {
            Some("test".into())
        }
        fn get_table_description(&self, _: u32) -> Option<String> {
            None
        }
        fn get_range_low(&self, _: u32, _: u16) -> f64 {
            0.0
        }
        fn get_range_high(&self, _: u32, _: u16) -> f64 {
            100.0
        }
        fn get_instrument_range_low(&self, _: u32, _: u16) -> Option<f64> {
            None
        }
        fn get_instrument_range_high(&self, _: u32, _: u16) -> Option<f64> {
            None
        }
        fn get_unit(&self, _: u32, _: u16) -> Option<String> {
            None
        }
        fn get_display_digits(&self, _: u32, _: u16) -> u8 {
            2
        }
        fn get_data_type(&self, _: u32, _: u16) -> sensearcdb_common::DataType {
            sensearcdb_common::DataType::F64
        }
        fn is_enabled(&self, _: u32) -> bool {
            true
        }
    }

    #[test]
    fn test_get_not_found() {
        let store = make_store();
        assert!(store.get(42).is_none());
    }

    #[test]
    fn test_update_and_get() {
        let store = make_store();
        let result = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(42.5))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        assert!(result.changed);
        assert_eq!(result.version, 1);

        let entry = store.get(1).unwrap();
        assert!((entry.value() - 42.5).abs() < F64_EPSILON);
        assert_eq!(entry.quality(), 0xC0);
        assert_eq!(entry.ts, 1000);
        assert_eq!(entry.server_ts, 1001);
        assert_eq!(entry.version, 1);
    }

    /// S2 Step 4：`update_typed` 显式传 tenant 写入 → `get` 读回保留非零 tenant_id。
    #[test]
    fn test_update_typed_preserves_tenant_id() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId(1),
            42,
            &[(0, TypedValue::F64(3.14))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        let entry = store.get(42).unwrap();
        assert_eq!(entry.tenant_id, sensearcdb_common::TenantId(1));
    }

    #[test]
    fn test_update_unchanged_within_epsilon() {
        let store = make_store();
        let r1 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        assert!(r1.changed);

        // 同 ts、值在 epsilon 内 → unchanged（列级 ts 纳入变化检测后，ts 相同才可比）。
        let r2 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0 + 1e-13))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        assert!(
            !r2.changed,
            "epsilon change should be detected as unchanged"
        );
    }

    /// G2：列级 ts 变化应判定为 changed（OPC UA STATUS_VALUE_TIMESTAMP）。
    #[test]
    fn test_update_typed_ts_change_detected() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        let r = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            1002,
            1001,
            None,
            false,
            None,
        );
        assert!(r.changed, "source ts change must be detected as changed");
    }

    #[test]
    fn test_update_changed_quality() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        let r2 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0x00)],
            1001,
            1002,
            None,
            false,
            None,
        );
        assert!(r2.changed, "quality change should be detected");
    }

    #[test]
    fn test_batch_get_typed() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(10.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            2,
            &[(0, TypedValue::F64(20.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            3,
            &[(0, TypedValue::F64(30.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );

        let results = store.batch_get_typed(&[1, 2, 3, 99]);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_batch_get_empty() {
        let store = make_store();
        let results = store.batch_get_typed(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_purge_table() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        assert!(store.get(1).is_some());

        store.purge_table(1, TtlPurgeMode::Remove);
        assert!(store.get(1).is_none());
    }

    #[test]
    fn test_purge_mark_expired() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );

        store.purge_table(1, TtlPurgeMode::MarkExpired);
        let entry = store.get(1).unwrap();
        assert!(entry.ttl_expired);
    }

    #[test]
    fn test_version_monotonic() {
        let store = make_store();
        let r1 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        let r2 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            2,
            &[(0, TypedValue::F64(2.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        assert!(r2.version > r1.version);
    }

    #[test]
    fn test_concurrent_updates_different_tags() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(make_store());
        let num_threads = 16;
        let tags_per_thread = 16;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let s = store.clone();
                thread::spawn(move || {
                    let base = t * tags_per_thread;
                    for i in 0..tags_per_thread {
                        let tid = (base + i) as u32;
                        s.update_typed(
                            sensearcdb_common::TenantId::DEFAULT,
                            tid,
                            &[(0, TypedValue::F64(i as f64))],
                            &[(0, 0xC0)],
                            1000 + i as i64,
                            2000 + i as i64,
                            None,
                            false,
                            None,
                        );
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let total = store.total_entries();
        assert_eq!(total, (num_threads * tags_per_thread) as usize);

        // Verify version is monotonic beyond entry count
        assert!(store.current_version() >= total as u64);
    }

    #[test]
    fn test_concurrent_updates_same_tag() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(make_store());
        let num_threads = 8;

        // All threads write to the same tag — last write wins
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let s = store.clone();
                thread::spawn(move || {
                    s.update_typed(
                        sensearcdb_common::TenantId::DEFAULT,
                        1,
                        &[(0, TypedValue::F64(t as f64))],
                        &[(0, 0xC0)],
                        1000 + t as i64,
                        2000 + t as i64,
                        None,
                        false,
                        None,
                    );
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Only one entry for tag 1
        let entry = store.get(1).unwrap();
        assert!(entry.value() >= 0.0 && entry.value() < num_threads as f64);
        assert_eq!(store.total_entries(), 1);
    }

    #[test]
    fn test_total_entries_zero_initially() {
        let store = make_store();
        assert_eq!(store.total_entries(), 0);
        assert_eq!(store.current_version(), 0);
    }

    #[test]
    fn test_bucket_count() {
        let store = make_store();
        assert!(store.bucket_count() > 0);
    }

    #[test]
    fn test_update_typed_multi_column_round_trip() {
        let store = make_store();
        let values = vec![
            (0u16, TypedValue::F64(3.14)),
            (1u16, TypedValue::I64(-42)),
            (2u16, TypedValue::String("open".to_string())),
            (3u16, TypedValue::Bool(true)),
            (4u16, TypedValue::Enum16(7)),
        ];
        let qualities = vec![
            (0u16, 0xC0u8),
            (1u16, 0xC0u8),
            (2u16, 0x40u8),
            (3u16, 0xC0u8),
            (4u16, 0x00u8),
        ];
        let r = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            9,
            &values,
            &qualities,
            1000,
            1001,
            None,
            false,
            None,
        );
        assert!(r.changed);

        let entry = store.get(9).unwrap();
        assert_eq!(entry.values, values);
        assert_eq!(entry.qualities, qualities);
        // 首列 facade 兼容
        assert_eq!(entry.value(), 3.14);
        assert_eq!(entry.quality(), 0xC0);

        let typed = store
            .batch_get_typed(&[9])
            .into_iter()
            .find(|(t, _)| *t == 9)
            .unwrap()
            .1;
        assert_eq!(typed.values, values);
        assert_eq!(typed.qualities, qualities);
    }

    #[test]
    fn test_update_typed_sorts_columns() {
        let store = make_store();
        let unsorted = vec![
            (2u16, TypedValue::I64(2)),
            (0u16, TypedValue::F64(1.0)),
            (1u16, TypedValue::I64(1)),
        ];
        let quals = vec![(2u16, 0x00u8), (0u16, 0xC0u8), (1u16, 0xC0u8)];
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &unsorted,
            &quals,
            1000,
            1001,
            None,
            false,
            None,
        );
        let entry = store.get(1).unwrap();
        assert_eq!(entry.values[0].0, 0);
        assert_eq!(entry.values[1].0, 1);
        assert_eq!(entry.values[2].0, 2);
        assert_eq!(entry.values[0], (0, TypedValue::F64(1.0)));
        assert_eq!(entry.qualities[0], (0, 0xC0));
    }

    #[test]
    fn test_update_typed_string_change_detected() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::String("open".into()))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        let r = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::String("close".into()))],
            &[(0, 0xC0)],
            1001,
            1002,
            None,
            false,
            None,
        );
        assert!(r.changed, "string value change must be detected");
    }

    #[test]
    fn test_update_typed_integer_exact_change() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::I64(1))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        let r = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::I64(1))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        assert!(!r.changed, "exact int equal must be unchanged");
        let r = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::I64(2))],
            &[(0, 0xC0)],
            1002,
            1003,
            None,
            false,
            None,
        );
        assert!(r.changed);
    }

    // ─── G2 列级 merge 回归 ───

    /// G2 核心：稀疏列先后写不丢列。
    #[test]
    fn test_update_typed_sparse_column_merge_keeps_absent() {
        let store = make_store();
        // 先写 {0}
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(10.0))],
            &[(0, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        // 再写 {1} —— 不得整行替换丢 col0
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(1, TypedValue::F64(20.0))],
            &[(1, 0x00)],
            3000,
            4000,
            None,
            false,
            None,
        );

        let entry = store.get(1).unwrap();
        assert_eq!(
            entry.values,
            vec![(0, TypedValue::F64(10.0)), (1, TypedValue::F64(20.0))]
        );
        assert_eq!(entry.qualities, vec![(0, 0xC0), (1, 0x00)]);
        // 列级 ts 独立
        assert_eq!(entry.ts_values, vec![(0, 1000), (1, 3000)]);
        assert_eq!(entry.server_ts_values, vec![(0, 2000), (1, 4000)]);
        // 兼容字段 = max
        assert_eq!(entry.ts, 3000);
        assert_eq!(entry.server_ts, 4000);
    }

    /// G2：混合覆盖 + 保留。
    #[test]
    fn test_update_typed_mixed_cover_and_keep() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            100,
            200,
            None,
            false,
            None,
        );
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(1, TypedValue::F64(20.0)), (2, TypedValue::F64(3.0))],
            &[(1, 0x00), (2, 0xC0)],
            300,
            400,
            None,
            false,
            None,
        );

        let entry = store.get(1).unwrap();
        assert_eq!(
            entry.values,
            vec![
                (0, TypedValue::F64(1.0)),
                (1, TypedValue::F64(20.0)),
                (2, TypedValue::F64(3.0)),
            ]
        );
        assert_eq!(entry.ts_values, vec![(0, 100), (1, 300), (2, 300)]);
    }

    // ─── 列级 ts 单调保护 ───

    /// ① 同列乱序：先 ts=200 再 ts=100 → 拒绝，读回 ts=200 的值不回退。
    #[test]
    fn test_update_typed_out_of_order_rejected() {
        let store = make_store();
        let r1 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(2.0))],
            &[(0, 0xC0)],
            200,
            2000,
            None,
            false,
            None,
        );
        assert!(r1.changed);

        let r2 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0x00)],
            100,
            1000,
            None,
            false,
            None,
        );
        assert!(!r2.changed, "乱序写入应被拒绝（changed=false）");

        let entry = store.get(1).unwrap();
        assert!(
            (entry.value() - 2.0).abs() < F64_EPSILON,
            "乱序点不得回滚当前值"
        );
        assert_eq!(entry.quality(), 0xC0);
        assert_eq!(entry.ts, 200);
        assert_eq!(entry.server_ts, 2000);
        assert_eq!(entry.ts_values, vec![(0, 200)]);
        assert_eq!(entry.server_ts_values, vec![(0, 2000)]);
    }

    /// ② 列级隔离：col0@500、col1@300，写 {col0@400,col1@400} → col0 拒、col1 收。
    #[test]
    fn test_update_typed_column_level_isolation() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            500,
            5000,
            None,
            false,
            None,
        );
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(1, TypedValue::F64(2.0))],
            &[(1, 0xC0)],
            300,
            3000,
            None,
            false,
            None,
        );

        let r = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(9.0)), (1, TypedValue::F64(8.0))],
            &[(0, 0x00), (1, 0x00)],
            400,
            4000,
            None,
            false,
            None,
        );
        assert!(r.changed, "col1 接受 → changed=true");

        let entry = store.get(1).unwrap();
        assert_eq!(
            entry.values,
            vec![(0, TypedValue::F64(1.0)), (1, TypedValue::F64(8.0))]
        );
        assert_eq!(entry.qualities, vec![(0, 0xC0), (1, 0x00)]);
        assert_eq!(entry.ts_values, vec![(0, 500), (1, 400)]);
        assert_eq!(entry.server_ts_values, vec![(0, 5000), (1, 4000)]);
        assert_eq!(entry.ts, 500);
        assert_eq!(entry.server_ts, 5000);
    }

    /// ③ 同 ts 语义统一为 LWW：write_seq 大者覆盖小者（后到者胜）；seq 小晚到不覆盖。
    #[test]
    fn test_update_typed_same_ts_lww_by_seq() {
        let store = make_store();
        // seq=5 写入 v=1.0。
        let r1 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            100,
            200,
            Some(5),
            false,
            None,
        );
        assert!(r1.changed);

        // 同 ts seq=10 写入 v=9.0 → 覆盖（后到者胜）。
        let r2 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(9.0))],
            &[(0, 0x00)],
            100,
            999,
            Some(10),
            false,
            None,
        );
        assert!(r2.changed, "同 ts 高 write_seq 必须覆盖");

        let entry = store.get(1).unwrap();
        assert!((entry.value() - 9.0).abs() < F64_EPSILON);
        assert_eq!(entry.quality(), 0x00);
        assert_eq!(entry.ts, 100);
        assert_eq!(entry.server_ts, 999, "覆盖时 server_ts 同步更新");
        assert_eq!(entry.write_seqs, vec![(0, Some(10))]);

        // 同 ts seq=3（乱序晚到）→ 拒绝，不回滚。
        let r3 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(0.5))],
            &[(0, 0xC0)],
            100,
            1000,
            Some(3),
            false,
            None,
        );
        assert!(!r3.changed, "同 ts 低 write_seq 不得回滚");
        let entry2 = store.get(1).unwrap();
        assert!((entry2.value() - 9.0).abs() < F64_EPSILON);
        assert_eq!(entry2.write_seqs, vec![(0, Some(10))]);
    }

    /// ③ None vs Some：带 seq 的新写覆盖遗留 None 条目。
    #[test]
    fn test_update_typed_some_seq_beats_none() {
        let store = make_store();
        // 遗留条目（None）。
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            100,
            200,
            None,
            false,
            None,
        );
        let r = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(9.0))],
            &[(0, 0x00)],
            100,
            999,
            Some(1),
            false,
            None,
        );
        assert!(r.changed, "Some(seq) 必须覆盖 None 遗留条目");
        let e = store.get(1).unwrap();
        assert!((e.value() - 9.0).abs() < F64_EPSILON);
        assert_eq!(e.write_seqs, vec![(0, Some(1))]);
    }

    /// ④ None vs None：退 server_ts 大者胜；server_ts 相同 → 保留 old。
    #[test]
    fn test_update_typed_none_vs_none_server_ts() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            100,
            200,
            None,
            false,
            None,
        );
        // None vs None，server_ts 大者胜。
        let r = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(9.0))],
            &[(0, 0x00)],
            100,
            999,
            None,
            false,
            None,
        );
        assert!(r.changed);
        let e = store.get(1).unwrap();
        assert!((e.value() - 9.0).abs() < F64_EPSILON);
        assert_eq!(e.write_seqs, vec![(0, None)]);

        // server_ts 相同 → 保留 old。
        let r2 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(8.0))],
            &[(0, 0xC0)],
            100,
            999,
            None,
            false,
            None,
        );
        assert!(!r2.changed, "None vs None 且 server_ts 相同 → 保留 old");
        let e2 = store.get(1).unwrap();
        assert!((e2.value() - 9.0).abs() < F64_EPSILON);
    }

    /// 独立 QA（D 节）：None 写不覆盖已有 Some（LWW 决胜 None < Some），
    /// 且 None 写落库后 write_seqs 列为 null（不被伪造为 Some(0)）。
    #[test]
    fn test_none_seq_stored_null_and_does_not_overwrite_some() {
        let store = make_store();

        // 1. None 首写：write_seqs 列为 null。
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0xC0)],
            100,
            200,
            None,
            false,
            None,
        );
        let e0 = store.get(1).unwrap();
        assert_eq!(e0.write_seqs, vec![(0, None)], "None 写必须存为 null，不得伪造 Some");

        // 2. Some(5) 同 ts 覆盖 None。
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(9.0))],
            &[(0, 0x00)],
            100,
            300,
            Some(5),
            false,
            None,
        );
        let e1 = store.get(1).unwrap();
        assert!((e1.value() - 9.0).abs() < F64_EPSILON);
        assert_eq!(e1.write_seqs, vec![(0, Some(5))]);

        // 3. None（同 ts，server_ts 更大）晚到：不得回滚 Some(5)。
        let r = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(0.5))],
            &[(0, 0xC0)],
            100,
            9999,
            None,
            false,
            None,
        );
        assert!(!r.changed, "None 不得覆盖已有 Some（即使 server_ts 更大）");
        let e2 = store.get(1).unwrap();
        assert!((e2.value() - 9.0).abs() < F64_EPSILON, "值必须保持 Some(5) 写入的 9.0");
        assert_eq!(e2.write_seqs, vec![(0, Some(5))], "write_seqs 不得被 None 回滚");
    }

    /// 跨节点迁移逐列导入：逐列 write_seqs / ts_values / server_ts_values 保真，
    /// 乱序入参归一化后仍与 values 对齐，不压平、不丢 None。
    #[test]
    fn test_import_columns_typed_preserves_per_column_fields() {
        let store = make_store();
        // 故意乱序，验证归一化（column_id 升序 + 三个逐列数组同步重排）。
        let values = vec![(2, TypedValue::I64(7)), (0, TypedValue::F64(1.5))];
        let qualities = vec![(2, 0x40), (0, 0xC0)];
        let ts_values = vec![(2, 2000), (0, 1000)];
        let server_ts_values = vec![(2, 4000), (0, 5000)];
        let write_seqs = vec![(2, None), (0, Some(7))];

        let r = store.import_columns_typed(
            sensearcdb_common::TenantId(3),
            42,
            &values,
            &qualities,
            &ts_values,
            &server_ts_values,
            &write_seqs,
            false,
            Some(99),
        );
        assert!(r.changed);

        let e = store.get(42).unwrap();
        // 归一化后 column_id 升序，五组对齐。
        assert_eq!(
            e.values,
            vec![(0, TypedValue::F64(1.5)), (2, TypedValue::I64(7))]
        );
        assert_eq!(e.qualities, vec![(0, 0xC0), (2, 0x40)]);
        assert_eq!(e.ts_values, vec![(0, 1000), (2, 2000)]);
        assert_eq!(e.server_ts_values, vec![(0, 5000), (2, 4000)]);
        assert_eq!(e.write_seqs, vec![(0, Some(7)), (2, None)]);
        // 兼容字段 = max(逐列)。
        assert_eq!(e.ts, 2000);
        assert_eq!(e.server_ts, 5000);
        assert_eq!(e.tenant_id, sensearcdb_common::TenantId(3));
        assert_eq!(e.writer_session_id, Some(99));
    }

    /// 遗留条目 write_seqs 为空（#[serde(default)]）→ 全列按 None 补齐，不 panic。
    #[test]
    fn test_import_columns_typed_legacy_empty_write_seqs() {
        let store = make_store();
        let r = store.import_columns_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            &[(0, 1000), (1, 2000)],
            &[(0, 1001), (1, 2001)],
            &[],
            false,
            None,
        );
        assert!(r.changed);
        let e = store.get(1).unwrap();
        assert_eq!(e.values, vec![(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))]);
        assert_eq!(e.write_seqs, vec![(0, None), (1, None)]);
    }

    /// ⑥ 全拒绝 → changed=false，不 insert / 不 bump entry version / 不动 prev_* /
    /// 不推进 global_version（无无效 checkpoint 落盘）。
    #[test]
    fn test_update_typed_all_rejected_no_side_effect() {
        let store = make_store();
        let r1 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(2.0))],
            &[(0, 0xC0)],
            200,
            2000,
            None,
            false,
            None,
        );
        assert!(r1.changed);

        let before = store.get(1).unwrap();
        let version_before = before.version;
        let prev_value_before = before.prev_value.clone();
        let prev_ts_before = before.prev_ts;
        let global_version_before = store.current_version();

        let r2 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0x00)],
            100,
            1000,
            None,
            false,
            None,
        );
        assert!(!r2.changed);

        let after = store.get(1).unwrap();
        assert_eq!(after.version, version_before, "entry version 不 bump");
        assert_eq!(after.prev_value, prev_value_before, "prev_value 不动");
        assert_eq!(after.prev_ts, prev_ts_before, "prev_ts 不动");
        assert_eq!(after.ts, 200);
        assert_eq!(after.server_ts, 2000);
        assert_eq!(after.ts_values, vec![(0, 200)]);
        assert_eq!(
            store.current_version(),
            global_version_before,
            "全拒绝时 global_version 不 +1（否则 checkpointer 会判脏并无效落盘）"
        );
        assert_eq!(
            r2.version, global_version_before,
            "拒绝时返回的 version 为当前值"
        );
    }

    // ─── 1a 快速路径（整行覆盖） ───

    /// 整行覆盖（列集合相同 + 每列严格更新 ts）→ changed，prev_* 正确追踪。
    #[test]
    fn test_update_typed_fast_path_full_row_overwrite() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[
                (0, TypedValue::F64(1.0)),
                (1, TypedValue::I64(2)),
                (2, TypedValue::F64(3.0)),
            ],
            &[(0, 0xC0), (1, 0xC0), (2, 0xC0)],
            100,
            1001,
            Some(5),
            false,
            None,
        );
        let r = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[
                (0, TypedValue::F64(9.0)),
                (1, TypedValue::I64(8)),
                (2, TypedValue::F64(7.0)),
            ],
            &[(0, 0x00), (1, 0x00), (2, 0x00)],
            200,
            2001,
            Some(6),
            false,
            None,
        );
        assert!(r.changed, "整行覆盖（严格更新）必须 changed");
        let e = store.get(1).unwrap();
        assert_eq!(
            e.values,
            vec![
                (0, TypedValue::F64(9.0)),
                (1, TypedValue::I64(8)),
                (2, TypedValue::F64(7.0)),
            ]
        );
        assert_eq!(e.qualities, vec![(0, 0x00), (1, 0x00), (2, 0x00)]);
        assert_eq!(e.ts_values, vec![(0, 200), (1, 200), (2, 200)]);
        assert_eq!(
            e.server_ts_values,
            vec![(0, 2001), (1, 2001), (2, 2001)]
        );
        assert_eq!(e.ts, 200);
        assert_eq!(e.server_ts, 2001);
        // S1 不存储 prev_value/prev_ts，投影恒 None。
        assert_eq!(e.prev_value, None);
        assert_eq!(e.prev_ts, None);
        assert_eq!(
            e.write_seqs,
            vec![(0, Some(6)), (1, Some(6)), (2, Some(6))]
        );
    }

    /// 1a 边界：列集合相同但 ts 相等（非严格更新）→ 回退 merge 走 write_seq LWW，
    /// 不得整行覆盖（同 ts 高 seq 覆盖、低 seq 回滚）。
    #[test]
    fn test_update_typed_fast_path_same_ts_falls_back_to_lww() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            100,
            1001,
            Some(5),
            false,
            None,
        );
        // 同 ts，高 seq 覆盖。
        let r = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(9.0)), (1, TypedValue::F64(8.0))],
            &[(0, 0x00), (1, 0x00)],
            100,
            999,
            Some(10),
            false,
            None,
        );
        assert!(r.changed, "同 ts 高 write_seq 必须覆盖");
        let e = store.get(1).unwrap();
        assert_eq!(
            e.values,
            vec![(0, TypedValue::F64(9.0)), (1, TypedValue::F64(8.0))]
        );
        assert_eq!(e.write_seqs, vec![(0, Some(10)), (1, Some(10))]);
        assert_eq!(e.server_ts, 999);

        // 同 ts，低 seq 回滚。
        let r2 = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(0.5)), (1, TypedValue::F64(0.6))],
            &[(0, 0xC0), (1, 0xC0)],
            100,
            1000,
            Some(3),
            false,
            None,
        );
        assert!(!r2.changed, "同 ts 低 write_seq 不得回滚");
        let e2 = store.get(1).unwrap();
        assert_eq!(
            e2.values,
            vec![(0, TypedValue::F64(9.0)), (1, TypedValue::F64(8.0))]
        );
        assert_eq!(e2.write_seqs, vec![(0, Some(10)), (1, Some(10))]);
    }

    // ─── G1 全量枚举 ───

    #[test]
    fn test_list_all_pagination_complete() {
        let store = make_store();
        for i in 1u32..=10u32 {
            store.update_typed(
                sensearcdb_common::TenantId::DEFAULT,
                i,
                &[(0, TypedValue::F64(i as f64))],
                &[(0, 0xC0)],
                1000,
                1001,
                None,
                false,
                None,
            );
        }

        let mut seen: Vec<u32> = Vec::new();
        let mut cursor = None;
        loop {
            let (page, next) = store.list_all(cursor, 3, sensearcdb_common::TenantId::DEFAULT);
            seen.extend(page.iter().map(|(tid, _)| *tid));
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        seen.sort_unstable();
        assert_eq!(seen, (1u32..=10u32).collect::<Vec<_>>());
        assert_eq!(store.total_entries(), 10);
    }

    #[test]
    fn test_list_all_deterministic() {
        let store = make_store();
        for i in 1u32..=10u32 {
            store.update_typed(
                sensearcdb_common::TenantId::DEFAULT,
                i,
                &[(0, TypedValue::F64(i as f64))],
                &[(0, 0xC0)],
                1000,
                1001,
                None,
                false,
                None,
            );
        }

        let collect = |limit: usize| {
            let mut out = Vec::new();
            let mut cursor = None;
            loop {
                let (page, next) =
                    store.list_all(cursor, limit, sensearcdb_common::TenantId::DEFAULT);
                out.extend(page.into_iter().map(|(tid, _)| tid));
                match next {
                    Some(c) => cursor = Some(c),
                    None => break,
                }
            }
            out
        };

        assert_eq!(collect(3), collect(5), "顺序必须确定性一致");
    }

    #[test]
    fn test_list_all_empty_store() {
        let store = make_store();
        let (page, next) = store.list_all(None, 100, sensearcdb_common::TenantId::DEFAULT);
        assert!(page.is_empty());
        assert!(next.is_none());
    }

    #[test]
    fn test_list_all_limit_one_advances() {
        let store = make_store();
        for i in 1u32..=3u32 {
            store.update_typed(
                sensearcdb_common::TenantId::DEFAULT,
                i,
                &[(0, TypedValue::F64(i as f64))],
                &[(0, 0xC0)],
                1000,
                1001,
                None,
                false,
                None,
            );
        }

        let (p1, c1) = store.list_all(None, 1, sensearcdb_common::TenantId::DEFAULT);
        assert_eq!(p1.len(), 1);
        assert!(c1.is_some());

        let (p2, c2) = store.list_all(c1, 1, sensearcdb_common::TenantId::DEFAULT);
        assert_eq!(p2.len(), 1);
        assert_ne!(p1[0].0, p2[0].0, "游标续传不得重复");
        assert!(c2.is_some());

        let (p3, c3) = store.list_all(c2, 1, sensearcdb_common::TenantId::DEFAULT);
        assert_eq!(p3.len(), 1);
        assert_ne!(p1[0].0, p3[0].0);
        assert_ne!(p2[0].0, p3[0].0);
        assert!(c3.is_none(), "三条目扫完后游标应为 None");
    }

    #[test]
    fn test_iter_stream_covers_all() {
        let store = make_store();
        for i in 1u32..=10u32 {
            store.update_typed(
                sensearcdb_common::TenantId::DEFAULT,
                i,
                &[(0, TypedValue::F64(i as f64))],
                &[(0, 0xC0)],
                1000,
                1001,
                None,
                false,
                None,
            );
        }

        let mut tids: Vec<u32> = store.iter_stream().map(|(tid, _)| tid).collect();
        tids.sort_unstable();
        assert_eq!(tids, (1u32..=10u32).collect::<Vec<_>>());
    }

    // ─── 5206 认证 + 租户隔离 ───

    /// 用例 11：`TypedSnapshotValue::from` 往返含 tenant_id（batch 过滤依赖此字段）。
    #[test]
    fn test_typed_value_from_preserves_tenant_id() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId(7),
            42,
            &[(0, TypedValue::F64(3.14))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );

        let entry = store.get(42).unwrap();
        let typed = TypedSnapshotValue::from(entry.as_ref());
        assert_eq!(typed.tenant_id, sensearcdb_common::TenantId(7));
        assert!((typed.value() - 3.14).abs() < F64_EPSILON);

        // batch_get_typed 返回的 TypedSnapshotValue 同样透传 tenant_id。
        let batched = store.batch_get_typed(&[42]);
        assert_eq!(batched.len(), 1);
        assert_eq!(batched[0].1.tenant_id, sensearcdb_common::TenantId(7));
    }

    /// 用例 12：`list_all(cursor, limit, tenant)` 混租户数据分页——每页仅本租户，
    /// 跨页不重不漏。
    #[test]
    fn test_list_all_tenant_filter_pagination() {
        let store = make_store();
        // 租户 1：偶数 tag；租户 2：奇数 tag。
        for i in 1u32..=10u32 {
            let tenant = if i % 2 == 0 {
                sensearcdb_common::TenantId(1)
            } else {
                sensearcdb_common::TenantId(2)
            };
            store.update_typed(
                tenant,
                i,
                &[(0, TypedValue::F64(i as f64))],
                &[(0, 0xC0)],
                1000,
                1001,
                None,
                false,
                None,
            );
        }

        // 租户 1 分页：只应看到偶数 tag（2/4/6/8/10）。
        let mut seen: Vec<u32> = Vec::new();
        let mut cursor = None;
        loop {
            let (page, next) = store.list_all(cursor, 2, sensearcdb_common::TenantId(1));
            assert!(
                page.iter()
                    .all(|(_, v)| v.tenant_id == sensearcdb_common::TenantId(1)),
                "每页仅本租户条目"
            );
            seen.extend(page.iter().map(|(tid, _)| *tid));
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        seen.sort_unstable();
        assert_eq!(seen, vec![2u32, 4, 6, 8, 10], "跨页不重不漏，仅本租户");

        // 租户 2 单页收齐奇数 tag。
        let (page, next) = store.list_all(None, 100, sensearcdb_common::TenantId(2));
        let mut odds: Vec<u32> = page.iter().map(|(tid, _)| *tid).collect();
        odds.sort_unstable();
        assert_eq!(odds, vec![1u32, 3, 5, 7, 9]);
        assert!(next.is_none());
    }

    /// 用例 12b（永久回归）：多租户交错 + 小页（limit=2）强制多页。
    ///
    /// 覆盖租户隔离与游标分页交互的核心回归点：跨页无遗漏、无重读、
    /// 每页无跨租户泄露、分页必然终止（不空转/死循环）。
    #[test]
    fn test_list_all_tenant_filter_small_page_interleaved() {
        let store = make_store();
        // 12 个交错 tag：偶数归 tenant1、奇数归 tenant2。
        for i in 1u32..=12u32 {
            let tenant = if i % 2 == 0 {
                sensearcdb_common::TenantId(1)
            } else {
                sensearcdb_common::TenantId(2)
            };
            store.update_typed(
                tenant,
                i,
                &[(0, TypedValue::F64(i as f64))],
                &[(0, 0xC0)],
                1000,
                1001,
                None,
                false,
                None,
            );
        }

        let mut seen: Vec<u32> = Vec::new();
        let mut page_count = 0usize;
        let mut cursor = None;
        loop {
            page_count += 1;
            // ④ 终止保护：每个非终止页必然至少含 1 条匹配项（limit=2），
            //    最多 6 条 tenant1 条目 + 1 张可能的终止空页，16 足够。
            assert!(page_count <= 16, "分页必须终止，不得空转/死循环");

            let (page, next) = store.list_all(cursor, 2, sensearcdb_common::TenantId(1));
            // ③ 每页仅本租户，无跨租户泄露。
            assert!(
                page.iter()
                    .all(|(_, v)| v.tenant_id == sensearcdb_common::TenantId(1)),
                "每页不得含跨租户条目"
            );
            seen.extend(page.iter().map(|(tid, _)| *tid));

            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        // ① 无遗漏：tenant1 跨页完整读回 {2,4,6,8,10,12}。
        let mut sorted = seen.clone();
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            vec![2u32, 4, 6, 8, 10, 12],
            "tenant1 跨页完整无遗漏"
        );

        // ② 无重读：去重后长度不变。
        let mut dedup = seen.clone();
        dedup.sort_unstable();
        dedup.dedup();
        assert_eq!(dedup.len(), seen.len(), "跨页不得重读");
    }

    /// P0-1 回归：无订阅者时 update_typed 值变化 → entry 落库正确但 notify 不触发。
    #[test]
    fn test_update_no_subscriber_stores_entry_without_notify() {
        let store = make_store();
        let sub_mgr = store.subscription_manager();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::DataChangeNotification>(16);

        // 订阅后立即退订，使 subscriber_count 归零（短路生效），rx 仍持有接收端。
        let id = sub_mgr.subscribe(vec![1], tx, false);
        assert!(sub_mgr.has_any_subscriber());
        assert!(sub_mgr.unsubscribe(&id));
        assert!(!sub_mgr.has_any_subscriber());

        let r = store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(42.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        assert!(r.changed);

        // entry 落库正确（落库与订阅无关）。
        let entry = store.get(1).unwrap();
        assert!((entry.value() - 42.0).abs() < F64_EPSILON);

        // 无订阅者 → notify 不触发。
        assert!(rx.try_recv().is_err());
    }

    // ─── Q5 列级权限过滤 ───

    /// Q5：`get_visible(None)` 全可见不收缩，与 `get` 完全一致。
    #[test]
    fn test_get_visible_none_unrestricted() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::I64(2))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        let full = store.get(1).unwrap();
        let vis = store.get_visible(1, None).unwrap();
        assert_eq!(vis.values, full.values);
        assert_eq!(vis.qualities, full.qualities);
        assert_eq!(vis.write_seqs, full.write_seqs);
        assert_eq!(vis.ts, full.ts);
        assert_eq!(vis.server_ts, full.server_ts);
    }

    /// Q5：`get_visible(Some)` 只保留白名单列，五组同下标对齐，ts/server_ts 重算。
    #[test]
    fn test_get_visible_filters_to_whitelist() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[
                (0, TypedValue::F64(1.0)),
                (1, TypedValue::I64(2)),
                (2, TypedValue::F64(3.0)),
            ],
            &[(0, 0xC0), (1, 0x00), (2, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        // 仅 col1 可见。
        let visible = |_tid: u32, cid: u16| cid == 1;
        let e = store.get_visible(1, Some(&visible)).unwrap();
        assert_eq!(e.values, vec![(1, TypedValue::I64(2))]);
        assert_eq!(e.qualities, vec![(1, 0x00)]);
        assert_eq!(e.ts_values, vec![(1, 1000)]);
        assert_eq!(e.server_ts_values, vec![(1, 1001)]);
        assert_eq!(e.write_seqs, vec![(1, None)]);
        assert_eq!(e.ts, 1000);
        assert_eq!(e.server_ts, 1001);
        assert_eq!(e.tenant_id, sensearcdb_common::TenantId::DEFAULT);
    }

    /// Q5：整表无可见列 → 返回 None（不泄露存在性）。
    #[test]
    fn test_get_visible_all_filtered_returns_none() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        let visible = |_tid: u32, _cid: u16| false;
        assert!(store.get_visible(1, Some(&visible)).is_none());
    }

    /// Q5：`batch_get_typed_visible` 过滤后仅保留白名单列；整表过滤后不返回。
    #[test]
    fn test_batch_get_typed_visible_filters() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            2,
            &[(0, TypedValue::F64(10.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );

        // 白名单只含 (1, 0)：tag1 只剩 col0，tag2 整表过滤不返回。
        let visible = |tid: u32, cid: u16| tid == 1 && cid == 0;
        let res = store.batch_get_typed_visible(&[1, 2], Some(&visible));
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].0, 1);
        assert_eq!(res[0].1.values, vec![(0, TypedValue::F64(1.0))]);
    }

    /// Q5：`list_all_visible` 与 tenant 过滤叠加：跨租户不泄露，本租户再叠加列级过滤。
    #[test]
    fn test_list_all_visible_filters_with_tenant() {
        let store = make_store();
        // tenant1: tag1 {col0, col1}; tenant2: tag2 {col0}
        store.update_typed(
            sensearcdb_common::TenantId(1),
            1,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        store.update_typed(
            sensearcdb_common::TenantId(2),
            2,
            &[(0, TypedValue::F64(10.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );

        // tenant1 只允许 col0。
        let visible = |_tid: u32, cid: u16| cid == 0;
        let (page, next) =
            store.list_all_visible(None, 100, sensearcdb_common::TenantId(1), Some(&visible));
        assert!(next.is_none());
        assert_eq!(
            page.len(),
            1,
            "tenant2 的 tag2 被 tenant 过滤；tag1 只保留 col0"
        );
        assert_eq!(page[0].0, 1);
        assert_eq!(page[0].1.values, vec![(0, TypedValue::F64(1.0))]);
    }

    /// Q5：`iter_stream_visible` 过滤后仅产出可见列。
    #[test]
    fn test_iter_stream_visible_filters() {
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(1.0)), (1, TypedValue::F64(2.0))],
            &[(0, 0xC0), (1, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        let visible = |_tid: u32, cid: u16| cid == 1;
        let items: Vec<(u32, SnapshotEntry)> = store.iter_stream_visible(Some(&visible)).collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0, 1);
        assert_eq!(items[0].1.values, vec![(1, TypedValue::F64(2.0))]);
    }
}
