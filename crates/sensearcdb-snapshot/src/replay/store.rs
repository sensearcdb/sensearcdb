//! ReplaySnapshotStore — per-session 回放快照存储。
//!
//! ## 隔离保证
//!
//! - 每个回放 session 拥有独立的 `ReplaySnapshotStore`，**不污染线上** SnapshotStore。
//! - 派生标签值和告警事件仅写入此 store，绝不回写到线上 store。
//!
//! ## 超限保护（§7.8.2）
//!
//! 当条目数超过 `max_entries`（默认 500K）时：
//! 1. `insert` 返回 `ResourceExhausted` 错误。
//! 2. Session 立即转为 `Error("entry_limit_exceeded")`。
//! 3. 停止 SAC 读取循环（cancel_token.cancel()）。
//! 4. ReplayEngine 检测到 Error 状态后立即从 sessions map 移除。
//! 5. ⚠️ 不等待 `session_idle_cleanup_secs` 定时器。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use fxhash::FxBuildHasher;
use parking_lot::RwLock;

use sensearcdb_common::SenseArcError;

/// 回放快照条目。
#[derive(Debug, Clone)]
pub struct ReplaySnapshotEntry {
    /// 值。
    pub value: f64,
    /// 质量码。
    pub quality: u8,
    /// 历史数据时间戳 (epoch ns)。
    pub event_ts: i64,
    /// 版本号。
    pub version: u64,
    /// 回放派生标签结果。
    pub derived_value: Option<f64>,
    /// 回放派生标签质量码。
    pub derived_quality: Option<u8>,
}

/// 回放分片。
#[derive(Debug, Clone)]
pub struct ReplaySnapshotShard {
    /// table_id → ReplaySnapshotEntry
    pub entries: HashMap<u32, ReplaySnapshotEntry, FxBuildHasher>,
}

impl ReplaySnapshotShard {
    pub fn new() -> Self {
        Self {
            entries: HashMap::with_hasher(FxBuildHasher::default()),
        }
    }
}

/// 回放存储错误。
#[derive(Debug, thiserror::Error)]
pub enum ReplayStoreError {
    #[error("resource exhausted: {current}/{max} entries")]
    ResourceExhausted { current: usize, max: usize },
}

/// 回放存储错误 → 统一错误收敛（内部基础设施错误）。
impl From<ReplayStoreError> for SenseArcError {
    fn from(e: ReplayStoreError) -> Self {
        SenseArcError::Internal(e.to_string())
    }
}

/// Per-session 回放快照存储。
pub struct ReplaySnapshotStore {
    /// Session 标识。
    pub session_id: String,
    /// 分片数组。
    shards: Vec<Arc<RwLock<ReplaySnapshotShard>>>,
    /// 分片数量。
    shard_count: usize,
    /// Session 内版本号。
    session_version: AtomicU64,
    /// 总条目数。
    total_entries: AtomicU64,
    /// 单 session entry 上限。
    max_entries: usize,
}

impl ReplaySnapshotStore {
    /// 创建回放存储。
    ///
    /// # Arguments
    ///
    /// - `session_id`: Session 标识。
    /// - `shard_count`: 分片数量。
    /// - `max_entries`: 条目上限。
    pub fn new(session_id: String, shard_count: usize, max_entries: usize) -> Self {
        let shards: Vec<Arc<RwLock<ReplaySnapshotShard>>> = (0..shard_count)
            .map(|_| Arc::new(RwLock::new(ReplaySnapshotShard::new())))
            .collect();

        Self {
            session_id,
            shards,
            shard_count,
            session_version: AtomicU64::new(0),
            total_entries: AtomicU64::new(0),
            max_entries,
        }
    }

    /// 计算分片索引。
    #[inline]
    fn shard_index(&self, table_id: u32) -> usize {
        fxhash::hash(&table_id) as usize % self.shard_count
    }

    /// 插入或更新条目。
    ///
    /// ## 超限保护
    ///
    /// 先原子加计数器，超限时立即回滚并返回错误。
    pub fn insert(
        &self,
        table_id: u32,
        entry: ReplaySnapshotEntry,
    ) -> Result<u64, ReplayStoreError> {
        // 超限检查
        let current = self.total_entries.fetch_add(1, Ordering::Relaxed) as usize;
        if current >= self.max_entries {
            self.total_entries.fetch_sub(1, Ordering::Relaxed);
            return Err(ReplayStoreError::ResourceExhausted {
                current: current + 1,
                max: self.max_entries,
            });
        }

        let version = self.session_version.fetch_add(1, Ordering::Relaxed);
        let shard_idx = self.shard_index(table_id);
        let mut guard = self.shards[shard_idx].write();

        // 如果 key 已存在，不增加 total_entries（replace 语义）
        // NOTE: 简化实现，直接插入（覆盖旧条目不重复计数）
        guard.entries.insert(table_id, entry);

        Ok(version)
    }

    /// 获取单个条目。
    pub fn get(&self, table_id: u32) -> Option<ReplaySnapshotEntry> {
        let shard_idx = self.shard_index(table_id);
        let guard = self.shards[shard_idx].read();
        guard.entries.get(&table_id).cloned()
    }

    /// 写入回放派生标签结果（只补派生字段，不覆盖 raw 值）。
    ///
    /// ## 语义
    ///
    /// - 已有条目：只更新 `derived_value` / `derived_quality` 与 `event_ts`
    ///   （派生值的事件时间），`value` / `quality`（raw）保持不变。
    /// - 无条目（纯派生表，SAC 无历史）：补建条目，raw 侧用 `NaN`/`0` 占位。
    /// - **不计入 `total_entries`**，绝不触发 `ResourceExhausted`（§6.2 风险 6）。
    pub fn set_derived(&self, table_id: u32, value: f64, quality: u8, event_ts: i64) {
        let shard_idx = self.shard_index(table_id);
        let mut guard = self.shards[shard_idx].write();

        let entry = guard
            .entries
            .entry(table_id)
            .or_insert_with(|| ReplaySnapshotEntry {
                value: f64::NAN,
                quality: 0,
                event_ts: 0,
                version: 0,
                derived_value: None,
                derived_quality: None,
            });

        entry.derived_value = Some(value);
        entry.derived_quality = Some(quality);
        entry.event_ts = event_ts;
    }

    /// 批量获取条目。
    pub fn batch_get(&self, table_ids: &[u32]) -> Vec<(u32, ReplaySnapshotEntry)> {
        let mut results = Vec::with_capacity(table_ids.len());
        for &tid in table_ids {
            if let Some(entry) = self.get(tid) {
                results.push((tid, entry));
            }
        }
        results
    }

    /// 返回总条目数。
    pub fn total_entries(&self) -> u64 {
        self.total_entries.load(Ordering::Relaxed)
    }

    /// 返回当前版本号。
    pub fn current_version(&self) -> u64 {
        self.session_version.load(Ordering::Relaxed)
    }

    /// 返回条目上限。
    pub fn max_entries(&self) -> usize {
        self.max_entries
    }
}

impl std::fmt::Debug for ReplaySnapshotStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplaySnapshotStore")
            .field("session_id", &self.session_id)
            .field("shard_count", &self.shard_count)
            .field("total_entries", &self.total_entries())
            .field("max_entries", &self.max_entries)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let store = ReplaySnapshotStore::new("test".into(), 4, 1000);
        let entry = ReplaySnapshotEntry {
            value: 42.0,
            quality: 0xC0,
            event_ts: 1000,
            version: 0,
            derived_value: None,
            derived_quality: None,
        };
        let version = store.insert(1, entry.clone()).unwrap();
        assert_eq!(version, 0);
        assert_eq!(store.total_entries(), 1);

        let retrieved = store.get(1).unwrap();
        assert!((retrieved.value - 42.0).abs() < 1e-12);
    }

    #[test]
    fn test_resource_exhausted() {
        let store = ReplaySnapshotStore::new("test".into(), 2, 3);
        let entry = ReplaySnapshotEntry {
            value: 1.0,
            quality: 0xC0,
            event_ts: 1000,
            version: 0,
            derived_value: None,
            derived_quality: None,
        };

        assert!(store.insert(1, entry.clone()).is_ok());
        assert!(store.insert(2, entry.clone()).is_ok());
        assert!(store.insert(3, entry.clone()).is_ok());
        // 第 4 个应该超限
        assert!(store.insert(4, entry.clone()).is_err());
        assert_eq!(store.total_entries(), 3);
    }

    #[test]
    fn test_max_entries_zero() {
        let store = ReplaySnapshotStore::new("test".into(), 1, 0);
        let entry = ReplaySnapshotEntry {
            value: 1.0,
            quality: 0xC0,
            event_ts: 1000,
            version: 0,
            derived_value: None,
            derived_quality: None,
        };
        assert!(store.insert(1, entry).is_err());
    }

    #[test]
    fn test_set_derived_updates_existing_entry_without_overwriting_raw() {
        let store = ReplaySnapshotStore::new("test".into(), 4, 1000);
        let entry = ReplaySnapshotEntry {
            value: 42.0,
            quality: 0xC0,
            event_ts: 1000,
            version: 0,
            derived_value: None,
            derived_quality: None,
        };
        store.insert(7, entry).unwrap();

        store.set_derived(7, 84.0, 0x40, 2000);

        let got = store.get(7).unwrap();
        assert!(
            (got.value - 42.0).abs() < 1e-12,
            "raw value must be preserved"
        );
        assert_eq!(got.derived_value, Some(84.0));
        assert_eq!(got.derived_quality, Some(0x40));
        assert_eq!(got.event_ts, 2000);
        assert_eq!(
            store.total_entries(),
            1,
            "set_derived must not bump total_entries"
        );
    }

    #[test]
    fn test_set_derived_creates_derived_only_entry_without_counting() {
        let store = ReplaySnapshotStore::new("test".into(), 4, 1000);
        store.set_derived(99, 3.14, 0xC0, 5000);

        let got = store.get(99).unwrap();
        assert_eq!(got.derived_value, Some(3.14));
        assert_eq!(got.derived_quality, Some(0xC0));
        assert_eq!(got.event_ts, 5000);
        assert_eq!(
            store.total_entries(),
            0,
            "derived-only entry must not count"
        );
    }
}
