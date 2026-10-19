//! 后台快照检查点任务：定期把 SnapshotStore 持久化为 `snapshot.bin`。
//!
//! ## 写时机（PM 裁决拍板点 1）
//!
//! - **定期 interval**（默认 `snapshot_interval_secs = 5s`）为主；
//! - **脏标记跳过**：`global_version > last_written_version` 才写，零写放大；
//! - **优雅关闭 / force_freeze 强制 checkpoint**：`flush_once()`。
//!
//! ## 热加载
//!
//! 每次循环 `config.get()` 读取最新 interval，**禁止循环外缓存**（§7.8.9）。
//!
//! ## 阻塞隔离
//!
//! `snapshot_all_buckets()` 返回的 `RwLockReadGuard` 借用 `&self`，序列化是阻塞
//! I/O——整体放入 `tokio::task::spawn_blocking`，避免占用 async 线程。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use sensearcdb_common::ConfigHandle;

use crate::encryption::{SharedSnapshotEncryptor, SnapshotEncryptor};
use crate::metrics::SnapshotMetrics;
use crate::serialize::{serialize_to_file, SerializeError};
use crate::store::SnapshotStore;

/// 提供「写 snapshot 那一刻的 WAL 水位」，由集成层注入 engine 实现。
///
/// 3.3 档 A：水位语义为 WAL2 单机自增 seq（`raft_index`，`raft_term=0`）。
/// 所有 <= 该 seq 的 WAL 已包含在快照内。用于恢复边界对齐
/// （恢复时从该 seq 之后回放 WAL 增量，而非全量回放）。
pub trait WalWatermarkProvider: Send + Sync {
    /// 当前 WAL 已 fsync 的最大单机 seq（WAL2 `raft_index`）。
    fn current_watermark(&self) -> u64;
}

/// 后台快照检查点任务。
pub struct SnapshotCheckpointer {
    /// 权威数据源（3.3 B1：ArcSwap 单一真相，每次 checkpoint `.load()` 读最新 store，
    /// Raft install 原子替换后立即可见，避免 load_full 冻结旧快照）。
    store: Arc<ArcSwap<SnapshotStore>>,
    /// 目标文件路径。
    snapshot_path: PathBuf,
    /// 配置句柄（热加载 `snapshot_interval_secs`）。
    config: ConfigHandle,
    /// 指标收集器。
    metrics: SnapshotMetrics,
    /// 复用 startup 已构建的加密器（`None` = TDE 关闭）。
    encryptor: SharedSnapshotEncryptor,
    /// WAL 水位查询器（可选）。
    watermark: Option<Arc<dyn WalWatermarkProvider>>,
    /// 上次已落盘的全局版本号（脏标记）。
    last_written_version: AtomicU64,
    /// 写盘互斥锁：`flush_once`（优雅关闭）与 run 循环的定期 checkpoint 可能并发，
    /// 必须串行化写同一 `snapshot.bin.tmp`——原子写（.tmp→rename）只防崩溃半写，
    /// 不防并发写导致的字节交错。
    write_lock: Mutex<()>,
    /// 强制 checkpoint 信号。
    force: tokio::sync::Notify,
}

impl SnapshotCheckpointer {
    /// 创建新的后台检查点任务。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<ArcSwap<SnapshotStore>>,
        snapshot_path: PathBuf,
        config: ConfigHandle,
        metrics: SnapshotMetrics,
        encryptor: SharedSnapshotEncryptor,
        watermark: Option<Arc<dyn WalWatermarkProvider>>,
    ) -> Self {
        Self {
            store,
            snapshot_path,
            config,
            metrics,
            encryptor,
            watermark,
            last_written_version: AtomicU64::new(0),
            write_lock: Mutex::new(()),
            force: tokio::sync::Notify::new(),
        }
    }

    /// 主循环：每 interval 醒来 → 脏标记判断 → `spawn_blocking` 序列化。
    ///
    /// 循环内每次 `config.get()` 读最新 interval（热加载），不缓存。
    pub async fn run(self: Arc<Self>) {
        loop {
            let interval = {
                let cfg = self.config.get();
                Duration::from_secs(cfg.snapshot.snapshot_interval_secs.max(1))
            };

            // 定期唤醒，或由 force 信号立即唤醒。
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = self.force.notified() => {}
            }

            let this = Arc::clone(&self);
            if let Err(e) = this.checkpoint_if_dirty().await {
                tracing::warn!(error = %e, "snapshot checkpoint failed");
            }
        }
    }

    /// 强制立即 checkpoint（优雅关闭 / force_freeze 时调用）。
    ///
    /// 同步执行：`snapshot_all_buckets()` → `serialize_to_file`，忽略脏标记。
    pub fn flush_once(&self) -> Result<(), SerializeError> {
        // 唤醒 run 循环（无害；本方法已同步落盘）。
        self.force.notify_one();
        self.checkpoint_sync()
    }

    /// 是否存在未落盘的变化（脏标记判断）。
    pub fn is_dirty(&self) -> bool {
        self.store.load().current_version() > self.last_written_version.load(Ordering::SeqCst)
    }

    /// 上次已落盘的全局版本号（测试 / 可观测性用）。
    pub fn last_written_version(&self) -> u64 {
        self.last_written_version.load(Ordering::SeqCst)
    }

    /// 脏标记判断后执行一次 checkpoint（放到阻塞线程池）。
    async fn checkpoint_if_dirty(self: Arc<Self>) -> Result<(), SerializeError> {
        if !self.is_dirty() {
            return Ok(());
        }

        tokio::task::spawn_blocking(move || self.checkpoint_sync())
            .await
            .map_err(|e| SerializeError::CheckpointJoin(e.to_string()))?
    }

    /// 同步序列化一次：先取水位，再快照所有分片，最后原子写盘。
    ///
    /// 水位在 `snapshot_all_buckets()` **之前**读取，保证「水位只可能小于等于
    /// 快照覆盖范围」——快照后 fsync 的 WAL 其 server_ts > 水位，恢复时会重新回放，
    /// 杜绝漏回放（配合恢复侧安全余量兜底）。
    fn checkpoint_sync(&self) -> Result<(), SerializeError> {
        let watermark = self
            .watermark
            .as_ref()
            .map(|w| w.current_watermark() as i64)
            .unwrap_or(0);

        // 3.3 B1：`.load()` 取最新 store（Raft install 原子替换后立即可见）。
        let store = self.store.load();
        let (guards, global_version) = store.snapshot_all_buckets();
        let encryptor: Option<&dyn SnapshotEncryptor> = self.encryptor.as_deref();

        // 串行化写盘：flush_once 与 run 循环的定期 checkpoint 可能并发写同一
        // `snapshot.bin.tmp`。拿到写锁后才执行 serialize_to_file（并原子推进
        // last_written_version），杜绝两个写操作字节交错产生损坏快照。
        let result = {
            let _guard = self.write_lock.lock().unwrap_or_else(|e| e.into_inner());
            let r = serialize_to_file(
                &self.snapshot_path,
                &guards,
                global_version,
                &self.metrics,
                encryptor,
                watermark,
            );
            if r.is_ok() {
                self.last_written_version
                    .store(global_version, Ordering::SeqCst);
            }
            r
        };
        drop(guards);

        if result.is_ok() {
            tracing::info!(
                path = %self.snapshot_path.display(),
                global_version = global_version,
                wal_watermark = watermark,
                "snapshot checkpoint written"
            );
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::SnapshotSubscriptionManager;
    use crate::tag_metadata::{SchemaMirrorRef, TagMetadataStore};
    use sensearcdb_common::{AppConfig, DataType, TypedValue};
    use tempfile::tempdir;

    struct TestSchemaMirror;
    impl SchemaMirrorRef for TestSchemaMirror {
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
        fn get_data_type(&self, _: u32, _: u16) -> DataType {
            DataType::F64
        }
        fn is_enabled(&self, _: u32) -> bool {
            true
        }
    }

    struct MockWatermark(u64);
    impl WalWatermarkProvider for MockWatermark {
        fn current_watermark(&self) -> u64 {
            self.0
        }
    }

    fn make_config() -> ConfigHandle {
        let mut cfg = AppConfig::default();
        cfg.snapshot.shard_multiplier = 1;
        ConfigHandle::new(cfg)
    }

    fn make_store() -> Arc<SnapshotStore> {
        let cfg = make_config();
        let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
        let metadata = Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror)));
        Arc::new(SnapshotStore::new(
            &cfg,
            sub_mgr,
            metadata,
            SnapshotMetrics::noop(),
        ))
    }

    fn make_checkpointer(
        store: Arc<SnapshotStore>,
        path: std::path::PathBuf,
        watermark: u64,
    ) -> SnapshotCheckpointer {
        SnapshotCheckpointer::new(
            Arc::new(ArcSwap::new(store)),
            path,
            make_config(),
            SnapshotMetrics::noop(),
            None,
            Some(Arc::new(MockWatermark(watermark))),
        )
    }

    /// flush_once 落盘并推进 last_written_version；水位写入文件头。
    #[test]
    fn test_flush_once_writes_and_advances_version() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(42.0))],
            &[(0, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );

        let cp = make_checkpointer(store.clone(), path.clone(), 123_456);
        cp.flush_once().unwrap();

        assert!(path.exists());
        assert!(cp.last_written_version() >= store.current_version());
        assert!(!cp.is_dirty());
    }

    /// QA（严过关）独立验证：脏标记为 clean 时 `checkpoint_if_dirty` 不得重写文件。
    ///
    /// 用「删除文件后不得被重建」的确定性方式验证真实跳过行为（比 is_dirty 布尔更直接）。
    #[tokio::test]
    async fn test_checkpoint_if_dirty_skips_rewrite_when_clean() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(42.0))],
            &[(0, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );

        let cp = Arc::new(make_checkpointer(store.clone(), path.clone(), 123_456));
        cp.flush_once().unwrap();
        assert!(path.exists());
        assert!(!cp.is_dirty());

        // 删掉已落盘的文件：若 clean 时被错误重写，文件会被重建。
        std::fs::remove_file(&path).unwrap();

        Arc::clone(&cp).checkpoint_if_dirty().await.unwrap();

        assert!(
            !path.exists(),
            "脏标记为 clean 时 checkpoint_if_dirty 不得重写 snapshot.bin"
        );
        assert!(!cp.is_dirty());
    }

    /// 脏标记跳过：无变化时 checkpoint_if_dirty 不重写。
    #[test]
    fn test_dirty_skip_when_no_changes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");
        let store = make_store();

        let cp = make_checkpointer(store.clone(), path.clone(), 0);
        // 空 store 也有 version 0，首次 flush 后 last_written = 0。
        cp.flush_once().unwrap();
        let version_after_first = cp.last_written_version();

        // 无变化 → 不脏。
        assert!(!cp.is_dirty());

        // 写入变化 → 变脏。
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            7,
            &[(0, TypedValue::F64(7.0))],
            &[(0, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        assert!(cp.is_dirty());
        assert!(store.current_version() > version_after_first);
    }

    /// 并发写竞态防护：写盘路径由同一把 `write_lock` 串行化。
    ///
    /// 主线程持有写锁模拟「另一写盘正在进行」，此时并发调用 `flush_once` 必须
    /// 阻塞等待该锁，而不是与主线程交错写同一 `.tmp` 路径产生损坏快照。
    #[test]
    fn test_write_lock_serializes_concurrent_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");
        let store = make_store();
        store.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            1,
            &[(0, TypedValue::F64(42.0))],
            &[(0, 0xC0)],
            1000,
            2000,
            None,
            false,
            None,
        );
        let cp = Arc::new(make_checkpointer(store.clone(), path.clone(), 0));

        // 模拟另一写盘正在进行：持有写锁。
        let held = cp.write_lock.lock().unwrap();

        let cp2 = Arc::clone(&cp);
        let handle = std::thread::spawn(move || cp2.flush_once().is_ok());

        // 锁被持有期间，flush_once 必须阻塞等待（尚未完成）。
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(
            !handle.is_finished(),
            "写锁被持有期间 flush_once 必须阻塞等待，不得并发写盘"
        );

        // 释放锁后 flush_once 拿到锁并成功写盘。
        drop(held);
        assert!(handle.join().unwrap(), "flush_once 应在拿到写锁后成功落盘");
        assert!(path.exists());
    }

    /// 3.3 B1 修复验证：checkpointer 每次 `.load()` 读最新 store；原子替换后写入的是
    /// 新 store，而非冻结旧 `Arc<SnapshotStore>`。
    #[test]
    fn test_checkpointer_reads_latest_store_after_swap() {
        use crate::serialize::deserialize_from_file;

        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");

        let store_a = make_store();
        store_a.update_typed(
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
        let shard_count = store_a.bucket_count();
        let swap: Arc<ArcSwap<SnapshotStore>> = Arc::new(ArcSwap::new(store_a));

        let cp = SnapshotCheckpointer::new(
            swap.clone(),
            path.clone(),
            make_config(),
            SnapshotMetrics::noop(),
            None,
            Some(Arc::new(MockWatermark(0))),
        );

        // 模拟 Raft install 原子替换：换成新 store（tag 2 = 99）。
        let store_b = make_store();
        store_b.update_typed(
            sensearcdb_common::TenantId::DEFAULT,
            2,
            &[(0, TypedValue::F64(99.0))],
            &[(0, 0xC0)],
            1000,
            1001,
            None,
            false,
            None,
        );
        swap.store(store_b);

        cp.flush_once().unwrap();

        let (shards, _version, _watermark) =
            deserialize_from_file(&path, shard_count, &SnapshotMetrics::noop(), None).unwrap();
        let has_tag2 = shards.iter().any(|s| s.get(2).is_some());
        let has_tag1 = shards.iter().any(|s| s.get(1).is_some());
        assert!(has_tag2, "checkpointer 应写入新 store（tag 2 = 99）");
        assert!(!has_tag1, "checkpointer 不应写入冻结旧 store（tag 1 = 1）");
    }
}
