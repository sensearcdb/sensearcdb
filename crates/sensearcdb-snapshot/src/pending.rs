//! pending 缓冲 + 独立 flusher（S2 写路径解耦）。
//!
//! 写路径（`write_batch`/`apply_batch_in_memory`）不再逐点 `update_typed` 同步落
//! 列式存储，而是把点 append 到 [`SnapshotPendingBuffer`]，由独立 [`SnapshotFlusher`]
//! 定时（`flush_interval_ms`）/ 满阈值（`flush_batch_size` 点 或 `flush_bytes` 字节）
//! 双触发批量 `update_batch`。
//!
//! ## 并发模型
//!
//! - **写路径**：`append` / `append_batch` 只在 `active` 上取一次极短
//!   `parking_lot::Mutex` 锁（push + 原子计数），无 snapshot 写锁、无 arrow builder。
//! - **flusher**：`swap()` 短锁 `mem::take` 走整批，消费在锁外（`update_batch`
//!   内部才逐个 shard 取写锁）。
//! - **双缓冲等价**：`mem::take` 天然实现 active↔flushing 原子交换，无需两个字段。
//! - **不阻塞写路径**：满阈值只在 append 后 `notify_one()`（异步信号）。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use sensearcdb_common::{ConfigHandle, TenantId, TypedValue};

use crate::columnar::PendingPoint;
use crate::metrics::SnapshotMetrics;
use crate::store::SnapshotStore;

/// pending 缓冲中的 owned 版待写入点（字段集合 = `columnar::PendingPoint`，values/qualities 为 owned）。
pub struct OwnedPendingPoint {
    pub tenant_id: TenantId,
    pub table_id: u32,
    pub values: Vec<(u16, TypedValue)>,
    pub qualities: Vec<(u16, u8)>,
    pub ts: i64,
    pub server_ts: i64,
    /// LWW 决胜键：单机 = 逐点 WAL seq；集群 = raft_index；PITR/导入 = None。
    pub write_seq: Option<u64>,
    /// 生产 sink 恒 false（现状 update_typed 传 false）。
    pub is_derived: bool,
    /// 生产 sink 恒 None（现状 update_typed 传 None）。
    pub writer_session_id: Option<u64>,
}

impl OwnedPendingPoint {
    /// 转借用版供 `SnapshotStore::update_batch(&[PendingPoint<'_>])` 消费。
    pub fn as_pending_point(&self) -> PendingPoint<'_> {
        PendingPoint {
            tenant_id: self.tenant_id,
            table_id: self.table_id,
            values: self.values.as_slice(),
            qualities: self.qualities.as_slice(),
            ts: self.ts,
            server_ts: self.server_ts,
            write_seq: self.write_seq,
            is_derived: self.is_derived,
            writer_session_id: self.writer_session_id,
        }
    }
}

/// pending 双缓冲：写路径短锁 append，flusher 短锁 `mem::take` 走整批。
pub struct SnapshotPendingBuffer {
    /// 写路径短锁 append 的 active 缓冲（`parking_lot::Mutex`，非 std/tokio Mutex）。
    active: parking_lot::Mutex<Vec<OwnedPendingPoint>>,
    /// active 的近似字节数（append 时 fetch_add，swap 时清零；非精确、仅阈值触发用）。
    active_bytes: AtomicUsize,
    /// 满点阈值（默认 8192）。
    max_batch_size: usize,
    /// 满字节阈值（默认 4MB = 4 * 1024 * 1024）。
    max_bytes: usize,
    /// 满阈值唤醒 flusher 的信号。
    force: tokio::sync::Notify,
}

impl SnapshotPendingBuffer {
    pub fn new(max_batch_size: usize, max_bytes: usize) -> Self {
        Self {
            active: parking_lot::Mutex::new(Vec::new()),
            active_bytes: AtomicUsize::new(0),
            max_batch_size,
            max_bytes,
            force: tokio::sync::Notify::new(),
        }
    }

    /// 单点 append（apply 路径逐点 `on_write` 用）。短锁 push + 累加字节，满阈值则 notify。
    pub fn append(&self, point: OwnedPendingPoint) {
        let bytes = approx_bytes(&point);
        let should_flush = {
            let mut active = self.active.lock();
            self.active_bytes.fetch_add(bytes, Ordering::Relaxed);
            active.push(point);
            active.len() >= self.max_batch_size
                || self.active_bytes.load(Ordering::Relaxed) >= self.max_bytes
        };
        if should_flush {
            self.force.notify_one();
        }
    }

    /// 批量 append（`on_write_batch` 用，一次锁 push 整批）。
    pub fn append_batch(&self, points: Vec<OwnedPendingPoint>) {
        if points.is_empty() {
            return;
        }
        let total_bytes: usize = points.iter().map(approx_bytes).sum();
        let should_flush = {
            let mut active = self.active.lock();
            self.active_bytes.fetch_add(total_bytes, Ordering::Relaxed);
            active.extend(points);
            active.len() >= self.max_batch_size
                || self.active_bytes.load(Ordering::Relaxed) >= self.max_bytes
        };
        if should_flush {
            self.force.notify_one();
        }
    }

    /// flusher 独占消费：短锁取走全部 active（等价 active↔flushing 交换），返回 owned 批。
    pub fn swap(&self) -> Vec<OwnedPendingPoint> {
        let mut active = self.active.lock();
        self.active_bytes.store(0, Ordering::Relaxed);
        std::mem::take(&mut *active)
    }

    /// 是否有待 flush 点（测试 / 可观测性用）。
    pub fn pending_len(&self) -> usize {
        self.active.lock().len()
    }
}

/// 近似字节估算（不精确；String 按长度，其余按固定 size_of）。
fn approx_bytes(p: &OwnedPendingPoint) -> usize {
    let base = std::mem::size_of::<OwnedPendingPoint>()
        + p.values.capacity() * std::mem::size_of::<(u16, TypedValue)>()
        + p.qualities.capacity() * std::mem::size_of::<(u16, u8)>();
    let strings: usize = p
        .values
        .iter()
        .map(|(_, v)| if let TypedValue::String(s) = v { s.len() } else { 0 })
        .sum();
    base + strings
}

/// 独立 flusher：定时 / 满阈值双触发，消费 pending 批量 `update_batch`。
pub struct SnapshotFlusher {
    pending: Arc<SnapshotPendingBuffer>,
    /// 每次 flush `.load()` 读最新 store（对齐 checkpointer 惯例，Raft install / 恢复 swap 后立即可见）。
    store: Arc<ArcSwap<SnapshotStore>>,
    /// 热加载 flush_interval_ms。
    config: ConfigHandle,
    metrics: SnapshotMetrics,
}

impl SnapshotFlusher {
    pub fn new(
        pending: Arc<SnapshotPendingBuffer>,
        store: Arc<ArcSwap<SnapshotStore>>,
        config: ConfigHandle,
        metrics: SnapshotMetrics,
    ) -> Self {
        Self {
            pending,
            store,
            config,
            metrics,
        }
    }

    /// 主循环：定时唤醒 或 满阈值 notify 唤醒 → flush。
    pub async fn run(self: Arc<Self>) {
        loop {
            let interval = {
                let cfg = self.config.get();
                Duration::from_millis(cfg.snapshot.flush_interval_ms.max(1))
            };
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = self.pending.force.notified() => {}
            }
            self.flush_now();
        }
    }

    /// 消费一次 pending：swap → 转借用切片 → 调 store.update_batch（fire-and-forget）。
    ///
    /// `pub` 供 derived 去抖 flusher 在重算前同步触发一次 snapshot flush，
    /// 保证 derived 读 snapshot 前本批源值已 flush（时序编排，见 P4）。
    /// swap 短锁 `mem::take` 幂等，与独立 snapshot flusher 并发调用无数据丢失。
    pub fn flush_now(&self) {
        let pts = self.pending.swap();
        if pts.is_empty() {
            return;
        }
        let store = self.store.load();
        let batch: Vec<PendingPoint<'_>> = pts.iter().map(|p| p.as_pending_point()).collect();
        // update_batch 是纯内存 CPU 操作（无 I/O）；直接同步调用即可。
        store.update_batch(&batch);
        self.metrics
            .updates_total
            .with_label_values(&[])
            .inc_by(pts.len() as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sensearcdb_common::{AppConfig, DataType, TenantId, TypedValue};

    use crate::tag_metadata::SchemaMirrorRef;
    use crate::tag_metadata::TagMetadataStore;
    use crate::subscription::SnapshotSubscriptionManager;

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

    fn make_store() -> (Arc<ArcSwap<SnapshotStore>>, Arc<SnapshotStore>) {
        let mut cfg = AppConfig::default();
        cfg.snapshot.shard_multiplier = 1;
        let handle = ConfigHandle::new(cfg);
        let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
        let metadata = Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror)));
        let store = Arc::new(SnapshotStore::new(
            &handle,
            sub_mgr,
            metadata,
            SnapshotMetrics::noop(),
        ));
        let swap = Arc::new(ArcSwap::new(store.clone()));
        (swap, store)
    }

    fn point(table_id: u32, v: f64, write_seq: Option<u64>) -> OwnedPendingPoint {
        OwnedPendingPoint {
            tenant_id: TenantId::DEFAULT,
            table_id,
            values: vec![(0u16, TypedValue::F64(v))],
            qualities: vec![(0u16, 0xC0)],
            ts: 100,
            server_ts: 200,
            write_seq,
            is_derived: false,
            writer_session_id: None,
        }
    }

    #[test]
    fn test_pending_buffer_append_and_swap() {
        let buf = SnapshotPendingBuffer::new(8192, 4 * 1024 * 1024);
        buf.append(point(1, 1.0, Some(10)));
        buf.append(point(2, 2.0, Some(11)));

        let batch = buf.swap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].table_id, 1);
        assert_eq!(batch[0].values, vec![(0u16, TypedValue::F64(1.0))]);
        assert_eq!(batch[0].qualities, vec![(0u16, 0xC0)]);
        assert_eq!(batch[0].write_seq, Some(10));
        assert_eq!(batch[1].table_id, 2);
        assert_eq!(batch[1].write_seq, Some(11));
        // swap 后 active 清空。
        assert_eq!(buf.pending_len(), 0);
        assert!(buf.swap().is_empty());
    }

    #[test]
    fn test_pending_buffer_append_batch() {
        let buf = SnapshotPendingBuffer::new(8192, 4 * 1024 * 1024);
        let pts = vec![point(1, 1.0, Some(1)), point(2, 2.0, None)];
        buf.append_batch(pts);

        let batch = buf.swap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[1].table_id, 2);
        assert_eq!(batch[1].write_seq, None);
        assert_eq!(buf.pending_len(), 0);
    }

    #[tokio::test]
    async fn test_flusher_timer_trigger() {
        let (swap, store) = make_store();
        let mut cfg = AppConfig::default();
        cfg.snapshot.shard_multiplier = 1;
        cfg.snapshot.flush_interval_ms = 10;
        let handle = ConfigHandle::new(cfg);

        let pending = Arc::new(SnapshotPendingBuffer::new(8192, 4 * 1024 * 1024));
        let flusher = Arc::new(SnapshotFlusher::new(
            pending.clone(),
            swap,
            handle,
            SnapshotMetrics::noop(),
        ));
        tokio::spawn(async move { flusher.run().await });

        pending.append(point(7, 3.5, Some(5)));

        // 轮询至 lastpoint 可见（延迟 ≤ 若干倍 interval）。
        let mut seen = false;
        for _ in 0..200 {
            if let Some((_, v)) = store.batch_get_typed(&[7]).into_iter().next() {
                if v.value() == 3.5 {
                    seen = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(seen, "flusher 定时触发后 lastpoint 应可见");
    }

    #[tokio::test]
    async fn test_flusher_threshold_trigger() {
        let (swap, store) = make_store();
        let mut cfg = AppConfig::default();
        cfg.snapshot.shard_multiplier = 1;
        // 定时器设极大，只靠满阈值触发。
        cfg.snapshot.flush_interval_ms = 60_000;
        let handle = ConfigHandle::new(cfg);

        let pending = Arc::new(SnapshotPendingBuffer::new(2, 4 * 1024 * 1024));
        let flusher = Arc::new(SnapshotFlusher::new(
            pending.clone(),
            swap,
            handle,
            SnapshotMetrics::noop(),
        ));
        tokio::spawn(async move { flusher.run().await });

        pending.append(point(8, 1.0, Some(1)));
        pending.append(point(8, 9.0, Some(2))); // 满阈值（2 点）触发 notify

        let mut seen = false;
        for _ in 0..200 {
            if let Some((_, v)) = store.batch_get_typed(&[8]).into_iter().next() {
                if v.value() == 9.0 {
                    seen = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(seen, "满阈值触发后 lastpoint 应不依赖定时器即可见");
    }

    #[tokio::test]
    async fn test_write_seq_lww_across_batches() {
        let (swap, store) = make_store();
        let mut cfg = AppConfig::default();
        cfg.snapshot.shard_multiplier = 1;
        cfg.snapshot.flush_interval_ms = 5;
        let handle = ConfigHandle::new(cfg);

        let pending = Arc::new(SnapshotPendingBuffer::new(8192, 4 * 1024 * 1024));
        let flusher = Arc::new(SnapshotFlusher::new(
            pending.clone(),
            swap,
            handle,
            SnapshotMetrics::noop(),
        ));
        tokio::spawn(async move { flusher.run().await });

        // 同 (table_id, column_id) 同 ts，低 seq 先入、高 seq 后入，跨批 flush。
        pending.append(point(9, 1.0, Some(1)));
        tokio::time::sleep(Duration::from_millis(20)).await;
        pending.append(point(9, 2.0, Some(2)));
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut final_val = 0.0;
        for _ in 0..200 {
            if let Some((_, v)) = store.batch_get_typed(&[9]).into_iter().next() {
                final_val = v.value();
                if final_val == 2.0 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(final_val, 2.0, "同 ts 高 seq 应跨批胜出");
    }
}
