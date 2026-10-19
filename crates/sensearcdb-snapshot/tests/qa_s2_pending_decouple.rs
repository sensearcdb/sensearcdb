//! QA 独立边界测试（S2 写路径解耦，fresh eyes）。
//!
//! 针对「append pending 缓冲 + 独立 flusher 批量 update_batch」的数据正确性敏感改动，
//! 不采信工程师自测，独立验证：
//! - d1：多线程 append 与 flusher 并发，零丢点、值不串（双缓冲 mem::take 交换正确性）。
//! - d2：lastpoint 延迟上界（flusher 定时触发真运转，写后 ≤ 有界时间可见）。
//! - d3：同 ts 高 seq 跨批 LWW（两批 flush，最终高 seq 胜，跨批不退化）。
//! - d4：append_batch 批量路径与 flush 并发，零丢点（覆盖 on_write_batch 一路）。

use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use sensearcdb_common::{AppConfig, ConfigHandle, DataType, TenantId, TypedValue};
use sensearcdb_snapshot::{
    OwnedPendingPoint, SchemaMirrorRef, SnapshotFlusher, SnapshotMetrics, SnapshotPendingBuffer,
    SnapshotStore, SnapshotSubscriptionManager, TagMetadataStore,
};

struct TestSchemaMirror;

impl SchemaMirrorRef for TestSchemaMirror {
    fn get_table_name(&self, _: u32) -> Option<String> {
        Some("tag".into())
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

/// 构造 store + 其 ArcSwap 包装（flusher 用 swap，测试直接读 store）。
fn make_store(shard_multiplier: usize) -> (Arc<ArcSwap<SnapshotStore>>, Arc<SnapshotStore>) {
    let mut cfg = AppConfig::default();
    cfg.snapshot.shard_multiplier = shard_multiplier;
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

/// 构造单列 F64 的 owned pending 点（唯一 table_id，ts 固定 100）。
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

fn store_value(store: &SnapshotStore, table_id: u32) -> Option<f64> {
    store
        .batch_get_typed(&[table_id])
        .into_iter()
        .next()
        .map(|(_, v)| v.value())
}

async fn wait_until_value(
    store: &SnapshotStore,
    table_id: u32,
    expected: f64,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        if store_value(store, table_id) == Some(expected) {
            return;
        }
        if Instant::now() > deadline {
            panic!(
                "timeout: table {} 未在 {:?} 内变为 {:?}（当前 {:?}）",
                table_id,
                timeout,
                expected,
                store_value(store, table_id)
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// ─── d1：多线程 append 与 flusher 并发，零丢点 ──────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d1_append_flush_concurrent_zero_loss() {
    let (swap, store) = make_store(1);
    let mut cfg = AppConfig::default();
    cfg.snapshot.shard_multiplier = 1;
    cfg.snapshot.flush_interval_ms = 1; // 定时器高频 flush，与 append 并发竞争
    let handle = ConfigHandle::new(cfg);
    let pending = Arc::new(SnapshotPendingBuffer::new(256, 256 * 1024)); // 小阈值也触发
    let flusher = Arc::new(SnapshotFlusher::new(
        pending.clone(),
        swap,
        handle,
        SnapshotMetrics::noop(),
    ));
    tokio::spawn(async move { flusher.run().await });

    const THREADS: u32 = 8;
    const PER_THREAD: u32 = 1000;
    let total = (THREADS * PER_THREAD) as usize;

    // 多 OS 线程并发 append（模拟写路径多线程），与 tokio flusher 天然并发竞争锁。
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let pending = pending.clone();
            std::thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let tid = t * PER_THREAD + i; // 唯一 table_id
                    pending.append(point(tid, tid as f64, Some(tid as u64)));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let all_ids: Vec<u32> = (0..total as u32).collect();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let seen = store.batch_get_typed(&all_ids).len();
        if seen == total {
            break;
        }
        if Instant::now() > deadline {
            panic!("零丢点失败：仅 {}/{} 可见", seen, total);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // 值不串：每个 tag 首列值 == tag id（append 的点未被覆盖/错位）。
    for (tid, v) in store.batch_get_typed(&all_ids) {
        assert_eq!(v.value(), tid as f64, "table {} 值错位", tid);
    }
}

// ─── d2：lastpoint 延迟上界（flusher 定时触发真运转） ───────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn d2_lastpoint_visibility_upper_bound() {
    let (swap, store) = make_store(1);
    let mut cfg = AppConfig::default();
    cfg.snapshot.shard_multiplier = 1;
    cfg.snapshot.flush_interval_ms = 20;
    let handle = ConfigHandle::new(cfg);
    let pending = Arc::new(SnapshotPendingBuffer::new(8192, 4 * 1024 * 1024));
    let flusher = Arc::new(SnapshotFlusher::new(
        pending.clone(),
        swap,
        handle,
        SnapshotMetrics::noop(),
    ));
    tokio::spawn(async move { flusher.run().await });

    let t0 = Instant::now();
    pending.append(point(7, 42.0, Some(1)));
    wait_until_value(&store, 7, 42.0, Duration::from_secs(5)).await;
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(2000),
        "lastpoint 应在有界时间内可见（实际 {}ms，远大于 flush_interval_ms=20ms）",
        elapsed.as_millis()
    );
}

// ─── d3：同 ts 高 seq 跨批 LWW（两批 flush，跨批不退化） ────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn d3_same_ts_high_seq_cross_batch_lww() {
    let (swap, store) = make_store(1);
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

    // 第一批：低 seq，等其 flush 后可见 1.0（证明已落库一批，而非批内合并）。
    pending.append(point(9, 1.0, Some(1)));
    wait_until_value(&store, 9, 1.0, Duration::from_secs(5)).await;

    // 第二批：高 seq（同 tag 同 ts），跨批覆盖。
    pending.append(point(9, 2.0, Some(2)));
    wait_until_value(&store, 9, 2.0, Duration::from_secs(5)).await;

    assert_eq!(
        store_value(&store, 9),
        Some(2.0),
        "同 ts 高 seq 应跨批胜出"
    );
}

// ─── d4：append_batch 批量路径与 flush 并发，零丢点 ─────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d4_append_batch_concurrent_zero_loss() {
    let (swap, store) = make_store(1);
    let mut cfg = AppConfig::default();
    cfg.snapshot.shard_multiplier = 1;
    cfg.snapshot.flush_interval_ms = 1;
    let handle = ConfigHandle::new(cfg);
    let pending = Arc::new(SnapshotPendingBuffer::new(256, 256 * 1024));
    let flusher = Arc::new(SnapshotFlusher::new(
        pending.clone(),
        swap,
        handle,
        SnapshotMetrics::noop(),
    ));
    tokio::spawn(async move { flusher.run().await });

    const THREADS: u32 = 4;
    const BATCH: u32 = 1000;
    let total = (THREADS * BATCH) as usize;

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let pending = pending.clone();
            std::thread::spawn(move || {
                let pts: Vec<OwnedPendingPoint> = (0..BATCH)
                    .map(|i| {
                        let tid = t * BATCH + i; // 唯一 table_id
                        point(tid, tid as f64, Some(tid as u64))
                    })
                    .collect();
                pending.append_batch(pts);
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let all_ids: Vec<u32> = (0..total as u32).collect();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let seen = store.batch_get_typed(&all_ids).len();
        if seen == total {
            break;
        }
        if Instant::now() > deadline {
            panic!("append_batch 零丢点失败：仅 {}/{} 可见", seen, total);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for (tid, v) in store.batch_get_typed(&all_ids) {
        assert_eq!(v.value(), tid as f64, "table {} 值错位", tid);
    }
}
