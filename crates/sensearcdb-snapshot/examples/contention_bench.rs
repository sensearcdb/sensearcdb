//! 临时：量化 snapshot 桶数对「8 worker 并发写」锁等待的影响。
//!
//! 8 线程各自写互不相交的 table 区间（TSBS 宽表 10 列），桶间碰撞仅由
//! `hash(table_id) % bucket_count` 决定。比较 shard_multiplier=2（64 桶）
//! 与 shard_multiplier=8（256 桶）的吞吐差异，间接反映锁等待下降。
//! 完成后随埋点一起移除。

use std::sync::Arc;
use std::time::Instant;

use sensearcdb_common::{AppConfig, ConfigHandle, DataType, TenantId, TypedValue};
use sensearcdb_snapshot::{
    SchemaMirrorRef, SnapshotMetrics, SnapshotStore, SnapshotSubscriptionManager, TagMetadataStore,
};

struct TestMirror;
impl SchemaMirrorRef for TestMirror {
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

fn run(multiplier: usize, threads: usize, iters_per_thread: usize) -> f64 {
    let mut cfg = AppConfig::default();
    cfg.snapshot.shard_multiplier = multiplier;
    let ch = ConfigHandle::new(cfg);
    let store = Arc::new(SnapshotStore::new(
        &ch,
        Arc::new(SnapshotSubscriptionManager::new()),
        Arc::new(TagMetadataStore::new(Arc::new(TestMirror))),
        SnapshotMetrics::noop(),
    ));

    // TSBS 宽表 10 列（对齐 fanout_bench / 真实 write 路径）。
    const COLS: usize = 10;
    const N_TABLES: u32 = 4000;
    let per_thread_tables = N_TABLES / threads as u32;

    let mut values: Vec<(u16, TypedValue)> = Vec::with_capacity(COLS);
    for c in 0..9u16 {
        values.push((c, TypedValue::F64(c as f64)));
    }
    values.push((9u16, TypedValue::I64(1)));
    let qualities: Vec<(u16, u8)> = (0..10u16).map(|c| (c, 0xC0u8)).collect();

    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(threads);
    for t in 0..threads {
        let store = store.clone();
        let values = values.clone();
        let qualities = qualities.clone();
        handles.push(std::thread::spawn(move || {
            let base = t as u32 * per_thread_tables;
            for i in 0..iters_per_thread {
                let table_id = base + (i as u32 % per_thread_tables) + 1;
                store.update_typed(
                    TenantId::DEFAULT,
                    table_id,
                    &values,
                    &qualities,
                    i as i64,
                    i as i64 + 1,
                    Some(i as u64),
                    false,
                    None,
                );
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let secs = t0.elapsed().as_secs_f64();
    (threads * iters_per_thread) as f64 / secs
}

fn main() {
    const THREADS: usize = 8;
    const ITERS: usize = 200_000;

    // 预热 + 顺序固定：先 64 桶后 256 桶。
    let t2 = run(2, THREADS, ITERS);
    let t8 = run(8, THREADS, ITERS);

    println!("=== snapshot 分桶锁并行 contention bench ({} threads) ===", THREADS);
    println!(
        "multiplier=2  (64 桶) : {:.0} points/s",
        t2
    );
    println!(
        "multiplier=8  (256 桶): {:.0} points/s",
        t8
    );
    println!(
        "speedup: {:.2}x",
        t8 / t2
    );
}
